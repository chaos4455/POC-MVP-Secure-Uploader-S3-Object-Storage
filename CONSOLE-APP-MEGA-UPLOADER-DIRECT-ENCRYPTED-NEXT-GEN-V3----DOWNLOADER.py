import os
import sys
import json
import base64
import boto3
from botocore.exceptions import ClientError, ConnectionError, ReadTimeoutError
import humanize
import inquirer
import threading
import tkinter as tk
from tkinter import filedialog
from pathlib import Path
import io 
import hashlib # Necessário para o hash do log MD
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple
import sqlite3
from datetime import datetime

# Criptografia
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM 

# UI Console
from rich.console import Console
from rich.prompt import Prompt
from rich.panel import Panel
from rich.theme import Theme
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TransferSpeedColumn, TimeRemainingColumn, TimeElapsedColumn

# ==============================================================================
# CONFIGURAÇÕES ROBUSTAS
# ==============================================================================

DATA_S3_ENDPOINT = "XXX"
ACCESS_KEY = "XXX"
SECRET_KEY = "XXX"

SCRIPT_DIR = Path(__file__).resolve().parent
KEY_FILE = SCRIPT_DIR / "masterkey.storagekey"

CHUNK_SIZE = 64 * 1024  # 64KB
MAX_DOWNLOAD_THREADS = 4
MAX_RETRIES = 3       # Número máximo de tentativas de download
RETRY_DELAY = 5       # Tempo de espera (segundos) entre as tentativas

custom_theme = Theme({
    "info": "cyan", "warning": "yellow", "error": "bold red", "success": "bold green",
    "folder": "bold blue", "file": "white", "enc_file": "magenta", "retry": "yellow on black",
    "path": "dim white", "renamed": "bold yellow"
})
console = Console(theme=custom_theme)

# LOCAL DE ARMAZENAMENTO DOS LOGS E DB (Reutilizado do Uploader)
LOG_DIR = Path("C:\\DATA-SYNC-UPLODER-CONTROL-DATA")
LOG_DB_PATH = LOG_DIR / "control_data.db"


# ==============================================================================
# 1. GESTOR DE CRIPTOGRAFIA (DECRYPTION ENGINE)
# ==============================================================================

class CryptoManager:
    """Gerencia a MasterKey e a Descriptografia (Download)."""

    def __init__(self):
        self.master_key = None

    def _derive_key_from_password(self, password_bytes: bytes, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=32, salt=salt,
            iterations=100000, backend=default_backend()
        )
        return kdf.derive(password_bytes)

    def unlock_master_key(self, password: str) -> bool:
        password_bytes = password.strip().encode('utf-8')
        
        if not KEY_FILE.exists():
            console.print(f"[error]ERRO CRÍTICO: Arquivo '{KEY_FILE.name}' não encontrado! (Caminho: {KEY_FILE})[/error]")
            return False

        try:
            with open(KEY_FILE, 'r') as f:
                data = json.load(f)

            salt = base64.b64decode(data['salt'])
            nonce = base64.b64decode(data['nonce'])
            ciphertext_with_tag = base64.b64decode(data['ciphertext'])
            
            kek = self._derive_key_from_password(password_bytes, salt)
            
            aesgcm = AESGCM(kek)
            self.master_key = aesgcm.decrypt(nonce, ciphertext_with_tag, associated_data=None)
            
            return True

        except InvalidTag:
            console.print("[error]❌ SENHA INCORRETA.[/error] A assinatura criptográfica GCM falhou. [bold yellow](Verifique se a senha foi digitada exatamente como na criação.)[/bold yellow]")
            return False
        except json.JSONDecodeError:
            console.print("[error]❌ ARQUIVO CORROMPIDO.[/error] O arquivo masterkey.storagekey não é um JSON válido.")
            return False
        except Exception as e:
            safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
            console.print(f"[error]❌ ERRO TÉCNICO DESCONHECIDO:[/error] {type(e).__name__}: {safe_error}")
            return False

# ==============================================================================
# 2. CLASSE DE LOG E CONTROLE EM SQLITE (Reutilizada e adaptada)
# ==============================================================================

class LogControlDB:
    def __init__(self, pc_name: str, collection: str, action: str):
        # Para download, o "root_path" é o destino, mas o DB controla por bucket/coleção.
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(LOG_DB_PATH)
        self.cursor = self.conn.cursor()
        self.pc_name = pc_name
        self.collection = collection
        self.action = action # 'UPLOAD' ou 'DOWNLOAD'
        
        # Configurar o DB antes de tentar ler qualquer tabela! (FIX V5.2)
        self._setup_db() 
        
        self.run_id = self._get_next_run_id()
        self.timestamp_start = datetime.now()

    def _setup_db(self):
        # Tabela para registrar cada execução
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS run_log (
                run_id INTEGER PRIMARY KEY,
                pc_name TEXT,
                collection_name TEXT,
                action TEXT,
                timestamp_start TEXT,
                timestamp_end TEXT,
                total_processed INTEGER,
                total_succeeded INTEGER,
                total_failed INTEGER
            )
        """)
        # Tabela para o status de cada arquivo/objeto
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                pc_name TEXT,
                s3_key TEXT,
                local_path_final TEXT, -- Caminho onde foi salvo (pode ser renomeado)
                status TEXT, -- DOWNLOADED, FAILED, RENAMED
                error_detail TEXT,
                FOREIGN KEY (run_id) REFERENCES run_log (run_id)
            )
        """)
        self.conn.commit()

    def _get_next_run_id(self):
        # Run ID é único globalmente
        self.cursor.execute("SELECT MAX(run_id) FROM run_log")
        max_id = self.cursor.fetchone()[0]
        return (max_id or 0) + 1

    def log_object_status(self, s3_key: str, status: str, local_path: Optional[Path] = None, error_detail: Optional[str] = None):
        # Limpar error_detail e path de caracteres inválidos antes de salvar no DB
        safe_error_detail = str(error_detail).encode('utf-8', 'ignore').decode('utf-8') if error_detail else None
        safe_local_path = str(local_path).encode('utf-8', 'ignore').decode('utf-8') if local_path else None
        
        self.cursor.execute("""
            INSERT INTO file_status 
            (run_id, pc_name, s3_key, local_path_final, status, error_detail)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            self.run_id, self.pc_name, s3_key, 
            safe_local_path, status, safe_error_detail
        ))
        self.conn.commit()

    def finalize_run(self, counts: dict):
        self.timestamp_end = datetime.now()
        self.cursor.execute("""
            INSERT INTO run_log (
                run_id, pc_name, collection_name, action, timestamp_start, timestamp_end, 
                total_processed, total_succeeded, total_failed
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            self.run_id, self.pc_name, self.collection, self.action,
            self.timestamp_start.isoformat(), self.timestamp_end.isoformat(),
            counts['processed'], counts['downloaded'], counts['failed']
        ))
        self.conn.commit()

    def get_run_metrics(self):
        self.cursor.execute("""
            SELECT s3_key, local_path_final, status, error_detail 
            FROM file_status 
            WHERE run_id = ? AND status != 'DOWNLOADED'
        """, (self.run_id,))
        non_succeeded_files = self.cursor.fetchall()

        self.cursor.execute("""
            SELECT status, COUNT(*) FROM file_status WHERE run_id = ? GROUP BY status
        """, (self.run_id,))
        summary_counts = dict(self.cursor.fetchall())
        
        return {
            'processed': summary_counts.get('DOWNLOADED', 0) + summary_counts.get('FAILED', 0) + summary_counts.get('RENAMED', 0),
            'downloaded': summary_counts.get('DOWNLOADED', 0),
            'failed': summary_counts.get('FAILED', 0),
            'renamed': summary_counts.get('RENAMED', 0),
            'non_succeeded_files': non_succeeded_files
        }

    def close(self):
        self.conn.close()


# ==============================================================================
# 3. MOTOR DE DOWNLOAD E RECONSTRUÇÃO (V7.0 - COM LOGGING)
# ==============================================================================

class HashStorageDownloader:
    
    def __init__(self, crypto_manager, collection_name):
        self.crypto = crypto_manager
        self.collection = collection_name
        self.s3 = boto3.client(
            's3', endpoint_url=DATA_S3_ENDPOINT,
            aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
        )
        self.bucket = None
        self.executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_THREADS) 
        
        self.pc_name = socket.gethostname().lower().replace(' ', '-')
        
        # Inicializa o DB Log
        self.log_db = LogControlDB(self.pc_name, self.collection, action='DOWNLOAD')
        console.print(f"[info]📊 Log Control DB ID: {self.log_db.run_id} | Arquivo: {LOG_DB_PATH.name}[/info]")
        
        self.metrics = {'downloaded': 0, 'failed': 0, 'processed': 0, 'renamed': 0}

    def list_buckets(self):
        try:
            resp = self.s3.list_buckets()
            # Filtramos aqui pelo nome da coleção selecionada ou por todos se a coleção não foi definida
            if self.collection:
                return [b['Name'] for b in resp.get('Buckets', []) if b['Name'] == f"DATA-hash-storage-{self.collection.lower()}"]
            else:
                 return [b['Name'] for b in resp.get('Buckets', []) if b['Name'].startswith('DATA-hash-storage-')]
        except Exception as e:
            safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
            console.print(f"[error]Erro ao listar buckets: {safe_error}[/error]")
            return []

    def select_folder_gui(self):
        try:
            root = tk.Tk()
            root.withdraw()
            root.attributes('-topmost', True)
            folder = filedialog.askdirectory(title="Onde salvar os arquivos restaurados (destino raiz)?")
            root.destroy()
            return folder
        except:
            return Prompt.ask("Digite o caminho da pasta de destino manualmente")

    def download_manager(self, items_to_download: list, dest_root: str):
        self.metrics['processed'] = len(items_to_download)
        console.print(f"\n[bold yellow]--- INICIANDO OPERAÇÃO DE DOWNLOAD/DECRYPT ({MAX_RETRIES} Retries) ---[/bold yellow]")
        
        total_size = sum(item['Size'] for item in items_to_download)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            
            download_task = progress.add_task("[blue]FASE 1: Download e Reconstrução", total=total_size)
            
            futures = []
            for item in items_to_download:
                futures.append(
                    self.executor.submit(self._download_single_file_stream, item, dest_root, progress, download_task)
                )
            
            for f in futures:
                try:
                    f.result()
                except Exception:
                    pass
            
            progress.update(download_task, description="[success]Download e Reconstrução concluídos.[/success]")

        # FASE FINAL: Logs e Relatório
        final_metrics = self.log_db.get_run_metrics()
        self.log_db.finalize_run(final_metrics)
        
        self._generate_markdown_log(final_metrics)
        
        final_panel_content = (
            f"[bold green]OPERAÇÃO FINALIZADA! - RUN ID: {self.log_db.run_id}[/bold green]\n\n"
            f"📦 Coleção: [cyan]{self.bucket}[/cyan]\n"
            f"✅ Objetos Restaurados: [success]{final_metrics['downloaded']}[/success]\n"
            f"⚠️ Objetos Renomeados (Conflito): [renamed]{final_metrics['renamed']}[/renamed]\n"
            f"❌ Falhas Fatais (Retry Esgotado): [error]{final_metrics['failed']}[/error]\n\n"
            "Detalhes completos e lista de erros/conflitos salvos em arquivo .md."
        )
        console.print(Panel(final_panel_content, title="[bold blue]Conclusão do Processo[/bold blue]"))


    def _handle_path_conflict(self, initial_path: Path, s3_key: str) -> Path:
        """Resolve o conflito de caminho, logando se houver renomeação."""
        final_path = initial_path
        
        if final_path.exists():
            counter = 1
            original_stem = final_path.stem
            original_suffix = final_path.suffix
            parent = final_path.parent
            
            while final_path.exists():
                final_path = parent / f"{original_stem} ({counter}){original_suffix}"
                counter += 1
            
            # Log da Renomeação
            self.log_db.log_object_status(
                s3_key=s3_key,
                status='RENAMED',
                local_path=final_path,
                error_detail=f"CONFLITO: Renomeado de {initial_path.name} para {final_path.name}"
            )
            self.metrics['renamed'] += 1

            console.print(f"[renamed]  -> CONFLITO DETECTADO. Renomeando para: [path]{final_path.name}[/path][/renamed]")
            
        return final_path
        
    def _download_single_file_stream(self, item, dest_root, progress, global_task_id):
        s3_key_unique = item['Key']
        total_s3_size = item['Size']
        
        original_path_rel = None
        bytes_read_from_s3 = 0
        temp_final_path = None
        
        try:
            # LOOP DE RETRY
            for attempt in range(1, MAX_RETRIES + 1):
                bytes_read_from_s3 = 0
                
                try:
                    # 1. TENTATIVA DE DOWNLOAD
                    response = self.s3.get_object(Bucket=self.bucket, Key=s3_key_unique)
                    stream = response['Body']
                    
                    if attempt > 1:
                        console.print(f"[retry]Retry {attempt}/{MAX_RETRIES} para S3 Key {s3_key_unique[:10]}...[/retry]")

                    # 2. PROCESSAR CABEÇALHO E METADATA
                    iv = stream.read(16)
                    if len(iv) < 16: raise IOError("Arquivo S3 muito pequeno/corrompido (sem IV).")
                        
                    cipher = Cipher(algorithms.AES(self.crypto.master_key), modes.CTR(iv), backend=default_backend())
                    decryptor = cipher.decryptor()
                    
                    meta_len_bytes = stream.read(4)
                    if len(meta_len_bytes) < 4: raise IOError("Arquivo S3 corrompido (tamanho metadata ausente).")
                    meta_len = int.from_bytes(meta_len_bytes, byteorder='big')
                    
                    encrypted_metadata = stream.read(meta_len)
                    decrypted_metadata_chunk = decryptor.update(encrypted_metadata)
                    
                    metadata_json = json.loads(decrypted_metadata_chunk.decode('utf-8'))
                    original_path_rel = metadata_json['path'] 
                    
                    # 3. CRIAR CAMINHO LOCAL (COM SANITIZAÇÃO E TRATAMENTO DE CONFLITO)
                    safe_original_path_rel = original_path_rel.encode('utf-8', 'ignore').decode('utf-8')
                    initial_path = Path(os.path.join(dest_root, safe_original_path_rel.replace('/', os.sep)))
                    
                    # Trata o conflito e LOGA a renomeação se ocorrer
                    final_path = self._handle_path_conflict(initial_path, s3_key_unique)
                    temp_final_path = final_path 
                    
                    os.makedirs(final_path.parent, exist_ok=True)
                    
                    console.print(f"[info]Iniciando: {s3_key_unique[:8]}... -> [path]{final_path.name}[/path][/info]")
                    
                    bytes_read_from_s3 = 16 + 4 + meta_len
                    
                    # 4. DOWNLOAD E DECRYPTION
                    with open(final_path, 'wb') as f_out:
                        while True:
                            chunk = stream.read(CHUNK_SIZE)
                            if not chunk: break
                                
                            decrypted_chunk = decryptor.update(chunk)
                            f_out.write(decrypted_chunk)
                            
                            progress.update(global_task_id, advance=len(chunk)) 
                            bytes_read_from_s3 += len(chunk)

                    decryptor.finalize() 
                    
                    # Garantir que a barra de progresso esteja completa
                    if bytes_read_from_s3 < total_s3_size:
                         progress.update(global_task_id, advance=total_s3_size - bytes_read_from_s3)
                         
                    console.print(f"[success]  -> RECONSTRUÍDO OK: {final_path.name}[/success]")
                    
                    # LOG DE SUCESSO
                    self.log_db.log_object_status(s3_key=s3_key_unique, status='DOWNLOADED', local_path=final_path)
                    self.metrics['downloaded'] += 1
                    return 

                # Captura de erros transientess (rede, timeout, S3)
                except (ClientError, ConnectionError, ReadTimeoutError) as e:
                    if temp_final_path and temp_final_path.exists():
                        try: temp_final_path.unlink()
                        except: pass
                        
                    if attempt < MAX_RETRIES:
                        safe_error_type = type(e).__name__
                        console.print(f"[warning]Tentativa {attempt} falhou para {s3_key_unique[:10]}... (Erro S3/Rede: {safe_error_type}). Esperando {RETRY_DELAY}s...[/warning]")
                        
                        if bytes_read_from_s3 > 0:
                            progress.update(global_task_id, advance=total_s3_size - bytes_read_from_s3) 
                            
                        time.sleep(RETRY_DELAY)
                        continue 

                    else:
                        raise Exception(f"Todas as tentativas de download falharam (S3/Rede). Último erro: {type(e).__name__}")

                # Captura erros internos/criptográficos
                except Exception as e:
                    raise Exception(f"Erro interno/criptográfico: {type(e).__name__}: {e}")

        except Exception as e:
            # Erro FATAL (após retries ou erro interno)
            path_display = original_path_rel if original_path_rel else s3_key_unique
            safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
            safe_path_display = str(path_display).encode('utf-8', 'ignore').decode('utf-8')
            
            console.print(f"\n[error]❌ ERRO CRÍTICO NO PROCESSAMENTO de {safe_path_display}: {type(e).__name__}: {safe_error}[/error]")
            
            # Tenta garantir que o arquivo incompleto é removido
            if temp_final_path and temp_final_path.exists(): 
                try: temp_final_path.unlink()
                except: pass
            
            # Compensar progresso
            if bytes_read_from_s3 < total_s3_size:
                progress.update(global_task_id, advance=total_s3_size - bytes_read_from_s3)
                
            # LOG DE FALHA
            self.log_db.log_object_status(s3_key=s3_key_unique, status='FAILED', error_detail=f"FATAL_DOWNLOAD: {safe_error}")
            self.metrics['failed'] += 1
            return False


    def navigate_and_download(self):
        # NOTA: O bucket será o nome da coleção definido na Main
        
        bucket_name = f"DATA-hash-storage-{self.collection.lower()}"
        self.bucket = bucket_name
        
        # 1. Verifica se o bucket existe
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                console.print(f"[error]❌ O Bucket '{self.bucket}' não existe ou você não tem acesso.[/error]")
                return
            raise # Lança outros erros S3/conexão

        # 2. Seleção de Pasta
        dest = self.select_folder_gui()
        if not dest: 
            console.print("[error]Nenhum diretório de destino selecionado. Encerrando.[/error]")
            return
            
        console.print(f"[info]Pasta de Destino: [bold yellow]{dest}[/bold yellow][/info]")

        console.print("[bold yellow]Listando todos os objetos únicos no bucket...[/bold yellow]")
        
        all_objs = self._get_all_objects_recursive("")
        
        if not all_objs:
            console.print("[warning]O bucket está vazio. Nada para restaurar.[/warning]")
            return

        console.print(f"[info]Total de objetos S3 encontrados: {len(all_objs)}[/info]")
        
        # 3. Inicia o Manager (que contém a lógica de logs finais)
        self.download_manager(all_objs, dest)
        
    def _generate_markdown_log(self, metrics: dict):
        """Gera um arquivo de log formatado em Markdown para o Download."""
        
        timestamp_log = self.log_db.timestamp_start.strftime("%Y%m%d%H%M%S")
        
        unique_hash = hashlib.sha1(f"{self.pc_name}{self.collection}{timestamp_log}".encode()).hexdigest()[:8]
        
        # Nome do arquivo MD inclui "DOWNLOAD"
        log_filename = f"DOWNLOAD.{self.pc_name}.{self.collection}.{timestamp_log}.{unique_hash}.md"
        log_path = LOG_DIR / log_filename
        
        md_content = f"""
# ⬇️ Relatório de Execução DATA DOWNLOADER - RUN ID: {self.log_db.run_id}

## 🗓️ Detalhes da Execução
| Parâmetro | Valor |
| :--- | :--- |
| **PC de Execução** | `{self.pc_name}` |
| **Coleção/Bucket Fonte** | `{self.collection}` (`{self.bucket}`) |
| **Objetos S3 Processados** | `{metrics['processed']}` |
| **Início da Execução** | `{self.log_db.timestamp_start.strftime('%Y-%m-%d %H:%M:%S')}` |
| **Fim da Execução** | `{self.log_db.timestamp_end.strftime('%Y-%m-%d %H:%M:%S')}` |

## 📊 Resumo do Processamento (KPIs)
| Status | Contagem |
| :--- | :--- |
| **✅ DOWNLOADED (Restaurados com Sucesso)** | `{metrics['downloaded']}` |
| **⚠️ RENAMED (Conflito de Arquivo)** | `{metrics['renamed']}` |
| **❌ FAILED (Falhas Fatais)** | `{metrics['failed']}` |

---

## 🛑 Detalhes de Falhas e Conflitos

{'Não houve falhas ou conflitos de arquivo. Todos os objetos foram restaurados com sucesso.' if not metrics['non_succeeded_files'] else ''}

| Status | Objeto S3 (Hash Key) | Caminho Local Final | Detalhe do Erro/Conflito |
| :--- | :--- | :--- | :--- |
"""

        for s3_key, local_path_final, status, error_detail in metrics['non_succeeded_files']:
            icon = '❌' if status == 'FAILED' else '⚠️'
            md_content += f"| {icon} **{status}** | `{s3_key[:20]}...` | `{local_path_final or 'N/A'}` | `{error_detail or 'N/A'}` |\n"

        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(md_content)
            console.print(f"[info]📄 Log Markdown salvo em: [path]{log_path}[/path][/info]")
        except Exception as e:
            console.print(f"[error]❌ ERRO FATAL ao salvar log MD: {e}[/error]")


    def _get_all_objects_recursive(self, prefix):
        """Lista todos os objetos (hashes + nonces) no bucket."""
        results = []
        paginator = self.s3.get_paginator('list_objects_v2')
        try:
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        results.append(obj)
        except Exception as e:
            safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
            console.print(f"[error]Erro ao listar objetos S3: {safe_error}[/error]")
        return results

# ==============================================================================
# MAIN
# ==============================================================================

if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    console.print(Panel.fit("[bold white on green] DATA CRYPTO HASH DOWNLOADER (V7.0 - LOG CONTROL INTEGRADO) [/bold white on green]", border_style="green"))

    crypto = CryptoManager()
    engine = None
    
    # 1. Autenticação
    while True:
        pwd = Prompt.ask("🔑 Digite a Senha Mestra", password=True)
        if not pwd.strip():
            console.print("[error]A senha não pode ser vazia.[/error]")
            continue
            
        if crypto.unlock_master_key(pwd):
            console.print("[success]🔓 Acesso concedido. Chave mestra pronta.[/success]")
            break
        
        if not KEY_FILE.exists():
            # Sai com ENTER se a chave não existe
            console.print("\n[bold magenta]*** FIM DA SESSÃO ***[/bold magenta]")
            input("Pressione ENTER para fechar...")
            sys.exit(1)
            
    # 2. Seleção do Bucket
    # Nota: Precisamos pedir o nome da Coleção aqui, pois o LogControlDB depende dela.
    
    collection_name = Prompt.ask("📂 Nome da Coleção/Bucket (ex: Backup-Trabalho)", default="Storage-Principal")
    collection_name = "".join(c for c in collection_name if c.isalnum() or c in ('-', '_')).strip()
    if not collection_name: 
        collection_name = "Storage"
        
    # 3. Execução do Motor
    try:
        engine = HashStorageDownloader(crypto, collection_name)
        engine.navigate_and_download()

    except Exception as e:
        console.print(Panel(f"[bold red]ERRO CRÍTICO NA EXECUÇÃO: [/bold red]{e}", title="Falha Inesperada", border_style="red"))
        
    finally:
        # Garante o fechamento do DB
        if engine and hasattr(engine, 'log_db'):
            try:
                engine.log_db.close()
            except Exception:
                pass 
    
    # 4. Finalização (NUNCA FECHAR SEM PEDIR ENTER)
    console.print("\n[bold magenta]*** FIM DA SESSÃO ***[/bold magenta]")
    input("Pressione ENTER para fechar...")
