import os
import sys
import json
import base64
import socket
import threading
import boto3
from botocore.exceptions import ClientError, ConnectionError, ReadTimeoutError
import shutil
import humanize
import tkinter as tk
from tkinter import filedialog
from pathlib import Path
import io 
import hashlib
import time
import queue 
from concurrent.futures import ThreadPoolExecutor, as_completed 
from typing import Optional, Tuple, Dict, Any
import sqlite3
from datetime import datetime

# Criptografia
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.exceptions import InvalidTag 

# UI Console
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TransferSpeedColumn
from rich.prompt import Prompt
from rich.panel import Panel
from rich.theme import Theme

# ==============================================================================
# CONFIGURAÇÕES E CONSTANTES GLOBAIS
# ==============================================================================

DATA_S3_ENDPOINT = "XXXX"
ACCESS_KEY = "XXX"
SECRET_KEY = "XXX"

# CONFIGURAÇÃO DE THREADS (OTIMIZADO PARA HDD SATA E REDE 1GBPS)
SCAN_WORKERS = 4  # Produtores: Maximize a leitura sequencial do HDD.
UPLOAD_WORKERS = 20 # Consumidores: Suficiente para preencher a rede de 100 MB/s.
MAX_THREADS = SCAN_WORKERS + UPLOAD_WORKERS # Total workers

CHUNK_SIZE = 128 * 2048 
NONCE_SIZE = 8 
MAX_RETRIES = 10 
RETRY_DELAY = 20 

SCRIPT_DIR = Path(__file__).resolve().parent
KEY_FILE = SCRIPT_DIR / "masterkey.storagekey"

FORCED_IGNORE_FILENAMES = {
    'desktop.ini', 'thumbs.db', 'autorun.inf', 'pagefile.sys', 'hiberfil.sys',
    'ntuser.dat', 'iconcache.db', '$recycle.bin', 'system volume information',
    'swapfile.sys', 'recovery', 'config.msi'
}

custom_theme = Theme({
    "info": "cyan", "warning": "yellow", "error": "bold red", "success": "bold green",
    "key": "bold magenta", "path": "dim white", "retry": "yellow on black"
})
console = Console(theme=custom_theme)

# LOCAL DE ARMAZENAMENTO DOS LOGS E DB
LOG_DIR = Path("C:\\SYNC-UPLODER-CONTROL-DATA")
LOG_DB_PATH = LOG_DIR / "control_data.db"

# ==============================================================================
# 1. MOTOR DE CRIPTOGRAFIA
# ==============================================================================

class CryptoEngine:
    SALT_SIZE = 16
    IV_SIZE = 16
    KEY_SIZE = 32

    def __init__(self):
        self.master_key = None

    def _derive_key_from_password(self, password_bytes: bytes, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=32, salt=salt,
            iterations=100000, backend=default_backend()
        )
        return kdf.derive(password_bytes)

    def load_or_create_master_key(self, password: str):
        password_bytes = password.strip().encode('utf-8')
        
        if KEY_FILE.exists():
            console.print(f"[info]🔓 Arquivo de chave encontrado: {KEY_FILE.name}. Validando senha...[/info]")
            try:
                with open(KEY_FILE, 'r') as f:
                    data = json.load(f)
                
                salt = base64.b64decode(data['salt'])
                nonce = base64.b64decode(data['nonce'])
                ciphertext_with_tag = base64.b64decode(data['ciphertext'])
                
                kek = self._derive_key_from_password(password_bytes, salt)
                
                aesgcm = AESGCM(kek)
                self.master_key = aesgcm.decrypt(nonce, ciphertext_with_tag, associated_data=None)
                
                console.print("[success]✅ Senha correta. Motor de criptografia armado.[/success]")
                
            except InvalidTag:
                console.print("[error]❌ SENHA INCORRETA.[/error] A assinatura criptográfica GCM falhou.")
                sys.exit(1)
            except Exception as e:
                console.print(f"[error]❌ ERRO TÉCNICO ao carregar chave:[/error] {e}")
                sys.exit(1)
        else:
            console.print(f"[warning]🆕 Arquivo de chave não encontrado. Criando nova identidade criptográfica em {KEY_FILE}...[/warning]")
            
            self.master_key = os.urandom(self.KEY_SIZE)
            salt = os.urandom(self.SALT_SIZE)
            nonce = os.urandom(12) 
            
            kek = self._derive_key_from_password(password_bytes, salt)
            
            aesgcm = AESGCM(kek)
            ciphertext_with_tag = aesgcm.encrypt(nonce, self.master_key, associated_data=None) 
            
            data = {
                'salt': base64.b64encode(salt).decode('utf-8'),
                'nonce': base64.b64encode(nonce).decode('utf-8'),
                'ciphertext': base64.b64encode(ciphertext_with_tag).decode('utf-8'), 
                'desc': "Mantenha este arquivo seguro. Ele e necessario para descriptografar seus dados."
            }
            with open(KEY_FILE, 'w') as f:
                json.dump(data, f, indent=4)
            console.print(f"[success]✅ {KEY_FILE.name} criado com sucesso. FAÇA BACKUP DESTE ARQUIVO![/success]")


    def create_encrypted_stream(self, file_path: Path, original_s3_key: str):
        return EncryptedFileStreamV2(file_path, self.master_key, original_s3_key)

# ==============================================================================
# 2. STREAM CRIPTOGRAFADO
# ==============================================================================

class EncryptedFileStreamV2:
    """Wrapper que injeta o IV, o tamanho do metadata e o metadata criptografado (caminho original)."""
    
    def __init__(self, path: Path, key: bytes, original_path_key: str):
        self.original_path_key = original_path_key 
        metadata_json = json.dumps({"path": original_path_key}).encode('utf-8')
        self._f = open(path, 'rb')
        self.key = key
        self.iv = os.urandom(16)
        
        cipher = Cipher(algorithms.AES(key), modes.CTR(self.iv), backend=default_backend())
        self.encryptor = cipher.encryptor()
        
        encrypted_metadata = self.encryptor.update(metadata_json)
        meta_len = len(encrypted_metadata)
        meta_len_bytes = meta_len.to_bytes(4, byteorder='big')
        
        header = self.iv + meta_len_bytes + encrypted_metadata
        self._header_buffer = io.BytesIO(header)
        self._header_size = len(header)
        
        self._file_size = os.path.getsize(path)
        self.total_size = self._header_size + self._file_size
        self.len = self.total_size 
        self._current_pos = 0

    def read(self, size=-1):
        if self._header_buffer.tell() < self._header_size:
            chunk = self._header_buffer.read(size)
            if chunk:
                self._current_pos += len(chunk)
                return chunk
        
        data = self._f.read(size)
        if not data:
            try:
                final_chunk = self.encryptor.finalize()
            except Exception:
                return b''
            
            if final_chunk:
                self._current_pos += len(final_chunk)
                return final_chunk
            return b''
        
        encrypted_data = self.encryptor.update(data)
        self._current_pos += len(encrypted_data)
        return encrypted_data

    def tell(self):
        return self._current_pos 

    def seek(self, offset, whence=0):
        if offset == 0 and whence == 0:
            self._f.seek(0)
            self._header_buffer.seek(0)
            self._current_pos = 0 
            
            cipher = Cipher(algorithms.AES(self.key), modes.CTR(self.iv), backend=default_backend())
            self.encryptor = cipher.encryptor()
            
            metadata_json = json.dumps({"path": self.original_path_key}).encode('utf-8')
            self.encryptor.update(metadata_json)
            
            return self._current_pos
        elif whence == 2 and offset == 0:
            return self.total_size
        else:
            raise IOError("Stream criptografado suporta apenas seek(0) para rewind.")

    def close(self):
        try:
            self._f.close()
        except:
            pass 

# ==============================================================================
# 3A. THREAD DEDICADA PARA GRAVAÇÃO DE LOG (BATCH LOGGING)
# ==============================================================================

class LogWriterThread(threading.Thread):
    def __init__(self, log_queue, db_path, run_id, pc_name, drive_letter):
        super().__init__()
        self.log_queue = log_queue
        self.db_path = db_path
        self.run_id = run_id
        self.pc_name = pc_name
        self.drive_letter = drive_letter
        
        self._stop_event = threading.Event()
        
        self.BATCH_SIZE = 100
        self.FLUSH_INTERVAL = 10 # Segundos
        
        self.conn = None
        self.cursor = None
        
        self.last_flush_time = time.time()
        self.batch_entries = []

    def _setup_db_connection(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def _flush_batch(self):
        if not self.batch_entries:
            return
            
        try:
            # Colunas: run_id, pc_name, drive_letter, file_path_full, file_name, file_size_bytes, status, s3_key, error_detail
            sql = """
                INSERT INTO file_status 
                (run_id, pc_name, drive_letter, file_path_full, file_name, file_size_bytes, status, s3_key, error_detail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.cursor.executemany(sql, self.batch_entries)
            self.conn.commit()
            
            # Limpa o lote após o commit
            self.batch_entries = []
            self.last_flush_time = time.time()
            
        except Exception as e:
            # Se a gravação em lote falhar (ex: DB lock), tentamos gravar individualmente (fallback robusto)
            print(f"ERRO CRÍTICO NO BATCH INSERT: {e}. Tentando gravação individual...")
            
            # Fallback (Grave o que puder individualmente)
            # Nota: O erro original de thread não deve ocorrer aqui, pois esta thread possui a conexão.
            for entry in self.batch_entries:
                try:
                    self.cursor.execute(sql, entry)
                except Exception as individual_e:
                    print(f"Falha ao gravar entrada individual: {individual_e}")
            self.conn.commit()
            self.batch_entries = []


    def run(self):
        self._setup_db_connection()
        
        while not self._stop_event.is_set():
            try:
                # Tenta pegar itens da fila com timeout
                item = self.log_queue.get(timeout=1)
                
                # Se for None, é o sinal de parada (flush and exit)
                if item is None:
                    break
                
                # O item é (file_path_full, file_name, file_size_bytes, status, s3_key, error_detail)
                fp, fn, fs, status, s3_key, error_detail = item
                
                # Prepara a tupla completa para o executemany
                entry = (
                    self.run_id, self.pc_name, self.drive_letter,
                    str(fp), fn, fs, status, s3_key, error_detail
                )
                self.batch_entries.append(entry)
                
                self.log_queue.task_done()
                
                # Checa se deve fazer o flush por tamanho
                if len(self.batch_entries) >= self.BATCH_SIZE:
                    self._flush_batch()
                    
            except queue.Empty:
                # O timeout ocorreu, checa se deve fazer o flush por tempo
                if time.time() - self.last_flush_time >= self.FLUSH_INTERVAL:
                    self._flush_batch()
            except Exception as e:
                # Erro inesperado no processamento da fila, loga e continua
                print(f"Erro inesperado no LogWriterThread: {e}")
                time.sleep(1)
        
        # Processamento final após receber o sinal de parada
        print("LogWriter: Processando entradas finais...")
        while not self.log_queue.empty():
            try:
                item = self.log_queue.get(timeout=0.1)
                if item is None: continue
                
                fp, fn, fs, status, s3_key, error_detail = item
                entry = (self.run_id, self.pc_name, self.drive_letter, str(fp), fn, fs, status, s3_key, error_detail)
                self.batch_entries.append(entry)
                self.log_queue.task_done()
            except queue.Empty:
                break
            except Exception as e:
                 print(f"Erro ao processar item final: {e}")
                 break
                 
        # Flush final garantido
        self._flush_batch()
        self.conn.close()
        print("LogWriter: Conexão DB fechada.")

    def stop(self):
        # Sinaliza a thread para sair do loop e adiciona None para desbloquear o get
        self._stop_event.set()
        self.log_queue.put(None)
        self.join()


# ==============================================================================
# 3B. CLASSE DE LOG E CONTROLE EM SQLITE (INTERFACE)
# ==============================================================================

class LogControlDB:
    def __init__(self, pc_name: str, root_path: str, collection: str):
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        # A conexão principal só é usada para setup e leitura final (na thread principal)
        self.conn_main = sqlite3.connect(LOG_DB_PATH)
        self.cursor_main = self.conn_main.cursor()
        
        self.pc_name = pc_name
        self.root_path = root_path
        self.drive_letter = Path(root_path).parts[0].replace('\\', '').replace(':', '')
        self.collection = collection
        
        self._setup_db() 
        
        self.run_id = self._get_next_run_id()
        self.timestamp_start = datetime.now()

        # Configuração do Logging Assíncrono
        self.log_queue = queue.Queue()
        self.log_writer_thread = LogWriterThread(
            self.log_queue, LOG_DB_PATH, self.run_id, self.pc_name, self.drive_letter
        )

    def start_writer(self):
        self.log_writer_thread.start()
        
    def stop_writer(self):
        self.log_writer_thread.stop()


    def _setup_db(self):
        # Tabela para registrar cada execução
        self.cursor_main.execute("""
            CREATE TABLE IF NOT EXISTS run_log (
                run_id INTEGER PRIMARY KEY,
                pc_name TEXT,
                collection_name TEXT,
                root_path TEXT,
                drive_letter TEXT,
                timestamp_start TEXT,
                timestamp_end TEXT,
                total_uploaded INTEGER,
                total_ignored INTEGER,
                total_failed INTEGER
            )
        """)
        # Tabela para o status de cada arquivo
        self.cursor_main.execute("""
            CREATE TABLE IF NOT EXISTS file_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                pc_name TEXT,
                drive_letter TEXT,
                file_path_full TEXT,
                file_name TEXT,
                file_size_bytes INTEGER,
                status TEXT, -- UPLOADED, IGNORED, FAILED
                s3_key TEXT,
                error_detail TEXT,
                FOREIGN KEY (run_id) REFERENCES run_log (run_id)
            )
        """)
        self.conn_main.commit()

    def _get_next_run_id(self):
        self.cursor_main.execute("""
            SELECT MAX(run_id) FROM run_log WHERE pc_name = ? AND root_path = ? AND collection_name = ?
        """, (self.pc_name, self.root_path, self.collection))
        max_id = self.cursor_main.fetchone()[0]
        return (max_id or 0) + 1

    def log_file_status(self, file_path: Path, status: str, file_size: int, s3_key: Optional[str] = None, error_detail: Optional[str] = None):
        """MÉTODO USADO PELOS WORKERS: Envia dados para a fila, não grava diretamente."""
        # Sanitiza o erro e o s3_key
        safe_error_detail = str(error_detail).encode('utf-8', 'ignore').decode('utf-8') if error_detail else None
        safe_s3_key = s3_key if s3_key else None
        
        log_entry = (
            file_path, file_path.name, file_size, 
            status, safe_s3_key, safe_error_detail
        )
        self.log_queue.put(log_entry)

    def finalize_run(self, counts: dict):
        """Executado na thread principal após todos os workers terminarem."""
        
        # 1. Espera o LogWriterThread terminar e garantir que todos os logs foram commitados.
        self.stop_writer()
        
        # 2. Insere o registro final da corrida (usando a conexão principal)
        self.timestamp_end = datetime.now()
        self.cursor_main.execute("""
            INSERT INTO run_log (
                run_id, pc_name, collection_name, root_path, drive_letter, timestamp_start, timestamp_end, 
                total_uploaded, total_ignored, total_failed
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            self.run_id, self.pc_name, self.collection, self.root_path, self.drive_letter,
            self.timestamp_start.isoformat(), self.timestamp_end.isoformat(),
            counts['uploaded'], counts['ignored'], counts['failed']
        ))
        self.conn_main.commit()

    def get_run_metrics(self):
        """Lê os resultados da corrida atual usando a conexão principal (Thread Segura)."""
        self.cursor_main.execute("""
            SELECT file_path_full, file_name, status, error_detail 
            FROM file_status 
            WHERE run_id = ? AND status != 'UPLOADED'
        """, (self.run_id,))
        non_uploaded_files = self.cursor_main.fetchall()

        self.cursor_main.execute("""
            SELECT status, COUNT(*) FROM file_status WHERE run_id = ? GROUP BY status
        """, (self.run_id,))
        summary_counts = dict(self.cursor_main.fetchall())
        
        return {
            'uploaded': summary_counts.get('UPLOADED', 0),
            'ignored': summary_counts.get('IGNORED', 0),
            'failed': summary_counts.get('FAILED', 0),
            'non_uploaded_files': non_uploaded_files
        }

    def close(self):
        """Fecha a conexão principal."""
        try:
            self.conn_main.close()
        except:
            pass

# ==============================================================================
# 4. UPLOAD ENGINE V9.0 (OTIMIZAÇÃO: 2 SCANNERS, 6 UPLOADERS)
# ==============================================================================

# Definição do tipo de dado que sai do Scanner e entra na Queue
HashedFileMetadata = Tuple[Path, int, str, str] # (fp, file_size, s3_key, metadata_path)

class UploadEngine:

    def __init__(self, collection_name, root_path, password):
        self.root_path = Path(root_path)
        self.collection = collection_name
        self.crypto = CryptoEngine()
        
        self.s3 = boto3.client(
            's3', endpoint_url=DATA_S3_ENDPOINT,
            aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
        )
        
        self.pc_name = socket.gethostname().lower().replace(' ', '-')
        self.bucket_name = f"DATA-hash-storage-{collection_name.lower()}" 
        
        self.crypto.load_or_create_master_key(password)
        self.clean_bucket_name = self.bucket_name.replace('.', '-')
        
        # Inicializa o DB Log
        self.log_db = LogControlDB(self.pc_name, root_path, self.collection)
        self.log_db.start_writer() # INICIA A THREAD DE GRAVAÇÃO DE LOG
        console.print(f"[info]📊 Log Control DB ID: {self.log_db.run_id} | Arquivo: {LOG_DB_PATH.name}[/info]")
        
        self._ensure_bucket()
        
        self.metrics = {'uploaded': 0, 'ignored': 0, 'failed': 0}
        
        # Fila Produtor-Consumidor (Hashing -> Upload)
        self.upload_queue = queue.Queue()


    def _safe_print(self, text, style=None):
        """Wrapper para lidar com UnicodeEncodeError no console (Windows)"""
        try:
            if style:
                console.print(f"[{style}]{text}[/{style}]")
            else:
                console.print(text)
        except UnicodeEncodeError:
            # Tenta sanitizar o texto removendo caracteres problemáticos ou substituindo
            sanitized_text = text.encode('ascii', 'replace').decode('ascii').replace('?', '*')
            if style:
                 console.print(f"[{style}]**** ERRO UNICODE ****: {sanitized_text}[/{style}]")
            else:
                 console.print(f"[bold red]**** ERRO UNICODE ****: {sanitized_text} [/bold red]")
        except Exception:
            console.print("[bold red]**** ERRO GERAL DE CONSOLE ****[/bold red]")


    def _ensure_bucket(self):
        try:
            self.s3.head_bucket(Bucket=self.clean_bucket_name)
        except:
            try:
                self.s3.create_bucket(Bucket=self.clean_bucket_name)
                self._safe_print(f"📦 Bucket {self.clean_bucket_name} criado.", style="success")
            except Exception as e:
                self._safe_print(f"Erro fatal criando bucket: {e}", style="error")
                self.log_db.close()
                sys.exit(1)

    def _calculate_content_hash(self, file_path: Path):
        """Calcula o SHA-512 do conteúdo do arquivo."""
        hasher = hashlib.sha512()
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(CHUNK_SIZE * 4) 
                    if not chunk:
                        break
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            return None

    def _get_original_path_metadata(self, local_path: Path):
        """Retorna o caminho relativo que sera armazenado no metadata."""
        try:
            rel_path = local_path.relative_to(self.root_path).as_posix()
        except ValueError:
            rel_path = local_path.name.as_posix()
            
        return rel_path 

    def _create_s3_key(self, content_hash: str):
        """Cria uma chave S3 única: [ContentHash]-[NonceBase64]"""
        nonce = os.urandom(NONCE_SIZE)
        nonce_b64 = base64.urlsafe_b64encode(nonce).decode('utf-8').rstrip('=')
        return f"{content_hash}-{nonce_b64}"

    def _scan_and_hash_worker(self, fp: Path, scanner_progress, task_id):
        """Worker thread para calcular hash, metadados, tamanho criptografado, e enfileirar o resultado."""
        file_size = 0
        
        try:
            file_size = fp.stat().st_size
            
            if file_size == 0:
                self._safe_print(f"[VAZIO] Ignorando {fp.name}: Arquivo vazio.", style="warning")
                self.log_db.log_file_status(fp, 'IGNORED', 0, error_detail="ZERO_BYTE_SIZE")
                self.metrics['ignored'] += 1
                return 0
            
            content_hash = self._calculate_content_hash(fp)
            if not content_hash: 
                safe_error = f"HASH_CALC_ERROR"
                self._safe_print(f"[ALERTA] Ignorando {fp.name}: Falha no cálculo do hash.", style="warning")
                self.log_db.log_file_status(fp, 'IGNORED', file_size, error_detail=safe_error)
                self.metrics['ignored'] += 1
                return 0
            
            s3_key_unique = self._create_s3_key(content_hash)
            original_metadata_path = self._get_original_path_metadata(fp)
            
            temp_stream = EncryptedFileStreamV2(fp, self.crypto.master_key, original_metadata_path)
            encrypted_size = temp_stream.total_size
            temp_stream.close()

            # Enfileira para o Uploader (Produtor)
            self.upload_queue.put((fp, file_size, s3_key_unique, original_metadata_path))
            
            scanner_progress.update(task_id, description=f"[green]Hash OK:[/green] {fp.name}")
            return encrypted_size
            
        except Exception as e:
            safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
            self._safe_print(f"[ERRO PROCESSO] Ignorando {fp.name}: {type(e).__name__}: {safe_error}", style="warning")
            
            self.log_db.log_file_status(fp, 'IGNORED', file_size, error_detail=f"PROCESSING_ERROR: {type(e).__name__}: {safe_error}")
            self.metrics['ignored'] += 1
            return 0


    def _upload_worker(self, progress, overall_task):
        """Worker thread que consome a fila e realiza o upload."""
        
        while True:
            try:
                item = self.upload_queue.get(timeout=1)
            except queue.Empty:
                continue

            if item is None:
                self.upload_queue.put(None) # Repõe o sentinel
                self._safe_print(f"💤 Uploader {threading.get_ident()} finalizado por sentinel.", style="info")
                break
                
            file_path, file_size, s3_hash_key, original_metadata_path = item
            
            def progress_callback(bytes_amount):
                progress.update(overall_task, advance=bytes_amount)
                
            enc_stream = None
            uploaded_successfully = False
            
            try:
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        enc_stream = self.crypto.create_encrypted_stream(file_path, original_metadata_path)
                        
                        if attempt > 1:
                            enc_stream.seek(0)
                            self._safe_print(f"Retry {attempt}/{MAX_RETRIES} para {file_path.name}...", style="retry")

                        self.s3.upload_fileobj(
                            Fileobj=enc_stream,
                            Bucket=self.clean_bucket_name,
                            Key=s3_hash_key, 
                            Callback=progress_callback,
                        )
                        
                        self._safe_print(f"✅ Upload CONCLUÍDO ({attempt} tentativas). Key: {s3_hash_key[:16]}... | File: {file_path.name}", style="success")
                        
                        self.log_db.log_file_status(file_path, 'UPLOADED', file_size, s3_key=s3_hash_key)
                        self.metrics['uploaded'] += 1
                        uploaded_successfully = True
                        break 
                        
                    except (ClientError, ConnectionError, ReadTimeoutError) as e:
                        if attempt < MAX_RETRIES:
                            safe_error_type = type(e).__name__
                            self._safe_print(f"Tentativa {attempt} falhou para {file_path.name} (Erro S3/Rede: {safe_error_type}). Esperando {RETRY_DELAY}s...", style="warning")
                            time.sleep(RETRY_DELAY)
                        else:
                            raise 
                    
                    finally:
                        if enc_stream:
                            enc_stream.close()
                
                if not uploaded_successfully:
                    raise Exception("Todas as tentativas de upload falharam devido a erros S3/Rede.")
                    
            except Exception as e:
                safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
                self._safe_print(f"❌ FALHA FATAL: {file_path.name} (Key {s3_hash_key[:10]}...): {type(e).__name__}: {safe_error}", style="error")
                
                # O log DB pode não ter o tamanho correto em caso de falha I/O, usa 0
                self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_hash_key, error_detail=f"FATAL_UPLOAD: {type(e).__name__}: {safe_error}")
                self.metrics['failed'] += 1
                
                # Avança o progress bar para compensar o arquivo não enviado
                try:
                    temp_stream = EncryptedFileStreamV2(file_path, self.crypto.master_key, original_metadata_path)
                    progress.update(overall_task, advance=temp_stream.total_size)
                    temp_stream.close()
                except Exception:
                    progress.update(overall_task, advance=CHUNK_SIZE) 
            
            finally:
                self.upload_queue.task_done()

    def run(self):
        total_bytes_encrypted = 0
        scanner_futures = []
        upload_futures = []
        
        self._safe_print(f"[bold]Varrendo diretório:[/bold] {self.root_path}", style=None)
        
        # Threads Otimizadas para HDD SATA e 1GBPS
        scanner_executor = ThreadPoolExecutor(max_workers=SCAN_WORKERS)
        uploader_executor = ThreadPoolExecutor(max_workers=UPLOAD_WORKERS)
        
        try:
            with Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TransferSpeedColumn(),
                TimeElapsedColumn(),
                console=console
            ) as progress:
                
                # O progress bar de scanning não tem total, pois o número de arquivos válidos é desconhecido
                scan_task = progress.add_task(f"[yellow]Fase 1: Varredura e Hashing ({SCAN_WORKERS} Workers)...", total=None)
                overall_task = progress.add_task(f"[green]Fase 2: Uploads Ativos ({UPLOAD_WORKERS} Workers)...", total=None)

                # 1. INICIA UPLOADERS (CONSUMIDORES)
                for i in range(UPLOAD_WORKERS):
                    upload_futures.append(
                        uploader_executor.submit(self._upload_worker, progress, overall_task)
                    )

                # 2. INICIA SCANNERS (PRODUTORES)
                
                files_scanned_count = 0
                for root, _, files in os.walk(self.root_path):
                    for f in files:
                        fp = Path(root) / f
                        files_scanned_count += 1
                        
                        progress.update(scan_task, description=f"[yellow]Fase 1: Varredura e Hashing - {files_scanned_count} arquivos[/yellow]")
                        
                        if f.lower() in FORCED_IGNORE_FILENAMES or fp.name == KEY_FILE.name:
                            self.log_db.log_file_status(fp, 'IGNORED', 0, error_detail="SYSTEM_FILE")
                            self.metrics['ignored'] += 1
                            continue
                            
                        scanner_futures.append(
                            scanner_executor.submit(self._scan_and_hash_worker, fp, progress, scan_task)
                        )

                # 3. AGUARDA E COLETA TAMANHO DO HASHING
                
                for future in as_completed(scanner_futures):
                    try:
                        encrypted_size = future.result()
                        total_bytes_encrypted += encrypted_size
                    except Exception:
                        pass 

                scanner_executor.shutdown()
                
                progress.update(scan_task, completed=True, description="[bold green]Fase 1 CONCLUÍDA[/bold green]")
                
                # Define o total do Progress Bar principal
                progress.update(overall_task, total=total_bytes_encrypted)
                self._safe_print(f"[info]Hashing Completo. Tamanho Total Criptografado: {humanize.naturalsize(total_bytes_encrypted)}[/info]", style=None)

                # 4. SINALIZAÇÃO DE FIM (POISON PILL)
                
                for _ in range(UPLOAD_WORKERS):
                    self.upload_queue.put(None)
                
                uploader_executor.shutdown() 

            # FASE 5: FINALIZAÇÃO E LOGS
            self.log_db.finalize_run(self.metrics)
            final_metrics = self.log_db.get_run_metrics()
            
            final_panel_content = (
                f"[bold green]OPERAÇÃO FINALIZADA! - RUN ID: {self.log_db.run_id}[/bold green]\n\n"
                f"📦 Coleção: [cyan]{self.collection}[/cyan] (Bucket: {self.clean_bucket_name})\n"
                f"✅ Uploads Concluídos: [success]{final_metrics['uploaded']}[/success]\n"
                f"🚫 Arquivos Ignorados: [warning]{final_metrics['ignored']}[/warning]\n"
                f"❌ Falhas Fatais (Retry Esgotado): [error]{final_metrics['failed']}[/error]\n\n"
                "Detalhes completos e lista de erros/ignorados salvos em arquivo .md."
            )
            self._safe_print(Panel(final_panel_content, title="[bold blue]Conclusão do Processo[/bold blue]"), style=None)
            
            self._generate_markdown_log(final_metrics)
            
        except Exception as e:
            # Garante que os pools são desligados em caso de erro crítico no main thread
            scanner_executor.shutdown(wait=False)
            uploader_executor.shutdown(wait=False)
            # Re-lança o erro para ser capturado no bloco principal
            raise e
        
    def _generate_markdown_log(self, metrics: dict):
        """Gera um arquivo de log formatado em Markdown."""
        
        timestamp_log = self.log_db.timestamp_start.strftime("%Y%m%d%H%M%S")
        unique_hash = hashlib.sha1(f"{self.pc_name}{self.collection}{timestamp_log}".encode()).hexdigest()[:8]
        
        log_filename = f"{self.pc_name}.{self.log_db.drive_letter}.{self.collection}.{timestamp_log}.{unique_hash}.md"
        log_path = LOG_DIR / log_filename
        
        md_content = f"""
# 🚀 Relatório de Execução DATA UPLOADER - RUN ID: {self.log_db.run_id}

## 🗓️ Detalhes da Execução
| Parâmetro | Valor |
| :--- | :--- |
| **PC de Execução** | `{self.pc_name}` |
| **Coleção/Bucket** | `{self.collection}` (`{self.clean_bucket_name}`) |
| **Pasta Raiz Varredura** | `{self.root_path}` |
| **Letra da Unidade** | `{self.log_db.drive_letter}` |
| **Início da Execução** | `{self.log_db.timestamp_start.strftime('%Y-%m-%d %H:%M:%S')}` |
| **Fim da Execução** | `{self.log_db.timestamp_end.strftime('%Y-%m-%d %H:%M:%S')}` |

## 📊 Resumo do Processamento
| Status | Contagem |
| :--- | :--- |
| **✅ UPLOADED (Concluídos)** | `{metrics['uploaded']}` |
| **🚫 IGNORED (Ignorados na Varredura)** | `{metrics['ignored']}` |
| **❌ FAILED (Falhas Fatais de Upload)** | `{metrics['failed']}` |

---

## 🛑 Detalhes de Arquivos Não Uploadados (Ignorados e Falhas Fatais)

{'Não houve arquivos ignorados ou com falha. Todos os arquivos válidos foram uploadados.' if not metrics['non_uploaded_files'] else ''}

| Status | Nome do Arquivo | Caminho Completo | Detalhe do Erro |
| :--- | :--- | :--- | :--- |
"""

        for full_path, name, status, error_detail in metrics['non_uploaded_files']:
            icon = '🚫' if status == 'IGNORED' else '❌'
            # Sanitiza para log MD
            safe_name = name.encode('ascii', 'replace').decode('ascii').strip()
            safe_full_path = full_path.encode('ascii', 'replace').decode('ascii').strip()
            safe_error_detail = error_detail.encode('ascii', 'replace').decode('ascii').strip() if error_detail else 'N/A'
            
            md_content += f"| {icon} **{status}** | `{safe_name}` | `{safe_full_path}` | `{safe_error_detail}` |\n"

        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(md_content)
            self._safe_print(f"📄 Log Markdown salvo em: [path]{log_path}[/path]", style="info")
        except Exception as e:
            self._safe_print(f"❌ ERRO FATAL ao salvar log MD: {e}", style="error")


# ==============================================================================
# FUNÇÕES E EXECUÇÃO PRINCIPAL
# ==============================================================================

def select_folder_gui():
    try:
        root = tk.Tk()
        root.withdraw() 
        root.attributes('-topmost', True) 
        
        folder_selected = filedialog.askdirectory(title="Selecione a PASTA RAIZ para Upload Seguro")
        root.destroy()
        return folder_selected
    except Exception as e:
        console.print(f"[error]Erro ao abrir seletor de pastas: {e}[/error]")
        return None

if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    # Atualiza o título para refletir a nova versão e otimização
    console.print(Panel.fit("[bold white on blue] DATA CRYPTO HASH UPLOADER (V9.0 - THRUPUT OPTIMIZED HDD & DB FIX) [/bold white on blue]", border_style="blue"))
    
    engine = None
    
    # 1. Seleção da Pasta
    target_folder = select_folder_gui()
    
    if not target_folder:
        console.print("[error]Nenhuma pasta selecionada. Encerrando.[/error]")
        console.print("\n[bold magenta]*** FIM DA SESSÃO ***[/bold magenta]")
        input("Pressione ENTER para fechar...")
        sys.exit(0)
        
    console.print(f"📁 Pasta Alvo: [bold yellow]{target_folder}[/bold yellow]")
    
    # 2. Entrada de Dados
    collection_name = Prompt.ask("📂 Nome da Coleção/Bucket (ex: Backup-Trabalho)", default="Storage-Principal")
    collection_name = "".join(c for c in collection_name if c.isalnum() or c in ('-', '_')).strip()
    if not collection_name: collection_name = "Storage"

    password = Prompt.ask("🔑 Digite a Senha Mestra", password=False)
    
    if not password.strip():
        console.print("[error]A senha não pode ser vazia.[/error]")
        console.print("\n[bold magenta]*** FIM DA SESSÃO ***[/bold magenta]")
        input("Pressione ENTER para fechar...")
        sys.exit(1)

    # 3. Execução do Motor
    try:
        engine = UploadEngine(collection_name, target_folder, password)
        engine.run()
    except Exception as e:
        if engine and hasattr(engine, '_safe_print'):
             engine._safe_print(Panel(f"[bold red]ERRO CRÍTICO NA EXECUÇÃO: [/bold red]{type(e).__name__}: {e}", title="Falha Inesperada", border_style="red"), style=None)
        else:
             console.print(Panel(f"[bold red]ERRO CRÍTICO NA EXECUÇÃO: [/bold red]{type(e).__name__}: {e}", title="Falha Inesperada", border_style="red"))
    finally:
        # Garante o encerramento seguro da thread de log e da conexão principal
        if engine and hasattr(engine, 'log_db'):
            try:
                # O finalize_run já chama stop_writer(), mas é seguro chamá-lo novamente
                engine.log_db.stop_writer() 
            except Exception:
                pass 
            try:
                engine.log_db.close()
            except Exception:
                pass 
    
    # 4. Finalização
    console.print("\n[bold magenta]*** FIM DA SESSÃO ***[/bold magenta]")
    input("Pressione ENTER para fechar...")
