# -*- coding: utf-8 -*-
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
from typing import Optional, Tuple, Dict, Any, List, Union
import sqlite3
from datetime import datetime
from multiprocessing import Process, Manager 
from boto3.session import Config 

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
# CONFIGURAÇÕES E CONSTANTES GLOBAIS - V12.1 (ENHANCED THREAD SAFETY)
# ==============================================================================

MEGA_S3_ENDPOINT = "XXX"
ACCESS_KEY = "XXX"
SECRET_KEY = "XXX"

# CONFIGURAÇÃO DE THREADS (MANTIDO para teste e I/O balanceado)
SCAN_WORKERS = 8    
UPLOAD_WORKERS = 100 # Manter baixo para stress testar a contenção e o fix
MAX_THREADS = SCAN_WORKERS + UPLOAD_WORKERS

# TAMANHO DA PARTE MPU (MANTIDO 8MB)
MPU_PART_SIZE = 8 * 1024 * 1024 
CHUNK_SIZE = MPU_PART_SIZE 

NONCE_SIZE = 8 
MAX_RETRIES = 100 
RETRY_DELAY = 50

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
LOG_DIR = Path("C:\\MEGA-SYNC-UPLODER-CONTROL-DATA")
LOG_DB_PATH = LOG_DIR / "control_data.db"

# ==============================================================================
# 0. TIPOS E CONTROLES
# ==============================================================================
UploadPartData = Tuple[str, str, int, int, int, Path, int] # s3_key, upload_id, part_number, offset, length, file_path, file_size 


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
    
    def __init__(self, path: Path, key: bytes, original_path_key: str):
        self.original_path_key = original_path_key 
        metadata_json = json.dumps({"path": original_path_key}).encode('utf-8')
        try:
            self._f = open(path, 'rb')
        except Exception:
             self._f = None 
             
        self.key = key
        self.iv = os.urandom(16)
        
        # Gera o header apenas uma vez para calcular o tamanho
        cipher = Cipher(algorithms.AES(key), modes.CTR(self.iv), backend=default_backend())
        self.temp_encryptor = cipher.encryptor() 
        
        encrypted_metadata = self.temp_encryptor.update(metadata_json)
        meta_len = len(encrypted_metadata)
        meta_len_bytes = meta_len.to_bytes(4, byteorder='big')
        
        self.header = self.iv + meta_len_bytes + encrypted_metadata
        self.header_size = len(self.header)
        
        self.file_size_raw = os.path.getsize(path) if self._f else 0
        self.total_size = self.header_size + self.file_size_raw
        self.len = self.total_size 
        
        # Variáveis para a leitura JIT
        self.active_encryptor = None
        self.active_iv = None
        self.active_pos = 0 # Posição real do stream JIT

    def _setup_encryptor_for_seek(self, initial_offset: int):
        """Prepara o estado do CTR (Cipher) para o offset solicitado (JIT)."""
        
        if initial_offset < 0 or initial_offset > self.total_size:
            raise ValueError("Offset fora dos limites do arquivo criptografado.")
            
        # 1. Trata o header (criptografado/IV + Length)
        if initial_offset < self.header_size:
            # O offset está dentro do header.
            self.active_iv = self.iv
            self.active_pos = initial_offset
            self.active_encryptor = None 
            self._f.seek(0) # Posição do arquivo raw (nao sera lido)
            return

        # 2. O offset está na parte de dados do arquivo (após o header)
        
        # Posição no arquivo raw (começa em 0)
        raw_offset = initial_offset - self.header_size 
        
        block_size = 16 
        block_index = raw_offset // block_size
        
        # Novo IV (IV para o bloco inicial de leitura)
        new_iv = bytearray(self.iv)
        temp_block_index = block_index
        # Tentativa mais robusta de calcular o counter ajustado (assumindo 4 bytes de counter little-endian no final)
        for i in range(4):
            idx = 15 - i
            current_byte = self.iv[idx]
            increment = temp_block_index % 256
            
            # Adiciona o incremento ao byte, lidando com o carry-over
            new_val = current_byte + increment
            new_iv[idx] = new_val & 0xFF
            
            temp_block_index = (temp_block_index // 256) + (new_val // 256)
        
        self.active_iv = bytes(new_iv)
        
        # Cria o novo encryptor com o IV ajustado
        cipher = Cipher(algorithms.AES(self.key), modes.CTR(self.active_iv), backend=default_backend())
        self.active_encryptor = cipher.encryptor()
        
        # O 'seek' no arquivo real deve ser no início do bloco
        raw_seek_offset = raw_offset - (raw_offset % block_size)
        self._f.seek(raw_seek_offset)
        self.active_pos = initial_offset

        # Se o offset não for o início do bloco (padding), precisa descartar os bytes iniciais
        remaining_in_block = raw_offset % block_size
        if remaining_in_block > 0:
            discard_data = self._f.read(remaining_in_block)
            self.active_encryptor.update(discard_data)
            

    def read(self, size: int) -> bytes:
        if not self._f: return b''
        
        bytes_read = b''
        remaining_size = size

        # Loop para garantir que o tamanho solicitado seja lido, se possível
        while remaining_size > 0:
            
            if self.active_pos < self.header_size:
                # 1. Lendo o header
                remaining_header = self.header_size - self.active_pos
                read_len = min(remaining_size, remaining_header)
                
                chunk = self.header[self.active_pos : self.active_pos + read_len]
                self.active_pos += len(chunk)
                bytes_read += chunk
                remaining_size -= len(chunk)
                
                # Transição do header para os dados
                if self.active_pos == self.header_size:
                     # Prepara o encryptor para o início dos dados
                     self._setup_encryptor_for_seek(self.header_size)
                     # Se não sobrou tamanho para ler, retorna o que leu
                     if remaining_size == 0:
                         return bytes_read
                else:
                    # Leu apenas parte do header
                    return bytes_read
                
            else:
                # 2. Lendo a parte de dados (arquivo encriptado)
                
                data_to_read_raw = min(remaining_size, self.total_size - self.active_pos)
                
                if data_to_read_raw == 0:
                    # Fim do arquivo (sem dados brutos para ler)
                    if self.active_encryptor:
                        # Tenta finalizar (se houver padding)
                        try:
                            final_chunk = self.active_encryptor.finalize()
                            self.active_encryptor = None
                            if final_chunk:
                                self.active_pos += len(final_chunk)
                                return bytes_read + final_chunk
                        except Exception:
                            pass 
                    return bytes_read
                
                # Lendo os dados brutos e encriptando JIT
                raw_data = self._f.read(data_to_read_raw)
                
                if not raw_data:
                    # Fim do arquivo raw. Finaliza a criptografia.
                    if self.active_encryptor:
                        try:
                            final_chunk = self.active_encryptor.finalize()
                            self.active_encryptor = None
                            if final_chunk:
                                self.active_pos += len(final_chunk)
                                return bytes_read + final_chunk
                        except Exception:
                            pass 
                    return bytes_read
                    
                encrypted_data = self.active_encryptor.update(raw_data)
                self.active_pos += len(encrypted_data)
                bytes_read += encrypted_data
                remaining_size -= len(encrypted_data)
                
                if len(encrypted_data) < data_to_read_raw: # Se a leitura de raw_data leu menos que o esperado, é o final do arquivo
                    return bytes_read
                    
        return bytes_read # Retorna quando remaining_size <= 0

    def tell(self): return self.active_pos 
    
    def seek(self, offset, whence=0):
        if whence == 0: # SEEK_SET
            self._setup_encryptor_for_seek(offset)
            return self.active_pos
        elif whence == 1: # SEEK_CUR
            return self.seek(self.active_pos + offset, 0)
        elif whence == 2: # SEEK_END
            return self.seek(self.total_size + offset, 0)
        else:
            raise IOError("Modo 'whence' inválido para seek.")
            
    def close(self):
        try:
            if self._f: self._f.close()
            self.active_encryptor = None
            self.active_iv = None
        except:
            pass 
            
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False 


# ==============================================================================
# 3A/3B. LOG E CONTROLE DB 
# ==============================================================================
# Classes LogWriterThread e LogControlDB inalteradas, exceto se houver necessidade de
# garantir o log_file_status após a falha/sucesso.

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
        self.FLUSH_INTERVAL = 10 
        
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
            sql = """
                INSERT INTO file_status 
                (run_id, pc_name, drive_letter, file_path_full, file_name, file_size_bytes, status, s3_key, error_detail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.cursor.executemany(sql, self.batch_entries)
            self.conn.commit()
            
            self.batch_entries = []
            self.last_flush_time = time.time()
            
        except Exception as e:
            print(f"ERRO CRÍTICO NO BATCH INSERT: {e}. Tentando gravação individual...")
            
            sql = """
                INSERT INTO file_status 
                (run_id, pc_name, drive_letter, file_path_full, file_name, file_size_bytes, status, s3_key, error_detail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
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
                item = self.log_queue.get(timeout=1)
                
                if item is None:
                    break
                
                fp, fn, fs, status, s3_key, error_detail = item
                
                entry = (
                    self.run_id, self.pc_name, self.drive_letter,
                    str(fp), fn, fs, status, s3_key, error_detail
                )
                self.batch_entries.append(entry)
                
                self.log_queue.task_done()
                
                if len(self.batch_entries) >= self.BATCH_SIZE:
                    self._flush_batch()
                    
            except queue.Empty:
                if time.time() - self.last_flush_time >= self.FLUSH_INTERVAL:
                    self._flush_batch()
            except Exception as e:
                print(f"Erro inesperado no LogWriterThread: {e}")
                time.sleep(1)
        
        print(f"LogWriter (Run ID {self.run_id}): Processando entradas finais...")
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
                 
        self._flush_batch()
        self.conn.close()
        print(f"LogWriter (Run ID {self.run_id}): Conexão DB fechada.")

    def stop(self):
        self._stop_event.set()
        self.log_queue.put(None)
        self.join()


class LogControlDB:
    def __init__(self, pc_name: str, root_path: str, collection: str, worker_id: int = 1):
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        self.conn_main = sqlite3.connect(LOG_DB_PATH, isolation_level='DEFERRED')
        self.cursor_main = self.conn_main.cursor()
        
        self.pc_name = pc_name
        self.root_path = root_path
        self.drive_letter = Path(root_path).parts[0].replace('\\', '').replace(':', '')
        self.collection = collection
        self.worker_id = worker_id 
        
        self._setup_db() 
        
        self.run_id = self._setup_run_log_entry() 
        self.timestamp_start = datetime.now()

        self.log_queue = queue.Queue()
        self.log_writer_thread = LogWriterThread(
            self.log_queue, LOG_DB_PATH, self.run_id, self.pc_name, self.drive_letter
        )

    def start_writer(self):
        self.log_writer_thread.start()
        
    def stop_writer(self):
        self.log_writer_thread.stop()


    def _setup_db(self):
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

    def _setup_run_log_entry(self):
        self.cursor_main.execute("""
            INSERT INTO run_log (
                pc_name, collection_name, root_path, drive_letter, timestamp_start, timestamp_end, 
                total_uploaded, total_ignored, total_failed
            )
            VALUES (?, ?, ?, ?, ?, NULL, 0, 0, 0)
        """, (
            f"{self.pc_name}-P{self.worker_id}", self.collection, self.root_path, self.drive_letter,
            datetime.now().isoformat()
        ))
        self.conn_main.commit()
        return self.cursor_main.lastrowid

    def log_file_status(self, file_path: Path, status: str, file_size: int, s3_key: Optional[str] = None, error_detail: Optional[str] = None):
        safe_error_detail = str(error_detail).encode('utf-8', 'ignore').decode('utf-8') if error_detail else None
        safe_s3_key = s3_key if s3_key else None
        
        log_entry = (
            file_path, file_path.name, file_size, 
            status, safe_s3_key, safe_error_detail
        )
        self.log_queue.put(log_entry)

    def finalize_run(self, counts: dict):
        self.stop_writer()
        
        self.timestamp_end = datetime.now()
        self.cursor_main.execute("""
            UPDATE run_log SET
                timestamp_end = ?,
                total_uploaded = ?,
                total_ignored = ?,
                total_failed = ?
            WHERE run_id = ?
        """, (
            self.timestamp_end.isoformat(),
            counts['uploaded'], counts['ignored'], counts['failed'],
            self.run_id
        ))
        self.conn_main.commit()

    def get_run_metrics(self):
        self.cursor_main.execute("""
            SELECT file_path_full, file_name, status, error_detail 
            FROM file_status 
            WHERE run_id = ? AND status != 'UPLOADED'
        """, (self.run_id,))
        non_uploaded_files = self.cursor_main.fetchall()
        
        return non_uploaded_files 

    def close(self):
        try:
            self.conn_main.close()
        except:
            pass

# ==============================================================================
# 4. UPLOAD ENGINE V12.1 (MPU JIT + THREAD SAFETY FIX)
# ==============================================================================

class UploadEngine:

    def __init__(self, collection_name, root_path, password, worker_id: int):
        self.root_path = Path(root_path)
        self.collection = collection_name
        self.worker_id = worker_id 
        self.crypto = CryptoEngine()
        
        # Otimização: Aumenta a concorrência do Boto3 por processo.
        s3_config = Config(
            max_pool_connections=UPLOAD_WORKERS + SCAN_WORKERS + 5,
            connect_timeout=120, 
            read_timeout=240     
        )
        self.s3 = boto3.client(
            's3', endpoint_url=MEGA_S3_ENDPOINT,
            aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,
            config=s3_config
        )
        
        self.pc_name = socket.gethostname().lower().replace(' ', '-')
        self.bucket_name = f"mega-hash-storage-{collection_name.lower()}" 
        
        self.crypto.load_or_create_master_key(password)
        self.clean_bucket_name = self.bucket_name.replace('.', '-')
        
        self.log_db = LogControlDB(self.pc_name, root_path, self.collection, self.worker_id)
        self.log_db.start_writer() 
        
        self.local_console = Console(theme=custom_theme)
        self._safe_print(f"[info]📊 Processo {self.worker_id} - Log DB ID: {self.log_db.run_id} | Arquivo: {LOG_DB_PATH.name}[/info]", style=None)
        
        self._ensure_bucket() 

        self.metrics = {'uploaded': 0, 'ignored': 0, 'failed': 0}
        
        # Fila de uploads agora contém (offset, length)
        self.upload_queue = queue.Queue()
        
        # MPU_STATE: {s3_key: {'UploadId': str, 'Parts': List[Dict], 'OriginalPath': Path, 'FatalError': bool, 'EncryptedSize': int}}
        self.mpu_state: Dict[str, Dict[str, Any]] = {} 
        self.mpu_lock = threading.Lock()
        
        self.progress_task_id = None


    def _safe_print(self, text, style=None):
        """Usa o console local do processo."""
        try:
            if style:
                self.local_console.print(f"[{style}]{text}[/{style}]")
            else:
                self.local_console.print(text)
        except UnicodeEncodeError:
            sanitized_text = text.encode('ascii', 'replace').decode('ascii').replace('?', '*')
            if style:
                 self.local_console.print(f"[{style}]**** ERRO UNICODE ****: {sanitized_text}[/{style}]")
            else:
                 self.local_console.print(f"[bold red]**** ERRO UNICODE ****: {sanitized_text} [/bold red]")
        except Exception:
            self.local_console.print("[bold red]**** ERRO GERAL DE CONSOLE ****[/bold red]")


    def _ensure_bucket(self):
        """
        Verifica/cria o bucket, implementando retry para erros de conexão.
        """
        max_attempts = 5
        retry_delay = 5
        
        for attempt in range(1, max_attempts + 1):
            try:
                self.s3.head_bucket(Bucket=self.clean_bucket_name)
                if attempt == 1:
                    self._safe_print(f"Bucket {self.clean_bucket_name} verificado.", style="info")
                return 
            
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                
                if error_code in ['404', 'NoSuchBucket']:
                    try:
                        self.s3.create_bucket(Bucket=self.clean_bucket_name)
                        self._safe_print(f"📦 Bucket {self.clean_bucket_name} criado.", style="success")
                        return 
                    except ClientError as create_e:
                        create_error_code = create_e.response.get('Error', {}).get('Code', 'Unknown')
                        if create_error_code == 'BucketAlreadyOwnedByYou':
                            self._safe_print(f"Bucket {self.clean_bucket_name} já existe (propriedade sua). Continuando...", style="info")
                            return 
                        else:
                            raise create_e 
                
                raise e 
            
            except Exception as e:
                is_connection_error = isinstance(e, (ConnectionError, ReadTimeoutError)) or 'Connection was closed' in str(e)
                
                if is_connection_error and attempt < max_attempts:
                    self._safe_print(f"[ALERTA] Falha de Conexão no Bucket (Tentativa {attempt}/{max_attempts}): {type(e).__name__}: {e}. Esperando {retry_delay}s...", style="retry")
                    time.sleep(retry_delay)
                    continue
                
                self._safe_print(f"Erro fatal verificando/criando bucket após {attempt} tentativas: {type(e).__name__}: {e}", style="error")
                self.log_db.close()
                sys.exit(1)
        
        self._safe_print(f"Erro fatal: Falha ao verificar/criar bucket {self.clean_bucket_name} após {max_attempts} tentativas.", style="error")
        self.log_db.close()
        sys.exit(1)


    def _calculate_content_hash(self, file_path: Path):
        """
        [MÁXIMA PERFORMANCE] Calcula um SHA-512 *de metadados* para criar uma chave S3 única.
        A chave é baseada no caminho relativo, tamanho e timestamp de modificação (evita leitura de disco).
        """
        try:
            # 1. Coleta Metadados (operação instantânea)
            stat_info = file_path.stat()
            file_size = stat_info.st_size
            mod_time = stat_info.st_mtime_ns # Alta precisão
            
            # 2. Cria o caminho relativo (chamar o método existente)
            rel_path = self._get_original_path_metadata(file_path)
            
            # 3. Combina e Hasheia
            data_to_hash = f"{rel_path}-{file_size}-{mod_time}".encode('utf-8')

            hasher = hashlib.sha512()
            hasher.update(data_to_hash)

            return hasher.hexdigest()
            
        except Exception as e:
            self._safe_print(f"[ERRO HASH] Falha ao ler metadados para {file_path.name}: {e}", style="error")
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

    def _abort_mpu(self, s3_key: str):
        """Tenta abortar um MPU em caso de falha fatal."""
        with self.mpu_lock:
            if s3_key in self.mpu_state:
                mpu_info = self.mpu_state[s3_key]
                upload_id = mpu_info.get('UploadId')
                file_path = mpu_info.get('OriginalPath')
                if upload_id:
                    try:
                        self.s3.abort_multipart_upload(
                            Bucket=self.clean_bucket_name,
                            Key=s3_key,
                            UploadId=upload_id
                        )
                        self._safe_print(f"[ALERTA] MPU Abortado para {file_path.name} ({s3_key[:10]}...).", style="warning")
                    except Exception as e:
                        pass
                del self.mpu_state[s3_key]

    def _scan_and_hash_worker(self, fp: Path, scanner_progress, task_id):
        """Worker thread (Produtor) que lê, criptografa, inicia MPU e enfileira chunks com offset/length."""
        file_size = 0
        s3_key_unique = None
        
        try:
            # 1. Checagens e Hashing (MAX SPEED)
            file_size = fp.stat().st_size
            
            if file_size == 0:
                self._safe_print(f"[VAZIO] Ignorando {fp.name}: Arquivo vazio.", style="warning")
                self.log_db.log_file_status(fp, 'IGNORED', 0, error_detail="ZERO_BYTE_SIZE")
                self.metrics['ignored'] += 1
                return 0
            
            content_hash = self._calculate_content_hash(fp)
            if not content_hash: 
                self._safe_print(f"[ALERTA] Ignorando {fp.name}: Falha no cálculo do hash (metadados).", style="warning")
                self.log_db.log_file_status(fp, 'IGNORED', file_size, error_detail="HASH_METADATA_ERROR")
                self.metrics['ignored'] += 1
                return 0
            
            s3_key_unique = self._create_s3_key(content_hash)
            original_metadata_path = self._get_original_path_metadata(fp)
            
            # 2. Cálculo do Tamanho Total Criptografado
            with self.crypto.create_encrypted_stream(fp, original_metadata_path) as temp_stream:
                encrypted_size = temp_stream.total_size
            
            # 3. Início do Multi-Part Upload (MPU)
            response = self.s3.create_multipart_upload(
                Bucket=self.clean_bucket_name,
                Key=s3_key_unique,
            )
            upload_id = response['UploadId']
            
            # Armazena o estado inicial do MPU
            with self.mpu_lock:
                self.mpu_state[s3_key_unique] = {
                    'UploadId': upload_id,
                    'Parts': [], 
                    'OriginalFileSize': file_size,
                    'OriginalPath': fp,
                    'FatalError': False,
                    'EncryptedSize': encrypted_size
                }
            
            # 4. Geração e Enfileiramento de Partes (Offset-Based) - ESTE É O PIPELINE
            current_offset = 0
            part_number = 1
            while current_offset < encrypted_size:
                chunk_length = min(MPU_PART_SIZE, encrypted_size - current_offset)
                
                # Enfileira o "endereço" da parte para o Uploader (Consumidor)
                self.upload_queue.put((
                    s3_key_unique, upload_id, part_number, 
                    current_offset, chunk_length, fp, file_size
                ))
                
                scanner_progress.update(task_id, description=f"[bold green]PathHash OK (MAX SPEED):[/bold green] {fp.name} - Part {part_number} ({humanize.naturalsize(chunk_length)})")
                
                current_offset += chunk_length
                part_number += 1
                    
            return encrypted_size
            
        except Exception as e:
            safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
            self._safe_print(f"[ERRO PROCESSO] Ignorando {fp.name}: {type(e).__name__}: {safe_error}", style="warning")
            
            self.log_db.log_file_status(fp, 'IGNORED', file_size, error_detail=f"PROCESSING_ERROR: {type(e).__name__}: {safe_error}")
            self.metrics['ignored'] += 1
            
            if s3_key_unique:
                self._abort_mpu(s3_key_unique)
                
            return 0


    def _upload_worker(self, progress: Progress, overall_task: int):
        """Worker thread (Consumidor) que realiza o upload de partes MPU (JIT)."""
        
        while True:
            try:
                item: Optional[UploadPartData] = self.upload_queue.get(timeout=1)
            except queue.Empty:
                continue

            if item is None:
                self.upload_queue.put(None) 
                self._safe_print(f"💤 Uploader P{self.worker_id} - {threading.get_ident()} finalizado por sentinel.", style="info")
                break
                
            s3_key, upload_id, part_number, offset, length, file_path, file_size = item
            
            uploaded_successfully = False
            etag = None
            chunk_data = None
            
            with self.mpu_lock:
                mpu_info = self.mpu_state.get(s3_key)
                original_path_metadata = mpu_info['OriginalPath'].as_posix() if mpu_info else str(file_path)

            try:
                # 1. Checagem de Falha
                with self.mpu_lock:
                    if not mpu_info or mpu_info.get('FatalError'):
                        self.upload_queue.task_done()
                        continue
                        
                # 2. Leitura Just-In-Time (JIT) do Chunk
                with self.crypto.create_encrypted_stream(file_path, original_path_metadata) as stream:
                    stream.seek(offset)
                    chunk_data = stream.read(length) 

                if chunk_data is None or len(chunk_data) != length:
                    raise IOError(f"Falha ao ler {length} bytes do disco. Lido: {len(chunk_data) if chunk_data else 0} bytes.")

                # 3. Tentativas de Upload
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        if attempt > 1:
                            self._safe_print(f"Retry {attempt}/{MAX_RETRIES} para {file_path.name} Part {part_number}...", style="retry")

                        # === O CANO DE UPLOAD: upload_part ===
                        response = self.s3.upload_part(
                            Bucket=self.clean_bucket_name,
                            Key=s3_key,
                            UploadId=upload_id,
                            PartNumber=part_number,
                            Body=chunk_data
                        )
                        
                        etag = response['ETag']
                        uploaded_successfully = True
                        break 
                        
                    except (ClientError, ConnectionError, ReadTimeoutError) as e:
                        
                        is_fatal_error = False
                        error_code = 'Unknown'
                        if isinstance(e, ClientError):
                            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                            # AQUI: Se a MPU foi concluída por outra thread, o NoSuchUpload é esperado, mas não é fatal.
                            if error_code in ['NoSuchUpload', 'InvalidPart']: 
                                is_fatal_error = True # Marca como fatal para sair do retry loop
                        
                        if is_fatal_error:
                            # Se for um erro fatal S3, verifica se a MPU não foi concluída.
                            with self.mpu_lock:
                                # Se o estado JÁ FOI DELETADO, é um NoSuchUpload "benigno" (MPU concluída).
                                if s3_key not in self.mpu_state:
                                    self._safe_print(f"⚠️ Alerta (Stale Part): Parte {part_number} de {file_path.name} (Key: {s3_key[:10]}...) tentou re-enviar após Completion. IGNORADO.", style="warning")
                                    # Simplesmente saímos do loop de retry sem lançar a exceção.
                                    uploaded_successfully = True
                                    break # Sai do loop de retries
                                
                                # Caso contrário, é um erro fatal real.
                                self.mpu_state[s3_key]['FatalError'] = True
                            
                            self._safe_print(f"[FIM] {file_path.name}: Upload cancelado ({error_code} detectado).", style="error")
                            raise Exception(f"FatalError: {error_code} Sessão abortada pelo servidor ou outra thread.")
                        
                        # Lógica de retry para erros de conexão ou timeout
                        if attempt < MAX_RETRIES:
                            safe_error_type = type(e).__name__
                            self._safe_print(f"Tentativa {attempt} falhou para {file_path.name} Part {part_number} ({safe_error_type}). Esperando {RETRY_DELAY}s...", style="warning")
                            time.sleep(RETRY_DELAY) 
                        else:
                            raise 
                            
                # Se o flag for True, pode ser sucesso ou o 'NoSuchUpload' ignorado acima.
                if not uploaded_successfully:
                    raise Exception("Todas as tentativas de upload da Parte MPU falharam.")
                
                # Se saiu do retry loop devido ao NoSuchUpload "benigno", apenas continua para o finally
                if s3_key not in self.mpu_state:
                    continue 

                # 4. ATUALIZA O PROGRESS BAR 
                progress.update(overall_task, advance=length)
                
                # 5. Atualiza o estado MPU com o ETag da parte
                with self.mpu_lock:
                    mpu_info_after_upload = self.mpu_state.get(s3_key)
                    if not mpu_info_after_upload or mpu_info_after_upload.get('FatalError'):
                         self.upload_queue.task_done()
                         continue
                         
                    mpu_info_after_upload['Parts'].append({
                        'PartNumber': part_number,
                        'ETag': etag
                    })
                    
                    total_parts_sent = len(mpu_info_after_upload['Parts'])
                    expected_parts = (mpu_info_after_upload['EncryptedSize'] + MPU_PART_SIZE - 1) // MPU_PART_SIZE

                # 6. FINALIZAÇÃO DO ARQUIVO
                if total_parts_sent == expected_parts:
                    self._safe_print(f"✅ Última Parte enviada. Iniciando COMPLETION para {file_path.name}.", style="info")
                    
                    # Esta seção está segura sob o mpu_lock
                    with self.mpu_lock:
                        mpu_info_completion = self.mpu_state.get(s3_key)
                        
                        if mpu_info_completion and not mpu_info_completion.get('FatalError'):
                            parts = sorted(mpu_info_completion['Parts'], key=lambda x: x['PartNumber'])
                            
                            self.s3.complete_multipart_upload(
                                Bucket=self.clean_bucket_name,
                                Key=s3_key,
                                UploadId=upload_id,
                                MultipartUpload={'Parts': parts}
                            )
                            
                            del self.mpu_state[s3_key] # ESTADO FINAL: REMOVIDO
                            
                            self._safe_print(f"✅ Upload CONCLUÍDO (MPU). Key: {s3_key[:16]}... | File: {file_path.name}", style="success")
                            self.log_db.log_file_status(file_path, 'UPLOADED', file_size, s3_key=s3_key)
                            self.metrics['uploaded'] += 1
                        else:
                            # Se FatalError foi setado (por outra thread que falhou a parte) enquanto esperava o lock
                            if s3_key in self.mpu_state: del self.mpu_state[s3_key]
                            
            except Exception as e:
                safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
                
                is_requeue_error = not ("FatalError" in safe_error or "NoSuchUpload" in safe_error or "InvalidPart" in safe_error)
                
                if is_requeue_error:
                    self._safe_print(f"⚠️ RE-ENFILEIRANDO (PARTE {part_number}): {file_path.name}: {safe_error}", style="warning")
                    self.upload_queue.put(item) 
                else:
                    # Se chegou aqui com um erro 'fatal' S3, significa que o estado *não* foi limpo na lógica de correção acima.
                    # Ou seja, é um erro fatal real que não é 'NoSuchUpload' de um arquivo concluído.
                    if "NoSuchUpload" not in safe_error and "InvalidPart" not in safe_error:
                         self._safe_print(f"❌ FALHA FATAL (PARTE {part_number}): {file_path.name}: {safe_error}", style="error")
                         self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"PART_FATAL: {safe_error}")
                         self.metrics['failed'] += 1
                    
                    if mpu_info:
                        try:
                            expected_parts = (mpu_info['EncryptedSize'] + MPU_PART_SIZE - 1) // MPU_PART_SIZE
                            
                            # Tenta avançar o progress bar para o que faltava, para evitar que o progress bar fique travado.
                            if len(mpu_info['Parts']) < expected_parts:
                                with self.mpu_lock:
                                    # Achar o tamanho real de partes completadas (se o mpu_info ainda existir)
                                    completed_parts = mpu_info.get('Parts', [])
                                    # Assumindo que o tamanho da parte falha é 'length'
                                    bytes_to_advance = length 
                                    
                                    # Apenas se o erro não foi um MPU abortado/cancelado (que já tratou o progress)
                                    if not mpu_info.get('FatalError', False):
                                         progress.update(overall_task, advance=bytes_to_advance) 

                        except Exception:
                            pass 

            finally:
                self.upload_queue.task_done()
                

    def run_partitioned(self, partitioned_file_list: List[Path]):
        total_bytes_encrypted = 0
        scanner_futures = []
        upload_futures = []
        
        self._safe_print(f"[bold]Processo {self.worker_id}: Varrendo partição de {len(partitioned_file_list)} arquivos.[/bold]", style=None)
        
        scanner_executor = ThreadPoolExecutor(max_workers=SCAN_WORKERS)
        uploader_executor = ThreadPoolExecutor(max_workers=UPLOAD_WORKERS)
        
        try:
            with Progress(
                TextColumn(f"[bold blue]P{self.worker_id} - [/bold blue]"+"{task.description}"), 
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TransferSpeedColumn(),
                TimeElapsedColumn(),
                console=self.local_console
            ) as progress:
                
                scan_task = progress.add_task(f"[yellow]Fase 1: Varredura, PathHashing e Chunking ({SCAN_WORKERS} Workers)...", total=len(partitioned_file_list))
                overall_task = progress.add_task(f"[green]Fase 2: Uploads MPU Ativos ({UPLOAD_WORKERS} Workers - Buffer: JIT)...", total=None)
                self.progress_task_id = overall_task 

                # 1. INICIA UPLOADERS (CONSUMIDORES)
                for i in range(UPLOAD_WORKERS):
                    upload_futures.append(
                        uploader_executor.submit(self._upload_worker, progress, overall_task)
                    )

                # 2. INICIA SCANNERS (PRODUTORES DE CHUNKS) - A PRODUÇÃO OCORRE CONCOMITANTEMENTE COM O CONSUMO
                
                files_scanned_count = 0
                for fp in partitioned_file_list:
                    files_scanned_count += 1
                    
                    progress.update(scan_task, description=f"[yellow]Fase 1: Varredura, PathHashing - {files_scanned_count}/{len(partitioned_file_list)} arquivos[/yellow]", advance=1)
                    
                    if fp.name.lower() in FORCED_IGNORE_FILENAMES or fp.name == KEY_FILE.name:
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
                
                progress.update(overall_task, total=total_bytes_encrypted)
                self._safe_print(f"[info]Chunking Completo. Tamanho Total Criptografado: {humanize.naturalsize(total_bytes_encrypted)}[/info]", style=None)

                # 4. AGUARDA O PROCESSAMENTO DA FILA (Uploaded e Fails)
                self.upload_queue.join() 
                
                # 5. SINALIZAÇÃO DE FIM (POISON PILL)
                for _ in range(UPLOAD_WORKERS):
                    self.upload_queue.put(None)
                
                uploader_executor.shutdown() 
                
            # 5.5. ABORTAMENTO CENTRALIZADO DE FALHAS
            with self.mpu_lock:
                for s3_key in list(self.mpu_state.keys()):
                    if self.mpu_state[s3_key].get('UploadId'):
                        self._abort_mpu(s3_key) 
                        
            # FASE 6: FINALIZAÇÃO E LOGS
            self.log_db.finalize_run(self.metrics) 
            
        except Exception as e:
            scanner_executor.shutdown(wait=False)
            uploader_executor.shutdown(wait=False)
            with self.mpu_lock:
                for s3_key in list(self.mpu_state.keys()):
                    if self.mpu_state[s3_key].get('UploadId'):
                        self._abort_mpu(s3_key) 
            raise e
        
# ==============================================================================
# FUNÇÕES E EXECUÇÃO PRINCIPAL (CONTROLADOR DE PROCESSOS - V12.1)
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

def partition_list(data: List[Any], num_partitions: int) -> List[List[Any]]:
    """Divide uma lista em N sublistas."""
    k, m = divmod(len(data), num_partitions)
    return [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(num_partitions)]

def generate_markdown_log(pc_name: str, collection: str, root_path: str, all_metrics: dict, all_non_uploaded: List[Tuple]):
    """Gera um arquivo de log formatado em Markdown com dados agregados."""
    
    timestamp_start = datetime.now()
    unique_hash = hashlib.sha1(f"{pc_name}{collection}{timestamp_start.isoformat()}".encode()).hexdigest()[:8]
    drive_letter = Path(root_path).parts[0].replace('\\', '').replace(':', '')
    
    log_filename = f"{pc_name}.{drive_letter}.{collection}.{timestamp_start.strftime('%Y%m%d%H%M%S')}.{unique_hash}.md"
    log_path = LOG_DIR / log_filename
    
    md_content = f"""
# 🚀 Relatório de Execução MEGA UPLOADER (AGREGADO)

## 🗓️ Detalhes da Execução
| Parâmetro | Valor |
| :--- | :--- |
| **PC de Execução** | `{pc_name}` |
| **Coleção/Bucket** | `{collection}` |
| **Pasta Raiz Varredura** | `{root_path}` |
| **Início da Execução** | `{timestamp_start.strftime('%Y-%m-%d %H:%M:%S')}` |
| **Fim da Execução** | `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}` |
| **Processos Paralelos** | `8` |

## 📊 Resumo do Processamento
| Status | Contagem |
| :--- | :--- |
| **✅ UPLOADED (Concluídos)** | `{all_metrics['uploaded']}` |
| **🚫 IGNORED (Ignorados na Varredura)** | `{all_metrics['ignored']}` |
| **❌ FAILED (Falhas Fatais de Upload)** | `{all_metrics['failed']}` |

---

## 🛑 Detalhes de Arquivos Não Uploadados (Ignorados e Falhas Fatais)

{'Não houve arquivos ignorados ou com falha.' if not all_non_uploaded else ''}

| Status | Nome do Arquivo | Caminho Completo | Detalhe do Erro |
| :--- | :--- | :--- | :--- |
"""
    for full_path, name, status, error_detail in all_non_uploaded:
        icon = '🚫' if status == 'IGNORED' else '❌'
        safe_name = name.encode('ascii', 'replace').decode('ascii').strip()
        safe_full_path = full_path.encode('ascii', 'replace').decode('ascii').strip()
        safe_error_detail = error_detail.encode('ascii', 'replace').decode('ascii').strip() if error_detail else 'N/A'
        
        md_content += f"| {icon} **{status}** | `{safe_name}` | `{safe_full_path}` | `{safe_error_detail}` |\n"

    try:
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(md_content)
        console.print(f"📄 Log Markdown (AGREGADO) salvo em: [path]{log_path}[/path]", style="info")
    except Exception as e:
        console.print(f"❌ ERRO FATAL ao salvar log MD: {e}", style="error")


def process_worker(
    worker_id: int, 
    collection_name: str, 
    root_path: str, 
    password: str, 
    file_list: List[Path],
    global_metrics_dict: Dict[str, Any],
    global_non_uploaded_list: List[Tuple], 
    total_workers: int
):
    """
    Função a ser executada por cada processo.
    """
    if not file_list:
        return

    local_console = Console(theme=custom_theme)
    local_console.print(Panel(
        f"[bold cyan]🚀 Processo {worker_id}/{total_workers} Iniciado - {len(file_list)} Arquivos[/bold cyan]", 
        border_style="cyan"
    ))
    
    engine = None
    try:
        engine = UploadEngine(collection_name, root_path, password, worker_id)
        engine.run_partitioned(file_list) 
        
        # 1. Coleta Métrica 
        local_metrics = engine.metrics
        
        # 2. Coleta Detalhes de Arquivos Não-Enviados
        local_non_uploaded = engine.log_db.get_run_metrics() 
        
        # 3. Atualiza o dicionário compartilhado e a lista compartilhada
        with engine.mpu_lock: 
            for key, count in local_metrics.items():
                global_metrics_dict[f'P{worker_id}_{key}'] = count
            
            global_non_uploaded_list.extend(local_non_uploaded)
                    
    except Exception as e:
        if engine and hasattr(engine, '_safe_print'):
             engine._safe_print(Panel(f"[bold red]ERRO CRÍTICO NO PROCESSO {worker_id}: [/bold red]{type(e).__name__}: {e}", title="Falha no Processo", border_style="red"), style=None)
        else:
             local_console.print(Panel(f"[bold red]ERRO CRÍTICO NO PROCESSO {worker_id}: [/bold red]{type(e).__name__}: {e}", title="Falha no Processo", border_style="red"))
    finally:
        if engine and hasattr(engine, 'log_db'):
            try:
                engine.log_db.stop_writer()
            except Exception:
                pass 
            try:
                engine.log_db.close()
            except Exception:
                pass 


if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    # Título atualizado para refletir a nova otimização de throughput
    console.print(Panel.fit("[bold white on magenta] MEGA CRYPTO HASH UPLOADER (V12.1 - ENHANCED THREAD SAFETY) [/bold white on magenta]", border_style="magenta"))
    
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

    # 3. Varredura e Particionamento
    all_files = []
    try:
        console.print("[info]Iniciando varredura primária para particionamento (Modo: MAX SPEED HASH)...[/info]")
        for root, _, files in os.walk(target_folder):
            for f in files:
                all_files.append(Path(root) / f)
        
        if not all_files:
            console.print("[warning]Nenhum arquivo encontrado para upload.[/warning]")
            console.print("\n[bold magenta]*** FIM DA SESSÃO ***[/bold magenta]")
            input("Pressione ENTER para fechar...")
            sys.exit(0)
            
        NUM_PROCESSES = 8 # Número fixo de motores (escala horizontal)
        file_partitions = partition_list(all_files, NUM_PROCESSES)
        
        console.print(f"[success]✅ Varredura Completa. Total de Arquivos: {len(all_files)}.[/success]")
        console.print(f"[info]⚙️ Distribuindo arquivos em {NUM_PROCESSES} processos...[/info]")

    except Exception as e:
        console.print(Panel(f"[bold red]ERRO CRÍTICO NA VARREDURA: [/bold red]{type(e).__name__}: {e}", title="Falha na Preparação", border_style="red"))
        sys.exit(1)


    # 4. Execução dos Processos
    processes = []
    manager = Manager()
    global_metrics = manager.dict({'uploaded': 0, 'ignored': 0, 'failed': 0}) 
    global_non_uploaded = manager.list() 
    
    try:
        for i in range(NUM_PROCESSES):
            p_id = i + 1
            p = Process(target=process_worker, args=(
                p_id, 
                collection_name, 
                target_folder, 
                password, 
                file_partitions[i], 
                global_metrics,
                global_non_uploaded, 
                NUM_PROCESSES
            ))
            processes.append(p)
            p.start()
            
        # 5. Espera e Agregação
        for p in processes:
            p.join()

        # Agregação final das métricas
        final_metrics = {'uploaded': 0, 'ignored': 0, 'failed': 0}
        
        for key, value in global_metrics.items():
            if '_uploaded' in key:
                final_metrics['uploaded'] += value
            elif '_ignored' in key:
                final_metrics['ignored'] += value
            elif '_failed' in key:
                final_metrics['failed'] += value
                
        # 6. Finalização e Log Centralizado 
        generate_markdown_log(
            socket.gethostname().lower().replace(' ', '-'), 
            collection_name, 
            target_folder, 
            final_metrics, 
            list(global_non_uploaded)
        )
        
        final_panel_content = (
            f"[bold green]OPERAÇÃO GLOBAL FINALIZADA! (8 PROCESSOS)[/bold green]\n\n"
            f"📦 Coleção: [cyan]{collection_name}[/cyan]\n"
            f"✅ Uploads Concluídos: [success]{final_metrics['uploaded']}[/success]\n"
            f"🚫 Arquivos Ignorados: [warning]{final_metrics['ignored']}[/warning]\n"
            f"❌ Falhas Fatais (Retry Esgotado): [error]{final_metrics['failed']}[/error]\n\n"
            "Relatório detalhado salvo em arquivo .md."
        )
        console.print(Panel(final_panel_content, title="[bold blue]Conclusão Global do Processo[/bold blue]"), style=None)
        
    except Exception as e:
        console.print(Panel(f"[bold red]ERRO CRÍTICO NO CONTROLADOR DE PROCESSOS: [/bold red]{type(e).__name__}: {e}", title="Falha Inesperada", border_style="red"))
    finally:
        for p in processes:
            if p.is_alive():
                p.terminate()
        
    # 7. Finalização
    console.print("\n[bold magenta]*** FIM DA SESSÃO ***[/bold magenta]")
    input("Pressione ENTER para fechar...")
