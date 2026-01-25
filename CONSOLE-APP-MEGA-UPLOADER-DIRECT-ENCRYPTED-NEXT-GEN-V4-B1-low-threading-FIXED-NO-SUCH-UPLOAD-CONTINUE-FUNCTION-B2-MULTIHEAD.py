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
from typing import Optional, Tuple, Dict, Any, List, Union, Set, Callable # Adicionado Set e Callable
import sqlite3
from datetime import datetime
from multiprocessing import Process, Manager, Queue as MPQueue # Importa Queue para uso no Manager
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
# CONFIGURAÇÕES E CONSTANTES GLOBAIS - V14.0 (SMALLEST-FIRST LOGIC)
# ==============================================================================

MEGA_S3_ENDPOINT = "XXX"
ACCESS_KEY = "XXX"
SECRET_KEY = "XXX"

# CONFIGURAÇÃO DE THREADS (MANTIDO para teste e I/O balanceado)
SCAN_WORKERS = 8    
UPLOAD_WORKERS = 100 
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

# --- NOVAS CONSTANTES PARA LÓGICA MULTI-HEAD ---
NUM_PROCESSES = 8 # Número fixo de motores (cada processo é uma cabeça)

VIDEO_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', 
    '.m4v', '.mpg', '.mpeg', '.3gp', '.ts', '.m2ts', '.vob'
}

SIZE_100MB = 100 * 1024 * 1024
SIZE_1GB = 1 * 1024 * 1024 * 1024
SIZE_5GB = 5 * 1024 * 1024 * 1024

HEAD_GROUPS = {
    # Stealing: Prioridade é do maior gargalo (H8/H7) ou maior quantidade (H5).
    # Mantido para robustez, mas o Smallest-First na alocação garante que os menores arquivos sejam atacados primeiro.
    1: {'name': 'TOP_1000_A', 'partner': 2, 'stealing': [8, 7, 5, 6, 4, 3]},
    2: {'name': 'TOP_1000_B', 'partner': 1, 'stealing': [8, 7, 5, 6, 4, 3]},
    3: {'name': 'VIDEO_SMALL', 'partner': 4, 'stealing': [8, 7, 5, 6, 1, 2]},
    4: {'name': 'VIDEO_LARGE', 'partner': 3, 'stealing': [8, 7, 5, 6, 1, 2]},
    5: {'name': 'GEN_TINY', 'partner': 6, 'stealing': [8, 7, 4, 3, 1, 2]},
    6: {'name': 'GEN_MED', 'partner': 5, 'stealing': [8, 7, 4, 3, 1, 2]},
    7: {'name': 'GEN_BIG', 'partner': 8, 'stealing': [5, 6, 4, 3, 1, 2]},
    8: {'name': 'GEN_HUGE', 'partner': 7, 'stealing': [5, 6, 4, 3, 1, 2]},
}
# --- FIM NOVAS CONSTANTES ---

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
# 1. MOTOR DE CRIPTOGRAFIA (INALTERADO)
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
# 2. STREAM CRIPTOGRAFADO (INALTERADO)
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
# 3A/3B. LOG E CONTROLE DB (INALTERADO)
# ==============================================================================
# Classes LogWriterThread inalteradas.

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
    def __init__(self, pc_name: str, root_path: str, collection: str, worker_id: int = 1, db_path: Path = LOG_DB_PATH):
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        self.db_path = db_path
        self.conn_main = sqlite3.connect(self.db_path, isolation_level='DEFERRED')
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
            self.log_queue, self.db_path, self.run_id, self.pc_name, self.drive_letter
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

    # --- MÉTODOS DE CONTINUAÇÃO (INALETRADO) ---
    @classmethod
    def get_past_runs_info(cls, db_path: Path) -> List[Tuple[int, str, str, str, str]]:
        """Busca todas as execuções anteriores, ordenadas da mais recente."""
        if not db_path.exists():
            return []
        
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    run_id, pc_name, collection_name, root_path, timestamp_start 
                FROM 
                    run_log 
                ORDER BY 
                    run_id DESC
            """)
            return cursor.fetchall()
        except Exception as e:
            console.print(f"[error]Erro ao buscar execuções passadas: {e}[/error]")
            return []
        finally:
            if conn: conn.close()
            
    @classmethod
    def get_uploaded_files_for_continuation(cls, db_path: Path, collection: str, drive_letter: str) -> Set[str]:
        """
        Retorna um conjunto (set) de caminhos de arquivos completos que JÁ FORAM UPLOADED
        para a combinação COLLECTION/DRIVE.
        """
        if not db_path.exists():
            return set()
            
        conn = None
        uploaded_files = set()
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    file_path_full 
                FROM 
                    file_status 
                WHERE 
                    status = 'UPLOADED' AND 
                    drive_letter = ? AND 
                    file_path_full IN (
                        SELECT file_path_full 
                        FROM file_status 
                        WHERE status = 'UPLOADED' 
                        AND EXISTS (
                            SELECT 1 FROM run_log 
                            WHERE run_log.run_id = file_status.run_id 
                            AND run_log.collection_name = ?
                        )
                    )
            """, (drive_letter, collection))
            
            for row in cursor.fetchall():
                uploaded_files.add(row[0])
                
        except Exception as e:
            console.print(f"[error]Erro ao buscar lista de arquivos enviados: {e}[/error]")
        finally:
            if conn: conn.close()
            
        return uploaded_files
    # --- FIM: MÉTODOS DE CONTINUAÇÃO ---


    def close(self):
        try:
            self.conn_main.close()
        except:
            pass

# ==============================================================================
# 4. UPLOAD ENGINE V14.0 (MPU JIT + MULTI-HEAD PRODUCER)
# ==============================================================================

class UploadEngine:

    def __init__(self, collection_name, root_path, password, head_id: int):
        self.root_path = Path(root_path)
        self.collection = collection_name
        self.head_id = head_id 
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
        
        self.log_db = LogControlDB(self.pc_name, root_path, self.collection, self.head_id)
        self.log_db.start_writer() 
        
        self.local_console = Console(theme=custom_theme)
        self._safe_print(f"[info]📊 HEAD {self.head_id} ({HEAD_GROUPS[self.head_id]['name']}) - Log DB ID: {self.log_db.run_id} | Arquivo: {LOG_DB_PATH.name}[/info]", style=None)
        
        self._ensure_bucket() 

        self.metrics = {'uploaded': 0, 'ignored': 0, 'failed': 0}
        
        # Fila de uploads AGORA É LOCAL (somente chunks do meu HEAD)
        self.upload_queue = queue.Queue()
        
        # MPU_STATE: {s3_key: {'UploadId': str, 'Parts': List[Dict], 'OriginalPath': Path, 'FatalError': bool, 'EncryptedSize': int}}
        self.mpu_state: Dict[str, Dict[str, Any]] = {} 
        self.mpu_lock = threading.Lock()
        
        self.progress_task_id = None
        
        # Lock de Scanner para Top 1000 (H1/H2)
        self.top_1000_lock = threading.Lock() 


    def _safe_print(self, text, style=None):
        """Usa o console local do processo."""
        try:
            # Adiciona o ID da Head ao print
            prefix = f"[HEAD {self.head_id}]"
            if style:
                self.local_console.print(f"{prefix}[{style}]{text}[/{style}]")
            else:
                self.local_console.print(f"{prefix}{text}")
        except UnicodeEncodeError:
            sanitized_text = text.encode('ascii', 'replace').decode('ascii').replace('?', '*')
            self.local_console.print(f"[HEAD {self.head_id}][bold red]**** ERRO UNICODE ****: {sanitized_text} [/bold red]")
        except Exception:
            self.local_console.print(f"[HEAD {self.head_id}][bold red]**** ERRO GERAL DE CONSOLE ****[/bold red]")


    def _ensure_bucket(self):
        """
        Verifica/cria o bucket, implementando retry para erros de conexão. (INALTERADO)
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
        [MÁXIMA PERFORMANCE] Calcula um SHA-512 *de metadados* para criar uma chave S3 única. (INALTERADO)
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
        """Tenta abortar um MPU em caso de falha fatal. (INALTERADO)"""
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

    def _scan_and_hash_worker_multi_head(self, file_path_tuple: Tuple[str, int]):
        """
        Worker thread (Produtor) que lê, criptografa, inicia MPU e enfileira chunks com offset/length.
        Recebe UM ARQUIVO por vez do Produtor Central.
        """
        fp = Path(file_path_tuple[0])
        file_size_original = file_path_tuple[1]
        s3_key_unique = None
        
        try:
            # 1. Checagens e Hashing (MAX SPEED)
            
            # Nota: A checagem de IGNORE JÁ FOI FEITA no controlador principal.
            file_size = fp.stat().st_size
            
            if file_size == 0:
                # Arquivo pode ter sido esvaziado após o scan principal
                self._safe_print(f"[VAZIO] Ignorando {fp.name}: Arquivo vazio.", style="warning")
                self.log_db.log_file_status(fp, 'IGNORED', 0, error_detail="ZERO_BYTE_SIZE")
                self.metrics['ignored'] += 1
                return 0
            
            content_hash = self._calculate_content_hash(fp)
            if not content_hash: 
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
                    'EncryptedSize': encrypted_size,
                    'HeadID': self.head_id # Marca qual Head é responsável por este MPU
                }
            
            # 4. Geração e Enfileiramento de Partes (Offset-Based) - PRODUZ NO SEU PRÓPRIO QUEUE
            current_offset = 0
            part_number = 1
            while current_offset < encrypted_size:
                chunk_length = min(MPU_PART_SIZE, encrypted_size - current_offset)
                
                # Enfileira o "endereço" da parte para o Uploader (Consumidor)
                self.upload_queue.put((
                    s3_key_unique, upload_id, part_number, 
                    current_offset, chunk_length, fp, file_size
                ))
                
                # Note: Não atualizamos o progress bar aqui, pois o scan agora é por arquivo
                
                current_offset += chunk_length
                part_number += 1
                    
            return encrypted_size
            
        except Exception as e:
            safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
            self._safe_print(f"[ERRO PROCESSO] Ignorando {fp.name}: {type(e).__name__}: {safe_error}", style="warning")
            
            self.log_db.log_file_status(fp, 'IGNORED', file_size_original, error_detail=f"PROCESSING_ERROR: {type(e).__name__}: {safe_error}")
            self.metrics['ignored'] += 1
            
            if s3_key_unique:
                self._abort_mpu(s3_key_unique)
                
            return 0


    def _upload_worker(self, progress: Progress, overall_task: int):
        """Worker thread (Consumidor) que realiza o upload de partes MPU (JIT). (LIGEIRAMENTE MODIFICADO PARA CLAREZA)"""
        
        while True:
            try:
                # Pega do meu queue local
                item: Optional[UploadPartData] = self.upload_queue.get(timeout=1)
            except queue.Empty:
                continue

            if item is None:
                self.upload_queue.put(None) 
                # Esta thread interna pode terminar, mas o Processo Mestre precisa do sinal
                break
                
            s3_key, upload_id, part_number, offset, length, file_path, file_size = item
            
            uploaded_successfully = False
            etag = None
            chunk_data = None
            
            with self.mpu_lock:
                mpu_info = self.mpu_state.get(s3_key)
                original_path_metadata = mpu_info['OriginalPath'].as_posix() if mpu_info else str(file_path)

            try:
                # 1. Checagem de Falha (Se outro worker no meu processo falhou fatalmente, ou se MPU foi completado/abortado)
                with self.mpu_lock:
                    if not mpu_info or mpu_info.get('FatalError'):
                        self.upload_queue.task_done()
                        continue
                        
                # 2. Leitura Just-In-Time (JIT) do Chunk
                with self.crypto.create_encrypted_stream(file_path, original_path_metadata) as stream:
                    stream.seek(offset)
                    chunk_data = stream.read(length) 

                if chunk_data is None or len(chunk_data) != length:
                    # Se o MPU foi completado (e o mpu_info foi removido), este erro é esperado. A exceção abaixo vai tratar.
                    # Mas se o mpu_info ainda está aqui, é um erro de disco/leitura.
                    if mpu_info:
                        self._safe_print(f"ERRO READ: Falha ao ler {length} bytes do disco. Lido: {len(chunk_data) if chunk_data else 0} bytes. Abortando MPU.", style="error")
                        self._abort_mpu(s3_key)
                        self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail="READ_FAIL_DISK_IO")
                        self.metrics['failed'] += 1
                    
                    self.upload_queue.task_done()
                    continue


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
                            
                            # ANALISE OTIMIZADA PARA NOSUCHUPLOAD: Se o upload_part falha porque a sessão MPU já foi completada/abortada.
                            if error_code in ['NoSuchUpload', 'InvalidPart']: 
                                # Verifica se a MPU foi concluída por outra thread no *final do processo* (estado removido)
                                with self.mpu_lock:
                                    if s3_key not in self.mpu_state:
                                        self._safe_print(f"⚠️ Alerta (Stale Part): Parte {part_number} de {file_path.name} tentou re-enviar após Completion/Abort. IGNORADO.", style="warning")
                                        uploaded_successfully = True # Marca como sucesso 'lógico' (sair do retry loop)
                                        break # Sai do loop de retries
                                    
                                    # Caso contrário, é um erro fatal *real* (e.g., MPU expirou no servidor).
                                    self.mpu_state[s3_key]['FatalError'] = True
                                    is_fatal_error = True
                            
                        if is_fatal_error:
                            self._safe_print(f"[FIM] {file_path.name}: Upload cancelado ({error_code} detectado).", style="error")
                            raise Exception(f"FatalError: {error_code} Sessão abortada/concluída pelo servidor/outra thread.")
                        
                        # Lógica de retry para erros de conexão ou timeout
                        if attempt < MAX_RETRIES:
                            time.sleep(RETRY_DELAY) 
                        else:
                            raise 
                            
                # Se saiu do retry loop devido ao NoSuchUpload "benigno", apenas continua para o finally
                if s3_key not in self.mpu_state:
                    continue 

                # 4. ATUALIZA O PROGRESS BAR 
                progress.update(overall_task, advance=length)
                
                # 5. Atualiza o estado MPU com o ETag da parte
                # AQUI: O Lock é fundamental para garantir que o Parts List seja atômico.
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
                    
                    # Lock novamente, para garantir que o Complete seja atômico
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
                            
                            del self.mpu_state[s3_key] # ESTADO FINAL: REMOVIDO (Protege contra NoSuchUpload Stale)
                            
                            self._safe_print(f"✅ Upload CONCLUÍDO (MPU). Key: {s3_key[:16]}... | File: {file_path.name}", style="success")
                            self.log_db.log_file_status(file_path, 'UPLOADED', file_size, s3_key=s3_key)
                            self.metrics['uploaded'] += 1
                        else:
                            # Se FatalError foi setado (por outra thread que falhou a parte) enquanto esperava o lock
                            if s3_key in self.mpu_state: del self.mpu_state[s3_key]
                            
            except Exception as e:
                safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
                
                # Se o erro não for de requeue e não for o NoSuchUpload benigno, é fatal
                is_fatal_error = ("FatalError" in safe_error or "NoSuchUpload" in safe_error or "InvalidPart" in safe_error)
                
                if not is_fatal_error:
                    self._safe_print(f"⚠️ RE-ENFILEIRANDO (PARTE {part_number}): {file_path.name}: {safe_error}", style="warning")
                    self.upload_queue.put(item) # Re-enqueue for local retry
                else:
                    self._safe_print(f"❌ FALHA FATAL (PARTE {part_number}): {file_path.name}: {safe_error}", style="error")
                    
                    if mpu_info:
                        self._abort_mpu(s3_key) # Aborta a sessão MPU inteira
                        self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"PART_FATAL: {safe_error}")
                        self.metrics['failed'] += 1
                        
                        # Tenta avançar o progress bar para o que faltava, para evitar que o progress bar fique travado.
                        try:
                            expected_parts = (mpu_info['EncryptedSize'] + MPU_PART_SIZE - 1) // MPU_PART_SIZE
                            completed_parts = len(mpu_info.get('Parts', []))
                            remaining_parts = expected_parts - completed_parts
                            
                            # Assume que o restante será ignorado/falhou
                            progress.update(overall_task, advance=(remaining_parts * MPU_PART_SIZE))
                        except Exception:
                            pass 

            finally:
                self.upload_queue.task_done()
                

    def run_multi_head(self, file_queue: MPQueue):
        """
        Loop principal do Head. Pega arquivos da fila e os transforma em chunks MPU.
        """
        scanner_futures = []
        
        scanner_executor = ThreadPoolExecutor(max_workers=SCAN_WORKERS)
        uploader_executor = ThreadPoolExecutor(max_workers=UPLOAD_WORKERS)
        
        self._safe_print(f"[bold]HEAD {self.head_id} Iniciado: Produzindo chunks no Worker Pool...[/bold]", style=None)
        
        # 1. INICIA UPLOADERS (CONSUMIDORES) - Rodam em loop infinito
        for i in range(UPLOAD_WORKERS):
            uploader_executor.submit(self._upload_worker, self.progress_tracker, self.progress_task_id)

        # 2. PRODUTOR (Esta thread)
        
        while True:
            try:
                # O timeout deve ser alto, pois a fila principal pode ser lenta
                file_path_tuple = file_queue.get(timeout=30) 
            except queue.Empty:
                break # Ociosidade longa, assumimos que esta fila primária está vazia
            except Exception as e:
                self._safe_print(f"[ERRO HEAD] Falha na fila de trabalho: {e}", style="error")
                break
                
            # Produz chunks MPU (multi-thread dentro do processo)
            scanner_futures.append(
                scanner_executor.submit(self._scan_and_hash_worker_multi_head, file_path_tuple)
            )

        # 3. Finalização da Produção
        self._safe_print("[info]Fila Primária (ou Stealing) drenada. Aguardando conclusão de hashing/chunking...[/info]")
        
        total_bytes_encrypted = 0
        for future in as_completed(scanner_futures):
            try:
                encrypted_size = future.result()
                total_bytes_encrypted += encrypted_size
                # Atualizamos o total do progress bar (este é o total de bytes que PRECISAM ser upados)
                self.progress_tracker.update(self.progress_task_id, total=self.progress_tracker.tasks[self.progress_task_id].total + encrypted_size)
            except Exception:
                pass 
                
        scanner_executor.shutdown()
        
        # 4. Finaliza Consumo
        self._safe_print(f"[info]Chunking Concluído. Total de Bytes a Upados (Chunks): {humanize.naturalsize(total_bytes_encrypted)}[/info]", style=None)
        self.upload_queue.join() 
        
        # 5. Sinalização de Fim (POISON PILL)
        for _ in range(UPLOAD_WORKERS):
            self.upload_queue.put(None)
        
        uploader_executor.shutdown() 
        
        # 6. Abortamento Centralizado de Falhas (do meu HEAD)
        with self.mpu_lock:
            for s3_key in list(self.mpu_state.keys()):
                if self.mpu_state[s3_key].get('UploadId'):
                    self._abort_mpu(s3_key) 
                        
        # FASE 7: FINALIZAÇÃO E LOGS
        self.log_db.finalize_run(self.metrics) 

    
    def set_progress_tracker(self, progress: Progress, task_id: Any):
        self.progress_tracker = progress
        self.progress_task_id = task_id
        
        
# ==============================================================================
# FUNÇÕES E EXECUÇÃO PRINCIPAL (CONTROLADOR DE PROCESSOS - V14.0)
# ==============================================================================

def allocate_files_to_queues(
    pending_files: List[Tuple[Path, int]], # (Path, Size)
    queues: Dict[int, MPQueue]
) -> List[Tuple[Path, int]]:
    """
    Implementa a lógica de 8-Head para distribuir os arquivos.
    AGORA COM ORDENAÇÃO SMALLEST-FIRST em todas as filas.
    """
    files_to_process_info = []
    ignored_system_files = []

    # 1. Classificação e Filtragem
    for fp, size in pending_files:
        if fp.name.lower() in FORCED_IGNORE_FILENAMES or fp.name == KEY_FILE.name:
            ignored_system_files.append((fp, size))
            continue
        
        files_to_process_info.append((fp, size, os.path.splitext(fp.name)[1].lower()))
        
    # 2. Ordenar por tamanho decrescente *apenas* para identificar o TOP 1000.
    files_to_process_info.sort(key=lambda x: x[1], reverse=True)
    
    top_1000 = files_to_process_info[:1000]
    remainder = files_to_process_info[1000:]
    
    # --- NOVOS BUCKETS TEMPORÁRIOS ---
    temp_buckets = {
        1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 7: [], 8: []
    }
    
    # 3. Alocação (Priority 0: Top 1000) - HEADS 1 e 2
    top_1000_a = top_1000[:500]
    top_1000_b = top_1000[500:]

    # Preenche buckets temporários
    temp_buckets[1].extend(top_1000_a)
    temp_buckets[2].extend(top_1000_b)
        
    # 4. Alocação (Remainder) - HEADS 3 a 8
    for fp, size, ext in remainder:
        # Vídeos (Priority 1)
        if ext in VIDEO_EXTENSIONS:
            if size <= SIZE_1GB:
                temp_buckets[3].append((fp, size, ext)) # H3: Small Video
            else:
                temp_buckets[4].append((fp, size, ext)) # H4: Large Video
        
        # Gerais (Priority 2)
        else:
            if size <= SIZE_100MB:
                temp_buckets[5].append((fp, size, ext)) # H5: Tiny General
            elif size <= SIZE_1GB:
                temp_buckets[6].append((fp, size, ext)) # H6: Medium General
            elif size <= SIZE_5GB:
                temp_buckets[7].append((fp, size, ext)) # H7: Big General
            else:
                temp_buckets[8].append((fp, size, ext)) # H8: Huge General
                
    # 5. ORDENAÇÃO SMALLEST-FIRST E ENFILEIRAMENTO FINAL
    for head_id, files in temp_buckets.items():
        if not files: continue
        
        # Ordena a lista de arquivos para este HEAD por tamanho ascendente (SMALLEST-FIRST)
        files.sort(key=lambda x: x[1], reverse=False)
        
        # Enfileira os arquivos ordenados
        for fp, size, _ in files:
             queues[head_id].put((fp.as_posix(), size))
                
    return ignored_system_files


def process_worker_multi_head(
    head_id: int, 
    collection_name: str, 
    root_path: str, 
    password: str, 
    head_queues: Dict[int, MPQueue],
    global_metrics_dict: Dict[str, Any],
    global_non_uploaded_list: List[Tuple], 
):
    """
    Função a ser executada por cada processo. Implementa a lógica de Work Stealing.
    """
    local_console = Console(theme=custom_theme)
    engine = None
    
    try:
        engine = UploadEngine(collection_name, root_path, password, head_id) 
        
        # 1. Configuração do Progress Bar Local
        with Progress(
            TextColumn(f"[bold blue]HEAD {head_id} - [/bold blue]"+"{task.description}"), 
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            console=engine.local_console
        ) as progress:
            
            # Tarefa principal (total é indefinido inicialmente, ajustado dinamicamente)
            overall_task = progress.add_task(f"[green]Fase 2: Uploads MPU Ativos ({UPLOAD_WORKERS} Workers)...", total=1) # Total mínimo 1 para iniciar.
            engine.set_progress_tracker(progress, overall_task)
            
            # 2. Loop de Work Stealing
            current_target_id = head_id
            
            while True:
                current_queue = head_queues[current_target_id]
                
                # A. Drenar a Fila Atual
                engine._safe_print(f"[info]Atacando fila {current_target_id} ({HEAD_GROUPS[current_target_id]['name']})...[/info]", style=None)
                
                # A lógica de produção de chunks (drenar a fila) é o core do run_multi_head
                engine.run_multi_head(current_queue)
                
                # B. Verificação de Fim de Tarefa
                
                # Se a Fila Primária (head_id) foi drenada
                if current_target_id == head_id and current_queue.empty():
                    engine._safe_print(f"[success]Fila Primária (HEAD {head_id}) drenada.[/success]")
                    
                    # C. Tentar Stealing Intra-Group
                    partner_id = HEAD_GROUPS[head_id]['partner']
                    if not head_queues[partner_id].empty():
                        current_target_id = partner_id
                        continue # Volta ao loop, ataca a fila do parceiro
                    
                    # D. Tentar Stealing Inter-Group
                    engine._safe_print(f"[warning]Grupo {head_id}/{partner_id} esgotado. Iniciando MUTATION (Inter-Group Stealing)...[/warning]")
                    
                    stolen_target_id = None
                    for target_id in HEAD_GROUPS[head_id]['stealing']:
                        if not head_queues[target_id].empty():
                            stolen_target_id = target_id
                            break
                            
                    if stolen_target_id is not None:
                        current_target_id = stolen_target_id
                        continue # Volta ao loop, ataca a fila roubada
                        
                    # E. FIM ABSOLUTO
                    engine._safe_print("[bold green]FIM ABSOLUTO: Todas as filas acessíveis drenadas.[/bold green]", style=None)
                    break 
                
                # Se drenou uma fila roubada (Stealing Target), volta à lógica de Stealing
                elif current_target_id != head_id:
                    engine._safe_print(f"[success]Fila Roubada ({current_target_id}) drenada. Voltando para o ciclo de Stealing...[/success]")
                    current_target_id = head_id # Volta ao ciclo para reavaliar a situação geral
                    continue
                
                # Caso Inesperado (Não deveria ocorrer se o run_multi_head for bem implementado)
                else:
                    engine._safe_print(f"[error]Erro na lógica do loop de Stealing. Fila {head_id} não esvaziada, mas loop parou.[/error]")
                    break

        # FIM DO PROCESSO: Coleta de métricas e log
        local_metrics = engine.metrics
        local_non_uploaded = engine.log_db.get_run_metrics() 
        
        # Atualiza o dicionário compartilhado e a lista compartilhada (Thread-Safe via Manager)
        for key, count in local_metrics.items():
            global_metrics_dict[f'H{head_id}_{key}'] = count
        
        global_non_uploaded_list.extend(local_non_uploaded)
                    
    except Exception as e:
        safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
        if engine and hasattr(engine, '_safe_print'):
             engine._safe_print(Panel(f"[bold red]ERRO CRÍTICO NO PROCESSO {head_id}: [/bold red]{type(e).__name__}: {safe_error}", title="Falha no Processo", border_style="red"), style=None)
        else:
             local_console.print(Panel(f"[bold red]ERRO CRÍTICO NO PROCESSO {head_id}: [/bold red]{type(e).__name__}: {safe_error}", title="Falha no Processo", border_style="red"))
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
    console.print(Panel.fit("[bold white on magenta] MEGA CRYPTO HASH UPLOADER (V14.0 - SMALLEST-FIRST BALANCING) [/bold white on magenta]", border_style="magenta"))
    
    target_folder = None
    collection_name = None
    password = None
    files_to_ignore_set = set()
    
    # --- LÓGICA DE CONTINUAÇÃO/SELEÇÃO DE RUNS (INALTERADO) ---
    past_runs = LogControlDB.get_past_runs_info(LOG_DB_PATH)
    
    # ... (Bloco de seleção de run/continuação - omitido para brevidade, mas permanece inalterado) ...
    if past_runs:
        # Lógica de seleção
        console.print(Panel("[bold yellow]🚨 Execuções Anteriores Encontradas (Modo Continuação)[/bold yellow]", border_style="yellow"))
        run_map = {}
        for i, (run_id, pc_name, col_name, r_path, start_time) in enumerate(past_runs):
            display_index = i + 1
            start_dt = datetime.fromisoformat(start_time).strftime('%Y-%m-%d %H:%M:%S')
            console.print(f"[bold magenta]{display_index}[/bold magenta]: [cyan]{col_name}[/cyan] (PC: {pc_name.split('-')[0]} | Pasta: {r_path} | Início: {start_dt})")
            run_map[str(display_index)] = {
                'run_id': run_id, 
                'collection_name': col_name, 
                'root_path': r_path
            }
            
        choice = Prompt.ask("Sua escolha (Número da Run / [bold cyan]N[/bold cyan]ova Run)", default="N")
        
        if choice.upper() in ['N', 'NOVA', 'NEW']:
            console.print("\n[bold green]Iniciando Nova Execução...[/bold green]")
            # Fall-through
        elif choice in run_map:
            selected_run = run_map[choice]
            collection_name = selected_run['collection_name']
            target_folder = selected_run['root_path']
            drive_letter = Path(target_folder).parts[0].replace('\\', '').replace(':', '')
            
            console.print(f"\n[bold green]✅ Modo Continuação Ativado.[/bold green]")
            console.print(f"📁 Pasta Alvo: [bold yellow]{target_folder}[/bold yellow]")
            console.print(f"📂 Coleção: [bold cyan]{collection_name}[/bold cyan]")
            
            password = Prompt.ask("🔑 Digite a Senha Mestra (Necessária para Key de Criptografia)", password=False)
            if not password.strip(): sys.exit(1)
                
            console.print(f"[info]Buscando arquivos já uploadados em {collection_name}/{drive_letter} para ignorar...[/info]")
            files_to_ignore_set = LogControlDB.get_uploaded_files_for_continuation(LOG_DB_PATH, collection_name, drive_letter)
            console.print(f"[success]✅ Encontrados {len(files_to_ignore_set)} arquivos para ignorar.[/success]")
        else:
            console.print("[error]Escolha inválida. Encerrando.[/error]")
            sys.exit(1)
    
    # 1. Seleção da Pasta (Apenas se Nova Execução)
    if target_folder is None:
        target_folder = select_folder_gui()
        if not target_folder: sys.exit(0)
            
        console.print(f"📁 Pasta Alvo: [bold yellow]{target_folder}[/bold yellow]")
        
        # 2. Entrada de Dados (Apenas se Nova Execução)
        collection_name = Prompt.ask("📂 Nome da Coleção/Bucket (ex: Backup-Trabalho)", default="Storage-Principal")
        collection_name = "".join(c for c in collection_name if c.isalnum() or c in ('-', '_')).strip()
        if not collection_name: collection_name = "Storage"

        password = Prompt.ask("🔑 Digite a Senha Mestra", password=False)
        if not password.strip(): sys.exit(1)
    
    # 3. Varredura e Alocação CENTRALIZADA (NOVO)
    all_files_on_disk = []
    files_to_process_raw = []
    
    try:
        console.print("[info]Iniciando varredura primária do disco...[/info]")
        
        # Coleta todos os arquivos
        for root, _, files in os.walk(target_folder):
            for f in files:
                full_path = Path(root) / f
                all_files_on_disk.append(full_path)
                
        # Filtra os arquivos que JÁ FORAM ENVIADOS (continuação)
        files_to_process_raw = [
            (fp, fp.stat().st_size) for fp in all_files_on_disk 
            if str(fp) not in files_to_ignore_set
        ]
        
        if not files_to_process_raw:
            console.print("[warning]Nenhum arquivo novo encontrado para upload (ou todos já foram enviados).[/warning]")
            sys.exit(0)
            
        ignored_by_continuation = len(all_files_on_disk) - len(files_to_process_raw)
        
        console.print(f"[success]✅ Varredura Completa. Total de Arquivos Pendentes: {len(files_to_process_raw)}.[/success]")
        console.print(f"[info]Skipping {ignored_by_continuation} arquivos já enviados (Continuação).[/info]")
        
        # --- NOVO: ALOCAÇÃO MULTI-HEAD ---
        manager = Manager()
        head_queues: Dict[int, MPQueue] = {i: manager.Queue() for i in range(1, NUM_PROCESSES + 1)}
        
        ignored_system_files = allocate_files_to_queues(
            files_to_process_raw, 
            head_queues
        )
        
        files_allocated_count = sum(q.qsize() for q in head_queues.values())
        console.print(f"[info]⚙️ Distribuindo {files_allocated_count} arquivos em {NUM_PROCESSES} Filas/Heads com Lógica de Prioridade...[/info]")
        
        # Adicionar os arquivos ignorados ao count final
        ignored_system_count = len(ignored_system_files)
        
    except Exception as e:
        console.print(Panel(f"[bold red]ERRO CRÍTICO NA VARREDURA E ALOCAÇÃO: [/bold red]{type(e).__name__}: {e}", title="Falha na Preparação", border_style="red"))
        sys.exit(1)


    # 4. Execução dos Processos
    processes = []
    global_metrics = manager.dict({'H_uploaded': 0, 'H_ignored': 0, 'H_failed': 0}) # Prefixo para agregação
    global_non_uploaded = manager.list() 
    
    try:
        for i in range(1, NUM_PROCESSES + 1):
            p = Process(target=process_worker_multi_head, args=(
                i, 
                collection_name, 
                target_folder, 
                password, 
                head_queues, 
                global_metrics,
                global_non_uploaded, 
            ))
            processes.append(p)
            p.start()
            
        # 5. Espera e Agregação
        for p in processes:
            p.join()

        # 6. Agregação final das métricas e Log Centralizado 
        final_metrics = {'uploaded': ignored_by_continuation, 'ignored': ignored_system_count, 'failed': 0}
        
        for key, value in global_metrics.items():
            if '_uploaded' in key:
                final_metrics['uploaded'] += value
            elif '_ignored' in key:
                final_metrics['ignored'] += value
            elif '_failed' in key:
                final_metrics['failed'] += value
                
        # Log de arquivos ignorados por sistema
        for fp, size in ignored_system_files:
            # Note: Não podemos usar o log_db de um processo, mas podemos incluir no relatório final.
            global_non_uploaded.append((str(fp), fp.name, 'IGNORED', 'SYSTEM_FILE_FILTER'))
                
        generate_markdown_log(
            socket.gethostname().lower().replace(' ', '-'), 
            collection_name, 
            target_folder, 
            final_metrics, 
            list(global_non_uploaded)
        )
        
        final_panel_content = (
            f"[bold green]OPERAÇÃO GLOBAL FINALIZADA! ({NUM_PROCESSES} HEADS)[/bold green]\n\n"
            f"📦 Coleção: [cyan]{collection_name}[/cyan]\n"
            f"✅ Uploads Concluídos: [success]{final_metrics['uploaded']}[/success] (Incluindo {ignored_by_continuation} da Continuação)\n"
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
