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
from typing import Optional, Tuple, Dict, Any, List, Union, Set, Callable
import sqlite3
from datetime import datetime
from multiprocessing import Process, Manager, Queue as MPQueue
from boto3.session import Config 
import uuid 

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.exceptions import InvalidTag 

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TransferSpeedColumn
from rich.prompt import Prompt
from rich.panel import Panel
from rich.theme import Theme

MEGA_S3_ENDPOINT = "XXX"
ACCESS_KEY = "XXX"
SECRET_KEY = "XXX"

SCAN_WORKERS = 8    
UPLOAD_WORKERS = 100 
MAX_THREADS = SCAN_WORKERS + UPLOAD_WORKERS

MPU_PART_SIZE = 8 * 1024 * 1024 
CHUNK_SIZE = MPU_PART_SIZE 

NONCE_SIZE = 8 
MAX_RETRIES = 100 
RETRY_DELAY = 1 

SCRIPT_DIR = Path(__file__).resolve().parent
KEY_FILE = SCRIPT_DIR / "masterkey.storagekey"

FORCED_IGNORE_FILENAMES = {
    'desktop.ini', 'thumbs.db', 'autorun.inf', 'pagefile.sys', 'hiberfil.sys',
    'ntuser.dat', 'iconcache.db', '$recycle.bin', 'system volume information',
    'swapfile.sys', 'recovery', 'config.msi'
}

FORCED_IGNORE_FOLDERS = {
    'games', 'jogos', 'steamapps', 'windows', 'arquivos de programas', 
    'program files', 'program files (x86)', 'rockstar games', 
    'appdata', 'temp', '$winreagent', 'msocache', '$windows.~bt', '$windows.~ws'
}

SCAN_MODE_FULL_DISK = 1
SCAN_MODE_MULTI_ORIGIN = 2

NUM_PROCESSES = 12 

VIDEO_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', 
    '.m4v', '.mpg', '.mpeg', '.3gp', '.ts', '.m2ts', '.vob'
}

# LIMITES DE TAMANHO CORRIGIDOS (incluindo 1GB e 100MB)
SIZE_100MB = 100 * 1024 * 1024
SIZE_1GB = 1 * 1024 * 1024 * 1024
SIZE_5GB = 5 * 1024 * 1024 * 1024

# NOVOS LIMITES DE TAMANHO (Small File Accelerator)
SIZE_10MB = 10 * 1024 * 1024
SIZE_1MB = 1 * 1024 * 1024
SIZE_100KB = 100 * 1024


HEAD_GROUPS = {
    # GRUPOS DEDICADOS A ARQUIVOS PEQUENOS (16 Heads, H9-H24)
    9: {'name': 'TINY_100KB_A', 'partner': 10, 'stealing': [11, 12]},
    10: {'name': 'TINY_100KB_B', 'partner': 9, 'stealing': [11, 12]},
    11: {'name': 'TINY_100KB_C', 'partner': 12, 'stealing': [9, 10]},
    12: {'name': 'TINY_100KB_D', 'partner': 11, 'stealing': [9, 10]},
    
    13: {'name': 'SMALL_1MB_A', 'partner': 14, 'stealing': [15, 16]},
    14: {'name': 'SMALL_1MB_B', 'partner': 13, 'stealing': [15, 16]},
    15: {'name': 'SMALL_1MB_C', 'partner': 16, 'stealing': [13, 14]},
    16: {'name': 'SMALL_1MB_D', 'partner': 15, 'stealing': [13, 14]},
    
    17: {'name': 'MED_10MB_A', 'partner': 18, 'stealing': [19, 20, 21, 22, 23, 24]},
    18: {'name': 'MED_10MB_B', 'partner': 17, 'stealing': [19, 20, 21, 22, 23, 24]},
    19: {'name': 'MED_10MB_C', 'partner': 20, 'stealing': [17, 18, 21, 22, 23, 24]},
    20: {'name': 'MED_10MB_D', 'partner': 19, 'stealing': [17, 18, 21, 22, 23, 24]},
    21: {'name': 'MED_10MB_E', 'partner': 22, 'stealing': [17, 18, 19, 20, 23, 24]},
    22: {'name': 'MED_10MB_F', 'partner': 21, 'stealing': [17, 18, 19, 20, 23, 24]},
    23: {'name': 'MED_10MB_G', 'partner': 24, 'stealing': [17, 18, 19, 20, 21, 22]},
    24: {'name': 'MED_10MB_H', 'partner': 23, 'stealing': [17, 18, 19, 20, 21, 22]},
    
    # GRUPOS ORIGINAIS (8 Heads, H1-H8) - AGORA APENAS PARA ARQUIVOS > 10MB
    1: {'name': 'TOP_A (>10M)', 'partner': 2, 'stealing': [8, 7, 5, 6, 4, 3]},
    2: {'name': 'TOP_B (>10M)', 'partner': 1, 'stealing': [8, 7, 5, 6, 4, 3]},
    3: {'name': 'VIDEO_SMALL (>10M)', 'partner': 4, 'stealing': [8, 7, 5, 6, 1, 2]},
    4: {'name': 'VIDEO_LARGE (>10M)', 'partner': 3, 'stealing': [8, 7, 5, 6, 1, 2]},
    5: {'name': 'GEN_MED_100MB (>10M)', 'partner': 6, 'stealing': [8, 7, 4, 3, 1, 2]},
    6: {'name': 'GEN_MED_1GB (>10M)', 'partner': 5, 'stealing': [8, 7, 4, 3, 1, 2]},
    7: {'name': 'GEN_BIG_5GB (>10M)', 'partner': 8, 'stealing': [5, 6, 4, 3, 1, 2]},
    8: {'name': 'GEN_HUGE (>5GB)', 'partner': 7, 'stealing': [5, 6, 4, 3, 1, 2]},
}

custom_theme = Theme({
    "info": "cyan", "warning": "yellow", "error": "bold red", "success": "bold green",
    "key": "bold magenta", "path": "dim white", "retry": "yellow on black", "alert": "bold yellow on red"
})
console = Console(theme=custom_theme)

LOG_DIR = Path("C:\\MEGA-SYNC-UPLODER-CONTROL-DATA")
LOG_DB_PATH = LOG_DIR / "control_data.db"

FileInfoTuple = Tuple[Path, int, str, str]

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
        
        self.active_encryptor = None
        self.active_iv = None
        self.active_pos = 0 

    def _setup_encryptor_for_seek(self, initial_offset: int):
        
        if initial_offset < 0 or initial_offset > self.total_size:
            raise ValueError("Offset fora dos limites do arquivo criptografado.")
            
        if initial_offset < self.header_size:
            self.active_iv = self.iv
            self.active_pos = initial_offset
            self.active_encryptor = None 
            self._f.seek(0) 
            return

        raw_offset = initial_offset - self.header_size 
        block_size = 16 
        block_index = raw_offset // block_size
        new_iv = bytearray(self.iv)
        temp_block_index = block_index
        for i in range(4):
            idx = 15 - i
            current_byte = self.iv[idx]
            increment = temp_block_index % 256
            
            new_val = current_byte + increment
            new_iv[idx] = new_val & 0xFF
            
            temp_block_index = (temp_block_index // 256) + (new_val // 256)
        
        self.active_iv = bytes(new_iv)
        
        cipher = Cipher(algorithms.AES(self.key), modes.CTR(self.active_iv), backend=default_backend())
        self.active_encryptor = cipher.encryptor()
        
        raw_seek_offset = raw_offset - (raw_offset % block_size)
        self._f.seek(raw_seek_offset)
        self.active_pos = initial_offset

        remaining_in_block = raw_offset % block_size
        if remaining_in_block > 0:
            discard_data = self._f.read(remaining_in_block)
            self.active_encryptor.update(discard_data)
            

    def read(self, size: int) -> bytes:
        if not self._f: return b''
        
        bytes_read = b''
        remaining_size = size

        while remaining_size > 0:
            
            if self.active_pos < self.header_size:
                remaining_header = self.header_size - self.active_pos
                read_len = min(remaining_size, remaining_header)
                
                chunk = self.header[self.active_pos : self.active_pos + read_len]
                self.active_pos += len(chunk)
                bytes_read += chunk
                remaining_size -= len(chunk)
                
                if self.active_pos == self.header_size:
                     self._setup_encryptor_for_seek(self.header_size)
                     if remaining_size == 0:
                         return bytes_read
                else:
                    return bytes_read
                
            else:
                data_to_read_raw = min(remaining_size, self.total_size - self.active_pos)
                
                if data_to_read_raw == 0:
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
                
                raw_data = self._f.read(data_to_read_raw)
                
                if not raw_data:
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
                
                if len(encrypted_data) < data_to_read_raw:
                    return bytes_read
                    
        return bytes_read

    def tell(self): return self.active_pos 
    
    def seek(self, offset, whence=0):
        if whence == 0:
            self._setup_encryptor_for_seek(offset)
            return self.active_pos
        elif whence == 1:
            return self.seek(self.active_pos + offset, 0)
        elif whence == 2:
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
        self.drive_letter = Path(root_path).parts[0].replace('\\', '').replace(':', '') if root_path and len(Path(root_path).parts) > 0 else 'UNKNOWN'
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
                status TEXT,
                s3_key TEXT,
                error_detail TEXT,
                FOREIGN KEY (run_id) REFERENCES run_log (run_id)
            )
        """)
        self.conn_main.commit()

    def _setup_run_log_entry(self):
        local_pc_name_with_head = f"{self.pc_name}-P{self.worker_id}"
        self.cursor_main.execute("""
            INSERT INTO run_log (
                pc_name, collection_name, root_path, drive_letter, timestamp_start, timestamp_end, 
                total_uploaded, total_ignored, total_failed
            )
            VALUES (?, ?, ?, ?, ?, NULL, 0, 0, 0)
        """, (
            local_pc_name_with_head, self.collection, self.root_path, self.drive_letter,
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

    @classmethod
    def get_past_runs_info(cls, db_path: Path) -> List[Tuple[int, str, str, str, str]]:
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

    def close(self):
        try:
            self.conn_main.close()
        except:
            pass

class UploadEngine:

    def __init__(self, collection_name, root_path, password, head_id: int):
        self.root_path = Path(root_path)
        self.collection = collection_name
        self.head_id = head_id 
        self.crypto = CryptoEngine()
        
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
        
        self.drive_letter = Path(root_path).parts[0].replace('\\', '').replace(':', '')
        
        self.log_db = LogControlDB(self.pc_name, root_path, self.collection, self.head_id)
        self.log_db.start_writer() 
        
        self.local_console = Console(theme=custom_theme)
        self._safe_print(f"[info]📊 HEAD {self.head_id} ({HEAD_GROUPS[self.head_id]['name']}) - Log DB ID: {self.log_db.run_id} | Arquivo: {LOG_DB_PATH.name}[/info]", style=None)
        
        self._ensure_bucket() 

        self.metrics = {'uploaded': 0, 'ignored': 0, 'failed': 0}
        
        self.upload_queue = queue.Queue()
        
        # ATOMIC MPU STATE: {s3_key: {'UploadId': str, 'Parts': List[Dict], 'OriginalPath': Path, 'FatalError': bool, 'EncryptedSize': int, 'TotalParts': int, 'CompletedPartsCount': int}}
        self.mpu_state: Dict[str, Dict[str, Any]] = {} 
        self.mpu_lock = threading.Lock()
        
        self.progress_task_id = None


    def _safe_print(self, text, style=None):
        try:
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
        max_attempts = 5
        retry_delay = 1
        
        for attempt in range(1, max_attempts + 1):
            try:
                self.s3.head_bucket(Bucket=self.clean_bucket_name)
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
                            return 
                        else:
                            raise create_e 
                
                raise e 
            
            except Exception as e:
                is_connection_error = isinstance(e, (ConnectionError, ReadTimeoutError)) or 'Connection was closed' in str(e)
                
                if is_connection_error and attempt < max_attempts:
                    self._safe_print(f"[ALERTA] Falha de Conexão no Bucket (Tentativa {attempt}/{max_attempts}): {type(e).__name__}. Esperando {retry_delay}s...", style="retry")
                    time.sleep(retry_delay)
                    continue
                
                self._safe_print(f"Erro fatal verificando/criando bucket após {attempt} tentativas: {type(e).__name__}: {e}", style="error")
                self.log_db.close()
                sys.exit(1)
        
        self._safe_print(f"Erro fatal: Falha ao verificar/criar bucket {self.clean_bucket_name} após {max_attempts} tentativas.", style="error")
        self.log_db.close()
        sys.exit(1)


    def _calculate_content_hash(self, file_path: Path):
        try:
            stat_info = file_path.stat()
            file_size = stat_info.st_size
            mod_time = stat_info.st_mtime_ns
            
            rel_path = self._get_original_path_metadata(file_path)
            
            data_to_hash = f"{rel_path}-{file_size}-{mod_time}".encode('utf-8')

            hasher = hashlib.sha512()
            hasher.update(data_to_hash)

            return hasher.hexdigest()
            
        except Exception as e:
            self._safe_print(f"[ERRO HASH] Falha ao ler metadados para {file_path.name}: {e}", style="error")
            return None

    def _get_original_path_metadata(self, local_path: Path):
        try:
            rel_path = local_path.relative_to(self.root_path).as_posix()
        except ValueError:
            rel_path = local_path.name.as_posix()
            
        return rel_path 

    def _create_s3_key(self, content_hash: str):
        nonce = os.urandom(NONCE_SIZE)
        nonce_b64 = base64.urlsafe_b64encode(nonce).decode('utf-8').rstrip('=')
        return f"{content_hash}-{nonce_b64}"

    def _abort_mpu(self, s3_key: str):
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
                if s3_key in self.mpu_state:
                    self.mpu_state[s3_key]['FatalError'] = True
                    self.mpu_state[s3_key]['Status'] = 'ABORTED'

    def _scan_and_hash_worker_multi_head(self, file_path_tuple: Tuple[str, int, str, str]):
        fp = Path(file_path_tuple[0])
        file_size_original = file_path_tuple[1]
        pc_name = file_path_tuple[2]
        drive_letter = file_path_tuple[3]
        s3_key_unique = None
        
        try:
            file_size = fp.stat().st_size
            
            if file_size == 0:
                self._safe_print(f"[VAZIO] Ignorando {fp.name}: Arquivo vazio.", style="warning")
                self.log_db.log_file_status(fp, 'IGNORED', 0, s3_key=None, error_detail="ZERO_BYTE_SIZE")
                self.metrics['ignored'] += 1
                return 0
            
            original_metadata_path = Path(pc_name) / drive_letter / self._get_original_path_metadata(fp)
            content_hash = self._calculate_content_hash(fp)
            if not content_hash: 
                self.log_db.log_file_status(fp, 'IGNORED', file_size, s3_key=None, error_detail="HASH_METADATA_ERROR")
                self.metrics['ignored'] += 1
                return 0
            
            s3_key_unique = self._create_s3_key(content_hash)
            
            with self.crypto.create_encrypted_stream(fp, original_metadata_path.as_posix()) as temp_stream:
                encrypted_size = temp_stream.total_size
            
            response = self.s3.create_multipart_upload(
                Bucket=self.clean_bucket_name,
                Key=s3_key_unique,
            )
            upload_id = response['UploadId']
            
            num_parts = (encrypted_size + MPU_PART_SIZE - 1) // MPU_PART_SIZE
            
            with self.mpu_lock:
                self.mpu_state[s3_key_unique] = {
                    'UploadId': upload_id,
                    'Parts': [], 
                    'OriginalFileSize': file_size,
                    'OriginalPath': fp,
                    'FatalError': False,
                    'EncryptedSize': encrypted_size,
                    'TotalParts': num_parts,
                    'CompletedPartsCount': 0,
                    'HeadID': self.head_id,
                    'PCName': pc_name,
                    'DriveLetter': drive_letter,
                    'Status': 'ACTIVE'
                }
            
            current_offset = 0
            part_number = 1
            while current_offset < encrypted_size:
                chunk_length = min(MPU_PART_SIZE, encrypted_size - current_offset)
                
                self.upload_queue.put((
                    s3_key_unique, upload_id, part_number, 
                    current_offset, chunk_length, fp, file_size
                ))
                
                current_offset += chunk_length
                part_number += 1
                    
            return encrypted_size
            
        except Exception as e:
            safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
            self._safe_print(f"[ERRO PROCESSO] Ignorando {fp.name}: {type(e).__name__}: {safe_error}", style="warning")
            
            self.log_db.log_file_status(fp, 'IGNORED', file_size_original, s3_key=s3_key_unique, error_detail=f"PROCESSING_ERROR: {type(e).__name__}: {safe_error}")
            self.metrics['ignored'] += 1
            
            if s3_key_unique:
                self._abort_mpu(s3_key_unique)
                
            return 0


    def _upload_worker(self, progress: Progress, overall_task: int):
        
        while True:
            try:
                item: Optional[Tuple] = self.upload_queue.get(timeout=1)
            except queue.Empty:
                continue

            if item is None:
                self.upload_queue.put(None)
                break
                
            s3_key, upload_id, part_number, offset, length, file_path, file_size = item
            
            etag = None
            chunk_data = None

            try:
                with self.mpu_lock:
                    mpu_info = self.mpu_state.get(s3_key)
                    if not mpu_info or mpu_info.get('FatalError') or mpu_info.get('Status') != 'ACTIVE':
                        self.upload_queue.task_done()
                        continue
                    
                    original_path_metadata = mpu_info['OriginalPath'].as_posix()
                    pc_name_meta = mpu_info['PCName']
                    drive_letter_meta = mpu_info['DriveLetter']
                
                full_metadata_path = Path(pc_name_meta) / drive_letter_meta / self._get_original_path_metadata(file_path)

                # 2. Leitura Just-In-Time (JIT) do Chunk
                with self.crypto.create_encrypted_stream(file_path, full_metadata_path.as_posix()) as stream:
                    stream.seek(offset)
                    chunk_data = stream.read(length) 

                if chunk_data is None or len(chunk_data) != length:
                    self._safe_print(f"[ERROR] ERRO READ: Falha ao ler {length} bytes de {file_path.name}. Abortando MPU.", style="error")
                    self._abort_mpu(s3_key)
                    self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail="READ_FAIL_DISK_IO")
                    self.metrics['failed'] += 1
                    self.upload_queue.task_done()
                    continue


                # 3. Tentativas de Upload
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        if attempt > 1:
                            self._safe_print(f"[RETRY] Retry {attempt}/{MAX_RETRIES} para {file_path.name} Part {part_number}...", style="retry")

                        response = self.s3.upload_part(
                            Bucket=self.clean_bucket_name,
                            Key=s3_key,
                            UploadId=upload_id,
                            PartNumber=part_number,
                            Body=chunk_data
                        )
                        etag = response['ETag']
                        break 
                        
                    except (ClientError, ConnectionError, ReadTimeoutError) as e:
                        
                        error_code = 'Unknown'
                        if isinstance(e, ClientError):
                            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                            
                            if error_code in ['NoSuchUpload', 'InvalidPart']: 
                                with self.mpu_lock:
                                    if s3_key not in self.mpu_state or self.mpu_state[s3_key].get('Status') != 'ACTIVE':
                                        self._safe_print(f"⚠️ Alerta (Stale Part): Parte {part_number} de {file_path.name} ignorada (concluído/abortado).", style="warning")
                                        self.upload_queue.task_done()
                                        return
                                    
                                    self._safe_print(f"[FIM] {file_path.name}: Upload cancelado ({error_code} detectado).", style="error")
                                    self.mpu_state[s3_key]['FatalError'] = True
                                    self.mpu_state[s3_key]['Status'] = 'ABORTED'
                                    self._abort_mpu(s3_key)
                                    self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"PART_FATAL_S3_STATE: {error_code}")
                                    self.metrics['failed'] += 1
                                    self.upload_queue.task_done()
                                    return
                            
                        if attempt < MAX_RETRIES:
                            time.sleep(RETRY_DELAY) 
                        else:
                            raise 
                            

                # 4. ATUALIZA O PROGRESS BAR 
                progress.update(overall_task, advance=length)
                
                # 5. ATOMIC CHAIN / LAST MAN STANDING
                with self.mpu_lock:
                    mpu_info = self.mpu_state.get(s3_key)
                    if not mpu_info or mpu_info.get('FatalError') or mpu_info.get('Status') != 'ACTIVE':
                        self.upload_queue.task_done()
                        continue
                         
                    mpu_info['Parts'].append({
                        'PartNumber': part_number,
                        'ETag': etag
                    })
                    mpu_info['CompletedPartsCount'] += 1
                    
                    if mpu_info['CompletedPartsCount'] == mpu_info['TotalParts']:
                        
                        mpu_info['Status'] = 'COMPLETING'
                        parts = sorted(mpu_info['Parts'], key=lambda x: x['PartNumber'])
                        
                        try:
                            # Executa o Complete (AINDA DENTRO DO LOCK)
                            self.s3.complete_multipart_upload(
                                Bucket=self.clean_bucket_name,
                                Key=s3_key,
                                UploadId=upload_id,
                                MultipartUpload={'Parts': parts}
                            )
                            
                            self._safe_print(f"✅ Upload CONCLUÍDO (Atomic Chain). Key: {s3_key[:16]}... | File: {file_path.name}", style="success")
                            self.log_db.log_file_status(file_path, 'UPLOADED', file_size, s3_key=s3_key)
                            self.metrics['uploaded'] += 1
                            del self.mpu_state[s3_key]
                            
                        except Exception as completion_e:
                            safe_completion_error = str(completion_e).encode('utf-8', 'ignore').decode('utf-8')
                            self._safe_print(f"❌ FALHA NA FINALIZAÇÃO (COMPLETION): {file_path.name}: {safe_completion_error}. ABORTANDO.", style="error")
                            
                            mpu_info['FatalError'] = True
                            mpu_info['Status'] = 'FAILED'
                            self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"COMPLETION_FAIL: {safe_completion_error}")
                            self.metrics['failed'] += 1
                            self._abort_mpu(s3_key)
                            
                    
            except Exception as e:
                safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
                
                is_fatal_error = ("FatalError" in safe_error or "NoSuchUpload" in safe_error or "InvalidPart" in safe_error)
                
                if not is_fatal_error:
                    self._safe_print(f"⚠️ RE-ENFILEIRANDO (PARTE {part_number}): {file_path.name}: {safe_error}", style="warning")
                    self.upload_queue.put(item)
                else:
                    self._safe_print(f"❌ FALHA FATAL (PARTE {part_number}): {file_path.name}: {safe_error}", style="error")
                    
                    if mpu_info:
                        self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"PART_FATAL: {safe_error}")
                        self.metrics['failed'] += 1
                        self._abort_mpu(s3_key)
                        
                        try:
                            progress.update(overall_task, advance=mpu_info['EncryptedSize'])
                        except Exception:
                            pass 

            finally:
                self.upload_queue.task_done()
                

    def run_multi_head(self, file_queue: MPQueue):
        scanner_futures = []
        
        scanner_executor = ThreadPoolExecutor(max_workers=SCAN_WORKERS)
        uploader_executor = ThreadPoolExecutor(max_workers=UPLOAD_WORKERS)
        
        self._safe_print(f"[bold]HEAD {self.head_id} Iniciado: Produzindo chunks no Worker Pool...[/bold]", style=None)
        
        for i in range(UPLOAD_WORKERS):
            uploader_executor.submit(self._upload_worker, self.progress_tracker, self.progress_task_id)

        while True:
            try:
                file_info: FileInfoTuple = file_queue.get(timeout=30)
            except queue.Empty:
                break
            except Exception as e:
                self._safe_print(f"[ERRO HEAD] Falha na fila de trabalho: {e}", style="error")
                break
                
            scanner_futures.append(
                scanner_executor.submit(self._scan_and_hash_worker_multi_head, file_info)
            )

        self._safe_print("[info]Fila Primária (ou Stealing) drenada. Aguardando conclusão de hashing/chunking...[/info]")
        
        total_bytes_encrypted = 0
        for future in as_completed(scanner_futures):
            try:
                encrypted_size = future.result()
                total_bytes_encrypted += encrypted_size
                self.progress_tracker.update(self.progress_task_id, total=self.progress_tracker.tasks[self.progress_task_id].total + encrypted_size)
            except Exception:
                pass 
                
        scanner_executor.shutdown()
        
        self._safe_print(f"[info]Chunking Concluído. Total de Bytes a Upados (Chunks): {humanize.naturalsize(total_bytes_encrypted)}[/info]", style=None)
        self.upload_queue.join() 
        
        for _ in range(UPLOAD_WORKERS):
            self.upload_queue.put(None)
        
        uploader_executor.shutdown() 
        
        with self.mpu_lock:
            for s3_key in list(self.mpu_state.keys()):
                if self.mpu_state[s3_key].get('Status') == 'ACTIVE' or self.mpu_state[s3_key].get('Status') == 'COMPLETING':
                    self._safe_print(f"[ALERTA] MPU Incompleto: {self.mpu_state[s3_key]['OriginalPath'].name}. Abortando e logando falha.", style="warning")
                    self.log_db.log_file_status(self.mpu_state[s3_key]['OriginalPath'], 'FAILED', self.mpu_state[s3_key]['OriginalFileSize'], s3_key=s3_key, error_detail="INCOMPLETE_ABORT_AT_END")
                    self.metrics['failed'] += 1
                    self._abort_mpu(s3_key)
                
                if s3_key in self.mpu_state: del self.mpu_state[s3_key]
                        
        self.log_db.finalize_run(self.metrics) 

    
    def set_progress_tracker(self, progress: Progress, task_id: Any):
        self.progress_tracker = progress
        self.progress_task_id = task_id
        
def is_folder_ignored(path_segment: str) -> bool:
    return path_segment.lower() in FORCED_IGNORE_FOLDERS

def generate_markdown_log(pc_name, collection_name, root_path, final_metrics, non_uploaded_files):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_name = LOG_DIR / f"log_summary_{pc_name}_{timestamp}.md"
    
    with open(log_file_name, 'w', encoding='utf-8') as f:
        f.write(f"# Relatório de Upload - Coleção: {collection_name}\n\n")
        f.write(f"**PC Local de Execução:** {socket.gethostname()}\n")
        f.write(f"**Pasta Raiz Analisada:** {root_path}\n")
        f.write(f"**Data/Hora da Conclusão:** {timestamp.replace('_', ' ')}\n")
        f.write(f"**Coleção MEGA/S3:** {collection_name}\n\n")
        
        f.write("## Resumo Global\n")
        f.write(f"- **Uploads Concluídos:** {final_metrics['uploaded']}\n")
        f.write(f"- **Arquivos Ignorados:** {final_metrics['ignored']}\n")
        f.write(f"- **Falhas Fatais:** {final_metrics['failed']}\n\n")
        
        f.write("## Detalhes de Arquivos Não Uploadados/Ignorados\n")
        f.write("| Status | Arquivo | Motivo/Detalhe |\n")
        f.write("| :--- | :--- | :--- |\n")
        
        for fp, fn, status, detail in non_uploaded_files:
            detail_safe = str(detail).replace('\n', ' ').replace('|', '/')
            if status == 'IGNORED_FOLDER':
                f.write(f"| {status} | {fp} (Pasta) | {detail_safe} |\n")
            else:
                f.write(f"| {status} | {fp} | {detail_safe} |\n")

    console.print(f"[success]✅ Relatório final de execução salvo em: [path]{log_file_name.name}[/path][/success]")

def select_folder_gui():
    root = tk.Tk()
    root.withdraw() 
    folder_selected = filedialog.askdirectory()
    return folder_selected

def scan_and_orchestrate_files(
    target_folder: str, 
    scan_mode: int,
    files_to_ignore_set: Set[str]
) -> Tuple[Dict[Tuple[str, str], List[FileInfoTuple]], List[Tuple[Path, int]], List[str]]:
    
    base_path = Path(target_folder)
    all_files_on_disk: List[Path] = []
    ignored_system_files: List[Tuple[Path, int]] = []
    ignored_folders_list: List[str] = []
    
    console.print("[info]Iniciando varredura primária do disco e aplicando filtros de sistema...[/info]")
    
    for root, dirs, files in os.walk(base_path, topdown=True):
        current_path = Path(root)
        
        try:
            relative_to_base_parts = current_path.relative_to(base_path).parts
        except ValueError:
            relative_to_base_parts = ()
            
        is_root_level_for_full_disk = (scan_mode == SCAN_MODE_FULL_DISK) and (len(relative_to_base_parts) <= 1)
        is_root_level_for_multi_origin = (scan_mode == SCAN_MODE_MULTI_ORIGIN) and (len(relative_to_base_parts) <= 2)
            
        dirs_to_remove = []
        for d in dirs:
            if is_folder_ignored(d) and (is_root_level_for_full_disk or is_root_level_for_multi_origin):
                ignored_folders_list.append(str(current_path / d))
                dirs_to_remove.append(d)
                
        for d_rem in dirs_to_remove:
            if d_rem in dirs:
                dirs.remove(d_rem)
            
        
        for f in files:
            file_path = current_path / f
            
            if file_path.name.lower() in FORCED_IGNORE_FILENAMES or file_path.name == KEY_FILE.name:
                try:
                    size = file_path.stat().st_size
                    ignored_system_files.append((file_path, size))
                except Exception:
                    ignored_system_files.append((file_path, 0))
                continue
            
            if str(file_path) in files_to_ignore_set:
                try:
                    size = file_path.stat().st_size
                    ignored_system_files.append((file_path, size))
                except Exception:
                    ignored_system_files.append((file_path, 0))
                continue
                
            all_files_on_disk.append(file_path)

    
    file_groups: Dict[Tuple[str, str], List[FileInfoTuple]] = {}
    
    current_pc_name_local = socket.gethostname().lower().replace(' ', '-')
    
    for fp in all_files_on_disk:
        try:
            size = fp.stat().st_size
        except Exception:
            ignored_system_files.append((fp, 0))
            continue
            
        pc_name = ""
        drive_letter = ""
        
        if scan_mode == SCAN_MODE_FULL_DISK:
            pc_name = current_pc_name_local
            drive_letter = base_path.parts[0].replace('\\', '').replace(':', '') if base_path.parts else 'ROOT'
            if not drive_letter: drive_letter = 'ROOT'
            
            group_key = (pc_name, drive_letter)
            
        elif scan_mode == SCAN_MODE_MULTI_ORIGIN:
            
            try:
                rel_path_parts = fp.relative_to(base_path).parts
                if len(rel_path_parts) >= 2:
                    pc_name = rel_path_parts[0].replace(' ', '-').replace('/', '_').strip()
                    drive_letter_raw = rel_path_parts[1]
                    
                    if '_' in drive_letter_raw:
                         drive_letter = drive_letter_raw.split('_')[0]
                    else:
                         drive_letter = drive_letter_raw
                    
                    drive_letter = drive_letter.replace(' ', '-').replace('/', '_').strip() 
                    if not pc_name or not drive_letter: raise ValueError("Estrutura Multi-Origin Inválida")
                         
                    group_key = (pc_name, drive_letter)
                    
                else:
                    pc_name = "ROOT_UNKNOWN"
                    drive_letter = "UNKNOWN"
                    group_key = (pc_name, drive_letter)
                    
            except Exception:
                ignored_system_files.append((fp, size))
                continue
        
        if group_key not in file_groups:
            file_groups[group_key] = []
            
        file_groups[group_key].append((fp, size, pc_name, drive_letter))
    
    
    return file_groups, ignored_system_files, ignored_folders_list

def orchestrate_and_queue_hierarchical(
    file_groups: Dict[Tuple[str, str], List[FileInfoTuple]],
    head_queues: Dict[int, MPQueue]
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    
    group_sizes = {key: sum(f[1] for f in files) for key, files in file_groups.items()}
    sorted_groups = sorted(group_sizes.items(), key=lambda item: item[1], reverse=False)
    
    total_files_allocated = 0
    total_size_allocated = 0
    group_report = []
    
    for (pc_name, drive_letter), total_size in sorted_groups:
        
        files_for_allocation = file_groups[(pc_name, drive_letter)]
        
        ignored_files_from_v14 = allocate_files_to_queues(
            files_for_allocation, 
            head_queues
        )
        
        num_files_allocated = len(files_for_allocation) - len(ignored_files_from_v14)
        total_files_allocated += num_files_allocated
        total_size_allocated += total_size
        
        console.print(f"[success]✅ Grupo {pc_name}/{drive_letter} ({humanize.naturalsize(total_size)}) alocado: {num_files_allocated} arquivos distribuídos nas Heads.[/success]")
        
        group_report.append({
            'pc_name': pc_name,
            'drive_letter': drive_letter,
            'total_size': total_size,
            'files_allocated': num_files_allocated
        })


    report_metrics = {
        'total_files_scanned': sum(len(f) for f in file_groups.values()),
        'total_files_allocated': total_files_allocated,
        'total_size_allocated': total_size_allocated
    }
    
    return report_metrics, group_report
    
def allocate_files_to_queues(
    pending_files: List[FileInfoTuple],
    queues: Dict[int, MPQueue]
) -> List[FileInfoTuple]:
    
    files_to_process_info: List[Tuple[Path, int, str, str, str]] = []
    
    for fp, size, pc_name, drive_letter in pending_files:
        files_to_process_info.append((fp, size, pc_name, drive_letter, os.path.splitext(fp.name)[1].lower()))
        
    # Separar arquivos pequenos (<= 10MB) e grandes (> 10MB)
    small_files = [f for f in files_to_process_info if f[1] <= SIZE_10MB]
    large_files = [f for f in files_to_process_info if f[1] > SIZE_10MB]
    
    # --- 1. ALOCAÇÃO DE ARQUIVOS PEQUENOS (HYPER-PARALLEL ACCELERATOR) ---
    small_files.sort(key=lambda x: x[1], reverse=False) # Smallest-First
    
    tiny_100kb = [f for f in small_files if f[1] <= SIZE_100KB]
    small_1mb = [f for f in small_files if SIZE_100KB < f[1] <= SIZE_1MB]
    med_10mb = [f for f in small_files if SIZE_1MB < f[1] <= SIZE_10MB]
    
    # Distribuição Cíclica (Round-Robin) nos grupos dedicados
    tiny_heads = [9, 10, 11, 12]
    small_heads = [13, 14, 15, 16]
    med_heads = [17, 18, 19, 20, 21, 22, 23, 24]
    
    for i, file_info in enumerate(tiny_100kb):
        queues[tiny_heads[i % len(tiny_heads)]].put((file_info[0].as_posix(), file_info[1], file_info[2], file_info[3]))
        
    for i, file_info in enumerate(small_1mb):
        queues[small_heads[i % len(small_heads)]].put((file_info[0].as_posix(), file_info[1], file_info[2], file_info[3]))
        
    for i, file_info in enumerate(med_10mb):
        queues[med_heads[i % len(med_heads)]].put((file_info[0].as_posix(), file_info[1], file_info[2], file_info[3]))
        
    
    # --- 2. ALOCAÇÃO DE ARQUIVOS GRANDES (> 10MB) ---
    large_files.sort(key=lambda x: x[1], reverse=True) 
    
    top_1000 = large_files[:1000]
    remainder = large_files[1000:]
    
    temp_buckets = {1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 7: [], 8: []}
    
    # Top 1000 (H1 e H2)
    top_1000_a = top_1000[:500]
    top_1000_b = top_1000[500:]

    temp_buckets[1].extend(top_1000_a)
    temp_buckets[2].extend(top_1000_b)
        
    # Restante (H3 a H8)
    for fp, size, pc_name, drive_letter, ext in remainder:
        if ext in VIDEO_EXTENSIONS:
            if size <= SIZE_1GB:
                temp_buckets[3].append((fp, size, pc_name, drive_letter, ext))
            else:
                temp_buckets[4].append((fp, size, pc_name, drive_letter, ext))
        else:
            if size <= SIZE_100MB:
                temp_buckets[5].append((fp, size, pc_name, drive_letter, ext))
            elif size <= SIZE_1GB:
                temp_buckets[6].append((fp, size, pc_name, drive_letter, ext)) 
            elif size <= SIZE_5GB:
                temp_buckets[7].append((fp, size, pc_name, drive_letter, ext)) 
            else:
                temp_buckets[8].append((fp, size, pc_name, drive_letter, ext)) 
                
    # Enfileiramento: Smallest-First em cada Head (H1-H8)
    for head_id, files in temp_buckets.items():
        if not files: continue
        
        files.sort(key=lambda x: x[1], reverse=False)
        
        for fp, size, pc_name, drive_letter, _ in files:
             queues[head_id].put((fp.as_posix(), size, pc_name, drive_letter))
                
    return []

def process_worker_multi_head(
    head_id: int, 
    collection_name: str, 
    root_path: str, 
    password: str, 
    head_queues: Dict[int, MPQueue],
    global_metrics_dict: Dict[str, Any],
    global_non_uploaded_list: List[Tuple], 
):
    
    local_console = Console(theme=custom_theme)
    engine = None
    
    try:
        engine = UploadEngine(collection_name, root_path, password, head_id) 
        
        with Progress(
            TextColumn(f"[bold blue]HEAD {head_id} - [/bold blue]"+"{task.description}"), 
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            console=engine.local_console
        ) as progress:
            
            overall_task = progress.add_task(f"[green]Fase 2: Uploads MPU Ativos ({UPLOAD_WORKERS} Workers)...", total=1)
            engine.set_progress_tracker(progress, overall_task)
            
            current_target_id = head_id
            
            while True:
                current_queue = head_queues[current_target_id]
                
                engine._safe_print(f"[info]Atacando fila {current_target_id} ({HEAD_GROUPS[current_target_id]['name']})...[/info]", style=None)
                
                engine.run_multi_head(current_queue)
                
                if current_target_id == head_id and current_queue.empty():
                    engine._safe_print(f"[success]Fila Primária (HEAD {head_id}) drenada.[/success]")
                    
                    partner_id = HEAD_GROUPS[head_id]['partner']
                    if partner_id in head_queues and not head_queues[partner_id].empty():
                        current_target_id = partner_id
                        continue
                    
                    engine._safe_print(f"[warning]Grupo esgotado. Iniciando MUTATION (Inter-Group Stealing)...[/warning]")
                    
                    stolen_target_id = None
                    for target_id in HEAD_GROUPS[head_id]['stealing']:
                        if target_id in head_queues and not head_queues[target_id].empty():
                            stolen_target_id = target_id
                            break
                            
                    if stolen_target_id is not None:
                        current_target_id = stolen_target_id
                        continue
                        
                    engine._safe_print("[bold green]FIM ABSOLUTO: Todas as filas acessíveis drenadas.[/bold green]", style=None)
                    break 
                
                elif current_target_id != head_id:
                    engine._safe_print(f"[success]Fila Roubada ({current_target_id}) drenada. Voltando para o ciclo de Stealing...[/success]")
                    current_target_id = head_id 
                    continue
                
                else:
                    engine._safe_print(f"[error]Erro na lógica do loop de Stealing. Fila {head_id} não esvaziada, mas loop parou.[/error]")
                    break

        local_metrics = engine.metrics
        local_non_uploaded = engine.log_db.get_run_metrics() 
        
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
    console.print(Panel.fit("[bold white on magenta] MEGA CRYPTO HASH UPLOADER (V20.1.1 - HYPER-PARALLEL ACCELERATOR) [/bold white on magenta]", border_style="magenta"))
    
    target_folder = None
    collection_name = None
    password = None
    files_to_ignore_set = set()
    scan_mode = None
    processes = []  # CORREÇÃO 2: Inicialização de 'processes'
    
    # 1. Seleção do Modo de Operação (Scan)
    console.print("\nEscolha o Modo de Operação (Scan):")
    console.print(f"**{SCAN_MODE_FULL_DISK}**: Full Disk/Pasta Única (PC Local) [bold magenta]<- Modo Padrão[/bold magenta]")
    console.print(f"**{SCAN_MODE_MULTI_ORIGIN}**: Multi-Origin Backup (Pasta contém backups de vários PCs/Drives)")
    
    try:
        mode_choice = Prompt.ask("Sua escolha (1 ou 2)", default='1')
        scan_mode = int(mode_choice)
        if scan_mode not in [SCAN_MODE_FULL_DISK, SCAN_MODE_MULTI_ORIGIN]:
            raise ValueError
    except ValueError:
        console.print("[error]Escolha de modo inválida. Usando Full Disk.[/error]")
        scan_mode = SCAN_MODE_FULL_DISK
    
    past_runs = LogControlDB.get_past_runs_info(LOG_DB_PATH)
    
    if past_runs:
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
        elif choice in run_map:
            selected_run = run_map[choice]
            collection_name = selected_run['collection_name']
            target_folder = selected_run['root_path']
            drive_letter = Path(target_folder).parts[0].replace('\\', '').replace(':', '') if target_folder and len(Path(target_folder).parts) > 0 else 'UNKNOWN'
            
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
    
    # 2. Seleção da Pasta e Entrada de Dados (Apenas se Nova Execução)
    if target_folder is None:
        target_folder = select_folder_gui()
        if not target_folder: sys.exit(0)
            
        console.print(f"📁 Pasta Alvo: [bold yellow]{target_folder}[/bold yellow]")
        
        collection_name = Prompt.ask("📂 Nome da Coleção/Bucket (ex: Backup-Trabalho)", default="Storage-Principal")
        collection_name = "".join(c for c in collection_name if c.isalnum() or c in ('-', '_')).strip()
        if not collection_name: collection_name = "Storage"

        password = Prompt.ask("🔑 Digite a Senha Mestra", password=False)
        if not password.strip(): sys.exit(1)
    
    # 3. Varredura e Alocação CENTRALIZADA
    current_pc_name = socket.gethostname().lower().replace(' ', '-')
    
    try:
        file_groups, ignored_system_files, ignored_folders_list = scan_and_orchestrate_files(
            target_folder, 
            scan_mode, 
            files_to_ignore_set
        )
        
        if ignored_folders_list:
            console.print(f"\n[alert]❌ PASTAS IGNORADAS PELO FILTRO DE SISTEMA ({len(ignored_folders_list)}):[/alert]")
            for path in ignored_folders_list[:10]:
                console.print(f"  - [warning]{path}[/warning]")
        
        ignored_by_continuation = len(files_to_ignore_set)

        if not file_groups:
            console.print("[warning]Nenhum arquivo novo encontrado para upload após filtros.[/warning]")
            global_non_uploaded = []
            for fp, size in ignored_system_files:
                error_detail = 'SYSTEM_FILE_FILTER' if str(fp) not in files_to_ignore_set else 'CONTINUATION_IGNORE'
                global_non_uploaded.append((str(fp), Path(fp).name, 'IGNORED', error_detail))
            
            final_metrics = {'uploaded': 0, 'ignored': len(ignored_system_files) + len(ignored_folders_list), 'failed': 0}
            generate_markdown_log(current_pc_name, collection_name, target_folder, final_metrics, global_non_uploaded)
            sys.exit(0)
            
        manager = Manager()
        head_queues: Dict[int, MPQueue] = {i: manager.Queue() for i in range(1, NUM_PROCESSES + 1)}
        
        console.print(f"[info]⚙️ Iniciando Orquestração Hierárquica e Alocação (Scan Mode: {scan_mode})...[/info]")
        
        orchestration_metrics, group_report = orchestrate_and_queue_hierarchical(
            file_groups, 
            head_queues
        )
        
        console.print("\n--- Distribuição Final ---")
        console.print(f"Total de Fontes (PC/Drive): [bold]{len(group_report)}[/bold]")
        console.print(f"Total de Arquivos Alocados: [bold green]{orchestration_metrics['total_files_allocated']}[/bold green]")
        console.print(f"Total de Dados Alocados: [bold cyan]{humanize.naturalsize(orchestration_metrics['total_size_allocated'])}[/bold cyan]")
        console.print("--------------------------\n")
        
        console.print(Panel(f"[bold yellow]ATENÇÃO: {NUM_PROCESSES - 8} Heads ({UPLOAD_WORKERS * (NUM_PROCESSES - 8)} Workers) dedicados a arquivos < 10MB! 🚀[/bold yellow]", border_style="yellow"))

        # 4. Inicia Processos
        processes = []
        global_metrics = manager.dict() 
        global_non_uploaded = manager.list() 
        
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
        
        
        for p in processes:
            p.join()

        # 5. Agregação final (Lendo o DB para métricas finais globais)
        
        total_uploaded = sum(global_metrics.get(f'H{i}_uploaded', 0) for i in range(1, NUM_PROCESSES + 1))
        total_failed = sum(global_metrics.get(f'H{i}_failed', 0) for i in range(1, NUM_PROCESSES + 1))
        total_ignored_worker = sum(global_metrics.get(f'H{i}_ignored', 0) for i in range(1, NUM_PROCESSES + 1))
        
        final_metrics = {
            'uploaded': total_uploaded + ignored_by_continuation,
            'failed': total_failed,
            'ignored': total_ignored_worker + len(ignored_system_files) + len(ignored_folders_list),
        }

        final_non_uploaded_report = list(global_non_uploaded)
        
        for fp, size in ignored_system_files:
            error_detail = 'SYSTEM_FILE_FILTER'
            if str(fp) in files_to_ignore_set:
                 error_detail = 'CONTINUATION_IGNORE'
            if not any(item[0] == str(fp) for item in final_non_uploaded_report):
                final_non_uploaded_report.append((str(fp), Path(fp).name, 'IGNORED', error_detail))
                
        for folder_path in ignored_folders_list:
            final_non_uploaded_report.append((folder_path, Path(folder_path).name, 'IGNORED_FOLDER', 'SYSTEM_FOLDER_FILTER'))
        
        
        generate_markdown_log(
            current_pc_name, 
            collection_name, 
            target_folder, 
            final_metrics, 
            final_non_uploaded_report
        )
        
        final_panel_content = (
            f"[bold green]OPERAÇÃO GLOBAL FINALIZADA! ({NUM_PROCESSES} HEADS)[/bold green]\n\n"
            f"📦 Coleção: [cyan]{collection_name}[/cyan]\n"
            f"✅ Uploads Concluídos: [success]{final_metrics['uploaded']}[/success]\n"
            f"🚫 Arquivos Ignorados: [warning]{final_metrics['ignored']}[/warning]\n"
            f"❌ Falhas Fatais (Retry Esgotado/MPU Abortado): [error]{final_metrics['failed']}[/error]\n\n"
            "Relatório detalhado salvo em arquivo .md."
        )
        console.print(Panel(final_panel_content, title="[bold blue]Conclusão Global do Processo[/bold blue]"), style=None)
        
    except Exception as e:
        console.print(Panel(f"[bold red]ERRO CRÍTICO NO CONTROLADOR DE PROCESSOS: [/bold red]{type(e).__name__}: {e}", title="Falha Inesperada", border_style="red"))
    finally:
        for p in processes:
            if p.is_alive():
                p.terminate()
        
    console.print("\n[bold magenta]*** FIM DA SESSÃO ***[/bold magenta]")
    input("Pressione ENTER para fechar...")
