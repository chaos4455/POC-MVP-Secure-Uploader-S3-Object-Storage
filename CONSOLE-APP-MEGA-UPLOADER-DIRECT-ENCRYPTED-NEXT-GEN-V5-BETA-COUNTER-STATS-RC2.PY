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
import multiprocessing
from multiprocessing import Process, Manager, Queue as MPQueue
from boto3.session import Config 
import uuid 

# Bibliotecas de criptografia
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.exceptions import InvalidTag 

# Bibliotecas de console Rich
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TransferSpeedColumn
from rich.prompt import Prompt
from rich.panel import Panel
from rich.theme import Theme

# ========================================================================
# === CONFIGURAÇÕES DE PERFORMANCE (TUNING) - MULTI-LANE CONCURRENCY FIX ===
# ========================================================================
MEGA_S3_ENDPOINT = "XXX"
ACCESS_KEY = "XXX"
SECRET_KEY = "XXX"

# Limiares de Granularidade (Tuning points herdados do SyncMesh V1)
SIZE_TINY_THRESHOLD = 1 * 1024 * 1024        # 1 MB (PUT Direto)
SIZE_SMALL_THRESHOLD = 10 * 1024 * 1024      # 10 MB (MPU Pequeno)
SIZE_MEDIUM_THRESHOLD = 100 * 1024 * 1024    # 100 MB (MPU Médio)
# Files > 100MB são considerados LARGE/TURBO

MPU_PART_SIZE = 16 * 1024 * 1024  # 16MB Chunks (Standard MPU part size)
CHUNK_SIZE = MPU_PART_SIZE 

MAX_RETRIES = 100 
RETRY_DELAY = 20 
NONCE_SIZE = 8 

# Base Configuration from SyncMesh V1 (Turbo Mode)
BASE_NUM_HEADS_TINY = 22 
BASE_NUM_HEADS_SMALL = 12 
BASE_NUM_HEADS_MEDIUM = 4 
BASE_NUM_HEADS_LARGE = 4  

BASE_THREADS_TINY = 22 # Tiny files are fast PUTs, high concurrency is key
BASE_THREADS_SMALL = 16 
BASE_THREADS_MEDIUM = 10   
BASE_THREADS_LARGE = 8     

# Variáveis globais a serem definidas por calculate_performance_params
NUM_PROCESSES = {}
WORKER_THREADS = {}
SCAN_WORKERS = 6 
MAX_POOL_CONNECTIONS = 700 
TOTAL_UPLOAD_WORKERS = 0

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

custom_theme = Theme({
    "info": "cyan", "warning": "yellow", "error": "bold red", "success": "bold green",
    "key": "bold magenta", "path": "dim white", "retry": "yellow on black", "alert": "bold yellow on red"
})
console = Console(theme=custom_theme)

LOG_DIR = Path("C:\\MEGA-SYNC-UPLODER-CONTROL-DATA")
LOG_DB_PATH = LOG_DIR / "control_data.db"

FileInfoTuple = Tuple[Path, int, str, str, str, str] 

# ========================================================================
# === CRYPTO ENGINE & STREAM (V2 - Com Metadados Criptografados) ===
# ========================================================================

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
            try:
                with open(KEY_FILE, 'r') as f: data = json.load(f)
                
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
        
        try:
            self._f = open(path, 'rb')
            stat_info = path.stat()
            file_size_raw = stat_info.st_size
            
            s3_dir_path = str(Path(original_path_key).parent) 
            
            metadata = {
                "s3_path_key_full": original_path_key, 
                "s3_directory_path": s3_dir_path,      
                "original_file_name": path.name,
                "original_file_extension": path.suffix,
                "original_file_size_bytes": file_size_raw,
                "original_creation_date": datetime.fromtimestamp(stat_info.st_ctime).isoformat(),
                "original_modification_date": datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
            }
            metadata_json = json.dumps(metadata).encode('utf-8')

        except Exception as e:
             self._f = None 
             file_size_raw = 0
             metadata_json = json.dumps({"path": original_path_key, "error": str(e)}).encode('utf-8')
             
        self.key = key
        self.iv = os.urandom(16) 
        
        cipher = Cipher(algorithms.AES(key), modes.CTR(self.iv), backend=default_backend())
        self.temp_encryptor = cipher.encryptor() 
        
        encrypted_metadata = self.temp_encryptor.update(metadata_json)
        meta_len = len(encrypted_metadata)
        
        meta_len_bytes = meta_len.to_bytes(4, byteorder='big')
        
        self.header = self.iv + meta_len_bytes + encrypted_metadata
        self.header_size = len(self.header)
        
        self.file_size_raw = file_size_raw
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
        
        chunks = [] 
        remaining_size = size

        while remaining_size > 0:
            
            if self.active_pos < self.header_size:
                remaining_header = self.header_size - self.active_pos
                read_len = min(remaining_size, remaining_header)
                
                chunk = self.header[self.active_pos : self.active_pos + read_len]
                self.active_pos += len(chunk)
                chunks.append(chunk)
                remaining_size -= len(chunk)
                
                if self.active_pos == self.header_size:
                     self._setup_encryptor_for_seek(self.header_size)
                     if remaining_size == 0: break
            else:
                data_to_read_raw = min(remaining_size, self.total_size - self.active_pos)
                
                if data_to_read_raw == 0:
                    if self.active_encryptor:
                        try:
                            final_chunk = self.active_encryptor.finalize()
                            self.active_encryptor = None
                            if final_chunk: chunks.append(final_chunk)
                        except Exception: pass 
                    break
                
                raw_data = self._f.read(data_to_read_raw)
                
                if not raw_data:
                    if self.active_encryptor:
                        try:
                            final_chunk = self.active_encryptor.finalize()
                            self.active_encryptor = None
                            if final_chunk: chunks.append(final_chunk)
                        except Exception: pass 
                    break
                    
                encrypted_data = self.active_encryptor.update(raw_data)
                self.active_pos += len(encrypted_data)
                chunks.append(encrypted_data)
                remaining_size -= len(encrypted_data)
                
                if len(encrypted_data) < data_to_read_raw: break
                    
        return b''.join(chunks) 

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
            if self._f: self._f.close(); self._f = None 
            self.active_encryptor = None
            self.active_iv = None
        except:
            pass 
            
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.close(); return False 

# ========================================================================
# === COLETOR DE LOGS CENTRALIZADO E PERSISTENTE (MANTIDO) ===
# ========================================================================

class LogCollectorProcess(Process):
    
    def __init__(self, log_queue: MPQueue, db_path: Path):
        super().__init__()
        self.log_queue = log_queue
        self.db_path = db_path
        self._stop_event = multiprocessing.Event()
        self.BATCH_SIZE = 100
        self.FLUSH_INTERVAL = 60 
        self.conn = None
        self.cursor = None
        self.last_flush_time = time.time()
        self.batch_entries = []
        self._safe_print("[info]Log Collector: Inicializado.[/info]")

    def _safe_print(self, text, style=None):
        console = Console(theme=custom_theme)
        prefix = f"[LOG_COLLECTOR]"
        if style:
            console.print(f"{prefix}[{style}]{text}[/{style}]")
        else:
            console.print(f"{prefix}{text}")

    def _setup_db_connection(self):
        self.conn = sqlite3.connect(self.db_path, timeout=30) 
        self.cursor = self.conn.cursor()

    def _flush_batch(self):
        if not self.batch_entries:
            self.last_flush_time = time.time()
            return
            
        try:
            sql = """
                INSERT INTO file_status 
                (run_id, pc_name, drive_letter, file_path_full, file_name, file_size_bytes, status, s3_key, error_detail, original_creation_date, original_modification_date, root_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.cursor.executemany(sql, self.batch_entries)
            self.conn.commit()
            
            num_flushed = len(self.batch_entries)
            self.batch_entries = []
            self.last_flush_time = time.time()
            
            if num_flushed > 0:
                self._safe_print(f"[success]✅ Persistência concluída: {num_flushed} logs gravados no DB.[/success]")
            
        except Exception as e:
            self._safe_print(f"[error]ERRO CRÍTICO NO BATCH INSERT (SQLite): {e}. Tentando gravação individual...[/error]", style="error")
            
            sql_fallback = """
                INSERT INTO file_status 
                (run_id, pc_name, drive_letter, file_path_full, file_name, file_size_bytes, status, s3_key, error_detail, original_creation_date, original_modification_date, root_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            saved_count = 0
            for entry in self.batch_entries:
                try:
                    self.cursor.execute(sql_fallback, entry)
                    saved_count += 1
                except Exception as individual_e:
                    self._safe_print(f"[error]Falha ao gravar entrada individual: {individual_e}[/error]")
            
            if saved_count > 0:
                self.conn.commit()
                self._safe_print(f"[warning]⚠️ Gravados {saved_count} logs individualmente após falha no batch.[/warning]")
                
            self.batch_entries = []
            self.last_flush_time = time.time()

    def run(self):
        self._setup_db_connection()
        while not self._stop_event.is_set():
            time_until_flush = self.FLUSH_INTERVAL - (time.time() - self.last_flush_time)
            try:
                if len(self.batch_entries) >= self.BATCH_SIZE:
                    self._flush_batch()
                    continue 
                timeout = max(0.1, time_until_flush)
                item = self.log_queue.get(timeout=timeout)
            except queue.Empty:
                item = None
            except Exception as e:
                self._safe_print(f"[error]Erro inesperado ao obter item da fila: {e}[/error]")
                time.sleep(1)
                continue
            
            if item:
                if item == "STOP_SIGNAL": break 
                try:
                    if len(item) != 12:
                        self._safe_print(f"[error]Erro de formato de log: Esperado 12 campos, recebido {len(item)}.[/error]")
                        continue
                    self.batch_entries.append(item)
                except Exception as e:
                    self._safe_print(f"[error]Erro ao processar item do log na fila: {e}[/error]")
            
            if time.time() - self.last_flush_time >= self.FLUSH_INTERVAL:
                self._flush_batch()
        
        self._safe_print(f"Processando entradas finais na fila de logs...")
        while True:
            try:
                item = self.log_queue.get_nowait()
                if item == "STOP_SIGNAL": continue
                self.batch_entries.append(item)
            except queue.Empty:
                break
            except Exception as e:
                 self._safe_print(f"[error]Erro ao processar item final: {e}[/error]")
                 break
                 
        self._flush_batch()
        if self.conn: self.conn.close()
        self._safe_print("Conexão DB fechada. Log Collector Encerrado.")

    def stop(self):
        self._stop_event.set()
        self.log_queue.put("STOP_SIGNAL") 

    def _check_and_add_column(self, cursor: sqlite3.Cursor, table_name: str, column_name: str, column_type: str):
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [info[1] for info in cursor.fetchall()]
        if column_name not in columns:
            self._safe_print(f"[warning]Migrando schema: Adicionando coluna '{column_name}' a '{table_name}'.[/warning]")
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
            except Exception as e:
                self._safe_print(f"[error]Falha ao adicionar coluna {column_name}: {e}[/error]")
                raise

    def setup_db_schema_if_not_exists(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS run_log (
                    run_id INTEGER PRIMARY KEY, pc_name TEXT, collection_name TEXT, root_path TEXT, drive_letter TEXT,
                    timestamp_start TEXT, timestamp_end TEXT, total_uploaded INTEGER, total_ignored INTEGER, total_failed INTEGER
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, pc_name TEXT, drive_letter TEXT, file_path_full TEXT,
                    file_name TEXT, file_size_bytes INTEGER, status TEXT, s3_key TEXT, error_detail TEXT, 
                    FOREIGN KEY (run_id) REFERENCES run_log (run_id)
                )
            """)
            self._check_and_add_column(cursor, "file_status", "original_creation_date", "TEXT")
            self._check_and_add_column(cursor, "file_status", "original_modification_date", "TEXT")
            self._check_and_add_column(cursor, "file_status", "root_path", "TEXT") 
            conn.commit()
        except Exception as e:
            console.print(f"[error]ERRO CRÍTICO ao configurar o Schema DB: {e}[/error]")
            sys.exit(1)
        finally:
            if conn: conn.close()
            

class DBIntegrityManager:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.local_console = Console(theme=custom_theme)
    
    def _safe_print(self, text, style=None):
        prefix = f"[DB_INTEGRITY]"
        if style:
            self.local_console.print(f"{prefix}[{style}]{text}[/{style}]")
        else:
            self.local_console.print(f"{prefix}{text}")

    def _check_and_add_column(self, cursor: sqlite3.Cursor, table_name: str, column_name: str, column_type: str):
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [info[1] for info in cursor.fetchall()]
        if column_name not in columns:
            self._safe_print(f"[warning]MIGRACAO PATCH: Adicionando coluna '{column_name}' a '{table_name}' para retrocompatibilidade.[/warning]")
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
            except Exception as e:
                self._safe_print(f"[error]Falha ao adicionar coluna {column_name}: {e}[/error]")
                raise
                
    def _normalize_path_string(self, path_str: str) -> str:
        """Normaliza uma string de caminho para o formato POSIX padronizado."""
        normalized = path_str.replace('\\', '/')
        is_windows_root = len(normalized) >= 2 and normalized[1] == ':'
        
        if is_windows_root:
            drive, rest = normalized.split(':', 1)
            cleaned_rest = '/'.join(filter(None, rest.split('/')))
            normalized = drive.upper() + ':' + ('/' if cleaned_rest else '') + cleaned_rest
        else:
            normalized = '/' + '/'.join(filter(None, normalized.split('/')))
        
        if normalized.endswith('/') and len(normalized) > 1:
            normalized = normalized[:-1]
            
        return normalized


    def fix_path_inconsistencies(self):
        """Corrige inconsistências de barra em caminhos."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id, file_path_full, root_path 
                FROM file_status 
                WHERE status = 'UPLOADED' OR file_path_full LIKE '%\\%' OR file_path_full LIKE '%//%'
            """)
            
            records_to_check = cursor.fetchall()
            if not records_to_check:
                self._safe_print("[success]Nenhum registro de caminho para checagem de integridade.[/success]")
                return

            self._safe_print(f"[warning]Iniciando correção de integridade de caminhos no DB para {len(records_to_check)} registros...[/warning]")

            update_count = 0
            
            for record_id, old_path_full, old_root_path in records_to_check:
                
                new_path_full = self._normalize_path_string(old_path_full)
                new_root_path = self._normalize_path_string(old_root_path) if old_root_path else None

                if new_path_full != old_path_full or (old_root_path and new_root_path != old_root_path):
                    
                    cursor.execute("""
                        UPDATE file_status 
                        SET 
                            file_path_full = ?, 
                            root_path = ?
                        WHERE id = ?
                    """, (new_path_full, new_root_path, record_id))
                    
                    if old_root_path and new_root_path != old_root_path:
                         cursor.execute("""
                            UPDATE run_log 
                            SET 
                                root_path = ? 
                            WHERE root_path = ?
                         """, (new_root_path, old_root_path))
                         
                    update_count += 1
            
            conn.commit()
            if update_count > 0:
                self._safe_print(f"[success]✅ Correção de caminhos concluída. {update_count} entradas corrigidas.[/success]")

        except Exception as e:
            self._safe_print(f"[error]ERRO CRÍTICO na Correção de Caminhos DB: {e}[/error]", style="error")
        finally:
            if conn: conn.close()


    def _patch_db_structure(self, cursor: sqlite3.Cursor):
        
        self._check_and_add_column(cursor, "file_status", "original_creation_date", "TEXT")
        self._check_and_add_column(cursor, "file_status", "original_modification_date", "TEXT")
        self._check_and_add_column(cursor, "file_status", "root_path", "TEXT") 

        cursor.execute("""
            SELECT id, run_id 
            FROM file_status 
            WHERE root_path IS NULL
        """)
        null_root_path_entries = cursor.fetchall()
        
        if null_root_path_entries:
            cursor.execute("SELECT run_id, root_path FROM run_log")
            run_log_map = {rid: rpath for rid, rpath in cursor.fetchall()}
            
            update_count = 0
            for file_id, run_id in null_root_path_entries:
                root_path = run_log_map.get(run_id)
                if root_path:
                    try:
                        cursor.execute("""
                            UPDATE file_status SET root_path = ? WHERE id = ?
                        """, (root_path, file_id))
                        update_count += 1
                    except Exception as e:
                        self._safe_print(f"[error]Falha ao atualizar root_path para file_id {file_id}: {e}[/error]")
            
            if update_count > 0:
                self._safe_print(f"[success]✅ Patch de 'root_path' concluído. {update_count} entradas corrigidas.[/success]")
            
            cursor.connection.commit()


    def backfill_missing_metadata(self):
        
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()
            
            self._patch_db_structure(cursor)

            cursor.execute("""
                SELECT 
                    id, file_path_full, file_size_bytes
                FROM 
                    file_status 
                WHERE 
                    status = 'UPLOADED' AND 
                    (original_creation_date IS NULL OR original_modification_date IS NULL)
                ORDER BY id ASC
            """)
            
            records_to_update = cursor.fetchall()
            if not records_to_update: return

            update_count = 0
            
            for record_id, file_path_full, existing_size in records_to_update:
                fp = Path(file_path_full)
                
                if fp.exists():
                    try:
                        stat_info = fp.stat()
                        ctime_iso = datetime.fromtimestamp(stat_info.st_ctime).isoformat()
                        mtime_iso = datetime.fromtimestamp(stat_info.st_mtime).isoformat()
                        current_size = stat_info.st_size
                        
                        if existing_size != current_size:
                             self._safe_print(f"[alert]Aviso: Tamanho {fp.name} (DB: {existing_size}, Disk: {current_size}) inconsistente. Corrigindo no DB.[/alert]")

                        cursor.execute("""
                            UPDATE file_status 
                            SET 
                                original_creation_date = ?, 
                                original_modification_date = ?,
                                file_size_bytes = ? 
                            WHERE id = ?
                        """, (ctime_iso, mtime_iso, current_size, record_id))
                        
                        update_count += 1
                        
                    except Exception as e:
                        pass 
            
            conn.commit()
            if update_count > 0:
                self._safe_print(f"[success]✅ Backfill concluído. {update_count} registros atualizados com sucesso.[/success]")

        except Exception as e:
            self._safe_print(f"[error]ERRO CRÍTICO no Backfill do DB: {e}[/error]", style="error")
        finally:
            if conn: conn.close()
            
            
class WorkerLogManager:
    def __init__(self, pc_name: str, root_path: str, collection: str, worker_id: int, log_queue: MPQueue, db_path: Path = LOG_DB_PATH):
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.pc_name = pc_name
        self.root_path = root_path
        self.drive_letter = Path(root_path).parts[0].replace('\\', '').replace(':', '') if root_path and len(Path(root_path).parts) > 0 else 'UNKNOWN'
        self.collection = collection
        self.worker_id = worker_id 
        self.log_queue = log_queue
        self.conn_run_log = sqlite3.connect(self.db_path, timeout=5)
        self.cursor_run_log = self.conn_run_log.cursor()
        self.run_id = self._setup_run_log_entry() 
        self.timestamp_start = datetime.now()

    def _setup_run_log_entry(self):
        local_pc_name_with_head = f"{self.pc_name}-P{self.worker_id}"
        self.cursor_run_log.execute("""
            INSERT INTO run_log (
                pc_name, collection_name, root_path, drive_letter, timestamp_start, timestamp_end, 
                total_uploaded, total_ignored, total_failed
            )
            VALUES (?, ?, ?, ?, ?, NULL, 0, 0, 0)
        """, (
            local_pc_name_with_head, self.collection, self.root_path, self.drive_letter,
            datetime.now().isoformat()
        ))
        self.conn_run_log.commit()
        return self.cursor_run_log.lastrowid

    def log_file_status(self, file_path: Path, status: str, file_size: int, s3_key: Optional[str] = None, error_detail: Optional[str] = None, ctime_iso: Optional[str] = None, mtime_iso: Optional[str] = None):
        safe_error_detail = str(error_detail).encode('utf-8', 'ignore').decode('utf-8') if error_detail else None
        safe_s3_key = s3_key if s3_key else None
        file_path_posix = file_path.as_posix() 
        log_entry = (
            self.run_id, self.pc_name, self.drive_letter, 
            file_path_posix, file_path.name, file_size, 
            status, safe_s3_key, safe_error_detail, 
            ctime_iso, mtime_iso, self.root_path 
        )
        self.log_queue.put(log_entry)

    def finalize_run(self, counts: dict):
        self.timestamp_end = datetime.now()
        self.cursor_run_log.execute("""
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
        self.conn_run_log.commit()
        self.close()

    def get_run_metrics(self):
        conn_read = sqlite3.connect(self.db_path)
        cursor_read = conn_read.cursor()
        cursor_read.execute("""
            SELECT file_path_full, file_name, status, error_detail 
            FROM file_status 
            WHERE run_id = ? AND status != 'UPLOADED'
        """, (self.run_id,))
        non_uploaded_files = cursor_read.fetchall()
        conn_read.close()
        return non_uploaded_files 

    @classmethod
    def get_past_runs_info(cls, db_path: Path) -> List[Tuple[int, str, str, str, str]]:
        if not db_path.exists(): return []
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT run_id, pc_name, collection_name, root_path, timestamp_start 
                FROM run_log 
                ORDER BY run_id DESC
            """)
            return cursor.fetchall()
        except Exception as e:
            console.print(f"[error]Erro ao buscar execuções passadas: {e}[/error]")
            return []
        finally:
            if conn: conn.close()
            
    @classmethod
    def get_uploaded_files_for_continuation(cls, db_path: Path, collection: str, root_path: str) -> Set[str]:
        if not db_path.exists(): return set()
        conn = None
        uploaded_files = set()
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT file_path_full 
                FROM file_status 
                WHERE status = 'UPLOADED' AND root_path = ? AND run_id IN (
                    SELECT run_id 
                    FROM run_log 
                    WHERE collection_name = ?
                )
            """, (root_path, collection))
            for row in cursor.fetchall():
                uploaded_files.add(row[0]) 
        except Exception as e:
            console.print(f"[error]Erro ao buscar lista de arquivos enviados para continuação: {e}[/error]")
        finally:
            if conn: conn.close()
        return uploaded_files

    def close(self):
        try:
            if self.conn_run_log: self.conn_run_log.close()
        except:
            pass

def select_folder_gui() -> Optional[str]:
    try:
        root = tk.Tk()
        root.withdraw() 
        initial_dir = Path.home()
        console.print("[info]Aguardando seleção de pasta (Janela do explorador)...[/info]")
        folder_path = filedialog.askdirectory(
            initialdir=initial_dir, 
            title="Selecione a Pasta Raiz para o Backup"
        )
        root.destroy()
        if folder_path:
            return folder_path
        else:
            console.print("[error]❌ Nenhuma pasta selecionada. Encerrando.[/error]")
            return None
    except Exception as e:
        console.print(f"[error]❌ Erro ao abrir a interface gráfica de seleção de pasta: {e}[/error]")
        return None


# ========================================================================
# === UPLOAD ENGINE (MULTI-LANE ADAPTED) ===
# ========================================================================

class UploadEngine:

    def __init__(self, collection_name, root_path, password, head_id: int, worker_count: int, pool_type: str, global_log_queue: MPQueue):
        self.root_path = Path(root_path)
        self.collection = collection_name
        self.head_id = head_id 
        self.upload_worker_count = worker_count
        self.pool_type = pool_type 
        self.crypto = CryptoEngine()
        
        # Semaforo ajustado para 3 I/O concorrentes (I/O reading for encryption)
        self.disk_io_semaphore = threading.Semaphore(3) 
        
        s3_config = Config(
            max_pool_connections=MAX_POOL_CONNECTIONS, 
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
        
        self.log_db = WorkerLogManager(self.pc_name, root_path, self.collection, self.head_id, global_log_queue)
        
        self.local_console = Console(theme=custom_theme)
        self._safe_print(f"[info]📊 HEAD P{self.head_id} ({self.pool_type} Pool) | Upload Workers: {self.upload_worker_count} | Scan Workers: {SCAN_WORKERS} | Log DB ID: {self.log_db.run_id}[/info]", style=None)
        
        self._ensure_bucket() 

        self.metrics = {'uploaded': 0, 'ignored': 0, 'failed': 0}
        
        self.upload_queue = queue.Queue()
        
        self.mpu_state: Dict[str, Dict[str, Any]] = {} 
        self.mpu_lock = threading.Lock()
        
        self.progress_task_id = None


    def _safe_print(self, text, style=None):
        try:
            prefix = f"[P{self.head_id}]"
            if style:
                self.local_console.print(f"{prefix}[{style}]{text}[/{style}]")
            else:
                self.local_console.print(f"{prefix}{text}")
        except UnicodeEncodeError:
            sanitized_text = text.encode('ascii', 'replace').decode('ascii').replace('?', '*')
            self.local_console.print(f"[P{self.head_id}][bold red]**** ERRO UNICODE ****: {sanitized_text} [/bold red]")
        except Exception:
            self.local_console.print(f"[P{self.head_id}][bold red]**** ERRO GERAL DE CONSOLE ****[/bold red]")


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
                        if create_error_code == 'BucketAlreadyOwnedByYou': return 
                        else: raise create_e 
                
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
                
                if upload_id and mpu_info.get('Status') != 'ABORTED':
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

    def _scan_and_hash_worker_unified(self, file_path_tuple: FileInfoTuple):
        fp = file_path_tuple[0]
        file_size_original = file_path_tuple[1]
        pc_name = file_path_tuple[2]
        drive_letter = file_path_tuple[3]
        ctime_iso = file_path_tuple[4]
        mtime_iso = file_path_tuple[5]
        s3_key_unique = None
        
        try:
            file_size = fp.stat().st_size
            
            if file_size == 0:
                self._safe_print(f"[VAZIO] Ignorando {fp.name}: Arquivo vazio.", style="warning")
                self.log_db.log_file_status(fp, 'IGNORED', 0, s3_key=None, error_detail="ZERO_BYTE_SIZE", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                self.metrics['ignored'] += 1
                return 0
            
            original_metadata_path = Path(pc_name) / drive_letter / self._get_original_path_metadata(fp)
            content_hash = self._calculate_content_hash(fp)
            if not content_hash: 
                self.log_db.log_file_status(fp, 'IGNORED', file_size, s3_key=None, error_detail="HASH_METADATA_ERROR", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                self.metrics['ignored'] += 1
                return 0
            
            s3_key_unique = self._create_s3_key(content_hash)
            
            # ----------------------------------------------------
            # DECISÃO DE UPLOAD: DIRECT PUT (small) vs MPU (large)
            # ----------------------------------------------------
            is_direct_put = file_size <= MPU_PART_SIZE 

            if is_direct_put:
                # --- DIRECT PUT (Optimized for small files <= 16MB) ---
                encrypted_data = None
                try:
                    with self.disk_io_semaphore:
                        with self.crypto.create_encrypted_stream(fp, original_metadata_path.as_posix()) as stream:
                            encrypted_data = stream.read(stream.total_size)
                            
                    self.s3.put_object(
                        Bucket=self.clean_bucket_name,
                        Key=s3_key_unique,
                        Body=encrypted_data
                    )
                    
                    self._safe_print(f"✅ PUT DIRETO CONCLUÍDO. Key: {s3_key_unique[:16]}... | File: {fp.name}", style="success")
                    self.log_db.log_file_status(fp, 'UPLOADED', file_size, s3_key=s3_key_unique, ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                    self.metrics['uploaded'] += 1
                    return len(encrypted_data)
                    
                except Exception as e:
                    safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
                    self._safe_print(f"[ERRO DIRECT PUT] Falha {fp.name}: {type(e).__name__}: {safe_error}", style="error")
                    self.log_db.log_file_status(fp, 'FAILED', file_size_original, s3_key=s3_key_unique, error_detail=f"DIRECT_PUT_FAIL: {type(e).__name__}: {safe_error}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                    self.metrics['failed'] += 1
                    return 0
                    
            else:
                # --- MPU STREAMING (For files > 16MB) ---
                
                with self.disk_io_semaphore: 
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
                        'CtimeISO': ctime_iso,
                        'MtimeISO': mtime_iso,
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
            
            self.log_db.log_file_status(fp, 'IGNORED', file_size_original, s3_key=s3_key_unique, error_detail=f"PROCESSING_ERROR: {type(e).__name__}: {safe_error}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
            self.metrics['ignored'] += 1
            
            if s3_key_unique and 'upload_id' in locals():
                self._abort_mpu(s3_key_unique)
                
            return 0


    def _upload_worker(self, progress: Progress, overall_task: int):
        
        while True:
            try:
                item: Optional[Tuple] = self.upload_queue.get(timeout=0.5) 
            except queue.Empty:
                if not self.mpu_state and overall_task > 1 and progress.tasks[overall_task].completed == progress.tasks[overall_task].total:
                    break
                time.sleep(0.5)
                continue

            if item is None: break
                
            s3_key, upload_id, part_number, offset, length, file_path, file_size = item
            
            etag = None
            chunk_data = None
            ctime_iso, mtime_iso, fatal_mpu_error = None, None, False 

            try:
                # 1. Checagem inicial e obtenção de metadados necessários
                with self.mpu_lock:
                    mpu_info = self.mpu_state.get(s3_key)
                    
                    if not mpu_info or mpu_info.get('FatalError') or mpu_info.get('Status') != 'ACTIVE':
                        self.upload_queue.task_done()
                        continue
                        
                    ctime_iso = mpu_info['CtimeISO']
                    mtime_iso = mpu_info['MtimeISO']
                    pc_name_meta = mpu_info['PCName']
                    drive_letter_meta = mpu_info['DriveLetter']
                    
                original_metadata_path = Path(pc_name_meta) / drive_letter_meta / self._get_original_path_metadata(file_path)
                
                # --- Reabrir o stream JIT para a leitura ---
                with self.disk_io_semaphore:
                    try:
                        with self.crypto.create_encrypted_stream(file_path, original_metadata_path.as_posix()) as stream:
                            stream.seek(offset) 
                            chunk_data = stream.read(length) 
                    except Exception as e:
                        raise IOError(f"Disk I/O failure during read: {e}") 

                if chunk_data is None or len(chunk_data) != length:
                    self._safe_print(f"[ERROR] ERRO READ: Falha ao ler {length} bytes de {file_path.name}. Abortando MPU.", style="error")
                    self._abort_mpu(s3_key)
                    self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail="READ_FAIL_DISK_IO", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                    self.metrics['failed'] += 1
                    self.upload_queue.task_done()
                    continue


                # 3. Tentativas de Upload
                upload_successful = False
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        if attempt > 1: self._safe_print(f"[RETRY] Retry {attempt}/{MAX_RETRIES} para {file_path.name} Part {part_number}...", style="retry")

                        response = self.s3.upload_part(
                            Bucket=self.clean_bucket_name,
                            Key=s3_key,
                            UploadId=upload_id,
                            PartNumber=part_number,
                            Body=chunk_data
                        )
                        etag = response['ETag']
                        upload_successful = True
                        break 
                        
                    except (ClientError, ConnectionError, ReadTimeoutError) as e:
                        error_code = 'Unknown'
                        if isinstance(e, ClientError):
                            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                            
                            if error_code in ['NoSuchUpload', 'InvalidPart']: 
                                with self.mpu_lock:
                                    mpu_info_check = self.mpu_state.get(s3_key)
                                    if mpu_info_check and mpu_info_check.get('Status') == 'ACTIVE':
                                        self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"PART_FATAL_S3_STATE: {error_code}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                                        self.metrics['failed'] += 1
                                        self._abort_mpu(s3_key)
                                        
                                    fatal_mpu_error = True 
                                    break 
                            
                        if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY) 
                        else: raise 
                            
                if fatal_mpu_error: continue 
                if not upload_successful: raise ConnectionError("Upload falhou após esgotar retries.")

                # 4. ATUALIZA O PROGRESS BAR
                progress.update(overall_task, advance=length)
                
                # 5. ATOMIC CHAIN / LAST MAN STANDING
                with self.mpu_lock:
                    mpu_info = self.mpu_state.get(s3_key)
                    if not mpu_info or mpu_info.get('FatalError') or mpu_info.get('Status') != 'ACTIVE':
                        self.upload_queue.task_done()
                        continue
                         
                    mpu_info['Parts'].append({'PartNumber': part_number, 'ETag': etag})
                    mpu_info['CompletedPartsCount'] += 1
                    
                    if mpu_info['CompletedPartsCount'] == mpu_info['TotalParts']:
                        
                        mpu_info['Status'] = 'COMPLETING'
                        parts = sorted(mpu_info['Parts'], key=lambda x: x['PartNumber'])
                        
                        try:
                            self.s3.complete_multipart_upload(
                                Bucket=self.clean_bucket_name, Key=s3_key, UploadId=upload_id,
                                MultipartUpload={'Parts': parts}
                            )
                            
                            self._safe_print(f"✅ Upload CONCLUÍDO (Atomic Chain). Key: {s3_key[:16]}... | File: {file_path.name}", style="success")
                            self.log_db.log_file_status(file_path, 'UPLOADED', file_size, s3_key=s3_key, ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                            self.metrics['uploaded'] += 1
                            
                            del self.mpu_state[s3_key]
                            
                        except ClientError as completion_e:
                            error_code = completion_e.response.get('Error', {}).get('Code', 'Unknown')
                            safe_completion_error = str(completion_e).encode('utf-8', 'ignore').decode('utf-8')
                            
                            if error_code == 'NoSuchUpload':
                                self._safe_print(f"❌ FALHA NA FINALIZAÇÃO (NoSuchUpload): {file_path.name}. Limpeza local.", style="error")
                                mpu_info['FatalError'] = True
                                mpu_info['Status'] = 'FAILED_CLEANUP'
                                self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"COMPLETION_FAIL_NOSUCHUPLOAD: {safe_completion_error}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                                self.metrics['failed'] += 1
                                del self.mpu_state[s3_key] 
                            else:
                                self._safe_print(f"❌ FALHA NA FINALIZAÇÃO (ClientError): {file_path.name}: {safe_completion_error}. ABORTANDO.", style="error")
                                mpu_info['FatalError'] = True
                                mpu_info['Status'] = 'FAILED'
                                self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"COMPLETION_FAIL: {safe_completion_error}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                                self.metrics['failed'] += 1
                                self._abort_mpu(s3_key) 
                                
                        except Exception as completion_e:
                            safe_completion_error = str(completion_e).encode('utf-8', 'ignore').decode('utf-8')
                            self._safe_print(f"❌ FALHA NA FINALIZAÇÃO (GENÉRICA): {file_path.name}: {safe_completion_error}. ABORTANDO.", style="error")
                            mpu_info['FatalError'] = True
                            mpu_info['Status'] = 'FAILED'
                            self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"COMPLETION_FAIL_GENERIC: {safe_completion_error}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                            self.metrics['failed'] += 1
                            self._abort_mpu(s3_key)
                    
            except Exception as e:
                safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
                
                is_mpu_aborted = False
                with self.mpu_lock:
                    mpu_info = self.mpu_state.get(s3_key)
                    if mpu_info and mpu_info.get('FatalError'): is_mpu_aborted = True
                
                if not is_mpu_aborted:
                    self._safe_print(f"❌ FALHA PERSISTENTE (PARTE {part_number}): {file_path.name}: {safe_error}. ABORTANDO MPU.", style="error")
                    
                    with self.mpu_lock:
                        if mpu_info and mpu_info.get('Status') == 'ACTIVE':
                            self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"PART_FATAL: {safe_error}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                            self.metrics['failed'] += 1
                            self._abort_mpu(s3_key) 
                            try:
                                if mpu_info: progress.update(overall_task, advance=mpu_info['EncryptedSize'])
                            except Exception: pass 
                else:
                    self._safe_print(f"[SKIP POST-FATAL] Chunk {part_number} para {file_path.name} falhou após MPU ser abortado por outro chunk.", style="warning")
            finally:
                self.upload_queue.task_done()
                
    
    def set_progress_tracker(self, progress: Progress, task_id: Any):
        self.progress_tracker = progress
        self.progress_task_id = task_id
        
# --- FUNÇÕES AUXILIARES GLOBAIS ---

def calculate_performance_params(mode: int):
    """Calcula os parâmetros de concorrência com base no modo de performance."""
    global NUM_PROCESSES, WORKER_THREADS, MAX_POOL_CONNECTIONS, TOTAL_UPLOAD_WORKERS
    
    if mode == 1: # TURBO (100%)
        scale = 1.0
        MAX_POOL_CONNECTIONS_BASE = 700
    elif mode == 2: # MID (50%)
        scale = 0.5
        MAX_POOL_CONNECTIONS_BASE = 350
    elif mode == 3: # LIGHT (25%)
        scale = 0.25
        MAX_POOL_CONNECTIONS_BASE = 175
    else:
        raise ValueError("Modo de performance inválido.")

    # Calculate Processes (Heads)
    NUM_PROCESSES['TINY'] = max(1, int(BASE_NUM_HEADS_TINY * scale))
    NUM_PROCESSES['SMALL'] = max(1, int(BASE_NUM_HEADS_SMALL * scale))
    NUM_PROCESSES['MEDIUM'] = max(1, int(BASE_NUM_HEADS_MEDIUM * scale))
    NUM_PROCESSES['LARGE'] = max(1, int(BASE_NUM_HEADS_LARGE * scale))

    # Calculate Threads per Process
    WORKER_THREADS['TINY'] = max(2, int(BASE_THREADS_TINY * scale))
    WORKER_THREADS['SMALL'] = max(2, int(BASE_THREADS_SMALL * scale))
    WORKER_THREADS['MEDIUM'] = max(2, int(BASE_THREADS_MEDIUM * scale))
    WORKER_THREADS['LARGE'] = max(2, int(BASE_THREADS_LARGE * scale))
    
    TOTAL_UPLOAD_WORKERS = sum(
        NUM_PROCESSES[lane] * WORKER_THREADS[lane] 
        for lane in ['TINY', 'SMALL', 'MEDIUM', 'LARGE']
    )
    
    MAX_POOL_CONNECTIONS = max(MAX_POOL_CONNECTIONS_BASE, TOTAL_UPLOAD_WORKERS + (sum(NUM_PROCESSES.values()) * SCAN_WORKERS) + 50)
    console.print(f"[info]Parâmetros de Performance Calculados (Total Upload Threads: {TOTAL_UPLOAD_WORKERS})[/info]")


def is_folder_ignored(path_segment: str) -> bool:
    return path_segment.lower() in FORCED_IGNORE_FOLDERS

def scan_and_orchestrate_files(
    target_folder: str, 
    scan_mode: int,
    files_to_ignore_set: Set[str],
    ignore_hc_files: bool 
) -> Tuple[Dict[Tuple[str, str], List[FileInfoTuple]], List[Tuple[Path, int, str]], List[str]]:

    base_path = Path(target_folder)
    all_files_on_disk: List[Path] = []
    ignored_system_files: List[Tuple[Path, int, str]] = [] 
    ignored_folders_list: List[str] = []
    
    console.print("[info]Iniciando varredura primária do disco e aplicando filtros...[/info]")
    
    for root, dirs, files in os.walk(base_path, topdown=True):
        current_path = Path(root)
        
        try:
            relative_to_base_parts = current_path.relative_to(base_path).parts
        except ValueError:
            relative_to_base_parts = ()
            
        is_root_level_check = (scan_mode == SCAN_MODE_FULL_DISK) and (len(relative_to_base_parts) <= 1)
        is_multi_origin_check = (scan_mode == SCAN_MODE_MULTI_ORIGIN) and (len(relative_to_base_parts) <= 2)
            
        dirs_to_remove = []
        for d in dirs:
            if is_folder_ignored(d) and (is_root_level_check or is_multi_origin_check):
                ignored_folders_list.append(str(current_path / d))
                dirs_to_remove.append(d)
                
        for d_rem in dirs_to_remove:
            if d_rem in dirs: dirs.remove(d_rem)
            
        
        for f in files:
            file_path = current_path / f
            file_name_lower = file_path.name.lower()
            file_suffix_lower = file_path.suffix.lower()
            
            is_ignored = False
            error_reason = ""
            file_path_posix = file_path.as_posix() 

            if file_name_lower in FORCED_IGNORE_FILENAMES or file_name_lower == KEY_FILE.name.lower():
                is_ignored = True
                error_reason = "SYSTEM_FILE_FILTER"
            elif file_path_posix in files_to_ignore_set:
                is_ignored = True
                error_reason = "CONTINUATION_IGNORE"
            elif ignore_hc_files and file_suffix_lower == '.hc':
                is_ignored = True
                error_reason = "HC_FILE_FILTER"
            
            if is_ignored:
                try:
                    size = file_path.stat().st_size
                    ignored_system_files.append((file_path, size, error_reason))
                except Exception:
                    ignored_system_files.append((file_path, 0, error_reason))
                continue
                
            all_files_on_disk.append(file_path)

    
    file_groups: Dict[Tuple[str, str], List[FileInfoTuple]] = {}
    current_pc_name_local = socket.gethostname().lower().replace(' ', '-')
    
    for fp in all_files_on_disk:
        try:
            stat_info = fp.stat()
            size = stat_info.st_size
            ctime_iso = datetime.fromtimestamp(stat_info.st_ctime).isoformat()
            mtime_iso = datetime.fromtimestamp(stat_info.st_mtime).isoformat()
            
        except Exception:
            ignored_system_files.append((fp, 0, 'STAT_ACCESS_ERROR'))
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
                    drive_letter = drive_letter_raw.split('_')[0] if '_' in drive_letter_raw else drive_letter_raw
                    drive_letter = drive_letter.replace(' ', '-').replace('/', '_').strip() 
                    if not pc_name or not drive_letter: raise ValueError("Estrutura Multi-Origin Inválida")
                    group_key = (pc_name, drive_letter)
                else:
                    pc_name = "ROOT_UNKNOWN"; drive_letter = "UNKNOWN"
                    group_key = (pc_name, drive_letter)
                    
            except Exception:
                ignored_system_files.append((fp, size, 'MULTI_ORIGIN_PARSE_ERROR'))
                continue
        
        if group_key not in file_groups: file_groups[group_key] = []
            
        file_groups[group_key].append((fp, size, pc_name, drive_letter, ctime_iso, mtime_iso))
    
    return file_groups, ignored_system_files, ignored_folders_list

def orchestrate_and_queue_split(
    file_groups: Dict[Tuple[str, str], List[FileInfoTuple]],
    tiny_queue: multiprocessing.Queue,
    small_queue: multiprocessing.Queue,
    medium_queue: multiprocessing.Queue,
    large_queue: multiprocessing.Queue
) -> Dict[str, Any]:
    
    total_files_allocated = 0
    total_size_allocated = 0
    
    files_by_size = {'TINY': [], 'SMALL': [], 'MEDIUM': [], 'LARGE': []}
    
    all_files: List[FileInfoTuple] = []
    for _, files in file_groups.items(): all_files.extend(files)

    for f in all_files:
        size = f[1]
        if size <= SIZE_TINY_THRESHOLD:
            files_by_size['TINY'].append(f)
        elif size <= SIZE_SMALL_THRESHOLD:
            files_by_size['SMALL'].append(f)
        elif size <= SIZE_MEDIUM_THRESHOLD:
            files_by_size['MEDIUM'].append(f)
        else:
            files_by_size['LARGE'].append(f)

    # Ordenação otimizada
    files_by_size['TINY'].sort(key=lambda x: x[1], reverse=False) 
    files_by_size['SMALL'].sort(key=lambda x: x[1], reverse=True) 
    files_by_size['MEDIUM'].sort(key=lambda x: x[1], reverse=True) 
    files_by_size['LARGE'].sort(key=lambda x: x[1], reverse=True) 
    
    queue_map = {
        'TINY': tiny_queue, 'SMALL': small_queue, 
        'MEDIUM': medium_queue, 'LARGE': large_queue
    }

    for lane, files in files_by_size.items():
        console.print(f"[info]Alocando {len(files)} arquivos {lane} -> {lane} Pool...[/info]")
        target_queue = queue_map[lane]
        for fp, size, pc_name, drive_letter, ctime_iso, mtime_iso in files:
            target_queue.put((fp.as_posix(), size, pc_name, drive_letter, ctime_iso, mtime_iso))
            total_files_allocated += 1
            total_size_allocated += size
            
    report_metrics = {
        'total_files_scanned': len(all_files),
        'total_files_tiny': len(files_by_size['TINY']),
        'total_files_small': len(files_by_size['SMALL']),
        'total_files_medium': len(files_by_size['MEDIUM']),
        'total_files_large': len(files_by_size['LARGE']),
        'total_files_allocated': total_files_allocated,
        'total_size_allocated': total_size_allocated
    }
    return report_metrics


def split_upload_process(
    head_id: int, 
    collection_name: str, 
    root_path: str, 
    password: str, 
    target_file_queue: multiprocessing.Queue, 
    worker_count: int,
    pool_type: str,
    global_metrics_dict: Dict[str, Any],
    global_non_uploaded_list: List[Tuple], 
    global_log_queue: MPQueue
):
    
    MAX_IDLE_TIME_STEALING_SECONDS = 60.0 
    MAX_UPLOAD_WAIT_TIME_SECONDS = 25.0 
    
    engine = None
    
    try:
        engine = UploadEngine(collection_name, root_path, password, head_id, worker_count, pool_type, global_log_queue) 
        
        with Progress(
            TextColumn(f"[bold blue]HEAD P{head_id} ({pool_type}) - [/bold blue]"+"{task.description}"), 
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            console=engine.local_console
        ) as progress:
            
            overall_task = progress.add_task(f"[green]Fase 2: Uploads Ativos ({worker_count} Upload Workers)...", total=1)
            engine.set_progress_tracker(progress, overall_task)
            
            uploader_executor = ThreadPoolExecutor(max_workers=worker_count) 
            scanner_executor = ThreadPoolExecutor(max_workers=SCAN_WORKERS) 
            active_scanner_futures: Set[Any] = set()

            for i in range(worker_count):
                uploader_executor.submit(engine._upload_worker, progress, overall_task)

            last_activity_time = time.time() 
            engine._safe_print(f"[bold]Iniciando ciclo contínuo de scanning (Puxando da Fila {pool_type})...[/bold]", style=None)
            
            while True:
                
                # 1. Checa e Processa Scanners Concluídos
                completed_futures = set()
                for future in list(active_scanner_futures):
                    if future.done():
                        completed_futures.add(future)
                        try:
                            encrypted_size = future.result()
                            if encrypted_size > 0 and engine.progress_tracker:
                                current_total = engine.progress_tracker.tasks[overall_task].total
                                engine.progress_tracker.update(overall_task, total=current_total + encrypted_size)
                        except Exception:
                            pass
                            
                active_scanner_futures -= completed_futures
                
                # 2. Lógica de Pacing (Controle de fluxo)
                if scanner_executor._max_workers == len(active_scanner_futures) or engine.upload_queue.qsize() > worker_count * 5:
                    time.sleep(0.1)
                    continue

                # 3. Pull da Fila Específica (Non-blocking pull)
                file_batch = []
                pull_size = SCAN_WORKERS 
                
                try:
                    for _ in range(pull_size): 
                        file_batch.append(target_file_queue.get(timeout=0.001)) 
                except multiprocessing.Queue.empty:
                    pass 
                
                if file_batch:
                    last_activity_time = time.time() 
                    
                    for file_info in file_batch:
                        file_info_tuple = (
                            Path(file_info[0]), file_info[1], file_info[2], file_info[3], file_info[4], file_info[5]
                        )
                        future = scanner_executor.submit(engine._scan_and_hash_worker_unified, file_info_tuple)
                        active_scanner_futures.add(future)
                        
                    time.sleep(0.01) 
                
                # 4. Checagem de Conclusão Global (apenas para a fila atribuída)
                if target_file_queue.empty() and not active_scanner_futures:
                    current_idle_time = time.time() - last_activity_time
                    
                    if current_idle_time >= MAX_IDLE_TIME_STEALING_SECONDS: 
                        engine._safe_print(f"[bold yellow]Fila {pool_type} DRENADA ({current_idle_time:.1f}s ocioso). Transição para espera de Uploads.[/bold yellow]", style=None)
                        break
                    else:
                        time.sleep(0.5) 
                        
                time.sleep(0.05)


            # --- 5. SHUTDOWN SEQUENCE ---
            
            scanner_executor.shutdown(wait=True)
            engine._safe_print("[info]Scanners desligados. Aguardando conclusão de uploads remanescentes...[/info]")
            
            upload_wait_start = time.time()
            
            while not engine.upload_queue.empty() or len(engine.mpu_state) > 0:
                time.sleep(1)
                
                if time.time() - upload_wait_start > MAX_UPLOAD_WAIT_TIME_SECONDS:
                    engine._safe_print(f"[alert]Tempo de espera ({MAX_UPLOAD_WAIT_TIME_SECONDS}s) excedido. Forçando finalização dos MPUs abertos.[/alert]", style=None)
                    break
                    
                progress.update(overall_task, description=f"[bold magenta]Fase 3: Limpeza/Aguardando Uploads ({len(engine.mpu_state)} MPUs restantes)...[/bold magenta]")

            
            # Limpeza final dos MPUs incompletos/abortados
            with engine.mpu_lock:
                for s3_key in list(engine.mpu_state.keys()):
                    mpu_info = engine.mpu_state[s3_key]
                    if mpu_info.get('Status') not in ['ABORTED', 'FAILED', 'FAILED_CLEANUP']: 
                        engine._safe_print(f"[ALERTA] MPU Incompleto: {mpu_info['OriginalPath'].name}. Abortando e logando falha.", style="warning")
                        engine.log_db.log_file_status(
                            mpu_info['OriginalPath'], 'FAILED', mpu_info['OriginalFileSize'], 
                            s3_key=s3_key, error_detail="INCOMPLETE_ABORT_AT_END",
                            ctime_iso=mpu_info.get('CtimeISO'), mtime_iso=mpu_info.get('MtimeISO')
                        )
                        engine.metrics['failed'] += 1
                        engine._abort_mpu(s3_key)
                        
            # Sinaliza os workers de upload para encerrar
            for _ in range(worker_count): engine.upload_queue.put(None)
            uploader_executor.shutdown(wait=True) 

        # 6. Coleta de Métricas
        local_metrics = engine.metrics
        local_non_uploaded = engine.log_db.get_run_metrics() 
        
        for key, count in local_metrics.items():
            global_metrics_dict[f'P{head_id}_{key}'] = count
        
        global_non_uploaded_list.extend(local_non_uploaded)
                    
    except Exception as e:
        safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
        process_name = f"P{head_id}"
        if engine and hasattr(engine, '_safe_print'):
             engine._safe_print(Panel(f"[bold red]ERRO CRÍTICO NO PROCESSO {process_name}: [/bold red]{type(e).__name__}: {safe_error}", title="Falha no Processo", border_style="red"), style=None)
        else:
             Console(theme=custom_theme).print(Panel(f"[bold red]ERRO CRÍTICO NO PROCESSO {process_name}: [/bold red]{type(e).__name__}: {safe_error}", title="Falha no Processo", border_style="red"))
    finally:
        if engine and hasattr(engine, 'log_db'):
            try: engine.log_db.finalize_run(engine.metrics)
            except Exception as e: Console(theme=custom_theme).print(f"[error]Erro ao finalizar DB do Head {head_id}: {e}[/error]")


# Funções de utilidade necessárias no bloco principal
def generate_markdown_log(pc_name, collection_name, root_path, metrics, non_uploaded_files):
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_filename = LOG_DIR / f"upload_log_{pc_name}_{collection_name}_{timestamp}.md"
    
    total_non_uploaded = len(non_uploaded_files)

    log_content = f"# Relatório de Upload - {collection_name}\n"
    log_content += f"**PC/Host:** {pc_name}\n"
    log_content += f"**Pasta Raiz:** `{root_path}`\n"
    log_content += f"**Início da Sessão:** {datetime.now().isoformat()}\n"
    log_content += f"**Versão do Script:** V24.0.8-MULTILANE-FIX\n\n" 
    
    log_content += "## Resumo da Operação\n"
    log_content += f"| Status | Contagem |\n"
    log_content += f"| :--- | :---: |\n"
    log_content += f"| ✅ Arquivos Uploaded | {metrics['uploaded']} |\n"
    log_content += f"| ❌ Falhas Fatais | {metrics['failed']} |\n"
    log_content += f"| 🚫 Arquivos Ignorados | {metrics['ignored']} |\n\n"

    log_content += f"## Detalhes de Arquivos Não Uploadados ({total_non_uploaded})\n"
    if non_uploaded_files:
        log_content += "| Status | Nome do Arquivo | Caminho Completo | Detalhe do Erro |\n"
        log_content += "| :--- | :--- | :--- | :--- |\n"
        for path_full, name, status, error_detail in non_uploaded_files:
            log_content += f"| {status} | {name} | `{path_full}` | {error_detail} |\n"
    else:
        log_content += "Nenhum arquivo listado nas categorias FAILED ou IGNORED (após filtros).\n"

    try:
        with open(log_filename, 'w', encoding='utf-8') as f:
            f.write(log_content)
        console.print(f"\n[success]Relatório de log gerado em: {log_filename.as_posix()}[/success]")
    except Exception as e:
        console.print(f"[error]Falha ao salvar o log em Markdown: {e}[/error]")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    os.system('cls' if os.name == 'nt' else 'clear')
    console.print(Panel.fit("[bold white on green] MEGA CRYPTO HASH UPLOADER (V24.0.8-MULTILANE-FIX) [/bold white on green]", border_style="green"))
    
    target_folder = None
    collection_name = None
    password = None
    files_to_ignore_set = set()
    scan_mode = None
    processes = []
    ignore_hc_files = False 
    
    try:
        # 0. Setup Inicial do DB Schema & Integrity
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        LogCollectorProcess(MPQueue(), LOG_DB_PATH).setup_db_schema_if_not_exists()
        
        db_manager = DBIntegrityManager(LOG_DB_PATH)
        db_manager.backfill_missing_metadata()
        db_manager.fix_path_inconsistencies() # Correção crítica de caminhos

        # 0.1 PROMPT Performance Mode
        console.print("""
    Selecione o Modo de Performance:
    1: 🚀 TURBO (Alta Concorrência, para NVMe/PC Rápido)
    2: ⚡ MID (Concorrência Média, para HD/PC Modesto)
    3: 🐢 LIGHT (Baixa Concorrência, para I/O Limitado)
        """)
        mode_input = Prompt.ask("Escolha o Modo (1/2/3)", default="1", choices=['1', '2', '3'])
        performance_mode = int(mode_input)
        
        calculate_performance_params(performance_mode)

        # 0.2 PROMPT: Filtro .hc
        hc_choice = Prompt.ask("⚠️ Deseja [bold red]IGNORAR[/bold red] arquivos '.hc' (MEGA Hidden Files)? (Y/n)", default='y')
        if hc_choice.lower() in ['y', 'yes']:
            ignore_hc_files = True
            console.print("[warning]Arquivos .hc serão ignorados durante o scan.[/warning]")
        
        # 1. Seleção do Modo de Operação (Scan)
        # (Lógica de seleção e continuação permanece)
        past_runs = WorkerLogManager.get_past_runs_info(LOG_DB_PATH)

        if past_runs:
            console.print(Panel("[bold yellow]🚨 Execuções Anteriores Encontradas (Modo Continuação)[/bold yellow]", border_style="yellow"))
            run_map = {}
            for i, (run_id, pc_name, col_name, r_path, start_time) in enumerate(past_runs):
                display_index = i + 1
                start_dt = datetime.fromisoformat(start_time).strftime('%Y-%m-%d %H:%M:%S')
                display_pc_name = pc_name.split('-P')[0] if '-P' in pc_name else pc_name
                console.print(f"[bold magenta]{display_index}[/bold magenta]: [cyan]{col_name}[/cyan] (PC: {display_pc_name} | Pasta: {r_path} | Início: {start_dt})")
                run_map[str(display_index)] = {'run_id': run_id, 'collection_name': col_name, 'root_path': r_path}
                
            choice = Prompt.ask("Sua escolha (Número da Run / [bold cyan]N[/bold cyan]ova Run)", default="N")
            
            if choice.upper() in ['N', 'NOVA', 'NEW']:
                console.print("\n[bold green]Iniciando Nova Execução...[/bold green]")
            elif choice in run_map:
                selected_run = run_map[choice]
                collection_name = selected_run['collection_name']
                target_folder = selected_run['root_path']
                
                console.print(f"\n[bold green]✅ Modo Continuação Ativado.[/bold green]")
                password = Prompt.ask("🔑 Senha Mestra", password=False)
                if not password.strip(): sys.exit(1)
                    
                files_to_ignore_set = WorkerLogManager.get_uploaded_files_for_continuation(LOG_DB_PATH, collection_name, target_folder)
                console.print(f"[success]✅ Encontrados {len(files_to_ignore_set)} arquivos para ignorar.[/success]")
            else:
                console.print("[error]Escolha inválida. Encerrando.[/error]"); sys.exit(1)
        
        if target_folder is None:
            target_folder = select_folder_gui()
            if not target_folder: sys.exit(0)
                
            console.print(f"📁 Pasta Alvo: [bold yellow]{target_folder}[/bold yellow]")
            
            collection_name = Prompt.ask("📂 Nome da Coleção/Bucket (ex: Backup-Trabalho)", default="Storage-Principal")
            collection_name = "".join(c for c in collection_name if c.isalnum() or c in ('-', '_')).strip()
            if not collection_name: collection_name = "Storage"

            password = Prompt.ask("🔑 Digite a Senha Mestra", password=False)
            if not password.strip(): sys.exit(1)

        console.print("\nEscolha o Modo de Operação (Scan):")
        console.print(f"**{SCAN_MODE_FULL_DISK}**: Full Disk/Pasta Única (PC Local) [bold magenta]<- Modo Padrão[/bold magenta]")
        console.print(f"**{SCAN_MODE_MULTI_ORIGIN}**: Multi-Origin Backup (Pasta contém backups de vários PCs/Drives)")
        try:
            mode_choice = Prompt.ask("Sua escolha (1 ou 2)", default='1')
            scan_mode = int(mode_choice)
            if scan_mode not in [SCAN_MODE_FULL_DISK, SCAN_MODE_MULTI_ORIGIN]: raise ValueError
        except ValueError:
            scan_mode = SCAN_MODE_FULL_DISK
        
        # 3. Varredura e Alocação
        current_pc_name = socket.gethostname().lower().replace(' ', '-')
        
        file_groups, ignored_system_files_raw, ignored_folders_list = scan_and_orchestrate_files(
            target_folder, scan_mode, files_to_ignore_set, ignore_hc_files 
        )
        
        if not file_groups and not ignored_system_files_raw:
            console.print("[warning]Nenhum arquivo novo encontrado para upload após filtros.[/warning]")
            final_metrics = {'uploaded': len(files_to_ignore_set), 'ignored': len(ignored_folders_list), 'failed': 0}
            generate_markdown_log(current_pc_name, collection_name, target_folder, final_metrics, [])
            sys.exit(0)
            
        manager = Manager()
        
        tiny_file_queue = manager.Queue()
        small_file_queue = manager.Queue()
        medium_file_queue = manager.Queue()
        large_file_queue = manager.Queue()
        global_log_queue = manager.Queue() 
        
        orchestration_metrics = orchestrate_and_queue_split(
            file_groups, tiny_file_queue, small_file_queue, medium_file_queue, large_file_queue
        )
        
        console.print("\n--- Distribuição Inicial (MULTI-LANE) ---")
        console.print(f"Arquivos TINY (<1MB): [bold green]{orchestration_metrics['total_files_tiny']}[/bold green]")
        console.print(f"Arquivos SMALL (1-10MB): [bold green]{orchestration_metrics['total_files_small']}[/bold green]")
        console.print(f"Arquivos MEDIUM (10-100MB): [bold green]{orchestration_metrics['total_files_medium']}[/bold green]")
        console.print(f"Arquivos LARGE (>100MB): [bold green]{orchestration_metrics['total_files_large']}[/bold green]")
        console.print(f"Total de Upload Heads: {sum(NUM_PROCESSES.values())}")
        console.print(f"Total de Upload Workers (Threads): {TOTAL_UPLOAD_WORKERS}")
        console.print(f"Total de Dados Novos a Alocar: [bold cyan]{humanize.naturalsize(orchestration_metrics['total_size_allocated'])}[/bold cyan]")
        console.print("--------------------------\n")
        
        # 4. Inicia Processos (Log Collector + Heads)
        processes = []
        global_metrics = manager.dict() 
        global_non_uploaded = manager.list() 
        
        log_collector = LogCollectorProcess(global_log_queue, LOG_DB_PATH)
        processes.append(log_collector); log_collector.start()
        
        head_id_counter = 1
        lane_configs = [
            ('TINY', NUM_PROCESSES['TINY'], WORKER_THREADS['TINY'], tiny_file_queue),
            ('SMALL', NUM_PROCESSES['SMALL'], WORKER_THREADS['SMALL'], small_file_queue),
            ('MEDIUM', NUM_PROCESSES['MEDIUM'], WORKER_THREADS['MEDIUM'], medium_file_queue),
            ('LARGE', NUM_PROCESSES['LARGE'], WORKER_THREADS['LARGE'], large_file_queue),
        ]

        for lane, num_heads, num_workers, target_queue in lane_configs:
            for i in range(num_heads):
                p = Process(target=split_upload_process, args=(
                    head_id_counter, collection_name, target_folder, password, 
                    target_queue, num_workers, lane,
                    global_metrics, global_non_uploaded, global_log_queue,
                ))
                processes.append(p)
                p.start()
                head_id_counter += 1
        
        # 5. Espera a conclusão dos Heads de Upload/Scanning
        upload_heads = [p for p in processes if p != log_collector] 
        for p in upload_heads: p.join()
            
        console.print("\n[success]TODAS AS HEADS DE UPLOAD CONCLUÍRAM. Sinalizando Log Collector.[/success]")
        
        log_collector.stop(); log_collector.join()

        # 6. Agregação final
        total_uploaded_this_run = sum(global_metrics.get(f'P{i}_uploaded', 0) for i in range(1, head_id_counter))
        total_failed = sum(global_metrics.get(f'P{i}_failed', 0) for i in range(1, head_id_counter))
        total_ignored_worker = sum(global_metrics.get(f'P{i}_ignored', 0) for i in range(1, head_id_counter))
        
        final_metrics = {
            'uploaded': total_uploaded_this_run + len(files_to_ignore_set),
            'failed': total_failed,
            'ignored': total_ignored_worker + len(ignored_system_files_raw) + len(ignored_folders_list),
        }

        final_non_uploaded_report = list(global_non_uploaded)
        
        for fp, size, reason in ignored_system_files_raw:
            if reason == 'CONTINUATION_IGNORE' and str(fp.as_posix()) in files_to_ignore_set: continue
            final_non_uploaded_report.append((str(fp), Path(fp).name, 'IGNORED', reason))
                
        for folder_path in ignored_folders_list:
            final_non_uploaded_report.append((folder_path, Path(folder_path).name, 'IGNORED_FOLDER', 'SYSTEM_FOLDER_FILTER'))
        
        
        generate_markdown_log(current_pc_name, collection_name, target_folder, final_metrics, final_non_uploaded_report)
        
        final_panel_content = (
            f"[bold green]OPERAÇÃO GLOBAL FINALIZADA! (LOGGING ROBUSTO GARANTIDO)[/bold green]\n\n"
            f"📦 Coleção: [cyan]{collection_name}[/cyan]\n"
            f"✅ Uploads Concluídos: [success]{final_metrics['uploaded']}[/success] (Incluindo {len(files_to_ignore_set)} da Continuação)\n"
            f"🚫 Arquivos Ignorados: [warning]{final_metrics['ignored']}[/warning]\n"
            f"❌ Falhas Fatais (Retry Esgotado/MPU Abortado): [error]{final_metrics['failed']}[/error]\n\n"
            "Relatório detalhado salvo em arquivo .md."
        )
        console.print(Panel(final_panel_content, title="[bold blue]Conclusão Global do Processo[/bold blue]"), style=None)
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupção Manual. Encerrando de forma limpa...[/yellow]")
    except Exception as e:
        console.print(Panel(f"[bold red]ERRO CRÍTICO NO CONTROLADOR DE PROCESSOS: [/bold red]{type(e).__name__}: {e}", title="Falha Inesperada", border_style="red"))
    finally:
        
        if 'log_collector' in locals() and log_collector.is_alive():
             log_collector.stop()
             log_collector.join(timeout=10)
             
        for p in processes:
            if p.is_alive():
                p.terminate()
        
    console.print("\n[bold magenta]*** FIM DA SESSÃO ***[/bold magenta]")
    input("Pressione ENTER para fechar...")
