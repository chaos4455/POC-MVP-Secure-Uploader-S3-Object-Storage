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

# ========================================================================
# === CONFIGURAÇÕES GLOBAIS ===
# ========================================================================
MEGA_S3_ENDPOINT = "XXX"
ACCESS_KEY = "XXX"
SECRET_KEY = "XXX"

# === REVISED GLOBAL CONSTANTS FOR V24.0.2 (Performance Refactor) ===
NUM_PROCESSES_LARGE = 8 
NUM_PROCESSES_SMALL = 4 
NUM_TOTAL_PROCESSES = NUM_PROCESSES_LARGE + NUM_PROCESSES_SMALL 

# Threads por Head (Processo)
LARGE_UPLOAD_WORKERS = 22   
SMALL_UPLOAD_WORKERS = 6    
MICRO_UPLOAD_WORKERS = 15   
SCAN_WORKERS = 8            

WORKERS_PER_HEAD_LARGE = LARGE_UPLOAD_WORKERS + SMALL_UPLOAD_WORKERS 
WORKERS_PER_HEAD_SMALL = MICRO_UPLOAD_WORKERS + SCAN_WORKERS          
TOTAL_UPLOAD_WORKERS = (NUM_PROCESSES_LARGE * WORKERS_PER_HEAD_LARGE) + (NUM_PROCESSES_SMALL * WORKERS_PER_HEAD_SMALL)

# Max Connections: Deve acomodar todos os workers + overhead
MAX_POOL_CONNECTIONS = TOTAL_UPLOAD_WORKERS + (NUM_TOTAL_PROCESSES * SCAN_WORKERS) + 50 

MPU_PART_SIZE = 32 * 1024 * 1024 
CHUNK_SIZE = MPU_PART_SIZE 

NONCE_SIZE = 8 
MAX_RETRIES = 100 
RETRY_DELAY = 20 

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

VIDEO_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', 
    '.m4v', '.mpg', '.mpeg', '.3gp', '.ts', '.m2ts', '.vob'
}

# LIMITES DE TAMANHO 
SIZE_100MB = 100 * 1024 * 1024
SIZE_1GB = 1 * 1024 * 1024 * 1024
SIZE_5GB = 5 * 1024 * 1024 * 1024

# NOVOS LIMITES DE TAMANHO (Small File Accelerator)
SIZE_10MB = 10 * 1024 * 1024
SIZE_1MB = 1 * 1024 * 1024
SIZE_100KB = 100 * 1024

# Mapeamento Lógico H1-H16 (Lógica de Alocação Inicial)
HEAD_GROUPS = {
    # GRUPOS LARGE (8 Heads, H1-H8) - Primários
    1: {'name': 'TOP_A (>10M)', 'size_pool': 'LARGE', 'primary_queues': [1], 'small_steal_initial': [9, 10]},
    2: {'name': 'TOP_B (>10M)', 'size_pool': 'LARGE', 'primary_queues': [2], 'small_steal_initial': [11, 12]},
    3: {'name': 'VIDEO_SMALL (>10M)', 'size_pool': 'LARGE', 'primary_queues': [3], 'small_steal_initial': [13]},
    4: {'name': 'VIDEO_LARGE (>10M)', 'size_pool': 'LARGE', 'primary_queues': [4], 'small_steal_initial': [14]},
    5: {'name': 'GEN_MED_100MB (>10M)', 'size_pool': 'LARGE', 'primary_queues': [5], 'small_steal_initial': [15]},
    6: {'name': 'GEN_MED_1GB (>10M)', 'size_pool': 'LARGE', 'primary_queues': [6], 'small_steal_initial': [16]},
    7: {'name': 'GEN_BIG_5GB (>10M)', 'size_pool': 'LARGE', 'primary_queues': [7], 'small_steal_initial': []},
    8: {'name': 'GEN_HUGE (>5GB)', 'size_pool': 'LARGE', 'primary_queues': [8], 'small_steal_initial': []},
    
    # GRUPOS SMALL (H9-H16) - Secundários (Consumidos pelos Micro-Processos P9-P12)
    9: {'name': 'TINY_100KB_A', 'size_pool': 'SMALL', 'primary_queues': [9]},
    10: {'name': 'TINY_100KB_B', 'size_pool': 'SMALL', 'primary_queues': [10]},
    11: {'name': 'SMALL_1MB_A', 'size_pool': 'SMALL', 'primary_queues': [11]},
    12: {'name': 'SMALL_1MB_B', 'size_pool': 'SMALL', 'primary_queues': [12]},
    13: {'name': 'MED_10MB_A', 'size_pool': 'SMALL', 'primary_queues': [13]},
    14: {'name': 'MED_10MB_B', 'size_pool': 'SMALL', 'primary_queues': [14]},
    15: {'name': 'MED_10MB_C', 'size_pool': 'SMALL', 'primary_queues': [15]},
    16: {'name': 'MED_10MB_D', 'size_pool': 'SMALL', 'primary_queues': [16]},
}

# Mapeamento Processual (Físico) para Fila (Lógica)
PROCESS_QUEUE_MAP = {
    h_id: [h_id] + HEAD_GROUPS[h_id].get('small_steal_initial', []) # Primárias (Q1..8) + Steal Inicial Small
    for h_id in range(1, NUM_PROCESSES_LARGE + 1)
}
PROCESS_QUEUE_MAP_SMALL = {
    9: [9, 13], 10: [10, 14], 11: [11, 15], 12: [12, 16]
}


custom_theme = Theme({
    "info": "cyan", "warning": "yellow", "error": "bold red", "success": "bold green",
    "key": "bold magenta", "path": "dim white", "retry": "yellow on black", "alert": "bold yellow on red"
})
console = Console(theme=custom_theme)

LOG_DIR = Path("C:\\MEGA-SYNC-UPLODER-CONTROL-DATA")
LOG_DB_PATH = LOG_DIR / "control_data.db"

# MODIFICAÇÃO CIRÚRGICA 1: Adiciona ctime e mtime (6 elementos)
FileInfoTuple = Tuple[Path, int, str, str, str, str]

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
        
        try:
            # OPTIMIZATION: Use larger buffer for open to assist disk I/O
            self._f = open(path, 'rb', buffering=MPU_PART_SIZE)
            stat_info = path.stat()
            file_size_raw = stat_info.st_size
            
            s3_dir_path = str(Path(original_path_key).parent) 
            
            # NOVO: Inclusão de metadados no header de criptografia
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
             metadata_json = json.dumps({"path": original_path_key, "error": f"FILE_ACCESS_ERROR: {type(e).__name__}"}).encode('utf-8')
             
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
                     if remaining_size == 0:
                         break
            else:
                data_to_read_raw = min(remaining_size, self.total_size - self.active_pos)
                
                if data_to_read_raw == 0:
                    if self.active_encryptor:
                        try:
                            final_chunk = self.active_encryptor.finalize()
                            self.active_encryptor = None
                            if final_chunk:
                                self.active_pos += len(final_chunk)
                                chunks.append(final_chunk)
                        except Exception:
                            pass 
                    break
                
                raw_data = self._f.read(data_to_read_raw)
                
                if not raw_data:
                    if self.active_encryptor:
                        try:
                            final_chunk = self.active_encryptor.finalize()
                            self.active_encryptor = None
                            if final_chunk:
                                self.active_pos += len(final_chunk)
                                chunks.append(final_chunk)
                        except Exception:
                            pass 
                    break
                    
                encrypted_data = self.active_encryptor.update(raw_data)
                self.active_pos += len(encrypted_data)
                chunks.append(encrypted_data)
                remaining_size -= len(encrypted_data)
                
                if len(encrypted_data) < data_to_read_raw:
                    break
                    
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

# ========================================================================
# === NOVO: CLASSES DB INTEGRITY (MIGRAÇÃO E BACKFILL) ===
# ========================================================================

class DBIntegrityManager:
    """Gerencia a migração do esquema DB e o backfill de metadados de arquivos UPLOADED antigos."""
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.local_console = Console(theme=custom_theme)
    
    def _safe_print(self, text, style=None):
        prefix = f"[DB_INTEGRITY]"
        if style:
            self.local_console.print(f"{prefix}[{style}]{text}[/{style}]")
        else:
            self.local_console.print(f"{prefix}{text}")

    def backfill_missing_metadata(self):
        
        self._safe_print("[info]Iniciando rotina de backfill de metadados para registros UPPLOADED antigos...[/info]")
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()

            # 1. Seleciona registros UPLOADED que estão faltando as datas
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
            if not records_to_update:
                self._safe_print("[success]Nenhum registro 'UPLOADED' antigo pendente de backfill de metadados.[/success]")
                return

            self._safe_print(f"[warning]Encontrados {len(records_to_update)} registros 'UPLOADED' antigos sem metadados de data. Tentando backfill...[/warning]")

            update_count = 0
            
            for record_id, file_path_full, existing_size in records_to_update:
                fp = Path(file_path_full)
                
                if fp.exists():
                    try:
                        stat_info = fp.stat()
                        
                        ctime_iso = datetime.fromtimestamp(stat_info.st_ctime).isoformat()
                        mtime_iso = datetime.fromtimestamp(stat_info.st_mtime).isoformat()
                        current_size = stat_info.st_size
                        
                        # Garante que o tamanho também está correto
                        if existing_size != current_size:
                             self._safe_print(f"[alert]Aviso: Tamanho do arquivo {fp.name} (DB: {existing_size}, Disk: {current_size}) inconsistente. Corrigindo no DB.[/alert]")

                        cursor.execute("""
                            UPDATE file_status 
                            SET 
                                original_creation_date = ?, 
                                original_modification_date = ?,
                                file_size_bytes = ? 
                            WHERE id = ?
                        """, (ctime_iso, mtime_iso, current_size, record_id))
                        
                        update_count += 1
                        
                    except Exception:
                        # Arquivo existe, mas falha ao ler metadados (permissão, I/O error)
                        pass 
                else:
                    # Arquivo original não existe mais (não pode ser backfilled)
                    pass 
            
            conn.commit()
            if update_count > 0:
                self._safe_print(f"[success]✅ Backfill concluído. {update_count} registros atualizados com sucesso.[/success]")

        except Exception as e:
            self._safe_print(f"[error]ERRO CRÍTICO no Backfill do DB: {e}[/error]", style="error")
        finally:
            if conn: conn.close()
            
# ========================================================================
# === LOG COLLECTOR PROCESS (MODIFICADO PARA SUPORTAR NOVO SCHEMA) ===
# ========================================================================

class LogCollectorProcess(Process):
    """
    Processo dedicado a receber logs de todas as Heads e gravá-los no DB de forma robusta.
    """
    
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
        # O Collector é o único que mantém a conexão principal aberta.
        self.conn = sqlite3.connect(self.db_path, timeout=30) 
        self.cursor = self.conn.cursor()

    def _flush_batch(self):
        if not self.batch_entries:
            self.last_flush_time = time.time()
            return
            
        try:
            # MODIFICAÇÃO CIRÚRGICA 3: SQL atualizado para 11 colunas
            # Entry format: (run_id, pc_name, drive_letter, file_path_full, file_name, file_size_bytes, status, s3_key, error_detail, original_creation_date, original_modification_date)
            sql = """
                INSERT INTO file_status 
                (run_id, pc_name, drive_letter, file_path_full, file_name, file_size_bytes, status, s3_key, error_detail, original_creation_date, original_modification_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            
            # Fallback para gravação individual
            sql = """
                INSERT INTO file_status 
                (run_id, pc_name, drive_letter, file_path_full, file_name, file_size_bytes, status, s3_key, error_detail, original_creation_date, original_modification_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            saved_count = 0
            for entry in self.batch_entries:
                try:
                    self.cursor.execute(sql, entry)
                    saved_count += 1
                except Exception as individual_e:
                    self._safe_print(f"[error]Falha ao gravar entrada individual: {individual_e}[/error]")
            
            if saved_count > 0:
                self.conn.commit()
                self._safe_print(f"[warning]⚠️ Gravados {saved_count} logs individualmente após falha no batch.[/warning]")
                
            self.batch_entries = []
            self.last_flush_time = time.time()


    def run(self):
        
        # REMOVIDO: self._setup_db_schema_if_not_exists() - A migração deve ser feita no main thread.
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
                pass 
            except Exception as e:
                self._safe_print(f"[error]Erro inesperado ao obter item da fila: {e}[/error]")
                time.sleep(1)
                continue
            
            if item:
                # Item format agora tem 11 elementos
                if item == "STOP_SIGNAL":
                    break
                
                try:
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
        
        if self.conn:
            self.conn.close()
        self._safe_print("Conexão DB fechada. Log Collector Encerrado.")

    def stop(self):
        self._stop_event.set()
        self.log_queue.put("STOP_SIGNAL") 

    # NOVO: Função auxiliar para adicionar colunas de forma segura (para migração)
    def _check_and_add_column(self, cursor: sqlite3.Cursor, table_name: str, column_name: str, column_type: str):
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [info[1] for info in cursor.fetchall()]
        if column_name not in columns:
            console.print(f"[warning]Migrando schema: Adicionando coluna '{column_name}' a '{table_name}'.[/warning]")
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
            except Exception as e:
                console.print(f"[error]Falha ao adicionar coluna {column_name}: {e}[/error]")
                raise
                
    # MODIFICAÇÃO CIRÚRGICA 2: Schema Migration no início
    def setup_db_schema_if_not_exists(self):
        # Este método deve ser chamado apenas uma vez no Main Thread
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Tabela run_log (estrutura original mantida)
            cursor.execute("""
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
            
            # Tabela file_status (estrutura original, que será migrada abaixo)
            cursor.execute("""
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
            
            # 3. MIGRATION/INTEGRITY CHECK: Adiciona colunas faltantes
            self._check_and_add_column(cursor, "file_status", "original_creation_date", "TEXT")
            self. _check_and_add_column(cursor, "file_status", "original_modification_date", "TEXT")

            conn.commit()
        except Exception as e:
            console.print(f"[error]ERRO CRÍTICO ao configurar o Schema DB: {e}[/error]")
            sys.exit(1)
        finally:
            if conn: conn.close()


class WorkerLogManager:
    """
    Gerencia o ID de execução (run_id) para um processo worker específico 
    e envia logs de status de arquivos para o LogCollectorProcess central.
    """
    def __init__(self, pc_name: str, root_path: str, collection: str, worker_id: int, log_queue: MPQueue, db_path: Path = LOG_DB_PATH):
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        self.db_path = db_path
        self.pc_name = pc_name
        self.root_path = root_path
        self.drive_letter = Path(root_path).parts[0].replace('\\', '').replace(':', '') if root_path and len(Path(root_path).parts) > 0 else 'UNKNOWN'
        self.collection = collection
        self.worker_id = worker_id 
        
        self.log_queue = log_queue
        
        # Conexão temporária e local para gerenciar APENAS a tabela run_log
        self.conn_run_log = sqlite3.connect(self.db_path, timeout=5)
        self.cursor_run_log = self.conn_run_log.cursor()
        
        self.run_id = self._setup_run_log_entry() 
        self.timestamp_start = datetime.now()


    def _setup_run_log_entry(self):
        # Esta função é chamada uma vez por Head (Processo)
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

    # MODIFICAÇÃO CIRÚRGICA 4: Adiciona ctime_iso e mtime_iso
    def log_file_status(self, 
        file_path: Path, 
        status: str, 
        file_size: int, 
        s3_key: Optional[str] = None, 
        error_detail: Optional[str] = None,
        ctime_iso: Optional[str] = None, 
        mtime_iso: Optional[str] = None 
    ):
        """
        Envia a entrada de log (agora com 11 campos) para a fila central do Log Collector.
        """
        safe_error_detail = str(error_detail).encode('utf-8', 'ignore').decode('utf-8') if error_detail else None
        safe_s3_key = s3_key if s3_key else None
        
        log_entry = (
            self.run_id, self.pc_name, self.drive_letter, 
            file_path.as_posix(), file_path.name, file_size, 
            status, safe_s3_key, safe_error_detail,
            ctime_iso, mtime_iso # Novos campos
        )
        self.log_queue.put(log_entry)

    def finalize_run(self, counts: dict):
        self.timestamp_end = datetime.now()
        
        # Atualiza a tabela run_log localmente
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
        # Busca apenas os status de arquivos para ESTA run_id que não foram 'UPLOADED'
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
        if not db_path.exists():
            return []
        
        conn = None
        try:
            conn = sqlite3.connect(db_path, timeout=30)
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
            conn = sqlite3.connect(db_path, timeout=30)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    file_path_full 
                FROM 
                    file_status 
                WHERE 
                    status = 'UPLOADED' AND 
                    drive_letter = ? AND 
                    run_id IN (
                        SELECT run_id 
                        FROM run_log 
                        WHERE collection_name = ?
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
            if self.conn_run_log: self.conn_run_log.close()
        except:
            pass


class UploadEngine:

    def __init__(self, collection_name, root_path, password, head_id: int, upload_worker_count: int, global_log_queue: MPQueue, pool_type: str = 'LARGE'):
        self.root_path = Path(root_path)
        self.collection = collection_name
        self.head_id = head_id 
        self.upload_worker_count = upload_worker_count
        self.pool_type = pool_type
        self.crypto = CryptoEngine()
        
        self.disk_io_semaphore = threading.Semaphore(4) 
        
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
        self._safe_print(f"[info]📊 HEAD P{self.head_id} ({self.pool_type} Pool) | Workers: {self.upload_worker_count} | Log DB ID: {self.log_db.run_id}[/info]", style=None)
        
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


    def _calculate_content_hash_from_stat(self, file_path: Path, stat_info: os.stat_result) -> str:
        file_size = stat_info.st_size
        mod_time = stat_info.st_mtime_ns
        
        rel_path = self._get_original_path_metadata(file_path)
        
        data_to_hash = f"{rel_path}-{file_size}-{mod_time}".encode('utf-8')

        hasher = hashlib.sha512()
        hasher.update(data_to_hash)

        return hasher.hexdigest()

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

    # MODIFICAÇÃO CIRÚRGICA 7: Recebe a tupla de 6 elementos (incluindo ctime/mtime)
    def _scan_and_hash_worker_multi_head(self, file_path_tuple: FileInfoTuple):
        fp = Path(file_path_tuple[0])
        file_size_original_from_queue = file_path_tuple[1]
        pc_name = file_path_tuple[2]
        drive_letter = file_path_tuple[3]
        ctime_iso = file_path_tuple[4] # NOVO
        mtime_iso = file_path_tuple[5] # NOVO
        s3_key_unique = None
        
        try:
            stat_info = fp.stat() 
            file_size = stat_info.st_size
            
            if file_size == 0:
                self._safe_print(f"[VAZIO] Ignorando {fp.name}: Arquivo vazio.", style="warning")
                # Log com as novas datas
                self.log_db.log_file_status(fp, 'IGNORED', 0, s3_key=None, error_detail="ZERO_BYTE_SIZE", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                self.metrics['ignored'] += 1
                return 0
            
            original_metadata_path = Path(pc_name) / drive_letter / self._get_original_path_metadata(fp)
            
            content_hash = self._calculate_content_hash_from_stat(fp, stat_info)
            if not content_hash: 
                # Log com as novas datas
                self.log_db.log_file_status(fp, 'IGNORED', file_size, s3_key=None, error_detail="HASH_METADATA_ERROR", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                self.metrics['ignored'] += 1
                return 0
            
            s3_key_unique = self._create_s3_key(content_hash)
            
            # CRIAÇÃO DO STREAM TEMPORÁRIO PARA CALCULAR O TAMANHO ENCRIPTADO
            with self.crypto.create_encrypted_stream(fp, original_metadata_path.as_posix()) as temp_stream:
                encrypted_size = temp_stream.total_size
            
            # Inicia o MPU no S3
            response = self.s3.create_multipart_upload(
                Bucket=self.clean_bucket_name,
                Key=s3_key_unique,
            )
            upload_id = response['UploadId']
            
            num_parts = (encrypted_size + MPU_PART_SIZE - 1) // MPU_PART_SIZE
            
            # Registra o estado atômico do MPU (incluindo as datas no estado)
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
                    'CtimeISO': ctime_iso, # NOVO: Armazena data no estado MPU
                    'MtimeISO': mtime_iso, # NOVO: Armazena data no estado MPU
                    'Status': 'ACTIVE'
                }
            
            # Enfileira todas as partes para os UPLOAD WORKERS
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
            
            # Log com as novas datas
            self.log_db.log_file_status(fp, 'IGNORED', file_size_original_from_queue, s3_key=s3_key_unique, error_detail=f"PROCESSING_ERROR: {type(e).__name__}: {safe_error}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
            self.metrics['ignored'] += 1
            
            if s3_key_unique:
                self._abort_mpu(s3_key_unique)
                
            return 0


    def _upload_worker(self, progress: Progress, overall_task: int):
        
        while True:
            try:
                item: Optional[Tuple] = self.upload_queue.get() 
            except Exception:
                time.sleep(1)
                continue

            if item is None:
                break
                
            s3_key, upload_id, part_number, offset, length, file_path, file_size = item
            
            etag = None
            chunk_data = None
            ctime_iso = None
            mtime_iso = None

            try:
                # 1. Checagem inicial do estado do MPU
                with self.mpu_lock:
                    mpu_info = self.mpu_state.get(s3_key)
                    if not mpu_info or mpu_info.get('FatalError') or mpu_info.get('Status') != 'ACTIVE':
                        self.upload_queue.task_done()
                        continue
                    
                    original_path_metadata = mpu_info['OriginalPath'].as_posix()
                    pc_name_meta = mpu_info['PCName']
                    drive_letter_meta = mpu_info['DriveLetter']
                    ctime_iso = mpu_info['CtimeISO'] # Extrai datas
                    mtime_iso = mpu_info['MtimeISO'] # Extrai datas
                
                full_metadata_path = Path(pc_name_meta) / drive_letter_meta / self._get_original_path_metadata(file_path)

                # 2. Leitura Just-In-Time (JIT) do Chunk (Com Semáforo para Limitar I/O de Disco/CPU)
                with self.disk_io_semaphore:
                    with self.crypto.create_encrypted_stream(file_path, full_metadata_path.as_posix()) as stream:
                        stream.seek(offset)
                        chunk_data = stream.read(length) 

                if chunk_data is None or len(chunk_data) != length:
                    self._safe_print(f"[ERROR] ERRO READ: Falha ao ler {length} bytes de {file_path.name}. Abortando MPU.", style="error")
                    self._abort_mpu(s3_key)
                    # Log com datas
                    self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail="READ_FAIL_DISK_IO", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
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
                                    # Log com datas
                                    self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"PART_FATAL_S3_STATE: {error_code}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
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
                            # Log com datas
                            self.log_db.log_file_status(file_path, 'UPLOADED', file_size, s3_key=s3_key, ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                            self.metrics['uploaded'] += 1
                            del self.mpu_state[s3_key]
                            
                        except ClientError as completion_e:
                            error_code = completion_e.response.get('Error', {}).get('Code', 'Unknown')
                            safe_completion_error = str(completion_e).encode('utf-8', 'ignore').decode('utf-8')
                            
                            if error_code == 'NoSuchUpload':
                                self._safe_print(f"❌ FALHA NA FINALIZAÇÃO (NoSuchUpload): {file_path.name}. O MPU não existe mais no S3. Limpando estado local.", style="error")
                                
                                mpu_info['FatalError'] = True
                                mpu_info['Status'] = 'FAILED_CLEANUP'
                                # Log com datas
                                self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"COMPLETION_FAIL_NOSUCHUPLOAD: {safe_completion_error}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                                self.metrics['failed'] += 1
                                del self.mpu_state[s3_key] 
                            else:
                                self._safe_print(f"❌ FALHA NA FINALIZAÇÃO (COMPLETION/ClientError): {file_path.name}: {safe_completion_error}. ABORTANDO.", style="error")
                                
                                mpu_info['FatalError'] = True
                                mpu_info['Status'] = 'FAILED'
                                # Log com datas
                                self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"COMPLETION_FAIL: {safe_completion_error}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                                self.metrics['failed'] += 1
                                self._abort_mpu(s3_key)
                                
                        except Exception as completion_e:
                            safe_completion_error = str(completion_e).encode('utf-8', 'ignore').decode('utf-8')
                            self._safe_print(f"❌ FALHA NA FINALIZAÇÃO (GENÉRICA): {file_path.name}: {safe_completion_error}. ABORTANDO.", style="error")
                            
                            mpu_info['FatalError'] = True
                            mpu_info['Status'] = 'FAILED'
                            # Log com datas
                            self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"COMPLETION_FAIL_GENERIC: {safe_completion_error}", ctime_iso=ctime_iso, mtime_iso=mtime_iso)
                            self.metrics['failed'] += 1
                            self._abort_mpu(s3_key)
                    
            except Exception as e:
                safe_error = str(e).encode('utf-8', 'ignore').decode('utf-8')
                
                is_fatal_error = ("FatalError" in safe_error or "NoSuchUpload" in safe_error or "InvalidPart" in safe_error)
                
                if not is_fatal_error:
                    self._safe_print(f"❌ FALHA PERSISTENTE (PARTE {part_number}): {file_path.name}: {safe_error}. ABORTANDO MPU.", style="error")
                
                else:
                    self._safe_print(f"❌ FALHA FATAL (PARTE {part_number}): {file_path.name}: {safe_error}", style="error")
                    
                
                with self.mpu_lock:
                    mpu_info = self.mpu_state.get(s3_key)
                    if mpu_info and mpu_info.get('Status') == 'ACTIVE':
                        # Log com datas
                        self.log_db.log_file_status(file_path, 'FAILED', file_size, s3_key=s3_key, error_detail=f"PART_FATAL: {safe_error}", ctime_iso=mpu_info.get('CtimeISO'), mtime_iso=mpu_info.get('MtimeISO'))
                        self.metrics['failed'] += 1
                        self._abort_mpu(s3_key)
                        
                        try:
                            if mpu_info:
                                progress.update(overall_task, advance=mpu_info['EncryptedSize'])
                        except Exception:
                            pass 

            finally:
                self.upload_queue.task_done()
                
    
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

# MODIFICAÇÃO CIRÚRGICA 5: Coleta de datas e retorno de tupla de 6 elementos
def scan_and_orchestrate_files(
    target_folder: str, 
    scan_mode: int,
    files_to_ignore_set: Set[str]
) -> Tuple[Dict[Tuple[str, str], List[FileInfoTuple]], List[Tuple[Path, int]], List[str]]:
    base_path = Path(target_folder)
    all_files_on_disk: List[Tuple[Path, int, str, str]] = []
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
            
            try:
                stat_info = file_path.stat()
                size = stat_info.st_size
                ctime_iso = datetime.fromtimestamp(stat_info.st_ctime).isoformat()
                mtime_iso = datetime.fromtimestamp(stat_info.st_mtime).isoformat()
            except Exception:
                size = 0
                ctime_iso = None
                mtime_iso = None

            if file_path.name.lower() in FORCED_IGNORE_FILENAMES or file_path.name == KEY_FILE.name:
                ignored_system_files.append((file_path, size))
                continue
            
            if str(file_path) in files_to_ignore_set:
                ignored_system_files.append((file_path, size))
                continue
                
            # Se chegamos aqui, o arquivo é válido e deve ser processado.
            all_files_on_disk.append((file_path, size, ctime_iso, mtime_iso))

    
    file_groups: Dict[Tuple[str, str], List[FileInfoTuple]] = {}
    
    current_pc_name_local = socket.gethostname().lower().replace(' ', '-')
    
    for fp_full_info in all_files_on_disk:
        fp, size, ctime_iso, mtime_iso = fp_full_info
        
        if size == 0:
            ignored_system_files.append((fp, 0)) # Ignora arquivos vazios tardiamente
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
            
        # Retorna a tupla de 6 elementos
        file_groups[group_key].append((fp, size, pc_name, drive_letter, ctime_iso, mtime_iso))
    
    
    return file_groups, ignored_system_files, ignored_folders_list

def orchestrate_and_queue_hierarchical(
    # FileInfoTuple agora é a tupla de 6 elementos
    file_groups: Dict[Tuple[str, str], List[FileInfoTuple]],
    head_queues: Dict[int, multiprocessing.Queue] 
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    # A soma de tamanho (item[1]) ainda é válida.
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
    
# MODIFICAÇÃO CIRÚRGICA 6: Manipulação da tupla de 6 elementos para enfileiramento
def allocate_files_to_queues(
    pending_files: List[FileInfoTuple],
    queues: Dict[int, multiprocessing.Queue] 
) -> List[FileInfoTuple]:
    
    # files_to_process_info agora contém 7 elementos: (Path, size, pc_name, drive_letter, ctime_iso, mtime_iso, ext)
    files_to_process_info: List[Tuple[Path, int, str, str, str, str, str]] = []
    
    for fp, size, pc_name, drive_letter, ctime_iso, mtime_iso in pending_files:
        files_to_process_info.append((fp, size, pc_name, drive_letter, ctime_iso, mtime_iso, os.path.splitext(fp.name)[1].lower()))
        
    small_files = [f for f in files_to_process_info if f[1] <= SIZE_10MB]
    large_files = [f for f in files_to_process_info if f[1] > SIZE_10MB]
    
    # --- 1. ALOCAÇÃO DE ARQUIVOS PEQUENOS (H9-H16) ---
    small_files.sort(key=lambda x: x[1], reverse=False) 
    
    tiny_100kb = [f for f in small_files if f[1] <= SIZE_100KB]
    small_1mb = [f for f in small_files if SIZE_100KB < f[1] <= SIZE_1MB]
    med_10mb = [f for f in small_files if SIZE_1MB < f[1] <= SIZE_10MB]
    
    # H9, H10 (TINY)
    tiny_heads = [9, 10]
    for i, file_info in enumerate(tiny_100kb):
        # Enfileira 6 elementos (fp.as_posix(), size, pc_name, drive_letter, ctime, mtime)
        queues[tiny_heads[i % len(tiny_heads)]].put((file_info[0].as_posix(), file_info[1], file_info[2], file_info[3], file_info[4], file_info[5]))
        
    # H11, H12 (SMALL)
    small_heads = [11, 12]
    for i, file_info in enumerate(small_1mb):
        queues[small_heads[i % len(small_heads)]].put((file_info[0].as_posix(), file_info[1], file_info[2], file_info[3], file_info[4], file_info[5]))
        
    # H13, H14, H15, H16 (MEDIUM)
    med_heads = [13, 14, 15, 16]
    for i, file_info in enumerate(med_10mb):
        queues[med_heads[i % len(med_heads)]].put((file_info[0].as_posix(), file_info[1], file_info[2], file_info[3], file_info[4], file_info[5]))
        
    
    # --- 2. ALOCAÇÃO DE ARQUIVOS GRANDES (H1-H8) ---
    large_files.sort(key=lambda x: x[1], reverse=True) 
    
    top_1000 = large_files[:1000]
    remainder = large_files[1000:]
    
    temp_buckets = {1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 7: [], 8: []}
    
    # Distribuição Top 1000: H1 e H2 levam os maiores (Balanceamento de tamanho)
    top_1000.sort(key=lambda x: x[1], reverse=True)
    size_h1, size_h2 = 0, 0
    for file_info in top_1000:
        # CORREÇÃO APLICADA AQUI (Substituindo linhas 1560-1561)
        if size_h1 <= size_h2:
            temp_buckets[1].append(file_info)
            size_h1 += file_info[1]
        else:
            temp_buckets[2].append(file_info)
            size_h2 += file_info[1]
        
    # Restante (H3 a H8) - Lógica de categorização por tamanho e tipo
    for fp, size, pc_name, drive_letter, ctime_iso, mtime_iso, ext in remainder:
        file_info_tuple = (fp, size, pc_name, drive_letter, ctime_iso, mtime_iso, ext)
        
        if ext in VIDEO_EXTENSIONS:
            if size <= SIZE_1GB:
                temp_buckets[3].append(file_info_tuple)
            else:
                temp_buckets[4].append(file_info_tuple)
        else:
            if size <= SIZE_100MB:
                temp_buckets[5].append(file_info_tuple)
            elif size <= SIZE_1GB:
                temp_buckets[6].append(file_info_tuple) 
            elif size <= SIZE_5GB:
                temp_buckets[7].append(file_info_tuple) 
            else:
                temp_buckets[8].append(file_info_tuple) 
                
    # Enfileiramento: Smallest-First em cada Head (H1-H8)
    for head_id, files in temp_buckets.items():
        if not files: continue
        
        files.sort(key=lambda x: x[1], reverse=False)
        
        for fp, size, pc_name, drive_letter, ctime_iso, mtime_iso, _ in files:
             queues[head_id].put((fp.as_posix(), size, pc_name, drive_letter, ctime_iso, mtime_iso))
                
    return []

# Lista de todas as 16 filas lógicas
ALL_LARGE_QUEUES = list(range(1, 8 + 1))
ALL_SMALL_QUEUES = list(range(9, 16 + 1))
ALL_QUEUES = ALL_LARGE_QUEUES + ALL_SMALL_QUEUES

def get_next_target_universal_steal(
    primary_queues: List[int], 
    head_queues: Dict[int, multiprocessing.Queue]
) -> Optional[int]:
    
    # 1. Prioridade Máxima: Filas Primárias (Aggressive Pull)
    for q_id in primary_queues:
        if not head_queues[q_id].empty():
            return q_id
    
    primary_large_count = sum(1 for q_id in primary_queues if q_id in ALL_LARGE_QUEUES)
    
    if primary_large_count > 0:
        # Foco em Large (ou híbrido), prioriza Large stealing, depois Small
        all_other_large = [q_id for q_id in ALL_LARGE_QUEUES if q_id not in primary_queues]
        steal_targets = all_other_large + ALL_SMALL_QUEUES
    else:
        # Foco em Small, prioriza Small stealing, depois Large
        all_other_small = [q_id for q_id in ALL_SMALL_QUEUES if q_id not in primary_queues]
        steal_targets = all_other_small + ALL_LARGE_QUEUES
        
    for q_id in steal_targets:
        if not head_queues[q_id].empty():
            return q_id
            
    return None


# ========================================================================
# === PROCESSO DE REBALANCEAMENTO DINÂMICO DE FILA E MUTAÇÃO DE HEADS ===
# ========================================================================

class QueueRebalancerProcess(Process):
    def __init__(self, head_queues: Dict[int, MPQueue], stop_event: multiprocessing.Event, global_process_assignments: Dict[int, List[int]]):
        super().__init__()
        self.head_queues = head_queues
        self.stop_event = stop_event
        self.global_process_assignments = global_process_assignments
        
        self.rebalance_interval = 60 
        self.OVERLOAD_THRESHOLD = 50 
        self.MIN_MOVE_COUNT = 10 
        self.MIN_POOL_THRESHOLD = 5 
        self.NUM_PROCESSES_LARGE = NUM_PROCESSES_LARGE

    def _safe_print(self, text, style=None):
        console = Console(theme=custom_theme)
        prefix = f"[REBALANCER]"
        if style:
            console.print(f"{prefix}[{style}]{text}[/{style}]")
        else:
            console.print(f"{prefix}{text}")

    def run(self):
        self._safe_print("[info]Iniciando monitoramento de filas para rebalanceamento dinâmico...[/info]")
        
        while not self.stop_event.is_set():
            time.sleep(self.rebalance_interval)
            
            if self.stop_event.is_set():
                break

            try:
                queue_sizes = {q_id: q.qsize() for q_id, q in self.head_queues.items()}
                total_files_remaining = sum(queue_sizes.values())
                
                if total_files_remaining == 0:
                    self._safe_print("[info]Todas as filas estão vazias. Rebalancer em espera passiva.[/info]")
                    continue

                large_q_sizes = {q_id: size for q_id, size in queue_sizes.items() if q_id in ALL_LARGE_QUEUES}
                small_q_sizes = {q_id: size for q_id, size in queue_sizes.items() if q_id in ALL_SMALL_QUEUES}

                total_small_files = sum(small_q_sizes.values())
                total_large_files = sum(large_q_sizes.values())
                
                # 1. VERTICAL MIGRATION / PROCESS MUTATION
                
                # Case A: Small Pool Exhausted (P9-P12 mutam para assistir Large)
                if total_small_files < self.MIN_POOL_THRESHOLD and total_large_files > self.OVERLOAD_THRESHOLD:
                    
                    idle_small_processes = list(range(self.NUM_PROCESSES_LARGE + 1, NUM_TOTAL_PROCESSES + 1)) 
                    active_large_queues = [q_id for q_id, size in large_q_sizes.items() if size > 0]
                    
                    if active_large_queues:
                        for i, p_id in enumerate(idle_small_processes):
                            map_id = i 
                            
                            q1_idx = (map_id * 2) % len(active_large_queues)
                            q2_idx = (map_id * 2 + 1) % len(active_large_queues)
                            
                            new_assignment = [active_large_queues[q1_idx], active_large_queues[q2_idx]]
                            
                            current_assignment = self.global_process_assignments.get(p_id)
                            if current_assignment != new_assignment: 
                                self.global_process_assignments[p_id] = new_assignment
                                self._safe_print(f"🧬 P{p_id} MUTOU (Small Exhausted). Targets: Q{new_assignment} (Assistindo Large).", style="key")
                                

                # Case B: Large Pool Exhausted (P1-P8 mutam para assistir Small)
                elif total_large_files < self.MIN_POOL_THRESHOLD and total_small_files > self.OVERLOAD_THRESHOLD:
                    
                    idle_large_processes = list(range(1, self.NUM_PROCESSES_LARGE + 1)) 
                    active_small_queues = [q_id for q_id, size in small_q_sizes.items() if size > 0]
                    
                    if active_small_queues:
                        for i, p_id in enumerate(idle_large_processes):
                            map_id = i
                            
                            q1_idx = (map_id * 2) % len(active_small_queues)
                            q2_idx = (map_id * 2 + 1) % len(active_small_queues)

                            new_assignment = [active_small_queues[q1_idx], active_small_queues[q2_idx]]
                            
                            current_assignment = self.global_process_assignments.get(p_id)
                            if current_assignment != new_assignment:
                                self.global_process_assignments[p_id] = new_assignment
                                self._safe_print(f"🧬 P{p_id} MUTOU (Large Exhausted). Targets: Q{new_assignment} (Assistindo Small).", style="key")
                
                # 2. HORIZONTAL BALANCING
                
                overloads = sorted(
                    [(q_id, size, q_id in ALL_LARGE_QUEUES) for q_id, size in queue_sizes.items() if size >= self.OVERLOAD_THRESHOLD], 
                    key=lambda x: x[1], reverse=True
                )
                
                for q_id, size, is_large in overloads:
                    
                    potential_targets = large_q_sizes if is_large else small_q_sizes
                        
                    target_q_id = min(
                        (q_target_id for q_target_id, q_target_size in potential_targets.items() 
                         if q_target_id != q_id and q_target_size < size // 2),
                        key=potential_targets.get,
                        default=None
                    )

                    if target_q_id is None: continue
                    
                    source_q = self.head_queues[q_id]
                    target_q = self.head_queues[target_q_id]
                    
                    move_count = max(self.MIN_MOVE_COUNT, (size - potential_targets[target_q_id]) // 2)
                    
                    items_moved = 0
                    
                    for _ in range(move_count):
                        try:
                            item = source_q.get_nowait()
                            target_q.put(item)
                            items_moved += 1
                        except queue.Empty:
                            break
                        except Exception:
                            break

                    if items_moved > 0:
                        self._safe_print(f"↔️ HORIZONTAL Movidos {items_moved} arquivos de Q{q_id} (Size={size}) para Q{target_q_id}.", style="key")


            except Exception as e:
                self._safe_print(f"[error]Erro inesperado no Rebalancer: {type(e).__name__}: {e}[/error]", style="error")
                time.sleep(5) 


def process_worker_multi_head(
    head_id: int, 
    collection_name: str, 
    root_path: str, 
    password: str, 
    head_queues: Dict[int, multiprocessing.Queue],
    global_process_assignments: Dict[int, List[int]], 
    global_metrics_dict: Dict[str, Any],
    global_non_uploaded_list: List[Tuple], 
    global_log_queue: MPQueue
):
    
    local_console = Console(theme=custom_theme)
    engine = None
    
    pool_type = 'LARGE'
    worker_count = WORKERS_PER_HEAD_LARGE
    
    if head_id > NUM_PROCESSES_LARGE:
        pool_type = 'SMALL'
        worker_count = WORKERS_PER_HEAD_SMALL
    
    
    MAX_IDLE_TIME_STEALING_SECONDS = 60.0 
    MAX_UPLOAD_WAIT_TIME_SECONDS = 25.0 
    
    IPC_CACHE_TTL = 2.0  # seconds
    last_ipc_read_time = 0.0
    primary_queues_cache = []
    
    try:
        engine = UploadEngine(collection_name, root_path, password, head_id, worker_count, global_log_queue, pool_type) 
        
        with Progress(
            TextColumn(f"[bold blue]HEAD P{head_id} ({pool_type}) - [/bold blue]"+"{task.description}"), 
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            console=engine.local_console
        ) as progress:
            
            overall_task = progress.add_task(f"[green]Fase 2: Uploads MPU Ativos ({worker_count} Workers)...", total=1)
            engine.set_progress_tracker(progress, overall_task)
            
            # --- SETUP CONCORRENTE ---
            uploader_executor = ThreadPoolExecutor(max_workers=worker_count) 
            scanner_executor = ThreadPoolExecutor(max_workers=SCAN_WORKERS) 
            active_scanner_futures: Set[Any] = set()

            # 1. Inicia os Uploaders (Consumidores)
            for i in range(worker_count):
                uploader_executor.submit(engine._upload_worker, progress, overall_task)

            # --- LOOP DE PRODUÇÃO E STEALING NÃO-BLOQUEANTE ---
            
            all_queues_exhausted = False
            last_activity_time = time.time() 
            
            engine._safe_print(f"[bold]Iniciando ciclo contínuo de scanning (Lendo atribuição dinâmica)...[/bold]", style=None)
            
            while not all_queues_exhausted:
                
                # --- DYNAMIC STATE READ (Cached) ---
                if time.time() - last_ipc_read_time > IPC_CACHE_TTL:
                    primary_queues_cache = global_process_assignments.get(head_id, []) 
                    last_ipc_read_time = time.time()
                
                primary_queues_current = primary_queues_cache
                
                if not primary_queues_current: 
                    time.sleep(0.5)
                    continue
                # --- END DYNAMIC STATE READ ---

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

                # 3. Tenta identificar o alvo de pull (Stealing Universal baseado no estado mutado)
                target_id = get_next_target_universal_steal(primary_queues_current, head_queues)
                
                if target_id is None:
                    
                    if not active_scanner_futures:
                        
                        current_idle_time = time.time() - last_activity_time
                        
                        if current_idle_time >= MAX_IDLE_TIME_STEALING_SECONDS: 
                            engine._safe_print(f"[bold yellow]Fase de Scanning/Chunking DRENADA ({current_idle_time:.1f}s ocioso). Transição para espera de Uploads.[/bold yellow]", style=None)
                            all_queues_exhausted = True
                        else:
                            time.sleep(0.5) 
                        continue
                    else:
                        time.sleep(0.2)
                        continue
                
                # 4. Pull de Arquivos da fila alvo
                queue_to_pull = head_queues[target_id]
                file_batch = []
                
                is_primary = target_id in primary_queues_current
                pull_size = SCAN_WORKERS * 2 if is_primary else 1
                
                try:
                    for _ in range(pull_size): 
                        file_batch.append(queue_to_pull.get(timeout=0.001)) 
                except multiprocessing.Queue.empty:
                    pass 
                
                if file_batch:
                    last_activity_time = time.time() 
                    
                    for file_info in file_batch:
                        # file_info é a tupla de 6 elementos (fp.as_posix(), size, pc_name, drive_letter, ctime_iso, mtime_iso)
                        # O scanner precisa converter o path de volta para Path
                        file_info_tuple_for_scanner = (
                            Path(file_info[0]), file_info[1], file_info[2], file_info[3], file_info[4], file_info[5]
                        )
                        future = scanner_executor.submit(engine._scan_and_hash_worker_multi_head, file_info_tuple_for_scanner)
                        active_scanner_futures.add(future)
                        
                    time.sleep(0.01) 
                
                time.sleep(0.05)


            # --- 5. SHUTDOWN SEQUENCE ---
            
            scanner_executor.shutdown(wait=True)
            engine._safe_print("[info]Scanners desligados. Aguardando conclusão de uploads remanescentes...[/info]")
            
            upload_wait_start = time.time()
            while not engine.upload_queue.empty() or len(engine.mpu_state) > 0:
                time.sleep(1)
                
                if time.time() - upload_wait_start > MAX_UPLOAD_WAIT_TIME_SECONDS:
                    engine._safe_print(f"[alert]Tempo de espera ({MAX_UPLOAD_WAIT_TIME_SECONDS}s) excedido para Uploads e MPU Completion. Forçando finalização dos MPUs abertos.[/alert]", style=None)
                    break
                    
                progress.update(overall_task, description=f"[bold magenta]Fase 3: Limpeza/Aguardando Uploads ({len(engine.mpu_state)} MPUs restantes)...[/bold magenta]")

            
            # Limpeza final dos MPUs incompletos/abortados
            with engine.mpu_lock:
                for s3_key in list(engine.mpu_state.keys()):
                    mpu_info = engine.mpu_state[s3_key]
                    if mpu_info.get('Status') != 'ABORTED' and mpu_info.get('Status') != 'FAILED' and mpu_info.get('Status') != 'FAILED_CLEANUP':
                        engine._safe_print(f"[ALERTA] MPU Incompleto: {mpu_info['OriginalPath'].name}. Abortando e logando falha.", style="warning")
                        # Log com datas
                        engine.log_db.log_file_status(
                            mpu_info['OriginalPath'], 
                            'FAILED', 
                            mpu_info['OriginalFileSize'], 
                            s3_key=s3_key, 
                            error_detail="INCOMPLETE_ABORT_AT_END",
                            ctime_iso=mpu_info.get('CtimeISO'), 
                            mtime_iso=mpu_info.get('MtimeISO')
                        )
                        engine.metrics['failed'] += 1
                        engine._abort_mpu(s3_key)
                        
            # Sinaliza os workers de upload para encerrar
            for _ in range(worker_count):
                engine.upload_queue.put(None)
            
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
             local_console.print(Panel(f"[bold red]ERRO CRÍTICO NO PROCESSO {process_name}: [/bold red]{type(e).__name__}: {safe_error}", title="Falha no Processo", border_style="red"))
    finally:
        if engine and hasattr(engine, 'log_db'):
            try:
                engine.log_db.finalize_run(engine.metrics)
            except Exception as e:
                console.print(f"[error]Erro ao finalizar DB do Head {head_id}: {e}[/error]")


if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    console.print(Panel.fit("[bold white on magenta] MEGA CRYPTO HASH UPLOADER (V24.0.3 - DB INTEGRITY & METADADOS) [/bold white on magenta]", border_style="magenta"))
    
    target_folder = None
    collection_name = None
    password = None
    files_to_ignore_set = set()
    scan_mode = None
    processes = []
    
    # 0. Setup Inicial do DB Schema & Integrity (NOVA LÓGICA)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # MODIFICAÇÃO CIRÚRGICA 9: Chamada ao Setup/Migração e Backfill
    # A) Executa a configuração do schema (cria tabelas e migra se necessário)
    LogCollectorProcess(MPQueue(), LOG_DB_PATH).setup_db_schema_if_not_exists()
    
    # B) Executa o backfill de dados para registros antigos
    DBIntegrityManager(LOG_DB_PATH).backfill_missing_metadata()
    
    past_runs = WorkerLogManager.get_past_runs_info(LOG_DB_PATH)
    
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
            files_to_ignore_set = WorkerLogManager.get_uploaded_files_for_continuation(LOG_DB_PATH, collection_name, drive_letter)
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
        head_queues: Dict[int, multiprocessing.Queue] = {i: multiprocessing.Queue() for i in range(1, 16 + 1)}
        
        global_log_queue = manager.Queue() 
        
        global_process_assignments = manager.dict()
        for p_id in range(1, NUM_TOTAL_PROCESSES + 1):
            if p_id <= NUM_PROCESSES_LARGE:
                global_process_assignments[p_id] = list(PROCESS_QUEUE_MAP[p_id])
            else:
                global_process_assignments[p_id] = list(PROCESS_QUEUE_MAP_SMALL[p_id]) 
        
        
        console.print(f"[info]⚙️ Iniciando Orquestração Hierárquica e Alocação (Total de 16 Filas Lógicas mapeadas em {NUM_TOTAL_PROCESSES} Processos)...[/info]")
        
        orchestration_metrics, group_report = orchestrate_and_queue_hierarchical(
            file_groups, 
            head_queues
        )
        
        console.print("\n--- Distribuição Inicial (V24.0.3 - Max Performance) ---")
        console.print(f"Total de Arquivos Alocados: [bold green]{orchestration_metrics['total_files_allocated']}[/bold green]")
        console.print(f"Total de Dados Alocados: [bold cyan]{humanize.naturalsize(orchestration_metrics['total_size_allocated'])}[/bold cyan]")
        console.print(f"Heads de Processamento Ativos: [bold yellow]{NUM_PROCESSES_LARGE} LARGE + {NUM_PROCESSES_SMALL} SMALL Processos[/bold yellow]")
        console.print(f"Total de Upload Workers: {TOTAL_UPLOAD_WORKERS} (Com Semáforo de I/O de Disco de 4x por Processo)")
        console.print("--------------------------\n")
        
        # 4. Inicia Processos (Log Collector + Rebalancer + Heads)
        processes = []
        global_metrics = manager.dict() 
        global_non_uploaded = manager.list() 
        rebalancer_stop_event = manager.Event()
        
        # A) Inicia o Log Collector
        log_collector = LogCollectorProcess(global_log_queue, LOG_DB_PATH)
        processes.append(log_collector)
        log_collector.start()
        
        # B) Inicia o Rebalancer 
        rebalancer = QueueRebalancerProcess(head_queues, rebalancer_stop_event, global_process_assignments)
        processes.append(rebalancer)
        rebalancer.start()
        
        # C) Processos Large (P1-P8)
        for i in range(1, NUM_PROCESSES_LARGE + 1):
            p = Process(target=process_worker_multi_head, args=(
                i, 
                collection_name, 
                target_folder, 
                password, 
                head_queues, 
                global_process_assignments, 
                global_metrics,
                global_non_uploaded,
                global_log_queue, 
            ))
            processes.append(p)
            p.start()
            
        # D) Processos Small (P9-P12)
        for i in range(1, NUM_PROCESSES_SMALL + 1):
            process_id = NUM_PROCESSES_LARGE + i 
            
            p = Process(target=process_worker_multi_head, args=(
                process_id, 
                collection_name, 
                target_folder, 
                password, 
                head_queues, 
                global_process_assignments, 
                global_metrics,
                global_non_uploaded,
                global_log_queue, 
            ))
            processes.append(p)
            p.start()
        
        
        # 5. Espera a conclusão dos Heads de Upload/Scanning (P1-P12)
        
        upload_heads = [p for p in processes if p != rebalancer and p != log_collector] 
        
        for p in upload_heads:
            p.join()
            
        console.print("\n[success]TODAS AS HEADS DE UPLOAD CONCLUÍRAM. Sinalizando Rebalancer e Log Collector.[/success]")
        
        # Sinaliza Rebalancer para parar e espera
        rebalancer_stop_event.set()
        rebalancer.join(timeout=10)
        if rebalancer.is_alive():
            console.print("[warning]Rebalancer não parou. Terminando forçadamente.[/warning]")
            rebalancer.terminate()

        # Sinaliza Log Collector para parar (processa logs pendentes e fecha DB)
        log_collector.stop()
        log_collector.join()


        # 6. Agregação final
        
        total_uploaded = sum(global_metrics.get(f'P{i}_uploaded', 0) for i in range(1, NUM_TOTAL_PROCESSES + 1))
        total_failed = sum(global_metrics.get(f'P{i}_failed', 0) for i in range(1, NUM_TOTAL_PROCESSES + 1))
        total_ignored_worker = sum(global_metrics.get(f'P{i}_ignored', 0) for i in range(1, NUM_TOTAL_PROCESSES + 1))
        
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
            f"[bold green]OPERAÇÃO GLOBAL FINALIZADA! (LOGGING ROBUSTO GARANTIDO)[/bold green]\n\n"
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
        rebalancer_stop_event.set()
        
        if 'log_collector' in locals() and log_collector.is_alive():
             log_collector.stop()
             log_collector.join(timeout=10)
             
        for p in processes:
            if p.is_alive():
                p.terminate()
        
    console.print("\n[bold magenta]*** FIM DA SESSÃO ***[/bold magenta]")
    input("Pressione ENTER para fechar...")
