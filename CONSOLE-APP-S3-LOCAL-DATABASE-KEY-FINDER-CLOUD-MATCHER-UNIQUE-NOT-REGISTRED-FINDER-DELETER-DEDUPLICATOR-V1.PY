import os
import time
import boto3
import sqlite3 # Adicionado para manipulação de banco de dados SQLite
from botocore.config import Config
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.theme import Theme
from rich import box

# ==============================================================================
# CONFIGURAÇÕES "TURBO" E DIRETÓRIO DE SAÍDA
# ==============================================================================

MEGA_S3_ENDPOINT = XXX"
ACCESS_KEY = "XXX"
SECRET_KEY = "XXX"
DB_OUTPUT_DIR = "C:\\mapped-bucket-file-keys" # Diretório para salvar os bancos de dados

# Configuração agressiva para permitir muitas conexões simultâneas
TURBO_CONFIG = Config(
    connect_timeout=30, 
    read_timeout=60,
    max_pool_connections=500,
    retries={'max_attempts': 10, 'mode': 'adaptive'}
)

custom_theme = Theme({
    "bucket": "bold cyan", 
    "size": "bold green",
    "count": "magenta",
    "processing": "yellow italic",
    "finished": "green",
    "error": "bold red",
    "db_saved": "bold bright_blue" # Novo estilo para confirmação do DB
})
console = Console(theme=custom_theme)

def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

# ==============================================================================
# WORKER: PROCESSA UM BUCKET INTEIRO E SALVA CHAVES NO DB
# ==============================================================================
def scan_bucket(bucket_name):
    """
    Worker que varre um bucket inteiro, calcula o tamanho total
    e salva todas as chaves de arquivo em um banco de dados SQLite.
    """
    try:
        # 1. Configuração S3 (Isolada por thread)
        session = boto3.session.Session()
        s3_client = session.client(
            's3',
            endpoint_url=MEGA_S3_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=TURBO_CONFIG
        )

        total_size = 0
        file_count = 0
        all_keys = [] # Lista para armazenar as chaves

        # 2. Varredura agressiva (Idêntico ao original)
        paginator = s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name, PaginationConfig={'PageSize': 1000}):
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_size += obj['Size']
                    file_count += 1
                    all_keys.append(obj['Key']) # Armazena a chave
        
        # 3. Geração do Banco de Dados SQLite
        
        # Garante que o diretório de saída exista
        os.makedirs(DB_OUTPUT_DIR, exist_ok=True)
        db_path = os.path.join(DB_OUTPUT_DIR, f"{bucket_name}.db")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Cria a tabela com id (PK) e key
        cursor.execute("CREATE TABLE IF NOT EXISTS keys (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL)")
        
        # Prepara os dados para inserção em massa
        # O SQLite executemany espera uma lista de tuplas, cada tupla contendo os valores para a linha.
        data_to_insert = [(key,) for key in all_keys]
        
        # Insere todas as chaves de uma vez
        cursor.executemany("INSERT INTO keys (key) VALUES (?)", data_to_insert)
        
        conn.commit()
        conn.close()
        
        return {
            "name": bucket_name,
            "size": total_size,
            "count": file_count,
            "status": "done",
            "db_path": db_path
        }

    except Exception as e:
        # Garante que qualquer falha (S3 ou DB) seja registrada como erro
        return {
            "name": bucket_name,
            "size": 0,
            "count": 0,
            "status": "error",
            "error_msg": str(e)
        }

# ==============================================================================
# MAIN
# ==============================================================================
def run_multithread_scan():
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # Cabeçalho
    console.print(Panel.fit(
        "[bold white on red] MEGA S3 - MULTITHREAD KEY MAPPER (V3) [/bold white on red]\n"
        f"[yellow]Iniciando scan paralelo. Keys serão salvas em: [/yellow][db_saved]{DB_OUTPUT_DIR}[/db_saved]",
        border_style="red"
    ))

    # 1. Obter Lista de Buckets (Primeira fase: Apenas listar nomes)
    try:
        s3_master = boto3.client(
            's3', 
            endpoint_url=MEGA_S3_ENDPOINT,
            aws_access_key_id=ACCESS_KEY, 
            aws_secret_access_key=SECRET_KEY,
            config=TURBO_CONFIG
        )
        resp = s3_master.list_buckets()
        all_buckets = [b['Name'] for b in resp.get('Buckets', [])]
        
    except Exception as e:
        console.print(f"[error]Erro ao listar buckets:[/error] {e}")
        return

    if not all_buckets:
        console.print("[warning]Nenhum bucket encontrado.[/warning]")
        return

    # Dicionário para manter estado da tabela
    results_map = {b: {"size": 0, "count": 0, "status": "processing", "db_path": None} for b in all_buckets}

    def generate_table():
        """Gera a tabela ordenada em tempo real"""
        table = Table(title=f"Monitoramento em Tempo Real ({len(all_buckets)} Buckets)", box=box.ROUNDED)
        table.add_column("Bucket", style="bucket")
        table.add_column("Tamanho", justify="right", style="size")
        table.add_column("Arquivos", justify="right", style="count")
        table.add_column("Status", justify="center")

        # Ordenação complexa (mantida idêntica ao original)
        sorted_items = []
        for name, data in results_map.items():
            sorted_items.append((name, data))
        
        sorted_items.sort(key=lambda x: (
            0 if x[1]['status'] == 'done' else 1,
            -x[1]['size']
        ))

        total_bytes = 0
        total_files = 0

        for name, data in sorted_items:
            if data['status'] == 'processing':
                status_style = "[yellow]⏳ Calculando...[/yellow]"
                size_str = "..."
                count_str = "..."
            elif data['status'] == 'error':
                # Inclui a mensagem de erro se possível, mas mantém a formatação compacta
                status_style = "[red]❌ Falha[/red]" 
                size_str = "0 B"
                count_str = "0"
            else:
                # Se concluído, confirma que o DB foi salvo
                status_style = "[green]✔ Concluído[/green] [db_saved](DB OK)[/db_saved]"
                size_str = sizeof_fmt(data['size'])
                count_str = f"{data['count']:,}"
                total_bytes += data['size']
                total_files += data['count']

            table.add_row(name, size_str, count_str, status_style)

        return table, total_bytes, total_files

    # 2. INICIAR THREAD POOL MASSIVO
    MAX_WORKERS = 400 
    
    with Live(generate_table()[0], refresh_per_second=4, console=console) as live:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_bucket = {executor.submit(scan_bucket, b): b for b in all_buckets}
            
            for future in as_completed(future_to_bucket):
                bucket_name = future_to_bucket[future]
                try:
                    res = future.result()
                    results_map[bucket_name] = res
                except Exception as exc:
                    results_map[bucket_name] = {"status": "error", "size": 0, "count": 0, "db_path": None, "error_msg": str(exc)}
                
                # Atualiza tabela na tela
                table, t_bytes, t_files = generate_table()
                live.update(table)

    # Exibe resumo final estático
    final_table, final_bytes, final_files = generate_table()
    console.print(final_table)
    
    console.print(Panel(
        f"[bold white]Resumo Final[/bold white]\n"
        f"Total Armazenado: [bold green]{sizeof_fmt(final_bytes)}[/bold green]\n"
        f"Total Arquivos: [bold magenta]{final_files:,}[/bold magenta]\n"
        f"Chaves Salvas em: [db_saved]{DB_OUTPUT_DIR}[/db_saved]",
        border_style="green"
    ))

    input("\nPressione ENTER para sair...")

if __name__ == "__main__":
    run_multithread_scan()
