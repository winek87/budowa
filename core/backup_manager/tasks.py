# plik: core/backup_manager/tasks.py
# Wersja 1.3 - Dodano logikę przywracania bazy danych z dumpu SQL.
# Opis: Ten moduł zawiera funkcje do tworzenia i przywracania archiwów.
# -*- coding: utf-8 -*-

import asyncio
import logging
import os
import shutil
import tarfile
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Callable

from ..config import (
    BACKUP_DIR, FILES_TO_BACKUP, PROJECT_ROOT, PROJECT_BACKUP_CONFIG,
    DB_TYPE, DB_CONFIG_MARIADB, DATABASE_FILE
)
from .ui.live_display import BackupLiveDisplay

logger = logging.getLogger(__name__)


async def create_mariadb_dump(temp_dir: Path) -> Path:
    """Tworzy zrzut bazy danych MariaDB/MySQL do pliku .sql, preferując socket."""
    dump_path = temp_dir / "database_dump.sql"
    db_config = DB_CONFIG_MARIADB
    
    logger.info(f"Tworzenie zrzutu bazy danych '{db_config['db']}' do pliku tymczasowego...")
    
    command_parts = ["mysqldump", f"--user={db_config['user']} --password={db_config['password']}"]
    
    if db_config.get("unix_socket") and Path(db_config["unix_socket"]).exists():
        logger.debug("Używam połączenia przez socket.")
        command_parts.append(f"--socket={db_config['unix_socket']}")
    else:
        logger.debug("Używam połączenia przez host/port TCP/IP.")
        command_parts.append(f"--host={db_config['host']}")
        command_parts.append(f"--port={db_config['port']}")
        
    command_parts.append(db_config['db'])
    command = " ".join(command_parts) + f" > \"{dump_path}\""
    
    logger.debug(f"Wykonuję polecenie: {command.replace(db_config['password'], '********')}")
    
    env = os.environ.copy()
    env["MYSQL_PWD"] = db_config["password"]
    
    proc = await asyncio.create_subprocess_shell(
        command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env
    )
    stdout, stderr = await proc.communicate()
    
    if proc.returncode != 0:
        error_message = stderr.decode('utf-8', errors='ignore')
        logger.critical(f"Błąd podczas tworzenia zrzutu bazy danych: {error_message}")
        raise RuntimeError(f"Nie udało się utworzyć zrzutu bazy danych: {error_message}")
        
    logger.info("Zrzut bazy danych utworzony pomyślnie.")
    return dump_path


async def restore_mariadb_dump(dump_path: Path):
    """Importuje zrzut .sql do bazy danych MariaDB/MySQL."""
    db_config = DB_CONFIG_MARIADB
    logger.info(f"Rozpoczynam przywracanie bazy danych '{db_config['db']}' z pliku {dump_path.name}...")

    # Komenda do importu, z uwzględnieniem gniazda (socket)
    command_parts = ["mysql", f"--user={db_config['user']}"]
    if db_config.get("unix_socket") and Path(db_config["unix_socket"]).exists():
        command_parts.append(f"--socket={db_config['unix_socket']}")
    else:
        command_parts.append(f"--host={db_config['host']}")
        command_parts.append(f"--port={db_config['port']}")
    
    command_parts.append(db_config['db'])
    command = " ".join(command_parts) + f" < \"{dump_path}\""
    
    env = os.environ.copy(); env["MYSQL_PWD"] = db_config["password"]
    
    proc = await asyncio.create_subprocess_shell(command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env)
    stdout, stderr = await proc.communicate()
    
    if proc.returncode != 0:
        error_message = stderr.decode('utf-8', errors='ignore')
        logger.critical(f"Błąd podczas przywracania bazy danych: {error_message}")
        raise RuntimeError(f"Nie udało się przywrócić bazy danych: {error_message}")
    
    logger.info("Baza danych została pomyślnie przywrócona.")


async def create_archive(
    archive_type: str, 
    files_to_pack: List[Path], 
    archive_path: Path, 
    display: BackupLiveDisplay,
    temp_dir_to_clean: Path | None = None
):
    """Tworzy archiwum, raportując postęp i sprzątając folder tymczasowy."""
    project_root_path = Path(PROJECT_ROOT)
    loop = asyncio.get_running_loop()
    display.progress_bar.update(display._task_id, total=len(files_to_pack))

    def archive_worker():
        archive_opener = zipfile.ZipFile if archive_type == 'zip' else tarfile.open
        mode = 'w' if archive_type == 'zip' else 'w:gz'
        archive_params = {'compression': zipfile.ZIP_DEFLATED} if archive_type == 'zip' else {}

        with archive_opener(archive_path, mode, **archive_params) as archive:
            for file_path in files_to_pack:
                arcname = file_path.name
                if temp_dir_to_clean and file_path.is_relative_to(temp_dir_to_clean):
                    arcname = Path("database_dump") / file_path.name
                else:
                    try:
                        arcname = file_path.relative_to(project_root_path)
                    except ValueError:
                        arcname = file_path.name
                
                if archive_type == 'zip':
                    archive.write(file_path, arcname=arcname)
                else:
                    archive.add(file_path, arcname=arcname)
                
                loop.call_soon_threadsafe(display.update_progress, str(arcname), file_path.stat().st_size)

    await asyncio.to_thread(archive_worker)


async def get_files_for_data_backup(temp_dir: Path) -> List[Path]:
    """Zbiera pliki do kopii zapasowej, obsługując SQLite i dumpy MariaDB."""
    found_files = []
    project_root_path = Path(PROJECT_ROOT)
    
    if DB_TYPE == "mariadb":
        dump_path = await create_mariadb_dump(temp_dir)
        found_files.append(dump_path)
    else:
        sqlite_path = project_root_path / DATABASE_FILE
        if sqlite_path.exists():
            found_files.append(sqlite_path)
        else:
            logger.warning(f"Plik bazy SQLite nie istnieje w {sqlite_path}")

    for path_str in FILES_TO_BACKUP:
        if DB_TYPE == "sqlite" and path_str == DATABASE_FILE:
            continue
            
        path_obj = project_root_path / path_str
        if not path_obj.exists():
            logger.warning(f"Pomijam w backupie (nie istnieje): {path_obj}")
            continue
        if path_obj.is_file():
            found_files.append(path_obj)
        elif path_obj.is_dir():
            found_files.extend(p for p in path_obj.rglob('*') if p.is_file())
            
    logger.info(f"Znaleziono {len(found_files)} plików do kopii zapasowej danych.")
    return found_files


async def get_files_for_core_app_backup() -> List[Path]:
    project_root_path = Path(PROJECT_ROOT); paths_to_include = [project_root_path / "core", project_root_path / "uruchom.py", project_root_path / "start.py"]
    files_to_pack = []
    for p in paths_to_include:
        if not p.exists(): continue
        if p.is_dir(): files_to_pack.extend([f for f in p.rglob('*') if f.is_file() and "__pycache__" not in f.parts])
        elif p.is_file(): files_to_pack.append(p)
    return files_to_pack


async def get_files_for_full_project_backup() -> List[Path]:
    project_root_path = Path(PROJECT_ROOT); config = PROJECT_BACKUP_CONFIG; gitignore_path = project_root_path / ".gitignore"
    exclude_patterns = set(config.get("PATTERNS_TO_EXCLUDE", []))
    if gitignore_path.exists():
        with open(gitignore_path, 'r', encoding='utf-8') as f: exclude_patterns.update(line.strip() for line in f if line.strip() and not line.startswith('#'))
    files_to_pack = []
    for item in project_root_path.rglob('*'):
        if not item.is_file(): continue
        relative_path_str = str(item.relative_to(project_root_path).as_posix())
        if any(Path(relative_path_str).match(pattern) for pattern in exclude_patterns): continue
        files_to_pack.append(item)
    return files_to_pack


async def restore_data_from_zip(archive_path: Path, status_callback: Callable[[str], None]):
    """Przywraca dane z archiwum .zip, z obsługą przywracania dumpu bazy danych."""
    project_root_path = Path(PROJECT_ROOT)
    
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        
        status_callback("Rozpakowywanie archiwum do folderu tymczasowego...")
        with zipfile.ZipFile(archive_path, 'r') as zipf:
            zipf.extractall(path=temp_dir)

        dump_file = temp_dir / "database_dump" / "database_dump.sql"
        if DB_TYPE == "mariadb" and dump_file.exists():
            status_callback("Znaleziono zrzut bazy MariaDB. Przywracanie...")
            await restore_mariadb_dump(dump_file)
            shutil.rmtree(dump_file.parent)

        status_callback("Usuwanie starych plików aplikacji...")
        paths_to_clean = [project_root_path / p for p in FILES_TO_BACKUP]
        if DB_TYPE == "sqlite":
            paths_to_clean.append(project_root_path / DATABASE_FILE)
            
        for path in paths_to_clean:
            if path.is_file(): path.unlink(missing_ok=True)
            elif path.is_dir(): shutil.rmtree(path, ignore_errors=True)

        status_callback("Kopiowanie nowych plików na miejsce...")
        await asyncio.to_thread(
            shutil.copytree, temp_dir, project_root_path, dirs_exist_ok=True
        )
