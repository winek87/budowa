# plik: core/database.py (WERSJA FINALNA 12.6 - Ostateczna, kompletna)
# Ten plik jest w pełni kompatybilny ze SQLite i MariaDB/MySQL.

import sys
import time
import json
import logging
import pickle
from pathlib import Path
from collections import defaultdict, OrderedDict
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import asyncio
import re
from contextlib import asynccontextmanager
import aiosqlite

from .config import DB_TYPE, DATABASE_FILE, DB_CONFIG_MARIADB, MAX_RETRIES

# Dynamiczny import sterowników w zależności od konfiguracji
if DB_TYPE == "mariadb":
    try:
        import aiomysql
    except ImportError:
        print("BŁĄD KRYTYCZNY: Wybrano bazę MariaDB, ale brakuje sterownika 'aiomysql'.")
        print("Uruchom: pip install aiomysql pymysql")
        sys.exit(1)
elif DB_TYPE == "sqlite":
    import aiosqlite
else:
    raise ValueError(f"Nieprawidłowy typ bazy danych w config.py: {DB_TYPE}")

import numpy as np

logger = logging.getLogger(__name__)
_db_initialized = False

# ##############################################################################
# ===                    SEKCJA 1: ZARZĄDZANIE POŁĄCZENIEM                    ===
# ##############################################################################

@asynccontextmanager
async def get_db_connection():
    """
    Zwraca asynchroniczny obiekt połączenia w bezpiecznym menedżerze kontekstu,
    dostosowany do wybranego typu bazy danych (SQLite lub MariaDB).
    Zapewnia automatyczne zamykanie połączenia.
    """
    conn = None
    try:
        if DB_TYPE == "mariadb":
            conn = await aiomysql.connect(**DB_CONFIG_MARIADB, loop=asyncio.get_running_loop())
        else:
            db_path = Path(DATABASE_FILE)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = await aiosqlite.connect(db_path)
        yield conn
    except Exception as e:
        logger.critical(f"Błąd połączenia z bazą danych ({DB_TYPE}): {e}", exc_info=True)
        raise
    finally:
        if conn:
            if DB_TYPE == "mariadb":
                if not conn.closed:
                    conn.close()
            elif DB_TYPE == "sqlite":
                await conn.close()

# ##############################################################################
# ===                     SEKCJA 2: INICJALIZACJA I MIGRACJE                 ===
# ##############################################################################

async def setup_database():
    """
    Inicjalizuje i migruje schemat bazy danych. Tworzy tabele, kolumny i indeksy,
    jeśli nie istnieją, dostosowując składnię SQL do wybranego typu bazy danych.
    """
    global _db_initialized
    if _db_initialized: return
    logger.info(f"Inicjalizacja i weryfikacja schematu bazy danych (Typ: {DB_TYPE})...")
    
    # Definicje schematów dla MariaDB
    if DB_TYPE == "mariadb":
        tables = {
            "app_state": "CREATE TABLE IF NOT EXISTS `app_state` ( `key` VARCHAR(255) PRIMARY KEY, `value` TEXT, `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
            "script_state": "CREATE TABLE IF NOT EXISTS script_state ( `key` VARCHAR(255) PRIMARY KEY, `value` TEXT ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
            "downloaded_media": "CREATE TABLE IF NOT EXISTS downloaded_media ( id INT AUTO_INCREMENT PRIMARY KEY, url TEXT, filename VARCHAR(255), final_path VARCHAR(1024), expected_path VARCHAR(1024), metadata_json JSON, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, status VARCHAR(50), retry_count INT DEFAULT 0, processing_status VARCHAR(50), exif_write_status VARCHAR(50), file_hash VARCHAR(32), perceptual_hash VARCHAR(16), ai_tags JSON, source VARCHAR(50) DEFAULT 'google_photos', google_photos_url TEXT, UNIQUE KEY unique_url (url(255)) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
            "people": "CREATE TABLE IF NOT EXISTS people ( person_id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, model_name VARCHAR(50) NOT NULL, master_embedding LONGBLOB NOT NULL, source_media_id INT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, status VARCHAR(50) NOT NULL DEFAULT 'active', FOREIGN KEY (source_media_id) REFERENCES downloaded_media(id) ON DELETE SET NULL, UNIQUE KEY unique_person (name, model_name) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
            "faces": "CREATE TABLE IF NOT EXISTS faces ( face_id INT AUTO_INCREMENT PRIMARY KEY, media_id INT NOT NULL, person_id INT, embedding LONGBLOB NOT NULL, facial_area JSON NOT NULL, model_name VARCHAR(50) NOT NULL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, estimated_age INT, dominant_emotion VARCHAR(50), estimated_gender VARCHAR(50), FOREIGN KEY (media_id) REFERENCES downloaded_media(id) ON DELETE CASCADE, FOREIGN KEY (person_id) REFERENCES people(person_id) ON DELETE SET NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
            "face_indexing_status": " CREATE TABLE IF NOT EXISTS face_indexing_status ( media_id INT NOT NULL, model_name VARCHAR(50) NOT NULL, status VARCHAR(50) NOT NULL, last_updated DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (media_id, model_name), FOREIGN KEY (media_id) REFERENCES downloaded_media(id) ON DELETE CASCADE ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        }
        indexes = {
            "downloaded_media": ["CREATE INDEX idx_media_status ON downloaded_media(status);", "CREATE INDEX idx_media_processing_status ON downloaded_media(processing_status);", "CREATE INDEX idx_media_source ON downloaded_media(source);", "CREATE INDEX idx_media_file_hash ON downloaded_media(file_hash);", "CREATE INDEX idx_media_perceptual_hash ON downloaded_media(perceptual_hash);", "CREATE INDEX idx_filename ON downloaded_media(filename);"],
            "faces": ["CREATE INDEX idx_faces_media_id ON faces(media_id);", "CREATE INDEX idx_faces_person_id ON faces(person_id);"],
            "face_indexing_status": ["CREATE INDEX idx_indexing_status ON face_indexing_status (model_name, status);"]
        }
        alter_statements = {
            'app_state': { 'key': 'VARCHAR(255) PRIMARY KEY', 'value': 'TEXT', 'last_updated': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP' },
            'downloaded_media': { 'id': 'INT AUTO_INCREMENT PRIMARY KEY', 'url': 'TEXT', 'filename': 'VARCHAR(255)', 'final_path': 'VARCHAR(1024)', 'expected_path': 'VARCHAR(1024)', 'metadata_json': 'JSON', 'timestamp': 'DATETIME DEFAULT CURRENT_TIMESTAMP', 'status': 'VARCHAR(50)', 'retry_count': 'INT DEFAULT 0', 'processing_status': 'VARCHAR(50)', 'exif_write_status': 'VARCHAR(50)', 'file_hash': 'VARCHAR(32)', 'perceptual_hash': 'VARCHAR(16)', 'ai_tags': 'JSON', 'source': "VARCHAR(50) DEFAULT 'google_photos'", 'google_photos_url': 'TEXT', 'analysis_results': 'JSON' },
            'people': { 'person_id': 'INT AUTO_INCREMENT PRIMARY KEY', 'name': 'VARCHAR(255) NOT NULL', 'model_name': 'VARCHAR(50) NOT NULL', 'master_embedding': 'LONGBLOB NOT NULL', 'source_media_id': 'INT', 'created_at': 'DATETIME DEFAULT CURRENT_TIMESTAMP', 'status': "VARCHAR(50) NOT NULL DEFAULT 'active'" },
            'faces': { 'face_id': 'INT AUTO_INCREMENT PRIMARY KEY', 'media_id': 'INT NOT NULL', 'person_id': 'INT', 'embedding': 'LONGBLOB NOT NULL', 'facial_area': 'JSON NOT NULL', 'model_name': 'VARCHAR(50) NOT NULL', 'timestamp': 'DATETIME DEFAULT CURRENT_TIMESTAMP', 'estimated_age': 'INT', 'dominant_emotion': 'VARCHAR(50)', 'estimated_gender': 'VARCHAR(50)', 'dominant_race': 'VARCHAR(50)' }
        }
    # Definicje schematów dla SQLite
    else:
        tables = {
            "app_state": "CREATE TABLE IF NOT EXISTS app_state (key TEXT PRIMARY KEY, value TEXT, last_updated DATETIME DEFAULT CURRENT_TIMESTAMP)",
            "script_state": "CREATE TABLE IF NOT EXISTS script_state (key TEXT PRIMARY KEY, value TEXT)",
            "downloaded_media": "CREATE TABLE IF NOT EXISTS downloaded_media (id INTEGER PRIMARY KEY, url TEXT NOT NULL UNIQUE)",
            "people": " CREATE TABLE IF NOT EXISTS people ( person_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, model_name TEXT NOT NULL, master_embedding BLOB NOT NULL, source_media_id INTEGER, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, status TEXT NOT NULL DEFAULT 'active', FOREIGN KEY (source_media_id) REFERENCES downloaded_media (id) ON DELETE SET NULL, UNIQUE(name, model_name) )",
            "faces": " CREATE TABLE IF NOT EXISTS faces ( face_id INTEGER PRIMARY KEY AUTOINCREMENT, media_id INTEGER NOT NULL, person_id INTEGER, embedding BLOB NOT NULL, facial_area TEXT NOT NULL, model_name TEXT NOT NULL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, estimated_age INTEGER, dominant_emotion TEXT, estimated_gender TEXT, FOREIGN KEY (media_id) REFERENCES downloaded_media (id) ON DELETE CASCADE, FOREIGN KEY (person_id) REFERENCES people (person_id) ON DELETE SET NULL )",
            "face_indexing_status": " CREATE TABLE IF NOT EXISTS face_indexing_status ( media_id INTEGER NOT NULL, model_name TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('pending', 'processed', 'no_faces_found', 'error')), last_updated DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (media_id, model_name), FOREIGN KEY (media_id) REFERENCES downloaded_media (id) ON DELETE CASCADE )"
        }
        indexes = {
            "downloaded_media": ["CREATE INDEX IF NOT EXISTS idx_media_status ON downloaded_media (status);", "CREATE INDEX IF NOT EXISTS idx_media_processing_status ON downloaded_media (processing_status);", "CREATE INDEX IF NOT EXISTS idx_media_source ON downloaded_media (source);", "CREATE INDEX IF NOT EXISTS idx_media_file_hash ON downloaded_media (file_hash);", "CREATE INDEX IF NOT EXISTS idx_media_perceptual_hash ON downloaded_media (perceptual_hash);"],
            "faces": ["CREATE INDEX IF NOT EXISTS idx_faces_media_id ON faces (media_id);", "CREATE INDEX IF NOT EXISTS idx_faces_person_id ON faces (person_id);"],
            "face_indexing_status": ["CREATE INDEX IF NOT EXISTS idx_indexing_status ON face_indexing_status (model_name, status);"]
        }
        alter_statements = {
            'app_state': { 'last_updated': 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
            'downloaded_media': { "filename": "TEXT", "final_path": "TEXT", "expected_path": "TEXT", "metadata_json": "TEXT", "timestamp": "DATETIME DEFAULT CURRENT_TIMESTAMP", "status": "TEXT", "retry_count": "INTEGER DEFAULT 0", "processing_status": "TEXT", "exif_write_status": "TEXT", "file_hash": "TEXT", "perceptual_hash": "TEXT", "ai_tags": "TEXT", "source": "TEXT DEFAULT 'google_photos'", "google_photos_url": "TEXT", 'analysis_results': 'TEXT' },
            'people': { 'status': "TEXT NOT NULL DEFAULT 'active'" },
            'faces': { 'estimated_age': 'INTEGER', 'dominant_emotion': 'TEXT', 'estimated_gender': 'TEXT', 'dominant_race': 'TEXT' }
        }
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cursor:
            # Tworzenie tabel
            for table_name, create_sql in tables.items():
                await cursor.execute(create_sql)
            # Weryfikacja i dodawanie kolumn
            for table_name, columns in alter_statements.items():
                if DB_TYPE == 'mariadb':
                    await cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
                else:
                    await cursor.execute(f"PRAGMA table_info({table_name});")
                rows = await cursor.fetchall()
                existing_columns = {row[0] if DB_TYPE == 'mariadb' else row[1] for row in rows}
                for col_name, col_type in columns.items():
                    if col_name not in existing_columns:
                        await cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {col_type}")
                        logger.info(f"Dodano kolumnę '{col_name}' do tabeli '{table_name}'.")
            # Tworzenie indeksów
            for table_name, index_sqls in indexes.items():
                for index_sql in index_sqls:
                    if DB_TYPE == 'mariadb':
                        index_name_match = re.search(r"CREATE INDEX (?:IF NOT EXISTS )?`?(\w+)`? ON", index_sql, re.IGNORECASE)
                        if index_name_match:
                            index_name = index_name_match.group(1)
                            await cursor.execute(f"SELECT COUNT(1) FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=DATABASE() AND table_name='{table_name}' AND index_name='{index_name}'")
                            if (await cursor.fetchone())[0] == 0:
                                await cursor.execute(index_sql.replace("IF NOT EXISTS ", ""))
                    else:
                        await cursor.execute(index_sql)
        if DB_TYPE == "sqlite":
            await conn.commit()
            
    _db_initialized = True
    logger.info("Inicjalizacja i weryfikacja schematu bazy danych zakończona.")

# ##############################################################################
# ===                     SEKCJA 3: ZARZĄDZANIE STANEM I STATYSTYKAMI          ===
# ##############################################################################

async def get_db_stats() -> defaultdict[str, int]:
    """
    Asynchronicznie pobiera i agreguje statystyki z bazy danych.
    """
    stats = defaultdict(int)
    if DB_TYPE == "sqlite" and not await asyncio.to_thread(Path(DATABASE_FILE).exists):
        return stats
    try:
        async with get_db_connection() as conn:
            # Ustawienie odpowiedniego kursora do pracy ze słownikami
            if DB_TYPE == "sqlite":
                conn.row_factory = aiosqlite.Row
                cursor_context = conn.cursor()
            else: # mariadb
                cursor_context = conn.cursor(aiomysql.DictCursor)

            async with cursor_context as cursor:
                queries = {
                    "status": "SELECT status, COUNT(id) as count FROM downloaded_media GROUP BY status",
                    "processing_status": "SELECT processing_status, COUNT(id) as count FROM downloaded_media GROUP BY processing_status",
                    "exif_write_status": "SELECT exif_write_status, COUNT(id) as count FROM downloaded_media GROUP BY exif_write_status",
                }
                for key, query in queries.items():
                    await cursor.execute(query)
                    rows = await cursor.fetchall()
                    for row in rows:
                        row_dict = dict(row)
                        if row_dict.get('count', 0) > 0 and row_dict.get(key):
                            prefix = "scan_" if key == "processing_status" else "exif_" if key == "exif_write_status" else ""
                            stats[f"{prefix}{str(row_dict[key]).lower()}"] = row_dict['count']

                await cursor.execute("SELECT COUNT(id) FROM downloaded_media")
                total_row = await cursor.fetchone()
                total = total_row[0] if total_row and isinstance(total_row, tuple) else (total_row.get('COUNT(id)', 0) if total_row else 0)
                stats['total'] = total
                return stats
    except Exception as e:
        logger.error(f"Błąd podczas pobierania statystyk z bazy danych: {e}", exc_info=True)
        return defaultdict(int)

async def set_state(key: str, value: str):
    """
    Zapisuje lub aktualizuje parę klucz-wartość w tabeli `script_state`.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                if DB_TYPE == "mariadb":
                    sql = "INSERT INTO script_state (`key`, `value`) VALUES (%s, %s) ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)"
                else: # sqlite
                    sql = "INSERT OR REPLACE INTO script_state (key, value) VALUES (?, ?)"
                await cursor.execute(sql, (key, value))
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Nie udało się zapisać stanu dla klucza '{key}': {e}", exc_info=True)

async def get_state(key: str) -> str | None:
    """
    Odczytuje wartość stanu dla podanego klucza z tabeli `script_state`.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                safe_key_col = "`key`" if DB_TYPE == "mariadb" else "key"
                query = f"SELECT value FROM script_state WHERE {safe_key_col} = {placeholder}"
                await cursor.execute(query, (key,))
                result = await cursor.fetchone()
                return result[0] if result else None
    except Exception as e:
        logger.error(f"Nie udało się odczytać stanu dla klucza '{key}': {e}", exc_info=True)
        return None

# ##############################################################################
# ===                     SEKCJA 4: GŁÓWNE OPERACJE NA MEDIACH               ===
# ##############################################################################

async def add_google_photo_entry(url: str, filename: str, final_path: Path, metadata: dict, status: str, retry_count: int, expected_path: str | None, processing_status: str | None = None):
    """
    Zapisuje lub aktualizuje wpis dla medium pobranego z Google Photos.
    """
    await setup_database()
    try:
        final_path_str = str(final_path) if final_path else None
        metadata_str = json.dumps(metadata, ensure_ascii=False) if metadata else None
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                if DB_TYPE == "mariadb":
                    sql = "INSERT INTO downloaded_media (url, filename, final_path, metadata_json, status, retry_count, expected_path, source, processing_status, timestamp) VALUES (%(url)s, %(filename)s, %(final_path)s, %(metadata_json)s, %(status)s, %(retry_count)s, %(expected_path)s, 'google_photos', %(processing_status)s, NOW()) ON DUPLICATE KEY UPDATE filename=VALUES(filename), final_path=VALUES(final_path), metadata_json=VALUES(metadata_json), status=VALUES(status), retry_count=VALUES(retry_count), expected_path=VALUES(expected_path), source=VALUES(source), processing_status=COALESCE(VALUES(processing_status), downloaded_media.processing_status), timestamp=NOW();"
                    params = { "url": url, "filename": filename, "final_path": final_path_str, "metadata_json": metadata_str, "status": status, "retry_count": retry_count, "expected_path": expected_path, "processing_status": processing_status }
                else:
                    sql = "INSERT INTO downloaded_media (url, filename, final_path, metadata_json, status, retry_count, expected_path, source, processing_status) VALUES (?, ?, ?, ?, ?, ?, ?, 'google_photos', ?) ON CONFLICT(url) DO UPDATE SET filename=excluded.filename, final_path=excluded.final_path, metadata_json=excluded.metadata_json, status=excluded.status, retry_count=excluded.retry_count, expected_path=excluded.expected_path, source=excluded.source, processing_status=COALESCE(excluded.processing_status, downloaded_media.processing_status), timestamp=CURRENT_TIMESTAMP;"
                    params = (url, filename, final_path_str, metadata_str, status, retry_count, expected_path, processing_status)
                await cursor.execute(sql, params)
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.critical(f"Nie udało się zapisać danych dla URL {url}: {e}", exc_info=True)

async def add_local_file_entry(file_path: Path, metadata: dict) -> bool:
    """
    Dodaje do bazy danych wpis dla pliku zaimportowanego z lokalnego dysku.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                metadata_str = json.dumps(metadata, ensure_ascii=False) if metadata else None
                deterministic_url = file_path.resolve().as_uri()
                if DB_TYPE == "mariadb":
                    sql = "INSERT IGNORE INTO downloaded_media (url, filename, final_path, expected_path, metadata_json, status, processing_status, exif_write_status, source) VALUES (%s, %s, %s, %s, %s, 'downloaded', 'Sukces', 'n/a', 'local_import')"
                else:
                    sql = "INSERT OR IGNORE INTO downloaded_media (url, filename, final_path, expected_path, metadata_json, status, processing_status, exif_write_status, source) VALUES (?, ?, ?, ?, ?, 'downloaded', 'Sukces', 'n/a', 'local_import')"
                params = (deterministic_url, file_path.name, str(file_path.resolve()), str(file_path.resolve()), metadata_str)
                await cursor.execute(sql, params)
                if DB_TYPE == "sqlite":
                    await conn.commit()
                return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Nie udało się dodać pliku lokalnego '{file_path.name}' do bazy: {e}", exc_info=True)
        return False

async def delete_entries_by_ids(ids: List[int]):
    """
    Usuwa wpisy z bazy danych na podstawie podanej listy ID.
    """
    await setup_database()
    if not ids: return
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                if DB_TYPE == "mariadb":
                    placeholders = ','.join(['%s'] * len(ids))
                    sql = f"DELETE FROM downloaded_media WHERE id IN ({placeholders})"
                    await cursor.execute(sql, ids)
                else:
                    placeholders = ','.join(['?'] * len(ids))
                    sql = f"DELETE FROM downloaded_media WHERE id IN ({placeholders})"
                    await cursor.execute(sql, ids)
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Błąd podczas usuwania wpisów: {e}", exc_info=True)

async def _get_single_column_for_url(column_name: str, url: str, default_value: Any = None) -> Any:
    """
    Prywatna funkcja pomocnicza do pobierania wartości z pojedynczej kolumny dla danego URL.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                safe_column_name = f"`{column_name}`" if DB_TYPE == "mariadb" else column_name
                query = f"SELECT {safe_column_name} FROM downloaded_media WHERE url = {placeholder}"
                await cursor.execute(query, (url,))
                result = await cursor.fetchone()
                return result[0] if result else default_value
    except Exception as e:
        logger.error(f"Błąd podczas pobierania kolumny '{column_name}' dla URL {url}: {e}", exc_info=True)
        return default_value

async def get_url_status_from_db(url: str) -> str | None:
    """
    Pobiera status pobierania (`downloaded`, `failed` itp.) dla danego URL-a.
    """
    return await _get_single_column_for_url(column_name='status', url=url)

async def get_retry_count_for_url(url: str) -> int:
    """
    Pobiera liczbę prób pobrania dla danego URL-a.
    """
    return await _get_single_column_for_url(column_name='retry_count', url=url, default_value=0)

# ##############################################################################
# ===                     SEKCJA 5: Funkcje dla Skanerów i Importerów      === #
# ##############################################################################

async def get_failed_urls_from_db() -> list[str]:
    """
    Pobiera listę URL-i, które zakończyły się błędem i mogą być ponowione.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                query = f"SELECT url FROM downloaded_media WHERE status = 'failed' AND retry_count < {placeholder}"
                await cursor.execute(query, (MAX_RETRIES,))
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania listy błędnych URL-i: {e}", exc_info=True)
        return []

async def get_all_urls_from_db() -> List[str]:
    """
    Pobiera z bazy danych listę WSZYSTKICH unikalnych adresów URL.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT url FROM downloaded_media WHERE url IS NOT NULL")
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania wszystkich URL-i: {e}", exc_info=True)
        return []

async def get_urls_to_fix() -> List[str]:
    """
    Pobiera listę URL-i, które wymagają ponownego skanowania metadanych.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                if DB_TYPE == "mariadb":
                    query = "SELECT url FROM downloaded_media WHERE status IN ('downloaded', 'skipped', 'scanned') AND JSON_VALID(metadata_json) = 1 AND JSON_EXTRACT(metadata_json, '$.FileName') IS NULL"
                else:
                    query = "SELECT url FROM downloaded_media WHERE status IN ('downloaded', 'skipped', 'scanned') AND json_valid(metadata_json) = 1 AND json_extract(metadata_json, '$.FileName') IS NULL"
                await cursor.execute(query)
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania URL-i do naprawy: {e}", exc_info=True)
        return []

async def get_urls_for_online_scan(scan_type: str = 'new_only') -> List[str]:
    """
    Pobiera listę URL-i do przetworzenia przez skaner online.
    """
    await setup_database()
    base_query = "SELECT url FROM downloaded_media WHERE (source IS NULL OR source != 'local_import')"
    conditions = { 'new_only': "AND (processing_status IS NULL OR processing_status != 'Sukces')", 'retry_errors': "AND processing_status = 'Błąd'", 'force_refresh': "" }
    query = base_query + conditions.get(scan_type, "")
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query)
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania URL-i do skanowania: {e}", exc_info=True)
        return []

async def update_scanned_entries_batch(batch_data: list[dict]):
    """
    Asynchronicznie zapisuje partię wyników ze skanera online do bazy danych.
    """
    await setup_database()
    if not batch_data: return
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                if DB_TYPE == "mariadb":
                    sql = "INSERT INTO downloaded_media (url, metadata_json, processing_status, expected_path, status) VALUES (%s, %s, %s, %s, 'scanned') ON DUPLICATE KEY UPDATE metadata_json = VALUES(metadata_json), processing_status = VALUES(processing_status), expected_path = VALUES(expected_path);"
                    params = [(item['url'], item['metadata_json'], item['processing_status'], item['expected_path']) for item in batch_data]
                else:
                    sql = "INSERT INTO downloaded_media (url, metadata_json, processing_status, expected_path, status) VALUES (?, ?, ?, ?, 'scanned') ON CONFLICT(url) DO UPDATE SET metadata_json = excluded.metadata_json, processing_status = excluded.processing_status, expected_path = excluded.expected_path;"
                    params = [(item['url'], item['metadata_json'], item['processing_status'], item['expected_path']) for item in batch_data]
                await cursor.executemany(sql, params)
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.critical(f"KRYTYCZNY BŁĄD podczas zapisu wsadowego wyników skanowania: {e}", exc_info=True)

async def get_records_for_path_correction() -> List[Dict[str, Any]]:
    """
    Pobiera rekordy, których final_path i expected_path się nie zgadzają.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT id, final_path, expected_path FROM downloaded_media WHERE final_path IS NOT NULL AND final_path != '' AND expected_path IS NOT NULL AND expected_path != '' AND final_path != expected_path"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania niespójnych ścieżek: {e}", exc_info=True)
        return []

async def update_final_path(entry_id: int, new_final_path: str):
    """
    Aktualizuje final_path dla pojedynczego wpisu w bazie danych.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET final_path = {placeholder} WHERE id = {placeholder}"
                await cursor.execute(sql, (new_final_path, entry_id))
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Błąd aktualizacji final_path dla ID {entry_id}: {e}", exc_info=True)

async def get_records_for_filename_fix() -> List[Dict[str, Any]]:
    """
    Pobiera rekordy do weryfikacji i potencjalnej naprawy nazwy pliku.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                if DB_TYPE == "mariadb":
                    query = "SELECT id, filename, final_path, expected_path, metadata_json FROM downloaded_media WHERE status = 'downloaded' AND JSON_VALID(metadata_json) = 1 AND JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.FileName')) IS NOT NULL AND filename != JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.FileName'))"
                else:
                    query = "SELECT id, filename, final_path, expected_path, metadata_json FROM downloaded_media WHERE status = 'downloaded' AND json_valid(metadata_json) = 1 AND json_extract(metadata_json, '$.FileName') IS NOT NULL AND filename != json_extract(metadata_json, '$.FileName')"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd pobierania rekordów do naprawy nazw: {e}", exc_info=True)
        return []

async def update_entry_after_rename(entry_id: int, new_filename: str, new_final_path: str, new_expected_path: str, new_metadata_json: str):
    """
    Kompleksowo aktualizuje wpis w bazie po zmianie nazwy pliku.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET filename = {placeholder}, final_path = {placeholder}, expected_path = {placeholder}, metadata_json = {placeholder} WHERE id = {placeholder}"
                params = (new_filename, new_final_path, new_expected_path, new_metadata_json, entry_id)
                await cursor.execute(sql, params)
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Błąd aktualizacji wpisu po zmianie nazwy dla ID {entry_id}: {e}", exc_info=True)

async def get_records_for_metadata_completion() -> List[Dict[str, Any]]:
    """
    Pobiera rekordy, które wymagają uzupełnienia metadanych i expected_path.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT id, final_path, metadata_json FROM downloaded_media WHERE status = 'downloaded' AND (expected_path IS NULL OR expected_path = '')"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd pobierania rekordów do uzupełnienia metadanych: {e}", exc_info=True)
        return []

async def update_entry_with_completed_metadata(entry_id: int, new_metadata_json: str, new_expected_path: str):
    """
    Aktualizuje wpis w bazie o uzupełnione metadane i obliczoną ścieżkę expected_path.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET metadata_json = {placeholder}, expected_path = {placeholder} WHERE id = {placeholder}"
                params = (new_metadata_json, new_expected_path, entry_id)
                await cursor.execute(sql, params)
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Błąd aktualizacji wpisu z uzupełnionymi metadanymi dla ID {entry_id}: {e}", exc_info=True)

async def get_all_db_records_for_takeout_import() -> List[Dict[str, Any]]:
    """Pobiera podstawowe dane (id, filename, metadata_json) dla wszystkich rekordów."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT id, filename, metadata_json FROM downloaded_media"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd pobierania rekordów dla importu z Takeout: {e}", exc_info=True)
        return []

async def update_takeout_metadata_batch(updates: List[Tuple[str, Optional[str], int]]):
    """Zapisuje partię zaktualizowanych metadanych i URL z Takeout do bazy."""
    await setup_database()
    if not updates: return
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET metadata_json = {placeholder}, google_photos_url = {placeholder} WHERE id = {placeholder}"
                await cursor.executemany(sql, updates)
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Błąd wsadowej aktualizacji metadanych z Takeout: {e}", exc_info=True)

async def get_all_filenames_from_db() -> set:
    """Pobiera zbiór wszystkich nazw plików (filename) z bazy danych."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                query = "SELECT filename FROM downloaded_media WHERE filename IS NOT NULL"
                await cursor.execute(query)
                rows = await cursor.fetchall()
                return {row[0] for row in rows}
    except Exception as e:
        logger.error(f"Błąd pobierania nazw plików z bazy: {e}", exc_info=True)
        return set()

async def get_aggregated_analytics_data() -> dict:
    """Pobiera i Agreguje dane analityczne bezpośrednio w bazie danych."""
    await setup_database()
    logger.info("Rozpoczynam agregację danych analitycznych w bazie danych...")
    valid_statuses = ('downloaded', 'skipped', 'archived', 'scanned')
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                if DB_TYPE == "mariadb":
                    placeholders = ','.join(['%s'] * len(valid_statuses))
                    json_size = "CAST(JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.size')) AS UNSIGNED)"
                    json_datetime = "JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.DateTime'))"
                    json_camera = "JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.Camera'))"
                    year_func = f"YEAR(STR_TO_DATE(LEFT({json_datetime}, 19), '%%Y-%%m-%%dT%%H:%%i:%%s'))"
                    
                    query = f"""
                        SELECT
                            COUNT(id) as total_files, SUM({json_size}) as total_size_bytes,
                            MIN({json_datetime}) as oldest_date, MAX({json_datetime}) as newest_date,
                            {year_func} as `year`, COUNT(id) as year_count, SUM({json_size}) as year_size
                        FROM downloaded_media
                        WHERE status IN ({placeholders}) AND JSON_VALID(metadata_json) = 1 AND {json_datetime} IS NOT NULL
                        GROUP BY `year`
                    """
                    camera_query = f"SELECT {json_camera} as camera, COUNT(id) as count FROM downloaded_media WHERE {json_camera} IS NOT NULL GROUP BY camera ORDER BY count DESC LIMIT 15"
                    params = valid_statuses
                else:
                    placeholders = ','.join(['?'] * len(valid_statuses))
                    json_size = "CAST(json_extract(metadata_json, '$.size') AS INTEGER)"
                    json_datetime = "json_extract(metadata_json, '$.DateTime')"
                    json_camera = "json_extract(metadata_json, '$.Camera')"
                    year_func = f"STRFTIME('%Y', {json_datetime})"
                    query = f"SELECT COUNT(id) as total_files, SUM({json_size}) as total_size_bytes, MIN({json_datetime}) as oldest_date, MAX({json_datetime}) as newest_date, {year_func} as year, COUNT(id) as year_count, SUM({json_size}) as year_size FROM downloaded_media WHERE status IN ({placeholders}) AND json_valid(metadata_json) = 1 AND {json_datetime} IS NOT NULL GROUP BY year"
                    camera_query = f"SELECT {json_camera} as camera, COUNT(id) as count FROM downloaded_media WHERE camera IS NOT NULL GROUP BY camera ORDER BY count DESC LIMIT 15"
                    params = valid_statuses
                await cursor.execute(query, params)
                rows = await cursor.fetchall()
                await cursor.execute(camera_query)
                camera_rows = await cursor.fetchall()
    except Exception as e:
        logger.error(f"Błąd podczas agregacji danych analitycznych: {e}", exc_info=True)
        return {}

    if not rows: return {}
    rows_dicts = [dict(r) for r in rows]
    camera_rows_dicts = [dict(r) for r in camera_rows]
    overall_stats = {
        "total_files": sum(r['year_count'] for r in rows_dicts if r.get('year_count')),
        "total_size_bytes": sum(r['year_size'] for r in rows_dicts if r.get('year_size')),
        "oldest_date": min(r['oldest_date'] for r in rows_dicts if r.get('oldest_date')),
        "newest_date": max(r['newest_date'] for r in rows_dicts if r.get('newest_date')),
    }
    yearly_data = { r['year']: {'count': r['year_count'], 'size': r['year_size']} for r in rows_dicts if r.get('year') and r.get('year_count') is not None and r.get('year_size') is not None }
    camera_data = [(r['camera'], r['count']) for r in camera_rows_dicts]
    return { "overall": overall_stats, "yearly": yearly_data, "cameras": camera_data }

async def get_raw_media_entries_for_analysis() -> list[dict]:
    """Pobiera surowe dane z bazy, niezbędne do działania wszystkich modułów analitycznych."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite": cursor.row_factory = aiosqlite.Row
                valid_statuses = ['downloaded', 'skipped', 'archived', 'scanned']
                placeholders = ','.join(['%s' if DB_TYPE == "mariadb" else '?'] * len(valid_statuses))
                json_valid_syntax = 'JSON_VALID(metadata_json) = 1' if DB_TYPE == "mariadb" else 'json_valid(metadata_json) = 1'
                query = f"SELECT id, metadata_json, final_path FROM downloaded_media WHERE status IN ({placeholders}) AND {json_valid_syntax}"
                await cursor.execute(query, valid_statuses)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania surowych danych do analizy: {e}", exc_info=True)
        return []

async def reset_media_for_reprocessing(ids_to_update: list[int]) -> int:
    """Resetuje statusy przetwarzania dla podanej listy ID mediów."""
    await setup_database()
    if not ids_to_update: return 0
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                update_params = [('pending', 'pending', entry_id) for entry_id in ids_to_update]
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET processing_status = {placeholder}, exif_write_status = {placeholder} WHERE id = {placeholder}"
                await cursor.executemany(sql, update_params)
                if DB_TYPE == "sqlite":
                    await conn.commit()
                return cursor.rowcount
    except Exception as e:
        logger.error(f"Błąd podczas resetowania statusów w bazie danych: {e}", exc_info=True)
        return 0

async def get_records_for_exif_writing() -> List[Dict[str, Any]]:
    """Pobiera rekordy, których metadane należy zapisać do plików za pomocą Exiftool."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT final_path, metadata_json FROM downloaded_media WHERE status = 'downloaded' AND metadata_json IS NOT NULL AND metadata_json != '{}'"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd pobierania rekordów do zapisu EXIF: {e}", exc_info=True)
        return []

async def get_exif_writer_stats() -> Dict[str, Any]:
    """Pobiera statystyki dla Exif Writer za pomocą jednego, wydajnego zapytania."""
    await setup_database()
    stats = {'total_ready': 0, 'success': 0, 'partial': 0, 'error': 0, 'not_written': 0}
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "mariadb": await cursor.execute("SHOW TABLES LIKE 'downloaded_media'")
                else: await cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='downloaded_media'")
                if not await cursor.fetchone(): return stats

                has_exif_status_col = False
                if DB_TYPE == "sqlite": await cursor.close(); cursor = await conn.cursor()
                if DB_TYPE == "mariadb":
                    await cursor.execute("SHOW COLUMNS FROM `downloaded_media` LIKE 'exif_write_status'")
                    if await cursor.fetchone(): has_exif_status_col = True
                else:
                    await cursor.execute("PRAGMA table_info(downloaded_media)")
                    if 'exif_write_status' in {info[1] for info in await cursor.fetchall()}: has_exif_status_col = True
                
                if not has_exif_status_col:
                    await cursor.execute("SELECT COUNT(*) FROM downloaded_media WHERE processing_status = 'Sukces' AND metadata_json IS NOT NULL")
                    count = (await cursor.fetchone() or (0,))[0]
                    stats['total_ready'] = count; stats['not_written'] = count
                    return stats

                query = """
                    SELECT
                        COUNT(*) as total_ready,
                        SUM(CASE WHEN exif_write_status = 'Success' THEN 1 ELSE 0 END) as success,
                        SUM(CASE WHEN exif_write_status = 'Partial' THEN 1 ELSE 0 END) as partial,
                        SUM(CASE WHEN exif_write_status = 'Error' THEN 1 ELSE 0 END) as error,
                        SUM(CASE WHEN exif_write_status IS NULL OR exif_write_status = '' OR exif_write_status = 'pending' THEN 1 ELSE 0 END) as not_written
                    FROM downloaded_media
                    WHERE processing_status = 'Sukces' AND metadata_json IS NOT NULL
                """
                await cursor.execute(query)
                result = await cursor.fetchone()
                
                if result:
                    row_dict = result if isinstance(result, dict) else dict(zip(['total_ready', 'success', 'partial', 'error', 'not_written'], result))
                    stats['total_ready'] = row_dict.get('total_ready', 0) or 0
                    stats['success'] = row_dict.get('success', 0) or 0
                    stats['partial'] = row_dict.get('partial', 0) or 0
                    stats['error'] = row_dict.get('error', 0) or 0
                    stats['not_written'] = row_dict.get('not_written', 0) or 0
                return stats
    except Exception as e:
        logger.error(f"BŁĄD podczas odczytu statystyk zapisu EXIF.", exc_info=True)
        return stats

async def get_downloaded_files_for_validation() -> List[Dict[str, Any]]:
    """Pobiera z bazy listę plików o statusie 'downloaded' do weryfikacji istnienia."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT id, final_path, filename FROM downloaded_media WHERE status = 'downloaded' AND final_path IS NOT NULL AND final_path != ''"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd pobierania plików do walidacji istnienia: {e}", exc_info=True)
        return []

async def get_records_to_hash() -> List[Dict[str, Any]]:
    """Pobiera z bazy listę plików, które nie mają jeszcze obliczonego hasha MD5."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT id, final_path FROM downloaded_media WHERE status = 'downloaded' AND (file_hash IS NULL OR file_hash = '')"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd pobierania plików do hashowania: {e}", exc_info=True)
        return []

async def update_hashes_batch(updates: List[Tuple[str, int]]):
    """Zapisuje partię obliczonych hashy MD5 do bazy danych."""
    await setup_database()
    if not updates: return
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET file_hash = {placeholder} WHERE id = {placeholder}"
                await cursor.executemany(sql, updates)
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Błąd wsadowej aktualizacji hashy: {e}", exc_info=True)

async def get_all_final_paths() -> List[Dict[str, Any]]:
    """Pobiera wszystkie istniejące `final_path` z bazy danych."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT id, final_path FROM downloaded_media WHERE final_path IS NOT NULL AND final_path != ''"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd pobierania wszystkich final_path: {e}", exc_info=True)
        return []

async def get_metadata_for_consistency_check() -> List[Dict[str, Any]]:
    """Pobiera dane niezbędne do sprawdzenia spójności metadanych (ścieżka vs data)."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                
                json_extract_syntax = "JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.DateTime'))" if DB_TYPE == "mariadb" else "json_extract(metadata_json, '$.DateTime')"
                query = f"""
                    SELECT id, final_path, {json_extract_syntax} as dt_from_json
                    FROM downloaded_media
                    WHERE status = 'downloaded' AND {json_extract_syntax} IS NOT NULL AND final_path IS NOT NULL AND final_path != ''
                """
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd pobierania danych do sprawdzenia spójności: {e}", exc_info=True)
        return []

async def get_entries_by_hash(file_hash: str) -> List[Dict[str, Any]]:
    """Pobiera wszystkie wpisy z bazy danych pasujące do danego hasha MD5."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                query = f"SELECT id, final_path, metadata_json FROM downloaded_media WHERE file_hash = {placeholder}"
                await cursor.execute(query, (file_hash,))
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd pobierania wpisów po hashu '{file_hash}': {e}", exc_info=True)
        return []

async def get_local_import_entries() -> list[dict]:
    """Pobiera wszystkie wpisy z bazy danych, które zostały zaimportowane z dysku lokalnego."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT id, url, final_path FROM downloaded_media WHERE source = 'local_import'"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania wpisów importowanych lokalnie: {e}", exc_info=True)
        return []

async def update_paths_for_entry(entry_id: int, new_path: str):
    """Aktualizuje `final_path` i `expected_path` dla pojedynczego wpisu w bazie danych."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET final_path = {placeholder}, expected_path = {placeholder} WHERE id = {placeholder}"
                await cursor.execute(sql, (new_path, new_path, entry_id))
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Błąd aktualizacji ścieżek dla ID {entry_id}: {e}", exc_info=True)

async def get_downloaded_entries_for_path_fixing() -> list[dict]:
    """Pobiera wszystkie wpisy z bazy, które pochodzą z pobierania (nie z importu lokalnego) i posiadają ścieżki do weryfikacji przez path_fix_tool."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT id, final_path, expected_path FROM downloaded_media WHERE (source IS NULL OR source != 'local_import') AND final_path IS NOT NULL AND final_path != ''"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania wpisów do naprawy ścieżek: {e}", exc_info=True)
        return []

async def update_paths_for_entry_by_id(entry_id: int, new_final_path: str, new_expected_path: str):
    """Aktualizuje `final_path` i `expected_path` dla pojedynczego wpisu w bazie danych."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET final_path = {placeholder}, expected_path = {placeholder} WHERE id = {placeholder}"
                await cursor.execute(sql, (new_final_path, new_expected_path, entry_id))
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Błąd aktualizacji ścieżek dla ID {entry_id}: {e}", exc_info=True)

async def get_imported_image_paths_for_analysis(extensions: tuple) -> list[Path]:
    """
    Pobiera ścieżki do plików zaimportowanych z dysku (`local_import`),
    które pasują do podanych rozszerzeń.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                extension_queries = [f"LOWER(final_path) LIKE '%.{ext.strip('.')}'" for ext in extensions]
                query = f"""
                    SELECT final_path FROM downloaded_media
                    WHERE source = 'local_import' AND final_path IS NOT NULL
                    AND ({' OR '.join(extension_queries)})
                """
                await cursor.execute(query)
                rows = await cursor.fetchall()
                results = []
                for row in rows:
                    if row[0]:
                        path_obj = Path(row[0])
                        if await asyncio.to_thread(path_obj.exists):
                            results.append(path_obj)
                return results
    except Exception as e:
        logger.error(f"Błąd pobierania ścieżek do analizy obrazów importowanych: {e}", exc_info=True)
        return []

async def get_video_paths_for_analysis(extensions: tuple, source_filter: str = 'all') -> list[Path]:
    """
    Pobiera ścieżki do plików wideo pasujących do podanych rozszerzeń,
    z możliwością filtrowania według źródła pliku.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                extension_queries = [f"LOWER(final_path) LIKE '%.{ext.strip('.')}'" for ext in extensions]
                query = f"""
                    SELECT final_path FROM downloaded_media
                    WHERE final_path IS NOT NULL
                    AND status IN ('downloaded', 'skipped', 'scanned')
                    AND ({' OR '.join(extension_queries)})
                """
                if source_filter == 'downloaded':
                    query += " AND (source IS NULL OR source = 'google_photos')"
                elif source_filter == 'local_import':
                    query += " AND source = 'local_import'"
                await cursor.execute(query)
                rows = await cursor.fetchall()
                results = []
                for row in rows:
                    if row[0]:
                        path_obj = Path(row[0])
                        if await asyncio.to_thread(path_obj.exists):
                            results.append(path_obj)
                return results
    except Exception as e:
        logger.error(f"Błąd pobierania ścieżek do analizy wideo: {e}", exc_info=True)
        return []

async def get_images_to_tag() -> List[Dict[str, Any]]:
    """
    Pobiera z bazy listę obrazów, które są gotowe do przetworzenia przez
    moduł inteligentnego tagowania (AI Tagger).
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row

                # ZOPTYMALIZOWANE ZAPYTANIE
                if DB_TYPE == "mariadb":
                    # Używamy JSON_LENGTH dla wydajności. Sprawdza czy JSON jest pusty.
                    query = """
                        SELECT id, final_path FROM downloaded_media
                        WHERE status = 'downloaded'
                        AND (ai_tags IS NULL OR JSON_LENGTH(ai_tags) = 0)
                        AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')
                    """
                else: # sqlite
                    # Upraszczamy warunek dla SQLite. Zakładamy, że niepoprawne puste tagi ('[]') 
                    # są rzadkością i nie warto dla nich spowalniać każdego zapytania.
                    query = """
                        SELECT id, final_path FROM downloaded_media
                        WHERE status = 'downloaded'
                        AND (ai_tags IS NULL OR ai_tags = '')
                        AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')
                    """

                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania obrazów do tagowania AI: {e}", exc_info=True)
        return []

async def update_ai_tags_batch(updates: List[Tuple[str, int]]):
    """
    Zapisuje partię tagów AI (jako string JSON) do bazy danych.
    """
    await setup_database()
    if not updates:
        return
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET ai_tags = {placeholder} WHERE id = {placeholder}"
                
                await cursor.executemany(sql, updates)
                
                if DB_TYPE == "sqlite":
                    await conn.commit()
                logger.info(f"Zapisano partię {len(updates)} tagów AI do bazy danych.")
    except Exception as e:
        logger.error(f"Błąd podczas wsadowej aktualizacji tagów AI: {e}", exc_info=True)

#################

async def get_images_without_perceptual_hash() -> List[Dict[str, Any]]:
    """
    Pobiera z bazy listę obrazów o statusie 'downloaded', które nie mają
    jeszcze obliczonego hasha percepcyjnego (pHash).
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                
                path_column = "REPLACE(LOWER(final_path), '\\\\', '/')" if DB_TYPE == "mariadb" else "LOWER(final_path)"
                like_clause = f"({path_column} LIKE '%.jpg' OR {path_column} LIKE '%.jpeg' OR {path_column} LIKE '%.png')"
                
                query = f"""
                    SELECT id, final_path FROM downloaded_media
                    WHERE (perceptual_hash IS NULL OR perceptual_hash = '') 
                    AND status = 'downloaded'
                    AND {like_clause}
                """
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania obrazów bez hasha percepcyjnego: {e}", exc_info=True)
        return []

async def update_perceptual_hash_batch(updates: List[Tuple[str, int]]):
    """
    Zapisuje partię obliczonych hashy percepcyjnych (pHash) do bazy danych.
    """
    await setup_database()
    if not updates: return
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET perceptual_hash = {placeholder} WHERE id = {placeholder}"
                await cursor.executemany(sql, updates)
                if DB_TYPE == "sqlite":
                    await conn.commit()
                logger.info(f"Zapisano partię {len(updates)} hashy percepcyjnych do bazy danych.")
    except Exception as e:
        logger.error(f"Błąd podczas wsadowej aktualizacji hashy percepcyjnych: {e}", exc_info=True)

#async def get_all_perceptual_hashes() -> List[Dict[str, Any]]:
#    """
#    Pobiera wszystkie obliczone hashe percepcyjne (pHash) z bazy danych.
#    """
#    await setup_database()
#    try:
#        import imagehash
#    except ImportError:
#        logger.error("Biblioteka 'imagehash' nie jest zainstalowana. Nie można przetworzyć hashy.")
#        return []
#    all_hashes_list = []
#    try:
#        async with get_db_connection() as conn:
#            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
#            async with conn.cursor(cursor_type) as cursor:
#                if DB_TYPE == "sqlite":
#                    cursor.row_factory = aiosqlite.Row
#                json_extract_syntax = "JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.DateTime'))" if DB_TYPE == "mariadb" else "json_extract(metadata_json, '$.DateTime')"
#                query = f"SELECT id, url, final_path, perceptual_hash, {json_extract_syntax} as dt_str FROM downloaded_media WHERE perceptual_hash IS NOT NULL AND perceptual_hash != '' AND status = 'downloaded'"
#                await cursor.execute(query)
#                async for rec in cursor:
#                    rec_dict = dict(rec)
#                    try:
#                        dt_str_val = rec_dict.get('dt_str')
#                        all_hashes_list.append({ "id": rec_dict['id'], "url": rec_dict['url'], "path": Path(rec_dict['final_path']), "hash": imagehash.hex_to_hash(rec_dict['perceptual_hash']), "datetime": datetime.fromisoformat(dt_str_val.replace('Z', '+00:00')) if dt_str_val else None })
#                    except (ValueError, TypeError, KeyError) as e:
#                        logger.warning(f"Pominięto rekord z nieprawidłowym hashem lub datą dla ID {rec_dict.get('id')}: {e}")
#                        continue
#        return all_hashes_list
#    except Exception as e:
#        logger.error(f"Błąd podczas pobierania hashy percepcyjnych z bazy: {e}", exc_info=True)
#        return []

async def get_all_perceptual_hashes() -> List[Dict]:
    """
    Pobiera wszystkie rekordy z bazy, które posiadają obliczony hash percepcyjny.
    """
    query = """
        SELECT id, final_path, perceptual_hash, timestamp FROM downloaded_media
        WHERE perceptual_hash IS NOT NULL AND perceptual_hash != ''
    """
    
    try:
        async with get_db_connection() as conn:
            # === POCZĄTEK POPRAWKI: Zmieniono `DB_type` na `DB_TYPE` ===
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            # === KONIEC POPRAWKI ===
            async with conn.cursor(cursor_type) as cursor:
                await cursor.execute(query)
                rows = await cursor.fetchall()
                if DB_TYPE == "sqlite":
                    return [
                        {"id": r[0], "path": r[1], "hash": r[2], "datetime": datetime.fromisoformat(r[3]) if r[3] else None}
                        for r in rows
                    ]
                # aiomysql z DictCursor zwraca już listę słowników
                # Dodajemy klucze dla spójności, jeśli ich brakuje
                return [{**row, 'path': row.get('final_path'), 'hash': row.get('perceptual_hash'), 'datetime': row.get('timestamp')} for row in rows]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania hashy percepcyjnych: {e}", exc_info=True)
        return []

async def get_metadata_for_display(entry_id: int, file_path: Path) -> Dict[str, str]:
    """
    Pobiera metadane dla pojedynczego wpisu i parsuje je do formatu
    przyjaznego do wyświetlania w interfejsie użytkownika.
    """
    await setup_database()
    from .utils import format_size_for_display
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                await cursor.execute(f"SELECT metadata_json FROM downloaded_media WHERE id = {placeholder}", (entry_id,))
                row = await cursor.fetchone()
                if not row: return {}
        metadata = json.loads(row[0] or '{}')
        date_tags = ['EXIF:DateTimeOriginal', 'EXIF:CreateDate', 'QuickTime:CreateDate', 'DateTime', 'XMP:CreateDate', 'File:FileModifyDate']
        date_str = next((str(metadata[tag]).split('+')[0].strip() for tag in date_tags if tag in metadata), "Brak")
        dimensions_str = metadata.get('File:ImageSize') or (f"{metadata.get('EXIF:ImageWidth')}x{metadata.get('EXIF:ImageHeight')}" if 'EXIF:ImageWidth' in metadata else "Brak")
        size_str = "Brak pliku"
        try:
            if await asyncio.to_thread(file_path.exists):
                size_str = format_size_for_display((await asyncio.to_thread(file_path.stat)).st_size)
        except (OSError, FileNotFoundError): pass
        file_type = metadata.get('File:FileType', "Brak")
        camera_model = metadata.get('EXIF:Model') or metadata.get('Camera', "Brak")
        f_number, exposure_time, iso = metadata.get('EXIF:FNumber'), metadata.get('EXIF:ExposureTime'), metadata.get('EXIF:ISO')
        exposure_str = f"f/{f_number}, {exposure_time}s, ISO {iso}" if all([f_number, exposure_time, iso]) else "Brak"
        lat, lon = metadata.get('EXIF:GPSLatitude'), metadata.get('EXIF:GPSLongitude')
        gps_str = f"{lat}, {lon}" if all([lat, lon]) else "Brak"
        return { "date": date_str, "dimensions": dimensions_str, "size": size_str, "type": file_type, "camera": camera_model, "exposure": exposure_str, "gps": gps_str }
    except Exception as e:
        logger.error(f"Błąd podczas pobierania metadanych do wyświetlenia dla ID {entry_id}: {e}", exc_info=True)
        return {}

async def clear_all_perceptual_hashes() -> int:
    """
    Czyści (ustawia na NULL) wszystkie istniejące hashe percepcyjne w bazie danych.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("UPDATE downloaded_media SET perceptual_hash = NULL WHERE perceptual_hash IS NOT NULL")
                if DB_TYPE == "sqlite":
                    await conn.commit()
                return cursor.rowcount
    except Exception as e:
        logger.error(f"Błąd podczas czyszczenia hashy percepcyjnych: {e}", exc_info=True)
        return 0

async def get_all_images_for_phash_recalculation() -> list[dict]:
    """
    Pobiera z bazy listę WSZYSTKICH obrazów o statusie 'downloaded',
    które nadają się do obliczenia hasha percepcyjnego.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT id, final_path FROM downloaded_media WHERE status = 'downloaded' AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania wszystkich obrazów do ponownego hashowania: {e}", exc_info=True)
        return []

async def get_imported_images_without_perceptual_hash() -> list[dict]:
    """
    Pobiera z bazy listę obrazów zaimportowanych z dysku,
    które nie mają jeszcze obliczonego hasha percepcyjnego (pHash).
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT id, final_path FROM downloaded_media WHERE (perceptual_hash IS NULL OR perceptual_hash = '') AND source = 'local_import' AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania importowanych obrazów bez hasha percepcyjnego: {e}", exc_info=True)
        return []

async def add_face(media_id: int, embedding: np.ndarray, facial_area: dict, model_name: str) -> Optional[int]:
    """
    Dodaje nowo wykrytą (nieznaną) twarz do bazy, z odporną na błędy
    serializacją obszaru twarzy.
    """
    await setup_database()
    try:
        embedding_blob = pickle.dumps(embedding)

        # --- KLUCZOWA POPRAWKA: Bezpieczne przetwarzanie współrzędnych ---
        safe_facial_area = {}
        for key, value in facial_area.items():
            # Najpierw wyodrębnij właściwą wartość
            raw_val = value[0] if isinstance(value, tuple) and value else value
            # Sprawdź, czy wartość nie jest None, zanim spróbujesz konwersji
            if raw_val is not None:
                safe_facial_area[key] = int(raw_val)
            else:
                # Jeśli wartość to None, przypisz 0 lub zaloguj ostrzeżenie
                safe_facial_area[key] = 0
                logger.warning(f"Otrzymano wartość None dla klucza '{key}' w facial_area. Ustawiono na 0.")
        # --- KONIEC POPRAWKI ---

        facial_area_json = json.dumps(safe_facial_area)
#        facial_area_json = json.dumps(facial_area)
        
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                sql = ("INSERT INTO faces (media_id, embedding, facial_area, model_name) VALUES (%s, %s, %s, %s)" if DB_TYPE == "mariadb" else "INSERT INTO faces (media_id, embedding, facial_area, model_name) VALUES (?, ?, ?, ?)")
                await cursor.execute(sql, (media_id, embedding_blob, facial_area_json, model_name))
                if DB_TYPE == "sqlite": await conn.commit()
                return cursor.lastrowid
    except Exception as e:
        logger.error(f"Nie udało się dodać twarzy dla media_id {media_id}: {e}", exc_info=True)
        return None

async def update_indexing_status_batch(updates: list[tuple[int, str, str]]):
    """
    Zapisuje partię zaktualizowanych statusów indeksowania do bazy.
    Oczekuje listy krotek: (media_id, model_name, new_status).
    """
    await setup_database()
    if not updates: return
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                if DB_TYPE == "mariadb":
                    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    params = [(upd[0], upd[1], upd[2], now_str) for upd in updates]
                    sql = "INSERT INTO face_indexing_status (media_id, model_name, status, last_updated) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE status = VALUES(status), last_updated = VALUES(last_updated);"
                else:
                    params = updates
                    sql = "INSERT INTO face_indexing_status (media_id, model_name, status, last_updated) VALUES (?, ?, ?, CURRENT_TIMESTAMP) ON CONFLICT(media_id, model_name) DO UPDATE SET status = excluded.status, last_updated = excluded.last_updated;"
                await cursor.executemany(sql, params)
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Błąd wsadowej aktualizacji statusu indeksowania: {e}", exc_info=True)

async def get_images_for_face_indexing_optimized(model_name: str, force_rescan: bool = False, limit: Optional[int] = None) -> list[dict]:
    """Pobiera z bazy listę obrazów, które wymagają zaindeksowania."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"

                if force_rescan:
                    await cursor.execute(f"UPDATE face_indexing_status SET status = 'pending' WHERE model_name = {placeholder}", (model_name,))

                insert_ignore_sql = "INSERT IGNORE INTO" if DB_TYPE == "mariadb" else "INSERT OR IGNORE INTO"
                like_clauses = " AND (LOWER(d.final_path) LIKE '%%.jpg' OR LOWER(d.final_path) LIKE '%%.jpeg' OR LOWER(d.final_path) LIKE '%%.png')"
                
                await cursor.execute(
                    f"""
                    {insert_ignore_sql} face_indexing_status (media_id, model_name, status)
                    SELECT d.id, {placeholder}, 'pending'
                    FROM downloaded_media d
                    WHERE
                        d.status IN ('downloaded', 'skipped', 'scanned') AND d.final_path IS NOT NULL
                        {like_clauses}
                    """,
                    (model_name,)
                )

                query = f"SELECT d.id, d.final_path FROM downloaded_media d JOIN face_indexing_status s ON d.id = s.media_id WHERE s.model_name = {placeholder} AND s.status = 'pending'"
                params = [model_name]

                if limit and limit > 0:
                    query += " LIMIT %s" if DB_TYPE == "mariadb" else " LIMIT ?"
                    params.append(limit)
                
                await cursor.execute(query, tuple(params))
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania obrazów do indeksowania (optimized): {e}", exc_info=True)
        return []

async def get_all_people(model_name: str) -> List[Dict[str, Any]]:
    """Pobiera listę wszystkich AKTYWNYCH znanych osób dla konkretnego modelu AI."""
    await setup_database()
    people = []
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                query = f"SELECT person_id, name, master_embedding FROM people WHERE model_name = {placeholder} AND status = 'active' ORDER BY name"
                await cursor.execute(query, (model_name,))
                rows = await cursor.fetchall()
                for row in rows:
                    person_data = dict(row)
                    person_data['master_embedding'] = pickle.loads(person_data['master_embedding'])
                    people.append(person_data)
    except Exception as e:
        logger.error(f"Nie udało się pobrać listy aktywnych osób: {e}", exc_info=True)
    return people

async def get_unknown_faces(model_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Pobiera listę nieznanych twarzy dla danego modelu."""
    await setup_database()
    unknown_faces = []
    placeholder = "%s" if DB_TYPE == "mariadb" else "?"
    query = f"SELECT face_id, media_id, embedding FROM faces WHERE person_id IS NULL AND model_name = {placeholder}"
    params = [model_name]
    if limit:
        query += " LIMIT %s" if DB_TYPE == "mariadb" else " LIMIT ?"
        params.append(limit)
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                await cursor.execute(query, tuple(params))
                rows = await cursor.fetchall()
                for row in rows:
                    face_data = dict(row)
                    face_data['embedding'] = pickle.loads(face_data['embedding'])
                    unknown_faces.append(face_data)
    except Exception as e:
        logger.error(f"Nie udało się pobrać listy nieznanych twarzy: {e}", exc_info=True)
    return unknown_faces

async def tag_face(face_id: int, person_id: int):
    """Przypisuje znaną osobę do nierozpoznanej twarzy (taguje)."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE faces SET person_id = {placeholder} WHERE face_id = {placeholder}"
                await cursor.execute(sql, (person_id, face_id))
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Nie udało się otagować twarzy face_id {face_id}: {e}", exc_info=True)

async def tag_faces_batch(updates: List[Tuple[int, int]]):
    """Przypisuje znane osoby do nierozpoznanych twarzy w jednej, wsadowej operacji."""
    await setup_database()
    if not updates: return
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE faces SET person_id = {placeholder} WHERE face_id = {placeholder}"
                await cursor.executemany(sql, updates)
                if DB_TYPE == "sqlite":
                    await conn.commit()
                logger.info(f"Wsadowo otagowano {len(updates)} twarzy.")
    except Exception as e:
        logger.error(f"Błąd podczas wsadowego tagowania twarzy: {e}", exc_info=True)

async def add_person(name: str, model_name: str, embedding: np.ndarray, source_media_id: Optional[int] = None) -> Optional[int]:
    """Dodaje nową znaną osobę do bazy danych."""
    await setup_database()
    try:
        embedding_blob = pickle.dumps(embedding)
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"INSERT INTO people (name, model_name, master_embedding, source_media_id) VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder})"
                await cursor.execute(sql, (name, model_name, embedding_blob, source_media_id))
                if DB_TYPE == "sqlite":
                    await conn.commit()
                return cursor.lastrowid
    except Exception as e:
        if "UNIQUE constraint failed" in str(e) or "Duplicate entry" in str(e):
            logger.warning(f"Osoba '{name}' dla modelu '{model_name}' już istnieje w bazie.")
        else:
            logger.error(f"Nie udało się dodać osoby '{name}': {e}", exc_info=True)
        return None

async def get_records_for_exif_processing(process_mode: str) -> List[Dict[str, Any]] | None:
    """Pobiera listę rekordów do przetworzenia przez Exif Writer."""
    await setup_database()
    logger.info(f"Pobieram rekordy z bazy dla Exif Writer w trybie: '{process_mode}'.")
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row

                if DB_TYPE == "mariadb":
                    base_query = """
                        SELECT metadata_json, final_path FROM downloaded_media
                        WHERE processing_status = 'Sukces'
                        AND metadata_json IS NOT NULL AND JSON_VALID(metadata_json) = 1
                        AND final_path IS NOT NULL AND final_path != ''
                    """
                else: # sqlite
                    base_query = """
                        SELECT metadata_json, final_path FROM downloaded_media
                        WHERE processing_status = 'Sukces'
                        AND metadata_json IS NOT NULL AND json_valid(metadata_json) = 1
                        AND final_path IS NOT NULL AND final_path != ''
                    """
                
                conditions = {
                    'new_only': "AND (exif_write_status IS NULL OR exif_write_status = '' OR exif_write_status = 'pending')",
                    'retry_errors': "AND (exif_write_status = 'Error' OR exif_write_status = 'Partial')",
                    'force_refresh': ""
                }
                query = base_query + conditions.get(process_mode, "")
                
                await cursor.execute(query)
                records = [dict(row) for row in await cursor.fetchall()]
                logger.info(f"Pobrano {len(records)} rekordów dla Exif Writer.")
                return records
    except Exception as e:
        logger.critical("Nie można pobrać danych z bazy dla Exif Writer.", exc_info=True)
        return None

async def update_person_status(person_id: int, new_status: str):
    """Aktualizuje status dla istniejącej osoby ('active' lub 'ignored')."""
    await setup_database()
    if new_status not in ['active', 'ignored']:
        logger.error(f"Niewspierany status osoby: {new_status}")
        return
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE people SET status = {placeholder} WHERE person_id = {placeholder}"
                await cursor.execute(sql, (new_status, person_id))
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Nie udało się zaktualizować statusu osoby o ID {person_id}: {e}", exc_info=True)

async def update_person_name(person_id: int, new_name: str):
    """Aktualizuje nazwę dla istniejącej osoby."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE people SET name = {placeholder} WHERE person_id = {placeholder}"
                await cursor.execute(sql, (new_name, person_id))
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Nie udało się zaktualizować osoby o ID {person_id}: {e}", exc_info=True)

async def delete_person(person_id: int, hard_delete: bool = False):
    """Usuwa znaną osobę (miękko lub twardo)."""
    if not hard_delete:
        await update_person_status(person_id, 'ignored')
    else:
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cursor:
                    placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                    sql = f"DELETE FROM people WHERE person_id = {placeholder}"
                    await cursor.execute(sql, (person_id,))
                    if DB_TYPE == "sqlite":
                        await conn.commit()
                    logger.info(f"Fizycznie usunięto profil osoby o ID {person_id}.")
        except Exception as e:
            logger.error(f"Nie udało się fizycznie usunąć osoby o ID {person_id}: {e}", exc_info=True)

async def get_all_people_with_status(model_name: str) -> List[Dict[str, Any]]:
    """Pobiera listę WSZYSTKICH osób (aktywnych i ignorowanych) wraz z ich statusem."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                query = f"SELECT person_id, name, status FROM people WHERE model_name = {placeholder} ORDER BY name"
                await cursor.execute(query, (model_name,))
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Nie udało się pobrać listy wszystkich osób ze statusem: {e}", exc_info=True)
        return []

async def reassign_faces(source_person_id: int, target_person_id: int):
    """Przenosi wszystkie twarze przypisane do osoby źródłowej do osoby docelowej."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE faces SET person_id = {placeholder} WHERE person_id = {placeholder}"
                await cursor.execute(sql, (target_person_id, source_person_id))
                if DB_TYPE == "sqlite":
                    await conn.commit()
                logger.info(f"Przeniesiono {cursor.rowcount} twarzy z osoby ID {source_person_id} do ID {target_person_id}.")
    except Exception as e:
        logger.error(f"Błąd podczas przenoszenia twarzy: {e}", exc_info=True)

async def delete_faces_for_person(person_id: int):
    """Fizycznie usuwa wszystkie wpisy twarzy powiązane z danym profilem osoby."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"DELETE FROM faces WHERE person_id = {placeholder}"
                await cursor.execute(sql, (person_id,))
                if DB_TYPE == "sqlite":
                    await conn.commit()
                logger.info(f"Usunięto {cursor.rowcount} twarzy powiązanych z osobą o ID {person_id}.")
    except Exception as e:
        logger.error(f"Błąd podczas usuwania twarzy dla osoby o ID {person_id}: {e}", exc_info=True)

async def get_person_details(person_id: int) -> Optional[Dict[str, Any]]:
    """Pobiera szczegółowe dane dla pojedynczej osoby, włączając awatar."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                query = f"SELECT p.person_id, p.name, p.status, p.model_name, m.final_path as avatar_path FROM people p LEFT JOIN downloaded_media m ON p.source_media_id = m.id WHERE p.person_id = {placeholder}"
                await cursor.execute(query, (person_id,))
                row = await cursor.fetchone()
                return dict(row) if row else None
    except Exception as e:
        logger.error(f"Błąd podczas pobierania szczegółów osoby o ID {person_id}: {e}", exc_info=True)
        return None

async def get_all_embeddings_for_person(person_id: int) -> List[np.ndarray]:
    """Pobiera listę wszystkich embeddingów dla twarzy otagowanych jako dana osoba."""
    await setup_database()
    embeddings = []
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                query = f"SELECT embedding FROM faces WHERE person_id = {placeholder}"
                await cursor.execute(query, (person_id,))
                rows = await cursor.fetchall()
                for row in rows:
                    embeddings.append(pickle.loads(row[0]))
    except Exception as e:
        logger.error(f"Nie udało się pobrać embeddingów dla osoby ID {person_id}: {e}", exc_info=True)
    return embeddings

async def update_master_embedding(person_id: int, new_embedding: np.ndarray):
    """Aktualizuje wzorcowy (master) embedding dla danej osoby."""
    await setup_database()
    try:
        embedding_blob = pickle.dumps(new_embedding)
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE people SET master_embedding = {placeholder} WHERE person_id = {placeholder}"
                await cursor.execute(sql, (embedding_blob, person_id))
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Nie udało się zaktualizować wzorcowego embeddingu dla osoby ID {person_id}: {e}", exc_info=True)

async def get_all_people_with_attributes(model_name: str) -> List[Dict[str, Any]]:
    """
    Pobiera listę osób z UŚREDNIONYMI atrybutami, liczbą twarzy
    i datą ostatniego wystąpienia. Główne zapytanie jest uproszczone,
    aby uniknąć błędów w MariaDB.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                # UPROSZCZONE ZAPYTANIE: Obliczamy tylko proste agregaty
                query = f"""
                    SELECT 
                        p.person_id, 
                        p.name, 
                        p.status, 
                        CAST(AVG(f.estimated_age) AS UNSIGNED) as avg_age,
                        COUNT(f.face_id) as face_count,
                        MAX(f.timestamp) as last_seen
                    FROM 
                        people p 
                    LEFT JOIN 
                        faces f ON p.person_id = f.person_id 
                    WHERE 
                        p.model_name = {placeholder} 
                    GROUP BY 
                        p.person_id, p.name, p.status 
                    ORDER BY 
                        p.name
                """
                await cursor.execute(query, (model_name,))
                people_list = [dict(row) for row in await cursor.fetchall()]

                # Krok 2: Dociągnij dominujące atrybuty osobnymi zapytaniami
                for person in people_list:
                    person_id = person['person_id']
                    
                    # Dociągnij płeć
                    await cursor.execute(f"SELECT estimated_gender FROM faces WHERE person_id = {placeholder} GROUP BY estimated_gender ORDER BY COUNT(*) DESC LIMIT 1", (person_id,))
                    gender_row = await cursor.fetchone()
                    person['dominant_gender'] = gender_row['estimated_gender'] if gender_row and gender_row['estimated_gender'] else None

                    # Dociągnij emocję
                    await cursor.execute(f"SELECT dominant_emotion FROM faces WHERE person_id = {placeholder} GROUP BY dominant_emotion ORDER BY COUNT(*) DESC LIMIT 1", (person_id,))
                    emotion_row = await cursor.fetchone()
                    person['common_emotion'] = emotion_row['dominant_emotion'] if emotion_row and emotion_row['dominant_emotion'] else None
                    
                    # Dociągnij rasę
                    await cursor.execute(f"SELECT dominant_race FROM faces WHERE person_id = {placeholder} GROUP BY dominant_race ORDER BY COUNT(*) DESC LIMIT 1", (person_id,))
                    race_row = await cursor.fetchone()
                    person['dominant_race'] = race_row['dominant_race'] if race_row and race_row['dominant_race'] else None

                return people_list
    except Exception as e:
        logger.error(f"Nie udało się pobrać listy osób z atrybutami: {e}", exc_info=True)
        return []

async def get_face_entries_for_person(person_id: int) -> List[Dict[str, Any]]:
    """Pobiera listę wszystkich wystąpień twarzy danej osoby z atrybutami i datą zdjęcia."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                
                # Używamy COALESCE, aby spróbować wyciągnąć datę z kilku możliwych pól w metadanych.
                # To zwiększa szansę na znalezienie poprawnej daty.
                if DB_TYPE == "mariadb":
                    json_extract_syntax = """
                        COALESCE(
                            JSON_UNQUOTE(JSON_EXTRACT(m.metadata_json, '$.DateTime')),
                            JSON_UNQUOTE(JSON_EXTRACT(m.metadata_json, '$."EXIF:DateTimeOriginal"')),
                            JSON_UNQUOTE(JSON_EXTRACT(m.metadata_json, '$."QuickTime:CreateDate"'))
                        )
                    """
                else: # sqlite
                    json_extract_syntax = """
                        COALESCE(
                            json_extract(m.metadata_json, '$.DateTime'),
                            json_extract(m.metadata_json, '$."EXIF:DateTimeOriginal"'),
                            json_extract(m.metadata_json, '$."QuickTime:CreateDate"')
                        )
                    """

                query = f"""
                    SELECT 
                        f.face_id, 
                        f.estimated_age, 
                        f.dominant_emotion, 
                        f.estimated_gender,
                        f.dominant_race, 
                        m.final_path, 
                        ({json_extract_syntax}) as photo_date 
                    FROM 
                        faces f 
                    JOIN 
                        downloaded_media m ON f.media_id = m.id 
                    WHERE 
                        f.person_id = {placeholder} 
                    ORDER BY 
                        photo_date DESC
                """
                await cursor.execute(query, (person_id,))
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania wpisów twarzy dla osoby ID {person_id}: {e}", exc_info=True)
        return []

async def get_all_tagged_people() -> List[Dict[str, Any]]:
    """Pobiera listę osób, które mają przypisane co najmniej jedno zdjęcie."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                query = "SELECT p.person_id, p.name, COUNT(f.face_id) as photo_count FROM people p JOIN faces f ON p.person_id = f.person_id GROUP BY p.person_id, p.name ORDER BY p.name;"
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Nie udało się pobrać listy otagowanych osób: {e}", exc_info=True)
        return []

async def get_media_for_person(person_id: int) -> List[Dict[str, Any]]:
    """Pobiera wszystkie media, na których otagowano daną osobę."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                query = f"SELECT DISTINCT m.id, m.final_path FROM downloaded_media m JOIN faces f ON m.id = f.media_id WHERE f.person_id = {placeholder} ORDER BY m.final_path;"
                await cursor.execute(query, (person_id,))
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Nie udało się pobrać mediów dla osoby o ID {person_id}: {e}", exc_info=True)
        return []

async def get_face_details_for_review(model_name: str) -> List[Dict[str, Any]]:
    """Pobiera pełen zestaw szczegółów dla wszystkich nieotagowanych twarzy."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                query = f"SELECT f.face_id, f.media_id, f.embedding, f.facial_area, m.final_path FROM faces f JOIN downloaded_media m ON f.media_id = m.id WHERE f.model_name = {placeholder} AND f.person_id IS NULL ORDER BY f.face_id"
                await cursor.execute(query, (model_name,))
                rows = await cursor.fetchall()
                results = []
                for row in rows:
                    details = dict(row)
                    details['embedding'] = pickle.loads(details['embedding'])
                    details['facial_area'] = json.loads(details['facial_area'])
                    results.append(details)
                return results
    except Exception as e:
        logger.error(f"Błąd pobierania szczegółów twarzy do przeglądania: {e}", exc_info=True)
        return []

async def delete_face_by_id(face_id: int) -> bool:
    """Usuwa pojedynczy wpis twarzy z bazy danych."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"DELETE FROM faces WHERE face_id = {placeholder}"
                await cursor.execute(sql, (face_id,))
                if DB_TYPE == "sqlite":
                    await conn.commit()
                return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Błąd podczas usuwania twarzy ID {face_id}: {e}", exc_info=True)
        return False

async def untag_face_by_media_and_person(media_id: int, person_id: int):
    """Usuwa powiązanie (tag) między zdjęciem a osobą."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE faces SET person_id = NULL WHERE media_id = {placeholder} AND person_id = {placeholder}"
                await cursor.execute(sql, (media_id, person_id))
                if DB_TYPE == "sqlite":
                    await conn.commit()
                if cursor.rowcount > 0:
                    logger.info(f"Usunięto {cursor.rowcount} tagów dla osoby ID {person_id} na zdjęciu ID {media_id}.")
                else:
                    logger.warning(f"Nie znaleziono tagów do usunięcia dla osoby ID {person_id} na zdjęciu ID {media_id}.")
    except Exception as e:
        logger.error(f"Błąd podczas usuwania tagu dla media_id {media_id}: {e}", exc_info=True)

async def update_person_avatar(person_id: int, media_id: int):
    """Aktualizuje zdjęcie profilowe (awatar) dla danej osoby."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE people SET source_media_id = {placeholder} WHERE person_id = {placeholder}"
                await cursor.execute(sql, (media_id, person_id))
                if DB_TYPE == "sqlite":
                    await conn.commit()
                logger.info(f"Pomyślnie zaktualizowano awatar dla osoby ID {person_id} na zdjęcie ID {media_id}.")
    except Exception as e:
        logger.error(f"Nie udało się zaktualizować awatara dla osoby ID {person_id}: {e}", exc_info=True)

async def get_faces_for_attribute_analysis(scope: str, strategy: str) -> List[Dict[str, Any]]:
    """
    Pobiera listę twarzy do analizy atrybutów, elastycznie budując zapytanie
    w oparciu o wybrany zakres (scope) i strategię (strategy).

    Args:
        scope (str): Zakres skanowania. Może być 'people_only' lub 'all_faces'.
        strategy (str): Strategia. Może być 'fill_missing' lub 'force_rescan'.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                
                base_query = """
                    SELECT 
                        f.face_id, m.final_path, f.facial_area,
                        f.estimated_age, f.estimated_gender, f.dominant_emotion, f.dominant_race
                    FROM faces f 
                    JOIN downloaded_media m ON f.media_id = m.id
                """
                
                where_clauses = []
                
                # Krok 1: Zastosuj filtr ZAKRESU (scope)
                if scope == 'people_only':
                    where_clauses.append("f.person_id IS NOT NULL")
                # Dla 'all_faces' nie dodajemy żadnego warunku, bierzemy wszystko

                # Krok 2: Zastosuj filtr STRATEGII (strategy)
                if strategy == 'fill_missing':
                    missing_condition = "(f.estimated_gender IS NULL OR f.estimated_age IS NULL OR f.dominant_emotion IS NULL OR f.dominant_race IS NULL)"
                    where_clauses.append(missing_condition)
                # Dla 'force_rescan' nie dodajemy żadnego warunku, bierzemy wszystko
                
                # Krok 3: Połącz warunki w finalne zapytanie
                final_query = base_query
                if where_clauses:
                    final_query += " WHERE " + " AND ".join(where_clauses)
                
                await cursor.execute(final_query)
                rows = await cursor.fetchall()
                results = []
                for row in rows:
                    details = dict(row)
                    # Upewniamy się, że facial_area jest poprawnie sparsowane
                    if isinstance(details.get('facial_area'), str):
                        details['facial_area'] = json.loads(details['facial_area'])
                    results.append(details)
                return results
    except Exception as e:
        logger.error(f"Błąd podczas pobierania twarzy do analizy atrybutów: {e}", exc_info=True)
        return []

async def update_face_attributes_batch(updates: List[Tuple[Optional[str], Optional[int], Optional[str], Optional[str], int]]):
    """Zapisuje partię atrybutów (w tym rasę) do tabeli `faces`."""
    await setup_database()
    if not updates: return
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                # Ważna kolejność: gender, age, emotion, race, face_id
                sql = f"UPDATE faces SET estimated_gender = {placeholder}, estimated_age = {placeholder}, dominant_emotion = {placeholder}, dominant_race = {placeholder} WHERE face_id = {placeholder}"
                await cursor.executemany(sql, updates)
                if DB_TYPE == "sqlite":
                    await conn.commit()
    except Exception as e:
        logger.error(f"Błąd wsadowej aktualizacji atrybutów twarzy: {e}", exc_info=True)

async def get_face_details_by_id(face_id: int) -> Optional[Dict[str, Any]]:
    """Pobiera szczegóły pojedynczej twarzy."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                query = f"SELECT f.face_id, f.media_id, f.embedding, f.facial_area, m.final_path FROM faces f JOIN downloaded_media m ON f.media_id = m.id WHERE f.face_id = {placeholder}"
                await cursor.execute(query, (face_id,))
                row = await cursor.fetchone()
                if row:
                    details = dict(row)
                    details['embedding'] = pickle.loads(details['embedding'])
                    details['facial_area'] = json.loads(details['facial_area'])
                    return details
                return None
    except Exception as e:
        logger.error(f"Błąd podczas pobierania szczegółów twarzy ID {face_id}: {e}", exc_info=True)
        return None

async def get_media_ids_with_indexed_faces(model_name: str) -> List[int]:
    """Zwraca listę ID mediów, które mają już jakieś twarze w bazie dla danego modelu."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                query = f"SELECT DISTINCT media_id FROM faces WHERE model_name = {placeholder}"
                await cursor.execute(query, (model_name,))
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Nie udało się pobrać ID zaindeksowanych mediów: {e}", exc_info=True)
        return []

async def get_media_id_by_path(file_path: str) -> Optional[int]:
    """
    Wyszukuje i zwraca ID wpisu w `downloaded_media` na podstawie pełnej,
    znormalizowanej ścieżki pliku.
    """
    await setup_database()
    try:
        normalized_path = str(Path(file_path).resolve())
        
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                # REPLACE działa tak samo w obu bazach, więc zapytanie jest prawie identyczne
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                # W MariaDB REPLACE wymaga podwójnego backslasha do eskejpowania
                sql = f"SELECT id FROM downloaded_media WHERE REPLACE(final_path, '\\\\', '/') = REPLACE({placeholder}, '\\\\', '/')"
                
                await cursor.execute(sql, (normalized_path,))
                result = await cursor.fetchone()
                
                if result:
                    # Wynik jest krotką (SQLite) lub słownikiem (aiomysql), ujednolicamy
                    media_id = result[0] if isinstance(result, tuple) else (result.get('id') if result else None)
                    if media_id is not None:
                        logger.debug(f"Znaleziono media_id: {media_id} dla ścieżki: {normalized_path}")
                        return media_id
                
                logger.warning(f"Nie znaleziono media_id dla ścieżki w bazie danych: {normalized_path}")
                return None
    except Exception as e:
        logger.error(f"Błąd podczas wyszukiwania media_id dla ścieżki '{file_path}': {e}", exc_info=True)
        return None

async def get_paginated_media_entries(limit: int, offset: int, sort_by: str, sort_order: str, status_filter: str) -> Tuple[int, List[Dict[str, Any]]]:
    """Pobiera stronę z wpisami z bazy, z opcjami sortowania i filtrowania."""
    total_entries = 0
    entries = []
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    conn.row_factory = aiosqlite.Row

                where_clause = ""
                params = []
                if status_filter:
                    where_clause = "WHERE status LIKE %s" if DB_TYPE == "mariadb" else "WHERE status LIKE ?"
                    params.append(f"%{status_filter}%")
                
                count_query = f"SELECT COUNT(id) as count FROM downloaded_media {where_clause}"
                await cursor.execute(count_query, tuple(params))
                total_row = await cursor.fetchone()
                total_entries = total_row[0] if isinstance(total_row, tuple) else total_row.get('count', 0)

                # Zabezpieczamy przed SQL Injection, sprawdzając, czy kolumny są na białej liście
                allowed_sort_cols = ['id', 'status', 'filename', 'retry_count', 'timestamp']
                if sort_by not in allowed_sort_cols: sort_by = 'id'
                if sort_order.upper() not in ['ASC', 'DESC']: sort_order = 'DESC'

                query = f"SELECT id, url, status, retry_count, final_path, expected_path, metadata_json FROM downloaded_media {where_clause} ORDER BY {sort_by} {sort_order} LIMIT %s OFFSET %s"
                if DB_TYPE == "sqlite":
                    query = query.replace('%s', '?')
                
                params.extend([limit, offset])
                await cursor.execute(query, tuple(params))
                entries = [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania strony z bazy danych: {e}", exc_info=True)
    return total_entries, entries

async def update_statuses_by_ids(ids: List[int], new_status: str) -> int:
    """Masowo aktualizuje status dla podanej listy ID."""
    if not ids: return 0
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                params = [(new_status, 0, entry_id) for entry_id in ids]
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET status = {placeholder}, retry_count = {placeholder} WHERE id = {placeholder}"
                await cursor.executemany(sql, params)
                if DB_TYPE == "sqlite":
                    await conn.commit()
                return cursor.rowcount
    except Exception as e:
        logger.error(f"Błąd podczas masowej aktualizacji statusów: {e}", exc_info=True)
        return 0

async def delete_faces_by_media_ids(media_ids: List[int], model_name: str):
    """
    Fizycznie usuwa wszystkie wpisy twarzy dla podanej listy ID mediów
    i konkretnego modelu AI. Używane przed ponownym skanowaniem.
    """
    await setup_database()
    if not media_ids: return
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                # Dzielimy na mniejsze paczki, aby uniknąć problemów z limitem placeholderów
                chunk_size = 500
                for i in range(0, len(media_ids), chunk_size):
                    chunk = media_ids[i:i + chunk_size]
                    
                    if DB_TYPE == "mariadb":
                        placeholders = ','.join(['%s'] * len(chunk))
                        sql = f"DELETE FROM faces WHERE media_id IN ({placeholders}) AND model_name = %s"
                        params = chunk + [model_name]
                    else: # sqlite
                        placeholders = ','.join(['?'] * len(chunk))
                        sql = f"DELETE FROM faces WHERE media_id IN ({placeholders}) AND model_name = ?"
                        params = chunk + [model_name]

                    await cursor.execute(sql, params)
                
                if DB_TYPE == "sqlite":
                    await conn.commit()
                logger.info(f"Usunięto stare wpisy twarzy dla {len(media_ids)} mediów (model: {model_name}) przed ponownym skanowaniem.")
    except Exception as e:
        logger.error(f"Błąd podczas usuwania starych wpisów twarzy: {e}", exc_info=True)

async def update_single_face_attribute(face_id: int, attribute: str, value: Any):
    """
    Aktualizuje pojedynczy atrybut (kolumnę) dla konkretnego wpisu twarzy.
    Funkcja jest zabezpieczona przed SQL injection poprzez mapowanie nazw atrybutów.
    """
    await setup_database()
    
    # Biała lista dozwolonych atrybutów i ich mapowanie na nazwy kolumn w bazie danych.
    # To kluczowe zabezpieczenie przed atakami typu SQL Injection.
    column_map = {
        'age': 'estimated_age',
        'gender': 'estimated_gender',
        'emotion': 'dominant_emotion',
        'race': 'dominant_race'
    }
    
    # Sprawdzamy, czy podany atrybut znajduje się na naszej białej liście.
    column_name = column_map.get(attribute)
    if not column_name:
        logger.error(f"Próba aktualizacji nieznanego lub niedozwolonego atrybutu: '{attribute}'")
        return

    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                # Dynamiczne, ale bezpieczne budowanie zapytania SQL.
                # Nazwa kolumny pochodzi z naszej zaufanej mapy, a nie bezpośrednio od użytkownika.
                # Wartości są przekazywane przez placeholdery.
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE faces SET {column_name} = {placeholder} WHERE face_id = {placeholder}"
                
                await cursor.execute(sql, (value, face_id))
                
                # Zatwierdzenie transakcji jest wymagane tylko dla SQLite w aiosqlite
                if DB_TYPE == "sqlite":
                    await conn.commit()

    except Exception as e:
        logger.error(f"Błąd podczas aktualizacji atrybutu '{attribute}' dla face_id {face_id}: {e}", exc_info=True)

async def get_or_create_media_id_for_path(file_path: Path) -> Optional[int]:
    """
    Wyszukuje media_id dla danej ścieżki. Jeśli nie istnieje, tworzy nowy wpis
    w downloaded_media (jako 'local_import') i zwraca jego ID.
    Jest to kluczowa funkcja dla trybu pełnego skanowania atrybutów.
    """
    await setup_database()
    
    # Najpierw spróbuj znaleźć istniejący wpis
    media_id = await get_media_id_by_path(str(file_path))
    if media_id:
        return media_id

    # Jeśli nie znaleziono, stwórz nowy
    logger.info(f"Nie znaleziono wpisu dla '{file_path.name}', tworzę nowy wpis w bazie...")
    from .database import add_local_file_entry # Import wewnątrz funkcji, by uniknąć cyklicznych zależności
    
    if await add_local_file_entry(file_path, {}):
        # Po dodaniu, spróbuj pobrać ID jeszcze raz
        return await get_media_id_by_path(str(file_path))

    logger.error(f"Nie udało się utworzyć wpisu w bazie dla pliku: {file_path}")
    return None

async def get_tagged_images() -> List[Dict[str, Any]]:
    """
    Pobiera z bazy listę obrazów, które posiadają tagi AI.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row

                if DB_TYPE == "mariadb":
                    query = """
                        SELECT id, final_path, ai_tags FROM downloaded_media
                        WHERE status = 'downloaded'
                        AND (ai_tags IS NOT NULL AND JSON_LENGTH(ai_tags) > 0)
                        ORDER BY final_path DESC
                    """
                else: # sqlite
                    query = """
                        SELECT id, final_path, ai_tags FROM downloaded_media
                        WHERE status = 'downloaded'
                        AND (ai_tags IS NOT NULL AND ai_tags != '' AND ai_tags != '[]')
                        ORDER BY final_path DESC
                    """
                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania otagowanych obrazów: {e}", exc_info=True)
        return []

async def clear_all_ai_tags() -> int:
    """
    Czyści (ustawia na NULL) wszystkie istniejące tagi AI w bazie danych,
    aby umożliwić ponowne tagowanie.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("UPDATE downloaded_media SET ai_tags = NULL WHERE ai_tags IS NOT NULL")
                if DB_TYPE == "sqlite":
                    await conn.commit()
                return cursor.rowcount
    except Exception as e:
        logger.error(f"Błąd podczas czyszczenia tagów AI: {e}", exc_info=True)
        return 0

async def get_records_for_ai_exif_writing() -> List[Dict[str, Any]]:
    """
    Pobiera rekordy, które mają tagi AI, ale nie mają jeszcze flagi potwierdzającej
    zapis tych tagów do metadanych pliku.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row

                # Szukamy klucza 'AI_Tags_Written' w metadanych. Jeśli go nie ma, tagi nie były zapisane.
                if DB_TYPE == "mariadb":
                    query = """
                        SELECT id, final_path, ai_tags FROM downloaded_media
                        WHERE ai_tags IS NOT NULL AND JSON_LENGTH(ai_tags) > 0
                        AND (metadata_json IS NULL OR JSON_EXTRACT(metadata_json, '$."XMP:Subject"') IS NULL)
                    """
                else: # sqlite
                    query = """
                        SELECT id, final_path, ai_tags FROM downloaded_media
                        WHERE ai_tags IS NOT NULL AND ai_tags != '' AND ai_tags != '[]'
                        AND (metadata_json IS NULL OR json_extract(metadata_json, '$."XMP:Subject"') IS NULL)
                    """

                await cursor.execute(query)
                return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania rekordów do zapisu tagów AI: {e}", exc_info=True)
        return []

async def update_ai_tags_for_entry(entry_id: int, new_tags_data: List[Dict[str, Any]]) -> bool:
    """
    Aktualizuje tagi AI dla pojedynczego wpisu w bazie danych.

    Args:
        entry_id (int): ID rekordu w tabeli downloaded_media.
        new_tags_data (List[Dict[str, Any]]): Nowa lista tagów (w formacie z wynikiem).

    Returns:
        bool: True, jeśli aktualizacja się powiodła, w przeciwnym razie False.
    """
    await setup_database()
    try:
        # Konwertuje listę tagów na tekst w formacie JSON
        new_tags_json = json.dumps(new_tags_data, ensure_ascii=False)
        
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                # Wybiera odpowiedni symbol zastępczy ('?' dla SQLite, '%s' dla MariaDB)
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                
                # Przygotowuje zapytanie SQL do aktualizacji
                sql = f"UPDATE downloaded_media SET ai_tags = {placeholder} WHERE id = {placeholder}"
                
                # Wykonuje zapytanie, przekazując bezpiecznie parametry
                await cursor.execute(sql, (new_tags_json, entry_id))
                
                # Zatwierdza zmiany, jeśli używamy SQLite
                if DB_TYPE == "sqlite":
                    await conn.commit()
                
                logger.info(f"Zaktualizowano tagi dla wpisu o ID: {entry_id}")
                
                # Zwraca True, jeśli jakikolwiek wiersz został zmieniony
                return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Błąd podczas aktualizacji tagów dla ID {entry_id}: {e}", exc_info=True)
        return False

async def get_all_unique_tags() -> List[Dict[str, Any]]:
    """
    Pobiera wszystkie rekordy z tagami AI, agreguje je i zwraca posortowaną
    listę unikalnych tagów wraz z liczbą ich wystąpień.

    Returns:
        List[Dict[str, Any]]: Lista słowników, np. [{'tag': 'plaża', 'count': 42}, ...].
    """
    await setup_database()
    logger.info("Rozpoczynam agregację unikalnych tagów AI z bazy danych...")
    tag_counts = defaultdict(int)
    
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row

                # Pobieramy tylko kolumnę ai_tags, gdzie nie jest ona pusta
                if DB_TYPE == "mariadb":
                    query = "SELECT ai_tags FROM downloaded_media WHERE ai_tags IS NOT NULL AND JSON_LENGTH(ai_tags) > 0"
                else:
                    query = "SELECT ai_tags FROM downloaded_media WHERE ai_tags IS NOT NULL AND ai_tags != '' AND ai_tags != '[]'"
                
                await cursor.execute(query)
                
                # Przetwarzanie w Pythonie jest konieczne z powodu złożoności danych JSON
                async for row in cursor:
                    try:
                        # row[0] lub row['ai_tags'] w zależności od kursora
                        tags_json = row[0] if isinstance(row, tuple) else row.get('ai_tags', '[]')
                        tags_data = json.loads(tags_json)
                        
                        # Obsługa starego (lista stringów) i nowego (lista obiektów) formatu
                        if tags_data and isinstance(tags_data[0], dict):
                            for tag_item in tags_data:
                                if 'label' in tag_item:
                                    tag_counts[tag_item['label']] += 1
                        elif tags_data: # Stary format
                            for tag_label in tags_data:
                                tag_counts[tag_label] += 1
                                
                    except (json.JSONDecodeError, IndexError, TypeError):
                        # Pomiń błędnie sformatowane wpisy JSON
                        continue

        # Konwersja defaultdict na listę słowników
        aggregated_list = [{'tag': tag, 'count': count} for tag, count in tag_counts.items()]
        
        # Sortowanie alfabetyczne według tagu dla spójnej kolejności
        aggregated_list.sort(key=lambda x: x['tag'].lower())
        
        logger.info(f"Zakończono agregację. Znaleziono {len(aggregated_list)} unikalnych tagów.")
        return aggregated_list

    except Exception as e:
        logger.error(f"Krytyczny błąd podczas agregacji tagów AI: {e}", exc_info=True)
        return []

async def rename_tag_globally(old_label: str, new_label: str) -> int:
    """
    Wyszukuje wszystkie wystąpienia tagu i zmienia jego nazwę na nową.

    Args:
        old_label (str): Bieżąca nazwa tagu do zmiany.
        new_label (str): Nowa nazwa, która ma zostać przypisana.

    Returns:
        int: Liczba zaktualizowanych rekordów.
    """
    await setup_database()
    logger.info(f"Rozpoczynam globalną zmianę nazwy tagu z '{old_label}' na '{new_label}'...")
    updates_to_perform = []
    
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    cursor.row_factory = aiosqlite.Row

                # Znajdź wszystkie rekordy, które potencjalnie zawierają stary tag
                # Używamy LIKE, co jest wystarczająco szybkie i uniwersalne
                search_term = f'%"label": "{old_label}"%'
                query = "SELECT id, ai_tags FROM downloaded_media WHERE ai_tags LIKE %s"
                if DB_TYPE == "sqlite":
                    query = query.replace('%s', '?')

                await cursor.execute(query, (search_term,))
                records_to_check = await cursor.fetchall()

                # Przetwarzanie w Pythonie
                for record in records_to_check:
                    record_id = record['id']
                    tags_json = record['ai_tags']
                    try:
                        tags_data = json.loads(tags_json)
                        tag_found_and_changed = False
                        for tag_item in tags_data:
                            if isinstance(tag_item, dict) and tag_item.get('label') == old_label:
                                tag_item['label'] = new_label
                                tag_found_and_changed = True
                        
                        if tag_found_and_changed:
                            new_tags_json = json.dumps(tags_data, ensure_ascii=False)
                            updates_to_perform.append((new_tags_json, record_id))
                    except (json.JSONDecodeError, TypeError):
                        continue

                # Zapisz wszystkie zmiany w jednej transakcji
                if updates_to_perform:
                    update_sql = "UPDATE downloaded_media SET ai_tags = %s WHERE id = %s"
                    if DB_TYPE == "sqlite":
                        update_sql = update_sql.replace('%s', '?')
                    
                    await cursor.executemany(update_sql, updates_to_perform)
                    if DB_TYPE == "sqlite":
                        await conn.commit()
                    logger.info(f"Pomyślnie zmieniono nazwę tagu w {len(updates_to_perform)} rekordach.")
                    return len(updates_to_perform)
        
        logger.info("Nie znaleziono żadnych rekordów wymagających aktualizacji.")
        return 0
    except Exception as e:
        logger.error(f"Błąd podczas globalnej zmiany nazwy tagu: {e}", exc_info=True)
        return 0

async def merge_tags_globally(source_labels: List[str], target_label: str) -> int:
    """
    Znajduje wszystkie rekordy z tagami źródłowymi, usuwa je i dodaje tag docelowy.
    """
    await setup_database()
    updates = []
    source_set = set(source_labels)
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite": cursor.row_factory = aiosqlite.Row
                
                # Pobierz wszystkie rekordy, które mają jakiekolwiek tagi
                query = "SELECT id, ai_tags FROM downloaded_media WHERE ai_tags IS NOT NULL AND ai_tags != '' AND ai_tags != '[]'"
                await cursor.execute(query)
                all_records = await cursor.fetchall()

                for record in all_records:
                    try:
                        tags_data = json.loads(record['ai_tags'])
                        if not tags_data or not isinstance(tags_data[0], dict): continue

                        original_labels = {item['label'] for item in tags_data}
                        # Sprawdź, czy rekord zawiera którykolwiek z tagów źródłowych
                        if not original_labels.intersection(source_set): continue

                        # Filtruj, usuwając tagi źródłowe
                        new_tags = [item for item in tags_data if item['label'] not in source_set]
                        new_labels = {item['label'] for item in new_tags}

                        # Dodaj tag docelowy, jeśli go jeszcze nie ma
                        if target_label not in new_labels:
                            # Zachowaj najwyższy score spośród usuwanych tagów
                            max_score = max([item.get('score', 0.0) for item in tags_data if item['label'] in source_set] or [1.0])
                            new_tags.append({'label': target_label, 'score': max_score})
                        
                        updates.append((json.dumps(new_tags, ensure_ascii=False), record['id']))
                    except (json.JSONDecodeError, TypeError): continue
                
                if updates:
                    update_sql = "UPDATE downloaded_media SET ai_tags = %s WHERE id = %s" if DB_TYPE == "mariadb" else "UPDATE downloaded_media SET ai_tags = ? WHERE id = ?"
                    await cursor.executemany(update_sql, updates)
                    if DB_TYPE == "sqlite": await conn.commit()
                    logger.info(f"Scalono tagi w {len(updates)} rekordach.")
                    return len(updates)
        return 0
    except Exception as e:
        logger.error(f"Błąd globalnego scalania tagów: {e}", exc_info=True)
        return 0

async def delete_tag_globally(label_to_delete: str) -> int:
    """
    Wyszukuje i usuwa tag ze wszystkich rekordów w bazie danych.
    """
    await setup_database()
    updates = []
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite": cursor.row_factory = aiosqlite.Row
                
                search_term = f'%"label": "{label_to_delete}"%'
                query = "SELECT id, ai_tags FROM downloaded_media WHERE ai_tags LIKE %s" if DB_TYPE == "mariadb" else "SELECT id, ai_tags FROM downloaded_media WHERE ai_tags LIKE ?"
                await cursor.execute(query, (search_term,))
                records = await cursor.fetchall()

                for record in records:
                    try:
                        tags_data = json.loads(record['ai_tags'])
                        # Stwórz nową listę, pomijając tag do usunięcia
                        new_tags = [item for item in tags_data if isinstance(item, dict) and item.get('label') != label_to_delete]
                        
                        # Jeśli lista się zmieniła, dodaj do aktualizacji
                        if len(new_tags) < len(tags_data):
                            updates.append((json.dumps(new_tags, ensure_ascii=False), record['id']))
                    except (json.JSONDecodeError, TypeError): continue
                
                if updates:
                    update_sql = "UPDATE downloaded_media SET ai_tags = %s WHERE id = %s" if DB_TYPE == "mariadb" else "UPDATE downloaded_media SET ai_tags = ? WHERE id = ?"
                    await cursor.executemany(update_sql, updates)
                    if DB_TYPE == "sqlite": await conn.commit()
                    logger.info(f"Usunięto tag '{label_to_delete}' z {len(updates)} rekordów.")
                    return len(updates)
        return 0
    except Exception as e:
        logger.error(f"Błąd globalnego usuwania tagu: {e}", exc_info=True)
        return 0

async def get_paginated_media_entries(limit: int, offset: int, sort_by: str, sort_order: str, status_filter: str) -> Tuple[int, List[Dict[str, Any]]]:
    """Pobiera stronę z wpisami z bazy, z opcjami sortowania i filtrowania."""
    total_entries = 0
    entries = []
    try:
        async with get_db_connection() as conn:
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                if DB_TYPE == "sqlite":
                    conn.row_factory = aiosqlite.Row

                where_clause = ""
                params = []
                if status_filter:
                    where_clause = "WHERE status LIKE %s" if DB_TYPE == "mariadb" else "WHERE status LIKE ?"
                    params.append(f"%{status_filter}%")
                
                count_query = f"SELECT COUNT(id) as count FROM downloaded_media {where_clause}"
                await cursor.execute(count_query, tuple(params))
                total_row = await cursor.fetchone()
                total_entries = total_row[0] if isinstance(total_row, tuple) else total_row.get('count', 0)

                # Zabezpieczamy przed SQL Injection, sprawdzając, czy kolumny są na białej liście
                allowed_sort_cols = ['id', 'status', 'filename', 'retry_count', 'timestamp']
                if sort_by not in allowed_sort_cols: sort_by = 'id'
                if sort_order.upper() not in ['ASC', 'DESC']: sort_order = 'DESC'

                query = f"SELECT id, url, status, retry_count, final_path, expected_path, metadata_json FROM downloaded_media {where_clause} ORDER BY {sort_by} {sort_order} LIMIT %s OFFSET %s"
                if DB_TYPE == "sqlite":
                    query = query.replace('%s', '?')
                
                params.extend([limit, offset])
                await cursor.execute(query, tuple(params))
                entries = [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Błąd podczas pobierania strony z bazy danych: {e}", exc_info=True)
    return total_entries, entries

async def update_statuses_by_ids(ids: List[int], new_status: str) -> int:
    """Masowo aktualizuje status dla podanej listy ID."""
    if not ids: return 0
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                params = [(new_status, 0, entry_id) for entry_id in ids]
                placeholder = "%s" if DB_TYPE == "mariadb" else "?"
                sql = f"UPDATE downloaded_media SET status = {placeholder}, retry_count = {placeholder} WHERE id = {placeholder}"
                await cursor.executemany(sql, params)
                if DB_TYPE == "sqlite":
                    await conn.commit()
                return cursor.rowcount
    except Exception as e:
        logger.error(f"Błąd podczas masowej aktualizacji statusów: {e}", exc_info=True)
        return 0

async def get_scanner_stats() -> Dict[str, int]:
    """
    Pobiera zagregowane statystyki potrzebne do wyświetlenia w menu skanera.
    """
    stats = {
        'unscanned': 0,
        'scan_errors': 0,
        'needs_completion': 0,
        'path_mismatches': 0,
        'name_mismatches': 0,
    }
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                # Nieskanowane metadane
                await cursor.execute("SELECT COUNT(*) FROM downloaded_media WHERE processing_status IS NULL OR processing_status != 'Sukces'")
                stats['unscanned'] = (await cursor.fetchone())[0]

                # Błędy skanowania
                await cursor.execute("SELECT COUNT(*) FROM downloaded_media WHERE processing_status = 'Błąd'")
                stats['scan_errors'] = (await cursor.fetchone())[0]

                # Wymaga uzupełnienia danych
                await cursor.execute("SELECT COUNT(*) FROM downloaded_media WHERE status = 'downloaded' AND (expected_path IS NULL OR expected_path = '')")
                stats['needs_completion'] = (await cursor.fetchone())[0]

                # Niespójne lokalizacje
                await cursor.execute("SELECT COUNT(*) FROM downloaded_media WHERE final_path IS NOT NULL AND expected_path IS NOT NULL AND final_path != expected_path")
                stats['path_mismatches'] = (await cursor.fetchone())[0]

                # Niespójne nazwy plików
                if DB_TYPE == "mariadb":
                    query = "SELECT COUNT(*) FROM downloaded_media WHERE status = 'downloaded' AND JSON_VALID(metadata_json) = 1 AND JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.FileName')) IS NOT NULL AND filename != JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.FileName'))"
                else: # sqlite
                    query = "SELECT COUNT(*) FROM downloaded_media WHERE status = 'downloaded' AND json_valid(metadata_json) = 1 AND json_extract(metadata_json, '$.FileName') IS NOT NULL AND filename != json_extract(metadata_json, '$.FileName')"
                await cursor.execute(query)
                stats['name_mismatches'] = (await cursor.fetchone())[0]
                
    except Exception as e:
        logger.error(f"Błąd podczas pobierania statystyk skanera: {e}", exc_info=True)
    
    return stats

async def get_entries_by_md5_hash(md5_hash: str) -> List[Dict[str, Any]]:
    """
    Pobiera z bazy danych wszystkie wpisy (pliki) pasujące do podanej sumy kontrolnej MD5.

    Args:
        md5_hash (str): Suma kontrolna MD5 do wyszukania.

    Returns:
        List[Dict[str, Any]]: Lista słowników, gdzie każdy słownik reprezentuje
                              jeden rekord z bazy danych pasujący do hasha.
                              Zwraca pustą listę, jeśli nic nie znaleziono.
    """
    query = """
        SELECT
            id,
            final_path,
            metadata_json
        FROM
            files
        WHERE
            md5_hash = ?
    """
    
    entries = []
    try:
        async with aiosqlite.connect(DATABASE_FILE) as db:
            # Użycie aiosqlite.Row pozwala na dostęp do kolumn po nazwie, jak w słowniku
            db.row_factory = aiosqlite.Row
            async with db.execute(query, (md5_hash,)) as cursor:
                # Użycie pętli jest bardziej pamięciooszczędne niż fetchall() dla dużej liczby wyników
                async for row in cursor:
                    entries.append(dict(row))
    except aiosqlite.Error as e:
        # Użycie loggera jest lepsze niż print, ale na potrzeby przykładu
        print(f"Błąd bazy danych podczas pobierania wpisów dla hasha {md5_hash}: {e}")

    return entries

async def get_duplicate_hashes() -> List[str]:
    """Pobiera listę hashy MD5, które występują w bazie więcej niż raz."""
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                query = "SELECT file_hash FROM downloaded_media WHERE file_hash IS NOT NULL AND file_hash != '' GROUP BY file_hash HAVING COUNT(id) > 1"
                await cursor.execute(query)
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Błąd pobierania zduplikowanych hashy: {e}", exc_info=True)
        return []

async def get_duplicate_hashes_old() -> List[Dict[str, Any]]:
    """
    Pobiera z bazy danych listę hashy, które występują więcej niż jeden raz,
    w formacie oczekiwanym przez moduł duplicate_finder.
    
    Returns:
        Lista słowników, gdzie każdy słownik ma klucz 'file_hash'.
        Np. [{'file_hash': 'hash1'}, {'file_hash': 'hash2'}]
    """
    # Używamy Twojego zapytania, które jest poprawne dla Twojej struktury bazy.
    query = """
        SELECT 
            file_hash 
        FROM 
            downloaded_media 
        WHERE 
            file_hash IS NOT NULL AND file_hash != '' 
        GROUP BY 
            file_hash 
        HAVING 
            COUNT(id) > 1
    """
    
    hashes = []
    try:
        # Zakładam, że masz funkcję get_db_connection(), tak jak w oryginale.
        async with get_db_connection() as conn:
            # Używamy row_factory, aby łatwo konwertować wyniki na słowniki.
            conn.row_factory = aiosqlite.Row
            async with conn.cursor() as cursor:
                await cursor.execute(query)
                rows = await cursor.fetchall()
                
                # KLUCZOWA ZMIANA:
                # Zamiast [row[0] for row in rows], co tworzy listę stringów,
                # robimy [dict(row) for row in rows], co tworzy listę słowników.
                # Zmieniamy też klucz 'md5_hash' na 'file_hash' w duplicate_finder.
                return [dict(row) for row in rows]
                
    except Exception as e:
        logger.error(f"Błąd pobierania zduplikowanych hashy: {e}", exc_info=True)
        return []

async def get_duplicate_hashes_bak_new() -> List[Dict[str, Any]]:
    """
    Pobiera listę hashy, które występują więcej niż raz, i ZAWSZE zwraca
    ją jako listę słowników, np. [{'file_hash': 'hash1'}],
    niezależnie od typu bazy danych.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            # Używamy Twojej logiki do wyboru odpowiedniego kursora. To jest kluczowe!
            cursor_type = aiomysql.DictCursor if DB_TYPE == "mariadb" else None
            async with conn.cursor(cursor_type) as cursor:
                
                # Ustawiamy row_factory tylko dla SQLite, tak jak w Twoim oryginalnym kodzie.
                if DB_TYPE == "sqlite":
                    # UWAGA: W niektórych wersjach aiosqlite może być konieczne ustawienie
                    # conn.row_factory zamiast cursor.row_factory. Poniższy kod jest bezpieczniejszy.
                    conn.row_factory = aiosqlite.Row

                query = "SELECT file_hash FROM downloaded_media WHERE file_hash IS NOT NULL AND file_hash != '' GROUP BY file_hash HAVING COUNT(id) > 1"
                await cursor.execute(query)
                rows = await cursor.fetchall()

                # === OSTATECZNA, INTELIGENTNA KOREKTA ===
                if DB_TYPE == "mariadb":
                    # aiomysql z DictCursor już zwraca listę słowników, np. [{'file_hash': '...'}].
                    # Nic więcej nie trzeba robić.
                    return rows
                else: # Dla SQLite
                    # aiosqlite z RowFactory zwraca listę obiektów aiosqlite.Row.
                    # Musimy je przekonwertować na standardowe słowniki.
                    return [dict(row) for row in rows]

    except Exception as e:
        logger.error(f"Błąd pobierania zduplikowanych hashy: {e}", exc_info=True)
        return []

async def get_validator_stats() -> Dict[str, Any]:
    """
    Pobiera zagregowane statystyki potrzebne do wyświetlenia w menu Walidatora.
    """
    stats = {
        'files_to_hash': 0,
        'duplicate_sets': 0,
        'total_files': 0,
        'ghost_files': None, # Zostanie obliczone poza bazą
    }
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                # Pliki do hashowania
                await cursor.execute("SELECT COUNT(*) FROM downloaded_media WHERE status = 'downloaded' AND file_hash IS NULL")
                stats['files_to_hash'] = (await cursor.fetchone())[0]

                # Zestawy duplikatów
                await cursor.execute("SELECT COUNT(file_hash) FROM (SELECT file_hash FROM downloaded_media WHERE file_hash IS NOT NULL GROUP BY file_hash HAVING COUNT(*) > 1) AS duplicates")
                stats['duplicate_sets'] = (await cursor.fetchone())[0]

                # Wszystkie pobrane pliki
                await cursor.execute("SELECT COUNT(*) FROM downloaded_media WHERE status = 'downloaded'")
                stats['total_files'] = (await cursor.fetchone())[0]

    except Exception as e:
        logger.error(f"Błąd podczas pobierania statystyk walidatora: {e}", exc_info=True)
    
    return stats

async def update_exif_write_status(file_path: str, status: str):
    """
    Aktualizuje status zapisu EXIF (`exif_write_status`) dla pojedynczego pliku
    w bazie danych, używając uniwersalnego połączenia.
    """
    logger.debug(f"Aktualizacja statusu EXIF na '{status}' dla: {Path(file_path).name}")
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                query = "UPDATE downloaded_media SET exif_write_status = %s WHERE final_path = %s"
                params = (status, file_path)
                if DB_TYPE == "sqlite":
                    query = query.replace('%s', '?')
                
                await cursor.execute(query, params)
                await conn.commit()
    except Exception as e:
        logger.error(f"Błąd DB podczas aktualizacji statusu EXIF dla '{file_path}': {e}", exc_info=True)

async def get_recent_takeout_paths() -> List[str]:
    """
    Pobiera do 5 unikalnych, ostatnio używanych ścieżek do folderu Takeout.
    """
    await setup_database()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                key_col = "`key`" if DB_TYPE == "mariadb" else '"key"'
                
                # Pobieramy wszystkie wpisy, posortowane od najnowszego
                query = f"SELECT value FROM script_state WHERE {key_col} LIKE 'recent_takeout_path_%' ORDER BY {key_col} DESC"
                await cursor.execute(query)
                rows = await cursor.fetchall()
                
                # Używamy OrderedDict do odfiltrowania duplikatów, zachowując kolejność
                unique_paths = list(OrderedDict.fromkeys(row[0] for row in rows))
                
                return unique_paths[:5] # Zwracamy 5 najnowszych unikalnych ścieżek
    except Exception as e:
        logger.error(f"Błąd podczas pobierania ostatnich ścieżek Takeout: {e}", exc_info=True)
        return []

async def add_recent_takeout_path(path: str):
    """
    Dodaje nową ścieżkę do historii i czyści stare wpisy, zostawiając
    tylko 5 najnowszych unikalnych ścieżek.
    """
    await setup_database()
    try:
        # Krok 1: Dodaj nowy wpis z unikalnym timestampem
        key = f"recent_takeout_path_{int(time.time())}"
        await set_state(key, path)
        
        # Krok 2: Pobierz wszystkie klucze i wartości
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                key_col = "`key`" if DB_TYPE == "mariadb" else '"key"'
                query = f"SELECT {key_col}, value FROM script_state WHERE {key_col} LIKE 'recent_takeout_path_%'"
                await cursor.execute(query)
                all_entries = await cursor.fetchall()

        # Krok 3: Zidentyfikuj, które klucze są do usunięcia
        # Grupujemy klucze według ich wartości (ścieżki)
        path_to_keys = {}
        for k, v in all_entries:
            if v not in path_to_keys:
                path_to_keys[v] = []
            path_to_keys[v].append(k)
            
        # Sortujemy unikalne ścieżki po ich najnowszym kluczu (timestampie)
        unique_paths_sorted = sorted(path_to_keys.keys(), key=lambda p: max(path_to_keys[p]), reverse=True)
        
        # Wszystkie ścieżki poza top 5 są do usunięcia
        paths_to_keep = unique_paths_sorted[:5]
        keys_to_delete = []
        
        for path, keys in path_to_keys.items():
            if path in paths_to_keep:
                # Jeśli ścieżka jest w top 5, zostawiamy tylko jej najnowszy klucz
                keys_to_delete.extend(sorted(keys)[:-1])
            else:
                # Jeśli ścieżki nie ma w top 5, usuwamy wszystkie jej klucze
                keys_to_delete.extend(keys)

        # Krok 4: Usuń stare i zduplikowane klucze
        if keys_to_delete:
            async with get_db_connection() as conn:
                async with conn.cursor() as cursor:
                    key_col = "`key`" if DB_TYPE == "mariadb" else '"key"'
                    placeholders = ', '.join(['%s'] * len(keys_to_delete))
                    query = f"DELETE FROM app_state WHERE {key_col} IN ({placeholders})"
                    if DB_TYPE == "sqlite":
                        query = query.replace('%s', '?')
                    
                    await cursor.execute(query, tuple(keys_to_delete))
                    await conn.commit()
    except Exception as e:
        logger.error(f"Błąd podczas dodawania/czyszczenia ścieżek Takeout: {e}", exc_info=True)

async def get_image_paths_for_analysis(extensions: tuple, scan_target: str) -> List[Dict[str, Any]]:
    """
    Pobiera z bazy listę ścieżek i ID obrazów do analizy, z obsługą
    filtrowania 'all' dla wszystkich źródeł.
    """
    # === POCZĄTEK POPRAWKI: Logika obsługi 'all' ===
    source_map = {
        'downloader': 'google_photos',
        'local_importer': 'local_import'
    }
    
    base_query = "SELECT id, final_path FROM downloaded_media WHERE final_path IS NOT NULL AND final_path != ''"
    params = ()

    if scan_target in source_map:
        db_source_filter = source_map[scan_target]
        base_query += " AND source = %s"
        params = (db_source_filter,)
    elif scan_target != 'all':
        logger.warning(f"Nieznany cel skanowania w get_image_paths_for_analysis: {scan_target}")
        return []
    # Jeśli scan_target == 'all', nie dodajemy warunku `WHERE source`, więc pobierze wszystko.
    
    query = base_query
    if DB_TYPE == "sqlite":
        query = query.replace('%s', '?')
    # === KONIEC POPRAWKI ===

    results = []
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()

        for row in rows:
            path_str = row[1]
            if not path_str: continue
            
            path_obj = Path(path_str)
            if path_obj.suffix.lower() not in extensions: continue
            if not await asyncio.to_thread(path_obj.exists): continue
                
            results.append({'id': row[0], 'path': path_obj})
            
    except Exception as e:
        logger.error(f"Błąd podczas pobierania ścieżek obrazów: {e}", exc_info=True)
    
    logger.info(f"Znaleziono {len(results)} pasujących obrazów dla celu '{scan_target}'.")
    return results

async def update_analysis_results(media_id: int, results: Dict):
    """
    Zapisuje wyniki analizy obrazu dla danego pliku do bazy,
    używając poprawnej logiki z kursorem.
    """
    results_json = json.dumps(results)
    query = "UPDATE downloaded_media SET analysis_results = %s WHERE id = %s"
    params = (results_json, media_id)
    if DB_TYPE == "sqlite":
        query = query.replace('%s', '?')

    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)

            if DB_TYPE == "sqlite":
                await conn.commit()
    except Exception as e:
        logger.error(f"Błąd podczas zapisu wyników analizy dla ID {media_id}: {e}", exc_info=True)

async def get_analysis_results_from_db(scan_target: str) -> Dict[str, List[Path]]:
    """
    Pobiera i agreguje zapisane wyniki analizy z bazy danych, używając
    poprawnego mapowania nazwy źródła.
    """

    source_map = {
        'downloader': 'google_photos',
        'local_importer': 'local_import'
    }
    db_source_filter = source_map.get(scan_target)

    if not db_source_filter:
        logger.warning(f"Nieznany cel skanowania w get_analysis_results_from_db: {scan_target}")
        return {"blurry": [], "dark": [], "small": [], "corrupted": []}

    query = "SELECT final_path, analysis_results FROM downloaded_media WHERE source = %s AND analysis_results IS NOT NULL"
    params = (db_source_filter,) # Używamy poprawnej wartości
    if DB_TYPE == "sqlite":
        query = query.replace('%s', '?')

    results = {"blurry": [], "dark": [], "small": [], "corrupted": []}
    
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()

        for row in rows:
            path, analysis_json = row
            if not path or not analysis_json: continue
            try:
                analysis = json.loads(analysis_json)
                if analysis.get('is_blurry'): results["blurry"].append(Path(path))
                if analysis.get('is_dark'): results["dark"].append(Path(path))
                if analysis.get('is_small'): results["small"].append(Path(path))
                if analysis.get('is_corrupted'): results["corrupted"].append(Path(path))
            except (json.JSONDecodeError, TypeError):
                continue
    except Exception as e:
        logger.error(f"Błąd podczas pobierania wyników analizy: {e}", exc_info=True)

    return results
