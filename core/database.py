# plik: core/database.py
# Wersja 11.2 - Dodano funkcje dla modułu integrity_validator_logic

# -*- coding: utf-8 -*-

# plik: core/database.py
# Wersja 11.0 - Finalna, uporządkowana wersja z pełnym CRUD dla osób.

import sys
import json
import logging
import pickle
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import asyncio

import aiosqlite
import numpy as np

from .config import DATABASE_FILE, MAX_RETRIES

logger = logging.getLogger(__name__)
_db_initialized = False

# Wklej tę nową, kompletną wersję funkcji do pliku: core/database.py

async def setup_database():
    """
    Inicjalizuje i weryfikuje strukturę bazy danych. Jeśli tabele lub kolumny
    nie istnieją, tworzy je. Ta funkcja jest teraz w pełni idempotentna.
    """
    global _db_initialized
    if _db_initialized:
        return

    logger.info("Rozpoczynam jednorazową inicjalizację i weryfikację schematu bazy danych...")
    try:
        db_path = Path(DATABASE_FILE)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiosqlite.connect(db_path) as conn:
            # --- Tabela Główna: downloaded_media ---
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS downloaded_media (
                    id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL UNIQUE
                )""")

            # --- DYNAMICZNA MIGRACJA KOLUMN ---
            # Najpierw pobieramy listę istniejących kolumn
            cursor = await conn.execute("PRAGMA table_info(downloaded_media);")
            existing_columns = {row[1] for row in await cursor.fetchall()}

            # Definiujemy wszystkie kolumny, które POWINNY istnieć
            all_columns = {
                "filename": "TEXT", "final_path": "TEXT", "expected_path": "TEXT",
                "metadata_json": "TEXT", "timestamp": "DATETIME DEFAULT CURRENT_TIMESTAMP",
                "status": "TEXT", "retry_count": "INTEGER DEFAULT 0",
                "processing_status": "TEXT", "exif_write_status": "TEXT",
                "file_hash": "TEXT", "perceptual_hash": "TEXT", "ai_tags": "TEXT",
                "source": "TEXT DEFAULT 'google_photos'", "google_photos_url": "TEXT"
            }

            # W pętli dodajemy tylko te kolumny, których brakuje
            for col_name, col_type in all_columns.items():
                if col_name not in existing_columns:
                    await conn.execute(f"ALTER TABLE downloaded_media ADD COLUMN {col_name} {col_type}")
                    logger.info(f"Dodano brakującą kolumnę '{col_name}' do tabeli 'downloaded_media'.")

            # --- Pozostałe Tabele (bez zmian) ---
            await conn.execute("CREATE TABLE IF NOT EXISTS script_state (key TEXT PRIMARY KEY, value TEXT)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS people (
                    person_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                    model_name TEXT NOT NULL, master_embedding BLOB NOT NULL,
                    source_media_id INTEGER, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (source_media_id) REFERENCES downloaded_media (id) ON DELETE SET NULL,
                    UNIQUE(name, model_name)
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS faces (
                    face_id INTEGER PRIMARY KEY AUTOINCREMENT, media_id INTEGER NOT NULL, person_id INTEGER,
                    embedding BLOB NOT NULL, facial_area TEXT NOT NULL, model_name TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (media_id) REFERENCES downloaded_media (id) ON DELETE CASCADE,
                    FOREIGN KEY (person_id) REFERENCES people (person_id) ON DELETE SET NULL
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS similar_image_pairs (
                    pair_id INTEGER PRIMARY KEY, image_id_a INTEGER, image_id_b INTEGER,
                    distance INTEGER, status TEXT DEFAULT 'pending',
                    FOREIGN KEY (image_id_a) REFERENCES downloaded_media (id) ON DELETE CASCADE,
                    FOREIGN KEY (image_id_b) REFERENCES downloaded_media (id) ON DELETE CASCADE
                )""")

            # Indeksy i czyszczenie
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_faces_media_id ON faces (media_id);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_faces_person_id ON faces (person_id);")
            await conn.execute("DROP TABLE IF EXISTS recognized_faces;") # Usuwamy starą tabelę, jeśli istnieje
            await conn.commit()
            
        _db_initialized = True
        logger.info("Inicjalizacja i weryfikacja schematu bazy danych zakończona.")

    except aiosqlite.Error as e:
        logger.critical(f"Nie można utworzyć lub zaktualizować bazy danych: {e}", exc_info=True)
        # W środowisku testowym nie chcemy zamykać programu
        if not str(DATABASE_FILE).startswith("file:"):
             sys.exit(1)

async def add_person(name: str, model_name: str, embedding: np.ndarray, source_media_id: Optional[int] = None) -> Optional[int]:
    """Dodaje nową znaną osobę do bazy danych."""
    await setup_database()
    try:
        embedding_blob = pickle.dumps(embedding)
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            cursor = await conn.execute(
                "INSERT INTO people (name, model_name, master_embedding, source_media_id) VALUES (?, ?, ?, ?)",
                (name, model_name, embedding_blob, source_media_id)
            )
            await conn.commit()
            return cursor.lastrowid
    except aiosqlite.IntegrityError:
        logger.warning(f"Osoba '{name}' dla modelu '{model_name}' już istnieje w bazie.")
        return None
    except Exception as e:
        logger.error(f"Nie udało się dodać osoby '{name}': {e}", exc_info=True)
        return None

async def get_all_people(model_name: str) -> List[Dict[str, Any]]:
    """Pobiera listę wszystkich znanych osób dla konkretnego modelu AI."""
    await setup_database()
    people = []
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = "SELECT person_id, name, master_embedding FROM people WHERE model_name = ? ORDER BY name"
            async with conn.execute(query, (model_name,)) as cursor:
                async for row in cursor:
                    person_data = dict(row)
                    person_data['master_embedding'] = pickle.loads(person_data['master_embedding'])
                    people.append(person_data)
    except Exception as e:
        logger.error(f"Nie udało się pobrać listy osób: {e}", exc_info=True)
    return people

async def update_person_name(person_id: int, new_name: str):
    """Aktualizuje nazwę dla istniejącej osoby."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.execute("UPDATE people SET name = ? WHERE person_id = ?", (new_name, person_id))
            await conn.commit()
    except Exception as e:
        logger.error(f"Nie udało się zaktualizować osoby o ID {person_id}: {e}", exc_info=True)

async def delete_person(person_id: int):
    """Usuwa znaną osobę z bazy. Jej twarze staną się ponownie 'nieznane'."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.execute("DELETE FROM people WHERE person_id = ?", (person_id,))
            await conn.commit()
    except Exception as e:
        logger.error(f"Nie udało się usunąć osoby o ID {person_id}: {e}", exc_info=True)

# --- FUNKCJE ZARZĄDZANIA TWARZAMI (FACES) ---
# ... (bez zmian) ...
async def add_face(media_id: int, embedding: np.ndarray, facial_area: dict, model_name: str) -> Optional[int]:
    """Dodaje nowo wykrytą (nieznaną) twarz do bazy."""
    await setup_database()
    try:
        embedding_blob = pickle.dumps(embedding)
        facial_area_json = json.dumps(facial_area)
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            cursor = await conn.execute(
                "INSERT INTO faces (media_id, embedding, facial_area, model_name) VALUES (?, ?, ?, ?)",
                (media_id, embedding_blob, facial_area_json, model_name)
            )
            await conn.commit()
            return cursor.lastrowid
    except Exception as e:
        logger.error(f"Nie udało się dodać twarzy dla media_id {media_id}: {e}", exc_info=True)
        return None

async def get_unknown_faces(model_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Pobiera listę nieznanych twarzy dla danego modelu."""
    await setup_database()
    unknown_faces = []
    query = "SELECT face_id, media_id, embedding FROM faces WHERE person_id IS NULL AND model_name = ?"
    if limit:
        query += f" LIMIT {limit}"
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(query, (model_name,)) as cursor:
                async for row in cursor:
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
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.execute("UPDATE faces SET person_id = ? WHERE face_id = ?", (person_id, face_id))
            await conn.commit()
    except Exception as e:
        logger.error(f"Nie udało się otagować twarzy face_id {face_id}: {e}", exc_info=True)

async def get_media_ids_with_indexed_faces(model_name: str) -> List[int]:
    """Zwraca listę ID mediów, które mają już jakieś twarze w bazie dla danego modelu."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            cursor = await conn.execute("SELECT DISTINCT media_id FROM faces WHERE model_name = ?", (model_name,))
            return [row[0] for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Nie udało się pobrać ID zaindeksowanych mediów: {e}", exc_info=True)
        return []

# --- FUNKCJE DO PRZEGLĄDANIA WYNIKÓW ---
# ... (bez zmian) ...
async def get_all_tagged_people() -> List[Dict[str, Any]]:
    """Pobiera listę osób, które mają przypisane co najmniej jedno zdjęcie."""
    await setup_database()
    query = """
        SELECT p.person_id, p.name, COUNT(f.face_id) as photo_count
        FROM people p JOIN faces f ON p.person_id = f.person_id
        GROUP BY p.person_id, p.name ORDER BY p.name;
    """
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Nie udało się pobrać listy otagowanych osób: {e}", exc_info=True)
        return []

async def get_media_for_person(person_id: int) -> List[Dict[str, Any]]:
    """Pobiera wszystkie media, na których otagowano daną osobę."""
    await setup_database()
    query = """
        SELECT DISTINCT m.id, m.final_path FROM downloaded_media m
        JOIN faces f ON m.id = f.media_id
        WHERE f.person_id = ? ORDER BY m.final_path;
    """
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(query, (person_id,))
            return [dict(row) for row in await cursor.fetchall()]
    except Exception as e:
        logger.error(f"Nie udało się pobrać mediów dla osoby o ID {person_id}: {e}", exc_info=True)
        return []
        
# --- POZOSTAŁE FUNKCJE POMOCNICZE ---
# ... (bez zmian) ...
async def get_db_stats() -> defaultdict[str, int]:
    """Asynchronicznie pobiera i agreguje statystyki z bazy danych."""
    await setup_database()
    stats = defaultdict(int)
    db_path = Path(DATABASE_FILE)

    if not await asyncio.to_thread(db_path.exists):
        return stats

    try:
        async with aiosqlite.connect(db_path) as conn:
            conn.row_factory = aiosqlite.Row
            queries = {
                "status": "SELECT status, COUNT(id) as count FROM downloaded_media GROUP BY status",
                "processing_status": "SELECT processing_status, COUNT(id) as count FROM downloaded_media GROUP BY processing_status",
                "exif_write_status": "SELECT exif_write_status, COUNT(id) as count FROM downloaded_media GROUP BY exif_write_status",
            }
            for key, query in queries.items():
                async with conn.execute(query) as cursor:
                    async for row in cursor:
                        if row['count'] > 0 and row[key]:
                            prefix = "scan_" if key == "processing_status" else "exif_" if key == "exif_write_status" else ""
                            stats[f"{prefix}{str(row[key]).lower()}"] = row['count']
            
            cursor = await conn.execute("SELECT COUNT(id) FROM downloaded_media")
            total = await cursor.fetchone()
            stats['total'] = total[0] if total else 0
            return stats
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania statystyk z bazy danych: {e}", exc_info=True)
        return defaultdict(int)


async def set_state(key: str, value: str):
    """Zapisuje lub aktualizuje parę klucz-wartość w tabeli `script_state`."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.execute("INSERT OR REPLACE INTO script_state (key, value) VALUES (?, ?)", (key, value))
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Nie udało się zapisać stanu dla klucza '{key}': {e}", exc_info=True)


async def get_state(key: str) -> str | None:
    """Odczytuje wartość stanu dla podanego klucza z tabeli `script_state`."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            cursor = await conn.execute("SELECT value FROM script_state WHERE key = ?", (key,))
            result = await cursor.fetchone()
            return result[0] if result else None
    except aiosqlite.Error as e:
        logger.error(f"Nie udało się odczytać stanu dla klucza '{key}': {e}", exc_info=True)
        return None


async def add_google_photo_entry(
    url: str, filename: str, final_path: Path, metadata: dict,
    status: str, retry_count: int, expected_path: str | None,
    processing_status: str | None = None
):
    """Zapisuje lub aktualizuje wpis dla medium pobranego z Google Photos."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            final_path_str = str(final_path) if final_path else None
            metadata_str = json.dumps(metadata, ensure_ascii=False, indent=2)
            await conn.execute(
                """
                INSERT INTO downloaded_media (url, filename, final_path, metadata_json, status, retry_count, expected_path, source, processing_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'google_photos', ?)
                ON CONFLICT(url) DO UPDATE SET
                    filename=excluded.filename, final_path=excluded.final_path, metadata_json=excluded.metadata_json,
                    status=excluded.status, retry_count=excluded.retry_count, expected_path=excluded.expected_path,
                    source=excluded.source,
                    processing_status=CASE WHEN excluded.processing_status IS NOT NULL THEN excluded.processing_status ELSE downloaded_media.processing_status END,
                    timestamp=CURRENT_TIMESTAMP
                """,
                (url, filename, final_path_str, metadata_str, status, retry_count, expected_path, processing_status)
            )
            await conn.commit()
    except aiosqlite.Error as e:
        logger.critical(f"Nie udało się zapisać danych dla URL {url}: {e}", exc_info=True)


async def add_local_file_entry(file_path: Path, metadata: dict) -> bool:
    """Dodaje do bazy danych wpis dla pliku zaimportowanego z lokalnego dysku."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            metadata_str = json.dumps(metadata, ensure_ascii=False, indent=2)
            deterministic_url = file_path.resolve().as_uri()
            cursor = await conn.execute(
                """
                INSERT OR IGNORE INTO downloaded_media (url, filename, final_path, expected_path, metadata_json, status, processing_status, exif_write_status, source)
                VALUES (?, ?, ?, ?, ?, 'downloaded', 'Sukces', 'n/a', 'local_import')
                """,
                (deterministic_url, file_path.name, str(file_path.resolve()), str(file_path.resolve()), metadata_str)
            )
            await conn.commit()
            return cursor.rowcount > 0
    except aiosqlite.Error as e:
        logger.error(f"Nie udało się dodać pliku lokalnego '{file_path.name}' do bazy: {e}", exc_info=True)
        return False


async def get_records_for_exif_processing(process_mode: str) -> List[aiosqlite.Row] | None:
    """Pobiera listę rekordów do przetworzenia przez Exif Writer."""
    await setup_database()
    logger.info(f"Pobieram rekordy z bazy dla Exif Writer w trybie: '{process_mode}'.")
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
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
            cursor = await conn.execute(query)
            records = await cursor.fetchall()
            logger.info(f"Pobrano {len(records)} rekordów dla Exif Writer.")
            return records
    except aiosqlite.Error as e:
        logger.critical("Nie można pobrać danych z bazy dla Exif Writer.", exc_info=True)
        return None

async def _get_single_column_for_url(column_name: str, url: str, default_value: Any = None) -> Any:
    """Prywatna funkcja pomocnicza do pobierania wartości z pojedynczej kolumny."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            query = f"SELECT {column_name} FROM downloaded_media WHERE url = ?"
            cursor = await conn.execute(query, (url,))
            result = await cursor.fetchone()
            return result[0] if result else default_value
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania kolumny '{column_name}' dla URL {url}: {e}", exc_info=True)
        return default_value

async def get_url_status_from_db(url: str) -> str | None:
    """Pobiera status pobierania dla danego URL-a."""
    return await _get_single_column_for_url(column_name='status', url=url)

async def get_retry_count_for_url(url: str) -> int:
    """Pobiera liczbę prób pobrania dla danego URL-a."""
    return await _get_single_column_for_url(column_name='retry_count', url=url, default_value=0)

async def get_failed_urls_from_db() -> list[str]:
    """Pobiera listę URL-i, które zakończyły się błędem i mogą być ponowione."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            query = "SELECT url FROM downloaded_media WHERE status = 'failed' AND retry_count < ?"
            cursor = await conn.execute(query, (MAX_RETRIES,))
            return [row[0] for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania listy błędnych URL-i: {e}", exc_info=True)
        return []

async def get_all_urls_from_db() -> List[str]:
    """Pobiera z bazy danych listę WSZYSTKICH unikalnych adresów URL."""
    await setup_database()
    urls = []
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            cursor = await conn.execute("SELECT url FROM downloaded_media WHERE url IS NOT NULL")
            urls = [row[0] for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania wszystkich URL-i: {e}", exc_info=True)
    return urls

async def get_urls_to_fix() -> List[str]:
    """Pobiera listę URL-i, które wymagają ponownego skanowania metadanych."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            query = """
                SELECT url FROM downloaded_media
                WHERE status IN ('downloaded', 'skipped', 'scanned')
                AND json_valid(metadata_json) = 1
                AND json_extract(metadata_json, '$.FileName') IS NULL
            """
            cursor = await conn.execute(query)
            return [row[0] for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania URL-i do naprawy: {e}", exc_info=True)
        return []

async def get_urls_for_online_scan(scan_type: str = 'new_only') -> List[str]:
    """Pobiera listę URL-i do przetworzenia przez skaner online."""
    await setup_database()
    base_query = "SELECT url FROM downloaded_media WHERE (source IS NULL OR source != 'local_import')"
    conditions = {
        'new_only': "AND (processing_status IS NULL OR processing_status != 'Sukces')",
        'retry_errors': "AND processing_status = 'Błąd'",
        'force_refresh': ""
    }
    query = base_query + conditions.get(scan_type, "")
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            cursor = await conn.execute(query)
            return [row[0] for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania URL-i do skanowania: {e}", exc_info=True)
        return []

async def update_scanned_entries_batch(batch_data: list[dict]):
    """Asynchronicznie zapisuje partię wyników ze skanera online do bazy danych."""
    await setup_database()
    if not batch_data: return
    params = [(item['url'], item['metadata_json'], item['processing_status'], item['expected_path']) for item in batch_data]
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.executemany(
                """
                INSERT INTO downloaded_media (url, metadata_json, processing_status, expected_path, status)
                VALUES (?, ?, ?, ?, 'scanned')
                ON CONFLICT(url) DO UPDATE SET
                    metadata_json = excluded.metadata_json,
                    processing_status = excluded.processing_status,
                    expected_path = excluded.expected_path
                """,
                params
            )
            await conn.commit()
    except aiosqlite.Error as e:
        logger.critical(f"KRYTYCZNY BŁĄD podczas zapisu wsadowego wyników skanowania: {e}", exc_info=True)

async def reset_media_for_reprocessing(ids_to_update: list[int]) -> int:
    """Resetuje statusy przetwarzania dla podanej listy ID mediów."""
    await setup_database()
    if not ids_to_update: return 0
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            update_params = [('pending', 'pending', entry_id) for entry_id in ids_to_update]
            cursor = await conn.executemany("UPDATE downloaded_media SET processing_status = ?, exif_write_status = ? WHERE id = ?", update_params)
            await conn.commit()
            return cursor.rowcount
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas resetowania statusów w bazie danych: {e}", exc_info=True)
        return 0

# ##############################################################################
# ===           SEKCJA DEDYKOWANA DLA MODUŁU EXIF WRITER                     ===
# ##############################################################################

async def get_exif_writer_stats() -> Dict[str, Any]:
    """Pobiera i oblicza statystyki specyficzne dla narzędzia Exif Writer."""
    await setup_database()
    logger.info("Pobieram statystyki zapisu EXIF z bazy danych...")
    stats = {'total_ready': 0, 'success': 0, 'partial': 0, 'error': 0, 'not_written': 0}
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='downloaded_media'")
            if not await cursor.fetchone():
                logger.warning("Brak tabeli 'downloaded_media' w bazie danych.")
                return stats

            cursor = await conn.execute("PRAGMA table_info(downloaded_media)")
            columns = {info[1] for info in await cursor.fetchall()}

            cursor = await conn.execute("SELECT COUNT(*) FROM downloaded_media WHERE processing_status = 'Sukces' AND metadata_json IS NOT NULL")
            stats['total_ready'] = (await cursor.fetchone() or [0])[0]

            if 'exif_write_status' in columns:
                cursor = await conn.execute("SELECT exif_write_status, COUNT(*) FROM downloaded_media WHERE exif_write_status IS NOT NULL GROUP BY exif_write_status")
                status_counts = {row[0]: row[1] for row in await cursor.fetchall()}
                stats['success'] = status_counts.get('Success', 0)
                stats['partial'] = status_counts.get('Partial', 0)
                stats['error'] = status_counts.get('Error', 0)

            stats['not_written'] = stats['total_ready'] - (stats['success'] + stats['partial'] + stats['error'])
            return stats
    except aiosqlite.Error as e:
        logger.error("BŁĄD podczas odczytu statystyk zapisu EXIF.", exc_info=True)
        return stats

async def get_records_for_exif_processing(process_mode: str) -> List[aiosqlite.Row] | None:
    """Pobiera listę rekordów do przetworzenia przez Exif Writer."""
    await setup_database()
    logger.info(f"Pobieram rekordy z bazy dla Exif Writer w trybie: '{process_mode}'.")
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
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
            cursor = await conn.execute(query)
            records = await cursor.fetchall()
            logger.info(f"Pobrano {len(records)} rekordów dla Exif Writer.")
            return records
    except aiosqlite.Error as e:
        logger.critical("Nie można pobrać danych z bazy dla Exif Writer.", exc_info=True)
        return None

# ##############################################################################
# ===        NOWA SEKCJA DLA MODUŁU ADVANCED_SCANNER_LOGIC (FAZA 1)         ===
# ##############################################################################

async def get_records_for_path_correction() -> List[Dict[str, Any]]:
    """Pobiera rekordy, których final_path i expected_path się nie zgadzają."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = """
                SELECT id, final_path, expected_path FROM downloaded_media
                WHERE final_path IS NOT NULL AND final_path != ''
                AND expected_path IS NOT NULL AND expected_path != ''
                AND final_path != expected_path
            """
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania niespójnych ścieżek: {e}", exc_info=True)
        return []

async def update_final_path(entry_id: int, new_final_path: str):
    """Aktualizuje final_path dla pojedynczego wpisu w bazie danych."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.execute("UPDATE downloaded_media SET final_path = ? WHERE id = ?", (new_final_path, entry_id))
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Błąd aktualizacji final_path dla ID {entry_id}: {e}", exc_info=True)


async def get_records_for_filename_fix() -> List[Dict[str, Any]]:
    """Pobiera rekordy do weryfikacji i potencjalnej naprawy nazwy pliku."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = """
                SELECT id, filename, final_path, expected_path, metadata_json
                FROM downloaded_media
                WHERE status = 'downloaded' AND json_valid(metadata_json) = 1
                AND json_extract(metadata_json, '$.FileName') IS NOT NULL
                AND filename != json_extract(metadata_json, '$.FileName')
            """
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania rekordów do naprawy nazw: {e}", exc_info=True)
        return []

async def update_entry_after_rename(entry_id: int, new_filename: str, new_final_path: str, new_expected_path: str, new_metadata_json: str):
    """Kompleksowo aktualizuje wpis w bazie po zmianie nazwy pliku."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.execute(
                """
                UPDATE downloaded_media SET
                    filename = ?, final_path = ?, expected_path = ?, metadata_json = ?
                WHERE id = ?
                """,
                (new_filename, new_final_path, new_expected_path, new_metadata_json, entry_id)
            )
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Błąd aktualizacji wpisu po zmianie nazwy dla ID {entry_id}: {e}", exc_info=True)

async def get_records_for_metadata_completion() -> List[Dict[str, Any]]:
    """Pobiera rekordy, które wymagają uzupełnienia metadanych i expected_path."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = """
                SELECT id, final_path, metadata_json FROM downloaded_media
                WHERE status = 'downloaded' AND json_valid(metadata_json) = 1
                AND (expected_path IS NULL OR expected_path = '')
            """
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania rekordów do uzupełnienia metadanych: {e}", exc_info=True)
        return []

async def update_entry_with_completed_metadata(entry_id: int, new_metadata_json: str, new_expected_path: str):
    """Aktualizuje wpis w bazie o uzupełnione metadane i obliczoną ścieżkę expected_path."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.execute(
                "UPDATE downloaded_media SET metadata_json = ?, expected_path = ? WHERE id = ?",
                (new_metadata_json, new_expected_path, entry_id)
            )
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Błąd aktualizacji wpisu z uzupełnionymi metadanymi dla ID {entry_id}: {e}", exc_info=True)

async def get_records_for_exif_writing() -> List[Dict[str, Any]]:
    """Pobiera rekordy, których metadane należy zapisać do plików za pomocą Exiftool."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = """
                SELECT final_path, metadata_json FROM downloaded_media
                WHERE status = 'downloaded'
                AND metadata_json IS NOT NULL AND metadata_json != '{}'
            """
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania rekordów do zapisu EXIF: {e}", exc_info=True)
        return []

# ##############################################################################
# ===        NOWA SEKCJA DLA MODUŁU INTEGRITY_VALIDATOR_LOGIC (FAZA 1)      ===
# ##############################################################################

async def get_downloaded_files_for_validation() -> List[Dict[str, Any]]:
    """Pobiera z bazy listę plików o statusie 'downloaded' do weryfikacji istnienia."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = "SELECT id, final_path, filename FROM downloaded_media WHERE status = 'downloaded' AND final_path IS NOT NULL AND final_path != ''"
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania plików do walidacji istnienia: {e}", exc_info=True)
        return []

async def get_records_to_hash() -> List[Dict[str, Any]]:
    """Pobiera z bazy listę plików, które nie mają jeszcze obliczonego hasha MD5."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = "SELECT id, final_path FROM downloaded_media WHERE status = 'downloaded' AND (file_hash IS NULL OR file_hash = '')"
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania plików do hashowania: {e}", exc_info=True)
        return []

async def update_hashes_batch(updates: List[Tuple[str, int]]):
    """Zapisuje partię obliczonych hashy MD5 do bazy danych."""
    await setup_database()
    if not updates: return
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.executemany("UPDATE downloaded_media SET file_hash = ? WHERE id = ?", updates)
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Błąd wsadowej aktualizacji hashy: {e}", exc_info=True)

async def get_all_final_paths() -> List[Dict[str, Any]]:
    """Pobiera wszystkie istniejące `final_path` z bazy danych."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = "SELECT id, final_path FROM downloaded_media WHERE final_path IS NOT NULL AND final_path != ''"
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania wszystkich final_path: {e}", exc_info=True)
        return []

async def delete_entries_by_ids(ids: List[int]):
    """Usuwa wpisy z bazy danych na podstawie podanej listy ID."""
    await setup_database()
    if not ids: return
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            placeholders = ','.join(['?'] * len(ids))
            await conn.execute(f"DELETE FROM downloaded_media WHERE id IN ({placeholders})", ids)
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas usuwania wpisów: {e}", exc_info=True)

async def get_metadata_for_consistency_check() -> List[Dict[str, Any]]:
    """Pobiera dane niezbędne do sprawdzenia spójności metadanych (ścieżka vs data)."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = """
                SELECT id, final_path, json_extract(metadata_json, '$.DateTime') as dt_from_json
                FROM downloaded_media
                WHERE status = 'downloaded' AND dt_from_json IS NOT NULL AND final_path IS NOT NULL AND final_path != ''
            """
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania danych do sprawdzenia spójności: {e}", exc_info=True)
        return []

async def get_duplicate_hashes() -> List[str]:
    """Pobiera listę hashy MD5, które występują w bazie więcej niż raz."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            query = "SELECT file_hash FROM downloaded_media WHERE file_hash IS NOT NULL AND file_hash != '' GROUP BY file_hash HAVING COUNT(id) > 1"
            cursor = await conn.execute(query)
            return [row[0] for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania zduplikowanych hashy: {e}", exc_info=True)
        return []

async def get_entries_by_hash(file_hash: str) -> List[Dict[str, Any]]:
    """Pobiera wszystkie wpisy z bazy danych pasujące do danego hasha MD5."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = "SELECT id, final_path, metadata_json FROM downloaded_media WHERE file_hash = ?"
            cursor = await conn.execute(query, (file_hash,))
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania wpisów po hashu '{file_hash}': {e}", exc_info=True)
        return []

# ##############################################################################
# ===           SEKCJA DEDYKOWANA DLA MODUŁÓW ANALITYCZNYCH                  ===
# ##############################################################################

async def get_aggregated_analytics_data() -> dict:
    """
    Pobiera i Agreguje dane analityczne bezpośrednio w bazie danych,
    zwracając gotowy słownik ze statystykami.
    """
    await setup_database()
    logger.info("Rozpoczynam agregację danych analitycznych w bazie danych...")
    
    # Definiujemy statusy, które kwalifikują plik do analizy
    valid_statuses = ('downloaded', 'skipped', 'archived', 'scanned')
    placeholders = ','.join('?' * len(valid_statuses))
    
    # Jedno, potężne zapytanie SQL, które wykonuje wszystkie obliczenia
    query = f"""
        SELECT
            COUNT(id) as total_files,
            SUM(CAST(json_extract(metadata_json, '$.size') AS INTEGER)) as total_size_bytes,
            MIN(json_extract(metadata_json, '$.DateTime')) as oldest_date,
            MAX(json_extract(metadata_json, '$.DateTime')) as newest_date,
            STRFTIME('%Y', json_extract(metadata_json, '$.DateTime')) as year,
            COUNT(id) as year_count,
            SUM(CAST(json_extract(metadata_json, '$.size') AS INTEGER)) as year_size
        FROM downloaded_media
        WHERE
            status IN ({placeholders})
            AND json_valid(metadata_json) = 1
            AND json_extract(metadata_json, '$.DateTime') IS NOT NULL
        GROUP BY
            year
    """
    
    # Dodatkowe zapytania, których nie da się łatwo połączyć
    camera_query = "SELECT json_extract(metadata_json, '$.Camera') as camera, COUNT(id) as count FROM downloaded_media WHERE camera IS NOT NULL GROUP BY camera ORDER BY count DESC LIMIT 15"
    
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            
            # Wykonaj główne zapytanie
            main_cursor = await conn.execute(query, valid_statuses)
            rows = await main_cursor.fetchall()
            
            # Wykonaj zapytanie o aparaty
            camera_cursor = await conn.execute(camera_query)
            camera_rows = await camera_cursor.fetchall()

    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas agregacji danych analitycznych: {e}", exc_info=True)
        return {}

    # Przetwarzamy wyniki w Pythonie do finalnej, czystej formy
    if not rows:
        return {}

    overall_stats = {
        "total_files": sum(r['year_count'] for r in rows),
        "total_size_bytes": sum(r['year_size'] for r in rows),
        "oldest_date": min(r['oldest_date'] for r in rows),
        "newest_date": max(r['newest_date'] for r in rows),
    }

    yearly_data = {
        r['year']: {'count': r['year_count'], 'size': r['year_size']}
        for r in rows if r['year']
    }
    
    camera_data = [(r['camera'], r['count']) for r in camera_rows]

    # Zwracamy jeden, kompletny słownik z gotowymi danymi
    return {
        "overall": overall_stats,
        "yearly": yearly_data,
        "cameras": camera_data
    }

async def get_all_db_records_for_takeout_import() -> List[Dict[str, Any]]:
    """Pobiera podstawowe dane (id, filename, metadata_json) dla wszystkich rekordów."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute("SELECT id, filename, metadata_json FROM downloaded_media")
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania rekordów dla importu z Takeout: {e}", exc_info=True)
        return []

async def update_takeout_metadata_batch(updates: List[Tuple[str, Optional[str], int]]):
    """Zapisuje partię zaktualizowanych metadanych i URL z Takeout do bazy."""
    await setup_database()
    if not updates: return
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.executemany(
                "UPDATE downloaded_media SET metadata_json = ?, google_photos_url = ? WHERE id = ?",
                updates
            )
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Błąd wsadowej aktualizacji metadanych z Takeout: {e}", exc_info=True)

async def get_all_filenames_from_db() -> set:
    """Pobiera zbiór wszystkich nazw plików (filename) z bazy danych."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            cursor = await conn.execute("SELECT filename FROM downloaded_media WHERE filename IS NOT NULL")
            return {row[0] for row in await cursor.fetchall()}
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania nazw plików z bazy: {e}", exc_info=True)
        return set()

async def get_image_paths_for_analysis_old(extensions: tuple) -> List[Path]:
    """Pobiera ścieżki do plików pasujących do podanych rozszerzeń."""
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            placeholders = ','.join('?' * len(extensions))
            query = f"""
                SELECT final_path FROM downloaded_media
                WHERE status = 'downloaded' AND final_path IS NOT NULL
                AND SUBSTR(LOWER(final_path), -LENGTH(final_path) + INSTR(LOWER(final_path), '.')) IN ({placeholders})
            """
            cursor = await conn.execute(query, extensions)
            return [Path(row[0]) for row in await cursor.fetchall() if row[0] and Path(row[0]).exists()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania ścieżek do analizy obrazów: {e}", exc_info=True)
        return []

async def get_images_without_perceptual_hash() -> List[Dict[str, Any]]:
    """
    Pobiera z bazy listę obrazów o statusie 'downloaded', które nie mają
    jeszcze obliczonego hasha percepcyjnego (pHash).

    Zwraca listę słowników zawierających 'id' i 'final_path' każdego obrazu.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            # Zapytanie wybiera tylko pliki graficzne, które zostały pobrane
            # i nie mają jeszcze przypisanego hasha percepcyjnego.
            query = """
                SELECT id, final_path FROM downloaded_media 
                WHERE (perceptual_hash IS NULL OR perceptual_hash = '') AND status = 'downloaded'
                AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')
            """
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania obrazów bez hasha percepcyjnego: {e}", exc_info=True)
        return []

async def update_perceptual_hash_batch(updates: List[Tuple[str, int]]):
    """
    Zapisuje partię obliczonych hashy percepcyjnych (pHash) do bazy danych.

    Przyjmuje listę krotek, gdzie każda krotka zawiera (perceptual_hash, id).
    """
    await setup_database()
    if not updates:
        return
    
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.executemany(
                "UPDATE downloaded_media SET perceptual_hash = ? WHERE id = ?",
                updates
            )
            await conn.commit()
            logger.info(f"Zapisano partię {len(updates)} hashy percepcyjnych do bazy danych.")
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas wsadowej aktualizacji hashy percepcyjnych: {e}", exc_info=True)

async def get_all_perceptual_hashes() -> List[Dict[str, Any]]:
    """
    Pobiera wszystkie obliczone hashe percepcyjne (pHash) z bazy danych.

    Zwraca listę słowników, gdzie każdy słownik zawiera kluczowe informacje
    potrzebne do algorytmu porównawczego, w tym sparsowany obiekt `imagehash`.
    """
    await setup_database()
    
    # Import jest tutaj, aby uniknąć zależności na poziomie modułu, jeśli biblioteka nie jest zainstalowana
    try:
        import imagehash
    except ImportError:
        logger.error("Biblioteka 'imagehash' nie jest zainstalowana. Nie można przetworzyć hashy.")
        return []
        
    all_hashes_list = []
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            # Zapytanie pobiera wszystkie niezbędne dane dla każdego pliku, który posiada pHash
            query = """
                SELECT id, url, final_path, perceptual_hash, json_extract(metadata_json, '$.DateTime') as dt_str 
                FROM downloaded_media 
                WHERE perceptual_hash IS NOT NULL AND perceptual_hash != '' AND status = 'downloaded'
            """
            cursor = await conn.execute(query)
            async for rec in cursor:
                try:
                    # Konwertujemy string z bazy z powrotem na obiekt imagehash
                    # oraz parsujemy datę, co jest kluczowe dla optymalizacji szybkiego skanu
                    all_hashes_list.append({
                        "id": rec['id'],
                        "url": rec['url'],
                        "path": Path(rec['final_path']),
                        "hash": imagehash.hex_to_hash(rec['perceptual_hash']),
                        "datetime": datetime.fromisoformat(rec['dt_str'].replace('Z', '+00:00')) if rec['dt_str'] else None
                    })
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Pominięto rekord z nieprawidłowym hashem lub datą dla ID {rec['id']}: {e}")
                    continue
        return all_hashes_list
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania hashy percepcyjnych z bazy: {e}", exc_info=True)
        return []

async def get_metadata_for_display(entry_id: int, file_path: Path) -> Dict[str, str]:
    """
    Pobiera metadane dla pojedynczego wpisu i parsuje je do formatu
    przyjaznego do wyświetlania w interfejsie użytkownika.

    Args:
        entry_id (int): ID wpisu w bazie danych.
        file_path (Path): Ścieżka do pliku na dysku (do weryfikacji rozmiaru).

    Returns:
        Dict[str, str]: Słownik z czytelnymi, sformatowanymi metadanymi.
    """
    await setup_database()
    
    # Import jest tutaj, aby uniknąć problemów z cyklicznymi zależnościami
    from .utils import format_size_for_display

    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            cursor = await conn.execute(
                "SELECT metadata_json FROM downloaded_media WHERE id = ?",
                (entry_id,)
            )
            row = await cursor.fetchone()
            if not row:
                return {}

        metadata = json.loads(row[0] or '{}')

        # Logika parsowania, przeniesiona i zaadaptowana z utils._parse_metadata_for_display
        date_tags = ['EXIF:DateTimeOriginal', 'EXIF:CreateDate', 'QuickTime:CreateDate', 'DateTime', 'XMP:CreateDate', 'File:FileModifyDate']
        date_str = "Brak"
        for tag in date_tags:
            if tag in metadata:
                date_str = str(metadata[tag]).split('+')[0].strip()
                break
        
        dimensions_str = metadata.get('File:ImageSize') or \
                         (f"{metadata.get('EXIF:ImageWidth')}x{metadata.get('EXIF:ImageHeight')}" if 'EXIF:ImageWidth' in metadata else "Brak")
        
        size_str = "Brak pliku"
        try:
            if await asyncio.to_thread(file_path.exists):
                size_bytes = (await asyncio.to_thread(file_path.stat)).st_size
                size_str = format_size_for_display(size_bytes)
        except (OSError, FileNotFoundError):
            pass

        file_type = metadata.get('File:FileType', "Brak")
        camera_model = metadata.get('EXIF:Model') or metadata.get('Camera', "Brak")
        
        f_number = metadata.get('EXIF:FNumber')
        exposure_time = metadata.get('EXIF:ExposureTime')
        iso = metadata.get('EXIF:ISO')
        exposure_str = f"f/{f_number}, {exposure_time}s, ISO {iso}" if all([f_number, exposure_time, iso]) else "Brak"
        
        lat = metadata.get('EXIF:GPSLatitude')
        lon = metadata.get('EXIF:GPSLongitude')
        gps_str = f"{lat}, {lon}" if all([lat, lon]) else "Brak"

        return {
            "date": date_str,
            "dimensions": dimensions_str,
            "size": size_str,
            "type": file_type,
            "camera": camera_model,
            "exposure": exposure_str,
            "gps": gps_str
        }

    except (aiosqlite.Error, json.JSONDecodeError, KeyError) as e:
        logger.error(f"Błąd podczas pobierania metadanych do wyświetlenia dla ID {entry_id}: {e}", exc_info=True)
        return {}

async def get_images_to_tag() -> List[Dict[str, Any]]:
    """
    Pobiera z bazy listę obrazów, które są gotowe do przetworzenia przez
    moduł inteligentnego tagowania (AI Tagger).

    Szuka plików o statusie 'downloaded', które nie mają jeszcze przypisanych
    tagów AI.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            # Zapytanie wybiera pobrane pliki (JPG, JPEG, PNG), które nie mają jeszcze tagów AI.
            query = """
                SELECT id, final_path FROM downloaded_media 
                WHERE status = 'downloaded' 
                AND (ai_tags IS NULL OR ai_tags = '' OR ai_tags = '[]')
                AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')
            """
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania obrazów do tagowania AI: {e}", exc_info=True)
        return []

async def update_ai_tags_batch(updates: List[Tuple[str, int]]):
    """
    Zapisuje partię tagów AI (jako string JSON) do bazy danych.

    Przyjmuje listę krotek, gdzie każda krotka zawiera (ai_tags_json, id).
    """
    await setup_database()
    if not updates:
        return
    
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.executemany(
                "UPDATE downloaded_media SET ai_tags = ? WHERE id = ?",
                updates
            )
            await conn.commit()
            logger.info(f"Zapisano partię {len(updates)} tagów AI do bazy danych.")
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas wsadowej aktualizacji tagów AI: {e}", exc_info=True)

async def get_raw_media_entries_for_analysis() -> list[dict]:
    """
    Pobiera surowe dane z bazy, niezbędne do działania wszystkich modułów analitycznych.
    Zwraca listę słowników z kluczami: id, metadata_json, final_path.

    UWAGA: Ta funkcja wczytuje wszystkie dane do pamięci i jest przeznaczona
    dla narzędzi wymagających dostępu do każdego rekordu (np. Eksploratory).
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            # Definiujemy statusy, które kwalifikują plik do analizy
            valid_statuses = ['downloaded', 'skipped', 'archived', 'scanned']
            placeholders = ','.join(['?'] * len(valid_statuses))
            
            query = f"""
                SELECT id, metadata_json, final_path FROM downloaded_media
                WHERE status IN ({placeholders}) AND json_valid(metadata_json) = 1
            """
            cursor = await conn.execute(query, valid_statuses)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania surowych danych do analizy: {e}", exc_info=True)
        return []

async def clear_all_perceptual_hashes() -> int:
    """
    Czyści (ustawia na NULL) wszystkie istniejące hashe percepcyjne w bazie danych.

    Returns:
        int: Liczba zaktualizowanych wierszy.
    """
    await setup_database()
    logger.info("Rozpoczynam czyszczenie wszystkich hashy percepcyjnych w bazie danych...")
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            cursor = await conn.execute("UPDATE downloaded_media SET perceptual_hash = NULL WHERE perceptual_hash IS NOT NULL")
            await conn.commit()
            logger.info(f"Pomyślnie wyczyszczono {cursor.rowcount} hashy percepcyjnych.")
            return cursor.rowcount
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas czyszczenia hashy percepcyjnych: {e}", exc_info=True)
        return 0


async def get_all_images_for_phash_recalculation() -> list[dict]:
    """
    Pobiera z bazy listę WSZYSTKICH obrazów o statusie 'downloaded',
    które nadają się do obliczenia hasha percepcyjnego, ignorując istniejące hashe.

    Zwraca listę słowników zawierających 'id' i 'final_path' każdego obrazu.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = """
                SELECT id, final_path FROM downloaded_media 
                WHERE status = 'downloaded'
                AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')
            """
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania wszystkich obrazów do ponownego hashowania: {e}", exc_info=True)
        return []

async def get_imported_images_without_perceptual_hash() -> list[dict]:
    """
    Pobiera z bazy listę obrazów zaimportowanych z dysku (`local_import`),
    które nie mają jeszcze obliczonego hasha percepcyjnego (pHash).

    Zwraca listę słowników zawierających 'id' i 'final_path' każdego obrazu.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            # To zapytanie jest dedykowane tylko dla plików importowanych z dysku.
            query = """
                SELECT id, final_path FROM downloaded_media 
                WHERE (perceptual_hash IS NULL OR perceptual_hash = '')
                AND source = 'local_import'
                AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')
            """
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania importowanych obrazów bez hasha percepcyjnego: {e}", exc_info=True)
        return []

async def get_imported_image_paths_for_analysis(extensions: tuple) -> list[Path]:
    """
    Pobiera ścieżki do plików zaimportowanych z dysku (`local_import`),
    które pasują do podanych rozszerzeń.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            # Tworzymy listę zapytań dla każdego rozszerzenia
            extension_queries = [f"LOWER(final_path) LIKE '%.{ext.strip('.')}'" for ext in extensions]
            
            query = f"""
                SELECT final_path FROM downloaded_media
                WHERE source = 'local_import' AND final_path IS NOT NULL
                AND ({' OR '.join(extension_queries)})
            """
            cursor = await conn.execute(query)
            # Zwracamy listę obiektów Path, upewniając się, że pliki istnieją
            return [Path(row[0]) for row in await cursor.fetchall() if row[0] and Path(row[0]).exists()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania ścieżek do analizy obrazów importowanych: {e}", exc_info=True)
        return []

async def get_local_import_entries() -> list[dict]:
    """
    Pobiera wszystkie wpisy z bazy danych, które zostały zaimportowane z dysku lokalnego.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = "SELECT id, url, final_path FROM downloaded_media WHERE source = 'local_import'"
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania wpisów importowanych lokalnie: {e}", exc_info=True)
        return []

async def update_paths_for_entry(entry_id: int, new_path: str):
    """
    Aktualizuje `final_path` i `expected_path` dla pojedynczego wpisu w bazie danych.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.execute(
                "UPDATE downloaded_media SET final_path = ?, expected_path = ? WHERE id = ?",
                (new_path, new_path, entry_id)
            )
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Błąd aktualizacji ścieżek dla ID {entry_id}: {e}", exc_info=True)

async def get_downloaded_entries_for_path_fixing() -> list[dict]:
    """
    Pobiera wszystkie wpisy z bazy, które pochodzą z pobierania (nie z importu lokalnego)
    i posiadają ścieżki do weryfikacji przez path_fix_tool.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            # KLUCZOWA ZMIANA: Dodajemy warunek, aby ignorować pliki importowane lokalnie.
            query = """
                SELECT id, final_path, expected_path FROM downloaded_media
                WHERE (source IS NULL OR source != 'local_import')
                AND final_path IS NOT NULL AND final_path != ''
            """
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania wpisów do naprawy ścieżek: {e}", exc_info=True)
        return []

async def update_paths_for_entry_by_id(entry_id: int, new_final_path: str, new_expected_path: str):
    """
    Aktualizuje `final_path` i `expected_path` dla pojedynczego wpisu w bazie danych.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.execute(
                "UPDATE downloaded_media SET final_path = ?, expected_path = ? WHERE id = ?",
                (new_final_path, new_expected_path, entry_id)
            )
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Błąd aktualizacji ścieżek dla ID {entry_id}: {e}", exc_info=True)

# W pliku: core/database.py
# ZASTĄP starą funkcję `get_image_paths_for_analysis` i USUŃ `get_imported_image_paths_for_analysis`

async def get_image_paths_for_analysis_old(extensions: tuple, source_filter: str = 'downloaded') -> list[Path]:
    """
    Pobiera ścieżki do plików pasujących do podanych rozszerzeń,
    z możliwością filtrowania według źródła pliku.

    Args:
        extensions (tuple): Krotka z rozszerzeniami plików do wyszukania (np. ('.jpg', '.png')).
        source_filter (str): Filtr źródła:
                             - 'downloaded': Zwraca tylko pliki pobrane z Google Photos.
                             - 'local_import': Zwraca tylko pliki importowane z dysku.
                             - 'all': Zwraca wszystkie pliki, niezależnie od źródła.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            # Budujemy warunek na podstawie rozszerzeń
            extension_queries = [f"LOWER(final_path) LIKE '%.{ext.strip('.')}'" for ext in extensions]
            
            # Budujemy główną kwerendę
            query = f"""
                SELECT final_path FROM downloaded_media
                WHERE final_path IS NOT NULL AND ({' OR '.join(extension_queries)})
            """
            
            # Dodajemy warunek na źródło pliku
            if source_filter == 'downloaded':
                query += " AND (source IS NULL OR source = 'google_photos')"
            elif source_filter == 'local_import':
                query += " AND source = 'local_import'"
            # Dla 'all' nie dodajemy żadnego dodatkowego warunku na źródło

            cursor = await conn.execute(query)
            # Zwracamy listę obiektów Path, upewniając się, że pliki istnieją
            return [Path(row[0]) for row in await cursor.fetchall() if row[0] and Path(row[0]).exists()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania ścieżek do analizy obrazów: {e}", exc_info=True)
        return []

async def get_image_paths_for_analysis(extensions: tuple, source_filter: str = 'downloaded') -> list[Path]:
    """
    Pobiera ścieżki do plików pasujących do podanych rozszerzeń,
    z możliwością filtrowania według źródła pliku.

    Args:
        extensions (tuple): Krotka z rozszerzeniami plików do wyszukania (np. ('.jpg', '.png')).
        source_filter (str): Filtr źródła:
                             - 'downloaded': Zwraca tylko pliki pobrane z Google Photos.
                             - 'local_import': Zwraca tylko pliki importowane z dysku.
                             - 'all': Zwraca wszystkie pliki, niezależnie od źródła.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            # Budujemy warunek na podstawie rozszerzeń
            extension_queries = [f"LOWER(final_path) LIKE '%.{ext.strip('.')}'" for ext in extensions]
            
            # Budujemy główną kwerendę
            query = f"""
                SELECT final_path FROM downloaded_media
                WHERE final_path IS NOT NULL AND ({' OR '.join(extension_queries)})
            """
            
            # Dodajemy warunek na źródło pliku
            if source_filter == 'downloaded':
                query += " AND (source IS NULL OR source = 'google_photos')"
            elif source_filter == 'local_import':
                query += " AND source = 'local_import'"
            # Dla 'all' nie dodajemy żadnego dodatkowego warunku na źródło

            cursor = await conn.execute(query)
            # Zwracamy listę obiektów Path, upewniając się, że pliki istnieją
            return [Path(row[0]) for row in await cursor.fetchall() if row[0] and Path(row[0]).exists()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd pobierania ścieżek do analizy obrazów: {e}", exc_info=True)
        return []

async def get_downloaded_entries_for_path_fixing() -> list[dict]:
    """
    Pobiera wszystkie wpisy z bazy, które pochodzą z pobierania (nie z importu lokalnego)
    i posiadają ścieżki do weryfikacji przez path_fix_tool.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            # KLUCZOWA ZMIANA: Dodajemy warunek, aby ignorować pliki importowane lokalnie.
            query = """
                SELECT id, final_path, expected_path FROM downloaded_media
                WHERE (source IS NULL OR source != 'local_import')
                AND final_path IS NOT NULL AND final_path != ''
            """
            cursor = await conn.execute(query)
            return [dict(row) for row in await cursor.fetchall()]
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania wpisów do naprawy ścieżek: {e}", exc_info=True)
        return []

async def update_paths_for_entry_by_id(entry_id: int, new_final_path: str, new_expected_path: str):
    """
    Aktualizuje `final_path` i `expected_path` dla pojedynczego wpisu w bazie danych.
    """
    await setup_database()
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.execute(
                "UPDATE downloaded_media SET final_path = ?, expected_path = ? WHERE id = ?",
                (new_final_path, new_expected_path, entry_id)
            )
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Błąd aktualizacji ścieżek dla ID {entry_id}: {e}", exc_info=True)
