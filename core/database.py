# -*- coding: utf-8 -*-

# plik: core/database.py
# Wersja 11.0 - Finalna, uporządkowana wersja z pełnym CRUD dla osób.

import sys
import json
import logging
import pickle
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Any, Optional
import asyncio

import aiosqlite
import numpy as np

from .config import DATABASE_FILE, MAX_RETRIES

logger = logging.getLogger(__name__)
_db_initialized = False


async def setup_database():
    """
    Inicjalizuje i weryfikuje strukturę bazy danych, w tym tabele 
    dla zaawansowanego rozpoznawania twarzy. Wykonywane tylko raz na sesję.
    """
    global _db_initialized
    if _db_initialized:
        return

    logger.info("Rozpoczynam jednorazową inicjalizację i weryfikację schematu bazy danych...")
    try:
        db_path = Path(DATABASE_FILE)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiosqlite.connect(db_path) as conn:
            # Tabela główna z mediami
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS downloaded_media (
                    id INTEGER PRIMARY KEY, url TEXT NOT NULL UNIQUE, filename TEXT, final_path TEXT,
                    expected_path TEXT, metadata_json TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    status TEXT, retry_count INTEGER DEFAULT 0, processing_status TEXT,
                    exif_write_status TEXT, file_hash TEXT, perceptual_hash TEXT,
                    ai_tags TEXT, source TEXT DEFAULT 'google_photos'
                )""")
            
            # Tabela stanu skryptu
            await conn.execute("CREATE TABLE IF NOT EXISTS script_state (key TEXT PRIMARY KEY, value TEXT)")
            
            # Tabela znanych osób
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS people (
                    person_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                    model_name TEXT NOT NULL, master_embedding BLOB NOT NULL,
                    source_media_id INTEGER, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (source_media_id) REFERENCES downloaded_media (id) ON DELETE SET NULL,
                    UNIQUE(name, model_name)
                )""")
            
            # Tabela wszystkich wykrytych twarzy
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS faces (
                    face_id INTEGER PRIMARY KEY AUTOINCREMENT, media_id INTEGER NOT NULL, person_id INTEGER,
                    embedding BLOB NOT NULL, facial_area TEXT NOT NULL, model_name TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (media_id) REFERENCES downloaded_media (id) ON DELETE CASCADE,
                    FOREIGN KEY (person_id) REFERENCES people (person_id) ON DELETE SET NULL
                )""")
            
            # Tabela duplikatów
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS similar_image_pairs (
                    pair_id INTEGER PRIMARY KEY, image_id_a INTEGER, image_id_b INTEGER,
                    distance INTEGER, status TEXT DEFAULT 'pending',
                    FOREIGN KEY (image_id_a) REFERENCES downloaded_media (id) ON DELETE CASCADE,
                    FOREIGN KEY (image_id_b) REFERENCES downloaded_media (id) ON DELETE CASCADE
                )""")
            
            # --- POCZĄTEK ZMIAN: Dodanie nowej kolumny na URL z Takeout ---
            try:
                # Ta operacja doda nową kolumnę tylko wtedy, gdy jeszcze nie istnieje.
                await conn.execute("ALTER TABLE downloaded_media ADD COLUMN google_photos_url TEXT")
                await conn.commit()
                logger.info("Pomyślnie dodano kolumnę 'google_photos_url' do tabeli 'downloaded_media'.")
            except aiosqlite.OperationalError as e:
                # To jest oczekiwany błąd, jeśli kolumna już istnieje. Ignorujemy go.
                if "duplicate column name" in str(e):
                    logger.debug("Kolumna 'google_photos_url' już istnieje. Pomijam dodawanie.")
                else:
                    # Rzuć błąd ponownie, jeśli jest to inny, nieoczekiwany problem
                    raise
            # --- KONIEC ZMIAN ---

            # Indeksy dla wydajności
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_faces_media_id ON faces (media_id);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_faces_person_id ON faces (person_id);")

            # Usunięcie starej, nieużywanej tabeli
            await conn.execute("DROP TABLE IF EXISTS recognized_faces;")
            await conn.commit()
            
        _db_initialized = True
        logger.info("Inicjalizacja i weryfikacja schematu bazy danych zakończona.")

    except aiosqlite.Error as e:
        logger.critical(f"Nie można utworzyć lub zaktualizować bazy danych: {e}", exc_info=True)
        sys.exit(1)


# --- FUNKCJE ZARZĄDZANIA OSOBAMI (PEOPLE) ---

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

# --- POZOSTAŁE FUNKCJE POMOCNICZE (np. do pobierania, statystyk) ---

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

