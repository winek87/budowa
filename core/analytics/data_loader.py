# -*- coding: utf-8 -*-

# plik: core/analytics/data_loader.py
# Wersja 4.2 - Scentralizowano funkcje pomocnicze
#
# ##############################################################################
# ===                    MODUŁ ŁADOWANIA DANYCH ANALITYCZNYCH                  ===
# ##############################################################################
#
# Ten plik zawiera kluczowe funkcje odpowiedzialne za łączenie się z bazą
# danych, pobieranie surowych danych o mediach, a następnie ich parsowanie
# i transformowanie do ustrukturyzowanej formy, gotowej do dalszej analizy
# przez inne moduły analityczne.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import json
import math
import asyncio
import logging
from pathlib import Path
from datetime import datetime

# --- Importy asynchroniczne ---
import aiosqlite

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from ..config import DATABASE_FILE
from ..database import setup_database

# --- Inicjalizacja i Konfiguracja Modułu ---
logger = logging.getLogger(__name__)

VALID_STATUSES_FOR_ANALYSIS = ['downloaded', 'skipped', 'archived', 'scanned']

# --- SEKCJA 1: POBIERANIE I PARSOWANIE DANYCH ---

async def get_all_media_entries() -> list[dict]:
    """
    Asynchronicznie pobiera wszystkie wpisy z bazy danych, parsuje ich JSON
    z metadanymi i zwraca ustrukturyzowaną listę słowników gotową do analizy.
    """
    await setup_database()

    db_path = Path(DATABASE_FILE)
    if not await asyncio.to_thread(db_path.exists):
        logger.error(f"Plik bazy danych '{DATABASE_FILE}' nie istnieje! Uruchom skaner, aby go utworzyć.")
        return []

    logger.info("Rozpoczynam asynchroniczne wczytywanie i parsowanie metadanych do analizy...")

    def _find_and_parse_date(metadata: dict) -> datetime | None:
        date_tags_priority = [
            'DateTime', 'EXIF:DateTimeOriginal', 'EXIF:CreateDate', 'QuickTime:CreateDate',
            'XMP:CreateDate', 'XMP:DateCreated', 'File:FileModifyDate'
        ]
        for tag in date_tags_priority:
            if date_str := metadata.get(tag):
                try:
                    cleaned_str = str(date_str).split('+')[0].split('.')[0].strip()
                    if ":" in cleaned_str[0:10] and 'T' not in cleaned_str:
                        return datetime.strptime(cleaned_str, '%Y:%m:%d %H:%M:%S')
                    else:
                        return datetime.fromisoformat(cleaned_str.replace('Z', '+00:00'))
                except (ValueError, TypeError): continue
        return None

    entries = []
    try:
        async with aiosqlite.connect(db_path) as conn:
            conn.row_factory = aiosqlite.Row
            placeholders = ','.join(['?'] * len(VALID_STATUSES_FOR_ANALYSIS))
            query = f"SELECT id, metadata_json, final_path FROM downloaded_media WHERE status IN ({placeholders}) AND json_valid(metadata_json) = 1"

            async with conn.execute(query, VALID_STATUSES_FOR_ANALYSIS) as cursor:
                async for row in cursor:
                    try:
                        details = json.loads(row['metadata_json'])
                        parsed_date = _find_and_parse_date(details)
                        if not parsed_date: continue

                        dimensions_str = None
                        if 'Dimensions' in details and details['Dimensions']: dimensions_str = details['Dimensions']
                        elif 'File:ImageSize' in details and details['File:ImageSize']: dimensions_str = details['File:ImageSize']
                        elif 'EXIF:ImageWidth' in details and 'EXIF:ImageHeight' in details: dimensions_str = f"{details['EXIF:ImageWidth']}×{details['EXIF:ImageHeight']}"
                        elif 'QuickTime:ImageWidth' in details and 'QuickTime:ImageHeight' in details: dimensions_str = f"{details['QuickTime:ImageWidth']}×{details['QuickTime:ImageHeight']}"

                        entry_data = {
                            'id': row['id'], 'dt': parsed_date,
                            'filename': details.get('FileName') or details.get('File:FileName', 'Brak nazwy'),
                            'size': _parse_human_readable_size(details.get('FileSize') or details.get('File:FileSize')),
                            'final_path': row['final_path'],
                            'Location': details.get('Location') or details.get('Composite:GPSPosition'),
                            'Camera': details.get('Camera') or details.get('EXIF:Model'),
                            'Dimensions': dimensions_str,
                            'TaggedPeople': details.get('TaggedPeople'),
                            'Albums': details.get('Albums'),
                            'Description': details.get('Description') or details.get('EXIF:ImageDescription'),
                        }
                        entries.append(entry_data)
                    except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                        continue

    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas odczytu danych analitycznych z bazy: {e}", exc_info=True)
        return []

    logger.info(f"Pomyślnie wczytano [bold green]{len(entries)}[/bold green] wpisów gotowych do analizy.", extra={"markup": True})
    return entries

# --- SEKCJA 2: FUNKCJE POMOCNICZE DO TRANSFORMACJI DANYCH ---

def _parse_human_readable_size(size_input: any) -> int | None:
    """
    Prywatna funkcja pomocnicza do konwersji różnych formatów rozmiaru pliku na liczbę całkowitą w bajtach.
    """
    if isinstance(size_input, int): return size_input
    if not isinstance(size_input, str): return None
    cleaned_str = str(size_input).replace('\xa0', '').replace(',', '.').strip().lower()
    try:
        if 'gb' in cleaned_str: return int(float(cleaned_str.replace('gb', '').strip()) * 1024**3)
        if 'mb' in cleaned_str: return int(float(cleaned_str.replace('mb', '').strip()) * 1024**2)
        if 'kb' in cleaned_str: return int(float(cleaned_str.replace('kb', '').strip()) * 1024)
        if 'b' in cleaned_str: return int(float(cleaned_str.replace('b', '').strip()))
        return int(float(cleaned_str))
    except (ValueError, TypeError): return None

