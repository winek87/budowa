# plik: core/analytics/data_loader.py (FINALNA WERSJA PO OPTYMALIZACJI)
# -*- coding: utf-8 -*-

# ##############################################################################
# ===                    MODUŁ ŁADOWANIA DANYCH ANALITYCZNYCH                  ===
# ##############################################################################
#
# Ten plik zawiera funkcje odpowiedzialne za pobieranie danych z bazy
# na potrzeby modułów analitycznych. Został zoptymalizowany, aby przenosić
# ciężar obliczeń na bazę danych w celu poprawy wydajności.
#
################################################################################

import logging
import json
from datetime import datetime

# ZMIANA: Importujemy nowe, zoptymalizowane funkcje z modułu bazy danych
from ..database import setup_database, get_aggregated_analytics_data, get_raw_media_entries_for_analysis

# --- Inicjalizacja i Konfiguracja Modułu ---
logger = logging.getLogger(__name__)


async def get_analytics_data() -> dict:
    """
    Pobiera gotowe, zagregowane dane analityczne bezpośrednio z bazy danych.
    Ta funkcja jest zoptymalizowana pod kątem wydajności i niskiego zużycia pamięci.
    """
    await setup_database()
    logger.info("Pobieram zagregowane dane analityczne z bazy...")
    
    # Wywołujemy naszą nową, potężną funkcję i zwracamy jej wynik
    data = await get_aggregated_analytics_data()
    
    if data:
        logger.info(f"Pomyślnie pobrano zagregowane dane dla {data.get('overall', {}).get('total_files', 0)} plików.")
    else:
        logger.error("Nie udało się pobrać danych analitycznych z bazy.")
        
    return data


async def get_all_media_entries() -> list[dict]:
    """
    Asynchronicznie pobiera i parsuje wszystkie pojedyncze wpisy z bazy.

    UWAGA: Ta funkcja wczytuje wszystkie dane do pamięci i jest przeznaczona
    dla narzędzi wymagających dostępu do każdego rekordu (np. Eksploratory).
    Do generowania standardowych raportów należy używać `get_analytics_data()`.
    """
    await setup_database()
    logger.info("Rozpoczynam wczytywanie i parsowanie wszystkich wpisów do analizy...")
    
    raw_entries = await get_raw_media_entries_for_analysis()
    if not raw_entries:
        logger.error("Nie udało się pobrać surowych danych z bazy do analizy.")
        return []

    # Ta wewnętrzna funkcja parsująca pozostaje, aby zapewnić spójny format
    # danych dla narzędzi, które potrzebują wszystkich rekordów.
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
    
    def _parse_human_readable_size(size_input: any) -> int | None:
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

    # Przetwarzamy (parsujemy) surowe dane w Pythonie
    entries = []
    for row in raw_entries:
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

    logger.info(f"Pomyślnie wczytano i sparsowano {len(entries)} wpisów.")
    return entries
