# plik: core/takeout_importer/tasks.py
# Wersja 1.3 - Wprowadzono dwuetapowe mapowanie i szczegółowe logowanie do dashboardu.
# Opis: Ten moduł zawiera czystą logikę biznesową dla Importera Takeout.
#       Odpowiada za mapowanie plików JSON, scalanie metadanych
#       i aktualizację bazy danych w sposób wydajny i niezawodny.
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Importy z modułów projektu `core`
from ..database import get_all_db_records_for_takeout_import, update_takeout_metadata_batch

# Importy z wewnętrznych modułów pakietu
from .ui.live_display import TakeoutImporterLiveDisplay

# Inicjalizacja
logger = logging.getLogger(__name__)

# Rozszerzona lista znanych rozszerzeń plików do inteligentnego parsowania
KNOWN_MEDIA_EXTENSIONS = (
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.heic', '.heif', 
    '.avif', '.mp4', '.mov', '.avi', '.mkv', '.webm', '.3gp', '.mpg', '.mpeg', 
    '.raw', '.dng', '.cr2', '.nef', '.orf', '.arw'
)


def _get_original_filename_fast(json_filename: str) -> str:
    """
    Szybka, "naiwna" metoda dopasowywania oparta na nazwie pliku .json.
    Usuwa znane przyrostki dodawane przez Google Takeout, np. '(1)', '.json',
    '.supplemental-metadata.json'.

    Args:
        json_filename (str): Nazwa pliku .json.

    Returns:
        str: Potencjalna nazwa oryginalnego pliku multimedialnego.
    """
    name = json_filename
    if name.endswith('.json'):
        name = name[:-5]
    if name.endswith(('.suppl', '.supplemental-metadata')):
        if name.endswith('.suppl'): name = name[:-6]
        if name.endswith('.supplemental-metadata'): name = name[:-22]
        if name.endswith(')') and '(' in name:
            name = name[:name.rfind('(')]
    return name


def _get_original_filename_from_title(json_path: Path) -> Optional[str]:
    """
    Precyzyjna metoda dopasowywania oparta na odczytaniu pola "title"
    z zawartości pliku .json. Jest to ostateczne źródło prawdy o nazwie pliku.

    Args:
        json_path (Path): Ścieżka do pliku .json.

    Returns:
        Optional[str]: Oryginalna nazwa pliku z pola "title" lub None w przypadku błędu.
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get("title")
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        # Ignorujemy pliki .json, których nie da się odczytać
        logger.warning(f"Nie udało się odczytać pliku JSON: {json_path}", exc_info=True)
        return None


async def process_takeout_folder(photos_path: Path, display: TakeoutImporterLiveDisplay):
    """
    Główna funkcja robocza: skanuje folder Takeout, używając dwuetapowej strategii
    mapowania, i aktualizuje bazę danych, raportując szczegółowy postęp do dashboardu.

    Args:
        photos_path (Path): Ścieżka do folderu 'Google Zdjęcia'.
        display (TakeoutImporterLiveDisplay): Obiekt dashboardu do raportowania postępu.
    """
    
    # --- Etap 1: Szybkie Mapowanie po Nazwie Pliku ---
    display.update_stage("Etap 1/4: Szybkie mapowanie...", total=None)
    display.add_log("⚙️", "Rozpoczynam szybkie mapowanie plików .json po nazwie...", "cyan")
    
    def find_and_map_fast():
        """Synchroniczna funkcja pomocnicza do wykonania w osobnym wątku."""
        json_map_by_name: Dict[str, Path] = {}
        unmapped_jsons: List[Path] = []
        all_jsons = list(photos_path.rglob('*.json'))
        
        display.progress_bar.update(display._task_id, total=len(all_jsons))
        
        for json_path in all_jsons:
            original_filename = _get_original_filename_fast(json_path.name)
            if original_filename and original_filename not in json_map_by_name:
                json_map_by_name[original_filename] = json_path
                display.advance('mapped')
            else:
                unmapped_jsons.append(json_path)
            display.update_status(f"Mapowanie: {json_path.name}")
        return json_map_by_name, unmapped_jsons

    json_map_by_name, unmapped_jsons = await asyncio.to_thread(find_and_map_fast)
    logger.info(f"Etap 1: Zmapowano {len(json_map_by_name)} plików po nazwie. Pozostało: {len(unmapped_jsons)}")
    display.add_log("✅", f"Zakończono mapowanie po nazwie. Znaleziono: [bold]{len(json_map_by_name)}[/] dopasowań.", "green")

    # --- Etap 2: Precyzyjne Mapowanie po Polu "title" (Fallback) ---
    json_map_by_title: Dict[str, Path] = {}
    if unmapped_jsons:
        display.update_stage(f"Etap 2/4: Analiza 'title' dla {len(unmapped_jsons)} plików...", total=len(unmapped_jsons))
        display.add_log("🔎", f"Rozpoczynam głęboką analizę [bold]{len(unmapped_jsons)}[/] pozostałych plików .json...", "cyan")
        for json_path in unmapped_jsons:
            title_filename = await asyncio.to_thread(_get_original_filename_from_title, json_path)
            if title_filename and title_filename not in json_map_by_title:
                json_map_by_title[title_filename] = json_path
                display.advance('mapped')
            else:
                display.advance('unmapped')
            display.update_status(f"Analiza 'title': {json_path.name}")
        logger.info(f"Etap 2: Zmapowano dodatkowo {len(json_map_by_title)} plików po polu 'title'.")
        display.add_log("✅", f"Zakończono. Znaleziono [bold]{len(json_map_by_title)}[/] dodatkowych dopasowań.", "green")

    # --- Etap 3: Pobieranie rekordów i scalanie ---
    display.update_stage("Etap 3/4: Pobieranie z bazy...", total=None)
    db_records = await get_all_db_records_for_takeout_import()
    logger.info(f"Pobrano {len(db_records)} rekordów z bazy do porównania.")
    display.add_log("⬇️", f"Pobrano [bold]{len(db_records)}[/] rekordów z bazy do porównania.", "dim")
    
    display.update_stage("Etap 4/4: Scalanie metadanych...", total=len(db_records))
    updates_batch = []
    
    for record in db_records:
        filename = record['filename']
        # Szukamy najpierw w precyzyjnej mapie, potem w szybkiej
        json_path = json_map_by_title.get(filename) or json_map_by_name.get(filename)
        
        if json_path:
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    takeout_meta = json.load(f)
                db_meta = json.loads(record['metadata_json'] or '{}')
                
                merged_meta = db_meta.copy()
                if not merged_meta.get('Description') and takeout_meta.get('description'): merged_meta['Description'] = takeout_meta['description']
                merged_meta['Title_Takeout'] = takeout_meta.get('title'); merged_meta['GeoData_Takeout'] = takeout_meta.get('geoData'); merged_meta['People_Takeout'] = [p['name'] for p in takeout_meta.get('people', []) if 'name' in p]
                if ts := takeout_meta.get('photoTakenTime', {}).get('timestamp'): merged_meta['Timestamp_Takeout'] = datetime.fromtimestamp(int(ts)).isoformat()
                
                updates_batch.append((json.dumps(merged_meta), takeout_meta.get("url"), record['id']))
                display.add_log("🔗", f"[cyan]{filename}[/] scalono z [dim]{json_path.name}[/dim]")
                display.advance('merged')
                
            except (json.JSONDecodeError, KeyError, OSError) as e:
                logger.warning(f"Błąd przetwarzania {json_path.name} dla {filename}: {e}")
        
        display.update_status(f"Sprawdzanie: {filename}")
        display.advance()

    # --- Ostatni etap: Zapis do bazy ---
    if updates_batch:
        display.update_stage("Zapisywanie do bazy...", total=len(updates_batch))
        display.add_log("💾", f"Rozpoczynam zapis [bold]{len(updates_batch)}[/] aktualizacji w bazie...", "cyan")
        
        CHUNK_SIZE = 500
        for i in range(0, len(updates_batch), CHUNK_SIZE):
            chunk = updates_batch[i:i+CHUNK_SIZE]
            await update_takeout_metadata_batch(chunk)
            for _ in range(len(chunk)):
                display.advance('updated')
            display.update_status(f"Zapisano {i+len(chunk)}/{len(updates_batch)} wpisów...")
            
        logger.info(f"Zaktualizowano {len(updates_batch)} rekordów w bazie.")
        display.add_log("✅", f"Zakończono zapis.", "green")
