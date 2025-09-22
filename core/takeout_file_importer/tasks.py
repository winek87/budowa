# plik: core/takeout_file_importer/tasks.py
# Wersja 1.3 - Zintegrowano obsługę Ctrl+C i szczegółowe logi do dashboardu.
# Opis: Ten moduł zawiera czystą logikę biznesową dla Importera Plików z Takeout.
#       Odpowiada za mapowanie plików, identyfikację brakujących elementów,
#       kopiowanie ich lub przenoszenie i zapisywanie do bazy danych.
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Importy z modułów projektu `core`
from ..config import DOWNLOADS_DIR_BASE
from ..database import add_local_file_entry, get_all_filenames_from_db
from ..utils import create_unique_filepath, get_date_from_metadata

# Importy z wewnętrznych modułów pakietu
from .ui.live_display import TakeoutFileImporterLiveDisplay
from .utils import stop_event

# Inicjalizacja
logger = logging.getLogger(__name__)

# Rozszerzona lista znanych rozszerzeń plików do inteligentnego parsowania
SUPPORTED_MEDIA_EXTENSIONS = {
    '.jpg', '.jpeg', '.png', '.heic', '.heif', '.gif', '.webp', '.bmp', '.tif', '.tiff',
    '.mp4', '.mov', '.avi', '.m4v', '.3gp', '.mkv', '.mpg', '.mpeg', '.webm',
    '.cr2', '.nef', '.arw', '.dng'
}


def _get_original_filename_fast(json_filename: str) -> str:
    """Szybka, "naiwna" metoda dopasowywania oparta na nazwie pliku .json."""
    name = json_filename[:-5] if json_filename.endswith('.json') else json_filename
    if name.endswith(')'):
        # Usuwa przyrostki typu (1), (2) itd.
        name = name[:name.rfind('(')]
    return name


def _get_original_filename_from_title(json_path: Path) -> Optional[str]:
    """Precyzyjna metoda dopasowywania oparta na odczytaniu pola "title" z pliku .json."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get("title")
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        logger.warning(f"Nie udało się odczytać pliku JSON: {json_path}", exc_info=True)
        return None


async def perform_file_import(
    photos_path: Path,
    display: TakeoutFileImporterLiveDisplay,
    import_mode: str
):
    """
    Wykonuje cały proces identyfikacji, kopiowania lub przenoszenia brakujących plików,
    raportując szczegółowy postęp do dashboardu na żywo i obsługując przerwanie.

    Args:
        photos_path (Path): Ścieżka do folderu 'Google Zdjęcia' w archiwum Takeout.
        display (TakeoutFileImporterLiveDisplay): Obiekt dashboardu do raportowania.
        import_mode (str): Tryb importu - 'copy' (kopiuj) lub 'move' (przenieś).
    """
    
    # --- Etap 1: Mapowanie plików w archiwum Takeout ---
    display.update_stage("Etap 1/3: Mapowanie plików w archiwum...", total=None)
    display.add_log_and_advance("success", "Rozpoczynam mapowanie plików mediów i .json...")
    
    def find_and_map_files():
        """Synchroniczna funkcja pomocnicza do wykonania w osobnym wątku."""
        media_map: Dict[str, Path] = {}
        json_map_by_name: Dict[str, Path] = {}
        unmapped_jsons: List[Path] = []
        all_files = list(photos_path.rglob('*.*'))
        for file_path in all_files:
            if stop_event.is_set(): break
            if file_path.suffix.lower() in SUPPORTED_MEDIA_EXTENSIONS:
                media_map[file_path.name] = file_path
            elif file_path.suffix.lower() == '.json':
                original_filename = _get_original_filename_fast(file_path.name)
                if original_filename and original_filename not in json_map_by_name:
                    json_map_by_name[original_filename] = file_path
                else:
                    unmapped_jsons.append(file_path)
        return media_map, json_map_by_name, unmapped_jsons

    media_map, json_map_by_name, unmapped_jsons = await asyncio.to_thread(find_and_map_files)
    if stop_event.is_set(): return
    display.add_log_and_advance("success", f"Zmapowano {len(media_map)} plików mediów i {len(json_map_by_name)} .json po nazwie.")

    json_map_by_title: Dict[str, Path] = {}
    if unmapped_jsons:
        for json_path in unmapped_jsons:
            if stop_event.is_set(): break
            title_filename = await asyncio.to_thread(_get_original_filename_from_title, json_path)
            if title_filename and title_filename not in json_map_by_title:
                json_map_by_title[title_filename] = json_path
    if stop_event.is_set(): return
    display.add_log_and_advance("success", f"Dodatkowo zmapowano {len(json_map_by_title)} .json po 'title'.")

    # --- Etap 2: Porównanie z bazą danych ---
    display.update_stage("Etap 2/3: Porównywanie z bazą danych...", total=None)
    display.add_log_and_advance("success", "Pobieram listę istniejących plików z bazy...")
    existing_filenames = await get_all_filenames_from_db()
    missing_files_map = {name: path for name, path in media_map.items() if name not in existing_filenames}
    display.add_log_and_advance("success", f"Znaleziono {len(missing_files_map)} brakujących plików.")

    if not missing_files_map: return

    # --- Etap 3: Importowanie plików ---
    op_text = 'Przenoszenie' if import_mode == 'move' else 'Kopiowanie'
    display.update_stage(f"Etap 3/3: {op_text} plików...", total=len(missing_files_map))
    display.add_log_and_advance("success", f"Rozpoczynam {op_text.lower()} {len(missing_files_map)} plików...")
    
    library_base_path = Path(DOWNLOADS_DIR_BASE)

    for filename, source_path in missing_files_map.items():
        if stop_event.is_set():
            logger.warning("Przerwanie przez użytkownika. Zatrzymuję pętlę importu.")
            break
        try:
            json_path = json_map_by_title.get(filename) or json_map_by_name.get(filename)
            if not json_path:
                display.add_log_and_advance("error", f"Brak pliku .json dla [dim]{filename}[/dim]")
                continue

            with open(json_path, 'r', encoding='utf-8') as f: metadata = json.load(f)
            creation_date = await get_date_from_metadata(metadata) or (datetime.fromtimestamp(int(ts)) if (ts := metadata.get('photoTakenTime', {}).get('timestamp')) else None)
            
            if not creation_date:
                display.add_log_and_advance("error", f"Brak daty w .json dla [dim]{filename}[/dim]")
                continue

            dest_dir = library_base_path / str(creation_date.year) / f"{creation_date.month:02d}"
            await asyncio.to_thread(dest_dir.mkdir, parents=True, exist_ok=True)
            final_path = create_unique_filepath(dest_dir, filename)
            
            dest_dir_str = f"{creation_date.year}/{creation_date.month:02d}"
            display.update_status(f"{op_text}: {filename} -> {dest_dir_str}")
            
            action_func = shutil.move if import_mode == 'move' else shutil.copy2
            await asyncio.to_thread(action_func, source_path, final_path)

            was_added = await add_local_file_entry(final_path, metadata)
            if was_added:
                display.add_log_and_advance("success", filename, f"Zapisano w: {dest_dir_str}", final_path.stat().st_size)
            else:
                await asyncio.to_thread(final_path.unlink)
                display.add_log_and_advance("error", filename, "Duplikat w bazie danych")

        except Exception as e:
            logger.error(f"Wystąpił błąd podczas importu pliku '{filename}': {e}", exc_info=True)
            display.add_log_and_advance("error", f"Błąd krytyczny dla {filename}", str(e))
