# plik: core/local_scanner/tasks.py
# Wersja 1.0 - Czysta logika biznesowa dla Lokalnego Importera.
# Opis: Ten moduł zawiera funkcje odpowiedzialne za skanowanie folderów,
#       przetwarzanie plików i interakcję z bazą danych.
# -*- coding: utf-8 -*-

import asyncio
import logging
import shutil
from pathlib import Path
from datetime import datetime
from typing import List

# Zależności zewnętrzne
try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False

# Importy z modułów projektu `core`
from ..config import DOWNLOADS_DIR_BASE
from ..database import add_local_file_entry
from ..utils import get_date_from_metadata, create_unique_filepath

# Importy z wewnętrznych modułów pakietu
from .ui.live_display import LocalScannerLiveDisplay

# Inicjalizacja
logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {
    '.jpg', '.jpeg', '.png', '.heic', '.gif', '.webp', '.bmp',
    '.mp4', '.mov', '.avi', '.m4v', '.3gp', '.mkv'
}

def find_supported_files(folder_path: Path) -> List[Path]:
    """Przeszukuje rekursywnie folder w poszukiwaniu obsługiwanych plików."""
    logger.info(f"Rozpoczynam wyszukiwanie plików w '{folder_path}'...")
    return [p for p in folder_path.rglob('*') if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS]

async def process_folder(folder_path: Path, import_mode: str, display: LocalScannerLiveDisplay):
    """
    Główna funkcja robocza: skanuje pliki, a następnie indeksuje je lub importuje,
    raportując postęp do obiektu LiveDisplay.

    Args:
        folder_path (Path): Ścieżka do folderu, który ma być przetworzony.
        import_mode (str): Tryb pracy ('copy' lub 'index').
        display (LocalScannerLiveDisplay): Obiekt dashboardu do raportowania postępu.
    """
    files_to_process = await asyncio.to_thread(find_supported_files, folder_path)

    if not files_to_process:
        logger.warning(f"Nie znaleziono obsługiwanych plików w '{folder_path}'.")
        # Zakończymy cicho, menu poinformuje użytkownika.
        return

    # Aktualizujemy total w pasku postępu, teraz gdy znamy liczbę plików
    display.progress_bar.update(display._task_id, total=len(files_to_process), description="Przetwarzanie...")
    
    library_base_path = Path(DOWNLOADS_DIR_BASE)
    loop = asyncio.get_running_loop()

    try:
        with exiftool.ExifToolHelper() as et:
            for source_path in files_to_process:
                try:
                    metadata_list = await loop.run_in_executor(None, et.get_metadata, str(source_path))
                    if not metadata_list:
                        display.update_progress('błędy', source_path)
                        continue
                    metadata = metadata_list[0]

                    final_path = source_path  # Domyślnie dla trybu 'index'
                    file_size = await asyncio.to_thread(source_path.stat)
                    file_size = file_size.st_size
                    
                    if import_mode == 'copy':
                        creation_date = await get_date_from_metadata(metadata)
                        if not creation_date:
                            logger.warning(f"Brak daty dla pliku {source_path.name}. Używam daty modyfikacji pliku.")
                            mtime = await asyncio.to_thread(source_path.stat)
                            creation_date = datetime.fromtimestamp(mtime.st_mtime)

                        dest_dir = library_base_path / str(creation_date.year) / f"{creation_date.month:02d}"
                        await asyncio.to_thread(dest_dir.mkdir, parents=True, exist_ok=True)
                        final_path = create_unique_filepath(dest_dir, source_path.name)
                        await asyncio.to_thread(shutil.copy2, source_path, final_path)

                    was_added = await add_local_file_entry(final_path, metadata)
                    
                    if was_added:
                        display.update_progress('zaimportowane', final_path, file_size)
                    else:
                        display.update_progress('pominięte', source_path, file_size)
                        if import_mode == 'copy':
                            # Jeśli plik został skopiowany, ale ostatecznie pominięty (duplikat), usuwamy kopię
                            await asyncio.to_thread(final_path.unlink)

                except Exception as e:
                    logger.error(f"Błąd przetwarzania pliku {source_path.name}: {e}", exc_info=True)
                    display.update_progress('błędy', source_path)

    except FileNotFoundError:
        logger.critical("Nie znaleziono programu 'exiftool'. Operacja przerwana.")
        # Wyjątek zostanie przechwycony przez pętlę nadrzędną w cli.py
        raise
