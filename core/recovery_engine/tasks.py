# plik: core/recovery_engine/tasks.py
# Wersja 1.0 - Czysta logika biznesowa dla Silnika Ratunkowego.
# Opis: Ten moduł zawiera funkcje odpowiedzialne za prostą strategię
#       pobierania plików "na siłę" i ich analizę lokalną.
# -*- coding: utf-8 -*-

import asyncio
import logging
import shutil
from pathlib import Path

# Zależności zewnętrzne
try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

# Importy z modułów projektu `core`
from ..config import WAIT_FOR_PAGE_LOAD, WAIT_FOR_DOWNLOAD_START, DOWNLOADS_DIR_BASE
from ..database import add_google_photo_entry
from ..utils import get_date_from_metadata, create_unique_filepath

# Importy z wewnętrznych modułów pakietu
from .ui.live_display import RecoveryEngineLiveDisplay

# Inicjalizacja
logger = logging.getLogger(__name__)

async def process_single_url_recovery(page: Page, url: str, display: RecoveryEngineLiveDisplay) -> bool:
    """
    Wykonuje prostą sekwencję ratunkową dla pojedynczego URL-a.

    Args:
        page (Page): Instancja strony Playwright.
        url (str): Adres URL do przetworzenia.
        display (RecoveryEngineLiveDisplay): Obiekt dashboardu do raportowania postępu.

    Returns:
        bool: True, jeśli operacja zakończyła się sukcesem, w przeciwnym razie False.
    """
    temp_path: Path | None = None
    try:
        display.update_status(f"Nawigacja do: ...{url[-40:]}")
        await page.goto(url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
        await asyncio.sleep(2) # Dodatkowa pauza na załadowanie

        display.update_status(f"Próba pobrania pliku...")
        async with page.expect_download(timeout=WAIT_FOR_DOWNLOAD_START * 1000) as download_info:
            await page.keyboard.press('Shift+D')
        download = await download_info.value

        if failure_reason := await download.failure():
            raise IOError(f"Pobieranie nie powiodło się: {failure_reason}")

        display.update_status("Analiza pliku lokalnie (ExifTool)...")
        temp_path = Path(await download.path())
        original_filename = download.suggested_filename
        
        if not EXIFTOOL_AVAILABLE:
            raise RuntimeError("Krytyczny błąd: biblioteka 'pyexiftool' jest niedostępna.")

        with exiftool.ExifToolHelper() as et:
            loop = asyncio.get_running_loop()
            metadata_list = await loop.run_in_executor(None, et.get_metadata, str(temp_path))

        if not metadata_list: raise ValueError("Nie udało się odczytać metadanych Exif.")
        metadata = metadata_list[0]

        display.update_status("Ustalanie daty i miejsca zapisu...")
        creation_date = await get_date_from_metadata(metadata)
        if not creation_date: raise ValueError("Nie udało się odczytać daty z metadanych pliku.")

        dest_dir = Path(DOWNLOADS_DIR_BASE) / str(creation_date.year) / f"{creation_date.month:02d}"
        await asyncio.to_thread(dest_dir.mkdir, parents=True, exist_ok=True)

        final_path = create_unique_filepath(dest_dir, original_filename)
        await asyncio.to_thread(shutil.move, str(temp_path), str(final_path))
        
        display.update_status("Zapisywanie do bazy danych...")
        await add_google_photo_entry(url, final_path.name, str(final_path), metadata, 'downloaded', 0, None, 'Sukces')
        
        logger.info(f"Sukces: Pomyślnie 'uratowano' plik: {final_path.name}")
        return True

    except Exception as e:
        logger.error(f"BŁĄD dla ...{url[-40:]}: {str(e)[:150]}...", exc_info=False)
        return False
    finally:
        if temp_path and await asyncio.to_thread(temp_path.exists):
            try:
                await asyncio.to_thread(temp_path.unlink)
            except OSError as e:
                logger.warning(f"Nie udało się usunąć pliku tymczasowego {temp_path}: {e}")
