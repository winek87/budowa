# plik: core/downloader/file_processor.py

# -*- coding: utf-8 -*-

import asyncio
import logging
import shutil
from pathlib import Path
from typing import Dict, Tuple

# Zależności zewnętrzne (opcjonalne)
try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False

from playwright.async_api import Download, Page, TimeoutError as PlaywrightTimeoutError

# Zmieniamy ścieżkę importu, aby odzwierciedlić nową lokalizację
from ..config import (
    DOWNLOADS_DIR_BASE,
    FORCED_DUPLICATES_DIR,
    METADATA_STRATEGY,
    THREE_DOTS_MENU_SELECTOR,
    DOWNLOAD_OPTION_SELECTOR,
    WAIT_FOR_SELECTOR,
    WAIT_FOR_DOWNLOAD_START
)

from ..utils import create_unique_filepath, get_date_from_metadata

logger = logging.getLogger(__name__)


async def download_file_with_fallbacks(page: Page) -> Download:
    """
    Próbuje pobrać plik, używając skrótu klawiszowego z fallbackiem na menu.
    """
    try:
        logger.info("Próba pobrania za pomocą skrótu klawiszowego (Shift+D)...")
        async with page.expect_download(timeout=WAIT_FOR_DOWNLOAD_START * 1000) as download_info:
            await page.keyboard.press('Shift+D')
        return await download_info.value
    except PlaywrightTimeoutError:
        logger.warning("Pobieranie przez Shift+D nie powiodło się. Próbuję przez menu (fallback)...")
        async with page.expect_download(timeout=WAIT_FOR_DOWNLOAD_START * 1000) as download_info:
            await page.click(THREE_DOTS_MENU_SELECTOR, timeout=WAIT_FOR_SELECTOR * 1000)
            await page.click(DOWNLOAD_OPTION_SELECTOR, timeout=WAIT_FOR_SELECTOR * 1000)
        return await download_info.value


async def finalize_and_move_file(
    download: Download,
    metadata_from_page: Dict,
    scan_mode: str
) -> Tuple[Path, Dict]:
    """
    Finalizuje pobieranie, scala metadane, ustala ścieżkę i przenosi plik.
    """
    temp_path = Path(await download.path())
    filename_from_playwright = download.suggested_filename
    logger.info(f"Pobieranie zakończone. Nazwa sugerowana: '{filename_from_playwright}', ścieżka tymczasowa: {temp_path}")

    final_metadata = metadata_from_page.copy()

    if METADATA_STRATEGY != 'EXIF_ONLY' and final_metadata.get('FileName'):
        final_filename = final_metadata['FileName']
    else:
        final_filename = filename_from_playwright
    final_metadata['FileName'] = final_filename

    if final_metadata.get('expected_path'):
        final_path = Path(final_metadata['expected_path']).with_name(final_filename)
    else:
        if not EXIFTOOL_AVAILABLE:
            raise RuntimeError("Brak Exiftool jest krytyczny, gdy `expected_path` nie jest dostępne.")
        with exiftool.ExifToolHelper() as et:
            metadata_from_exif = et.get_metadata(str(temp_path))[0]
        final_metadata.update(metadata_from_exif)

        creation_date = await get_date_from_metadata(final_metadata)
        if not creation_date:
            raise ValueError("Nie udało się ustalić daty z żadnego źródła.")

        dest_dir = Path(DOWNLOADS_DIR_BASE) / str(creation_date.year) / f"{creation_date.month:02d}"
        final_path = dest_dir / final_filename

    dest_dir = final_path.parent
    await asyncio.to_thread(dest_dir.mkdir, parents=True, exist_ok=True)

    if scan_mode == 'forced' and await asyncio.to_thread(final_path.exists):
        dest_dir_duplicates = Path(FORCED_DUPLICATES_DIR)
        await asyncio.to_thread(dest_dir_duplicates.mkdir, parents=True, exist_ok=True)
        final_path = create_unique_filepath(dest_dir_duplicates, final_path.name)
    else:
        final_path = create_unique_filepath(dest_dir, final_path.name)

    await asyncio.to_thread(shutil.move, str(temp_path), str(final_path))
    logger.info(f"Przeniesiono plik do finalnej lokalizacji: {final_path}")

    return final_path, final_metadata
