# plik: core/downloader/page_processor.py
# Wersja 1.1 - Dodano przekazywanie funkcji callback do raportowania statusu.
# Opis: Ten moduł zawiera logikę odpowiedzialną za przetwarzanie pojedynczej strony,
#       teraz z możliwością raportowania szczegółowego postępu.
# -*- coding: utf-8 -*-

import asyncio
import re
import logging
from pathlib import Path
from typing import Dict, Callable

from playwright.async_api import Page

from ..config import METADATA_STRATEGY, WAIT_FOR_SELECTOR
from ..database import (
    add_google_photo_entry, get_retry_count_for_url
)
from ..scanner.online.page_parser import get_advanced_photo_details_from_page
from .file_processor import download_file_with_fallbacks, finalize_and_move_file

logger = logging.getLogger(__name__)

async def _confirm_page_loaded(page: Page, current_url: str, status_callback: Callable[[str], None]):
    """
    Wizualnie potwierdza załadowanie strony, raportując status.
    """
    status_callback("Weryfikacja: Oczekiwanie na załadowanie kluczowych elementów...")
    logger.info(f"Weryfikacja załadowania strony: ...{current_url[-40:]}")
    photo_id_match = re.search(r'(AF1Qip[\w-]+)', current_url)
    if not photo_id_match:
        raise ValueError("Nie można wyodrębnić ID zdjęcia z URL.")
    photo_id = photo_id_match.group(1)

    main_container_selector = f"[data-media-key='{photo_id}']"
    await page.wait_for_selector(main_container_selector, state='visible', timeout=WAIT_FOR_SELECTOR * 1000)
    await asyncio.sleep(1)
    status_callback("Weryfikacja: Strona załadowana poprawnie.")
    logger.info(f"Wizualne potwierdzenie dla zdjęcia {photo_id[:10]}... OK.")
    return photo_id

async def process_single_photo_page(
    page: Page,
    current_url: str,
    scan_mode: str,
    status_callback: Callable[[str], None]
) -> tuple[bool, str, dict]:
    """
    Główny orkiestrator, który wywołuje poszczególne kroki przetwarzania strony
    i przekazuje do nich funkcję zwrotną do raportowania statusu.
    """
    metadata_from_page = {}
    try:
        await _confirm_page_loaded(page, current_url, status_callback)

        if METADATA_STRATEGY in ['HYBRID', 'ONLINE_ONLY']:
            status_callback("Skanowanie: Pobieranie metadanych ze strony (scraping)...")
            metadata_from_page = await get_advanced_photo_details_from_page(page, current_url) or {}

        # Przekazujemy callback do funkcji pobierającej
        download = await download_file_with_fallbacks(page, status_callback)
        if await download.failure():
            raise IOError(f"Pobieranie nie powiodło się: {await download.failure()}")

        # === POCZĄTEK POPRAWKI: Logika dla trybu profilowania ===
        if scan_mode == 'profiling':
            status_callback("Tryb profilowania: Pomijanie zapisu i przenoszenia...")
            temp_path = await download.path()
            if temp_path:
                try:
                    # Próbujemy usunąć plik tymczasowy
                    await asyncio.to_thread(Path(temp_path).unlink)
                except Exception:
                    pass # Ignorujemy błędy, jeśli się nie uda
            return True, "profiling_success", metadata_from_page
        # === KONIEC POPRAWKI ===

        # Przekazujemy callback do funkcji finalizującej
        status_callback("Finalizacja pliku...")
        final_path, final_metadata = await finalize_and_move_file(
            download, metadata_from_page, scan_mode, status_callback
        )

        status_callback("Zapis: Aktualizowanie bazy danych...")
        final_metadata['final_path'] = str(final_path)
        try:
            final_metadata['size'] = (await asyncio.to_thread(final_path.stat)).st_size
        except (OSError, FileNotFoundError):
            pass
        
        expected_path_str = str(final_path.parent / final_metadata['FileName'])
        await add_google_photo_entry(
            url=current_url, filename=final_path.name, final_path=str(final_path),
            metadata=final_metadata, status='downloaded', retry_count=0,
            expected_path=expected_path_str, processing_status='Sukces'
        )
        return True, "downloaded", final_metadata

    except Exception as e:
        status_callback(f"Błąd: {type(e).__name__}")
        logger.error(f"Przetwarzanie strony ...{current_url[-40:]} nie powiodło się. Błąd: {e}", exc_info=False)
        
        status_to_save = 'skipped' if metadata_from_page else 'failed'
        retry_count = 0 if status_to_save == 'skipped' else (await get_retry_count_for_url(current_url)) + 1
        error_metadata = metadata_from_page.copy(); error_metadata['error_message'] = str(e)
        
        await add_google_photo_entry(
            url=current_url, filename="processing_failed", final_path=None,
            metadata=error_metadata, status=status_to_save, retry_count=retry_count,
            expected_path=None, processing_status='Błąd'
        )
        return status_to_save == 'skipped', status_to_save, error_metadata
