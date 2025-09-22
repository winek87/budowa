# plik: core/takeout_url_processor/tasks.py
# Wersja 1.0 - Czysta logika biznesowa dla Naprawy z URL-i Takeout.
# Opis: Ten moduł zawiera funkcje odpowiedzialne za pobieranie URL-i
#       z bazy i ich przetwarzanie za pomocą głównego silnika.
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import List

from playwright.async_api import Page

# Importy z modułów projektu `core`
from ..config import DB_TYPE
from ..database import get_db_connection
from ..downloader.page_processor import process_single_photo_page

# Inicjalizacja
logger = logging.getLogger(__name__)


async def get_urls_to_process_from_db() -> List[str]:
    """
    Pobiera z bazy listę URL-i z Takeout dla plików, które wymagają naprawy.
    """
    logger.info("Pobieram z bazy listę URL-i z Takeout do naprawy...")
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cursor:
                # Zapytanie jest uniwersalne dla SQLite i MariaDB
                query = """
                    SELECT google_photos_url FROM downloaded_media
                    WHERE status IN ('failed', 'skipped')
                    AND google_photos_url IS NOT NULL
                    AND google_photos_url != ''
                """
                await cursor.execute(query)
                # aiomysql zwraca krotki krotek, aiosqlite listę krotek
                rows = await cursor.fetchall()
                urls = [row[0] for row in rows]
                
                logger.info(f"Znaleziono {len(urls)} URL-i do naprawy.")
                return urls
    except Exception as e:
        logger.error(f"Błąd podczas pobierania URL-i do naprawy: {e}", exc_info=True)
        return []


async def process_single_url_for_repair(page: Page, url: str) -> bool:
    """
    Wywołuje główny procesor `process_single_photo_page` dla danego URL-a.

    Args:
        page (Page): Instancja strony Playwright.
        url (str): Adres URL do przetworzenia.

    Returns:
        bool: True, jeśli operacja zakończyła się sukcesem.
    """
    # Prosty callback, który loguje postęp, ale nie wyświetla go na UI
    # (główny dashboard ma swój własny, nadrzędny status)
    status_callback = lambda msg: logger.debug(f"Status naprawy dla {url[-20:]}: {msg}")

    # Wywołujemy główny procesor z silnika Downloader
    success, status, metadata = await process_single_photo_page(
        page, url, 'main', status_callback
    )

    if success:
        logger.info(f"Pomyślnie przetworzono URL: {url} (status: {status})")
    else:
        logger.warning(f"Nie udało się przetworzyć URL: {url} (status: {status})")
        
    return success
