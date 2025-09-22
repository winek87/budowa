# plik: core/scanner/online/page_parser.py
# Wersja 1.2 - Poprawiono błąd TypeError w wywołaniu `is_visible`
# Opis: Ten moduł zawiera funkcję odpowiedzialną za analizę (scraping)
#       strony pojedynczego zdjęcia w Google Photos i ekstrakcję metadanych.
# -*- coding: utf-8 -*-

import asyncio
import json
import re
import logging
from datetime import datetime
from pathlib import Path

# --- Zależności zewnętrzne ---
from playwright.async_api import Page

# --- Importy z modułów projektu `core` ---
from ...config import (
    DOWNLOADS_DIR_BASE,
    WAIT_FOR_SELECTOR,
    WAIT_FOR_PAGE_LOAD,
    INFO_PANEL_BUTTON_SELECTOR,
)

# --- Inicjalizacja ---
logger = logging.getLogger(__name__)

# Mapa polskich skrótów miesięcy na numery
MONTHS_MAP = {
    'sty': 1, 'lut': 2, 'mar': 3, 'kwi': 4, 'maj': 5, 'cze': 6,
    'lip': 7, 'sie': 8, 'wrz': 9, 'paź': 10, 'lis': 11, 'gru': 12
}

async def get_advanced_photo_details_from_page(page: Page, current_url: str) -> dict | None:
    """
    Skaner Online: Pobiera wszystkie zaawansowane metadane ze strony zdjęcia
    i oblicza OCZEKIWANĄ ścieżkę zapisu (`expected_path`).
    """

    async def get_attribute_safely(locator, attribute='aria-label'):
        """Bezpiecznie pobiera atrybut, aby uniknąć błędów."""
        try:
            return await locator.get_attribute(attribute, timeout=1000)
        except Exception:
            return None

    async def _scan_page_content():
        """Wykonuje pojedynczą próbę skanowania zawartości strony."""
        logger.debug(f"Rozpoczynam skanowanie zawartości strony dla URL: ...{current_url[-40:]}")
        scan_results = {}

        photo_id_match = re.search(r'AF1Qip[\w-]+', current_url)
        if not photo_id_match:
            logger.warning(f"Nie udało się wyodrębnić ID zdjęcia z URL: {current_url}")
            return None
        photo_id = photo_id_match.group(0)

        info_panel_selector = f"c-wiz[jslog*='{photo_id}']"
        
        # === POCZĄTEK POPRAWKI: Usunięto `timeout` z `is_visible` ===
        if not await page.is_visible(info_panel_selector):
            panel_opened = False
            
            # Metoda 1: Kliknięcie przycisku
            try:
                logger.debug("Panel informacji jest zamknięty. Próba 1: Kliknięcie przycisku.")
                await page.click(INFO_PANEL_BUTTON_SELECTOR, timeout=WAIT_FOR_SELECTOR * 1000)
                await page.wait_for_selector(info_panel_selector, state="visible", timeout=WAIT_FOR_SELECTOR * 1000)
                logger.info("Panel boczny otwarty za pomocą kliknięcia.")
                panel_opened = True
            except Exception as e:
                logger.warning(f"Metoda 1 (kliknięcie) nie powiodła się: {type(e).__name__}. Próbuję metody 2 (klawisz 'i').")

            # Metoda 2: Skrót klawiszowy 'i'
            if not panel_opened:
                try:
                    logger.debug("Próba 2: Wciśnięcie klawisza 'i'.")
                    await page.keyboard.press('i')
                    await page.wait_for_selector(info_panel_selector, state="visible", timeout=WAIT_FOR_SELECTOR * 1000)
                    logger.info("Panel boczny otwarty za pomocą skrótu klawiszowego.")
                    panel_opened = True
                except Exception as e:
                    logger.error(f"Wszystkie metody otwarcia panelu zawiodły. Błąd metody 2: {type(e).__name__}. Metadane mogą być niekompletne.")
        else:
            logger.debug("Panel informacji był już otwarty.")
        # === KONIEC POPRAWKI ===

        wiz_element = page.locator(info_panel_selector).first

        # Reszta funkcji pozostaje bez zmian
        date_from_panel, date_from_aria = None, None
        date_text_locator = wiz_element.locator(".R9U8ab")
        if await date_text_locator.count() > 0:
            date_text = await date_text_locator.first.inner_text()
            match = re.search(r'(\d{1,2})\s+([a-zA-Z]{3})\s+(\d{4}),\s+(\d{2}:\d{2})', date_text)
            if match:
                day, month_str, year, time_str = match.groups()
                if month := MONTHS_MAP.get(month_str.lower()):
                    hour, minute = map(int, time_str.split(':'))
                    date_from_panel = datetime(int(year), month, int(day), hour, minute)
                    scan_results["DateTime_Panel"] = date_from_panel.isoformat()
                    logger.debug(f"Znaleziono datę w panelu bocznym: {date_from_panel.isoformat()}")

        main_image_locator = page.locator("img.BiCYpc[aria-label]")
        if await main_image_locator.count() > 0:
            aria_label = await main_image_locator.first.get_attribute('aria-label')
            if aria_label:
                match_aria = re.search(r'(\d{1,2})\s+([a-zA-Z]{3})\s+(\d{4}),\s+(\d{2}:\d{2}:\d{2})', aria_label, re.IGNORECASE)
                if match_aria:
                    day, month_str, year, time_str = match_aria.groups()
                    if month := MONTHS_MAP.get(month_str.lower()):
                        h, m, s = map(int, time_str.split(':'))
                        date_from_aria = datetime(int(year), month, int(day), h, m, s)
                        scan_results["DateTime_AriaLabel"] = date_from_aria.isoformat()
                        logger.debug(f"Znaleziono datę w atrybucie aria-label: {date_from_aria.isoformat()}")

        if date_from_panel:
            scan_results["DateTime"] = date_from_panel.isoformat()
            scan_results["DateTimeSource"] = "Panel Boczny"
        elif date_from_aria:
            scan_results["DateTime"] = date_from_aria.isoformat()
            scan_results["DateTimeSource"] = "Atrybut 'aria-label'"

        pairs = {
            "FileName": "div.R9U8ab[aria-label^='Nazwa pliku:']", "Camera": "div.R9U8ab[aria-label^='Nazwa aparatu:']",
            "Location": "div.R9U8ab[aria-label='Lokalizacja']", "Dimensions": "span[aria-label^='Rozmiar w pikselach']",
            "FileSize": "span[aria-label^='Rozmiar pliku:']"
        }
        for key, selector in pairs.items():
            locator = wiz_element.locator(selector).first
            if await locator.count() > 0:
                if aria := await get_attribute_safely(locator):
                    value = aria.split(":", 1)[-1].strip() if ':' in aria else aria
                    scan_results[key] = value

        if description := await wiz_element.locator("textarea[aria-label='Opis']").input_value():
            scan_results["Description"] = description.strip()

        people_locators = await wiz_element.locator("a[aria-label^='Na zdjęciu:']").all()
        if tagged_people := [await get_attribute_safely(loc) for loc in people_locators]:
            scan_results["TaggedPeople"] = [p.replace("Na zdjęciu:", "").strip() for p in tagged_people if p]

        albums_section = wiz_element.locator("div.KlIBpb:has-text('Albumy')")
        if await albums_section.count() > 0:
            album_locators = await albums_section.locator("div.AJM7gb").all()
            scan_results["Albums"] = [await loc.inner_text() for loc in album_locators]

        experimental_details = {}
        map_link_locator = wiz_element.locator("a.cFLCHe")
        if await map_link_locator.count() > 0:
            href = await map_link_locator.get_attribute('href')
            if href and (gps_match := re.search(r'(-?\d+\.\d+),(-?\d+\.\d+)', href)):
                experimental_details["GPS_Coords"] = {"latitude": float(gps_match.group(1)), "longitude": float(gps_match.group(2))}

        if experimental_details:
            scan_results["Experimental_Details"] = experimental_details

        return scan_results

    try:
        details = await _scan_page_content()
        if details is not None and "DateTime" not in details:
            logger.warning(f"Brak kluczowej daty dla ...{current_url[-40:]}. Odświeżam i próbuję ponownie.")
            await page.reload(wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
            await asyncio.sleep(2)
            details = await _scan_page_content()

        if details is None: return None

        if "DateTime" in details and "FileName" in details:
            try:
                dt = datetime.fromisoformat(details["DateTime"])
                dest_dir = Path(DOWNLOADS_DIR_BASE) / str(dt.year) / f"{dt.month:02d}"
                details['expected_path'] = str(dest_dir / details['FileName'])
            except (ValueError, TypeError): details['expected_path'] = None
        else: details['expected_path'] = None

        logger.info(f"Skanowanie online dla ...{current_url[-40:]} zakończone pomyślnie.")
        return details

    except Exception as e:
        logger.error(f"Krytyczny błąd podczas analizy strony {current_url}: {e}", exc_info=True)
        return None
