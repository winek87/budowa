# plik: core/downloader/page_navigator.py
# -*- coding: utf-8 -*-

import logging
import asyncio
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError
from rich.text import Text

# Zmieniamy ścieżkę importu, aby odzwierciedlić nową lokalizację
from ..config import (
    NAV_ARROW_RIGHT_SELECTOR,
    NAV_ARROW_LEFT_SELECTOR,
    NAV_REFRESH_ATTEMPTS,
    WAIT_FOR_SELECTOR,
    WAIT_FOR_PAGE_LOAD,
    NAV_BLIND_CLICK_ENABLED,
    DIRECTION_KEY
)

from ..utils import stop_event

logger = logging.getLogger(__name__)

async def unstoppable_navigate(page: Page, current_url: str, status_text: Text) -> bool:
    """
    Niezwykle 'uparta' funkcja do nawigacji, która próbuje wielu strategii.
    """
    if stop_event.is_set():
        return False

    selector = NAV_ARROW_RIGHT_SELECTOR if DIRECTION_KEY == "ArrowRight" else NAV_ARROW_LEFT_SELECTOR

    def update_status(msg: str):
        if status_text:
            status_text.plain = msg

    logger.info(f"Rozpoczynam próbę nawigacji z ...{current_url[-40:]}")

    try:
        update_status("Nawigacja: Próba standardowego kliknięcia...")
        await page.click(selector, timeout=WAIT_FOR_SELECTOR * 1000)
        await page.wait_for_url(lambda url: url != current_url, timeout=WAIT_FOR_SELECTOR * 1000)
        return True
    except PlaywrightTimeoutError:
        logger.warning("Strategia 1 (kliknięcie) nie powiodła się.")

    for attempt in range(1, NAV_REFRESH_ATTEMPTS + 1):
        if stop_event.is_set(): return False
        update_status(f"Nawigacja: Strategia odświeżania, próba {attempt}/{NAV_REFRESH_ATTEMPTS}...")
        try:
            await page.reload(wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
            await page.click(selector, timeout=WAIT_FOR_SELECTOR * 1000)
            await page.wait_for_url(lambda url: url != current_url, timeout=WAIT_FOR_SELECTOR * 1000)
            return True
        except PlaywrightTimeoutError:
            continue

    if NAV_BLIND_CLICK_ENABLED:
        if stop_event.is_set(): return False
        update_status("Nawigacja: Próba za pomocą klawiatury...")
        try:
            await page.keyboard.press(DIRECTION_KEY)
            await page.wait_for_url(lambda url: url != current_url, timeout=WAIT_FOR_SELECTOR * 1000)
            return True
        except PlaywrightTimeoutError:
            logger.warning("Strategia 3 (klawiatura) nie powiodła się.")

    update_status("Nawigacja: Ostateczna próba (pełne przeładowanie)...")
    try:
        await page.goto(current_url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
        await page.click(selector, timeout=WAIT_FOR_SELECTOR * 1000)
        await page.wait_for_url(lambda url: url != current_url, timeout=WAIT_FOR_SELECTOR * 1000)
        return True
    except (PlaywrightTimeoutError, Exception):
        logger.critical("Wszystkie strategie nawigacji zawiodły. Przerywam.")
        return False
