# plik: core/scanner/online/playwright_manager.py
# Wersja 1.0 - Dedykowany menedżer dla Playwright (refaktoryzacja)
# Opis: Ten moduł zawiera klasę PlaywrightManager, która hermetyzuje
#       cykl życia przeglądarki (uruchamianie, zamykanie, tworzenie stron).
#       Działa jako asynchroniczny menedżer kontekstu dla bezpiecznego
#       i czystego zarządzania zasobami.
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from contextlib import asynccontextmanager

# --- Zależności zewnętrzne ---
from playwright.async_api import async_playwright, BrowserContext, Page, Playwright

# --- Importy z modułów projektu `core` ---
from ...config import (
    SESSION_DIR,
    BROWSER_TYPE,
    BROWSER_ARGS,
    ENABLE_RESOURCE_BLOCKING,
    BLOCKED_RESOURCE_TYPES
)

# --- Inicjalizacja ---
logger = logging.getLogger(__name__)

async def _block_unwanted_resources(route):
    """Przechwytuje i opcjonalnie blokuje żądania sieciowe strony."""
    if route.request.resource_type in BLOCKED_RESOURCE_TYPES:
        await route.abort()
    else:
        await route.continue_()

class PlaywrightManager:
    """
    Asynchroniczny menedżer kontekstu do zarządzania instancją Playwright.
    """
    def __init__(self, headless_mode: bool):
        self.headless = headless_mode
        self._playwright: Playwright | None = None
        self._browser: BrowserContext | None = None
        logger.info(f"Inicjalizacja menedżera Playwright (tryb headless: {self.headless}).")

    async def __aenter__(self) -> 'PlaywrightManager':
        """Uruchamia Playwright i przeglądarkę przy wejściu do bloku `async with`."""
        try:
            logger.debug("Uruchamianie instancji Playwright...")
            self._playwright = await async_playwright().start()
            logger.debug(f"Uruchamianie przeglądarki '{BROWSER_TYPE}' z kontekstem sesji...")
            self._browser = await getattr(self._playwright, BROWSER_TYPE).launch_persistent_context(
                Path(SESSION_DIR).expanduser(),
                headless=self.headless,
                args=BROWSER_ARGS.get(BROWSER_TYPE, [])
            )
            return self
        except Exception as e:
            logger.critical(f"Nie udało się uruchomić Playwright: {e}", exc_info=True)
            # Jeśli wystąpi błąd, upewnij się, że zasoby są zwolnione
            await self.__aexit__(None, None, None)
            raise # Rzuć błąd dalej, aby przerwać operację

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Zamyka przeglądarkę i Playwright przy wyjściu z bloku `async with`."""
        logger.debug("Zamykanie zasobów Playwright...")
        if self._browser:
            await self._browser.close()
            logger.info("Kontekst przeglądarki został zamknięty.")
        if self._playwright:
            await self._playwright.stop()
            logger.info("Instancja Playwright została zatrzymana.")

    async def new_page(self) -> Page:
        """Tworzy, konfiguruje i zwraca nową stronę (kartę) w przeglądarce."""
        if not self._browser:
            raise RuntimeError("Przeglądarka nie została zainicjowana. Użyj menedżera w bloku `async with`.")
        
        page = await self._browser.new_page()
        logger.debug("Utworzono nową stronę w przeglądarce.")

        if ENABLE_RESOURCE_BLOCKING:
            logger.info("Blokowanie zasobów sieciowych jest WŁĄCZONE.")
            await page.route("**/*", _block_unwanted_resources)
        
        return page
