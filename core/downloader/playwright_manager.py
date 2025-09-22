# plik: core/downloader/playwright_manager.py
# Wersja 1.0 - Menedżer Playwright dla modułu Downloader.
# Opis: Ten moduł zawiera funkcje pomocnicze związane z konfiguracją
#       i zarządzaniem Playwright w kontekście pobierania.
# -*- coding: utf-8 -*-

from ..config import BLOCKED_RESOURCE_TYPES

async def block_unwanted_resources(route):
    """
    Przechwytuje i opcjonalnie blokuje żądania sieciowe strony.
    """
    if route.request.resource_type in BLOCKED_RESOURCE_TYPES:
        await route.abort()
    else:
        await route.continue_()
