# plik: core/interceptor/tasks.py
# Wersja 1.0 - Czysta logika biznesowa dla Podsłuchu Sieciowego.
# Opis: Ten moduł zawiera klasę Interceptor, która zarządza procesem
#       nasłuchiwania i przechwytywania odpowiedzi sieciowych.
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Callable

from playwright.async_api import Page, Response

# Inicjalizacja
logger = logging.getLogger(__name__)

class Interceptor:
    """Zarządza procesem przechwytywania odpowiedzi sieciowych."""

    def __init__(self, page: Page, update_callback: Callable):
        """
        Inicjalizuje interceptor.

        Args:
            page (Page): Obiekt strony Playwright do nasłuchiwania.
            update_callback (Callable): Funkcja zwrotna wywoływana po
                                        przechwyceniu nowych danych.
        """
        self.page = page
        self.captured_data: List[Dict[str, Any]] = []
        self.update_callback = update_callback

    async def _handle_response(self, response: Response):
        """
        Funkcja wywoływana dla każdej odpowiedzi z serwera.
        Sprawdza, czy jest to JSON i zapisuje dane.
        """
        if "application/json" in response.headers.get("content-type", "").lower():
            try:
                data = await response.json()
                short_url = response.url.split('?')[0]
                size_kb = len(json.dumps(data)) / 1024
                
                # Zapisujemy pełne dane i skrócone info do przekazania
                full_data = {"url": response.url, "json_data": data}
                display_info = {"name": Path(short_url).name, "size_kb": size_kb}
                
                self.captured_data.append(full_data)
                self.update_callback(display_info) # Informujemy dashboard o nowym znalezisku

            except (json.JSONDecodeError, Exception) as e:
                logger.debug(f"Błąd podczas obsługi odpowiedzi z {response.url}: {e}")

    async def start_listening(self):
        """Rozpoczyna nasłuchiwanie na odpowiedzi sieciowe."""
        self.page.on("response", self._handle_response)
        logger.info("Interceptor rozpoczął nasłuchiwanie.")

    async def stop_listening(self):
        """Kończy nasłuchiwanie na odpowiedzi sieciowe."""
        self.page.remove_listener("response", self._handle_response)
        logger.info("Interceptor zakończył nasłuchiwanie.")
