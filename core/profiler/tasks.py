# plik: core/profiler/tasks.py
# Wersja 1.3 - Naprawiono błąd AttributeError przez poprawne przekazanie callbacku.
# Opis: Ten moduł zawiera klasę PerformanceProfiler, która zarządza
#       całym procesem mierzenia wydajności kluczowych komponentów
#       silnika pobierającego, w tym uruchamianiem operacji i zbieraniem wyników.
# -*- coding: utf-8 -*-

import asyncio
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Callable

from playwright.async_api import Page

# Importy z modułów projektu `core`
from ..config import START_URL, WAIT_FOR_PAGE_LOAD, DIRECTION_KEY, WAIT_FOR_SELECTOR
from ..database import get_state
from ..downloader.page_navigator import unstoppable_navigate
from ..downloader.page_processor import process_single_photo_page
from .ui.live_display import ProfilerLiveDisplay

# Inicjalizacja
logger = logging.getLogger(__name__)


class PerformanceProfiler:
    """
    Klasa zarządzająca procesem profilowania wydajności silnika.
    Mierzy czas, przechowuje wyniki, oblicza statystyki i generuje raport.
    """
    def __init__(self, page: Page, num_samples: int):
        """
        Inicjalizuje profiler.

        Args:
            page (Page): Obiekt strony Playwright, na której będą wykonywane operacje.
            num_samples (int): Liczba próbek (zdjęć/filmów) do przetworzenia w teście.
        """
        self.page = page
        self.num_samples = num_samples
        self.timings = defaultdict(list)
        self.display: ProfilerLiveDisplay | None = None
        logger.debug(f"Profiler zainicjalizowany dla {num_samples} próbek.")

    async def _measure(self, key: str, async_func: Callable, *args, **kwargs) -> any:
        """
        Mierzy czas wykonania podanej funkcji asynchronicznej i raportuje do dashboardu.

        Args:
            key (str): Nazwa operacji do zmierzenia (np. "Nawigacja").
            async_func (Callable): Asynchroniczna funkcja do wykonania.
            *args, **kwargs: Argumenty do przekazania do `async_func`.

        Returns:
            any: Wynik zwrócony przez wykonaną funkcję `async_func`.
        """
        if self.display:
            self.display.update_status(f"Mierzenie: [bold]{key}[/]...")
        
        start_time = time.monotonic()
        result = await async_func(*args, **kwargs)
        duration = time.monotonic() - start_time
        
        self.timings[key].append(duration)
        if self.display:
            self.display.update_timing(key, duration)
            
        logger.info(f"Zmierzono '{key}': {duration:.3f}s")
        return result

    def get_stats(self) -> dict:
        """
        Oblicza finalne statystyki (średnia, min, max, suma) dla zebranych czasów.

        Returns:
            dict: Słownik ze statystykami dla każdej zmierzonej operacji.
        """
        stats = {}
        for key, times in self.timings.items():
            if not times: continue
            stats[key] = {
                "count": len(times),
                "total": sum(times),
                "avg": sum(times) / len(times),
                "min": min(times),
                "max": max(times),
            }
        return stats

    async def run_profile(self, mode: str, display: ProfilerLiveDisplay):
        """
        Uruchamia główną pętlę profilowania w wybranym trybie.

        Args:
            mode (str): Tryb profilowania ('full_cycle' lub 'processing_only').
            display (ProfilerLiveDisplay): Obiekt dashboardu do raportowania postępu.
        """
        self.display = display
        start_url = await get_state('last_scan_url') or START_URL
        if not start_url:
            raise RuntimeError("Brak startowego URL. Uruchom najpierw skanowanie, aby zapisać postęp.")

        current_url = start_url

        for i in range(self.num_samples):
            display.update_status(f"Próbka {i+1}/{self.num_samples}: Nawigacja do strony...")
            await self.page.goto(current_url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
            page_before_nav = self.page.url

            # Mierzymy czas przetwarzania, przekazując specjalny tryb 'profiling'
            # oraz metodę `display.update_status` jako funkcję callback.
            await self._measure(
                "Przetwarzanie (Skan+Pobieranie)",
                process_single_photo_page,
                self.page, 
                page_before_nav, 
                "profiling", 
                display.update_status
            )

            if mode == 'full_cycle':
                # W trybie pełnego cyklu, mierzymy również czas nawigacji
                nav_success = await self._measure(
                    "Nawigacja (Strzałka)",
                    unstoppable_navigate,
                    self.page, page_before_nav, display.update_status
                )
                if not nav_success:
                    logger.error("Nawigacja nie powiodła się. Przerywam profilowanie.")
                    break
                current_url = self.page.url
            else: # tryb 'processing_only'
                display.update_status(f"Próbka {i+1}/{self.num_samples}: Przejście do następnego...")
                await self.page.keyboard.press(DIRECTION_KEY)
                await self.page.wait_for_url(lambda url: url != page_before_nav, timeout=WAIT_FOR_SELECTOR * 1000)
                current_url = self.page.url

            display.advance()
