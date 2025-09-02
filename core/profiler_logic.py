# -*- coding: utf-8 -*-

# plik: core/profiler_logic.py
# Wersja 4.0 - W pełni asynchroniczny i zintegrowany z nowymi modułami
#
# ##############################################################################
# ===                   MODUŁ PROFILERA WYDAJNOŚCI                           ===
# ##############################################################################
#
# "Profiler Wydajności" to specjalistyczne narzędzie deweloperskie do
# mierzenia i analizowania szybkości działania kluczowych komponentów
# głównego silnika pobierającego.
#
# Jego zadaniem jest uruchomienie silnika "Master" na krótkiej, zdefiniowanej
# przez użytkownika próbce danych i precyzyjne zmierzenie czasu wykonania
# każdej operacji. Wyniki są prezentowane w czytelnej tabeli.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import time
import logging
from pathlib import Path
from collections import defaultdict

# --- Playwright ---
from playwright.async_api import async_playwright, Page

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.text import Text

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import (
    START_URL, WAIT_FOR_PAGE_LOAD, DIRECTION_KEY, BROWSER_TYPE,
    SESSION_DIR, BROWSER_ARGS, DOWNLOADS_DIR_BASE, WAIT_FOR_SELECTOR
)
from .database import get_state
from .master_logic import unstoppable_navigate, process_single_photo_page
from .utils import create_interactive_menu

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


# ##############################################################################
# ===                     SEKCJA 1: GŁÓWNA KLASA PROFILERA                     ===
# ##############################################################################

class PerformanceProfiler:
    """
    Klasa zarządzająca całym procesem profilowania wydajności silnika.

    Mierzy czas wykonania kluczowych operacji asynchronicznych, przechowuje
    wyniki, oblicza statystyki (średnia, min, max, suma) i generuje
    szczegółowy raport końcowy w formie tabeli.
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
        logger.debug(f"Profiler zainicjalizowany dla {num_samples} próbek.")

    async def _measure(self, key: str, async_func, *args, **kwargs) -> any:
        """
        Mierzy czas wykonania podanej funkcji asynchronicznej i zapisuje wynik.

        Jest to "opakowująca" metoda, która uruchamia przekazaną funkcję,
        mierzy czas jej wykonania i przechowuje wynik w słowniku `self.timings`.

        Args:
            key (str): Nazwa operacji do zmierzenia (np. "Nawigacja").
            async_func: Asynchroniczna funkcja do wykonania i zmierzenia.
            *args: Argumenty pozycyjne do przekazania do `async_func`.
            **kwargs: Argumenty kluczowe do przekazania do `async_func`.

        Returns:
            any: Wynik zwrócony przez wykonaną funkcję `async_func`.
        """
        logger.debug(f"Rozpoczynam pomiar dla operacji: '{key}'...")
        start_time = time.monotonic()
        result = await async_func(*args, **kwargs)
        duration = time.monotonic() - start_time
        self.timings[key].append(duration)
        logger.info(f"Zmierzono '{key}': [bold cyan]{duration:.3f}s[/bold cyan]", extra={"markup": True})
        return result

    def _calculate_stats(self) -> dict:
        """
        Oblicza statystyki (średnia, min, max, suma) dla zebranych czasów.

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

    def _display_report(self):
        """
        Wyświetla finalny raport wydajności w formie estetycznej tabeli Rich.
        """
        console.print(Panel(f"📊 Raport Wydajności dla {self.num_samples} próbek 📊", style="bold green"))
        stats = self._calculate_stats()

        table = Table(title="Wyniki Profilowania", show_lines=True)
        table.add_column("Operacja", style="cyan", width=35)
        table.add_column("L. Wywołań", style="magenta", justify="right")
        table.add_column("Łączny Czas (s)", style="yellow", justify="right")
        table.add_column("Średni Czas (s)", style="green", justify="right")
        table.add_column("Min Czas (s)", style="dim", justify="right")
        table.add_column("Max Czas (s)", style="dim", justify="right")

        total_time = 0
        for key, data in sorted(stats.items()):
            table.add_row(
                key, str(data["count"]), f"{data['total']:.2f}",
                f"{data['avg']:.3f}", f"{data['min']:.3f}", f"{data['max']:.3f}",
            )
            total_time += data['total']
        
        console.print(table)
        console.print(f"\n[bold]Całkowity czas profilowania: [cyan]{total_time:.2f} s[/cyan][/bold]")
        avg_per_sample = total_time / self.num_samples if self.num_samples > 0 else 0
        console.print(f"[bold]Średni czas na jedną próbkę: [cyan]{avg_per_sample:.2f} s[/cyan][/bold]")
        logger.info(f"Raport wydajności wygenerowany. Całkowity czas: {total_time:.2f}s, Średni na próbkę: {avg_per_sample:.2f}s.")


    async def run_profile(self, mode: str):
        """
        Uruchamia główną pętlę profilowania w wybranym trybie.

        Args:
            mode (str): Tryb profilowania do uruchomienia.
                        Dostępne opcje:
                        - 'full_cycle': Mierzy zarówno czas nawigacji, jak i
                          przetwarzania strony.
                        - 'processing_only': Mierzy tylko czas przetwarzania
                          strony (skanowanie + pobieranie).
        """
        # Pobierz startowy URL asynchronicznie
        start_url = await get_state('last_scan_url') or START_URL
        if not start_url:
            logger.error("Brak startowego URL. Uruchom najpierw skanowanie, aby zapisać postęp.")
            return

        current_url = start_url
        
        with console.status(f"[bold yellow]Uruchamianie profilowania (Tryb: {mode})...[/]", spinner="dots") as status:
            for i in range(self.num_samples):
                status.update(f"[bold yellow]Próbka {i+1}/{self.num_samples}... Nawigacja do strony.[/]")
                
                # Zawsze musimy najpierw nawigować do strony
                await self.page.goto(current_url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
                page_before_nav = self.page.url
                
                status.update(f"[bold yellow]Próbka {i+1}/{self.num_samples}... Przetwarzanie.[/]")

                # Zmierz czas wykonania głównej logiki przetwarzania strony
                await self._measure(
                    "Przetwarzanie (Skan + Pobieranie)",
                    process_single_photo_page,
                    self.page, page_before_nav, Path(DOWNLOADS_DIR_BASE), "main"
                )

                # W trybie "pełnego cyklu" mierzymy również nawigację
                if mode == 'full_cycle':
                    status.update(f"[bold yellow]Próbka {i+1}/{self.num_samples}... Mierzenie nawigacji.[/]")
                    nav_success = await self._measure(
                        "Nawigacja (Strzałka)",
                        unstoppable_navigate,
                        self.page, DIRECTION_KEY, page_before_nav, Text() # Text() jako dummy
                    )

                    if not nav_success:
                        logger.error("Nawigacja nie powiodła się. Przerywam profilowanie.")
                        break
                    current_url = self.page.url
                else:
                    # W trybie "tylko przetwarzanie", przechodzimy do następnego
                    # elementu bez mierzenia czasu.
                    await self.page.keyboard.press(DIRECTION_KEY)
                    await self.page.wait_for_url(lambda url: url != page_before_nav, timeout=WAIT_FOR_SELECTOR * 1000)
                    current_url = self.page.url
                    
        # Po zakończeniu pętli, zawsze wyświetl raport
        self._display_report()


# ##############################################################################
# ===                    SEKCJA 2: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_profiler():
    """
    Wyświetla interaktywne menu i zarządza całym procesem profilowania.

    Proces:
    1.  Wyświetla menu pozwalające wybrać tryb profilowania.
    2.  Prosi użytkownika o podanie liczby próbek do analizy.
    3.  Uruchamia przeglądarkę w trybie `headless` dla spójności pomiarów.
    4.  Tworzy instancję klasy `PerformanceProfiler`.
    5.  Uruchamia profilowanie w wybranym przez użytkownika trybie.
    6.  Po zakończeniu, wyświetla pełny raport.
    """
    console.clear()
    logger.info("Uruchamiam Profiler Wydajności...")
    console.print(Panel("⏱️ Profiler Wydajności Silnika Master ⏱️", expand=False, style="bold yellow"))
    
    # Krok 1: Wybór trybu profilowania
    menu_items = [
        ("Pełny Cykl (Nawigacja + Przetwarzanie)", "full_cycle"),
        ("Tylko Przetwarzanie (Skan + Pobieranie)", "processing_only"),
        ("Anuluj", "exit")
    ]
    selected_mode = await create_interactive_menu(menu_items, "Wybierz tryb profilowania", border_style="blue")

    if selected_mode == "exit" or selected_mode is None:
        logger.warning("Profilowanie anulowane przez użytkownika w menu wyboru trybu.")
        return

    # Krok 2: Pobranie liczby próbek
    try:
        num_samples_str = Prompt.ask("\n[cyan]Na ilu zdjęciach/plikach przeprowadzić test?[/]", default="10")
        num_samples = int(num_samples_str)
        if num_samples <= 0: raise ValueError
    except ValueError:
        logger.error(f"Nieprawidłowa liczba próbek: '{num_samples_str}'. Przerywam.")
        console.print("[bold red]Należy podać dodatnią liczbę całkowitą.[/bold red]")
        return

    if not Confirm.ask(f"\n[cyan]Czy na pewno uruchomić profilowanie dla {num_samples} próbek w trybie '{selected_mode}'?[/]"):
        logger.warning("Profilowanie anulowane przez użytkownika przed startem.")
        return

    logger.info(f"Rozpoczynam profilowanie dla {num_samples} próbek (Tryb: {selected_mode}).")
    
    # Krok 3: Uruchomienie profilera
    async with async_playwright() as p:
        browser, page = None, None
        try:
            with console.status("[cyan]Uruchamianie przeglądarki w trybie headless...[/]"):
                browser = await getattr(p, BROWSER_TYPE).launch_persistent_context(
                    Path(SESSION_DIR).expanduser(),
                    headless=True, # Profiler zawsze działa w trybie headless dla spójności pomiarów
                    args=BROWSER_ARGS.get(BROWSER_TYPE)
                )
                page = await browser.new_page()

            profiler = PerformanceProfiler(page, num_samples)
            await profiler.run_profile(mode=selected_mode)

        except Exception as e:
            logger.critical("Wystąpił krytyczny błąd podczas profilowania.", exc_info=True)
            console.print(f"[bold red]Wystąpił błąd krytyczny: {e}[/bold red]")
        finally:
            if browser:
                await browser.close()
            logger.info("Profiler zakończył pracę.")
