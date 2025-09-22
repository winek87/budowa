# plik: core/profiler/cli.py
# Wersja 1.3 - Dodano wyciszanie logów i ulepszono przepływ.
# Opis: Moduł zarządzający przepływem pracy dla profilowania wydajności.
#       Odpowiada za interakcję z użytkownikiem, uruchomienie silnika
#       profilującego i prezentację wyników.
# -*- coding: utf-8 -*-

import asyncio
import logging
from pathlib import Path

from playwright.async_api import async_playwright
from rich.console import Console
from rich.panel import Panel

# Importy z modułów projektu `core`
from ..config import SESSION_DIR, BROWSER_TYPE, BROWSER_ARGS

# Importy z wewnętrznych modułów pakietu
from .tasks import PerformanceProfiler
from .view import get_profiler_options_from_user, display_profiler_report
from .ui.live_display import ProfilerLiveDisplay
from .utils import silence_loggers

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)


async def run_profiler():
    """
    Główny punkt wejścia, który zarządza całym procesem profilowania.

    Przepływ pracy:
    1.  Wyświetla interaktywne menu, aby zebrać od użytkownika tryb i liczbę próbek.
    2.  Uruchamia przeglądarkę w trybie headless.
    3.  Uruchamia dashboard na żywo i silnik profilujący.
    4.  Po zakończeniu pomiarów, wyświetla kompletny, sformatowany raport.
    """
    # Krok 1: Zbierz opcje od użytkownika za pomocą dedykowanego menu UI
    options = await get_profiler_options_from_user()
    if not options:
        logger.warning("Profilowanie anulowane przez użytkownika w menu wyboru opcji.")
        return

    mode, num_samples = options['mode'], options['samples']
    logger.info(f"Rozpoczynam profilowanie dla {num_samples} próbek (Tryb: {mode}).")
    console.clear()
    
    # Definiujemy loggery, które generują "szum" podczas operacji
    loggers_to_silence = [
        'core.profiler.tasks',
        'core.downloader.page_processor',
        'core.downloader.file_processor',
        'core.downloader.page_navigator',
        'core.scanner.online.page_parser'
    ]

    # Używamy menedżera kontekstu, aby wyciszyć logi na czas pracy dashboardu
    with silence_loggers(loggers_to_silence):
        async with async_playwright() as p:
            browser = None
            try:
                # Krok 2: Uruchom dashboard i silnik profilujący
                with ProfilerLiveDisplay(total_samples=num_samples, console=console) as display:
                    display.update_status("Uruchamianie przeglądarki (headless)...")
                    browser = await getattr(p, BROWSER_TYPE).launch_persistent_context(
                        Path(SESSION_DIR).expanduser(),
                        headless=True, # Profiler zawsze działa w trybie headless dla spójności pomiarów
                        args=BROWSER_ARGS.get(BROWSER_TYPE)
                    )
                    page = await browser.new_page()

                    profiler = PerformanceProfiler(page, num_samples)
                    await profiler.run_profile(mode=mode, display=display)
                
                # Krok 3: Po zakończeniu dashboardu, zbierz wyniki i wyświetl raport
                stats = profiler.get_stats()
                console.clear()
                display_profiler_report(stats, num_samples)

            except Exception as e:
                # Błędy nadal będą logowane do pliku.
                # Krytyczne błędy przerwią dashboard i zostaną wyświetlone w panelu.
                logger.critical("Wystąpił krytyczny błąd podczas profilowania.", exc_info=True)
                console.print(Panel(f"[bold red]Wystąpił błąd krytyczny: {e}[/]", title="Błąd", border_style="red"))
            finally:
                if browser:
                    await browser.close()
                # Ten log pojawi się w pliku, ale nie na konsoli w trakcie działania
#                logger.info("Profiler zakończył pracę.")

    # Prośba o powrót jest teraz częścią `display_profiler_report`
