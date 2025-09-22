# plik: core/advanced_recovery/cli.py
# Wersja 1.0 - Nowy, interaktywny interfejs dla Zaawansowanego Silnika Naprawy.
# Opis: Ten moduł zarządza całym przepływem pracy dla zaawansowanej naprawy,
#       od interakcji z użytkownikiem po uruchomienie pętli przetwarzania.
# -*- coding: utf-8 -*-

import asyncio
import logging
from pathlib import Path

# Zależności zewnętrzne
try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False

from playwright.async_api import async_playwright

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt

# Importy z modułów projektu `core`
from ..config import BROWSER_TYPE, SESSION_DIR, DEFAULT_HEADLESS_MODE, BROWSER_ARGS
from ..database import get_failed_urls_from_db
from ..utils import stop_event

# Importy z wewnętrznych modułów pakietu
from .tasks import process_single_url_with_shake
from .ui.live_display import AdvancedRecoveryLiveDisplay

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)

def _check_dependencies() -> bool:
    """Sprawdza, czy wszystkie zależności są spełnione."""
    if not EXIFTOOL_AVAILABLE:
        console.print(Panel("[bold red]Błąd: Brak 'pyexiftool'![/bold red]\nUruchom: [cyan]pip install pyexiftool[/cyan]", title="Brak Zależności"))
        return False
    return True

async def run_advanced_recovery():
    """Główny punkt wejścia, który zarządza całym procesem zaawansowanej naprawy."""
    console.clear()
    logger.info("Uruchamiam Zaawansowany Silnik Naprawy Błędów...")

    if not _check_dependencies():
        return

    failed_urls = await get_failed_urls_from_db()
    if not failed_urls:
        logger.info("Nie znaleziono żadnych plików z błędami do naprawy.")
        console.print(Panel("✅ [bold green]Brak plików do naprawy[/]\n\nWygląda na to, że wszystkie wpisy w bazie są w porządku.", title="Gratulacje!", border_style="green"))
        return

    console.print(Panel(f"Znaleziono [bold red]{len(failed_urls)}[/] plików z błędami w bazie danych.", title="[bold magenta]🌀 Zaawansowany Silnik Naprawy (Shake & Retry) 🌀[/]", border_style="magenta"))
    if not Confirm.ask("\n[cyan]Czy chcesz uruchomić zaawansowaną procedurę naprawczą?[/]"):
        logger.warning("Zaawansowana naprawa anulowana przez użytkownika."); return

    context, page = None, None
    try:
        async with async_playwright() as p:
            context = await getattr(p, BROWSER_TYPE).launch_persistent_context(
                Path(SESSION_DIR).expanduser(), 
                headless=DEFAULT_HEADLESS_MODE,
                accept_downloads=True, 
                args=BROWSER_ARGS.get(BROWSER_TYPE)
            )
            page = await context.new_page()
            
            with AdvancedRecoveryLiveDisplay(total_items=len(failed_urls), console=console) as display:
                for url in failed_urls:
                    if stop_event.is_set():
                        logger.warning("Operacja przerwana przez użytkownika.")
                        display.add_log_entry("🟡 Przerwano przez użytkownika.", "yellow")
                        break

                    success = await process_single_url_with_shake(page, url, display)
                    
                    if success:
                        display.update_progress("sukcesy", url)
                    else:
                        display.update_progress("porażki", url)
    
    except Exception as e:
        if not stop_event.is_set():
            logger.critical("Wystąpił krytyczny błąd w Zaawansowanym Silniku Naprawczym.", exc_info=True)
            console.print(Panel(f"[bold red]Wystąpił błąd krytyczny:[/]\n\n{e}\n\n[dim]Sprawdź pliki logów, aby uzyskać więcej informacji.[/dim]", title="Błąd Krytyczny", border_style="red"))
    finally:
        if context:
            await context.close()
        logger.info("Zaawansowana Naprawa zakończyła pracę.")
