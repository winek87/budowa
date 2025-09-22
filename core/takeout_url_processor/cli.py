# plik: core/takeout_url_processor/cli.py
# Wersja 1.0 - Nowy, interaktywny interfejs dla Naprawy z URL-i Takeout.
# Opis: Moduł zarządzający przepływem pracy dla naprawy wpisów
#       w bazie z użyciem URL-i z Google Takeout.
# -*- coding: utf-8 -*-

import asyncio
import logging
from pathlib import Path

from playwright.async_api import async_playwright

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm

# Importy z modułów projektu `core`
from ..config import BROWSER_TYPE, SESSION_DIR, DEFAULT_HEADLESS_MODE, BROWSER_ARGS
from ..utils import stop_event

# Importy z wewnętrznych modułów pakietu
from .tasks import get_urls_to_process_from_db, process_single_url_for_repair
from .ui.live_display import TakeoutUrlProcessorLiveDisplay

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)


async def run_takeout_url_processor():
    """Główny punkt wejścia, który zarządza całym procesem naprawy."""
    console.clear()
    logger.info("Uruchomiono narzędzie do naprawy z URL-i z Takeout.")
    
    with console.status("[cyan]Sprawdzanie bazy danych w poszukiwaniu plików do naprawy...[/]"):
        urls_to_process = await get_urls_to_process_from_db()

    if not urls_to_process:
        console.print(Panel("✅ [bold green]Brak plików do naprawy[/]\n\nNie znaleziono żadnych wpisów, które można by naprawić za pomocą tej metody.", title="Wszystko w porządku!", border_style="green"))
        return

    console.print(Panel(f"Znaleziono [bold cyan]{len(urls_to_process)}[/] plików, które można spróbować pobrać/przeskanować ponownie za pomocą URL-i z Takeout.", title="[bold green]🔧 Naprawa z Użyciem URL-i z Takeout 🔧[/]", border_style="green"))
    if not Confirm.ask("\n[cyan]Czy chcesz kontynuować?[/]"):
        logger.warning("Operacja naprawy anulowana przez użytkownika.")
        return

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
            
            with TakeoutUrlProcessorLiveDisplay(total_items=len(urls_to_process), console=console) as display:
                for url in urls_to_process:
                    if stop_event.is_set():
                        logger.warning("Operacja przerwana przez użytkownika.")
                        break

                    display.update_status(f"Przetwarzanie: ...{url[-40:]}")
                    success = await process_single_url_for_repair(page, url)
                    
                    if success:
                        display.update_progress("sukcesy", url)
                    else:
                        display.update_progress("porażki", url)
    
    except Exception as e:
        if not stop_event.is_set():
            logger.critical("Wystąpił krytyczny błąd podczas procesu naprawy.", exc_info=True)
            console.print(Panel(f"[bold red]Wystąpił błąd krytyczny:[/]\n\n{e}\n\n[dim]Sprawdź pliki logów.[/dim]", title="Błąd Krytyczny", border_style="red"))
    finally:
        if context:
            await context.close()
        logger.info("Zakończono proces naprawy z użyciem URL-i z Takeout.")
