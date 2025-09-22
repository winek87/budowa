# plik: core/recovery_engine/cli.py
# Wersja 1.1 - Ulepszono interakcję z użytkownikiem (potwierdzenie i podsumowanie).
# Opis: Moduł zarządzający przepływem pracy dla Silnika Ratunkowego.
# -*- coding: utf-8 -*-

import asyncio
import logging
from pathlib import Path
from typing import Dict

try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False

from playwright.async_api import async_playwright
from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

from ..config import BROWSER_TYPE, SESSION_DIR, DEFAULT_HEADLESS_MODE, BROWSER_ARGS
from ..database import get_failed_urls_from_db
from ..utils import stop_event, get_key
from .tasks import process_single_url_recovery
from .ui.live_display import RecoveryEngineLiveDisplay

console = Console()
logger = logging.getLogger(__name__)

def _check_dependencies() -> bool:
    if not EXIFTOOL_AVAILABLE:
        console.print(Panel("[bold red]Błąd: Brak 'pyexiftool'![/]", title="Brak Zależności"))
        return False
    return True

# === POCZĄTEK POPRAWKI: Nowa funkcja do interaktywnego potwierdzenia ===
async def _confirm_recovery_start(num_failed: int) -> bool:
    """Wyświetla interaktywne menu potwierdzenia i zwraca wybór użytkownika."""
    selected_index = 0
    menu_items = [
        {"icon": "✅", "text": "Tak, uruchom Silnik Ratunkowy", "action": True, "description": "Rozpocznie próbę pobrania wszystkich plików oznaczonych jako błędne."},
        {"icon": "❌", "text": "Nie, anuluj i wróć do menu", "action": False, "description": "Anuluje operację i wraca do menu głównego."},
    ]

    def build_confirm_layout() -> Layout:
        table = Table.grid(expand=True, padding=(0, 2))
        for i, item in enumerate(menu_items):
            style = "bold white on yellow" if i == selected_index else ""
            prefix = "» " if i == selected_index else "  "
            table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
        
        menu_panel = Panel(table, title=f"[bold]Znaleziono [red]{num_failed}[/] plików z błędami. Czy kontynuować?[/bold]", border_style="yellow")
        info_panel = Panel(Align.center(f"[italic]{menu_items[selected_index]['description']}[/]", vertical="middle"), title="[bold]Opis opcji[/]", border_style="dim")
        layout = Layout()
        layout.split_column(menu_panel, info_panel)
        return layout

    with Live(build_confirm_layout(), screen=True, transient=True, auto_refresh=False) as live:
        while True:
            live.update(build_confirm_layout(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if key == "UP": selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
            elif key == "DOWN": selected_index = (selected_index + 1) % len(menu_items)
            elif key == "ENTER": return menu_items[selected_index]['action']
            elif key and key.upper() == 'Q': return False
# === KONIEC POPRAWKI ===

async def run_recovery_downloader():
    """Główny punkt wejścia, który zarządza całym procesem ratunkowym."""
    console.clear()
    logger.info("Uruchamiam Prosty Silnik Ratunkowy...")

    if not _check_dependencies(): return

    failed_urls = await get_failed_urls_from_db()
    if not failed_urls:
        logger.info("Nie znaleziono plików z błędami do naprawy.")
        console.print(Panel("✅ [bold green]Brak plików do naprawy[/]", title="Gratulacje!", border_style="green"))
        return

    # === POCZĄTEK POPRAWKI: Używamy nowego menu potwierdzenia ===
    if not await _confirm_recovery_start(len(failed_urls)):
        logger.warning("Operacja ratunkowa anulowana przez użytkownika."); return
    # === KONIEC POPRAWKI ===

    context, page = None, None
    try:
        async with async_playwright() as p:
            context = await getattr(p, BROWSER_TYPE).launch_persistent_context(Path(SESSION_DIR).expanduser(), headless=DEFAULT_HEADLESS_MODE, accept_downloads=True, args=BROWSER_ARGS.get(BROWSER_TYPE))
            page = await context.new_page()
            
            with RecoveryEngineLiveDisplay(total_items=len(failed_urls), console=console) as display:
                for url in failed_urls:
                    if stop_event.is_set():
                        logger.warning("Operacja przerwana przez użytkownika."); break
                    success = await process_single_url_recovery(page, url, display)
                    if success: display.update_progress("sukcesy", url)
                    else: display.update_progress("porażki", url)
    
    except Exception as e:
        if not stop_event.is_set():
            logger.critical("Wystąpił krytyczny błąd w Silniku Ratunkowym.", exc_info=True)
            console.print(Panel(f"[bold red]Błąd: {e}[/]\n[dim]Sprawdź logi.[/dim]", title="Błąd Krytyczny", border_style="red"))
    finally:
        if context: await context.close()
        logger.info("Silnik Ratunkowy zakończył pracę.")

    # === POCZĄTEK POPRAWKI: Używamy spójnego Prompt.ask ===
    Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić do menu...[/]")
    # === KONIEC POPRAWKI ===
