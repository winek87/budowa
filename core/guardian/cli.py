# plik: core/guardian/cli.py
# Wersja 1.2 - Ulepszono pod-menu powiadomień do pełnego, interaktywnego interfejsu.
# Opis: Moduł zarządzający menu i przepływem pracy dla Strażnika Systemu.
# -*- coding: utf-8 -*-

import asyncio
import logging
from functools import partial
from typing import Dict, List, Any

from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

# Importy z modułów projektu `core`
from .. import config as core_config
from .utils import silence_loggers
from ..utils import get_key
from ..doctor.tasks import (
    DoctorCheckError, DoctorInfo, check_dependencies,
    check_exiftool_program, check_session_validity, check_database_integrity
)

# Importy z wewnętrznych modułów pakietu
from .tasks import generate_and_send_report, send_telegram_notification
from .ui import GuardianLiveDisplay

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)


class GuardianMenuApp:
    # (Metody __init__, _define_menu_items, _run_health_check - bez zmian)
    def __init__(self):
        self.selected_index: int = 0; self.menu_items: List[Dict[str, Any]] = []; self._define_menu_items()
    def _define_menu_items(self):
        self.menu_items = [{"icon": "🩺", "text": "Uruchom Szybki Bilans Zdrowia", "action": self._run_health_check, "description": "Przeprowadza szybki test kluczowych komponentów systemu, takich jak sesja, baza danych i zewnętrzne zależności."}, {"icon": "🔔", "text": "Powiadomienia i Raporty (Telegram)", "action": self._configure_notifications, "description": "Pozwala na wysłanie wiadomości testowej lub pełnego raportu o stanie kolekcji na skonfigurowany kanał Telegram."}, {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "description": "Zamyka Strażnika Systemu i wraca do menu głównego."}]
    async def _run_health_check(self):
        console.clear(); health_checks = [{'name': "Ważność sesji logowania", 'func': partial(check_session_validity, core_config), 'async': True}, {'name': "Integralność Bazy Danych", 'func': partial(check_database_integrity, core_config), 'async': True}, {'name': "Zależności (Python)", 'func': check_dependencies, 'async': False}, {'name': "Zależności (ExifTool)", 'func': check_exiftool_program, 'async': False}]; overall_ok = True
        try:
            with GuardianLiveDisplay(tests_to_run=health_checks, console=console) as display:
                for test in health_checks:
                    display.start_test(test['name'])
                    try:
                        details = await test['func']() if test['async'] else await asyncio.to_thread(test['func'])
                        display.end_test(test['name'], "✅ OK", details, "green")
                    except DoctorInfo as e: display.end_test(test['name'], "ℹ️ INFO", str(e), "cyan")
                    except DoctorCheckError as e: display.end_test(test['name'], "❌ BŁĄD", str(e), "red"); overall_ok = False
                    except Exception as e:
                        logger.critical(f"Krytyczny błąd w teście zdrowia '{test['name']}'", exc_info=True); display.end_test(test['name'], "💥 KRYTYCZNY", str(e), "bold white on red"); overall_ok = False
                    await asyncio.sleep(0.3)
                display.finish(); await asyncio.sleep(1)
        except Exception as e: logger.critical("Nieoczekiwany błąd w Bilansie Zdrowia.", exc_info=True)
        if overall_ok: console.print(Panel("✅ [bold green]Bilans zdrowia zakończony pomyślnie.[/]", border_style="green"))
        else: console.print(Panel("⚠️ [bold red]Bilans zdrowia wykrył problemy![/]", border_style="red"))

    async def _configure_notifications(self):
        """Wyświetla interaktywne, dwukolumnowe pod-menu dla powiadomień."""
        test_message = "🚀 *Wiadomość testowa z GPhotos Toolkit!* 🚀\n\nJeśli widzisz tę wiadomość, Twoja konfiguracja jest **poprawna**. ✅"
        
        # === POCZĄTEK POPRAWKI: Definiujemy, co wyciszyć ===
        loggers_to_silence = ['httpx']
        # === KONIEC POPRAWKI ===
        
        async def send_test():
            # === POCZĄTEK POPRAWKI: Używamy `silence_loggers` ===
            with silence_loggers(loggers_to_silence):
                with console.status("[cyan]Wysyłanie wiadomości testowej...[/]"):
                    if await send_telegram_notification(test_message):
                        console.print("\n[bold green]✅ Wiadomość testowa wysłana pomyślnie![/]")
            # === KONIEC POPRAWKI ===

        async def send_report():
            # === POCZĄTEK POPRAWKI: Używamy `silence_loggers` ===
            with silence_loggers(loggers_to_silence):
                with console.status("[cyan]Generowanie i wysyłanie raportu...[/]"):
                    if await generate_and_send_report():
                        console.print("\n[bold green]✅ Raport został pomyślnie wysłany![/]")
            # === KONIEC POPRAWKI ===

        # === POCZĄTEK POPRAWKI: Używamy pełnego menu zamiast prostego promptu ===
        menu_items = [
            {"icon": "🧪", "text": "Wyślij wiadomość testową", "action": send_test, "description": "Wysyła prostą wiadomość na Twój kanał Telegram, aby sprawdzić, czy konfiguracja (token bota, ID czatu) jest poprawna."},
            {"icon": "📊", "text": "Wygeneruj i wyślij pełny raport", "action": send_report, "description": "Pobiera aktualne statystyki z bazy danych, formatuje je w czytelny raport i wysyła na kanał Telegram."},
            {"icon": "🚪", "text": "Wróć", "action": "back", "description": "Wróć do menu Strażnika Systemu."},
        ]
        selected_index = 0

        while True:
            def build_notifications_layout() -> Layout:
                table = Table.grid(expand=True, padding=(0, 2))
                for i, item in enumerate(menu_items):
                    style = "bold white on cyan" if i == selected_index else ""
                    prefix = "» " if i == selected_index else "  "
                    table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
                
                menu_panel = Panel(table, title="[bold cyan]🔔 Opcje Powiadomień (Telegram)[/]", border_style="cyan")
                info_panel = Panel(Align.center(f"[italic]{menu_items[selected_index]['description']}[/]", vertical="middle"), title="[bold]Opis Opcji[/]", border_style="dim")
                
                layout = Layout(); body = Layout(name="body"); body.split_row(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1))
                footer = Text.from_markup("Nawigacja: ▲/▼ | Wybór: Enter | Powrót: Q", justify="center")
                layout.split_column(body, Layout(footer, size=1)); return layout

            console.clear()
            with Live(build_notifications_layout(), transient=True, auto_refresh=False) as live:
                while True:
                    live.update(build_notifications_layout(), refresh=True)
                    key = await asyncio.to_thread(get_key)
                    if key == "UP": selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                    elif key == "DOWN": selected_index = (selected_index + 1) % len(menu_items)
                    elif key == "ENTER": break
                    elif key and key.upper() == 'Q': selected_index = -1; break
            
            if selected_index == -1 or menu_items[selected_index]['action'] == 'back': break
            
            await menu_items[selected_index]['action']()
            Prompt.ask("\n[bold]Naciśnij Enter...[/]")
        # === KONIEC POPRAWKI ===

    # (Metoda run() i funkcja run_guardian_menu() bez zmian)
    async def run(self):
        self._define_menu_items(); selected_index = 0
        while True:
            def build_layout() -> Layout:
                table = Table.grid(expand=True, padding=(0, 2))
                for i, item in enumerate(self.menu_items):
                    style = "bold white on yellow" if i == selected_index else ""; prefix = "» " if i == selected_index else "  "
                    table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
                menu_panel = Panel(table, title="[bold yellow]🛡️ Strażnik Systemu 🛡️[/]", border_style="yellow")
                info_panel = Panel(Align.center(f"[italic]{self.menu_items[selected_index]['description']}[/]", vertical="middle"), title="[bold]Opis Opcji[/]", border_style="dim")
                layout = Layout(); body = Layout(name="body"); body.split_row(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1))
                footer = Text.from_markup("Nawigacja: ▲/▼ | Wybór: Enter | Powrót: Q", justify="center")
                layout.split_column(body, Layout(footer, size=1)); return layout
            console.clear()
            with Live(build_layout(), transient=True, auto_refresh=False) as live:
                while True:
                    live.update(build_layout(), refresh=True)
                    key = await asyncio.to_thread(get_key)
                    if key == "UP": selected_index = (selected_index - 1 + len(self.menu_items)) % len(self.menu_items)
                    elif key == "DOWN": selected_index = (selected_index + 1) % len(self.menu_items)
                    elif key == "ENTER": break
                    elif key and key.upper() == 'Q': selected_index = -1; break
            if selected_index == -1 or self.menu_items[selected_index]['action'] == 'back': break
            await self.menu_items[selected_index]['action']()
            Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić do menu Strażnika...[/]")

async def run_guardian_menu():
    app = GuardianMenuApp(); await app.run()
