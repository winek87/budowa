# plik: core/downloader/cli.py
# Wersja 1.0 - Dedykowane, interaktywne menu dla Centrum Pobierania.
# -*- coding: utf-8 -*-

import asyncio
import logging
from functools import partial
from typing import List, Dict, Any

from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text
from rich.padding import Padding

# Importy z modułów projektu `core`
from ..config import DEFAULT_HEADLESS_MODE
from ..database import get_failed_urls_from_db, get_state
from ..utils import get_key
from .runner import run_with_restarts
from .tools import interactive_retry_failed_files

console = Console()
logger = logging.getLogger(__name__)

class DownloaderMenu:
    """Klasa zarządzająca menu dla Centrum Pobierania (Silnik Master)."""
    def __init__(self):
        self.selected_index = 0
        self.menu_items: List[Dict[str, Any]] = []

    async def _define_menu_items(self):
        """Dynamicznie definiuje opcje menu na podstawie stanu bazy."""
        failed_urls = await get_failed_urls_from_db()
        last_scan_url = await get_state('last_scan_url')
        scan_label = f"Wznów skan (od ...{last_scan_url[-40:]})" if last_scan_url else "Rozpocznij nowy skan"
        
        self.menu_items = [
            {"icon": "▶️ ", "text": scan_label, "action": partial(run_with_restarts, scan_mode='main', retry_failed=False, headless_mode=DEFAULT_HEADLESS_MODE), "description": "Kontynuuje pobieranie od ostatnio zapisanego adresu URL."},
            {"icon": "🛠️ ", "text": "Napraw błędy, następnie wznów", "action": partial(run_with_restarts, scan_mode='main', retry_failed=True, headless_mode=DEFAULT_HEADLESS_MODE) if failed_urls else None, "description": "Najpierw próbuje pobrać wszystkie URL-e z błędem, a potem wznawia skan.", "count": len(failed_urls)},
            {"icon": "✍️ ", "text": "Interaktywne ponawianie błędów", "action": interactive_retry_failed_files, "description": "Wyświetla listę nieudanych URL-i i pozwala wybrać, które ponowić."},
            {"icon": "🔄", "text": "Wymuś pełne odświeżenie", "action": partial(run_with_restarts, scan_mode='forced', retry_failed=False, headless_mode=DEFAULT_HEADLESS_MODE), "description": "Rozpoczyna pobieranie od początku, ignorując zapisany postęp."},
            {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "description": "Zamyka to menu i wraca do menu głównego."},
        ]
        self.selected_index = next((i for i, item in enumerate(self.menu_items) if item.get("action")), 0)

    def _build_layout(self) -> Layout:
        # Ta logika jest przeniesiona z `_build_downloader_submenu_layout`
        menu_table = Table.grid(expand=True, padding=(0, 1))
        menu_table.add_column("main", ratio=1); menu_table.add_column("stats", width=8, justify="right")
        for i, item in enumerate(self.menu_items):
            style = "bold white on dark_cyan" if i == self.selected_index else ""
            prefix = "» " if i == self.selected_index else "  "
            label_text = Text(f"{prefix}{item['icon']} {item['text']}")
            if item.get("action") is None: label_text.stylize("dim")
            count_text = Text(f"({item['count']})", style="bold red") if item.get('count', 0) > 0 else Text("")
            menu_table.add_row(label_text, count_text, style=style)
        menu_panel = Panel(Padding(menu_table, (1, 2)), title="[bold blue]🚀 Centrum Pobierania[/]", border_style="blue")
        selected_item = self.menu_items[self.selected_index]
        info_panel = Panel(Align.center(f"[bold underline bright_blue]{selected_item['icon']} {selected_item['text']}[/]\n\n[italic]{selected_item['description']}[/]", vertical="middle"), title="[bold]Opis[/]", border_style="dim")
        layout = Layout(); footer = Text.from_markup("Nawigacja: ▲/▼ | Wybór: Enter | Powrót: Q", justify="center")
        layout.split_column(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1), Layout(Align.center(footer), size=1))
        return layout

    async def run(self):
        while True:
            await self._define_menu_items()
            with Live(self._build_layout(), screen=True, auto_refresh=False, transient=True) as live:
                while True:
                    live.update(self._build_layout(), refresh=True)
                    key = await asyncio.to_thread(get_key)
                    if not key: continue
                    if key == "UP":
                        original_index = self.selected_index
                        while True:
                            self.selected_index = (self.selected_index - 1 + len(self.menu_items)) % len(self.menu_items)
                            if self.menu_items[self.selected_index].get("action"): break
                            if self.selected_index == original_index: break
                    elif key == "DOWN":
                        original_index = self.selected_index
                        while True:
                            self.selected_index = (self.selected_index + 1) % len(self.menu_items)
                            if self.menu_items[self.selected_index].get("action"): break
                            if self.selected_index == original_index: break
                    elif key.upper() == 'Q': self.selected_index = -1; break
                    elif key == "ENTER": break
            
            if self.selected_index == -1 or self.menu_items[self.selected_index].get("action") == "back":
                break
            
            selected_action = self.menu_items[self.selected_index].get("action")
            if selected_action:
                console.clear()
                await selected_action()
                Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter, aby wrócić...[/]")

async def run_downloader_menu():
    """Główny punkt wejścia do modułu menu Centrum Pobierania."""
    await DownloaderMenu().run()
