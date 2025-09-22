# plik: core/advanced_tools/cli.py
# Wersja 1.0 - Dedykowane, interaktywne menu dla narzędzi zaawansowanych.
# -*- coding: utf-8 -*-

import asyncio
import logging
from typing import List, Dict, Any

from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

# Importy z modułów projektu `core`
from ..utils import get_key
from ..config_editor_logic import run_config_editor
from ..db_editor_logic import run_db_editor
from ..attribute_explorer.cli import run_attribute_explorer
from ..profiler.cli import run_profiler
from ..test_suite_logic import run_test_suite
from ..code_analyzer.cli import run_code_analyzer
from ..interceptor.cli import run_interceptor

console = Console()
logger = logging.getLogger(__name__)

class AdvancedToolsMenu:
    """Klasa zarządzająca menu dla narzędzi zaawansowanych i deweloperskich."""
    def __init__(self):
        self.selected_index = 0
        self.menu_items: List[Dict[str, Any]] = [
            {"icon": "📝", "text": "Edytor Konfiguracji", "action": run_config_editor, "desc": "Interaktywny edytor do zmiany ustawień w pliku `core/config.py`."},
            {"icon": "💾", "text": "Edytor Bazy Danych", "action": run_db_editor, "desc": "Narzędzie do bezpośredniego wykonywania zapytań SQL na bazie danych."},
            {"icon": "🕵️ ", "text": "Odkrywca Atrybutów Strony", "action": run_attribute_explorer, "desc": "Skanuje stronę i wyświetla wszystkie atrybuty HTML do znalezienia selektorów."},
            {"icon": "⏱️", "text": "Profiler Wydajności", "action": run_profiler, "desc": "Mierzy i analizuje szybkość działania kluczowych komponentów silnika."},
            {"icon": "✅", "text": "Audytor Kodu", "action": run_code_analyzer, "desc": "Uruchamia analizę statyczną (linting) i testy jednostkowe dla kodu projektu."},
            {"icon": "📡", "text": "Podsłuch Sieciowy", "action": run_interceptor, "desc": "Przechwytuje i wyświetla ukrytą komunikację (JSON) z serwerami Google."},
            {"icon": "🧪", "text": "Pakiet Testowy", "action": run_test_suite, "desc": "Uruchamia zestaw zintegrowanych testów weryfikujących poprawność działania."},
            {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "desc": "Zamyka to menu."},
        ]

    def _build_layout(self) -> Layout:
        table = Table.grid(expand=True, padding=(0, 2))
        for i, item in enumerate(self.menu_items):
            style = "bold white on magenta" if i == self.selected_index else ""
            prefix = "» " if i == self.selected_index else "  "
            table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
        
        menu_panel = Panel(table, title="[bold magenta]⚙️ Narzędzia Zaawansowane[/]", border_style="magenta")
        info_panel = Panel(Align.center(f"[italic]{self.menu_items[self.selected_index]['desc']}[/]", vertical="middle"), title="Opis", border_style="dim")
        
        layout = Layout(); body = Layout(); body.split_row(menu_panel, info_panel)
        footer = Text.from_markup("Nawigacja: ▲/▼ | Wybór: Enter | Powrót: Q", justify="center")
        layout.split_column(body, Layout(footer, size=1)); return layout

    async def run(self):
        while True:
            console.clear()
            with Live(self._build_layout(), transient=True, auto_refresh=False) as live:
                while True:
                    live.update(self._build_layout(), refresh=True)
                    key = await asyncio.to_thread(get_key)
                    if key == "UP": self.selected_index = (self.selected_index - 1 + len(self.menu_items)) % len(self.menu_items)
                    elif key == "DOWN": self.selected_index = (self.selected_index + 1) % len(self.menu_items)
                    elif key == "ENTER": break
                    elif key and key.upper() == 'Q': self.selected_index = -1; break
            
            if self.selected_index == -1 or self.menu_items[self.selected_index]['action'] == 'back': break
            
            action = self.menu_items[self.selected_index]['action']
            console.clear()
            await action()
            Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić...[/]")

async def run_advanced_tools_menu():
    """Główny punkt wejścia do modułu."""
    await AdvancedToolsMenu().run()
