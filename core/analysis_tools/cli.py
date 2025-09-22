# plik: core/analysis_tools/cli.py
# Wersja 1.0 - Dedykowane, interaktywne menu dla narzędzi analitycznych.
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
from ..analytics import run_analytics
from ..smart_archiver.cli import run_smart_archiver
from ..visual_duplicate_finder.cli import run_visual_duplicate_finder
from ..validator.cli import run_integrity_validator
from ..doctor.cli import run_doctor
from ..guardian.cli import run_guardian_menu

console = Console()
logger = logging.getLogger(__name__)

class AnalysisToolsMenu:
    """Klasa zarządzająca menu dla narzędzi analitycznych i diagnostycznych."""
    def __init__(self):
        self.selected_index = 0
        self.menu_items: List[Dict[str, Any]] = [
            {"icon": "📊", "text": "Analiza i Statystyki", "action": run_analytics, "desc": "Generuje i wyświetla szczegółowe statystyki dotyczące Twojej kolekcji."},
            {"icon": "🧹", "text": "Asystent Porządkowania", "action": run_smart_archiver, "desc": "Automatycznie wyszukuje zdjęcia nieostre, ciemne, uszkodzone lub małe."},
            {"icon": "🧩", "text": "Znajdź Duplikaty Wizualne", "action": run_visual_duplicate_finder, "desc": "Wyszukuje wizualnie podobne obrazy za pomocą hashy percepcyjnych."},
            {"icon": "🛡️", "text": "Walidator Integralności", "action": run_integrity_validator, "desc": "Sprawdza spójność danych między bazą a plikami na dysku."},
            {"icon": "🩺", "text": "Diagnostyka (Doktor)", "action": run_doctor, "desc": "Przeprowadza pełny bilans zdrowia aplikacji i jej zależności."},
            {"icon": "🛡️", "text": "Strażnik Systemu", "action": run_guardian_menu, "desc": "Uruchamia szybki bilans zdrowia i zarządza powiadomieniami."},
            {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "desc": "Zamyka to menu."},
        ]

    def _build_layout(self) -> Layout:
        table = Table.grid(expand=True, padding=(0, 2))
        for i, item in enumerate(self.menu_items):
            style = "bold white on green" if i == self.selected_index else ""
            prefix = "» " if i == self.selected_index else "  "
            table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
        
        menu_panel = Panel(table, title="[bold green]🔬 Analiza i Diagnostyka[/]", border_style="green")
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

async def run_analysis_tools_menu():
    """Główny punkt wejścia do modułu."""
    await AnalysisToolsMenu().run()
