# plik: core/menu_logic.py
# Wersja 4.1 - Przeniesiono logikę pod-menu Downloadera do dedykowanego modułu.
# Opis: Ten moduł zarządza głównym menu aplikacji, delegując obsługę
#       pod-menu do wyspecjalizowanych, autonomicznych modułów CLI.
# -*- coding: utf-8 -*-

import asyncio
import logging
from typing import List, Dict, Any, Callable

from rich.align import Align
from rich.console import Console, Group
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text
from rich.padding import Padding
from rich.rule import Rule

# --- NOWE, CZYSTE IMPORTY ---
from .database import get_db_stats
from .utils import get_key

# Główne moduły
from .downloader.cli import run_downloader_menu
from .scanner.cli import run_scanner_menu
from .local_scanner.cli import run_local_scanner_menu

# Pod-menu
from .analysis_tools.cli import run_analysis_tools_menu
from .maintenance_tools.cli import run_maintenance_tools_menu
from .advanced_tools.cli import run_advanced_tools_menu

# Narzędzia standalone
from .downloader.tools import run_single_file_download
from .ai_tagger_logic import run_ai_tagger_menu
from .face_recognition_logic import run_face_recognition_menu

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


class MainMenuApp:
    """Główna klasa aplikacji, zarządzająca menu głównym i wywoływaniem modułów."""

    def __init__(self):
        """Inicjalizuje stan menu."""
        self.selected_index: int = 0
        self.menu_items: List[Dict[str, Any]] = self._define_menu_items()
        
        self.actions_with_own_loop: List[Callable] = [
            run_downloader_menu, run_scanner_menu, run_local_scanner_menu,
            run_analysis_tools_menu, run_maintenance_tools_menu,
            run_advanced_tools_menu, run_ai_tagger_menu, run_face_recognition_menu
        ]
        self.selected_index = next((i for i, item in enumerate(self.menu_items) if item.get('action')), 0)

    def _define_menu_items(self) -> List[Dict[str, Any]]:
        """
        Definiuje strukturę menu, podłączając nowe, dedykowane moduły CLI
        do odpowiednich opcji.
        """
        return [
            {
             "type": "header",
             "text": "GŁÓWNE MODUŁY"
            },
            {
             "icon": "🚀", 
             "text": "Centrum Pobierania",
             "action": run_downloader_menu,
             "description": "Główny silnik do pobierania zdjęć i filmów. Umożliwia wznawianie, naprawę błędów i pełne odświeżanie kolekcji."
            },
            {
             "icon": "🔎",
             "text": "Skaner i Menedżer Metadanych",
             "action": run_scanner_menu,
             "description": "Zaawansowane narzędzie do skanowania metadanych, naprawy plików, uzupełniania danych z EXIF i zapisu tagów."
             },
            {
             "icon": "📦",
             "text": "Lokalny Importer i Organizator",
             "action": run_local_scanner_menu,
             "description": "Skanuje lokalne foldery, importuje pliki do bazy, organizuje je w strukturę ROK/MIESIĄC i wykrywa duplikaty."
             },
            {
             "icon": "🔗",
             "text": "Pobierz pojedynczy plik z URL",
             "action": run_single_file_download,
             "description": "Szybkie narzędzie do pobrania jednego pliku po wklejeniu jego adresu URL z Google Photos."
             },

            {
             "type": "header",
             "text": "NARZĘDZIA AI"
             },
            {
             "icon": "🤖",
             "text": "Inteligentne Tagowanie Obrazów",
             "action": run_ai_tagger_menu, "description": "Wykorzystuje modele AI do automatycznego analizowania i tagowania zawartości zdjęć."
             },
            {
             "icon": "👨‍👩‍👧‍👦",
             "text": "Rozpoznawanie i Grupowanie Twarzy",
             "action": run_face_recognition_menu,
             "description": "Wykrywa twarze na zdjęciach, grupuje je według osób i pozwala na ich nazwanie."
             },

            {
             "type": "header",
             "text": "NARZĘDZIA DODATKOWE"
             },
            {
             "icon": "🔬",
             "text": "Analiza i Diagnostyka",
             "action": run_analysis_tools_menu,
             "description": "Zestaw narzędzi do analizy statystyk, wyszukiwania duplikatów i weryfikacji integralności danych."
             },
            {
             "icon": "🛠️ ",
             "text": "Utrzymanie i Naprawa",
             "action": run_maintenance_tools_menu,
             "description": "Narzędzia do importu z Takeout, naprawy plików, zarządzania kopiami zapasowymi i odświeżania sesji."
             },
            {
             "icon": "⚙️ ",
             "text": "Zaawansowane / Deweloperskie",
             "action": run_advanced_tools_menu,
             "description": "Narzędzia dla zaawansowanych użytkowników, w tym edytory, profiler i podsłuch sieciowy."
             },

            {
             "type": "separator"},
            {
             "icon": "🚪",
             "text": "Wyjście z Aplikacji",
             "action": "exit",
             "description": "Bezpiecznie zamyka aplikację."
             },
        ]

    def _build_layout(self, stats: dict) -> Layout:
        """Tworzy dynamiczny, pionowy układ interfejsu głównego menu."""
        menu_table = Table.grid(expand=True, padding=(0, 1));
        for i, item in enumerate(self.menu_items):
            item_type = item.get("type")
            if item_type == "header":
                menu_table.add_row(); menu_table.add_row(Text(f" {item['text']} ", style="bold underline gold3"))
            elif item_type == "separator":
                menu_table.add_row(Rule(style="dim white"))
            else:
                style = "bold white on blue" if i == self.selected_index else ""
                prefix = "» " if i == self.selected_index else "  "
                label_text = f"{prefix}{item['icon']} {item['text']}"
                count_text = ""
                if item.get('action') == run_downloader_menu and stats.get('failed', 0) > 0:
                    count_text = f" [bold red]({stats['failed']})[/]"
                menu_table.add_row(Text.from_markup(label_text + count_text, style=style))
        
        menu_panel = Panel(Padding(Align.center(menu_table), (1, 2)), title="[bold cyan]Menu Główne[/]", border_style="cyan")
        
        selected_item = self.menu_items[self.selected_index]
        description_text = Text(selected_item.get('description', ''), style="italic white", justify="center")
        
        stats_table = Table(box=None, show_header=False, padding=(0, 1));
        stats_table.add_column(style="dim cyan", justify="right", width=18); stats_table.add_column(style="bold", justify="left")
        stats_table.add_row("Pobrane pliki:", f"[green]{stats.get('downloaded', 0)}[/]"); stats_table.add_row("Pominięte:", f"[yellow]{stats.get('skipped', 0)}[/]"); stats_table.add_row("Zeskanowane:", f"[bright_blue]{stats.get('scanned', 0)}[/]"); stats_table.add_row("Błędy:", f"[bright_red]{stats.get('failed', 0)}[/]"); stats_table.add_row(Rule(style="dim")); stats_table.add_row("[bold white]Wszystkie wpisy:", f"[bold cyan]{stats.get('total', 0)}[/]")
        
        info_panel_content = Group(Align.center(f"[bold underline bright_cyan]{selected_item['icon']} {selected_item['text']}[/]\n"), Padding(description_text, (1, 0, 2, 0)), Rule("Statystyki Kolekcji", style="dim cyan"), Padding(Align.center(stats_table), (1, 0)))
        info_panel = Panel(info_panel_content, title="[bold]Informacje[/]", border_style="dim")
        
        layout = Layout()
        header = Panel(Align.center(Text(">>> Google Photos Toolkit v18.0 <<<", style="bold white on blue")), border_style="blue")
        footer = Text.from_markup(" Nawigacja: ▲/▼ | Wybór: Enter | Wyjście: Q ", style="white", justify="center")
        
        layout.split_column(
            Layout(header, name="header", size=3),
            Layout(menu_panel, name="main", ratio=2),
            Layout(info_panel, name="info", ratio=1),
            Layout(Align.center(footer), name="footer", size=1)
        )
        return layout

    async def _handle_input(self) -> str:
        """Asynchronicznie obsługuje wejście z klawiatury do nawigacji w menu."""
        key = await asyncio.to_thread(get_key)
        if not key: return 'CONTINUE'
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
        elif key.upper() == 'Q':
            return 'EXIT_APP'
        elif key == "ENTER":
            return 'EXECUTE_ACTION'
        return 'CONTINUE'

    async def _execute_action(self) -> str | bool:
        """Wykonuje wybraną akcję z menu."""
        selected_action = self.menu_items[self.selected_index].get("action")
        if not selected_action:
            return True
        if selected_action == "exit":
            return 'EXIT_APP'
        
        console.clear()
        await selected_action()
        
        if selected_action not in self.actions_with_own_loop:
            Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić...[/]")
        return True

    async def run(self):
        """Główna pętla aplikacji, która zarządza cyklem życia interfejsu."""
        while True:
            stats = await get_db_stats()
            action = 'CONTINUE'
            with Live(self._build_layout(stats), screen=True, auto_refresh=False, transient=True) as live:
                while action == 'CONTINUE':
                    live.update(self._build_layout(stats), refresh=True)
                    action = await self._handle_input()
            
            if action == 'EXIT_APP':
                break
            if action == 'EXECUTE_ACTION':
                if await self._execute_action() == 'EXIT_APP':
                    break


async def run_main_menu():
    """Główny punkt wejścia, tworzy instancję i uruchamia pętlę menu."""
    app = MainMenuApp()
    await app.run()
