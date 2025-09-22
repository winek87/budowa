# plik: core/validator/cli.py
# Wersja 1.0 - Nowe, dynamiczne menu dla Walidatora Integralności
# -*- coding: utf-8 -*-

import asyncio
import logging
from typing import List, Dict, Any

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text
from rich.align import Align
from rich.layout import Layout
from rich.live import Live
from rich.padding import Padding

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from ..utils import get_key
from ..database import get_validator_stats # <--- Nasza nowa funkcja

# --- IMPORTY Z PRZYSZŁYCH MODUŁÓW (placeholdery) ---
# Te funkcje przeniesiemy w kolejnych krokach
from .tasks.existence import verify_file_existence
from .tasks.hashing import verify_and_write_hashes
from .tasks.duplicates import find_duplicates_by_hash
from .tasks.consistency import find_and_fix_inconsistencies, synchronize_local_file_paths
from .tasks.metadata import analyze_metadata_consistency

# --- Inicjalizacja ---
console = Console(record=True)
logger = logging.getLogger(__name__)

class ValidatorMenuApp:
    """Klasa hermetyzująca logikę i stan menu Walidatora."""
    
    def __init__(self):
        self.selected_index = 0
        self.menu_items: List[Dict[str, Any]] = self._define_menu_items()
        self.selected_index = next((i for i, item in enumerate(self.menu_items) if item.get('action')), 0)

    def _define_menu_items(self) -> List[Dict[str, Any]]:
        return [
            {"type": "header", "text": "GŁÓWNE NARZĘDZIA"},
            {"icon": "👻", "text": "Weryfikuj istnienie plików (Baza vs Dysk)", "action": verify_file_existence, "description": "Sprawdza, czy każdy plik oznaczony jako 'pobrany' w bazie danych faktycznie istnieje na dysku."},
            {"icon": "🧮", "text": "Oblicz i zapisz sumy kontrolne (hash)", "action": verify_and_write_hashes, "description": "Oblicza sumy kontrolne MD5 dla plików, które ich jeszcze nie mają. Niezbędne do znajdowania duplikatów."},
            {"icon": "🧩", "text": "Znajdź i zarządzaj duplikatami", "action": find_duplicates_by_hash, "description": "Wyszukuje pliki o identycznej zawartości (na podstawie hasha) i pozwala na interaktywne usunięcie nadmiarowych kopii."},
            
            {"type": "header", "text": "NARZĘDZIA NAPRAWCZE"},
            {"icon": "🔗", "text": "Wyszukaj niespójności (Sieroty i Duchy)", "action": find_and_fix_inconsistencies, "description": "Kompleksowe narzędzie, które znajduje pliki na dysku bez wpisu w bazie ('sieroty') oraz wpisy w bazie bez pliku ('duchy')."},
            {"icon": "🔄", "text": "Synchronizuj ścieżki plików lokalnych", "action": synchronize_local_file_paths, "description": "Naprawia nieaktualne ścieżki w bazie dla plików, które zostały zaimportowane z lokalnego dysku."},
            {"icon": "📊", "text": "Analizuj spójność metadanych", "action": analyze_metadata_consistency, "description": "Sprawdza, czy rok i miesiąc w ścieżce pliku zgadzają się z datą zapisaną w jego metadanych."},
            
            {"type": "separator"},
            {"icon": "🚪", "text": "Wróć do menu głównego", "action": "exit", "description": "Zamyka Walidator Integralności i wraca do menu głównego."},
        ]
    
    def _format_label(self, base_text: str, count: int, color: str = "yellow") -> str:
        if count > 0:
            return f"{base_text} ([{color}]{count}[/])"
        return base_text

    def _build_layout(self, stats: Dict[str, Any]) -> Layout:
        # Layout będzie bardzo podobny do tego z menu skanera, aby zachować spójność
        menu_table = Table.grid(expand=True, padding=(0, 1)); menu_table.add_column(ratio=1); menu_table.add_column(width=10, justify="right")
        
        for i, item in enumerate(self.menu_items):
            item_type, action_func = item.get("type"), item.get("action")
            row_style = "bold white on blue" if i == self.selected_index else ""

            if item_type == "header":
                menu_table.add_row(); menu_table.add_row(Text(f" {item['text']} ", style="bold underline yellow")); menu_table.add_row()
            elif item_type == "separator":
                menu_table.add_row("─" * 60, style="dim")
            else:
                prefix = "» " if i == self.selected_index else "  "
                label = item['text']
                count_text = Text("")

                if action_func == verify_and_write_hashes: label = self._format_label(label, stats['files_to_hash'])
                if action_func == find_duplicates_by_hash: label = self._format_label(label, stats['duplicate_sets'], "red")

                menu_table.add_row(f"{prefix}{item['icon']} {label}", count_text, style=row_style)

        menu_panel = Panel(Padding(menu_table, (0, 1)), title="[bold yellow]Walidator Integralności Danych[/]", border_style="yellow")
        
        selected_item = self.menu_items[self.selected_index]
        info_panel = Panel(Align.center(f"[bold underline bright_yellow]{selected_item['icon']} {selected_item['text']}[/]\n\n[italic]{selected_item['description']}[/]", vertical="middle"), title="[bold]Opis[/]", border_style="dim")
        
        layout = Layout(); footer = Text.from_markup(" Nawigacja: [on bright_black] ▲ ▼ [/] | Wybór: [on bright_black] Enter [/] | Powrót: [on bright_black] Q [/] ", style="white")
        layout.split_column(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1), Layout(Align.center(footer), size=1))
        return layout

    async def _handle_input(self) -> str:
        """Oczekuje na klawisz i zwraca akcję do wykonania."""
        key = await asyncio.to_thread(get_key)
        if not key: 
            return 'CONTINUE'

        if key == "UP":
            original_index = self.selected_index
            while True:
                self.selected_index = (self.selected_index - 1 + len(self.menu_items)) % len(self.menu_items)
                # Pomiń nagłówki i separatory
                if self.menu_items[self.selected_index].get("action"):
                    break
                if self.selected_index == original_index:
                    break
        elif key == "DOWN":
            original_index = self.selected_index
            while True:
                self.selected_index = (self.selected_index + 1) % len(self.menu_items)
                # Pomiń nagłówki i separatory
                if self.menu_items[self.selected_index].get("action"):
                    break
                if self.selected_index == original_index:
                    break
        elif key.upper() == 'Q': 
            return 'EXIT_MENU'
        elif key == "ENTER": 
            return 'EXECUTE_ACTION'
            
        return 'CONTINUE'

    async def _execute_action(self) -> bool:
        selected_action = self.menu_items[self.selected_index].get("action")
        if not selected_action: return True
        if selected_action == "exit": return False
        
        console.clear()
        await selected_action()
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter...[/]")
        return True

    async def run(self):
        while True:
            stats = await get_validator_stats()
            with Live(self._build_layout(stats), screen=True, auto_refresh=False, transient=True) as live:
                while True:
                    live.update(self._build_layout(stats), refresh=True)
                    action = await self._handle_input()
                    if action != 'CONTINUE': break
            if action == 'EXIT_MENU': break
            if action == 'EXECUTE_ACTION':
                if not await self._execute_action(): break

async def run_integrity_validator():
    """Główny punkt wejścia, tworzy instancję i uruchamia pętlę menu."""
    app = ValidatorMenuApp()
    await app.run()
