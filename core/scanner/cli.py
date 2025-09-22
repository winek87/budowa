# plik: core/scanner/cli.py
# Wersja 5.3 - Ostateczna poprawka błędu TypeError z `colspan`.
# Opis: Ten plik zawiera interaktywny interfejs dla modułu Skanera,
#       który dynamicznie wyświetla stan kolekcji i opisy opcji.
# -*- coding: utf-8 -*-

import logging
from typing import List, Dict, Any, Callable
import asyncio

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from rich.align import Align
from rich.padding import Padding
from rich.table import Table

# --- IMPORTY Z MODUŁÓW PROJEKTU `core` ---
from ..config import URL_INPUT_FILE, DEFAULT_HEADLESS_MODE
from ..utils import get_key
from ..database import setup_database, get_scanner_stats

# --- IMPORTY Z WEWNĘTRZNYCH MODUŁÓW PAKIETU `scanner` ---
from .online.runner import run_online_scanner
from .offline.tasks import run_path_corrector, run_filename_fixer, run_metadata_completer, run_exif_writer
from .tools import export_urls_from_db, export_fix_urls, run_single_url_test

# --- Inicjalizacja ---
console = Console(record=True)
logger = logging.getLogger(__name__)


class ScannerMenuApp:
    """Klasa hermetyzująca logikę i stan menu skanera."""

    def __init__(self):
        self.selected_index = 0
        self.menu_items: List[Dict[str, Any]] = self._define_menu_items()
        self.selected_index = next((i for i, item in enumerate(self.menu_items) if item.get('action')), 0)
        self.actions_with_own_loop = [run_online_scanner, run_path_corrector, run_filename_fixer, run_metadata_completer]

    def _define_menu_items(self) -> List[Dict[str, Any]]:
        # Definicje menu bez zmian
        return [
            {"type": "header", "text": "GŁÓWNY PRZEPŁYW PRACY"},
            {"icon": "🌐", "text": "Krok 1: Dokończ skanowanie metadanych", "action": "full_scan", "description": "Uruchamia skaner online, aby pobrać brakujące metadane dla wpisów w bazie."},
            {"icon": "🧩", "text": "Krok 2: Uzupełnij dane i ścieżki z plików", "action": "complete_metadata", "description": "Skanuje pliki na dysku, odczytuje z nich dane EXIF i uzupełnia brakujące informacje w bazie danych."},
            {"icon": "📁", "text": "Krok 3: Sprawdź i napraw LOKALIZACJE", "action": "correct_paths", "description": "Porównuje rzeczywistą lokalizację plików z idealną (obliczoną na podstawie daty) i przenosi je w odpowiednie miejsca."},
            {"icon": "✏️ ", "text": "Krok 4: Sprawdź i napraw NAZWY", "action": "fix_filenames", "description": "Porównuje nazwy plików na dysku z nazwami zapisanymi w metadanych i koryguje je."},

            {"type": "header", "text": "ZAAWANSOWANE OPERACJE ONLINE"},
            {"icon": "🔁", "text": "Ponów tylko te URL-e, które miały błąd", "action": "retry_errors", "description": "Inteligentne ponawianie skanowania tylko dla adresów URL, które wcześniej zwróciły błąd."},
            {"icon": "🔄", "text": "Odśwież metadane dla WSZYSTKICH wpisów", "action": "force_refresh", "description": "Wymusza ponowne skanowanie wszystkich adresów URL w bazie danych w celu odświeżenia metadanych."},
            {"icon": "📄", "text": "Skanuj URL-e z pliku 'urls_to_scan.txt'", "action": "scan_all", "description": "Uruchamia skaner online dla wszystkich adresów URL znajdujących się w pliku wejściowym."},

            {"type": "header", "text": "NARZĘDZIA POMOCNICZE"},
            {"icon": "✍️ ", "text": "ZAPISZ metadane z bazy do plików", "action": "write_to_files", "description": "Używa Exiftool do zapisania metadanych (opisy, tagi, GPS) z bazy danych bezpośrednio do plików na dysku."},
            {"icon": "📤", "text": "Wygeneruj plik 'urls_to_scan.txt' z bazy", "action": "export_urls", "description": "Eksportuje wszystkie adresy URL z bazy danych do pliku tekstowego, gotowego do użycia w innych narzędziach."},
            {"icon": "📥", "text": "Wygeneruj plik 'urls_to_fix.txt'", "action": "export_fix_urls", "description": "Eksportuje tylko te adresy URL, które wymagają ponownego skanowania z powodu brakujących kluczowych metadanych."},
            {"icon": "🔬", "text": "Uruchom PEŁNY TEST diagnostyczny", "action": "single_url_test", "description": "Pozwala przetestować działanie skanera na jednym, konkretnym adresie URL w trybie interaktywnym."},

            {"type": "separator"},
            {"icon": "🚪", "text": "Wróć do menu głównego", "action": "exit", "description": "Zamyka menu skanera i wraca do menu głównego aplikacji."},
        ]

    def _build_layout(self, stats: Dict[str, int]) -> Layout:
        """Tworzy dynamiczny layout menu skanera z użyciem Tabeli."""

        menu_table = Table.grid(expand=True, padding=(0, 1))
        menu_table.add_column("main", ratio=1)
        menu_table.add_column("stats", width=8, justify="right")

        for i, item in enumerate(self.menu_items):
            item_type = item.get("type")
            action = item.get("action")

            row_style = "bold white on dark_cyan" if i == self.selected_index else ""

            # === POCZĄTEK POPRAWKI ===
            if item_type == "header":
                menu_table.add_row()
                # Przekazujemy tylko JEDEN argument, `Table.grid` sam go rozciągnie
                menu_table.add_row(Text(f" {item['text']} ", style="bold underline gold3"))
                menu_table.add_row()
            elif item_type == "separator":
                # Przekazujemy tylko JEDEN argument
                menu_table.add_row("─" * 60, style="dim")
            # === KONIEC POPRAWKI ===
            else:
                prefix = "» " if i == self.selected_index else "  "
                label_text = f"{prefix}{item['icon']} {item['text']}"

                count_text = Text("", style="yellow")
                if action == "full_scan" and stats['unscanned'] > 0: count_text = Text(f"({stats['unscanned']})", style="yellow")
                if action == "complete_metadata" and stats['needs_completion'] > 0: count_text = Text(f"({stats['needs_completion']})", style="yellow")
                if action == "correct_paths" and stats['path_mismatches'] > 0: count_text = Text(f"({stats['path_mismatches']})", style="red")
                if action == "fix_filenames" and stats['name_mismatches'] > 0: count_text = Text(f"({stats['name_mismatches']})", style="red")
                if action == "retry_errors" and stats['scan_errors'] > 0: count_text = Text(f"({stats['scan_errors']})", style="red")

                menu_table.add_row(label_text, count_text, style=row_style)

        menu_panel = Panel(Padding(menu_table, (0, 1)), title="[bold cyan]Skaner i Menedżer Kolekcji[/]", border_style="cyan")

        selected_item = self.menu_items[self.selected_index]
        info_panel = Panel(
            Align.center(
                f"[bold underline bright_cyan]{selected_item['icon']} {selected_item['text']}[/]\n\n"
                f"[italic]{selected_item['description']}[/]",
                vertical="middle"
            ),
            title="[bold]Opis[/]", border_style="dim"
        )

        layout = Layout()
        footer = Text.from_markup(" Nawigacja: [on bright_black] ▲ ▼ [/]   |   Wybór: [on bright_black] Enter [/]   |   Powrót: [on bright_black] Q [/] ", style="white")
        layout.split_column(
            Layout(menu_panel, ratio=2),
            Layout(info_panel, ratio=1),
            Layout(Align.center(footer), size=1)
        )
        return layout

    async def _handle_input(self) -> str:
        key = await asyncio.to_thread(get_key);
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
        elif key.upper() == 'Q': return 'EXIT_MENU'
        elif key == "ENTER": return 'EXECUTE_ACTION'
        return 'CONTINUE'

    async def _execute_action(self) -> bool:
        """Wykonuje wybraną akcję. Zwraca False, aby wyjść z menu, lub True, aby kontynuować."""
        selected_action_key = self.menu_items[self.selected_index].get("action")
        if not selected_action_key: 
            return True # Kontynuuj pętlę menu (np. dla nagłówka)
        
        if selected_action_key == "exit":
            logger.info("Wybrano 'Wróć do menu głównego'.")
            return False # Zwróć False, aby zasygnalizować potrzebę wyjścia
            
        console.clear()
        online_modes = ['full_scan', 'retry_errors', 'force_refresh', 'scan_all', 'scan_fix_file']
        action_map = {
            'correct_paths': run_path_corrector, 'fix_filenames': run_filename_fixer,
            'complete_metadata': run_metadata_completer, 'write_to_files': run_exif_writer,
            'export_urls': export_urls_from_db, 'export_fix_urls': export_fix_urls,
            'single_url_test': run_single_url_test,
        }
        if selected_action_key in online_modes:
            input_file_path = "urls_to_fix.txt" if selected_action_key == 'scan_fix_file' else URL_INPUT_FILE
            run_headless = Confirm.ask("Uruchomić w trybie niewidocznym (headless)?", default=DEFAULT_HEADLESS_MODE)
            await run_online_scanner(process_mode=selected_action_key, run_headless=run_headless, input_file=input_file_path)
        elif selected_action_key in action_map:
            await action_map[selected_action_key]()
        
        quick_actions = ['export_urls', 'export_fix_urls']
        if selected_action_key in quick_actions:
             Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter...[/]")
        
        return True # Domyślnie kontynuuj pętlę menu skanera

    async def run(self):
        """Główna pętla menu skanera."""
        while True:
            stats = await get_scanner_stats()
            with Live(self._build_layout(stats), screen=True, auto_refresh=False, transient=True) as live:
                while True:
                    live.update(self._build_layout(stats), refresh=True)
                    action = await self._handle_input()
                    if action != 'CONTINUE': break
            
            if action == 'EXIT_MENU': 
                break # Wyjście przez 'Q'
            
            if action == 'EXECUTE_ACTION':
                if not await self._execute_action():
                    break

async def run_scanner_menu():
    """Główny punkt wejścia, tworzy instancję i uruchamia pętlę menu."""
    await setup_database()
    app = ScannerMenuApp()
    await app.run()
