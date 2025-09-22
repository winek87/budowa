# plik: core/code_analyzer/cli.py
# Wersja 1.2 - Naprawiono błąd importu przez poprawną strukturę pakietu.
# Opis: Moduł zarządzający menu i przepływem pracy dla analizy kodu.
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

from ..utils import get_key
from .tasks import run_linter_async, run_tests_async
# === POCZĄTEK POPRAWKI: Używamy teraz jednego, poprawnego importu ===
from .ui.views import (
    CodeAnalyzerLiveDisplay,
    display_scrollable_linter_results, 
    display_unittest_results, 
    display_skipped_panel
)
# === KONIEC POPRAWKI ===


console = Console()
logger = logging.getLogger(__name__)

PATHS_TO_LINT = ["core", "uruchom.py", "start.py"]
TESTS_PATH = "tests"

# (Reszta pliku cli.py pozostaje bez zmian, ponieważ logika jest poprawna)

class CodeAnalyzerMenuApp:
    def __init__(self):
        self.selected_index: int = 0; self.menu_items: List[Dict[str, Any]] = self._define_menu_items()
    def _define_menu_items(self) -> List[Dict[str, Any]]:
        return [{"icon": "🚀", "text": "Uruchom pełny audyt (Flake8 + Unittest)", "action": "full", "description": "Przeprowadza kompletną analizę kodu, obejmującą zarówno sprawdzanie stylu i błędów (Flake8), jak i weryfikację funkcjonalności (testy jednostkowe)."}, {"icon": "🎨", "text": "Tylko analiza statyczna (Flake8)", "action": "linter", "description": "Skanuje kod źródłowy w poszukiwaniu potencjalnych błędów, niespójności stylistycznych i złych praktyk. Nie uruchamia testów."}, {"icon": "🧪", "text": "Tylko testy jednostkowe (Unittest)", "action": "tests", "description": "Uruchamia wszystkie zdefiniowane w projekcie testy jednostkowe, aby sprawdzić, czy kluczowe komponenty działają poprawnie. Nie sprawdza stylu kodu."}, {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "description": "Zamyka Audytora Kodu i wraca do menu głównego."}]
    def _build_layout(self) -> Layout:
        table = Table.grid(expand=True, padding=(0, 2))
        for i, item in enumerate(self.menu_items):
            style = "bold white on yellow" if i == self.selected_index else ""; prefix = "» " if i == self.selected_index else "  "
            table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
        menu_panel = Panel(table, title="[bold yellow]📋 Audytor Kodu[/]", border_style="yellow")
        selected_item = self.menu_items[self.selected_index]
        info_panel = Panel(Align.center(f"[bold underline bright_yellow]{selected_item['icon']} {selected_item['text']}[/]\n\n[italic]{selected_item['description']}[/]", vertical="middle"), title="[bold]Opis Opcji[/]", border_style="dim")
        layout = Layout(); body = Layout(name="body"); body.split_row(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1))
        footer = Text.from_markup("Nawigacja: [on bright_black] ▲/▼ [/] | Wybór: [on bright_black] Enter [/] | Powrót: [on bright_black] Q [/]", justify="center")
        layout.split_column(body, Layout(footer, size=1)); return layout
    async def _handle_input(self) -> str:
        key = await asyncio.to_thread(get_key);
        if not key: return 'CONTINUE'
        if key == "UP": self.selected_index = (self.selected_index - 1 + len(self.menu_items)) % len(self.menu_items)
        elif key == "DOWN": self.selected_index = (self.selected_index + 1) % len(self.menu_items)
        elif key.upper() == 'Q': return 'EXIT_MENU'
        elif key == "ENTER": return 'EXECUTE_ACTION'
        return 'CONTINUE'
    async def _execute_action(self, selected_action: str):
        if selected_action == "back": return
        console.clear(); linter_results, test_successful, test_output = None, None, None
        with CodeAnalyzerLiveDisplay(console=console) as display:
            if selected_action in ["full", "linter"]:
                display.update_status(linter_msg="Uruchamianie...:yellow", spinner_msg="Uruchamianie analizy statycznej (Flake8)...")
                _, linter_results = await run_linter_async(PATHS_TO_LINT)
                linter_status = "✅ Zakończono:green" if not linter_results or "SKIPPED" in linter_results[0] else f"❌ Znaleziono problemy ({len(linter_results)}):red"
                display.update_status(linter_msg=linter_status)
            if selected_action in ["full", "tests"]:
                display.update_status(tests_msg="Uruchamianie...:yellow", spinner_msg="Uruchamianie testów jednostkowych (Unittest)...")
                test_successful, test_output = await run_tests_async(TESTS_PATH)
                tests_status = "✅ Sukces:green" if test_successful else "❌ Błędy:red"
                if "SKIPPED" in test_output or "INFO" in test_output: tests_status = "🟡 Pominięto:yellow"
                display.update_status(tests_msg=tests_status)
            display.update_status(spinner_msg="Analiza zakończona."); await asyncio.sleep(1)
        console.clear(); console.print(Panel(f"Wyniki audytu dla opcji: [bold]{selected_action.upper()}[/]", expand=False, border_style="yellow"))
        if linter_results is not None:
            if not linter_results or "SKIPPED" in linter_results[0] or "BŁĄD" in linter_results[0]:
                if not linter_results: console.print(Panel("[bold green]✅ Brak problemów w kodzie.[/]", title="Wynik Analizy Statycznej (Flake8)", border_style="green"))
                elif "SKIPPED" in linter_results[0]: display_skipped_panel("Analiza Statyczna (Flake8)", linter_results[0])
                else: console.print(Panel(f"[bold red]{linter_results[0]}[/]", title="Błąd Analizy Statycznej (Flake8)", border_style="red"))
            else: await display_scrollable_linter_results(linter_results)
        if test_output is not None:
            if "SKIPPED" in test_output or "INFO" in test_output: display_skipped_panel("Testy Jednostkowe (Unittest)", test_output)
            else: display_unittest_results(test_successful, test_output)
        Prompt.ask("\n[bold]Audyt zakończony. Naciśnij Enter, aby wrócić do menu...[/]")
    async def run(self):
        while True:
            console.clear()
            with Live(self._build_layout(), screen=True, auto_refresh=False, transient=True) as live:
                while True:
                    live.update(self._build_layout(), refresh=True)
                    action = await self._handle_input()
                    if action != 'CONTINUE': break
            if action == 'EXIT_MENU': break
            if action == 'EXECUTE_ACTION':
                selected_action = self.menu_items[self.selected_index].get("action")
                if selected_action == 'back': break
                if selected_action: await self._execute_action(selected_action)

async def run_code_analyzer():
    app = CodeAnalyzerMenuApp()
    await app.run()
