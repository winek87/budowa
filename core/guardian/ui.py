# plik: core/guardian/ui.py
# Wersja 1.0 - Dashboard i komponenty UI dla Strażnika Systemu.
# Opis: Zarządza dashboardem na żywo dla procesu Bilansu Zdrowia.
# -*- coding: utf-8 -*-

from typing import List, Dict, Any

from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

class GuardianLiveDisplay:
    """Zarządza dashboardem na żywo dla Bilansu Zdrowia."""

    def __init__(self, tests_to_run: List[Dict[str, Any]], console: Console):
        self.console = console
        self._live: Live | None = None
        self._title_text = Text("🩺 Szybki Bilans Zdrowia Systemu 🩺", justify="center", style="bold yellow")
        
        self.tests = tests_to_run
        self.statuses = {test['name']: Text("Oczekuje...", style="dim") for test in self.tests}
        self.current_spinner = Spinner("dots", "")
        self._layout = self._build_layout()

    def _build_layout(self) -> Layout:
        """Tworzy główny layout dashboardu."""
        status_table = Table.grid(padding=(0, 2), expand=True)
        status_table.add_column("Komponent Systemu", style="cyan", no_wrap=True, ratio=1)
        status_table.add_column("Status / Wynik", style="white", ratio=2)

        for test in self.tests:
            status_table.add_row(test['name'], self.statuses[test['name']])

        status_panel = Panel(Align.center(status_table), title="[bold yellow]Postęp Diagnostyki[/]", border_style="yellow")
        spinner_panel = Panel(Align.center(self.current_spinner), border_style="dim")

        layout = Layout()
        layout.split_column(
            Layout(Panel(self._title_text), name="header", size=3),
            Layout(status_panel, name="main"),
            Layout(spinner_panel, name="footer", size=3)
        )
        return layout

    def __enter__(self):
        self._live = Live(self._layout, screen=True, transient=True, auto_refresh=False, console=self.console)
        self._live.start(); return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live: self._live.stop()
    
    def _refresh(self):
        if self._live:
            self._live.update(self._build_layout(), refresh=True)

    def start_test(self, test_name: str):
        self.statuses[test_name].plain = "Sprawdzanie..."
        self.statuses[test_name].style = "yellow"
        self.current_spinner.text = Text(f"Uruchomiono: {test_name}", style="cyan")
        self._refresh()
    
    def end_test(self, test_name: str, status: str, message: str, style: str):
        self.statuses[test_name] = Text.from_markup(f"[{style}]{status}[/]\n[dim]{message}[/dim]")
        self._refresh()

    def finish(self):
        self.current_spinner.text = Text("Bilans zdrowia zakończony.", style="green")
        self._refresh()
