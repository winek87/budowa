# plik: core/attribute_explorer/ui/live_display.py
# Wersja 1.0 - Dashboard dla Odkrywcy Atrybutów.
# Opis: Zarządza dashboardem na żywo na czas analizy strony.
# -*- coding: utf-8 -*-

from collections import deque
from typing import Deque

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, BarColumn, SpinnerColumn, TextColumn
from rich.rule import Rule
from rich.text import Text

class AttributeExplorerLiveDisplay:
    """Zarządza dashboardem postępu dla Odkrywcy Atrybutów."""

    def __init__(self, console: Console):
        self.console = console
        self._live: Live | None = None
        self._title_text = Text("🔎 Odkrywca Atrybutów 🔎", justify="center", style="bold green")
        
        self._status_message_text = Text("Inicjalizacja...", style="yellow")
        self._recent_logs: Deque[Text] = deque(maxlen=5)
        self.found_elements = 0

        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%")
        )
        self._task_id = self.progress_bar.add_task("Postęp", total=None)
        self._layout = self._build_layout()

    def _build_right_panel(self) -> Panel:
        """Tworzy prawe 'Centrum Dowodzenia'."""
        content_group = Group(
            Rule("Status Akcji", style="dim"), self._status_message_text,
            Rule("Statystyki", style="dim"),
            Text.from_markup(f" Znalezione elementy: [bold cyan]{self.found_elements}[/]", justify="right"),
        )
        return Panel(content_group, title="[bold green]Status Operacji[/]", border_style="green")

    def _build_layout(self) -> Layout:
        """Tworzy główny, dwukolumnowy layout dashboardu."""
        layout = Layout()
        layout.split(
            Layout(Panel(self._title_text), name="header", size=3),
            Layout(self.progress_bar, name="progress", size=3),
            Layout(ratio=1, name="body")
        )
        layout["body"].split_row(
            Layout(Panel(Group(*self._recent_logs), title="[bold]Logi[/]"), name="main"),
            Layout(self._build_right_panel(), name="side", size=50)
        )
        return layout

    def __enter__(self):
        # transient=True sprawia, że dashboard zniknie po zakończeniu
        self._live = Live(self._layout, screen=True, transient=True, auto_refresh=False, console=self.console)
        self._live.start(); return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live: self._live.stop()

    def _update_view(self):
        if self._live:
            self._layout["side"].update(self._build_right_panel())
            self._layout["main"].update(Panel(Group(*self._recent_logs), title="[bold]Logi[/]"))
            self._live.refresh()
            
    def update_status(self, message: str):
        self._status_message_text.plain = message
        self._update_view()

    def add_log(self, message: str, style: str = "white"):
        self._recent_logs.appendleft(Text(message, style=style))
        self._update_view()
