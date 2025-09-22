# plik: core/video_fixer/ui.py
# Wersja 2.0 - Ujednolicony, dwuetapowy dashboard.
# -*- coding: utf-8 -*-

from collections import deque
from pathlib import Path
from typing import Deque

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, BarColumn, SpinnerColumn, TextColumn, TimeRemainingColumn
from rich.rule import Rule
from rich.text import Text
from rich.table import Table

class VideoFixerLiveDisplay:
    """Zarządza ujednoliconym, dwuetapowym dashboardem dla Naprawiacza Wideo."""

    def __init__(self, console: Console):
        self.console = console
        self._live: Live | None = None
        self._title_text = Text("📹 Naprawiacz Plików Wideo (FFmpeg) 📹", justify="center", style="bold blue")
        
        self.progress = Progress(SpinnerColumn(), TextColumn("[cyan]{task.description}"), BarColumn(), TextColumn("{task.completed}/{task.total}"))
        self._task_id = self.progress.add_task("Inicjalizacja...", total=1)
        
        self.action_logs: Deque[Text] = deque(maxlen=40)
        self.error_logs: Deque[Text] = deque(maxlen=40)
        self.stats = {"diagnosed": 0, "problems": 0, "fixed": 0, "failed": 0}
        self.current_stage = "diagnostics" # 'diagnostics' or 'fixing'

    def _build_layout(self) -> Layout:
        """Buduje spójny, dwukolumnowy layout."""
        right_panel_content = None
        if self.current_stage == 'diagnostics':
            right_panel_content = Group(
                Rule("Postęp Diagnostyki", style="dim"),
                Text.from_markup(f" Przeskanowano: [cyan]{self.stats['diagnosed']}[/]"),
                Text.from_markup(f" Znaleziono problemów: [red]{self.stats['problems']}[/]"),
            )
        else: # 'fixing'
            right_panel_content = Group(
                Rule("Postęp Naprawy", style="dim"),
                Text.from_markup(f" Naprawiono pomyślnie: [green]{self.stats['fixed']}[/]"),
                Text.from_markup(f" Błędy / Niepowodzenia: [red]{self.stats['failed']}[/]"),
            )

        left_panel = Panel(Group(*self.action_logs), title="[bold]Ostatnie Akcje[/]")
        right_panel = Panel(Group(*self.error_logs), title="[bold]Szczegóły / Błędy[/]")
        
        body_grid = Table.grid(expand=True); body_grid.add_column(); body_grid.add_column()
        body_grid.add_row(left_panel, right_panel)

        layout = Layout()
        layout.split_column(
            Layout(Panel(self._title_text), size=3),
            Layout(self.progress, size=3),
            Layout(Panel(right_panel_content, title="[bold blue]Status Operacji[/]"), size=5),
            body_grid
        )
        return layout

    def __enter__(self):
        self._live = Live(self._build_layout(), screen=True, transient=False, auto_refresh=False, console=self.console)
        self._live.start(); return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live: self._live.stop()

    def update(self):
        if self._live:
            self._live.update(self._build_layout(), refresh=True)
            
    def switch_to_fixing_stage(self, total_to_fix: int):
        """Przestawia dashboard w tryb naprawy."""
        self.current_stage = 'fixing'
        self.progress.reset(self._task_id)
        self.progress.update(self._task_id, description="Naprawiam...", total=total_to_fix)
        self.action_logs.clear(); self.error_logs.clear()
        self.action_logs.appendleft(Text.from_markup(f"[green]Rozpoczynam naprawę {total_to_fix} plików...[/]"))
        self.update()
