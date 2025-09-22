# plik: core/path_fixer/ui/live_display.py
# Wersja 1.0 - Dashboard dla Narzędzia do Naprawy Ścieżek.
# Opis: Zarządza dashboardem na żywo dla procesu aktualizacji ścieżek w bazie.
# -*- coding: utf-8 -*-

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, BarColumn, SpinnerColumn, TextColumn, TimeRemainingColumn
from rich.layout import Layout
from rich.text import Text

class PathFixerLiveDisplay:
    """Zarządza dashboardem postępu dla operacji naprawy ścieżek."""

    def __init__(self, total_items: int, console: Console):
        self.console = console
        self._live: Live | None = None
        self._title_text = Text("🛠️ Naprawa Ścieżek Plików w Bazie Danych 🛠️", justify="center", style="bold blue")

        self.progress_bar = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed} z {task.total}"),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•", TimeRemainingColumn()
        )
        self._task_id = self.progress_bar.add_task("Aktualizacja...", total=total_items)
        self._layout = self._build_layout()

    def _build_layout(self) -> Layout:
        """Tworzy główny layout dashboardu."""
        layout = Layout()
        layout.split_column(
            Layout(Panel(self._title_text), name="header", size=3),
            Layout(self.progress_bar, name="progress", size=3)
        )
        return layout

    def __enter__(self):
        self._live = Live(self._layout, screen=True, transient=True, auto_refresh=False, console=self.console)
        self._live.start(); return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live: self._live.stop()
    
    def update_progress(self, advance: int = 1):
        """Aktualizuje pasek postępu."""
        self.progress_bar.update(self._task_id, advance=advance)
