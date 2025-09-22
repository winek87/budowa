# plik: core/visual_duplicate_finder/ui/live_display.py
# Wersja 1.0 - Dashboard dla Wyszukiwarki Duplikatów.
# Opis: Zarządza dashboardem na żywo dla procesu obliczania hashy percepcyjnych.
# -*- coding: utf-8 -*-

from collections import deque
from typing import Deque

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, BarColumn, SpinnerColumn, TextColumn, TimeRemainingColumn
from rich.rule import Rule
from rich.text import Text
from rich.align import Align

class HashCalculationLiveDisplay:
    """Zarządza dashboardem postępu dla obliczania hashy percepcyjnych."""

    def __init__(self, total_items: int, title: str, console: Console):
        self.console = console
        self._live: Live | None = None
        self._title_text = Text(f"🔬 {title} 🔬", justify="center", style="bold cyan")
        
        self._current_file_text = Text("Inicjalizacja...", style="yellow")
        self._recent_logs: Deque[Text] = deque(maxlen=8)
        self.counters = {"success": 0, "failed": 0}

        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(),
            TextColumn("{task.completed}/{task.total}"), "[progress.percentage]{task.percentage:>3.0f}%", "•", TimeRemainingColumn()
        )
        self._task_id = self.progress_bar.add_task("Hashing...", total=total_items)
        self._layout = self._build_layout()

    def _build_right_panel(self) -> Panel:
        """Tworzy prawe 'Centrum Dowodzenia' ze statystykami."""
        content = Group(
            Rule("Aktualny Plik", style="dim"), self._current_file_text,
            Rule("Statystyki Sesji", style="dim"),
            Text.from_markup(f" Obliczono: [bold green]{self.counters['success']}[/]", justify="right"),
            Text.from_markup(f" Błędy: [bold red]{self.counters['failed']}[/]", justify="right"),
        )
        return Panel(content, title="[bold cyan]Status Operacji[/]", border_style="cyan")

    def _build_layout(self) -> Layout:
        """Tworzy główny, dwukolumnowy layout dashboardu."""
        layout = Layout()
        footer = Panel(Align.center(Text.from_markup("Naciśnij [bold cyan]Ctrl+C[/]...")), border_style="dim")
        layout.split(Layout(Panel(self._title_text), name="header", size=3), Layout(self.progress_bar, name="progress", size=3), Layout(ratio=1, name="body"), Layout(footer, name="footer", size=3))
        layout["body"].split_row(Layout(Panel(Group(*self._recent_logs), title="[bold]Ostatnio Przetworzone[/]"), name="main"), Layout(self._build_right_panel(), name="side", size=50))
        return layout

    def __enter__(self):
        self._live = Live(self._layout, screen=True, transient=True, auto_refresh=True, console=self.console)
        self._live.start(); return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live: self._live.stop()
        summary = (f"[bold cyan]Zakończono obliczanie hashy![/]\n\n"
                   f"  - [green]Pomyślnie obliczono: {self.counters['success']}[/]\n"
                   f"  - [red]Błędy: {self.counters['failed']}[/]\n")
        self.console.print(Panel(summary, title="Podsumowanie"))

    def update_progress(self, status: str, filename: str):
        """Aktualizuje stan po przetworzeniu jednego pliku."""
        self.counters[status] += 1
        self._current_file_text.plain = filename
        
        icon = "✅" if status == "success" else "❌"
        style = "green" if status == "success" else "red"
        self._recent_logs.appendleft(Text.from_markup(f"[{style}]{icon} {filename}[/]"))
        
        self.progress_bar.update(self._task_id, advance=1)
