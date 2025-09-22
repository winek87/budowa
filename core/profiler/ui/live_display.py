# plik: core/profiler/ui/live_display.py
# Wersja 1.2 - Przebudowano dashboard na bardziej informacyjny, dwukolumnowy layout.
# Opis: Zarządza dashboardem na żywo na czas trwania pomiarów wydajności,
#       prezentując postęp i wyniki w czasie rzeczywistym.
# -*- coding: utf-8 -*-

from collections import defaultdict
from typing import Dict

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, SpinnerColumn, TextColumn)
from rich.rule import Rule
from rich.table import Table
from rich.text import Text


class ProfilerLiveDisplay:
    """Zarządza dashboardem postępu dla Profilera Wydajności."""

    def __init__(self, total_samples: int, console: Console):
        """
        Inicjalizuje dashboard.

        Args:
            total_samples (int): Całkowita liczba próbek do przetworzenia.
            console (Console): Instancja konsoli Rich.
        """
        self.console = console
        self._live: Live | None = None
        self._title_text = Text("⏱️ Profiler Wydajności Silnika Master ⏱️", justify="center", style="bold yellow")
        
        self._status_message_text = Text("Inicjalizacja...", style="cyan")
        self._all_timings: Dict[str, list[float]] = defaultdict(list)
        self.current_sample = 0
        self.total_samples = total_samples

        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(),
            TextColumn("{task.completed}/{task.total} próbek"),
            "[progress.percentage]{task.percentage:>3.0f}%"
        )
        self._task_id = self.progress_bar.add_task("Postęp", total=total_samples)
        self._layout = self._build_layout()

    def _build_summary_table(self) -> Table:
        """
        Buduje tabelę z podsumowaniem dotychczasowych, uśrednionych pomiarów.
        """
        table = Table(box=None, show_header=True, header_style="bold green")
        table.add_column("Operacja", style="cyan")
        table.add_column("Średnia (s)", style="green", justify="right")
        table.add_column("Min (s)", style="dim", justify="right")
        table.add_column("Max (s)", style="dim", justify="right")
        
        if not self._all_timings:
            table.add_row("[dim]Oczekiwanie na pierwsze wyniki...[/dim]", "", "", "")
        
        for key, times in sorted(self._all_timings.items()):
            if not times: continue
            avg = sum(times) / len(times)
            table.add_row(key, f"{avg:.3f}", f"{min(times):.3f}", f"{max(times):.3f}")
        return table

    def _build_right_panel(self) -> Panel:
        """Tworzy prawe 'Centrum Dowodzenia' z ostatnimi pomiarami."""
        # W tym widoku panel boczny jest prostszy, pokazuje tylko aktualny status.
        content = Group(
            Rule("Status Akcji", style="dim"), self._status_message_text,
        )
        return Panel(content, title="[bold yellow]Status Operacji[/]", border_style="yellow")

    def _build_layout(self) -> Layout:
        """Tworzy główny, dwukolumnowy layout dashboardu."""
        summary_panel = Panel(self._build_summary_table(), title="[bold]Podsumowanie Pomiarów (na żywo)[/]", border_style="dim")
        
        layout = Layout()
        footer = Panel(
            Align.center(Text.from_markup("Naciśnij [bold cyan]Ctrl+C[/], aby bezpiecznie przerwać operację")),
            border_style="dim"
        )

        layout.split(
            Layout(Panel(self._title_text), name="header", size=3),
            Layout(self.progress_bar, name="progress", size=3),
            Layout(ratio=1, name="body"),
            Layout(footer, name="footer", size=3)
        )
        layout["body"].split_row(
            Layout(summary_panel, name="main"),
            Layout(self._build_right_panel(), name="side", size=50)
        )
        return layout

    def __enter__(self):
        """Uruchamia `rich.Live` przy wejściu do bloku `with`."""
        self._live = Live(self._layout, screen=True, transient=True, auto_refresh=True, console=self.console)
        self._live.start(); return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Zatrzymuje `rich.Live` przy wyjściu z bloku `with`."""
        if self._live: self._live.stop()
            
    def update_status(self, message: str):
        """Aktualizuje główny komunikat statusu."""
        self._status_message_text.plain = message
    
    def update_timing(self, key: str, duration: float):
        """
        Zapisuje nowy pomiar czasu dla danej operacji.
        Dashboard odświeży się automatycznie dzięki `auto_refresh=True`.
        """
        self._all_timings[key].append(duration)
        
    def advance(self):
        """Przesuwa pasek postępu o jeden krok."""
        self.progress_bar.update(self._task_id, advance=1)
        self.current_sample += 1
