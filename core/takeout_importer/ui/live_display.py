# plik: core/takeout_importer/ui/live_display.py
# Wersja 1.1 - Dodano bardziej szczegółowe logi w panelu akcji.
# Opis: Zarządza dashboardem na żywo dla procesu importu i scalania
#       metadanych z archiwum Google Takeout.
# -*- coding: utf-8 -*-

from collections import deque
from typing import Deque

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, SpinnerColumn, TextColumn)
from rich.rule import Rule
from rich.text import Text


class TakeoutImporterLiveDisplay:
    """Zarządza dashboardem postępu dla importu z Takeout."""

    def __init__(self, console: Console):
        """
        Inicjalizuje dashboard.

        Args:
            console (Console): Instancja konsoli Rich.
        """
        self.console = console
        self._live: Live | None = None
        self._title_text = Text("📦 Importer Metadanych z Google Takeout 📦", justify="center", style="bold green")
        
        self._status_message_text = Text("Inicjalizacja...", style="cyan")
        self._recent_logs: Deque[Text] = deque(maxlen=8)
        self.counters = {"mapped": 0, "merged": 0, "updated": 0, "unmapped": 0}

        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(),
            TextColumn("{task.completed}/{task.total}"), "[progress.percentage]{task.percentage:>3.0f}%"
        )
        self._task_id = self.progress_bar.add_task("Postęp...", total=None)
        self._layout = self._build_layout()

    def _build_right_panel(self) -> Panel:
        """Tworzy prawe 'Centrum Dowodzenia' ze statystykami."""
        content = Group(
            Rule("Status Akcji", style="dim"), self._status_message_text,
            Rule("Statystyki", style="dim"),
            Text.from_markup(f" Zmapowane pliki .json: [bold cyan]{self.counters['mapped']}[/]", justify="right"),
            Text.from_markup(f" Scalone metadane: [bold green]{self.counters['merged']}[/]", justify="right"),
            Text.from_markup(f" Zaktualizowane wpisy: [bold bright_green]{self.counters['updated']}[/]", justify="right"),
            Text.from_markup(f" Niezmapowane .json: [bold yellow]{self.counters['unmapped']}[/]", justify="right"),
        )
        return Panel(content, title="[bold green]Status Operacji[/]", border_style="green")

    def _build_layout(self) -> Layout:
        """Tworzy główny, dwukolumnowy layout dashboardu."""
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
            Layout(Panel(Group(*self._recent_logs), title="[bold]Ostatnie Akcje[/]"), name="main"),
            Layout(self._build_right_panel(), name="side", size=50)
        )
        return layout

    def __enter__(self):
        """Uruchamia `rich.Live` przy wejściu do bloku `with`."""
        self._live = Live(self._layout, screen=True, transient=True, auto_refresh=True, console=self.console)
        self._live.start(); return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Zatrzymuje `rich.Live` i wyświetla podsumowanie przy wyjściu z bloku `with`."""
        if self._live: self._live.stop()
        summary = (
            f"[bold green]Import zakończony![/]\n\n"
            f"  - [green]Zaktualizowano wpisów: {self.counters['updated']}[/]\n"
            f"  - [yellow]Niezmapowanych plików .json: {self.counters['unmapped']}[/]\n"
        )
        self.console.print(Panel(summary, title="Podsumowanie"))

    def update_stage(self, stage_name: str, total: int | None = None):
        """
        Aktualizuje pasek postępu, aby odzwierciedlał nowy etap pracy.

        Args:
            stage_name (str): Nazwa nowego etapu (np. "Mapowanie plików...").
            total (int | None, optional): Całkowita liczba kroków dla tego etapu.
        """
        self.progress_bar.reset(self._task_id)
        self.progress_bar.update(self._task_id, description=stage_name, total=total)

    def update_status(self, message: str):
        """Aktualizuje komunikat statusu w 'Centrum Dowodzenia'."""
        self._status_message_text.plain = message

    def add_log(self, icon: str, message: str, style: str = "white"):
        """
        Dodaje sformatowany wpis do panelu logów "Ostatnie Akcje".

        Args:
            icon (str): Ikona reprezentująca typ akcji (np. '✅').
            message (str): Treść komunikatu.
            style (str, optional): Styl `rich` do zastosowania.
        """
        self._recent_logs.appendleft(Text.from_markup(f"[{style}]{icon} {message}[/]"))
        
    def advance(self, counter_key: str | None = None):
        """
        Przesuwa pasek postępu o jeden krok i opcjonalnie inkrementuje
        odpowiedni licznik w statystykach.

        Args:
            counter_key (str | None, optional): Klucz licznika do inkrementacji
                                                (np. 'mapped', 'merged').
        """
        if counter_key:
            self.counters[counter_key] += 1
        self.progress_bar.update(self._task_id, advance=1)
