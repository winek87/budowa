# plik: core/takeout_file_importer/ui/live_display.py
# Wersja 1.1 - Dodano bardziej szczegółowe logi i obsługę pełnej ścieżki.
# Opis: Zarządza dashboardem na żywo dla procesu importu fizycznych plików
#       z archiwum Google Takeout, prezentując szczegółowy postęp.
# -*- coding: utf-8 -*-

from collections import deque
from typing import Deque

from rich.align import Align
from rich.console import Console, Group
from rich.filesize import decimal
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, SpinnerColumn, TextColumn)
from rich.rule import Rule
from rich.text import Text


class TakeoutFileImporterLiveDisplay:
    """Zarządza dashboardem postępu dla importu plików z Takeout."""

    def __init__(self, console: Console):
        """
        Inicjalizuje dashboard.

        Args:
            console (Console): Instancja konsoli Rich.
        """
        self.console = console
        self._live: Live | None = None
        self._title_text = Text("📦 Importer Plików z Archiwum Google Takeout 📦", justify="center", style="bold green")

        self._status_message_text = Text("Inicjalizacja...", style="cyan")
        self._recent_logs: Deque[Text] = deque(maxlen=8)
        self.counters = {"imported": 0, "errors": 0, "total_size": 0}

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
            Rule("Statystyki Sesji", style="dim"),
            Text.from_markup(f" Zaimportowano plików: [bold green]{self.counters['imported']}[/]", justify="right"),
            Text.from_markup(f" Rozmiar danych: [bold yellow]{decimal(self.counters['total_size'])}[/]", justify="right"),
            Text.from_markup(f" Błędy/Pominięte: [bold red]{self.counters['errors']}[/]", justify="right"),
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
            Layout(Panel(Group(*self._recent_logs), title="[bold]Ostatnio Zaimportowane[/]"), name="main"),
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
        summary = (f"[bold green]Import plików zakończony![/]\n\n"
                   f"  - [green]Zaimportowano nowych plików: {self.counters['imported']}[/]\n"
                   f"  - [yellow]Całkowity rozmiar: {decimal(self.counters['total_size'])}[/]\n"
                   f"  - [red]Błędów/Pominiętych: {self.counters['errors']}[/]\n")
        self.console.print(Panel(summary, title="Podsumowanie"))

    def update_stage(self, stage_name: str, total: int | None = None):
        """Aktualizuje nazwę i total paska postępu dla nowego etapu."""
        self.progress_bar.reset(self._task_id)
        self.progress_bar.update(self._task_id, description=stage_name, total=total)

    def update_status(self, message: str):
        """Aktualizuje komunikat statusu w 'Centrum Dowodzenia'."""
        self._status_message_text.plain = message

    def add_log_and_advance(self, status: str, filename: str, details: str = "", size: int | None = None):
        """
        Dodaje wpis do logów, aktualizuje liczniki i przesuwa pasek postępu.

        Args:
            status (str): Klucz statusu ('success' lub 'error').
            filename (str): Nazwa pliku.
            details (str, optional): Dodatkowe informacje do wyświetlenia (np. ścieżka).
            size (int | None, optional): Rozmiar pliku w bajtach.
        """
        icon_map = {"success": "✅", "error": "❌"}
        style_map = {"success": "green", "error": "red"}
        
        size_str = f" [dim]({decimal(size)})[/]" if size is not None else ""
        details_str = f"\n   [dim]└─> {details}[/]" if details else ""
        
        self._recent_logs.appendleft(Text.from_markup(
            f"[{style_map.get(status, 'white')}]{icon_map.get(status, 'ℹ️')} {filename}{size_str}{details_str}[/]"
        ))

        if status == "success":
            self.counters['imported'] += 1
            if size: self.counters['total_size'] += size
        else:
            self.counters['errors'] += 1
        
        self.progress_bar.update(self._task_id, advance=1)
