# plik: core/local_scanner/ui/live_display.py
# Wersja 1.2 - W pełni funkcjonalny i informacyjny dashboard dla Lokalnego Importera.
# Opis: Zarządza dashboardem na żywo dla operacji importu/indeksowania plików,
#       zapewniając spójny interfejs z innymi modułami aplikacji.
# -*- coding: utf-8 -*-

import time
from collections import deque
from pathlib import Path
from typing import Deque

from rich.align import Align
from rich.console import Console, Group
from rich.filesize import decimal
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, SpinnerColumn, TextColumn,
                           TimeRemainingColumn)
from rich.rule import Rule
from rich.text import Text


class LocalScannerLiveDisplay:
    """Zarządza dashboardem postępu dla operacji na plikach lokalnych."""

    def __init__(self, total_items: int, title: str, console: Console):
        """
        Inicjalizuje dashboard.

        Args:
            total_items (int): Wstępna całkowita liczba plików do przetworzenia.
                               Może być 0, jeśli liczba jest jeszcze nieznana.
            title (str): Tytuł operacji wyświetlany na górze.
            console (Console): Instancja konsoli Rich.
        """
        self.console = console
        self._live: Live | None = None
        self._title_text = Text(title, justify="center", style="bold blue")
        
        self._start_time = time.time()
        self._processed_count = 0
        self._current_file_text = Text("Inicjalizacja...", style="yellow")
        self._recent_logs: Deque[Text] = deque(maxlen=8)
        self._counters = {"zaimportowane": 0, "pominięte": 0, "błędy": 0}

        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), "•", TimeRemainingColumn()
        )
        self._task_id = self.progress_bar.add_task("Wyszukiwanie plików...", total=total_items or None)
        self._layout = self._build_layout()

    def _build_right_panel(self) -> Panel:
        """Tworzy prawe 'Centrum Dowodzenia' ze szczegółowymi statystykami."""
        elapsed_time = time.time() - self._start_time
        speed = (self._processed_count / elapsed_time) if elapsed_time > 0 else 0
        
        op_text = "Zaimportowano" if "Import" in self._title_text.plain else "Zaindeksowano"

        content_group = Group(
            Rule("Aktualny Plik", style="dim"), self._current_file_text,
            Rule("Statystyki", style="dim"),
            Text.from_markup(f" {op_text}: [bold cyan]{self._counters['zaimportowane']}[/]", justify="right"),
            Text.from_markup(f" Pominięto: [bold yellow]{self._counters['pominięte']}[/]", justify="right"),
            Text.from_markup(f" Błędy: [bold red]{self._counters['błędy']}[/]", justify="right"),
            Rule(style="dim"),
            Text.from_markup(f" Prędkość: [bold magenta]{speed:.1f} plików/s[/]", justify="right")
        )
        return Panel(content_group, title="[bold blue]Status Operacji[/]", border_style="blue")

    def _build_layout(self) -> Layout:
        """Tworzy główny, dwukolumnowy layout dashboardu z nagłówkiem, ciałem i stopką."""
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
            Layout(Panel(Group(*self._recent_logs), title="[bold green]Ostatnie Akcje[/]"), name="main"),
            Layout(self._build_right_panel(), name="side", size=50)
        )
        return layout

    def __enter__(self):
        """Uruchamia `rich.Live` przy wejściu do bloku `with`."""
        self._live = Live(self._layout, screen=True, transient=True, auto_refresh=False, console=self.console)
        self._live.start(); return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Zatrzymuje `rich.Live` i wyświetla podsumowanie przy wyjściu z bloku `with`."""
        if self._live: self._live.stop()
        op_text = "Import" if "Import" in self._title_text.plain else "Indeksowanie"
        summary_text = (
            f"[bold green]{op_text} zakończony![/]\n\n"
            f"  - [green]Nowe pliki: {self._counters['zaimportowane']}[/]\n"
            f"  - [yellow]Pominięte: {self._counters['pominięte']}[/]\n"
            f"  - [red]Błędy: {self._counters['błędy']}[/]\n"
        )
        self.console.print(Panel(summary_text, title="Podsumowanie"))

    def _update_view(self):
        """Prywatna metoda do odświeżania całego interfejsu na żywo."""
        if self._live:
            self._layout["side"].update(self._build_right_panel())
            self._layout["main"].update(Panel(Group(*self._recent_logs), title="[bold green]Ostatnie Akcje[/]"))
            self._live.refresh()
            
    def _add_log_entry(self, icon: str, file_path: Path, style: str, size: int | None = None):
        """Dodaje sformatowany wpis do panelu logów."""
        size_str = f" [dim]({decimal(size)})[/]" if size is not None else ""
        log_text = Text.from_markup(f"[{style}]{icon} {file_path.name}{size_str}[/]")
        self._recent_logs.appendleft(log_text)

    def update_progress(self, status_key: str, file_path: Path, file_size: int | None = None):
        """
        Główna metoda aktualizująca stan dashboardu po przetworzeniu jednego pliku.

        Args:
            status_key (str): Klucz statusu ('zaimportowane', 'pominięte', 'błędy').
            file_path (Path): Obiekt Path do przetworzonego pliku.
            file_size (int | None, optional): Rozmiar pliku w bajtach.
        """
        self._processed_count += 1
        self._counters[status_key] += 1
        
        self._current_file_text.plain = file_path.name
        
        icon_map = {"zaimportowane": "✅", "pominięte": "🟡", "błędy": "❌"}
        style_map = {"zaimportowane": "green", "pominięte": "yellow", "błędy": "red"}
        
        self._add_log_entry(icon_map.get(status_key, "ℹ️"), file_path, style_map.get(status_key, "white"), file_size)
        
        self.progress_bar.update(self._task_id, advance=1)
        self._update_view()
