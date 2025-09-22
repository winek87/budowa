# plik: core/downloader/ui/live_display.py
# Wersja 2.4 - W pełni funkcjonalny, informacyjny i spójny dashboard dla modułu Downloader.
# Opis: Ta klasa zarządza dashboardem na żywo dla operacji pobierania,
#       zapewniając spójny, dwukolumnowy interfejs zgodny ze standardem aplikacji.
# -*- coding: utf-8 -*-

import time
from collections import deque
from typing import Deque, Dict

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, SpinnerColumn, TextColumn,
                           TimeRemainingColumn)
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

# Importujemy funkcję tworzącą panel z naszego pakietu UI
from .panels import create_summary_panel


class DownloaderLiveDisplay:
    """Zarządza dashboardem postępu dla operacji pobierania w stylu Skanera."""

    def __init__(self, total_items: int, title: str, console: Console):
        """
        Inicjalizuje dashboard.

        Args:
            total_items (int): Całkowita liczba plików do przetworzenia (używane w trybie naprawy).
            title (str): Tytuł operacji wyświetlany na górze.
            console (Console): Instancja konsoli Rich.
        """
        self.console = console
        self._live: Live | None = None
        self._total = total_items
        self._title_text = Text(title, justify="center", style="bold cyan")
        
        self._start_time = time.time()
        self._processed_count = 0
        
        self._current_url_text = Text("Inicjalizacja...", style="dim white")
        self._status_message_text = Text("Oczekiwanie na rozpoczęcie...", style="bold yellow")
        
        self._recent_summaries: Deque[Panel] = deque(maxlen=5)
        self._counters = {"pobrane": 0, "pominięte": 0, "błędy": 0}

        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%", "•",
            TextColumn("Pobrane: [green]{task.fields[pobrane]}[/]"), "•",
            TextColumn("Pominięte: [yellow]{task.fields[pominięte]}[/]"), "•",
            TextColumn("Błędy: [red]{task.fields[bledy]}[/]"), "•",
            TimeRemainingColumn()
        )
        self._task_id = self.progress_bar.add_task("Postęp", total=self._total or None, pobrane=0, pominięte=0, bledy=0)
        self._layout = self._build_layout()

    def _build_right_panel(self) -> Panel:
        """Tworzy prawe 'Centrum Dowodzenia' ze szczegółowymi statystykami."""
        elapsed_time = time.time() - self._start_time
        speed = (self._processed_count / elapsed_time * 60) if elapsed_time > 0 else 0
        
        stats_table = Table.grid(padding=(0, 1))
        stats_table.add_column(style="dim cyan", justify="right"); stats_table.add_column(style="bold")
        stats_table.add_row("Prędkość:", f"{speed:.1f} URL/min")
        stats_table.add_row("Pobrane:", f"[green]{self._counters['pobrane']}[/]")
        stats_table.add_row("Pominięte:", f"[yellow]{self._counters['pominięte']}[/]")
        stats_table.add_row("Błędy:", f"[red]{self._counters['błędy']}[/]")

        return Panel(
            Group(
                Rule("Status Akcji", style="dim"), self._status_message_text,
                Rule("Aktualny URL", style="dim"), self._current_url_text,
                Rule("Statystyki", style="dim"), stats_table
            ),
            title="[bold blue]Centrum Dowodzenia[/]", border_style="blue"
        )

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
            Layout(Panel(Group(*self._recent_summaries), title="[bold green]Ostatnie Operacje[/]"), name="main"),
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
        summary_text = (
            f"[bold green]Pobieranie zakończone![/]\n\n"
            f"  - [green]Pobrane: {self._counters['pobrane']}[/]\n"
            f"  - [yellow]Pominięte: {self._counters['pominięte']}[/]\n"
            f"  - [red]Błędy: {self._counters['błędy']}[/]\n"
        )
        self.console.print(Panel(summary_text, title="Podsumowanie"))

    def refresh(self):
        """Prywatna metoda do odświeżania całego interfejsu na żywo."""
        if self._live:
            self._layout["side"].update(self._build_right_panel())
            self._layout["main"].update(Panel(Group(*self._recent_summaries), title="[bold green]Ostatnie Operacje[/]"))
            self._live.refresh()

    def update_current_url(self, url: str):
        """Aktualizuje tylko tekst z adresem URL w panelu bocznym."""
        self._current_url_text.plain = f"...{url[-42:]}"
        # Celowo nie odświeżamy tutaj, robi to `update_status_message`

    def update_status_message(self, message: str):
        """Aktualizuje komunikat statusu i odświeża cały dashboard."""
        self._status_message_text.plain = message
        self.refresh()

    def _update_progress(self, status_key: str, url: str, metadata: Dict):
        """Wewnętrzna metoda do aktualizacji stanu po każdej operacji."""
        self._processed_count += 1
        self._counters[status_key] += 1
        
        panel = create_summary_panel(url, status_key, metadata)
        self._recent_summaries.appendleft(panel)
        
        self.progress_bar.update(self._task_id, advance=1, **self._counters)
    
    def update_downloaded(self, url: str, metadata: Dict):
        """Pełna obsługa pomyślnie pobranego pliku."""
        self.update_current_url(url)
        self._update_progress("pobrane", url, metadata)
        self.update_status_message("✅ Sukces! Czekam na następny...")

    def update_skipped(self, url: str, metadata: Dict):
        """Pełna obsługa pominiętego pliku."""
        self.update_current_url(url)
        self._update_progress("pominięte", url, metadata)
        self.update_status_message("🟡 Pominięto. Czekam na następny...")

    def update_failed(self, url: str, metadata: Dict):
        """Pełna obsługa pliku, który zwrócił błąd."""
        self.update_current_url(url)
        self._update_progress("błędy", url, metadata)
        self.update_status_message("❌ Błąd! Czekam na następny...")
