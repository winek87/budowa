# plik: core/advanced_recovery/ui/live_display.py
# Wersja 1.1 - W pełni funkcjonalny i informacyjny dashboard dla Zaawansowanego Silnika Naprawy.
# Opis: Zarządza dashboardem na żywo dla operacji "potrząśnięcia i ponowienia",
#       zapewniając spójny interfejs zgodny ze standardem aplikacji.
# -*- coding: utf-8 -*-

import time
from collections import deque
from typing import Deque

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, SpinnerColumn, TextColumn,
                           TimeRemainingColumn)
from rich.rule import Rule
from rich.text import Text


class AdvancedRecoveryLiveDisplay:
    """Zarządza dashboardem postępu dla Zaawansowanej Naprawy."""

    def __init__(self, total_items: int, console: Console):
        """
        Inicjalizuje dashboard.

        Args:
            total_items (int): Całkowita liczba URL-i do przetworzenia.
            console (Console): Instancja konsoli Rich.
        """
        self.console = console
        self._live: Live | None = None
        self._title_text = Text("🌀 Zaawansowany Silnik Naprawy (Shake & Retry) 🌀", justify="center", style="bold magenta")
        
        self._start_time = time.time()
        self._processed_count = 0
        self._status_message_text = Text("Inicjalizacja...", style="yellow")
        self._recent_logs: Deque[Text] = deque(maxlen=8)
        self._counters = {"sukcesy": 0, "porażki": 0}

        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%", "•",
            TextColumn("Sukcesy: [green]{task.fields[sukcesy]}[/]"), "•",
            TextColumn("Porażki: [red]{task.fields[porażki]}[/]"), "•",
            TimeRemainingColumn()
        )
        self._task_id = self.progress_bar.add_task("Naprawianie...", total=total_items, sukcesy=0, porażki=0)
        self._layout = self._build_layout()

    def _build_right_panel(self) -> Panel:
        """Tworzy prawe 'Centrum Dowodzenia' ze szczegółowymi statystykami."""
        elapsed_time = time.time() - self._start_time
        speed = (self._processed_count / elapsed_time * 60) if elapsed_time > 0 else 0

        content_group = Group(
            Rule("Status Akcji", style="dim"), self._status_message_text,
            Rule("Statystyki", style="dim"),
            Text.from_markup(f" Sukcesy: [green]{self._counters['sukcesy']}[/]", justify="right"),
            Text.from_markup(f" Porażki: [red]{self._counters['porażki']}[/]", justify="right"),
            Rule(style="dim"),
            Text.from_markup(f" Prędkość: [magenta]{speed:.1f} URL/min[/]", justify="right")
        )
        return Panel(content_group, title="[bold magenta]Status Operacji[/]", border_style="magenta")

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
        summary_text = (
            f"[bold magenta]Naprawa zakończona![/]\n\n"
            f"  - [green]Naprawiono pomyślnie: {self._counters['sukcesy']}[/]\n"
            f"  - [red]Nadal z błędem: {self._counters['porażki']}[/]\n"
        )
        self.console.print(Panel(summary_text, title="Podsumowanie"))

    def _update_view(self):
        """Prywatna metoda do odświeżania całego interfejsu na żywo."""
        if self._live:
            self._layout["side"].update(self._build_right_panel())
            self._layout["main"].update(Panel(Group(*self._recent_logs), title="[bold green]Ostatnie Akcje[/]"))
            self._live.refresh()
            
    def update_status_message(self, message: str):
        """
        Aktualizuje komunikat statusu w 'Centrum Dowodzenia' i odświeża dashboard.
        """
        self._status_message_text.plain = message
        self._update_view()

    def add_log_entry(self, message: str, style: str = "white"):
        """Dodaje sformatowany wpis do panelu logów."""
        self._recent_logs.appendleft(Text(message, style=style))
        self._update_view()

    def update_progress(self, status_key: str, url: str):
        """
        Główna metoda aktualizująca stan dashboardu po przetworzeniu jednego URL-a.

        Args:
            status_key (str): Klucz statusu ('sukcesy' lub 'porażki').
            url (str): Adres URL, którego dotyczyła operacja.
        """
        self._processed_count += 1
        self._counters[status_key] += 1
        
        icon = "✅" if status_key == "sukcesy" else "❌"
        style = "green" if status_key == "sukcesy" else "red"
        self.add_log_entry(f"{icon} ...{url[-45:]}", style=style)
        
        self.progress_bar.update(self._task_id, advance=1, **self._counters)
        self._update_view()
