# plik: core/backup_manager/ui/live_display.py
# Wersja 1.3 - W pełni funkcjonalny i informacyjny dashboard dla Menedżera Kopii Zapasowych.
# Opis: Zarządza dashboardem na żywo dla operacji tworzenia archiwum,
#       prezentując szczegółowe informacje o postępie w czasie rzeczywistym.
# -*- coding: utf-8 -*-

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


class BackupLiveDisplay:
    """Zarządza dashboardem postępu dla operacji tworzenia kopii zapasowej."""

    def __init__(self, title: str, console: Console):
        """
        Inicjalizuje dashboard.

        Args:
            title (str): Tytuł operacji wyświetlany na górze.
            console (Console): Instancja konsoli Rich.
        """
        self.console = console
        self._live: Live | None = None
        self._title_text = Text(title, justify="center", style="bold green")
        
        self._current_folder_text = Text("Inicjalizacja...", style="yellow")
        self._recent_logs: Deque[Text] = deque(maxlen=8)
        self.processed_files = 0
        self.total_size = 0
        self.total_files = 0

        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), "•", TimeRemainingColumn()
        )
        self._task_id = self.progress_bar.add_task("Pakowanie...", total=None)
        self._layout = self._build_layout()

    def _build_right_panel(self) -> Panel:
        """Tworzy prawe 'Centrum Dowodzenia' ze szczegółowymi statystykami."""
        remaining_files = self.total_files - self.processed_files
        
        content = Group(
            Rule("Aktualny Folder", style="dim"), self._current_folder_text,
            Rule("Statystyki", style="dim"),
            Text.from_markup(f" Spakowane pliki: [bold cyan]{self.processed_files}[/]", justify="right"),
            Text.from_markup(f" Pozostało: [bold bright_blue]{remaining_files}[/]", justify="right"),
            Text.from_markup(f" Rozmiar archiwum: [bold yellow]{decimal(self.total_size)}[/]", justify="right")
        )
        return Panel(content, title="[bold green]Status Operacji[/]", border_style="green")

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
            Layout(Panel(Group(*self._recent_logs), title="[bold]Ostatnio Dodane[/]"), name="main"),
            Layout(self._build_right_panel(), name="side", size=50)
        )
        return layout

    def __enter__(self):
        """Uruchamia `rich.Live` przy wejściu do bloku `with`."""
        self._live = Live(self._layout, screen=True, transient=True, auto_refresh=False, console=self.console)
        self._live.start(); return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Zatrzymuje `rich.Live` przy wyjściu z bloku `with`."""
        if self._live: self._live.stop()
        
    def _update_view(self):
        """Prywatna metoda do odświeżania całego interfejsu na żywo."""
        if self._live:
            self._layout["side"].update(self._build_right_panel())
            self._layout["main"].update(Panel(Group(*self._recent_logs), title="[bold]Ostatnio Dodane[/]"))
            self._live.refresh()
            
    def update_progress(self, arcname: str, file_size: int):
        """
        Główna metoda aktualizująca stan dashboardu po spakowaniu jednego pliku.

        Args:
            arcname (str): Względna ścieżka pliku wewnątrz archiwum.
            file_size (int): Rozmiar pliku w bajtach.
        """
        if self.processed_files == 0:
            # Przy pierwszym wywołaniu ustawiamy całkowitą liczbę plików
            # na podstawie wartości ustawionej w pasku postępu
            self.total_files = self.progress_bar.tasks[0].total if self.progress_bar.tasks else 0
        
        self.processed_files += 1
        self.total_size += file_size
        
        # Wyświetlamy folder nadrzędny jako "Aktualny Folder"
        self._current_folder_text.plain = str(Path(arcname).parent)
        
        # Dodajemy pełną ścieżkę do panelu logów
        log_text = Text.from_markup(f"✅ [green]{arcname}[/] [dim]({decimal(file_size)})[/]")
        self._recent_logs.appendleft(log_text)
        
        self.progress_bar.update(self._task_id, advance=1, description=f"Pakowanie: [dim]{Path(arcname).name}[/dim]")
        self._update_view()
