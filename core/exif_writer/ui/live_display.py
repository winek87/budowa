# plik: core/exif_writer/ui/live_display.py
# Wersja 1.4 - W pełni funkcjonalny i informacyjny dashboard dla Zapisywarki EXIF.
# Opis: Zarządza dashboardem na żywo dla operacji zapisu metadanych,
#       prezentując szczegółowe informacje o postępie i każdej operacji.
# -*- coding: utf-8 -*-

from collections import deque
from pathlib import Path
from typing import Deque, Dict, List, Union

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


class ExifWriterLiveDisplay:
    """Zarządza dashboardem postępu dla operacji zapisu metadanych."""

    def __init__(self, total_items: int, title: str, console: Console):
        """
        Inicjalizuje dashboard.

        Args:
            total_items (int): Całkowita liczba plików do przetworzenia.
            title (str): Tytuł operacji wyświetlany na górze.
            console (Console): Instancja konsoli Rich.
        """
        self.console = console
        self._live: Live | None = None
        self._title_text = Text(f"✍️ {title} ✍️", justify="center", style="bold green")
        
        self._current_file_text = Text("Inicjalizacja...", style="yellow")
        self._current_folder_text = Text("-", style="dim")
        self._recent_logs: Deque[Panel] = deque(maxlen=5)
        self._counters = {"sukces": 0, "częściowy": 0, "błąd": 0, "pominięty": 0}
        self.total_files = total_items

        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), "•", TimeRemainingColumn()
        )
        self._task_id = self.progress_bar.add_task("Postęp", total=self.total_files)
        self._layout = self._build_layout()

    def _build_right_panel(self) -> Panel:
        """Tworzy prawe 'Centrum Dowodzenia' ze szczegółowymi statystykami."""
        processed_count = sum(self._counters.values())
        remaining_files = self.total_files - processed_count

        stats_table = Table.grid(padding=(0, 1))
        stats_table.add_column(style="dim", justify="right"); stats_table.add_column(style="bold")
        stats_table.add_row("Sukces:", f"[green]{self._counters['sukces']}[/]")
        stats_table.add_row("Częściowy:", f"[yellow]{self._counters['częściowy']}[/]")
        stats_table.add_row("Błąd:", f"[red]{self._counters['błąd']}[/]")
        stats_table.add_row("Pominięty:", f"[dim]{self._counters['pominięty']}[/]")
        
        progress_table = Table.grid(padding=(0,1))
        progress_table.add_column(style="dim", justify="right"); progress_table.add_column(style="bold")
        progress_table.add_row("Przetworzono:", f"[cyan]{processed_count}[/]")
        progress_table.add_row("Pozostało:", f"[bright_blue]{remaining_files}[/]")

        return Panel(
            Group(
                Rule("Aktualny Plik", style="dim"), self._current_file_text,
                Rule("Aktualny Folder", style="dim"), self._current_folder_text,
                Rule("Postęp Ogólny", style="dim"), progress_table,
                Rule("Statystyki Sesji", style="dim"), stats_table
            ),
            title="[bold green]Status Operacji[/]", border_style="green"
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
            Layout(Panel(Group(*self._recent_logs), title="[bold]Ostatnie Akcje[/]"), name="main"),
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
        summary = (f"[bold green]Proces zapisu zakończony![/]\n\n"
                   f"  - [green]Pełen sukces: {self._counters['sukces']}[/]\n"
                   f"  - [yellow]Częściowy sukces: {self._counters['częściowy']}[/]\n"
                   f"  - [red]Błędy: {self._counters['błąd']}[/]\n"
                   f"  - [dim]Pominięte: {self._counters['pominięty']}[/]\n")
        self.console.print(Panel(summary, title="Podsumowanie"))
            
    def _update_view(self):
        """Prywatna metoda do odświeżania całego interfejsu na żywo."""
        if self._live:
            self._layout["side"].update(self._build_right_panel())
            self._layout["main"].update(Panel(Group(*self._recent_logs), title="[bold]Ostatnie Akcje[/]"))
            self._live.refresh()

    def update_progress(self, status_key: str, file_path: Path, details: Union[Dict, List]):
        """
        Główna metoda aktualizująca stan dashboardu po przetworzeniu jednego pliku.

        Args:
            status_key (str): Klucz statusu ('sukces', 'błąd', 'pominięty' itd.).
            file_path (Path): Obiekt Path do przetworzonego pliku.
            details (Union[Dict, List]): Szczegóły operacji - lista zapisanych tagów
                                         lub słownik z błędem/informacją.
        """
        self._counters[status_key] += 1
        self._current_file_text.plain = file_path.name
        self._current_folder_text.plain = str(file_path.parent)
        
        style_map = {"sukces": "green", "częściowy": "yellow", "błąd": "red", "pominięty": "dim"}
        icon_map = {"sukces": "✅", "częściowy": "⚠️", "błąd": "❌", "pominięty": "⚫"}
        
        content = Text("")
        if isinstance(details, list):
            # Tworzymy czytelną tabelkę z zapisanymi tagami
            tags_table = Table.grid(padding=(0, 2))
            tags_table.add_column(style="dim")
            tags_table.add_column(style="white")
            for arg in details:
                parts = arg.split('=', 1)
                tag_name = parts[0].replace('-', '').replace('+=', '')
                tag_value = parts[1] if len(parts) > 1 else ""
                tags_table.add_row(f"- {tag_name}:", tag_value)
            content = tags_table
        elif isinstance(details, dict):
            # Wyświetlamy informację o błędzie lub pominięciu
            info_text = details.get("error") or details.get("info", "Brak szczegółów.")
            content = Text(info_text, style=style_map.get(status_key, "white"))

        panel_title = f"{icon_map.get(status_key, 'ℹ️')} {file_path.name}"
        panel = Panel(content, title=panel_title, border_style=style_map.get(status_key, "white"))
        self._recent_logs.appendleft(panel)
        
        self.progress_bar.update(self._task_id, advance=1)
        self._update_view()
