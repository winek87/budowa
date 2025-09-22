# plik: core/smart_archiver/ui/live_display.py
# Wersja 1.2 - Naprawiono aktualizację liczników i dodano szczegółowe logi.
# Opis: Zarządza dashboardem na żywo na czas analizy obrazów.
# -*- coding: utf-8 -*-

from collections import deque
from pathlib import Path
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


class ArchiverAnalysisLiveDisplay:
    """Zarządza dashboardem postępu dla analizy obrazów."""

    def __init__(self, total_items: int, console: Console):
        self.console = console
        self._live: Live | None = None
        self._title_text = Text("🧹 Analiza Obrazów (Asystent Porządkowania) 🧹", justify="center", style="bold blue")
        
        self._recent_finds: Deque[Text] = deque(maxlen=40)
        self.counters = {"blurry": 0, "dark": 0, "small": 0, "corrupted": 0}

        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(),
            TextColumn("{task.completed}/{task.total}"), "[progress.percentage]{task.percentage:>3.0f}%", "•",
            TimeRemainingColumn()
        )
        self._task_id = self.progress_bar.add_task("Analiza (wieloprocesowa)...", total=total_items)
        self._layout = self._build_layout()

    def _build_right_panel(self) -> Panel:
        """Tworzy prawe 'Centrum Dowodzenia' ze statystykami znalezionych problemów."""
        content = Group(
            Rule("Znalezione Problemy", style="dim"),
            Text.from_markup(f" Nieostre: [bold yellow]{self.counters['blurry']}[/]", justify="right"),
            Text.from_markup(f" Ciemne: [bold bright_black]{self.counters['dark']}[/]", justify="right"),
            Text.from_markup(f" Małe pliki: [bold cyan]{self.counters['small']}[/]", justify="right"),
            Text.from_markup(f" Uszkodzone: [bold red]{self.counters['corrupted']}[/]", justify="right"),
        )
        return Panel(content, title="[bold blue]Status Operacji[/]", border_style="blue")

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
            Layout(Panel(Group(*self._recent_finds), title="[bold]Ostatnio Znalezione[/]"), name="main"),
            Layout(self._build_right_panel(), name="side", size=40)
        )
        return layout

    def __enter__(self):
        self._live = Live(self._layout, screen=True, transient=True, auto_refresh=False, console=self.console)
        self._live.start(); return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live: self._live.stop()
    
    def _update_view(self):
        """Wewnętrzna, prywatna metoda do ręcznego odświeżania całego widoku."""
        if self._live:
            # Musimy jawnie przebudować prawy panel, aby zaktualizować liczniki
            self._layout["side"].update(self._build_right_panel())
            # Odświeżamy cały layout, w tym lewy panel z logami
            self._live.update(self._build_layout(), refresh=True)
            
    def update_with_result(self, result: Dict):
        """
        Główna metoda, która aktualizuje cały dashboard na podstawie wyniku
        analizy pojedynczego pliku.
        """
        found_problems = []
        if result.get('is_blurry'):
            self.counters['blurry'] += 1
            found_problems.append("[yellow]Nieostre[/]")
        if result.get('is_dark'):
            self.counters['dark'] += 1
            found_problems.append("[bright_black]Ciemne[/]")
        if result.get('is_small'):
            self.counters['small'] += 1
            found_problems.append("[cyan]Mały plik[/]")
        if result.get('is_corrupted'):
            self.counters['corrupted'] += 1
            found_problems.append("[red]Uszkodzone[/]")
            
        # Dodajemy wpis do logów TYLKO jeśli znaleziono problem
        if found_problems:
            log_table = Table.grid(expand=True, padding=(0, 1))
            log_table.add_column(style="white"); log_table.add_column(ratio=1, justify="left")
            
            # Skracamy ścieżkę, aby zmieściła się w panelu
            path_obj = result['path']
            display_path = path_obj.name if len(str(path_obj)) < 40 else f".../{'/'.join(path_obj.parts[-2:])}"
            
            log_table.add_row(
                f"🟡 {display_path}",
                f"[dim]({', '.join(found_problems)})[/]"
            )
            self._recent_finds.appendleft(log_table)
            
        self.progress_bar.update(self._task_id, advance=1)
        
        # Ręcznie odświeżamy cały widok po każdej aktualizacji
        self._update_view()
