# plik: core/validator/ui.py
# Wersja 1.1 - Podzielony panel logów na "Znalezione" i "Przeskanowane"
# -*- coding: utf-8 -*-

from collections import deque
from typing import Dict, Any

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, SpinnerColumn, TextColumn,
                           TimeRemainingColumn)
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from rich.align import Align

class ValidatorLiveDisplay:
    """Zarządza dashboardem postępu dla zadań Walidatora."""

    def __init__(self, total_items: int, title: str):
        self.console = Console()
        self._live: Live | None = None
        self._total = total_items
        self._title = title
        
        self._processed_count = 0
        self._found_count = 0
        self._current_item = Text("Inicjalizacja...", style="dim")
        
        # === POCZĄTEK ZMIAN: Dwie oddzielne listy na logi ===
        self._recent_finds = deque(maxlen=10) # Lista na znalezione problemy
        self._recent_scans = deque(maxlen=50)  # Lista na ostatnio skanowane pliki
        # === KONIEC ZMIAN ===
        
        self._progress_bar = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.1f}%",
            TextColumn("({task.completed}/{task.total})"),
            "•",
            TimeRemainingColumn(),
        )
        self._task_id = self._progress_bar.add_task("Postęp", total=self._total)
        self._layout = self._build_layout()

    def _build_right_panel(self) -> Panel:
        stats_table = Table.grid(padding=(0, 1))
        stats_table.add_column(style="dim cyan", justify="right")
        stats_table.add_column(style="bold")
        stats_table.add_row("Przeskanowano:", str(self._processed_count))
        stats_table.add_row("Znaleziono:", f"[bold yellow]{self._found_count}[/]")

        return Panel(
            Group(
                Rule("Status", style="dim"),
                self._current_item,
                Rule("Statystyki", style="dim"),
                stats_table,
            ),
            title="Centrum Dowodzenia",
            border_style="blue"
        )

    def _build_layout(self) -> Layout:
        layout = Layout()
        layout.split(
            Layout(Panel(Text(self._title, justify="center", style="bold yellow")), name="header", size=3),
            Layout(self._progress_bar, name="progress", size=3),
            Layout(ratio=1, name="body")
        )

        # === POCZĄTEK ZMIAN: Podział lewego panelu ===
        left_panel_layout = Layout(name="left_panel")
        left_panel_layout.split_column(
            Layout(Panel(Group(*self._recent_finds), title="[yellow]Ostatnio Znalezione Problemy[/]"), name="finds"),
            Layout(Panel(Group(*self._recent_scans), title="Ostatnio Przeskanowane"), name="scans", ratio=1)
        )
        
        layout["body"].split_row(
            left_panel_layout,
            Layout(self._build_right_panel(), name="side", size=45)
        )
        # === KONIEC ZMIAN ===
        
        return layout

    def __enter__(self):
        self._live = Live(self._layout, screen=True, transient=False, auto_refresh=False)
        self._live.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live:
            self._live.stop()

    def refresh(self):
        if self._live:
            self._layout["side"].update(self._build_right_panel())
            # === POCZĄTEK ZMIAN: Aktualizacja obu paneli logów ===
            self._layout["finds"].update(Panel(Group(*self._recent_finds), title="[yellow]Ostatnio Znalezione Problemy[/]"))
            self._layout["scans"].update(Panel(Group(*self._recent_scans), title="Ostatnio Przeskanowane"))
            # === KONIEC ZMIAN ===
            self._live.refresh()
            
    def update_progress(self, current_item_text: str, found_item: str | None = None):
        self._processed_count += 1
        self._current_item = Text(f"...{current_item_text[-38:]}", style="cyan")
        
        # === POCZĄTEK ZMIAN: Dodawanie do odpowiednich list ===
        # Zawsze dodajemy do listy ostatnio skanowanych
        self._recent_scans.appendleft(Text(f"✓ {current_item_text}", style="dim"))
        
        if found_item:
            self._found_count += 1
            # Tylko jeśli coś znaleziono, dodajemy do listy problemów
            self._recent_finds.appendleft(Text.from_markup(f"[yellow]•[/] {found_item}"))
        # === KONIEC ZMIAN ===
        
        self._progress_bar.update(self._task_id, advance=1)
        self.refresh()
