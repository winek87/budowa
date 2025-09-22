# plik: core/scanner/ui/live_display.py
# Wersja 2.8 (Ulepszona) - Dodano licznik "ukończono/wszystkie" do paska postępu
# -*- coding: utf-8 -*-

import json
import time
from collections import deque
from typing import Dict, Any

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

class ScannerLiveDisplay:
    """Zarządza dashboardem postępu skanowania ze szczegółowymi logami."""
    
    def __init__(self, total_urls: int, title: str, console: Console):
        self.console = console
        self._live: Live | None = None
        self._total = total_urls
        self._title = title
        
        self._start_time = time.time()
        self._processed_count = 0
        self._current_url = Text("Inicjalizacja...", style="dim")
        self._last_batch_save_time = "N/A"
        self._recent_logs = deque(maxlen=5)
        self._counters = {"poprawne": 0, "błędy": 0}
        
        # === POCZĄTEK ZMIAN: Dodanie licznika do paska postępu ===
        self._progress_bar = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.1f}%",
            # Dodajemy nową kolumnę z licznikiem
            TextColumn("({task.completed} z {task.total})"),
            "•",
            TimeRemainingColumn(),
            console=self.console
        )
        # === KONIEC ZMIAN ===
        
        self._task_id = self._progress_bar.add_task("Postęp", total=self._total)
        self._layout = self._build_layout()

    # (Reszta pliku pozostaje bez zmian, poniżej dla kompletności)
    def _build_right_panel(self) -> Panel:
        elapsed_time = time.time() - self._start_time
        speed = (self._processed_count / elapsed_time * 60) if elapsed_time > 0 else 0
        stats_table = Table.grid(padding=(0, 1)); stats_table.add_column(style="dim cyan", justify="right"); stats_table.add_column(style="bold")
        stats_table.add_row("Prędkość:", f"{speed:.1f} URL/min"); stats_table.add_row("Poprawne:", f"[green]{self._counters['poprawne']}[/]"); stats_table.add_row("Błędy:", f"[red]{self._counters['błędy']}[/]"); stats_table.add_row("Zapisano:", self._last_batch_save_time)
        return Panel(Group(Rule("Status", style="dim"), self._current_url, Rule("Statystyki", style="dim"), stats_table), title="Centrum Dowodzenia", border_style="blue")

    def _build_layout(self) -> Layout:
        layout = Layout(); layout.split(Layout(Panel(Text(self._title, justify="center", style="bold cyan")), name="header", size=3), Layout(self._progress_bar, name="progress", size=3), Layout(ratio=1, name="body"))
        layout["body"].split_row(Layout(Panel(Group(*self._recent_logs), title="Ostatnie Akcje"), name="main"), Layout(self._build_right_panel(), name="side", size=45))
        return layout

    def __enter__(self):
        self._live = Live(self._layout, screen=True, transient=False, auto_refresh=False, console=self.console)
        self._live.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live: self._live.stop()
        self.console.print(Panel(f"[bold green]Skanowanie zakończone![/]\n\n  - [green]Poprawne: {self._counters['poprawne']}[/]\n  - [red]Błędy: {self._counters['błędy']}[/]\n", title="Podsumowanie"))

    def refresh(self):
        if self._live:
            self._layout["side"].update(self._build_right_panel())
            self._layout["main"].update(Panel(Group(*self._recent_logs), title="Ostatnie Akcje"))
            self._live.refresh()
            
    def set_batch_save_time(self): self._last_batch_save_time = time.strftime("%H:%M:%S")
    def update_current_url(self, url: str): self._current_url = Text(f"...{url[-38:]}", style="yellow"); self.refresh()
    
    def update_success(self, url: str, details: Dict[str, Any]):
        self._processed_count += 1
        self._counters["poprawne"] += 1
        result_table = Table(show_header=False, box=None, padding=0, expand=True); result_table.add_column(style="cyan", justify="right", width=25); result_table.add_column()
        for key, value in details.items():
            val_str = "";
            if isinstance(value, list): val_str = "\n".join(f"- {item}" for item in value)
            elif isinstance(value, dict): val_str = json.dumps(value, indent=2, ensure_ascii=False)
            else: val_str = str(value)
            result_table.add_row(f"{key}:", val_str)
        self._recent_logs.appendleft(Panel(result_table, title=f"[bold green]✅ Sukces![/] ...{url[-40:]}"))
        self._progress_bar.update(self._task_id, advance=1); self.refresh()
    
    def update_error(self, url: str, error_message: str):
        self._processed_count += 1
        self._counters["błędy"] += 1
        self._recent_logs.appendleft(Panel(f"Nie udało się pobrać danych.\nBłąd: {error_message[:150]}...", title=f"[bold red]❌ Błąd![/] ...{url[-40:]}", border_style="red"))
        self._progress_bar.update(self._task_id, advance=1); self.refresh()
