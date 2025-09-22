# plik: core/image_fixer/ui.py
# Wersja 1.2 - Naprawiono błąd w postępie całkowitym i ulepszono logi.
# Opis: Ten moduł zawiera dedykowane klasy `LiveDisplay` do zarządzania
#       dashboardami dla obu etapów pracy Naprawiacza Obrazów: diagnostyki
#       i właściwej naprawy.
# -*- coding: utf-8 -*-

from collections import deque
from pathlib import Path
from typing import Deque, List

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


class ImageDiagnosticsLiveDisplay:
    """Zarządza ulepszonym, dwukolumnowym dashboardem dla diagnostyki obrazów."""

    def __init__(self, total_items: int, test_names: List[str], console: Console):
        """
        Inicjalizuje dashboard diagnostyczny.

        Args:
            total_items (int): Liczba obrazów do przeanalizowania.
            test_names (List[str]): Lista nazw testów, które zostaną uruchomione.
            console (Console): Instancja konsoli Rich.
        """
        self.console = console
        self._live: Live | None = None
        self._title = Text("🛠️ Dashboard Diagnostyczny Obrazów 🛠️", justify="center", style="bold yellow")
        
        # Inicjalizujemy paski postępu z poprawną liczbą operacji
        self.overall_progress = Progress(
            TextColumn("[bold]Postęp całkowity:[/]", justify="right"), BarColumn(),
            TextColumn("{task.completed}/{task.total} operacji")
        )
        self.overall_task = self.overall_progress.add_task("Całość", total=total_items * len(test_names))
        
        self.test_progress = Progress(
            TextColumn("[cyan]{task.description}", justify="right"), BarColumn(),
            TextColumn("{task.completed}/{task.total}")
        )
        self.test_task = self.test_progress.add_task("Oczekiwanie...", total=total_items)
        
        self.recent_logs: Deque[Text] = deque(maxlen=15)
        self.stats = {name: 0 for name in test_names}
        self.total_problems = 0
        self._layout = self._build_layout()

    def _build_right_panel(self) -> Panel:
        """Tworzy prawe 'Centrum Dowodzenia' ze statystykami."""
        table = Table.grid(padding=(0, 1))
        table.add_column(style="dim", justify="right")
        table.add_column(style="bold red")
        
        for test_name, error_count in self.stats.items():
            table.add_row(f"{test_name}:", str(error_count))
            
        return Panel(
            Group(
                Rule("Błędy wg testu", style="dim"),
                table,
                Rule("Podsumowanie", style="dim"),
                Text.from_markup(f"Łącznie problemów: [bold red]{self.total_problems}[/]", justify="right")
            ),
            title="[bold yellow]Wyniki[/]", border_style="yellow"
        )

    def _build_layout(self) -> Layout:
        """Tworzy spójny, dwukolumnowy layout."""
        progress_group = Group(self.overall_progress, self.test_progress)
        
        layout = Layout()
        footer = Panel(
            Align.center(Text.from_markup("Diagnostyka w toku... Naciśnij [bold cyan]Enter[/], aby kontynuować po zakończeniu.")),
            border_style="dim"
        )
        
        layout.split(
            Layout(Panel(self._title), size=3),
            Layout(progress_group, size=4),
            Layout(ratio=1, name="body"),
            Layout(footer, name="footer", size=3)
        )
        layout["body"].split_row(
            Layout(Panel(Group(*self.recent_logs), title="[bold]Ostatnio Wykryte Problemy[/]"), name="main"),
            Layout(self._build_right_panel(), name="side", size=45)
        )
        return layout

    def __enter__(self):
        """Uruchamia `rich.Live` z `transient=False`, aby dashboard pozostał widoczny."""
        self._live = Live(self._build_layout(), screen=True, transient=False, auto_refresh=False, console=self.console)
        self._live.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Zatrzymuje `rich.Live` po wyjściu z bloku `with`."""
        if self._live:
            self._live.stop()
        
    def update(self, test_name: str, filename: str, error: str | None):
        """Aktualizuje dashboard o wynik analizy jednego pliku."""
        if error:
            # Usuwamy zbędne prefixy z komunikatu błędu dla większej czytelności
            clean_error = error.replace("Exiftool:", "").replace("Pillow Verify:", "").replace("OpenCV:", "").replace("Pillow Load:", "").strip()
            log_entry_text = f"[cyan]{filename}[/]: [yellow]{test_name}[/] - [dim]{clean_error[:80]}[/dim]"
            
            # Sprawdzamy, czy ten sam błąd nie został już dodany do logów
            if not any(log_entry_text in str(log) for log in self.recent_logs):
                # Zliczamy tylko unikalne problemy (plik + typ testu)
                if not any(f"{filename}[/]: [yellow]{test_name}[/]" in str(log) for log in self.recent_logs):
                    self.stats[test_name] += 1
                    self.total_problems = sum(self.stats.values())
                self.recent_logs.appendleft(Text.from_markup(log_entry_text))
        
        self.test_progress.update(self.test_task, advance=1)
        self.overall_progress.update(self.overall_task, advance=1)
        
        if self._live:
            self._live.update(self._build_layout(), refresh=True)

    def switch_test(self, test_name: str, total_items: int):
        """Przygotowuje dashboard na rozpoczęcie nowego testu."""
        self.test_progress.reset(self.test_task)
        self.test_progress.update(self.test_task, description=test_name, total=total_items)


class ImageFixerLiveDisplay:
    """Zarządza dashboardem na żywo dla procesu naprawy obrazów."""

    def __init__(self, total_items: int, engine: str, backup_dir: Path, console: Console):
        self.console = console
        self._live: Live | None = None
        self._title = Text(f"🛠️ Dashboard Naprawczy (Silnik: {engine}) 🛠️", justify="center", style="bold yellow")
        
        self.progress = Progress(
            SpinnerColumn(), TextColumn("[green]{task.description}"), BarColumn(),
            "{task.completed}/{task.total}", TimeRemainingColumn()
        )
        self._task_id = self.progress.add_task("Naprawiam...", total=total_items)
        
        self.action_logs: Deque[Text] = deque(maxlen=8)
        self.error_logs: Deque[Text] = deque(maxlen=8)
        self.stats = {"fixed": 0, "failed": 0}
        self.backup_dir_str = str(backup_dir)
        self._layout = self._build_layout()

    def _build_layout(self) -> Layout:
        """Tworzy dwupanelowy layout do wyświetlania logów i błędów."""
        summary = Panel(f"Naprawiono: [green]{self.stats['fixed']}[/]\nBłędy: [red]{self.stats['failed']}[/]\n[dim]Kopie w: {self.backup_dir_str}[/dim]")
        
        logs_grid = Table.grid(expand=True)
        logs_grid.add_column(ratio=1); logs_grid.add_column(ratio=1)
        logs_grid.add_row(
            Panel(Group(*self.action_logs), title="[bold blue]Logi Akcji[/]"),
            Panel(Group(*self.error_logs), title="[bold red]Logi Błędów[/]")
        )
        layout = Layout()
        layout.split_column(
            Layout(Panel(self._title), size=3),
            Layout(self.progress, size=3),
            Layout(summary, size=5),
            logs_grid
        )
        return layout

    def __enter__(self):
        """Uruchamia `rich.Live` z `transient=True`, aby dashboard zniknął po naprawie."""
        self._live = Live(self._build_layout(), screen=True, transient=True, auto_refresh=False, console=self.console)
        self._live.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live:
            self._live.stop()
        
    def update(self, status: str, filename: str, details: str = ""):
        """Aktualizuje dashboard o status naprawy jednego pliku."""
        if status == "success":
            self.stats['fixed'] += 1
            self.action_logs.appendleft(Text(f"✅ {filename}", style="green"))
        else:
            self.stats['failed'] += 1
            self.action_logs.appendleft(Text(f"❌ {filename}", style="red"))
            self.error_logs.appendleft(Text(f"Błąd ({filename}): {details}", style="red"))
        
        self.progress.update(self._task_id, advance=1)
        
        if self._live:
            self._live.update(self._build_layout(), refresh=True)
