# plik: core/code_analyzer/ui.py
# Wersja 1.0 - Komponenty UI dla Audytora Kodu.
# Opis: Ten moduł zawiera funkcje generujące interaktywne i statyczne
#       widoki wyników dla analizy kodu.
# -*- coding: utf-8 -*-

import asyncio
from pathlib import Path
from typing import List

from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# Importujemy naszą dedykowaną funkcję do obsługi klawiszy
from ..attribute_explorer.utils import get_key

console = Console()


async def display_scrollable_linter_results(linter_results: List[str]):
    """
    Wyświetla wyniki lintera w interaktywnym, przewijalnym dashboardzie.
    """
    scroll_pos = 0
    
    def build_layout() -> Layout:
        visible_rows = console.height - 10
        start_index = max(0, min(scroll_pos, len(linter_results) - visible_rows if len(linter_results) > visible_rows else 0))
        visible_data = linter_results[start_index : start_index + visible_rows]

        table = Table(
            title=f"[bold]Szczegółowy Raport Flake8 ({len(linter_results)} problemów)[/]",
            show_header=True, header_style="bold red", border_style="dim", show_lines=True
        )
        table.add_column("Plik", style="cyan", width=30)
        table.add_column("Linia", style="magenta")
        table.add_column("Kol.", style="yellow")
        table.add_column("Opis", ratio=1)

        for line in visible_data:
            parts = line.split(':', 3)
            if len(parts) == 4:
                file, line_num, col_num, message = parts
                table.add_row(Path(file).name, line_num, col_num, message.strip())
        
        info = Panel(Align.center(f"Wyświetlanie {start_index+1}-{min(start_index+visible_rows, len(linter_results))} z {len(linter_results)}."), border_style="red")
        footer = Text.from_markup("Nawigacja: [on bright_black] ▲/▼, PgUp/PgDn, Home/End [/] | Wyjście: [on bright_black] Q [/]", justify="center")
        
        layout = Layout()
        layout.split_column(Layout(table), Layout(info, size=3), Layout(footer, size=1))
        return layout

    with Live(build_layout(), screen=True, auto_refresh=False) as live:
        while True:
            live.update(build_layout(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key or key.upper() == 'Q':
                break
            
            visible_rows = console.height - 10
            if key == "UP": scroll_pos = max(0, scroll_pos - 1)
            elif key == "DOWN": scroll_pos = min(len(linter_results) - 1, scroll_pos + 1)
            elif key == "PAGE_UP": scroll_pos = max(0, scroll_pos - visible_rows)
            elif key == "PAGE_DOWN": scroll_pos = min(len(linter_results) - visible_rows, scroll_pos + visible_rows)
            elif key == "HOME": scroll_pos = 0
            elif key == "END": scroll_pos = max(0, len(linter_results) - visible_rows)


def display_unittest_results(is_successful: bool, output: str):
    """Wyświetla wyniki testów jednostkowych w estetycznym panelu."""
    border_style = "green" if is_successful else "red"
    title = "✅ Wynik Testów Jednostkowych: SUKCES" if is_successful else "❌ Wynik Testów Jednostkowych: BŁĘDY"
    
    # Usuwamy niepotrzebne linie z wyniku unittest, aby był bardziej czytelny
    lines = output.strip().split('\n')
    cleaned_lines = [line for line in lines if not line.strip().startswith('---') and not line.strip().startswith('Ran')]
    summary_line = next((line for line in lines if line.strip().startswith('Ran')), "")
    
    cleaned_output = "\n".join(cleaned_lines)
    
    panel_content = f"{cleaned_output}\n\n[bold {border_style}]{summary_line}[/]"
    
    console.print(Panel(panel_content, title=f"[bold]{title}[/]", border_style=border_style))


def display_skipped_panel(title: str, reason: str):
    """Wyświetla panel informacyjny o pominięciu analizy."""
    console.print(Panel(f"[bold yellow]{reason}[/]", title=title, border_style="yellow"))
