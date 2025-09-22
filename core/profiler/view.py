# plik: core/profiler/ui.py
# Wersja 1.1 - Zintegrowano podsumowanie i prompt powrotu w raporcie.
# Opis: Ten moduł zawiera funkcje generujące interaktywne menu
#       i statyczną, kompletną tabelę z wynikami profilowania.
# -*- coding: utf-8 -*-

import asyncio
from typing import Dict, List, Any

from rich.align import Align
from rich.console import Console, Group
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.prompt import Prompt

from ..utils import get_key

console = Console()


async def get_profiler_options_from_user() -> Dict | None:
    """
    Wyświetla interaktywne menu i zbiera opcje profilowania (tryb, liczba próbek)
    od użytkownika.
    
    Returns:
        Dict | None: Słownik z opcjami lub None, jeśli użytkownik anulował.
    """
    
    selected_index = 0
    menu_items: List[Dict[str, Any]] = [
        {"icon": "🚀", "text": "Pełny Cykl (Nawigacja + Przetwarzanie)", "action": "full_cycle", "description": "Mierzy czas potrzebny na przejście do następnego elementu ORAZ czas jego pełnego przetworzenia. Symuluje normalną pracę."},
        {"icon": "⚙️ ", "text": "Tylko Przetwarzanie (Skan + Pobieranie)", "action": "processing_only", "description": "Mierzy wyłącznie czas potrzebny na przetworzenie już załadowanej strony. Pomija czas nawigacji."},
        {"icon": "🚪", "text": "Anuluj", "action": "back", "description": "Wróć do menu głównego bez uruchamiania profilera."},
    ]
    
    def build_layout() -> Layout:
        table = Table.grid(expand=True, padding=(0, 2))
        for i, item in enumerate(menu_items):
            style = "bold white on yellow" if i == selected_index else ""
            prefix = "» " if i == selected_index else "  "
            table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
        
        menu_panel = Panel(table, title="[bold yellow]⏱️ Profiler - Wybierz Tryb Pracy[/]", border_style="yellow")
        
        selected_item = menu_items[selected_index]
        info_panel = Panel(Align.center(f"[bold underline bright_yellow]{selected_item['icon']} {selected_item['text']}[/]\n\n[italic]{selected_item['description']}[/]", vertical="middle"), title="[bold]Opis Opcji[/]", border_style="dim")
        
        layout = Layout()
        body = Layout(name="body")
        body.split_row(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1))
        footer = Text.from_markup("Nawigacja: [on bright_black] ▲/▼ [/] | Wybór: [on bright_black] Enter [/] | Anuluj: [on bright_black] Q [/]", justify="center")
        layout.split_column(body, Layout(footer, size=1))
        return layout
    
    console.clear()
    with Live(build_layout(), screen=True, transient=True, auto_refresh=False) as live:
        while True:
            live.update(build_layout(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue
            if key == "UP": selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
            elif key == "DOWN": selected_index = (selected_index + 1) % len(menu_items)
            elif key == "ENTER": break
            elif key.upper() == 'Q': return None
            
    selected_mode = menu_items[selected_index]['action']
    if selected_mode == 'back': return None
    
    try:
        num_samples_str = console.input(f"\n[cyan]Na ilu plikach przeprowadzić test? [/](domyślnie: [bold]10[/]) >>> ")
        num_samples = int(num_samples_str) if num_samples_str else 10
        if num_samples <= 0: raise ValueError
    except ValueError:
        console.print("[bold red]Należy podać dodatnią liczbę całkowitą.[/]")
        return None
        
    return {"mode": selected_mode, "samples": num_samples}


def display_profiler_report(stats: dict, num_samples: int):
    """
    Wyświetla finalny, kompletny raport wydajności, łącząc tabelę,
    podsumowanie i prośbę o powrót w jeden, spójny widok.

    Args:
        stats (dict): Słownik ze statystykami wygenerowany przez `PerformanceProfiler`.
        num_samples (int): Liczba próbek użyta w teście.
    """
    total_time = sum(data['total'] for data in stats.values())
    avg_per_sample = total_time / num_samples if num_samples > 0 else 0

    table = Table(title="Wyniki Profilowania", show_lines=True, border_style="dim")
    table.add_column("Operacja", style="cyan", width=35)
    table.add_column("L. Wywołań", style="magenta", justify="right")
    table.add_column("Łączny Czas (s)", style="yellow", justify="right")
    table.add_column("Średni Czas (s)", style="green", justify="right")
    table.add_column("Min Czas (s)", style="dim", justify="right")
    table.add_column("Max Czas (s)", style="dim", justify="right")

    for key, data in sorted(stats.items()):
        table.add_row(
            key, str(data["count"]), f"{data['total']:.2f}",
            f"{data['avg']:.3f}", f"{data['min']:.3f}", f"{data['max']:.3f}",
        )
    
    summary_text = Text.from_markup(
        f"\n[bold]Całkowity czas profilowania:[/bold] [cyan]{total_time:.2f} s[/]\n"
        f"[bold]Średni czas na jedną próbkę:[/bold] [cyan]{avg_per_sample:.2f} s[/]"
    )
    
    # Grupujemy tabelę i tekst podsumowania w jeden renderowalny obiekt
    report_group = Group(table, summary_text)

    # Wyświetlamy wszystko w jednym, eleganckim panelu
    console.print(Panel(
        report_group,
        title=f"📊 Raport Wydajności dla {num_samples} próbek 📊",
        style="bold green",
        expand=False
    ))
    
    # Prośba o powrót jest teraz integralną częścią raportu
 #   Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić...[/]")
