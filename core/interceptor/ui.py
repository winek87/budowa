# plik: core/interceptor/ui.py
# Wersja 1.0 - Komponenty UI dla Podsłuchu Sieciowego.
# Opis: Ten moduł zawiera dashboard na żywo oraz funkcję do
#       prezentacji finalnych wyników przechwyconych danych.
# -*- coding: utf-8 -*-

import json
from collections import deque
from typing import Deque, List, Dict, Any

from rich.align import Align
from rich.console import Console, Group
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.json import JSON

console = Console()


class InterceptorLiveDisplay:
    """Zarządza dashboardem na żywo dla procesu nasłuchu sieciowego."""

    def __init__(self, console: Console):
        self.console = console
        self._live: Live | None = None
        self._title_text = Text("📡 Podsłuch Sieciowy (Interceptor) 📡", justify="center", style="bold red")
        
        self._recent_captures: Deque[Text] = deque(maxlen=20)
        self.total_captured = 0
        
        self._layout = self._build_layout()

    def _build_layout(self) -> Layout:
        """Tworzy główny layout dashboardu."""
        info_panel = Panel(
            "[bold cyan]Strona załadowana.[/]\n\n"
            "Wykonuj akcje w oknie przeglądarki (np. otwórz panel info, przewiń), "
            "aby wygenerować ruch sieciowy i przechwycić odpowiedzi JSON.\n\n"
            f"[bold]Przechwycono dotychczas: [yellow]{self.total_captured}[/]\n\n"
            "[bold red]Gdy skończysz, ZAMKNIJ OKNO PRZEGLĄDARKI, aby zobaczyć podsumowanie.[/]",
            title="[bold yellow]Instrukcje[/]", border_style="yellow"
        )
        
        logs_panel = Panel(
            Group(*self._recent_captures),
            title="[bold]Przechwycone Odpowiedzi (na żywo)[/]",
            border_style="dim"
        )
        
        layout = Layout()
        layout.split_column(
            Layout(Panel(self._title_text), name="header", size=3),
            Layout(info_panel, size=8),
            Layout(logs_panel, name="logs")
        )
        return layout

    def __enter__(self):
        self._live = Live(self._layout, screen=True, transient=True, auto_refresh=True, console=self.console)
        self._live.start(); return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live: self._live.stop()
            
    def add_capture(self, capture_info: Dict):
        """Dodaje informację o nowo przechwyconej odpowiedzi do widoku."""
        self.total_captured += 1
        display_text = Text.from_markup(
            f"✅ [green]{capture_info['name']}[/] [dim]({capture_info['size_kb']:.1f} KB)[/]"
        )
        self._recent_captures.appendleft(display_text)


def display_interceptor_summary(captured_data: List[Dict[str, Any]]):
    """Wyświetla finalne, szczegółowe podsumowanie przechwyconych danych."""
    console.clear()
    if not captured_data:
        console.print(Panel("[bold yellow]Nie udało się przechwycić żadnych odpowiedzi w formacie JSON.[/]", title="Brak Wyników"))
        return

    console.print(Panel(f"Przechwycono łącznie [bold cyan]{len(captured_data)}[/] odpowiedzi w formacie JSON.", title="[green]Analiza Zakończona[/]"))
    
    for item in captured_data:
        json_renderable = JSON.from_data(item["json_data"])
        console.print(Panel(
            json_renderable,
            title=f"[bold]URL:[/bold] [cyan]{item['url']}[/]",
            border_style="cyan",
            subtitle=f"Rozmiar: {len(json.dumps(item['json_data']))/1024:.2f} KB"
        ))
