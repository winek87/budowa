# plik: core/exif_writer/cli.py
# Wersja 1.3 - Zintegrowano obsługę natychmiastowego przerwania (Ctrl+C).
# Opis: Moduł zarządzający interaktywnym menu i przepływem pracy dla
#       narzędzia do zapisu metadanych EXIF.
# -*- coding: utf-8 -*-

import asyncio
import logging
import shutil
from pathlib import Path
from typing import List, Dict, Any

from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

# Importy z modułów projektu `core`
from ..utils import get_key
from ..database import get_exif_writer_stats, get_records_for_exif_processing, setup_database

# Importy z wewnętrznych modułów pakietu
from .tasks import process_single_file
from .ui.live_display import ExifWriterLiveDisplay
from .utils import stop_event, setup_signal_handlers

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)


class ExifWriterMenuApp:
    """Klasa hermetyzująca logikę i stan menu Zapisywarki EXIF."""

    def __init__(self):
        """Inicjalizuje stan menu."""
        self.selected_index: int = 0
        self.menu_items: List[Dict[str, Any]] = []
        self.stats: Dict[str, Any] = {}

    def _define_menu_items(self):
        """
        Definiuje strukturę menu, dynamicznie aktywując/dezaktywując opcje
        i wyświetlając liczniki na podstawie aktualnych statystyk z bazy danych.
        """
        errors_to_retry = self.stats.get('error', 0) + self.stats.get('partial', 0)
        
        self.menu_items = [
            {
                "icon": "✍️ ", "text": "Zapisz metadane dla nowych plików",
                "action": "new_only" if self.stats.get('not_written', 0) > 0 else None,
                "description": "Przetwarza tylko pliki, dla których metadane nie były jeszcze zapisywane.",
                "count": self.stats.get('not_written', 0)
            },
            {
                "icon": "🔁", "text": "Ponów pliki z błędami/częściowe",
                "action": "retry_errors" if errors_to_retry > 0 else None,
                "description": "Próbuje ponownie zapisać metadane dla plików, które wcześniej zakończyły się błędem lub częściowym sukcesem.",
                "count": errors_to_retry
            },
            {
                "icon": "🔄", "text": "Odśwież wszystkie dane",
                "action": "force_refresh",
                "description": "Wymusza ponowny zapis metadanych dla wszystkich zeskanowanych plików w bazie danych. Używaj ostrożnie."
            },
            {
                "icon": "🚪", "text": "Wróć do menu głównego",
                "action": "back", "description": "Zamyka Zapisywarkę EXIF i wraca do menu głównego."
            },
        ]
        self.selected_index = next((i for i, item in enumerate(self.menu_items) if item.get("action") is not None), 0)

    def _build_layout(self) -> Layout:
        """Tworzy dynamiczny, dwukolumnowy layout menu."""
        table = Table.grid(expand=True, padding=(0, 1))
        table.add_column("main", ratio=1); table.add_column("stats", width=8, justify="right")
        
        for i, item in enumerate(self.menu_items):
            style = "bold white on green" if i == self.selected_index else ""
            prefix = "» " if i == self.selected_index else "  "
            
            label_text = Text(f"{prefix}{item['icon']} {item['text']}")
            if item.get("action") is None:
                label_text.stylize("dim")

            count = item.get('count')
            count_text = Text(f"({count})", style="yellow") if count else Text("")
            
            table.add_row(label_text, count_text, style=style)
        
        menu_panel = Panel(table, title="[bold green]✍️ Zapisywarka EXIF - Wybierz Tryb Pracy[/]", border_style="green")
        
        selected_item = self.menu_items[self.selected_index]
        info_panel = Panel(Align.center(f"[bold underline bright_green]{selected_item['icon']} {selected_item['text']}[/]\n\n[italic]{selected_item['description']}[/]", vertical="middle"), title="[bold]Opis Opcji[/]", border_style="dim")
        
        layout = Layout(); body = Layout(name="body"); body.split_row(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1))
        footer = Text.from_markup("Nawigacja: [on bright_black] ▲/▼ [/] | Wybór: [on bright_black] Enter [/] | Powrót: [on bright_black] Q [/]", justify="center")
        layout.split_column(body, Layout(footer, size=1))
        return layout

    async def _handle_input(self) -> str:
        """Asynchronicznie obsługuje wejście z klawiatury do nawigacji w menu."""
        key = await asyncio.to_thread(get_key);
        if not key: return 'CONTINUE'
        
        if key == "UP":
            original_index = self.selected_index
            while True:
                self.selected_index = (self.selected_index - 1 + len(self.menu_items)) % len(self.menu_items)
                if self.menu_items[self.selected_index].get("action") is not None: break
                if self.selected_index == original_index: break
        elif key == "DOWN":
            original_index = self.selected_index
            while True:
                self.selected_index = (self.selected_index + 1) % len(self.menu_items)
                if self.menu_items[self.selected_index].get("action") is not None: break
                if self.selected_index == original_index: break
        elif key.upper() == 'Q':
            return 'EXIT_MENU'
        elif key == "ENTER":
            return 'EXECUTE_ACTION'
        return 'CONTINUE'

    async def _run_writer_core(self, process_mode: str):
        """Uruchamia główną logikę zapisu z dashboardem i obsługą przerwania."""
        setup_signal_handlers()
        stop_event.clear()
        
        records = await get_records_for_exif_processing(process_mode)
        if not records:
            console.print(Panel("[bold green]✅ Nie znaleziono żadnych plików do przetworzenia w wybranym trybie.[/]", title="Informacja"))
            await asyncio.sleep(2.5)
            return

        title_map = {
            'new_only': "Zapis dla Nowych Plików",
            'retry_errors': "Ponawianie Błędów",
            'force_refresh': "Pełne Odświeżanie"
        }
        
        with ExifWriterLiveDisplay(total_items=len(records), title=title_map.get(process_mode, "Zapis Metadanych"), console=console) as display:
            for record in records:
                if stop_event.is_set():
                    logger.warning("Przerwanie przez użytkownika. Zatrzymuję pętlę zapisu...")
                    break
                
                try:
                    result = await process_single_file(record)
                    file_path_obj = Path(record.get("final_path", "Brak pliku"))
                    display.update_progress(result['status'], file_path_obj, result['details'])
                except asyncio.CancelledError:
                    logger.warning("Operacja zapisu EXIF przerwana w trakcie przetwarzania pliku.")
                    break

        Prompt.ask("\n[bold]Proces zapisu zakończony. Naciśnij Enter...[/]")

    async def run(self):
        """Główna pętla menu, która zarządza cyklem życia interfejsu."""
        while True:
            console.clear()
            self.stats = await get_exif_writer_stats()
            if self.stats.get('total_ready') is None:
                console.print("[bold yellow]Baza jest pusta lub nie zawiera tabeli 'downloaded_media'.[/]")
                await asyncio.sleep(3)
                break
            
            self._define_menu_items()
            
            action = 'CONTINUE'
            with Live(self._build_layout(), screen=True, auto_refresh=False, transient=True) as live:
                while action == 'CONTINUE':
                    live.update(self._build_layout(), refresh=True)
                    action = await self._handle_input()
            
            if action == 'EXIT_MENU':
                break
            
            if action == 'EXECUTE_ACTION':
                selected_action = self.menu_items[self.selected_index].get("action")
                if selected_action == 'back':
                    break
                if selected_action:
                    await self._run_writer_core(selected_action)

def _check_dependencies() -> bool:
    """Sprawdza, czy program ExifTool jest dostępny w systemie."""
    if not shutil.which("exiftool"):
        console.print(Panel("[bold red]Błąd: Nie znaleziono 'exiftool'![/bold red]\nZainstaluj ExifTool i dodaj go do systemowej ścieżki PATH.", title="Brak Zależności"))
        return False
    return True

async def run_exif_writer():
    """Główny punkt wejścia, sprawdza zależności i uruchamia menu."""
    if not _check_dependencies():
        return
    await setup_database()
    app = ExifWriterMenuApp()
    await app.run()
