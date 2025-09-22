# plik: core/maintenance_tools/cli.py
# Wersja 1.0 - Dedykowane, interaktywne menu dla narzędzi utrzymaniowych.
# -*- coding: utf-8 -*-

import asyncio
import logging
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
from ..takeout_importer.cli import run_takeout_importer
from ..takeout_file_importer.cli import run_takeout_file_importer
from ..takeout_url_processor.cli import run_takeout_url_processor
from ..video_fixer.cli import run_video_fixer
from ..image_fixer.cli import run_image_fixer
from ..path_fixer.cli import run_path_fixer
from ..backup_manager.cli import run_backup_manager
from ..exif_writer.cli import run_exif_writer
from ..session_logic import refresh_session
from ..recovery_engine.cli import run_recovery_downloader
from ..advanced_recovery.cli import run_advanced_recovery

console = Console()
logger = logging.getLogger(__name__)

class MaintenanceToolsMenu:
    """Klasa zarządzająca menu dla narzędzi utrzymaniowych i naprawczych."""
    def __init__(self):
        self.selected_index = 0
        self.menu_items: List[Dict[str, Any]] = [
            {"icon": "📦", "text": "Importuj Metadane z Takeout", "action": run_takeout_importer, "desc": "Wzbogaca bazę o dane (GPS, opisy) z plików .json z archiwum Takeout."},
            {"icon": "➕", "text": "Importuj Pliki z Takeout", "action": run_takeout_file_importer, "desc": "Importuje brakujące pliki fizyczne z archiwum Takeout do Twojej biblioteki."},
            {"icon": "🔗", "text": "Uzupełnij z URL-i Takeout", "action": run_takeout_url_processor, "desc": "Używa URL-i z Takeout do ponownego pobrania plików, które miały błędy."},
            {"icon": "📹", "text": "Napraw Pliki Wideo", "action": run_video_fixer, "desc": "Diagnozuje i próbuje naprawić uszkodzone pliki wideo za pomocą FFmpeg."},
            {"icon": "🖼️", "text": "Napraw Pliki Graficzne", "action": run_image_fixer, "desc": "Diagnozuje i próbuje naprawić uszkodzone pliki obrazów (JPG, PNG)."},
            {"icon": "-", "text": "Napraw Ścieżki w Bazie", "action": run_path_fixer, "desc": "Masowo aktualizuje ścieżki w bazie, jeśli przeniosłeś folder projektu."},
            {"icon": "💾", "text": "Menedżer Kopii Zapasowych", "action": run_backup_manager, "desc": "Zarządza tworzeniem i przywracaniem kopii zapasowych danych i kodu."},
            {"icon": "✍️ ", "text": "Zapisz Metadane do Plików (EXIF)", "action": run_exif_writer, "desc": "Zapisuje metadane (opisy, tagi) z bazy danych bezpośrednio do plików."},
            {"icon": "🔄", "text": "Odśwież Sesję Logowania", "action": refresh_session, "desc": "Uruchamia przeglądarkę, aby odświeżyć wygasłą sesję logowania Google."},
            {"icon": "⛑️", "text": "Silnik Ratunkowy", "action": run_recovery_downloader, "desc": "Prosta próba pobrania wszystkich plików, które wcześniej miały błędy."},
            {"icon": "🌀", "text": "Zaawansowana Naprawa (Shake)", "action": run_advanced_recovery, "desc": "Używa techniki 'potrząśnięcia' stroną do naprawy uporczywych błędów."},
            {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "desc": "Zamyka to menu."},
        ]

    def _build_layout(self) -> Layout:
        table = Table.grid(expand=True, padding=(0, 2))
        for i, item in enumerate(self.menu_items):
            style = "bold white on yellow" if i == self.selected_index else ""
            prefix = "» " if i == self.selected_index else "  "
            table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
        
        menu_panel = Panel(table, title="[bold yellow]🛠️ Utrzymanie i Naprawa[/]", border_style="yellow")
        info_panel = Panel(Align.center(f"[italic]{self.menu_items[self.selected_index]['desc']}[/]", vertical="middle"), title="Opis", border_style="dim")
        
        layout = Layout(); body = Layout(); body.split_row(menu_panel, info_panel)
        footer = Text.from_markup("Nawigacja: ▲/▼ | Wybór: Enter | Powrót: Q", justify="center")
        layout.split_column(body, Layout(footer, size=1)); return layout

    async def run(self):
        while True:
            console.clear()
            with Live(self._build_layout(), transient=True, auto_refresh=False) as live:
                while True:
                    live.update(self._build_layout(), refresh=True)
                    key = await asyncio.to_thread(get_key)
                    if key == "UP": self.selected_index = (self.selected_index - 1 + len(self.menu_items)) % len(self.menu_items)
                    elif key == "DOWN": self.selected_index = (self.selected_index + 1) % len(self.menu_items)
                    elif key == "ENTER": break
                    elif key and key.upper() == 'Q': self.selected_index = -1; break
            
            if self.selected_index == -1 or self.menu_items[self.selected_index]['action'] == 'back': break
            
            action = self.menu_items[self.selected_index]['action']
            console.clear()
            await action()
            Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić...[/]")

async def run_maintenance_tools_menu():
    """Główny punkt wejścia do modułu."""
    await MaintenanceToolsMenu().run()
