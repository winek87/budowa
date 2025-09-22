# plik: core/takeout_file_importer/cli.py
# Wersja 1.3 - Dodano obsługę Ctrl+C i szczegółowe logi do dashboardu.
# Opis: Moduł zarządzający interfejsem użytkownika i przepływem pracy dla
#       Importera Plików z archiwum Google Takeout.
# -*- coding: utf-8 -*-

import asyncio
import logging
from pathlib import Path
from typing import Dict

from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

# Importy z modułów projektu `core`
from ..database import (add_recent_takeout_path, get_all_filenames_from_db,
                        get_recent_takeout_paths)
from ..utils import get_key

# Importy z wewnętrznych modułów pakietu
from .tasks import SUPPORTED_MEDIA_EXTENSIONS, perform_file_import
from .ui.live_display import TakeoutFileImporterLiveDisplay
from .utils import silence_loggers, stop_event, setup_signal_handlers

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)


async def _get_import_mode_from_user(num_missing: int) -> str | None:
    """
    Wyświetla interaktywne menu pozwalające użytkownikowi wybrać tryb importu
    (kopiowanie lub przenoszenie plików).

    Args:
        num_missing (int): Liczba brakujących plików do wyświetlenia w tytule.

    Returns:
        str | None: Wybrany tryb ('copy' lub 'move') lub None, jeśli anulowano.
    """
    selected_index = 0
    menu_items = [
        {"icon": "📥", "text": "Kopiuj brakujące pliki", "action": "copy", "description": "Bezpieczna opcja. Pliki z Takeout zostaną [bold green]SKOPIOWANE[/] do biblioteki. Oryginały w Takeout pozostaną nietknięte."},
        {"icon": "✂️ ", "text": "Przenieś brakujące pliki", "action": "move", "description": "Szybsza opcja dla dużych kolekcji. Pliki z Takeout zostaną [bold yellow]PRZENIESIONE[/] do biblioteki, zwalniając miejsce."},
        {"icon": "❌", "text": "Anuluj", "action": None, "description": "Wróć do menu głównego bez importowania plików."},
    ]

    def build_mode_menu() -> Layout:
        """Wewnętrzna funkcja do budowania layoutu menu wyboru trybu."""
        table = Table.grid(expand=True, padding=(0, 2))
        for i, item in enumerate(menu_items):
            style = "bold white on green" if i == selected_index else ""
            prefix = "» " if i == selected_index else "  "
            table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))

        menu_panel = Panel(table, title=f"[bold]Znaleziono [cyan]{num_missing}[/] brakujących plików. Co chcesz zrobić?[/bold]", border_style="green")
        info_panel = Panel(Align.center(f"[italic]{menu_items[selected_index]['description']}[/]", vertical="middle"), title="[bold]Opis opcji[/]", border_style="dim")
        layout = Layout()
        layout.split_column(menu_panel, info_panel)
        return layout

    with Live(build_mode_menu(), screen=True, transient=True, auto_refresh=False) as live:
        while True:
            live.update(build_mode_menu(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if key == "UP": selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
            elif key == "DOWN": selected_index = (selected_index + 1) % len(menu_items)
            elif key == "ENTER": return menu_items[selected_index]['action']
            elif key and key.upper() == 'Q': return None


async def run_takeout_file_importer():
    """
    Uruchamia interaktywny proces importu plików z archiwum Google Takeout,
    oferując historię ostatnio używanych ścieżek.
    """
    console.clear()
    logger.info("Uruchomiono interfejs Importera Plików z Google Takeout.")

    console.print(Panel(
        "To narzędzie przeskanuje Twój rozpakowany folder Google Takeout, "
        "znajdzie pliki, których brakuje w Twojej lokalnej bibliotece, "
        "i skopiuje je do odpowiednich folderów (`ROK/MIESIĄC`).",
        title="📦 [bold]Importer Plików z Archiwum Google Takeout[/]",
        border_style="green", expand=False
    ))

    recent_paths = await get_recent_takeout_paths()
    selected_path_str = ""
    menu_options = recent_paths + ["[ Wpisz nową ścieżkę ręcznie ]", "[ Anuluj i wróć ]"]
    selected_index = 0

    def build_path_menu() -> Panel:
        table = Table.grid(padding=(0, 2), expand=False)
        table.add_row("[bold]Wybierz ostatnio używaną ścieżkę lub wpisz nową:[/bold]\n")
        for i, path in enumerate(menu_options):
            style = "bold white on green" if i == selected_index else ""
            prefix = "» " if i == selected_index else "  "
            table.add_row(Text(f"{prefix} {path}", style=style))
        return Panel(table)

    with Live(build_path_menu(), auto_refresh=False, transient=True) as live:
        while True:
            live.update(build_path_menu(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue
            if key == "UP": selected_index = (selected_index - 1 + len(menu_options)) % len(menu_options)
            elif key == "DOWN": selected_index = (selected_index + 1) % len(menu_options)
            elif key.upper() == 'Q': selected_path_str = "exit"; break
            elif key == "ENTER":
                selected_choice = menu_options[selected_index]
                if "Wpisz nową ścieżkę" in selected_choice:
                    selected_path_str = Prompt.ask("\n[cyan]Podaj pełną ścieżkę do folderu 'Takeout'[/]")
                elif "Anuluj" in selected_choice: selected_path_str = "exit"
                else: selected_path_str = selected_choice
                break

    if not selected_path_str or selected_path_str == "exit" or not selected_path_str.strip():
        logger.warning("Nie podano ścieżki lub anulowano."); return

    await add_recent_takeout_path(selected_path_str.strip())

    setup_signal_handlers()
    stop_event.clear()
    loggers_to_silence = ['core.takeout_file_importer.tasks']

    with silence_loggers(loggers_to_silence):
        try:
            takeout_path = Path(selected_path_str.strip()).expanduser().resolve()
            possible_names = ["Google Zdjęcia", "Zdjęcia Google", "Google Photos"]
            google_photos_path = None

            with console.status("[cyan]Wyszukiwanie folderu ze zdjęciami...[/]"):
                for name in possible_names:
                    potential_path = takeout_path / name
                    if await asyncio.to_thread(potential_path.is_dir):
                        google_photos_path = potential_path; break

            if not google_photos_path:
                logger.error(f"W ścieżce '{takeout_path}' nie znaleziono folderu: {possible_names}")
                console.print(Panel(f"[bold red]Błąd: Nie znaleziono folderu ze zdjęciami.[/]\n\nSzukano w: [cyan]{takeout_path}[/]\nOczekiwano jednego z: [yellow]{', '.join(possible_names)}[/yellow].", title="Błąd Ścieżki")); return

            logger.info(f"Znaleziono folder ze zdjęciami: {google_photos_path}")

            with console.status("[cyan]Analiza i porównywanie z bazą danych (może potrwać)...[/]"):
                existing_filenames = await get_all_filenames_from_db()
                media_files = [p for p in google_photos_path.rglob('*.*') if p.suffix.lower() in SUPPORTED_MEDIA_EXTENSIONS]
                missing_count = sum(1 for p in media_files if p.name not in existing_filenames)

            if missing_count == 0:
                console.print("\n[bold green]✅ Gratulacje! Twoja lokalna kolekcja jest już kompletna.[/bold green]")
                return

            import_mode = await _get_import_mode_from_user(missing_count)
            if not import_mode:
                logger.warning("Import anulowany przez użytkownika."); return

            with TakeoutFileImporterLiveDisplay(console=console) as display:
                await perform_file_import(google_photos_path, display, import_mode)

        except Exception as e:
            logger.critical("Krytyczny błąd w Importerze Plików z Takeout.", exc_info=True)
            console.print(Panel(f"[bold red]Błąd: {e}[/]", title="Błąd Krytyczny", border_style="red"))
