# plik: core/takeout_importer/cli.py
# Wersja 1.3 - Dodano wyciszanie logów i w pełni interaktywną historię ścieżek.
# Opis: Moduł zarządzający przepływem pracy dla importu i scalania
#       metadanych z archiwum Google Takeout.
# -*- coding: utf-8 -*-

import asyncio
import logging
from pathlib import Path

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
from ..database import get_recent_takeout_paths, add_recent_takeout_path

# Importy z wewnętrznych modułów pakietu
from .tasks import process_takeout_folder
from .ui.live_display import TakeoutImporterLiveDisplay
from .utils import silence_loggers

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)


async def run_takeout_importer():
    """
    Uruchamia interaktywny proces importowania metadanych z Google Takeout.

    Przepływ pracy:
    1. Wyświetla ekran powitalny.
    2. Pobiera i wyświetla listę ostatnio używanych ścieżek, umożliwiając
       szybki wybór za pomocą strzałek lub ręczne wpisanie nowej ścieżki.
    3. Po wybraniu, zapisuje ścieżkę do historii na potrzeby przyszłych uruchomień.
    4. Weryfikuje, czy w podanej lokalizacji znajduje się folder 'Google Zdjęcia'.
    5. Uruchamia główny proces importu z dedykowanym dashboardem na żywo,
       wyciszając jednocześnie logi w konsoli, aby nie zakłócały interfejsu.
    6. Po zakończeniu, wraca do menu głównego.
    """
    console.clear()
    logger.info("Uruchomiono interfejs Importera Danych z Google Takeout.")
    
    console.print(Panel(
        "To narzędzie przeskanuje Twój rozpakowany folder Google Takeout, odnajdzie pliki `.json` "
        "i zaktualizuje nimi odpowiednie wpisy w Twojej bazie danych, wzbogacając je o "
        "najdokładniejsze metadane (GPS, opisy, tagi osób).",
        title="📦 [bold]Importer Metadanych z Google Takeout[/]",
        border_style="green", expand=False
    ))
    
    # --- Interaktywne menu wyboru ścieżki z historią ---
    recent_paths = await get_recent_takeout_paths()
    selected_path_str = ""
    
    menu_options = recent_paths + ["[ Wpisz nową ścieżkę ręcznie ]", "[ Anuluj i wróć ]"]
    selected_index = 0

    def build_path_menu() -> Panel:
        """Wewnętrzna funkcja do budowania layoutu menu wyboru ścieżki."""
        table = Table.grid(padding=(0, 2), expand=False)
        table.add_row("[bold]Wybierz ostatnio używaną ścieżkę lub wpisz nową:[/bold]\n")
        for i, path in enumerate(menu_options):
            style = "bold white on green" if i == selected_index else ""
            prefix = "» " if i == selected_index else "  "
            table.add_row(Text(f"{prefix} {path}", style=style))
        return Panel(table)

    with Live(build_path_menu(), auto_refresh=False, transient=True, vertical_overflow="visible") as live:
        while True:
            live.update(build_path_menu(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue
            
            if key == "UP": selected_index = (selected_index - 1 + len(menu_options)) % len(menu_options)
            elif key == "DOWN": selected_index = (selected_index + 1) % len(menu_options)
            elif key.upper() == 'Q':
                selected_path_str = "exit"
                break
            elif key == "ENTER":
                selected_choice = menu_options[selected_index]
                if "Wpisz nową ścieżkę" in selected_choice:
                    selected_path_str = Prompt.ask("\n[cyan]Podaj pełną ścieżkę do folderu 'Takeout'[/]")
                elif "Anuluj" in selected_choice:
                    selected_path_str = "exit"
                else:
                    selected_path_str = selected_choice
                break

    if not selected_path_str or selected_path_str == "exit" or not selected_path_str.strip():
        logger.warning("Nie podano ścieżki lub anulowano. Powrót do menu.")
        return
    
    # Zapisujemy wybraną/wpisaną ścieżkę do historii
    await add_recent_takeout_path(selected_path_str.strip())
    
    # Definiujemy loggery, które generują "szum" podczas operacji
    loggers_to_silence = ['core.takeout_importer.tasks', 'core.database']
    
    with silence_loggers(loggers_to_silence):
        try:
            takeout_path = Path(selected_path_str.strip()).expanduser().resolve()
            possible_names = ["Google Zdjęcia", "Zdjęcia Google", "Google Photos"]
            google_photos_path = None

            with console.status("[cyan]Wyszukiwanie folderu ze zdjęciami...[/]"):
                for name in possible_names:
                    potential_path = takeout_path / name
                    if await asyncio.to_thread(potential_path.is_dir):
                        google_photos_path = potential_path
                        break
            
            if not google_photos_path:
                logger.error(f"W ścieżce '{takeout_path}' nie znaleziono folderu: {possible_names}")
                console.print(Panel(f"[bold red]Błąd: Nie znaleziono folderu ze zdjęciami.[/]\n\nSzukano w: [cyan]{takeout_path}[/]\nOczekiwano jednego z: [yellow]{', '.join(possible_names)}[/yellow].", title="Błąd Ścieżki"))
                return

            logger.info(f"Znaleziono prawidłowy folder ze zdjęciami: {google_photos_path}")
            
            with TakeoutImporterLiveDisplay(console=console) as display:
                await process_takeout_folder(google_photos_path, display)

        except Exception as e:
            logger.critical("Wystąpił krytyczny błąd podczas importu z Takeout.", exc_info=True)
            console.print(Panel(f"[bold red]Wystąpił nieoczekiwany błąd.[/]\n\nBłąd: {e}", title="Błąd Krytyczny", border_style="red"))
