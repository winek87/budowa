# -*- coding: utf-8 -*-

# plik: core/takeout_file_importer_logic.py
# Wersja 1.0 - Nowy moduł do importu plików z archiwum Takeout
#
# ##############################################################################
# ===           MODUŁ IMPORTERA PLIKÓW Z ARCHIWUM GOOGLE TAKEOUT             ===
# ##############################################################################
#
# Ten moduł zawiera logikę dla nowego, potężnego narzędzia, które pozwala
# na importowanie FIZYCZNYCH PLIKÓW (zdjęć, wideo) z rozpakowanego archiwum
# Google Takeout bezpośrednio do głównej biblioteki aplikacji.
#
# Jego główne zadania to:
#  1. Zidentyfikowanie w archiwum Takeout plików, których brakuje w lokalnej bazie.
#  2. Odczytanie metadanych (zwłaszcza daty) z towarzyszących plików .json.
#  3. Skopiowanie brakujących plików do prawidłowej struktury folderów ROK/MIESIĄC.
#  4. Utworzenie kompletnych, nowych wpisów w bazie danych.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import json
import logging
import shutil
from pathlib import Path

# --- Importy asynchroniczne ---
import aiosqlite

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.progress import Progress

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import DATABASE_FILE, DOWNLOADS_DIR_BASE
from .database import add_local_file_entry # Używamy tej funkcji do dodawania wpisów
from .utils import get_date_from_metadata, create_unique_filepath
# Importujemy sprawdzoną funkcję do parsowania nazw plików .json
from .takeout_importer_logic import _get_original_filename

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)

# Lista obsługiwanych rozszerzeń plików multimedialnych
SUPPORTED_MEDIA_EXTENSIONS = {
    '.jpg', '.jpeg', '.png', '.heic', '.heif', '.gif', '.webp', '.bmp', '.tif', '.tiff',
    '.mp4', '.mov', '.avi', '.m4v', '.3gp', '.mkv', '.mpg', '.mpeg', '.webm',
    '.cr2', '.nef', '.arw', '.dng' # Pliki RAW
}

async def _perform_file_import(photos_path: Path):
    """
    Główna, asynchroniczna funkcja robocza, która wykonuje cały proces
    identyfikacji, kopiowania i zapisywania brakujących plików z Takeout.

    :param photos_path: Ścieżka do folderu 'Google Zdjęcia' w archiwum Takeout.
    """
    try:
        # --- Krok 1: Zmapuj wszystkie pliki w folderze Takeout ---
        media_map = {}
        json_map = {}
        with console.status("[cyan]Mapowanie plików w archiwum Takeout (może potrwać)...[/]"):
            def find_and_map_files():
                all_files = list(photos_path.rglob('*.*'))
                for file_path in all_files:
                    if file_path.suffix.lower() in SUPPORTED_MEDIA_EXTENSIONS:
                        media_map[file_path.name] = file_path
                    elif file_path.suffix.lower() == '.json':
                        # Używamy inteligentnej funkcji, aby uzyskać nazwę pliku, do którego odnosi się ten JSON
                        original_filename = _get_original_filename(file_path.name)
                        if original_filename:
                            json_map[original_filename] = file_path
            await asyncio.to_thread(find_and_map_files)
        logger.info(f"Zmapowano {len(media_map)} plików multimedialnych i {len(json_map)} plików JSON w Takeout.")

        # --- Krok 2: Pobierz listę istniejących plików z bazy danych ---
        with console.status("[cyan]Pobieranie listy istniejących plików z bazy danych...[/]"):
            async with aiosqlite.connect(DATABASE_FILE) as conn:
                cursor = await conn.execute("SELECT filename FROM downloaded_media WHERE filename IS NOT NULL")
                existing_filenames = {row[0] for row in await cursor.fetchall()}
        logger.info(f"Znaleziono {len(existing_filenames)} istniejących plików w lokalnej bazie danych.")

        # --- Krok 3: Zidentyfikuj pliki, których brakuje w lokalnej kolekcji ---
        missing_files_map = {name: path for name, path in media_map.items() if name not in existing_filenames}
        
        if not missing_files_map:
            logger.info("Nie znaleziono żadnych brakujących plików. Twoja kolekcja jest spójna z archiwum Takeout.")
            console.print("\n[bold green]✅ Gratulacje! Wygląda na to, że Twoja lokalna kolekcja jest już kompletna.[/bold green]")
            return

        # --- Krok 4: Poproś o potwierdzenie przed rozpoczęciem importu ---
        console.print(f"\nZnaleziono [bold cyan]{len(missing_files_map)}[/bold cyan] plików w Takeout, których brakuje w Twojej bibliotece.")
        if not Confirm.ask("[cyan]Czy chcesz je teraz zaimportować (skopiować) do swojej głównej biblioteki?[/]"):
            logger.warning("Import plików z Takeout anulowany przez użytkownika.")
            return

        # --- Krok 5: Główna pętla importu ---
        imported_count, error_count = 0, 0
        library_base_path = Path(DOWNLOADS_DIR_BASE)

        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[green]Importowanie plików...", total=len(missing_files_map))
            for filename, source_path in missing_files_map.items():
                try:
                    # Znajdź pasujący plik .json dla brakującego pliku
                    json_path = json_map.get(filename)
                    if not json_path:
                        logger.warning(f"Nie znaleziono pasującego pliku .json dla '{filename}'. Plik zostanie pominięty.")
                        error_count += 1
                        continue

                    # Odczytaj metadane (zwłaszcza datę) z pliku .json
                    with open(json_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)

                    creation_date = await get_date_from_metadata(metadata)
                    if not creation_date:
                        # Fallback na datę z `photoTakenTime` jeśli `get_date_from_metadata` zawiedzie
                        if ts := metadata.get('photoTakenTime', {}).get('timestamp'):
                             creation_date = datetime.fromtimestamp(int(ts))
                        else:
                            logger.warning(f"Brak daty w .json dla '{filename}'. Plik zostanie pominięty.")
                            error_count += 1
                            continue
                    
                    # Utwórz ścieżkę docelową i skopiuj plik
                    dest_dir = library_base_path / str(creation_date.year) / f"{creation_date.month:02d}"
                    await asyncio.to_thread(dest_dir.mkdir, parents=True, exist_ok=True)
                    final_path = create_unique_filepath(dest_dir, filename)
                    await asyncio.to_thread(shutil.copy2, source_path, final_path)

                    # Dodaj nowy, kompletny wpis do bazy danych
                    was_added = await add_local_file_entry(final_path, metadata)
                    if was_added:
                        imported_count += 1
                        logger.info(f"Pomyślnie zaimportowano plik '{filename}' do '{final_path}'.")
                    else:
                        logger.warning(f"Plik '{filename}' już istniał w bazie (sprawdzono po URI). Pomijam.")
                        await asyncio.to_thread(final_path.unlink) # Usuń skopiowany plik, jeśli wpis już był

                except Exception as e:
                    error_count += 1
                    logger.error(f"Wystąpił błąd podczas importu pliku '{filename}': {e}", exc_info=True)
                finally:
                    progress.update(task, advance=1)

        # --- Krok 6: Wyświetl podsumowanie ---
        console.print(f"\n[bold green]✅ Proces importu plików z Takeout zakończony![/]")
        console.print(f"  - Zaimportowano: [bold cyan]{imported_count}[/] nowych plików.")
        if error_count > 0:
            console.print(f"  - Błędy/Pominięte: [bold red]{error_count}[/]. Sprawdź logi po szczegóły.")

    except Exception as e:
        logger.critical("Wystąpił nieoczekiwany, krytyczny błąd w Importerze Plików z Takeout.", exc_info=True)
        console.print(f"\n[bold red]Wystąpił krytyczny błąd. Sprawdź plik logu.[/bold red]")

async def run_takeout_file_importer():
    """
    Uruchamia interaktywny proces importowania plików z archiwum Google Takeout.

    Funkcja ta pełni rolę głównego punktu wejścia dla użytkownika.
    1. Wyświetla panel powitalny i instrukcje.
    2. Prosi użytkownika o podanie ścieżki do głównego folderu Google Takeout.
    3. Weryfikuje, czy podana ścieżka jest prawidłowa i zawiera jeden
       z oczekiwanych podfolderów ze zdjęciami (np. 'Google Zdjęcia').
    4. Uruchamia właściwy proces skanowania i kopiowania plików,
       wywołując `_perform_file_import`.
    """
    console.clear()
    logger.info("Uruchomiono interfejs Importera Plików z Google Takeout.")
    console.print(Panel(
        "📦 Importer Plików z Archiwum Google Takeout 📦",
        expand=False,
        style="bold green",
        subtitle="Wersja 1.0"
    ))
    console.print(
        "\nTo narzędzie przeskanuje Twój rozpakowany folder Google Takeout, "
        "znajdzie pliki, których brakuje w Twojej lokalnej bibliotece, "
        "i skopiuje je do odpowiednich folderów (`ROK/MIESIĄC`)."
    )
    
    takeout_path_str = Prompt.ask("\n[bold cyan]Podaj pełną ścieżkę do folderu 'Takeout'[/bold cyan]")
    
    try:
        if not takeout_path_str.strip():
            logger.warning("Nie podano ścieżki. Anulowano.")
            return

        takeout_path = Path(takeout_path_str.strip()).expanduser().resolve()
        
        # Sprawdzamy kilka możliwych nazw folderu ze zdjęciami dla większej kompatybilności.
        possible_names = ["Google Zdjęcia", "Zdjęcia Google", "Google Photos"]
        google_photos_path = None
        
        logger.info("Sprawdzanie istnienia folderu ze zdjęciami w: %s", takeout_path)
        for name in possible_names:
            potential_path = takeout_path / name
            if await asyncio.to_thread(potential_path.is_dir):
                google_photos_path = potential_path
                logger.info("Znaleziono prawidłowy folder ze zdjęciami: %s", google_photos_path)
                break

        if not google_photos_path:
            logger.error("W ścieżce '%s' nie znaleziono żadnego z folderów: %s", takeout_path, possible_names)
            console.print(
                f"\n[bold red]Błąd: Nie znaleziono wymaganego folderu ze zdjęciami.[/]\n"
                f"W lokalizacji [cyan]{takeout_path}[/cyan] szukano folderów o nazwach: "
                f"[yellow]{', '.join(possible_names)}[/yellow].\n"
                "Upewnij się, że podałeś ścieżkę do głównego folderu 'Takeout'."
            )
            return

    except Exception as e:
        logger.error("Nieprawidłowa ścieżka: '%s'. Błąd: %s", takeout_path_str, e, exc_info=True)
        console.print(f"\n[bold red]Błąd: Podana ścieżka jest nieprawidłowa lub niedostępna.[/]")
        return

    # Jeśli walidacja się powiodła, uruchamiamy główny proces.
    await _perform_file_import(google_photos_path)

