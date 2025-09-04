# -*- coding: utf-8 -*-

# plik: core/takeout_importer_logic.py
# Wersja 2.2 - Dodano zapisywanie adresu URL z Takeout do bazy danych

################################################################################
# ===                     MODUŁ IMPORTERA GOOGLE TAKEOUT                     ===
################################################################################
#
# Ten plik zawiera logikę dla narzędzia do importowania i scalania metadanych
# z archiwum Google Takeout. Jego celem jest wzbogacenie istniejącej bazy
# danych o najdokładniejsze metadane (GPS, opisy, tagi), które są dostępne
# w plikach .json dostarczanych przez Google.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import json
import logging
from pathlib import Path
import asyncio
from datetime import datetime
from typing import Optional

# --- Importy asynchroniczne ---
import aiosqlite

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.progress import Progress

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .database import get_all_db_records_for_takeout_import, update_takeout_metadata_batch
# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)

# Lista znanych rozszerzeń plików multimedialnych do inteligentnego parsowania nazw
KNOWN_MEDIA_EXTENSIONS = (
    # Obrazy
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.heic', '.heif', '.avif',
    # Wideo
    '.mp4', '.mov', '.avi', '.mkv', '.webm', '.3gp', '.mpg', '.mpeg',
    # RAW
    '.raw', '.dng', '.cr2', '.nef', '.orf', '.arw'
)

def _get_original_filename(json_filename: str) -> Optional[str]:
    """
    Inteligentnie wyodrębnia oryginalną nazwę pliku multimedialnego
    z nazwy pliku .json z Google Takeout.

    :param json_filename: Pełna nazwa pliku .json z archiwum Takeout.
    :return: Wyodrębniona, oryginalna nazwa pliku multimedialnego lub None.
    """
    lower_name = json_filename.lower()
    for ext in KNOWN_MEDIA_EXTENSIONS:
        if ext in lower_name:
            end_index = lower_name.rfind(ext) + len(ext)
            return json_filename[:end_index]
    if lower_name.endswith('.json'):
        return json_filename[:-5]
    logger.warning(
        "Nie można było ustalić oryginalnej nazwy pliku dla '%s'. "
        "Plik JSON zostanie zignorowany.", json_filename
    )
    return None

async def _process_takeout_folder(photos_path: Path) -> None:
    """
    Skanuje podany folder Google Takeout, parsuje pliki .json i asynchronicznie
    aktualizuje wpisy w lokalnej bazie danych.

    :param photos_path: Obiekt Path wskazujący na folder 'Google Zdjęcia'.
    """
    logger.info("Rozpoczynam skanowanie folderu Takeout: %s", photos_path)
    try:
        # --- Krok 1: Zmapuj wszystkie pliki .json ---
        json_map = {}
        unmapped_jsons_count = 0
        with console.status("[cyan]Mapowanie plików .json z Takeout (może potrwać)...[/]"):
            def find_json_files():
                return list(photos_path.rglob('*.json'))
            all_json_files = await asyncio.to_thread(find_json_files)
            total_jsons = len(all_json_files)
        logger.info("Znaleziono %d plików .json w archiwum Takeout. Rozpoczynam mapowanie.", total_jsons)

        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[cyan]Tworzenie mapy metadanych...", total=total_jsons)
            for json_path in all_json_files:
                original_filename = _get_original_filename(json_path.name)
                if original_filename:
                    json_map[original_filename] = json_path
                else:
                    unmapped_jsons_count += 1
                progress.update(task, advance=1)
        if unmapped_jsons_count > 0:
            logger.warning("Nie można było zmapować %d z %d plików .json.", unmapped_jsons_count, total_jsons)
            console.print(f"[yellow]Ostrzeżenie: Nie udało się zmapować {unmapped_jsons_count} plików .json.[/yellow]")
        logger.info("Pomyślnie zmapowano %d plików .json.", len(json_map))

        # --- Krok 2: Pobierz rekordy z bazy ---
        with console.status("[cyan]Pobieranie rekordów z lokalnej bazy danych...[/]"):
            # Użycie scentralizowanej funkcji
            db_records = await get_all_db_records_for_takeout_import()
        logger.info("Pobrano %d rekordów z lokalnej bazy do porównania.", len(db_records))

        # --- Krok 3: Porównaj dane i przygotuj aktualizacje ---
        updates_batch = []
        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[cyan]Scalanie metadanych...", total=len(db_records))
            for record in db_records:
                filename = record['filename']
                filename_stem = Path(filename).stem if filename else None
                json_to_process_path = json_map.get(filename) or (filename_stem and json_map.get(filename_stem))

                if json_to_process_path:
                    try:
                        with open(json_to_process_path, 'r', encoding='utf-8') as f:
                            takeout_meta = json.load(f)
                        db_meta = json.loads(record['metadata_json'] or '{}')
                        
                        merged_meta = db_meta.copy()
                        if not merged_meta.get('Description') and takeout_meta.get('description'):
                            merged_meta['Description'] = takeout_meta['description']
                        
                        merged_meta['Title_Takeout'] = takeout_meta.get('title')
                        merged_meta['GeoData_Takeout'] = takeout_meta.get('geoData')
                        merged_meta['People_Takeout'] = [p.get('name') for p in takeout_meta.get('people', []) if p.get('name')]
                        
                        if ts := takeout_meta.get('photoTakenTime', {}).get('timestamp'):
                            merged_meta['Timestamp_Takeout'] = datetime.fromtimestamp(int(ts)).isoformat()
                        
                        takeout_url = takeout_meta.get("url")
                        updates_batch.append((json.dumps(merged_meta), takeout_url, record['id']))
                        logger.debug("Przygotowano aktualizację dla pliku: %s", filename)
                    except (json.JSONDecodeError, KeyError, OSError) as e:
                        logger.warning("Nie udało się przetworzyć pliku .json '%s' dla '%s': %s", json_to_process_path.name, filename, e)
                
                progress.update(task, advance=1)
        
        updated_count = len(updates_batch)
        logger.info("Przygotowano %d aktualizacji do zapisu w bazie danych.", updated_count)

        # --- Krok 4: Zapisz zmiany w bazie danych ---
        if updates_batch:
            with console.status(f"[cyan]Zapisywanie {updated_count} aktualizacji w bazie danych...[/]"):
                # Użycie scentralizowanej funkcji
                await update_takeout_metadata_batch(updates_batch)
            logger.info("Pomyślnie zaktualizowano %d rekordów w bazie danych.", updated_count)
            console.print(f"\n[bold green]✅ Sukces! Zaktualizowano metadane dla {updated_count} plików.[/bold green]")
        else:
            logger.info("Nie znaleziono pasujących plików do zaktualizowania.")
            console.print("\n[bold yellow]Nie znaleziono pasujących plików do zaktualizowania.[/bold yellow]")

    except Exception as e:
        logger.critical("Wystąpił krytyczny błąd podczas importu z Takeout.", exc_info=True)
        console.print(f"\n[bold red]Wystąpił nieoczekiwany błąd. Sprawdź plik logu.[/bold red]")

async def run_takeout_importer() -> None:
    """
    Uruchamia interaktywny proces importowania metadanych z Google Takeout.
    """
    console.clear()
    logger.info("Uruchomiono interfejs Importera Danych z Google Takeout.")
    console.print(Panel(
        "📦 Importer Metadanych z Google Takeout 📦",
        expand=False, style="bold green", subtitle="Wersja 2.2"
    ))
    console.print(
        "\nTo narzędzie przeskanuje Twój rozpakowany folder Google Takeout, "
        "odnajdzie pliki `.json` i zaktualizuje nimi wpisy w bazie danych."
    )
    takeout_path_str = Prompt.ask("\n[bold cyan]Podaj pełną ścieżkę do folderu 'Takeout'[/bold cyan]")
    
    try:
        takeout_path = Path(takeout_path_str.strip()).expanduser().resolve()
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
            logger.error(
                "W ścieżce '%s' nie znaleziono żadnego z folderów: %s",
                takeout_path, possible_names
            )
            console.print(
                f"\n[bold red]Błąd: Nie znaleziono folderu ze zdjęciami w [cyan]{takeout_path}[/cyan].[/]\n"
                f"Szukano folderów: [yellow]{', '.join(possible_names)}[/yellow]."
            )
            return
    except Exception as e:
        logger.error("Nieprawidłowa ścieżka: '%s'. Błąd: %s", takeout_path_str, e, exc_info=True)
        console.print(f"\n[bold red]Błąd: Podana ścieżka jest nieprawidłowa lub niedostępna.[/]")
        return

    await _process_takeout_folder(google_photos_path)
