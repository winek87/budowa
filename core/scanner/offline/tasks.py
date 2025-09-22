# plik: core/scanner/offline/tasks.py
# Wersja 2.1 - Ujednolicono wszystkie zadania offline z użyciem OfflineTaskRunner.
# Opis: Ten moduł zawiera konkretne implementacje dla wszystkich zadań
#       offline, zrefaktoryzowane w celu użycia uniwersalnego runnera
#       dla spójnego interfejsu użytkownika.
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import shutil
from pathlib import Path
from datetime import datetime

# --- Zależności zewnętrzne ---
try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False
    
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm

# --- Importy z modułów projektu `core` ---
from ...config import DOWNLOADS_DIR_BASE
from ...database import (
    get_records_for_path_correction,
    update_final_path,
    get_records_for_filename_fix,
    update_entry_after_rename,
    get_records_for_metadata_completion,
    update_entry_with_completed_metadata,
    get_records_for_exif_writing
)
from ...utils import create_unique_filepath, get_date_from_metadata

# --- Importy z wewnętrznych modułów pakietu `scanner` ---
from .base_task import OfflineTaskRunner

# --- Inicjalizacja ---
logger = logging.getLogger(__name__)
console = Console()

# === Zadanie 1: Korektor Lokalizacji Plików ===
async def _process_path_correction(record: dict) -> str | None:
    final_path = Path(record['final_path'])
    expected_path = Path(record['expected_path'])
    if not final_path.name or not expected_path.name:
        logger.warning(f"Pominięto rekord ID {record['id']} z powodu nieprawidłowej ścieżki.")
        return 'skipped'
    if not await asyncio.to_thread(final_path.exists):
        logger.error(f"BŁĄD: Plik źródłowy {final_path} nie istnieje. Pomijam.")
        raise FileNotFoundError(f"Plik źródłowy nie istnieje: {final_path}")
    await asyncio.to_thread(expected_path.parent.mkdir, parents=True, exist_ok=True)
    await asyncio.to_thread(shutil.move, str(final_path), str(expected_path))
    await update_final_path(record['id'], str(expected_path))
    console.print(f"  [green]Przeniesiono:[/green] [dim]{final_path.name}[/dim] -> [cyan]{expected_path.parent.name}/{expected_path.name}[/cyan]")
    return None
async def run_path_corrector():
    runner = OfflineTaskRunner(
        task_name="Korektor Lokalizacji Plików",
        fetch_data_func=get_records_for_path_correction,
        process_record_func=_process_path_correction,
        border_style="green"
    )
    await runner.run()

# === Zadanie 2: Naprawa Nazw Plików ===
async def _process_filename_fix(record: dict) -> None:
    current_path = Path(record['final_path'])
    metadata = json.loads(record['metadata_json'])
    filename_from_meta = metadata.get('FileName')
    if not await asyncio.to_thread(current_path.exists):
        raise FileNotFoundError(f"Plik źródłowy nie istnieje: {current_path}")
    new_path = create_unique_filepath(current_path.parent, filename_from_meta)
    await asyncio.to_thread(current_path.rename, new_path)
    new_filename = new_path.name
    metadata['FileName'] = new_filename
    if 'DateTime' in metadata:
        dt = datetime.fromisoformat(metadata["DateTime"])
        dest_dir = Path(DOWNLOADS_DIR_BASE) / str(dt.year) / f"{dt.month:02d}"
        new_expected_path = str(dest_dir / new_filename)
        metadata['expected_path'] = new_expected_path
    else:
        new_expected_path = str(new_path)
    await update_entry_after_rename(record['id'], new_filename, str(new_path), new_expected_path, json.dumps(metadata, ensure_ascii=False))
    console.print(f"  [green]Zmieniono nazwę:[/green] [dim]{current_path.name}[/dim] -> [cyan]{new_filename}[/cyan]")

async def run_filename_fixer():
    """Uruchamia naprawę nazw plików w pętli, aż wszystkie będą poprawne."""
    console.clear()
    console.print(Panel("🔧 Naprawa Nazw Plików z Pełną Synchronizacją 🔧", style="yellow"))
    if not Confirm.ask("\n[bold red]UWAGA:[/bold red] Ta operacja zmieni nazwy plików na dysku. Kontynuować?", default=False, console=console):
        return
        
    run_count = 0
    while True:
        run_count += 1
        logger.info(f"Rozpoczynam przebieg {run_count} weryfikacji nazw plików.")
        
        # Tworzymy runnera dla JEDNEGO przebiegu
        runner = OfflineTaskRunner(
            task_name=f"Naprawa Nazw Plików (Przebieg {run_count})",
            fetch_data_func=get_records_for_filename_fix,
            process_record_func=_process_filename_fix,
            border_style="yellow"
        )
        await runner.run()
        
        # Pętla kończy się, gdy runner nie znalazł nic do przetworzenia
        if runner.success_count == 0 and runner.error_count == 0:
            logger.info("Nie znaleziono więcej rozbieżności w nazwach plików. Zakończono.")
            break
            
    console.print("\n[bold green]✅ Zakończono. Wszystkie nazwy plików są teraz spójne z metadanymi.[/bold green]")


# === Zadanie 3: Uzupełnianie Metadanych ===
async def _process_metadata_completion(record: dict) -> str | None:
    current_path = Path(record['final_path'])
    if not await asyncio.to_thread(current_path.exists):
        return 'skipped'
    with exiftool.ExifToolHelper() as et:
        exif_metadata_list = await asyncio.get_running_loop().run_in_executor(None, et.get_metadata, str(current_path))
    if not exif_metadata_list:
        raise ValueError(f"Nie odczytano EXIF dla {current_path.name}")
    merged_metadata = exif_metadata_list[0]
    merged_metadata.update(json.loads(record['metadata_json']))
    if 'DateTime' not in merged_metadata or not merged_metadata['DateTime']:
        if date_obj := await get_date_from_metadata(merged_metadata):
            merged_metadata['DateTime'] = date_obj.isoformat()
    if 'FileName' not in merged_metadata or not merged_metadata['FileName']:
        merged_metadata['FileName'] = merged_metadata.get('File:FileName', current_path.name)
    if 'DateTime' in merged_metadata and 'FileName' in merged_metadata:
        dt = datetime.fromisoformat(merged_metadata["DateTime"])
        dest_dir = Path(DOWNLOADS_DIR_BASE) / str(dt.year) / f"{dt.month:02d}"
        expected_path = str(dest_dir / merged_metadata['FileName'])
        await update_entry_with_completed_metadata(record['id'], json.dumps(merged_metadata, ensure_ascii=False), expected_path)
        console.print(f"  [green]Uzupełniono dane dla:[/green] [cyan]{current_path.name}[/cyan]")
    else:
        raise ValueError(f"Nie udało się ustalić daty/nazwy dla ID {record['id']}")
    return None
async def run_metadata_completer():
    if not EXIFTOOL_AVAILABLE:
        console.print(Panel("[bold red]Błąd: Brak 'pyexiftool'![/bold red]\nUruchom: [cyan]pip install pyexiftool[/cyan]", title="Brak Zależności"))
        return
    runner = OfflineTaskRunner(
        task_name="Uzupełniacz Danych i Ścieżek",
        fetch_data_func=get_records_for_metadata_completion,
        process_record_func=_process_metadata_completion,
        border_style="blue"
    )
    await runner.run()

# === POCZĄTEK ZMIAN: Refaktoryzacja Zadania 4 ===
# === Zadanie 4: Zapisywanie Metadanych (EXIF) ===

async def _process_exif_writing(record: dict) -> str | None:
    """
    Przetwarza pojedynczy rekord: odczytuje metadane z JSON i zapisuje je do pliku.
    Zwraca 'skipped' jeśli nie ma czego zapisać lub plik nie istnieje.
    """
    file_path = Path(record['final_path'])
    if not await asyncio.to_thread(file_path.exists):
        logger.warning(f"Pominięto: Plik nie istnieje {file_path}")
        return 'skipped'

    data = json.loads(record['metadata_json'])
    
    tags_to_write = {}
    if desc := data.get("Description"):
        tags_to_write["EXIF:ImageDescription"] = desc
        tags_to_write["XMP:Description"] = desc
    if people := data.get("TaggedPeople"):
        tags_to_write["XMP:Subject"] = people
        tags_to_write["XMP:PersonInImage"] = people
    if albums := data.get("Albums"):
        tags_to_write["XMP:Keywords"] = albums
    if gps := data.get("Experimental_Details", {}).get("GPS_Coords"):
        tags_to_write["EXIF:GPSLatitude"] = gps.get("latitude")
        tags_to_write["EXIF:GPSLongitude"] = gps.get("longitude")

    if not tags_to_write:
        logger.debug(f"Pominięto: Brak tagów do zapisu dla {file_path.name}")
        return 'skipped'

    params = []
    for tag, value in tags_to_write.items():
        if isinstance(value, list):
            for v in value: params.append(f"-{tag}={v}")
        elif value is not None:
            params.append(f"-{tag}={value}")
    
    with exiftool.ExifToolHelper() as et:
        # Pętla asyncio jest już uruchomiona, więc używamy run_in_executor
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, et.execute, "-overwrite_original", "-m", *params, str(file_path))
    
    logger.debug(f"Pomyślnie zapisano {len(tags_to_write)} tagów do pliku {file_path.name}")
    return None

async def run_exif_writer():
    """Odczytuje metadane z bazy danych i zapisuje je do plików na dysku."""
    if not EXIFTOOL_AVAILABLE:
        console.print(Panel("[bold red]Błąd: Brak 'pyexiftool'![/bold red]\nUruchom: [cyan]pip install pyexiftool[/cyan]", title="Brak Zależności"))
        return
    
    # Logika specyficzna dla tego zadania (potwierdzenie) zostaje tutaj.
    console.clear()
    console.print(Panel("✍️ Zapisywanie Metadanych z Bazy do Plików (Exiftool)", expand=False, style="red"))
    console.print("\n[bold yellow]⚠️ UWAGA: Ta operacja nieodwracalnie zmodyfikuje pliki na dysku![/bold yellow]")
    if not Confirm.ask("Czy na pewno chcesz kontynuować? (Zalecana jest kopia zapasowa)", default=False, console=console):
        logger.warning("Operacja zapisu metadanych anulowana przez użytkownika.")
        return

    # Używamy uniwersalnego runnera do wykonania całej pracy.
    runner = OfflineTaskRunner(
        task_name="Zapisywanie Metadanych do Plików",
        fetch_data_func=get_records_for_exif_writing,
        process_record_func=_process_exif_writing,
        border_style="red"
    )
    await runner.run()

# === KONIEC ZMIAN ===
