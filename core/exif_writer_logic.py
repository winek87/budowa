# -*- coding: utf-8 -*-

# plik: core/exif_writer_logic.py
# Wersja 5.0 - Pełna integracja z hermetyzowanym modułem bazy danych
#
# ##############################################################################
# ===                    MODUŁ ZAPISU METADANYCH (EXIF)                      ===
# ##############################################################################
#
# Ten plik zawiera logikę narzędzia "Zapisywarka EXIF". Jego zadaniem jest
# odczytanie bogatych metadanych z bazy danych, a następnie
# trwałe "wypalenie" tych informacji bezpośrednio w plikach na dysku
# za pomocą potężnego narzędzia ExifTool.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import json
import subprocess
import shutil
import logging
from pathlib import Path
from collections import deque
import asyncio
import aiosqlite

# --- Importy z `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.live import Live
from rich.layout import Layout
from rich.table import Table
from rich.prompt import Prompt

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import DATABASE_FILE
# ZMIANA: Dodajemy check_dependency
from .utils import stop_event, create_interactive_menu, check_dependency

# ZMIANA: Importujemy nowe, dedykowane funkcje z modułu bazy danych
from .database import get_exif_writer_stats, get_records_for_exif_processing, setup_database

# --- Konfiguracja i Inicjalizacja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)

# Ścieżka do programu ExifTool. Jeśli jest w PATH systemu, wystarczy "exiftool".
EXIFTOOL_PATH = "exiftool"

# Zestawy rozszerzeń plików do rozróżniania, które tagi zapisać
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".3gp"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".gif", ".webp"}

# ##############################################################################
# ===                    SEKCJA 1: FUNKCJE POMOCNICZE                        ===
# ##############################################################################
#
# Ta sekcja zawiera funkcje wspierające, które przygotowują dane i sprawdzają
# zależności przed uruchomieniem głównej logiki zapisu.
#

def check_exiftool() -> bool:
    """
    Sprawdza, czy program ExifTool jest zainstalowany i dostępny w systemie.
    Używa scentralizowanej funkcji z `utils.py`.
    """
    # ZMIANA: Cała stara logika została zastąpiona jednym, czystym wywołaniem.
    # Używamy `shutil.which`, bo nie potrzebujemy tu pełnego `check_dependency`
    # z pip, a jedynie sprawdzenia, czy program jest w PATH.
    if not shutil.which(EXIFTOOL_PATH):
        console.print(Panel(
            f"[bold red]Błąd: Nie znaleziono programu '{EXIFTOOL_PATH}'![/bold red]\n\n"
            "Zainstaluj ExifTool i upewnij się, że jest w systemowej ścieżce PATH.",
            title="Brak Zależności", border_style="red"
        ))
        return False
    return True

async def get_writer_db_stats() -> bool:
    """
    Asynchronicznie odczytuje i wyświetla w tabeli statystyki z bazy
    danych, delegując zapytania do modułu `database.py`.
    """
    logger.info("Pobieram statystyki zapisu EXIF z bazy danych...")
    
    # ZMIANA: Cała logika zapytań SQL została przeniesiona do `database.py`.
    try:
        stats = await get_exif_writer_stats()

        if stats.get('total_ready') is None:
            console.print("[bold yellow]Baza danych jest pusta lub nie zawiera tabeli 'downloaded_media'.[/bold yellow]")
            return False

        # Wyświetl statystyki w tabeli Rich
        stats_table = Table(title="Statystyki Zapisu Metadanych", show_header=False, box=None, padding=(0, 1))
        stats_table.add_column(style="cyan")
        stats_table.add_column(style="bold", justify="right")
        stats_table.add_row("Pliki gotowe do zapisu (zeskanowane):", f"{stats['total_ready']}")
        stats_table.add_row("  - [green]Zapisano poprawnie[/green]:", f"{stats['success']}")
        stats_table.add_row("  - [yellow]Zapisano częściowo[/yellow]:", f"{stats['partial']}")
        stats_table.add_row("  - [red]Błędy zapisu[/red]:", f"{stats['error']}")
        stats_table.add_row("Pozostało do zapisu:", f"[bold cyan]{stats['not_written']}[/bold cyan]")
        
        console.print(Panel(stats_table, title="[bold blue]Aktualny Stan Zapisu[/]", border_style="blue"))
        logger.info("Pomyślnie pobrano i wyświetlono statystyki zapisu EXIF.")
        return True
        
    except Exception as e:
        logger.error("BŁĄD podczas odczytu statystyk zapisu z bazy danych.", exc_info=True)
        console.print(f"[bold red]Wystąpił nieoczekiwany błąd: {e}[/bold red]")
        return False

async def setup_and_get_records(process_mode: str) -> list | None:
    """
    Asynchronicznie pobiera listę rekordów do przetworzenia, delegując
    zapytania do modułu `database.py`.
    """
    logger.info(f"Pobieram rekordy z bazy w trybie: '{process_mode}'.")

    # ZMIANA: Cała logika zapytań SQL i migracji schematu została przeniesiona.
    try:
        records = await get_records_for_exif_processing(process_mode)
        if records is None: # Oznacza błąd w module bazy danych
            console.print("[bold red]Wystąpił błąd podczas pobierania rekordów z bazy danych.[/bold red]")
            return None
        
        logger.info(f"Pobrano {len(records)} rekordów do przetworzenia.")
        return records

    except Exception as e:
        logger.critical("Nie można pobrać danych z bazy do zapisu EXIF.", exc_info=True)
        console.print(f"[bold red]Wystąpił nieoczekiwany błąd: {e}[/bold red]")
        return None

async def update_write_status(file_path: str, status: str):
    """
    Asynchronicznie aktualizuje status zapisu EXIF (`exif_write_status`)
    dla pojedynczego pliku w bazie danych.
    """
    logger.debug(f"Aktualizuję status zapisu EXIF na '{status}' dla pliku: {Path(file_path).name}")
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.execute(
                "UPDATE downloaded_media SET exif_write_status = ? WHERE final_path = ?",
                (status, file_path)
            )
            await conn.commit()
        logger.debug(f"Status zapisu EXIF dla '{Path(file_path).name}' został zaktualizowany.")
            
    except aiosqlite.Error as e:
        logger.error(f"BŁĄD podczas aktualizacji statusu zapisu EXIF dla '{file_path}': {e}", exc_info=True)

def build_exiftool_args(details: dict, file_path: Path) -> list[str]:
    """
    Na podstawie słownika z metadanymi buduje listę argumentów wiersza poleceń
    dla programu ExifTool.
    """
    args = []
    file_extension = file_path.suffix.lower()

    if dt_iso := details.get("DateTime"):
        try:
            dt_str = dt_iso.replace("-", ":", 2).replace("T", " ")
            if file_extension in IMAGE_EXTENSIONS:
                args.extend([f"-DateTimeOriginal={dt_str}", f"-CreateDate={dt_str}", f"-ModifyDate={dt_str}"])
            elif file_extension in VIDEO_EXTENSIONS:
                args.extend([f"-CreateDate={dt_str}", f"-ModifyDate={dt_str}"])
        except Exception:
            logger.warning(f"Nie udało się sparsować daty '{dt_iso}' dla pliku {file_path.name}")

    if camera := details.get("Camera"):
        parts = camera.split(" ", 1)
        if len(parts) > 1: args.extend([f"-Make={parts[0]}", f"-Model={parts[1]}"])
        else: args.append(f"-Model={camera}")

    keywords = set()
    for key in ["TaggedPeople", "Albums"]:
        if value := details.get(key):
            if isinstance(value, list):
                keywords.update(str(item).strip() for item in value)

    for keyword in sorted(list(keywords)):
        args.extend([f"-Keywords+={keyword}", f"-Subject+={keyword}"])

    if gps_data := details.get("GPS"):
        if 'latitude' in gps_data and 'longitude' in gps_data:
            args.extend([
                f"-GPSLatitude={gps_data['latitude']}",
                f"-GPSLongitude={gps_data['longitude']}",
                f"-GPSLatitudeRef={'N' if gps_data['latitude'] >= 0 else 'S'}",
                f"-GPSLongitudeRef={'E' if gps_data['longitude'] >= 0 else 'W'}"
            ])
    elif location := details.get("Location"):
        args.extend([f"-City={location}", f"-Location={location}"])

    if description := details.get("Description"):
        args.extend([f"-ImageDescription={description}", f"-Caption-Abstract={description}"])
        
    logger.debug(f"Zbudowano {len(args)} argumentów ExifTool dla pliku {file_path.name}.")
    return args

async def run_writer_core(process_mode: str):
    """
    Główna, asynchroniczna logika zapisu metadanych z interfejsem w terminalu.
    """
    title_map = {'new_only': "Nowe Pliki", 'retry_errors': "Ponawianie Błędów", 'force_refresh': "Pełne Odświeżanie"}
    logger.info(f"Uruchamiam Zapisywarkę Metadanych w trybie: [bold cyan]{title_map.get(process_mode)}[/bold cyan]", extra={"markup": True})

    records = await setup_and_get_records(process_mode)
    if not records:
        logger.warning("Nie znaleziono plików do przetworzenia dla wybranego trybu.")
        console.print("\n[bold green]✅ Nie znaleziono żadnych plików do przetworzenia w wybranym trybie.[/bold green]")
        return

    logger.info(f"Znaleziono [cyan]{len(records)}[/cyan] rekordów do zapisu metadanych.", extra={"markup": True})

    counters = {"poprawne": 0, "błędy": 0, "częściowo": 0, "pominięte": 0}
    recent_logs = deque(maxlen=5)
    progress_bar = Progress(TextColumn("[bold blue]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.1f}%", "{task.completed}/{task.total}", TimeRemainingColumn())
    task = progress_bar.add_task("[green]Postęp...", total=len(records))
    layout = Layout()
    layout.split_column(Layout(progress_bar, name="progress", size=3), Layout(name="main_body"), Layout(name="footer", size=3))

    with Live(layout, vertical_overflow="visible", screen=True, transient=True) as live:
        for record in records:
            if stop_event.is_set():
                logger.warning("Przerwanie przez użytkownika. Zatrzymuję zapis...")
                break

            file_path_str = record["final_path"] if "final_path" in record.keys() and record["final_path"] else "Brak ścieżki"
            file_path = Path(file_path_str)
            write_status = "Error"
            log_panel = Panel(f"[red]Nieznany błąd dla {file_path.name}[/red]")

            try:
                details = json.loads(record["metadata_json"])
                
                if not await asyncio.to_thread(file_path.exists):
                    log_panel = Panel(f"Pominięto - plik nie istnieje: [dim]{file_path_str}[/dim]", border_style="yellow")
                    counters["pominięte"] += 1
                else:
                    args = build_exiftool_args(details, file_path)
                    if not args:
                        log_panel = Panel(f"Pominięto - brak tagów do zapisu: [dim]{file_path.name}[/dim]", border_style="yellow")
                        counters["pominięte"] += 1
                        write_status = "Skipped"
                    else:
                        command = [EXIFTOOL_PATH, "-m", "-overwrite_original", "-api", "quicktimeutc", "-charset", "utf8", *args, str(file_path)]
                        result = await asyncio.to_thread(subprocess.run, command, capture_output=True, text=True, check=False, encoding='utf-8')

                        if result.returncode == 0 and ("1 image files updated" in result.stdout.lower() or "1 video files updated" in result.stdout.lower()):
                            counters["poprawne"] += 1; write_status = "Success"
                            log_panel = Panel(Text.from_markup("\n".join(f"[dim]- {arg}[/dim]" for arg in args)), title=f"✅ Zapisano do: [bold green]{file_path.name}[/]", border_style="green")
                        else:
                            logger.warning(f"Zapis wsadowy nie powiódł się dla {file_path.name}. Próbuję pojedynczo...")
                            written_args = []
                            for arg in args:
                                single_cmd = [EXIFTOOL_PATH, "-m", "-overwrite_original", arg, str(file_path)]
                                if (await asyncio.to_thread(subprocess.run, single_cmd, capture_output=True, check=False)).returncode == 0:
                                    written_args.append(arg)
                            if written_args:
                                counters["częściowo"] += 1; write_status = "Partial"
                                summary = Text(f"Zapisano {len(written_args)}/{len(args)} tagów:\n")
                                for arg in written_args: summary.append(f"[dim]- {arg}[/dim]\n")
                                log_panel = Panel(summary, title=f"⚠️ Zapisano częściowo: [bold yellow]{file_path.name}[/]", border_style="yellow")
                            else:
                                counters["błędy"] += 1; write_status = "Error"
                                error_msg = result.stderr.strip() or result.stdout.strip()
                                log_panel = Panel(f"❌ [red]Błąd zapisu:[/red] [dim]{file_path.name}[/dim]\nSzczegóły: {error_msg[:100]}", border_style="red")
            except Exception as e:
                counters["błędy"] += 1; write_status = "Error"
                log_panel = Panel(f"❌ [red]Krytyczny błąd skryptu:[/red] [dim]{e}[/dim]", border_style="red")
                logger.error(f"Krytyczny błąd podczas przetwarzania pliku {file_path_str}", exc_info=True)

            await update_write_status(file_path_str, write_status)
            recent_logs.appendleft(log_panel)
            
            progress_bar.update(task, advance=1)
            layout["main_body"].update(Panel(Group(*recent_logs), title="Ostatnie Akcje"))
            counters_table = Table.grid(expand=True)
            counters_table.add_column(justify="center"); counters_table.add_column(justify="center"); counters_table.add_column(justify="center"); counters_table.add_column(justify="center")
            counters_table.add_row(f"[green]Poprawne: {counters['poprawne']}[/]", f"[yellow]Częściowe: {counters['częściowo']}[/yellow]", f"[red]Błędy: {counters['błędy']}[/]", f"[dim]Pominięte: {counters['pominięte']}[/dim]")
            layout["footer"].update(Panel(counters_table, title="Statystyki Sesji"))
            live.refresh()
            
    logger.info("[bold green]Zakończono zapisywanie metadanych.[/bold green]", extra={"markup": True})

# ##############################################################################
# ===                    SEKCJA 2: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_exif_writer():
    """
    Uruchamia interaktywne menu dla modułu zapisu metadanych EXIF.
    """
    console.clear()
    console.print(Panel("✍️ Zapisywanie Metadanych z Bazy do Plików (Exiftool)", expand=False, style="green"))
    
    exiftool_ok = await asyncio.to_thread(check_exiftool)
    if not exiftool_ok:
        Prompt.ask("\n[bold]Wykryto problemy. Naciśnij Enter, aby wrócić...[/]", console=console)
        return

    stats_ok = await get_writer_db_stats()
    if not stats_ok:
        Prompt.ask("\n[bold]Nie można pobrać statystyk z bazy. Naciśnij Enter, aby wrócić...[/]", console=console)
        return

    menu_items = [
        ("Zapisz metadane dla nowych plików", "new_only"),
        ("Ponów pliki, które miały błędy zapisu", "retry_errors"),
        ("Odśwież wszystkie dane (zapisz od nowa dla wszystkich)", "force_refresh"),
        ("Anuluj i wróć do menu głównego", "exit")
    ]
    
    selected_mode = await create_interactive_menu(
        menu_items,
        "Wybierz tryb pracy Zapisywarki",
        border_style="blue"
    )

    if selected_mode and selected_mode != "exit":
        await run_writer_core(process_mode=selected_mode)
        Prompt.ask("\n[bold]Proces zapisu zakończony. Naciśnij Enter, aby wrócić...[/]", console=console)
    else:
        logger.info("Anulowano. Powrót do menu głównego.")
