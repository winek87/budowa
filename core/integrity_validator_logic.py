# plik: core/integrity_validator_logic.py
# Wersja 9.1 - Scentralizowana logika bazy danych i ujednolicone ścieżki (Refaktoryzacja Fazy 1)

# -*- coding: utf-8 -*-

# plik: core/integrity_validator_logic.py
# Wersja 9.0 - Pełna integracja z asynchronicznym modułem bazy danych
#
# ##############################################################################
# ===                        MODUŁ WALIDATORA INTEGRALNOŚCI                  ===
# ##############################################################################
#
# "Walidator Integralności" to zaawansowany zestaw narzędzi do "sprzątania"
# i weryfikacji pobranej kolekcji. Pozwala na:
#
#  - Obliczanie i zapisywanie sum kontrolnych (hash MD5) dla plików.
#  - Wyszukiwanie "duchów" (wpisów w bazie bez pliku na dysku) i "sierot"
#    (plików na dysku bez wpisu w bazie).
#  - Analizę spójności między datą w metadanych a lokalizacją pliku.
#  - Interaktywne zarządzanie duplikatami plików na podstawie ich zawartości.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import hashlib
import os
import json
import logging
import uuid
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from datetime import datetime
from urllib.parse import unquote, urlparse

# --- Zależności zewnętrzne (opcjonalne) ---
try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False

# --- Importy asynchroniczne ---
import aiosqlite

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text
from rich.live import Live
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.progress import Progress
from rich.align import Align
from rich.layout import Layout

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import DATABASE_FILE, DOWNLOADS_DIR_BASE, LOCAL_SCANNER_DIRECTORIES
from .utils import create_interactive_menu, _interactive_file_selector, get_key, _parse_metadata_for_display

# NOWE, SCENTRALIZOWANE IMPORTY Z MODUŁU BAZY DANYCH
from .database import (
    add_local_file_entry,
    get_downloaded_files_for_validation,
    get_records_to_hash,
    update_hashes_batch,
    get_all_final_paths,
    delete_entries_by_ids,
    get_metadata_for_consistency_check,
    get_duplicate_hashes,
    get_entries_by_hash,
    get_local_import_entries,
    update_paths_for_entry
)

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console()
logger = logging.getLogger(__name__)


# ##############################################################################
# ===           SEKCJA 1: FUNKCJE POMOCNICZE (ASYNC & EFFICIENT)             ===
# ##############################################################################

async def _get_all_disk_paths(root_dir: Path) -> Set[Path]:
    """Asynchronicznie i wydajnie skanuje podany folder w poszukiwaniu plików."""
    disk_paths: Set[Path] = set()
    logger.info(f"Rozpoczynam asynchroniczne skanowanie dysku w folderze: {root_dir}")
    
    loop = asyncio.get_running_loop()
    
    def scan_directory():
        logger.debug("Uruchamiam blokującą operację rglob w osobnym wątku...")
        for path in root_dir.rglob('*'):
            if path.is_file():
                disk_paths.add(path.resolve())
    
    await loop.run_in_executor(None, scan_directory)
    
    logger.info(f"Zakończono skanowanie dysku. Znaleziono {len(disk_paths)} plików.")
    return disk_paths


async def _resolve_duplicates_interactively(duplicate_set: List[Dict]) -> Dict:
    """
    Wyświetla interfejs do rozwiązania pojedynczego zestawu duplikatów,
    korzystając z uniwersalnego komponentu UI.
    """
    from .utils import create_side_by_side_comparison_panel # Importujemy nasz nowy komponent

    selected_to_keep_index = 0
    try:
        # Prosta logika do wstępnego wyboru większego pliku
        size_a_str = duplicate_set[0].get('size', '0 MB').split(' ')[0].replace(',', '.')
        size_b_str = duplicate_set[1].get('size', '0 MB').split(' ')[0].replace(',', '.')
        if float(size_b_str) > float(size_a_str):
            selected_to_keep_index = 1
    except (ValueError, IndexError):
        pass

    def generate_main_layout() -> Layout:
        """Wewnętrzna funkcja renderująca cały widok, włączając nagłówek i stopkę."""

        # Krok 1: Przygotuj dane dla uniwersalnego komponentu
        item_a_details = {
            "ID w Bazie": duplicate_set[0].get('id', 'Brak'),
            "Ścieżka": duplicate_set[0].get('relative_path', 'Brak'),
            "separator_1": "",
            "Data": duplicate_set[0].get('date', 'Brak'),
            "Rozmiar": duplicate_set[0].get('size', 'Brak'),
            "Wymiary": duplicate_set[0].get('dimensions', 'Brak'),
            "separator_2": "",
            "Aparat": duplicate_set[0].get('camera', 'Brak'),
        }
        item_b_details = {
            "ID w Bazie": duplicate_set[1].get('id', 'Brak'),
            "Ścieżka": duplicate_set[1].get('relative_path', 'Brak'),
            "separator_1": "",
            "Data": duplicate_set[1].get('date', 'Brak'),
            "Rozmiar": duplicate_set[1].get('size', 'Brak'),
            "Wymiary": duplicate_set[1].get('dimensions', 'Brak'),
            "separator_2": "",
            "Aparat": duplicate_set[1].get('camera', 'Brak'),
        }

        # Krok 2: Wywołaj uniwersalny komponent, aby wygenerował panel porównawczy
        comparison_panel = create_side_by_side_comparison_panel(
            item_a_details,
            item_b_details,
            is_a_selected=(selected_to_keep_index == 0)
        )

        # Krok 3: Dodaj nagłówek i stopkę
        title_text = f"Wybierz plik do ZACHOWANIA\n[dim]Hash: {duplicate_set[0].get('hash', 'Brak')}[/dim]"
        footer = Align.center(Text("[bold]L/P[/](wybierz)•[bold]ENTER[/](zatwierdź)•[bold]P[/](pomiń)•[bold]Q[/](zakończ)"))
        main_layout = Layout()
        main_layout.split_column(
            Layout(Align.center(Text(title_text)), size=3),
            comparison_panel,
            Layout(footer, size=1)
        )
        return main_layout

    with Live(generate_main_layout(), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_main_layout(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue

            if key.upper() in ["Q", "ESC"]: return {"action": "quit"}
            if key.upper() == "P": return {"action": "skip"}
            if key in ["LEFT", "RIGHT"]: selected_to_keep_index = 1 - selected_to_keep_index
            if key == "ENTER":
                to_keep = duplicate_set[selected_to_keep_index]
                to_delete = [duplicate_set[1 - selected_to_keep_index]]
                return {"action": "resolve", "keep": to_keep, "delete": to_delete}

# ##############################################################################
# ===                    SEKCJA 2: GŁÓWNE FUNKCJE WALIDATORA                 ===
# ##############################################################################

async def verify_file_existence():
    """Weryfikuje, czy pliki z bazy danych istnieją na dysku."""
    console.clear()
    logger.info("Uruchamiam Weryfikator Istnienia Plików...")
    console.print(Panel("👻 Weryfikator Istnienia Plików ('Duchy' w bazie) 👻", expand=False, style="bold yellow"))

    try:
        records_to_check = await get_downloaded_files_for_validation()

        if not records_to_check:
            logger.warning("Nie znaleziono plików do weryfikacji.")
            console.print("\n[green]Nie znaleziono w bazie żadnych plików ze statusem 'downloaded' do weryfikacji.[/green]")
            return

        missing_files = []
        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[green]Sprawdzanie plików na dysku...", total=len(records_to_check))
            for record in records_to_check:
                file_path = Path(record['final_path'])
                if not await asyncio.to_thread(file_path.exists):
                    missing_files.append(record)
                    logger.warning(f"Brak pliku na dysku dla ID={record['id']}: {file_path}")
                progress.update(task, advance=1)

        if not missing_files:
            logger.info("Weryfikacja zakończona pomyślnie.")
            console.print(f"\n[bold green]✅ Weryfikacja zakończona. Wszystkie {len(records_to_check)} pliki z bazy danych istnieją na dysku.[/bold green]")
        else:
            logger.error(f"Znaleziono {len(missing_files)} brakujących plików ('duchów')!")
            console.print(f"\n[bold red]⚠️ Znaleziono {len(missing_files)} brakujących plików:[/bold red]")
            
            table = Table(title="Lista Brakujących Plików ('Duchy')")
            table.add_column("ID Wpisu", style="cyan", justify="right")
            table.add_column("Oczekiwana Ścieżka", style="red")
            for missing in missing_files:
                table.add_row(str(missing['id']), missing['final_path'])
            
            console.print(table)
            console.print("\n[yellow]Powyższe pliki zostały prawdopodobnie usunięte lub przeniesione ręcznie.[/yellow]")
            console.print("[dim]Użyj 'Narzędzia Zaawansowane -> Edytor Bazy Danych', aby usunąć te martwe wpisy.[/dim]")

    except Exception as e:
        logger.critical(f"Wystąpił błąd podczas weryfikacji istnienia plików: {e}", exc_info=True)


async def verify_and_write_hashes():
    """
    Oblicza i zapisuje sumy kontrolne MD5 dla plików, wykorzystując
    wielordzeniowe przetwarzanie dla maksymalnej wydajności.
    """
    console.clear()
    logger.info("Uruchamiam weryfikację i zapis sum kontrolnych (MD5)...")
    console.print(Panel("🧮 Obliczanie i Zapis Sum Kontrolnych (MD5) 🧮", expand=False, style="bold yellow"))
    
    try:
        from concurrent.futures import ProcessPoolExecutor

        records_to_hash = await get_records_to_hash()

        if not records_to_hash:
            logger.info("Wszystkie pliki mają już obliczone sumy kontrolne.")
            console.print("\n[bold green]✅ Wszystkie pobrane pliki w bazie mają już obliczone sumy kontrolne.[/bold green]")
            return

        logger.info(f"Znaleziono {len(records_to_hash)} plików do obliczenia hasha.")
        
        updates_batch: list[tuple[str, int]] = []
        BATCH_SIZE = 100
        
        loop = asyncio.get_running_loop()
        
        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[cyan]Obliczanie hashy (na wielu rdzeniach)...[/]", total=len(records_to_hash))
            
            # Używamy ProcessPoolExecutor do uruchomienia hashowania na wszystkich dostępnych rdzeniach CPU
            with ProcessPoolExecutor() as executor:
                # Przygotowujemy listę ścieżek do przetworzenia
                paths_to_process = [Path(rec['final_path']) for rec in records_to_hash]
                
                # Tworzymy listę przyszłych wyników (futures)
                # UWAGA: _calculate_hash_for_file musi być funkcją na poziomie modułu
                futures = [loop.run_in_executor(executor, _calculate_hash_for_file, path) for path in paths_to_process]
                
                # Mapujemy z powrotem wyniki do ID rekordów
                path_to_id_map = {Path(rec['final_path']): rec['id'] for rec in records_to_hash}
                
                # Przetwarzamy wyniki w miarę ich napływania
                for i, future in enumerate(asyncio.as_completed(futures)):
                    try:
                        file_hash = await future
                        original_path = paths_to_process[i]
                        
                        if file_hash:
                            entry_id = path_to_id_map[original_path]
                            updates_batch.append((file_hash, entry_id))
                    except Exception as e:
                        logger.error(f"Błąd podczas hashowania w podprocesie: {e}")
                    finally:
                        progress.update(task, advance=1)

                    if len(updates_batch) >= BATCH_SIZE:
                        await update_hashes_batch(updates_batch)
                        logger.info(f"Zapisano partię {len(updates_batch)} hashy do bazy.")
                        updates_batch.clear()

        if updates_batch:
            await update_hashes_batch(updates_batch)
            logger.info(f"Zapisano ostatnią partię {len(updates_batch)} hashy.")
            
        console.print(f"\n[bold green]✅ Zakończono. Zaktualizowano sumy kontrolne dla {len(records_to_hash) - len(updates_batch)} plików.[/bold green]")
            
    except Exception as e:
        logger.critical(f"Wystąpił błąd podczas obliczania hashy: {e}", exc_info=True)

async def _calculate_hash_for_file(file_path: Path) -> str | None:
    logger.debug(f"Rozpoczynam obliczanie hasha MD5 dla pliku '{file_path.name}'...")
    hasher = hashlib.md5()
    try:
        def read_and_hash():
            with open(file_path, "rb") as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)
            return hasher.hexdigest()
        file_hash = await asyncio.to_thread(read_and_hash)
        logger.debug(f"Obliczono hash dla '{file_path.name}': {file_hash}")
        return file_hash
    except (IOError, OSError) as e:
        logger.error(f"Błąd odczytu pliku '{file_path}' podczas hashowania: {e}", exc_info=True)
        return None

async def _get_all_disk_paths(root_dir: Path) -> Set[Path]:
    disk_paths: Set[Path] = set()
    logger.info(f"Rozpoczynam asynchroniczne skanowanie dysku w folderze: {root_dir}")
    loop = asyncio.get_running_loop()
    def scan_directory():
        logger.debug("Uruchamiam blokującą operację rglob w osobnym wątku...")
        for path in root_dir.rglob('*'):
            if path.is_file():
                disk_paths.add(path.resolve())
    await loop.run_in_executor(None, scan_directory)
    logger.info(f"Zakończono skanowanie dysku. Znaleziono {len(disk_paths)} plików.")
    return disk_paths

async def _resolve_duplicates_interactively(duplicate_set: List[Dict]) -> Dict:
    selected_to_keep_index = 0
    try:
        size_a_str = duplicate_set[0].get('size', '0 MB').split(' ')[0].replace(',', '.')
        size_b_str = duplicate_set[1].get('size', '0 MB').split(' ')[0].replace(',', '.')
        if float(size_b_str) > float(size_a_str):
            selected_to_keep_index = 1
    except (ValueError, IndexError):
        pass
    def generate_main_layout() -> Layout:
        item_a_details = {
            "ID w Bazie": duplicate_set[0].get('id', 'Brak'), "Ścieżka": duplicate_set[0].get('relative_path', 'Brak'),
            "separator_1": "", "Data": duplicate_set[0].get('date', 'Brak'),
            "Rozmiar": duplicate_set[0].get('size', 'Brak'), "Wymiary": duplicate_set[0].get('dimensions', 'Brak'),
            "separator_2": "", "Aparat": duplicate_set[0].get('camera', 'Brak'),
        }
        item_b_details = {
            "ID w Bazie": duplicate_set[1].get('id', 'Brak'), "Ścieżka": duplicate_set[1].get('relative_path', 'Brak'),
            "separator_1": "", "Data": duplicate_set[1].get('date', 'Brak'),
            "Rozmiar": duplicate_set[1].get('size', 'Brak'), "Wymiary": duplicate_set[1].get('dimensions', 'Brak'),
            "separator_2": "", "Aparat": duplicate_set[1].get('camera', 'Brak'),
        }
        comparison_panel = create_side_by_side_comparison_panel(
            item_a_details, item_b_details, is_a_selected=(selected_to_keep_index == 0)
        )
        title_text = f"Wybierz plik do ZACHOWANIA\n[dim]Hash: {duplicate_set[0].get('hash', 'Brak')}[/dim]"
        footer = Align.center(Text("[bold]L/P[/](wybierz)•[bold]ENTER[/](zatwierdź)•[bold]P[/](pomiń)•[bold]Q[/](zakończ)"))
        main_layout = Layout()
        main_layout.split_column(
            Layout(Align.center(Text(title_text)), size=3),
            comparison_panel,
            Layout(footer, size=1)
        )
        return main_layout
    with Live(generate_main_layout(), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_main_layout(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue
            if key.upper() in ["Q", "ESC"]: return {"action": "quit"}
            if key.upper() == "P": return {"action": "skip"}
            if key in ["LEFT", "RIGHT"]: selected_to_keep_index = 1 - selected_to_keep_index
            if key == "ENTER":
                to_keep = duplicate_set[selected_to_keep_index]
                to_delete = [duplicate_set[1 - selected_to_keep_index]]
                return {"action": "resolve", "keep": to_keep, "delete": to_delete}


async def find_and_fix_inconsistencies():
    """
    Znajduje i pozwala naprawić niespójności między bazą danych a dyskiem,
    takie jak "duchy" (wpisy w bazie bez plików) i "sieroty" (pliki na dysku bez wpisów).
    """
    console.clear()
    logger.info("Uruchamiam wyszukiwanie niespójności (Baza vs. Dysk)...")
    console.print(Panel("👻 Wyszukiwanie Niespójności (Duchy i Sieroty)", expand=False, style="bold yellow"))

    try:
        # Krok 1: Wczytaj dane z bazy
        with console.status("[cyan]Wczytywanie rekordów z bazy danych...[/]"):
            db_records = await get_all_final_paths()
        db_paths = {Path(rec['final_path']).resolve() for rec in db_records}
        logger.info(f"Znaleziono {len(db_paths)} unikalnych ścieżek w bazie danych.")

        # Krok 2: Zbuduj listę folderów do przeskanowania i poinformuj użytkownika
        paths_to_scan = {Path(DOWNLOADS_DIR_BASE).resolve()}
        for p_str in LOCAL_SCANNER_DIRECTORIES:
            paths_to_scan.add(Path(p_str).resolve())

        console.print("\n[bold]Skanowane będą następujące lokalizacje (zgodnie z config.py):[/bold]")
        for path in paths_to_scan:
            console.print(f"  - [cyan]{path}[/cyan]")
        
        # Krok 3: Skanuj pliki na dysku
        disk_paths = set()
        with console.status("[cyan]Skanowanie plików na dysku (może to potrwać)...[/]"):
            for root_dir in paths_to_scan:
                if await asyncio.to_thread(root_dir.is_dir):
                    logger.info(f"Skanuję rekursywnie folder: {root_dir}")
                    disk_paths.update(await _get_all_disk_paths(root_dir))
        logger.info(f"Znaleziono {len(disk_paths)} plików na dysku w skonfigurowanych lokalizacjach.")
        
        # Krok 4: Porównaj wyniki i znajdź niespójności
        db_ghosts = [rec for rec in db_records if Path(rec['final_path']).resolve() not in disk_paths]
        disk_orphans = sorted([path for path in disk_paths if path not in db_paths])

        # Krok 5: Obsłuż znalezione "duchy"
        if db_ghosts:
            console.print(f"\n[bold yellow]Znaleziono {len(db_ghosts)} 'duchów' w bazie (wpisy bez plików na dysku).[/]")
            if Confirm.ask("[cyan]Czy chcesz usunąć te martwe wpisy z bazy?[/]", default=True):
                ids_to_delete = [ghost['id'] for ghost in db_ghosts]
                await delete_entries_by_ids(ids_to_delete)
                console.print(f"[green]Usunięto {len(ids_to_delete)} martwych wpisów z bazy.[/green]")
        
        # Krok 6: Obsłuż znalezione "sieroty"
        if disk_orphans:
            console.print(f"\n[bold yellow]Znaleziono {len(disk_orphans)} 'sierot' na dysku (pliki bez wpisów w bazie).[/]")
            selected_files_to_process = await _interactive_file_selector(disk_orphans, "Wybierz 'osierocone' pliki do dalszych działań")

            if selected_files_to_process:
                action = await create_interactive_menu(
                    [("Zaimportuj wybrane pliki do bazy", "import"), ("Usuń wybrane pliki z dysku", "delete"), ("Anuluj", "cancel")],
                    "Co zrobić z wybranymi plikami?"
                )
                
                if action == "import" and EXIFTOOL_AVAILABLE:
                    imported_count = 0
                    error_count = 0
                    with Progress() as progress:
                        task = progress.add_task("[green]Importuję pliki...", total=len(selected_files_to_process))
                        for file_path in selected_files_to_process:
                            # --- POCZĄTEK KLUCZOWEJ POPRAWKI ---
                            try:
                                with exiftool.ExifToolHelper() as et:
                                    metadata_list = et.get_metadata(str(file_path))
                                    if not metadata_list:
                                        raise ValueError("Exiftool nie zwrócił żadnych metadanych.")
                                    metadata = metadata_list[0]
                                
                                if await add_local_file_entry(file_path, metadata):
                                    imported_count += 1
                                    logger.info(f"Pomyślnie zaimportowano plik: {file_path.name}")
                                else:
                                    logger.warning(f"Pominięto import pliku (prawdopodobnie już istnieje w bazie pod innym URL): {file_path.name}")
                            except Exception as e:
                                error_count += 1
                                logger.error(f"Błąd podczas importu pliku {file_path.name}: {e}", exc_info=True)
                                console.print(f"[bold red]Błąd importu pliku '{file_path.name}'. Sprawdź logi.[/bold red]")
                            # --- KONIEC KLUCZOWEJ POPRAWKI ---
                            progress.update(task, advance=1)
                    
                    console.print(f"\n[bold green]Zakończono import.[/bold green]")
                    console.print(f"  - Pomyślnie zaimportowano: [cyan]{imported_count}[/cyan] plików.")
                    if error_count > 0:
                        console.print(f"  - Błędy: [red]{error_count}[/red]. Sprawdź logi, aby uzyskać więcej informacji.")

                elif action == "delete":
                    if Confirm.ask(f"[bold red]Czy na pewno chcesz trwale usunąć {len(selected_files_to_process)} plików z dysku?[/]", default=False):
                        deleted_count = 0
                        with Progress() as progress:
                            task = progress.add_task("[red]Usuwam pliki...", total=len(selected_files_to_process))
                            for file_path in selected_files_to_process:
                                try:
                                    await asyncio.to_thread(os.remove, file_path)
                                    deleted_count += 1
                                except OSError as e:
                                    logger.error(f"Nie udało się usunąć pliku {file_path}: {e}")
                                progress.update(task, advance=1)
                        console.print(f"[green]Pomyślnie usunięto {deleted_count} plików.[/green]")
        
        if not db_ghosts and not disk_orphans:
            console.print("\n[bold green]✅ Nie znaleziono żadnych niespójności. Baza danych jest w pełni zsynchronizowana z plikami na dysku.[/bold green]")

    except Exception as e:
        logger.critical(f"Błąd krytyczny podczas wyszukiwania niespójności: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/bold red]")


async def synchronize_local_file_paths():
    """
    NOWE NARZĘDZIE: Znajduje wpisy w bazie dla plików importowanych lokalnie
    i naprawia w nich `final_path`, jeśli jest nieaktualny.
    """
    console.clear()
    logger.info("Uruchamiam synchronizację ścieżek dla plików lokalnych...")
    console.print(Panel("🔄 Synchronizacja Ścieżek Plików Lokalnych", expand=False, style="bold blue"))

    try:
        with console.status("[cyan]Pobieranie wpisów z bazy danych...[/]"):
            local_entries = await get_local_import_entries()

        if not local_entries:
            console.print("\n[green]Nie znaleziono w bazie żadnych plików importowanych lokalnie.[/green]")
            return

        fixed_count = 0
        with Progress() as progress:
            task = progress.add_task("[green]Weryfikuję wpisy...", total=len(local_entries))
            for entry in local_entries:
                try:
                    # Odtwarzamy oryginalną ścieżkę z URL
                    url_path_str = unquote(urlparse(entry['url']).path)
                    # Usuwamy wiodący '/' dla systemów Windows
                    if os.name == 'nt' and url_path_str.startswith('/'):
                        disk_path = Path(url_path_str[1:])
                    else:
                        disk_path = Path(url_path_str)
                    
                    current_final_path = Path(entry['final_path']) if entry['final_path'] else None

                    # Sprawdzamy, czy plik istnieje na dysku i czy ścieżka w bazie jest poprawna
                    if await asyncio.to_thread(disk_path.exists) and disk_path.resolve() != (current_final_path.resolve() if current_final_path else None):
                        logger.warning(f"Naprawiam ścieżkę dla ID={entry['id']}. Stara: '{current_final_path}', Nowa: '{disk_path.resolve()}'")
                        await update_paths_for_entry(entry['id'], str(disk_path.resolve()))
                        fixed_count += 1
                except Exception as e:
                    logger.error(f"Błąd podczas przetwarzania wpisu ID={entry['id']}: {e}")
                finally:
                    progress.update(task, advance=1)
        
        console.print(f"\n[bold green]✅ Synchronizacja zakończona.[/bold green]")
        console.print(f"   - Naprawiono [cyan]{fixed_count}[/cyan] nieaktualnych wpisów.")

    except Exception as e:
        logger.critical(f"Błąd krytyczny podczas synchronizacji ścieżek: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/bold red]")


async def find_and_fix_inconsistencies_bak():
    """
    Znajduje i pozwala naprawić niespójności między bazą danych a dyskiem,
    takie jak "duchy" (wpisy w bazie bez plików) i "sieroty" (pliki na dysku bez wpisów).
    """
    console.clear()
    logger.info("Uruchamiam wyszukiwanie niespójności (Baza vs. Dysk)...")
    console.print(Panel("👻 Wyszukiwanie Niespójności (Duchy i Sieroty)", expand=False, style="bold yellow"))

    try:
        # Krok 1: Wczytaj dane z bazy
        with console.status("[cyan]Wczytywanie rekordów z bazy danych...[/]"):
            db_records = await get_all_final_paths()
        db_paths = {Path(rec['final_path']).resolve() for rec in db_records}
        logger.info(f"Znaleziono {len(db_paths)} unikalnych ścieżek w bazie danych.")

        # Krok 2: Zbuduj listę folderów do przeskanowania i poinformuj użytkownika
        # Używamy seta, aby uniknąć skanowania tego samego folderu wielokrotnie
        paths_to_scan = {Path(DOWNLOADS_DIR_BASE).resolve()}
        for p_str in LOCAL_SCANNER_DIRECTORIES:
            paths_to_scan.add(Path(p_str).resolve())

        console.print("\n[bold]Skanowane będą następujące lokalizacje (zgodnie z config.py):[/bold]")
        for path in paths_to_scan:
            console.print(f"  - [cyan]{path}[/cyan]")
        
        # Krok 3: Skanuj pliki na dysku
        disk_paths = set()
        with console.status("[cyan]Skanowanie plików na dysku (może to potrwać)...[/]"):
            for root_dir in paths_to_scan:
                if await asyncio.to_thread(root_dir.is_dir):
                    logger.info(f"Skanuję rekursywnie folder: {root_dir}")
                    disk_paths.update(await _get_all_disk_paths(root_dir))
        logger.info(f"Znaleziono {len(disk_paths)} plików na dysku w skonfigurowanych lokalizacjach.")
        
        # Krok 4: Porównaj wyniki i znajdź niespójności
        logger.info(f"Porównuję {len(db_paths)} wpisów z bazy z {len(disk_paths)} plikami na dysku.")
        db_ghosts = [rec for rec in db_records if Path(rec['final_path']).resolve() not in disk_paths]
        disk_orphans = sorted([path for path in disk_paths if path not in db_paths])

        # Krok 5: Obsłuż znalezione "duchy"
        if db_ghosts:
            console.print(f"\n[bold yellow]Znaleziono {len(db_ghosts)} 'duchów' w bazie (wpisy bez plików na dysku).[/]")
            if Confirm.ask("[cyan]Czy chcesz usunąć te martwe wpisy z bazy?[/]", default=True):
                ids_to_delete = [ghost['id'] for ghost in db_ghosts]
                await delete_entries_by_ids(ids_to_delete)
                console.print(f"[green]Usunięto {len(ids_to_delete)} martwych wpisów z bazy.[/green]")
        
        # Krok 6: Obsłuż znalezione "sieroty"
        if disk_orphans:
            console.print(f"\n[bold yellow]Znaleziono {len(disk_orphans)} 'sierot' na dysku (pliki bez wpisów w bazie).[/]")
            selected_files_to_process = await _interactive_file_selector(disk_orphans, "Wybierz 'osierocone' pliki do dalszych działań")

            if selected_files_to_process:
                action = await create_interactive_menu(
                    [("Zaimportuj wybrane pliki do bazy", "import"), ("Usuń wybrane pliki z dysku", "delete"), ("Anuluj", "cancel")],
                    "Co zrobić z wybranymi plikami?"
                )
                
                if action == "import" and EXIFTOOL_AVAILABLE:
                    # Logika importu
                    imported_count = 0
                    with Progress() as progress:
                        task = progress.add_task("[green]Importuję pliki...", total=len(selected_files_to_process))
                        for file_path in selected_files_to_process:
                            with exiftool.ExifToolHelper() as et:
                                metadata = et.get_metadata(str(file_path))[0]
                            if await add_local_file_entry(file_path, metadata):
                                imported_count += 1
                            progress.update(task, advance=1)
                    console.print(f"[green]Pomyślnie zaimportowano {imported_count} plików.[/green]")

                elif action == "delete":
                    # Logika usuwania
                    if Confirm.ask(f"[bold red]Czy na pewno chcesz trwale usunąć {len(selected_files_to_process)} plików z dysku?[/]", default=False):
                        deleted_count = 0
                        with Progress() as progress:
                            task = progress.add_task("[red]Usuwam pliki...", total=len(selected_files_to_process))
                            for file_path in selected_files_to_process:
                                try:
                                    await asyncio.to_thread(os.remove, file_path)
                                    deleted_count += 1
                                except OSError as e:
                                    logger.error(f"Nie udało się usunąć pliku {file_path}: {e}")
                                progress.update(task, advance=1)
                        console.print(f"[green]Pomyślnie usunięto {deleted_count} plików.[/green]")
        
        if not db_ghosts and not disk_orphans:
            console.print("\n[bold green]✅ Nie znaleziono żadnych niespójności. Baza danych jest w pełni zsynchronizowana z plikami na dysku.[/bold green]")

    except Exception as e:
        logger.critical(f"Błąd krytyczny podczas wyszukiwania niespójności: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/bold red]")

async def analyze_metadata_consistency():
    """Sprawdza spójność między ścieżką pliku a datą w metadanych."""
    console.clear()
    logger.info("Uruchamiam analizę spójności metadanych...")
    console.print(Panel("🔗 Analiza Spójności Metadanych (Ścieżka vs. Data) 🔗", expand=False, style="bold yellow"))
    
    mismatches = []
    
    try:
        all_records = await get_metadata_for_consistency_check()
        
        if not all_records:
            console.print("\n[green]Nie znaleziono plików z wymaganymi danymi do analizy.[/green]")
            return

        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[cyan]Analizowanie spójności...", total=len(all_records))
            for row in all_records:
                try:
                    path = Path(row['final_path'])
                    if len(path.parts) < 3: continue
                        
                    db_datetime = datetime.fromisoformat(row['dt_from_json'].replace('Z', '+00:00'))
                    path_month = int(path.parent.name)
                    path_year = int(path.parent.parent.name)

                    if db_datetime.year != path_year or db_datetime.month != path_month:
                        mismatches.append({'path': str(path), 'db_date': f"{db_datetime.year}-{db_datetime.month:02d}", 'path_date': f"{path_year}-{path_month:02d}"})
                except (ValueError, IndexError, TypeError):
                    continue
                finally:
                    progress.update(task, advance=1)

        if not mismatches:
            console.print("\n[bold green]✅ Spójność metadanych idealna![/bold green]")
        else:
            # ... (wyświetlanie wyników bez zmian) ...
            pass

    except Exception as e:
        logger.critical(f"Błąd krytyczny podczas analizy spójności: {e}", exc_info=True)


async def find_duplicates_by_hash():
    """Znajduje i pozwala zarządzać duplikatami plików na podstawie hasha MD5."""
    console.clear()
    logger.info("Uruchamiam Menedżer Duplikatów (wg hash MD5)...")
    console.print(Panel("🧩 Menedżer Duplikatów Plików (wg zawartości) 🧩", expand=False, style="bold yellow"))
    
    try:
        duplicate_hashes = await get_duplicate_hashes()

        if not duplicate_hashes:
            console.print("\n[bold green]✅ Nie znaleziono żadnych duplikatów.[/bold green]")
            return

        logger.warning(f"Znaleziono {len(duplicate_hashes)} zestawy duplikatów.")
        all_files_to_delete = []

        for i, hash_val in enumerate(duplicate_hashes):
            console.clear()
            console.print(Panel(f"[bold yellow]Zestaw duplikatów {i + 1}/{len(duplicate_hashes)}[/]", expand=False))

            files_in_set_raw = await get_entries_by_hash(hash_val)
            files_in_set = []
            for file_row in files_in_set_raw:
                metadata = json.loads(file_row['metadata_json'] or '{}')
                file_path = Path(file_row['final_path'])
                display_info = _parse_metadata_for_display(metadata, file_path)
                files_in_set.append({"id": file_row['id'], "path": file_path, "hash": hash_val, **display_info})

            if len(files_in_set) > 1:
                resolution = await _resolve_duplicates_interactively(files_in_set)
                if resolution.get("action") == "quit": break
                if resolution.get("action") == "resolve":
                    all_files_to_delete.extend(resolution['delete'])

        if all_files_to_delete:
            console.clear()
            console.print(Panel("[bold red]Podsumowanie Akcji Usunięcia[/]", expand=False))
            console.print(f"Wybrano [cyan]{len(all_files_to_delete)}[/cyan] plików do usunięcia.")

            if Confirm.ask("\n[bold red]Czy na pewno chcesz TRWALE usunąć te pliki?[/]", default=False):
                ids_to_delete = [f['id'] for f in all_files_to_delete]
                
                with Progress(console=console, transient=True) as progress:
                    task = progress.add_task("[red]Usuwam pliki...", total=len(all_files_to_delete))
                    for file_info in all_files_to_delete:
                        if await asyncio.to_thread(file_info['path'].exists):
                            await asyncio.to_thread(os.remove, file_info['path'])
                        progress.update(task, advance=1)
                
                await delete_entries_by_ids(ids_to_delete)
                console.print(f"\n[bold green]✅ Usunięto {len(ids_to_delete)} duplikatów.[/bold green]")

    except Exception as e:
        logger.critical(f"Błąd krytyczny w menedżerze duplikatów: {e}", exc_info=True)


# ##############################################################################
# ===                    SEKCJA 3: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_integrity_validator():
    """Wyświetla i zarządza interaktywnym menu dla Walidatora Integralności."""
    logger.info("Uruchamiam menu Walidatora Integralności Danych.")
    
    menu_items = [
        ("Sprawdź istnienie plików (Baza vs Dysk)", verify_file_existence),
        ("Oblicz i zapisz sumy kontrolne plików (hash MD5)", verify_and_write_hashes),
        ("Wyszukaj niespójności (pliki-sieroty i duchy)", find_and_fix_inconsistencies),
        ("[bold blue]Synchronizuj ścieżki dla plików lokalnych (Napraw 'sieroty')[/bold blue]", synchronize_local_file_paths),
        ("Analizuj spójność metadanych (ścieżka vs. data)", analyze_metadata_consistency),
        ("Znajdź i zarządzaj duplikatami (wg zawartości)", find_duplicates_by_hash),
        ("Wróć do menu głównego", "exit")
    ]


    while True:
        console.clear()
        selected_action = await create_interactive_menu(
            menu_items,
            "Walidator Integralności Danych",
            border_style="blue"
        )

        if selected_action == "exit" or selected_action is None:
            logger.info("Zamykanie Walidatora Integralności.")
            break

        await selected_action()
        
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter...[/]")
