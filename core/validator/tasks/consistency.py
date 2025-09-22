# plik: core/validator/tasks/consistency.py
# Wersja 1.5 - Używa dedykowanego, ulepszonego menu z validator.utils
# -*- coding: utf-8 -*-

import asyncio
import logging
import os
from pathlib import Path
from urllib.parse import unquote, urlparse

try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
from rich.prompt import Confirm

from ...config import DOWNLOADS_DIR_BASE, LOCAL_SCANNER_DIRECTORIES
from ...database import (add_local_file_entry, get_all_final_paths,
                           delete_entries_by_ids, get_local_import_entries,
                           update_paths_for_entry)
from ..utils import interactive_file_selector, create_validator_menu

logger = logging.getLogger(__name__)
console = Console()

async def _get_all_disk_paths(root_dir: Path) -> set[Path]:
    disk_paths: set[Path] = set()
    loop = asyncio.get_running_loop()
    def scan_directory():
        for path in root_dir.rglob('*'):
            if path.is_file(): disk_paths.add(path.resolve())
    await loop.run_in_executor(None, scan_directory)
    return disk_paths

async def find_and_fix_inconsistencies():
    console.clear()
    console.print(Panel("👻 Wyszukiwanie Niespójności (Duchy i Sieroty)", expand=False, style="bold yellow"))
    try:
        with console.status("[cyan]Wczytywanie rekordów z bazy danych...[/]"):
            db_records = await get_all_final_paths()
        db_paths = {Path(rec['final_path']).resolve() for rec in db_records}
        
        paths_to_scan = {Path(DOWNLOADS_DIR_BASE).resolve()}
        paths_to_scan.update(Path(p_str).resolve() for p_str in LOCAL_SCANNER_DIRECTORIES)

        console.print("\n[bold]Skanowane lokalizacje (z config.py):[/bold]")
        for path in paths_to_scan: console.print(f"  - [cyan]{path}[/cyan]")

        disk_paths = set()
        with console.status("[cyan]Skanowanie plików na dysku (może to potrwać)...[/]"):
            for root_dir in paths_to_scan:
                if await asyncio.to_thread(root_dir.is_dir):
                    disk_paths.update(await _get_all_disk_paths(root_dir))
        
        db_ghosts = [rec for rec in db_records if Path(rec['final_path']).resolve() not in disk_paths]
        disk_orphans = sorted([path for path in disk_paths if path not in db_paths])

        if db_ghosts:
            console.print(f"\n[bold yellow]Znaleziono {len(db_ghosts)} 'duchów' w bazie (wpisy bez plików).[/]")
            if Confirm.ask("[cyan]Czy chcesz usunąć te martwe wpisy z bazy?[/]", default=True):
                ids_to_delete = [ghost['id'] for ghost in db_ghosts]
                await delete_entries_by_ids(ids_to_delete)
                console.print(f"[green]Usunięto {len(ids_to_delete)} martwych wpisów.[/green]")

        if disk_orphans:
            console.print(f"\n[bold yellow]Znaleziono {len(disk_orphans)} 'sierot' na dysku (pliki bez wpisów).[/]")
            selected_files = await interactive_file_selector(disk_orphans, "Wybierz 'osierocone' pliki do dalszych działań")

            if selected_files:
                action = await create_validator_menu(
                    [("Zaimportuj wybrane pliki do bazy", "import"), ("Usuń wybrane pliki z dysku", "delete"), ("Anuluj", "cancel")],
                    "Co zrobić z wybranymi plikami?"
                )

                if action == "import":
                    if not EXIFTOOL_AVAILABLE:
                        console.print(Panel("[bold red]Błąd: Brak 'pyexiftool'![/bold red]\nUruchom: [cyan]pip install pyexiftool[/cyan]", title="Brak Zależności"))
                        return
                    
                    imported_count, error_count = 0, 0
                    with Progress() as progress:
                        task = progress.add_task("[green]Importuję pliki...", total=len(selected_files))
                        for file_path in selected_files:
                            try:
                                with exiftool.ExifToolHelper() as et:
                                    metadata_list = et.get_metadata(str(file_path))
                                if not metadata_list: raise ValueError("Exiftool nie zwrócił metadanych.")
                                
                                if await add_local_file_entry(file_path, metadata_list[0]):
                                    imported_count += 1
                                else:
                                    logger.warning(f"Pominięto import (prawdopodobnie już istnieje): {file_path.name}")
                            except Exception as e:
                                error_count += 1
                                logger.error(f"Błąd importu pliku {file_path.name}: {e}", exc_info=True)
                            progress.update(task, advance=1)
                    console.print(f"\n[bold green]Import zakończony. Zaimportowano: [cyan]{imported_count}[/cyan]. Błędy: [red]{error_count}[/red].[/bold green]")

                elif action == "delete":
                    if Confirm.ask(f"[bold red]Czy na pewno chcesz TRWALE usunąć {len(selected_files)} plików z dysku?[/]", default=False):
                        deleted_count = 0
                        with Progress() as progress:
                            task = progress.add_task("[red]Usuwam pliki...", total=len(selected_files))
                            for file_path in selected_files:
                                try:
                                    await asyncio.to_thread(os.remove, file_path)
                                    deleted_count += 1
                                except OSError as e:
                                    logger.error(f"Nie udało się usunąć pliku {file_path}: {e}")
                                progress.update(task, advance=1)
                        console.print(f"[green]Pomyślnie usunięto {deleted_count} plików.[/green]")

        if not db_ghosts and not disk_orphans:
            console.print("\n[bold green]✅ Nie znaleziono niespójności. Baza danych jest zsynchronizowana z dyskiem.[/bold green]")
    except Exception as e:
        logger.critical(f"Błąd krytyczny: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/bold red]")

async def synchronize_local_file_paths():
    console.clear()
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
                    url_path_str = unquote(urlparse(entry['url']).path)
                    disk_path = Path(url_path_str[1:]) if os.name == 'nt' and url_path_str.startswith('/') else Path(url_path_str)
                    current_final_path = Path(entry['final_path']) if entry['final_path'] else None
                    if await asyncio.to_thread(disk_path.exists) and disk_path.resolve() != (current_final_path.resolve() if current_final_path else None):
                        await update_paths_for_entry(entry['id'], str(disk_path.resolve()))
                        fixed_count += 1
                except Exception as e:
                    logger.error(f"Błąd przetwarzania wpisu ID={entry['id']}: {e}")
                finally:
                    progress.update(task, advance=1)
        console.print(f"\n[bold green]✅ Synchronizacja zakończona.[/bold green]")
        console.print(f"   - Naprawiono [cyan]{fixed_count}[/cyan] nieaktualnych wpisów.")
    except Exception as e:
        logger.critical(f"Błąd krytyczny: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/bold red]")
