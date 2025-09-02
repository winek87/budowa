# plik: core/backup_logic.py
# Wersja 6.2 - Poprawiona, stabilna obsługa paska postępu w asyncio

import os
import asyncio
import shutil
import logging
from pathlib import Path
from datetime import datetime
import zipfile
import tarfile
from time import sleep, strftime

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import (
    BACKUP_DIR,
    FILES_TO_BACKUP,
    PROJECT_ROOT,
    PROJECT_BACKUP_CONFIG
)
from .utils import create_interactive_menu
from .database import setup_database

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


# ##############################################################################
# ===                   SEKCJA 1: FUNKCJE TWORZENIA KOPII                    ===
# ##############################################################################

async def create_core_app_backup():
    """
    Tworzy archiwum .tar.gz zawierające wyłącznie kluczowe pliki aplikacji.
    """
    logger.info("Rozpoczynam procedurę tworzenia kopii zapasowej rdzenia aplikacji.")
    console.print(Panel("[bold blue]📦 Tworzenie Kopii Zapasowej Rdzenia Aplikacji (Kod) 📦[/]", border_style="blue"))

    config = PROJECT_BACKUP_CONFIG
    archive_dir = Path(config["ARCHIVE_DIR"])
    project_root_path = Path(PROJECT_ROOT)

    try:
        await asyncio.to_thread(archive_dir.mkdir, parents=True, exist_ok=True)
    except OSError as e:
        logger.critical(f"Nie można utworzyć folderu na kopie zapasowe: {e}", exc_info=True)
        return

    timestamp = strftime("%Y%m%d_%H%M%S")
    base_name = "core_app_backup"
    archive_path = archive_dir / f"{base_name}_{timestamp}.tar.gz"

    paths_to_include = [
        project_root_path / "core",
        project_root_path / "uruchom.py",
        project_root_path / "start.py"
    ]
    
    files_to_pack = []
    for p in paths_to_include:
        if not p.exists():
            logger.warning(f"Pomijam w backupie (nie istnieje): {p}")
            continue
        if p.is_dir():
            files_to_pack.extend([f for f in p.rglob('*') if f.is_file() and "__pycache__" not in f.parts])
        elif p.is_file():
            files_to_pack.append(p)

    if not files_to_pack:
        logger.error("Nie znaleziono żadnych kluczowych plików do archiwizacji.")
        return

    progress = Progress(
        TextColumn("[cyan]{task.description}[/]"), BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%", "•",
        TextColumn("[magenta]{task.completed}/{task.total} plików[/]"), "•",
        TimeRemainingColumn(), transient=True
    )

    loop = asyncio.get_running_loop()
    try:
        with progress:
            task = progress.add_task("Pakowanie rdzenia...", total=len(files_to_pack))
            def create_tar_archive():
                with tarfile.open(archive_path, "w:gz") as tar:
                    for path in files_to_pack:
                        arcname = path.relative_to(project_root_path)
                        # --- POPRAWKA: Używamy prostej funkcji lambda ---
                        loop.call_soon_threadsafe(lambda p=arcname: progress.update(task, advance=1, description=f"Pakowanie: [dim]{p}[/dim]"))
                        tar.add(path, arcname=arcname)
                        sleep(0.01)

            await asyncio.to_thread(create_tar_archive)
        
        console.print(f"\n[green]✅ Kopia zapasowa rdzenia aplikacji utworzona w:[/] [cyan]{archive_path.resolve()}[/]")
    except Exception as e:
        logger.critical(f"Krytyczny błąd podczas tworzenia kopii rdzenia: {e}", exc_info=True)


async def create_full_project_backup():
    """
    Tworzy archiwum .tar.gz z plikami całego projektu, respektując .gitignore.
    """
    logger.info("Rozpoczynam procedurę tworzenia pełnej kopii zapasowej projektu.")
    console.print(Panel("[bold blue]📦 Tworzenie Pełnej Kopii Zapasowej Projektu 📦[/]", border_style="blue"))

    config = PROJECT_BACKUP_CONFIG
    archive_dir = Path(config["ARCHIVE_DIR"])
    project_root_path = Path(PROJECT_ROOT)

    try:
        await asyncio.to_thread(archive_dir.mkdir, parents=True, exist_ok=True)
    except OSError as e:
        logger.critical(f"Nie można utworzyć folderu na kopie zapasowe: {e}", exc_info=True)
        return

    timestamp = strftime("%Y%m%d_%H%M%S")
    base_name = config["BASE_NAME"]
    archive_path = archive_dir / f"{base_name}_{timestamp}.tar.gz"

    def get_files_to_backup():
        gitignore_path = project_root_path / ".gitignore"
        exclude_patterns = set(config.get("PATTERNS_TO_EXCLUDE", []))
        if gitignore_path.exists():
            with open(gitignore_path, 'r', encoding='utf-8') as f:
                exclude_patterns.update(line.strip() for line in f if line.strip() and not line.startswith('#'))
        
        files_to_pack = []
        for item in project_root_path.rglob('*'):
            if not item.is_file(): continue
            relative_path_str = str(item.relative_to(project_root_path).as_posix())
            if any(Path(relative_path_str).match(pattern) for pattern in exclude_patterns):
                continue
            files_to_pack.append(item)
        return files_to_pack

    paths_to_pack = await asyncio.to_thread(get_files_to_backup)
    if not paths_to_pack:
        logger.error("Nie znaleziono żadnych plików do archiwizacji.")
        return

    progress = Progress(
        TextColumn("[cyan]{task.description}[/]"), BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%", "•",
        TextColumn("[magenta]{task.completed}/{task.total} plików[/]"), "•",
        TimeRemainingColumn(), transient=True
    )

    loop = asyncio.get_running_loop()
    try:
        with progress:
            task = progress.add_task("Pakowanie projektu...", total=len(paths_to_pack))
            def create_tar_archive():
                with tarfile.open(archive_path, "w:gz") as tar:
                    for path in paths_to_pack:
                        arcname = path.relative_to(project_root_path)
                        # --- POPRAWKA: Używamy prostej funkcji lambda ---
                        loop.call_soon_threadsafe(lambda p=arcname: progress.update(task, advance=1, description=f"Pakowanie: [dim]{p}[/dim]"))
                        tar.add(path, arcname=arcname)
                        sleep(0.01)
            await asyncio.to_thread(create_tar_archive)
        console.print(f"\n[green]✅ Pełna kopia zapasowa projektu utworzona w:[/] [cyan]{archive_path.resolve()}[/]")
    except Exception as e:
        logger.critical(f"Krytyczny błąd podczas tworzenia kopii projektu: {e}", exc_info=True)


async def create_data_backup():
    """
    Tworzy kopię zapasową kluczowych PLIKÓW DANYCH (baza, sesja, config).
    """
    logger.info("Rozpoczynam tworzenie nowej kopii zapasowej danych użytkownika.")

    backup_dir_path = Path(BACKUP_DIR)
    project_root_path = Path(PROJECT_ROOT)

    try:
        await asyncio.to_thread(backup_dir_path.mkdir, parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_filename = backup_dir_path / f"backup_danych_{timestamp}.zip"

        console.print(Panel(f"📦 Tworzenie Kopii Zapasowej Danych do Pliku: [cyan]{backup_filename.name}[/cyan]", expand=False, style="blue"))

        def find_files():
            found = []
            for path_str in FILES_TO_BACKUP:
                path_obj = Path(path_str)
                if not path_obj.exists():
                    logger.warning(f"Pomijam w backupie (nie istnieje): {path_obj}")
                    continue
                if path_obj.is_file():
                    found.append(path_obj)
                elif path_obj.is_dir():
                    found.extend([p for p in path_obj.rglob('*') if p.is_file()])
            return found
        files_to_archive = await asyncio.to_thread(find_files)

        if not files_to_archive:
            logger.error("Nie znaleziono żadnych plików do archiwizacji.")
            return

        progress = Progress(
            TextColumn("[cyan]{task.description}[/]"), BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%", "•",
            TextColumn("[magenta]{task.completed}/{task.total} plików[/]"), "•",
            TimeRemainingColumn(), transient=True
        )
        
        loop = asyncio.get_running_loop()
        with progress:
            task = progress.add_task("Pakowanie danych...", total=len(files_to_archive))
            def zip_files():
                with zipfile.ZipFile(backup_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file_path in files_to_archive:
                        try:
                            arcname = file_path.relative_to(project_root_path)
                        except ValueError:
                            arcname = Path(os.path.basename(os.path.dirname(file_path))) / file_path.name
                        
                        # --- POPRAWKA: Używamy prostej funkcji lambda ---
                        loop.call_soon_threadsafe(lambda p=file_path.name: progress.update(task, advance=1, description=f"Pakowanie: [dim]{p}[/dim]"))
                        zipf.write(file_path, arcname=arcname)
                        sleep(0.01)
            await asyncio.to_thread(zip_files)

        final_size_mb = (await asyncio.to_thread(backup_filename.stat)).st_size / (1024 * 1024)
        console.print(Panel(f"Kopia zapasowa danych o rozmiarze [magenta]{final_size_mb:.2f} MB[/] została utworzona.\nLokalizacja:\n[cyan]{backup_filename.resolve()}[/cyan]", title="[green]✅ Sukces![/]", border_style="green"))
    except Exception as e:
        logger.critical(f"Wystąpił krytyczny błąd podczas tworzenia kopii zapasowej danych: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd krytyczny: {e}[/bold red]")


# ##############################################################################
# ===                   SEKCJA 2: FUNKCJE PRZYWRACANIA I MENU                ===
# ##############################################################################

async def restore_data_backup():
    """
    Przywraca wybraną kopię zapasową DANYCH UŻYTKOWNIKA z archiwum .zip.
    """
    console.clear()
    logger.info("Rozpoczynam procedurę przywracania kopii zapasowej danych.")

    backup_dir_path = Path(BACKUP_DIR)
    project_root_path = Path(PROJECT_ROOT)

    try:
        await asyncio.to_thread(backup_dir_path.mkdir, parents=True, exist_ok=True)
        def find_backups():
            return sorted([f for f in backup_dir_path.glob("backup_danych_*.zip")], key=os.path.getmtime, reverse=True)
        available_backups = await asyncio.to_thread(find_backups)

        if not available_backups:
            logger.warning(f"Nie znaleziono plików kopii zapasowych w '{backup_dir_path}'.")
            return

        table = Table(title="Dostępne kopie zapasowe danych (od najnowszej)")
        table.add_column("Nr", style="cyan"); table.add_column("Nazwa Pliku", style="yellow")
        for i, p in enumerate(available_backups):
            table.add_row(str(i + 1), p.name)
        console.print(table)

        choice_str = Prompt.ask("\nPodaj numer kopii do przywrócenia (lub 'a' aby anulować)")
        if choice_str.lower() == 'a': return
        choice_idx = int(choice_str) - 1
        if not (0 <= choice_idx < len(available_backups)): raise ValueError
        backup_to_restore = available_backups[choice_idx]

        if not Confirm.ask(f"Czy na pewno chcesz przywrócić dane z [cyan]{backup_to_restore.name}[/]?", default=False): return

        def remove_old_data():
            for path_str in FILES_TO_BACKUP:
                path_to_remove = Path(path_str)
                if path_to_remove.is_file(): path_to_remove.unlink(missing_ok=True)
                elif path_to_remove.is_dir(): shutil.rmtree(path_to_remove, ignore_errors=True)
        await asyncio.to_thread(remove_old_data)

        with console.status("[yellow]Przywracanie plików...[/]"):
            def unzip_files():
                with zipfile.ZipFile(backup_to_restore, 'r') as zipf:
                    zipf.extractall(path=project_root_path)
            await asyncio.to_thread(unzip_files)
        
        console.print(Panel(f"Kopia zapasowa [cyan]{backup_to_restore.name}[/cyan] została przywrócona.", title="[green]✅ Sukces![/]", border_style="green"))
    except Exception as e:
        logger.critical(f"Wystąpił krytyczny błąd podczas przywracania kopii zapasowej: {e}", exc_info=True)


async def run_backup_manager():
    """
    Wyświetla i zarządza interaktywnym menu dla Menedżera Kopii Zapasowych.
    """
    await setup_database()
    
    menu_items = [
        ("Utwórz kopię zapasową RDZENIA APLIKACJI (kod)", create_core_app_backup),
        ("Utwórz kopię zapasową DANYCH (baza, sesja)", create_data_backup),
        ("Utwórz PEŁNĄ kopię zapasową PROJEKTU (z .gitignore)", create_full_project_backup),
        ("Przywróć DANE z kopii zapasowej", restore_data_backup),
        ("Wróć do menu głównego", "exit")
    ]
    
    while True:
        console.clear()
        selected_action = await create_interactive_menu(
            menu_items,
            "Menedżer Kopii Zapasowych",
            border_style="green"
        )
        if selected_action in ["exit", None]: break
        await selected_action()
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter...[/]")
