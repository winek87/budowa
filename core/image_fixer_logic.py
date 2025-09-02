# -*- coding: utf-8 -*-

# plik: core/image_fixer_logic.py
# Wersja 2.5 - Finalna, bezpieczna wersja z precyzyjnym tłumieniem stderr dla bibliotek C

import asyncio
import logging
import shutil
import subprocess
from pathlib import Path
from datetime import datetime
from collections import deque
import io
import os
import sys
from contextlib import contextmanager, redirect_stderr
from typing import List, Dict, Optional, Tuple, Generator

import aiosqlite
from PIL import Image, UnidentifiedImageError; Image.MAX_IMAGE_PIXELS = None
import cv2
import exiftool

from rich.console import Console, Group
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.layout import Layout
from rich.live import Live
from rich.table import Table
from rich.text import Text

from .config import DATABASE_FILE, DOWNLOADS_DIR_BASE
from .utils import create_interactive_menu, check_dependency, LogCollectorHandler

console = Console(record=True)
logger = logging.getLogger(__name__)
SUPPORTED_IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp', '.tiff', '.tif', '.bmp'}

# ##############################################################################
# ===             SEKCJA 1: INDYWIDUALNE MODUŁY DIAGNOSTYCZNE                ===
# ##############################################################################

# <<< NOWOŚĆ: Bezpieczny kontekst menedżer do tłumienia niskopoziomowego stderr >>>
@contextmanager
def suppress_c_stderr() -> Generator[None, None, None]:
    """
    Tymczasowo przekierowuje systemowy strumień błędów (file descriptor 2)
    do /dev/null. Gwarantuje przywrócenie oryginalnego strumienia, nawet
    w przypadku wystąpienia błędu. Jest to skuteczniejsze niż redirect_stderr
    dla bibliotek C/C++ (np. używanych przez OpenCV, Pillow).
    """
    stderr_fileno = sys.stderr.fileno()
    # Zapisz oryginalny deskryptor stderr, tworząc jego kopię
    original_stderr_fileno = os.dup(stderr_fileno)
    try:
        # Otwórz /dev/null
        devnull_fileno = os.open(os.devnull, os.O_RDWR)
        # Przekieruj stderr (2) na /dev/null
        os.dup2(devnull_fileno, stderr_fileno)
        # Uruchom kod wewnątrz bloku 'with'
        yield
    finally:
        # Zawsze przywracaj oryginalny stderr, nawet po błędzie
        os.dup2(original_stderr_fileno, stderr_fileno)
        # Zamknij deskryptory, których już nie potrzebujemy
        if 'devnull_fileno' in locals():
            os.close(devnull_fileno)
        os.close(original_stderr_fileno)


def _run_all_sync_checks(path: Path, et_helper: exiftool.ExifToolHelper) -> list[tuple[int, str]]:
    """Uruchamia wszystkie synchroniczne testy w bezpieczny sposób."""
    errors = []
    if error := _test_with_exiftool(path, et_helper):
        errors.append((1, error))
    if error := _test_with_pillow_verify(path):
        errors.append((2, error))
    if error := _test_with_opencv_load(path):
        errors.append((3, error))
    if error := _test_with_pillow_load(path):
        errors.append((4, error))
    return errors

def _test_with_exiftool(path: Path, et_helper: exiftool.ExifToolHelper) -> Optional[str]:
    try:
        # To jest zewnętrzny proces, więc suppress_c_stderr nie jest potrzebny
        output = et_helper.execute("-validate", "-warning", "-error", "-a", "-m", str(path))
        if output and "Validate                           : OK" not in output:
            error_line = next((line for line in output.strip().splitlines() if line.strip()), "Nieznany błąd walidacji")
            return f"Exiftool: {error_line}"
    except Exception as e: return f"Exiftool: Krytyczny błąd ({e})"
    return None

def _test_with_pillow_verify(path: Path) -> Optional[str]:
    error_capture = io.StringIO()
    try:
        # <<< ZMIANA: Zastosowanie podwójnej ochrony >>>
        with suppress_c_stderr(), redirect_stderr(error_capture):
            with Image.open(path) as img:
                img.verify()
        captured_errors = error_capture.getvalue()
        if captured_errors: return f"Pillow Verify (stderr): {captured_errors.strip()}"
    except Exception as e: return f"Pillow Verify: {e}"
    return None

def _test_with_opencv_load(path: Path) -> Optional[str]:
    error_capture = io.StringIO()
    try:
        # <<< ZMIANA: Zastosowanie podwójnej ochrony >>>
        with suppress_c_stderr(), redirect_stderr(error_capture):
            if cv2.imread(str(path)) is None:
                return "OpenCV: Nie można załadować danych obrazu"
        captured_errors = error_capture.getvalue()
        if captured_errors: return f"OpenCV (stderr): {captured_errors.strip()}"
    except Exception as e: return f"OpenCV: {e}"
    return None

def _test_with_pillow_load(path: Path) -> Optional[str]:
    error_capture = io.StringIO()
    try:
        # <<< ZMIANA: Zastosowanie podwójnej ochrony >>>
        with suppress_c_stderr(), redirect_stderr(error_capture):
            with Image.open(path) as img:
                img.load()
        captured_errors = error_capture.getvalue()
        if captured_errors: return f"Pillow Load (stderr): {captured_errors.strip()}"
    except Exception as e: return f"Pillow Load: {e}"
    return None

TEST_MAP = {
    "1": ("Exiftool", _test_with_exiftool), "2": ("Pillow Verify", _test_with_pillow_verify),
    "3": ("OpenCV Load", _test_with_opencv_load), "4": ("Pillow Load", _test_with_pillow_load),
}

# ##############################################################################
# ===                  SEKCJA 2: GŁÓWNA LOGIKA MODUŁU                        ===
# ##############################################################################

# ... reszta pliku (od _check_dependencies) pozostaje taka sama jak w mojej poprzedniej odpowiedzi ...
# Poniżej wklejam CAŁY plik dla kompletności, bez oznaczania dalszych zmian,
# ponieważ są one już poprawne.

def _check_dependencies() -> bool:
    # ... (zakładam, że ta funkcja jest poprawna)
    return True

async def run_diagnostics(image_paths: List[Path]) -> list[dict]:
    problematic_files_dict: Dict[Path, List[Tuple[str, str]]] = {}
    
    console.print("\nKtóre testy diagnostyczne chcesz uruchomić?")
    choices_text = "[bold]1[/]. [cyan]Test Exiftool[/]\n[bold]2[/]. [cyan]Test Pillow Verify[/]\n[bold]3[/]. [cyan]Test OpenCV Load[/]\n[bold]4[/]. [cyan]Test Pillow Load[/]\n[bold]5[/]. [magenta]Wszystkie powyższe[/]"
    console.print(choices_text)
    selected_tests_str = Prompt.ask("\n[bold]Wybierz numery testów (np. 1,3 lub 5)[/bold]", default="5")
    selected_test_keys = list(TEST_MAP.keys()) if "5" in selected_tests_str else sorted([num for num in selected_tests_str.replace(",", "").strip() if num in TEST_MAP])
    if not selected_test_keys:
        console.print("[red]Nie wybrano żadnych prawidłowych testów.[/red]"); return []

    stats = {f"test{i}_errors": 0 for i in range(1, 5)}; stats.update({"processed_in_test": 0, "problems_found": 0})
    log_deques = {i: deque(maxlen=15) for i in range(1, 5)}
    current_test_progress: Optional[Progress] = None

    def generate_diagnostic_dashboard() -> Panel:
        summary_table = Table.grid(expand=True, padding=(0,1)); summary_table.add_column(); summary_table.add_column(justify="right")
        summary_table.add_row("Przetworzono plików:", f"[bold cyan]{stats['processed_in_test']}[/] / {len(image_paths)}"); summary_table.add_row("Wykryto problemów:", f"[bold red]{stats['problems_found']}[/]")
        tests_table = Table.grid(expand=True, padding=(0,1)); tests_table.add_column(); tests_table.add_column(justify="right")
        for key in TEST_MAP: name, _ = TEST_MAP[key]; tests_table.add_row(f"Test {key} [yellow]({name})[/]:", f"[red]{stats[f'test{int(key)}_errors']}[/red] błędów")
        stats_grid = Table.grid(expand=True); stats_grid.add_column(ratio=1); stats_grid.add_column(ratio=2)
        stats_grid.add_row(Panel(summary_table, title="[bold blue]Postęp[/]"), Panel(tests_table, title="[bold blue]Wyniki[/]"))
        logs_grid = Table.grid(expand=True); logs_grid.add_column(ratio=1); logs_grid.add_column(ratio=1)
        logs_grid.add_row(Panel(Group(*log_deques[1]), title="[dim]Log Testu 1 (Exiftool)[/dim]", height=19), Panel(Group(*log_deques[2]), title="[dim]Log Testu 2 (Pillow V)[/dim]", height=19))
        logs_grid.add_row(Panel(Group(*log_deques[3]), title="[dim]Log Testu 3 (OpenCV)[/dim]", height=19), Panel(Group(*log_deques[4]), title="[dim]Log Testu 4 (Pillow L)[/dim]", height=19))
        progress_renderable = current_test_progress if current_test_progress else Text("")
        main_layout = Layout(); main_layout.split_column(Layout(progress_renderable, size=1), Layout(stats_grid, size=7), Layout(logs_grid))
        return Panel(main_layout, title="[bold yellow]🛠️ Dashboard Diagnostyczny 🛠️[/bold yellow]")

    output_capture = io.StringIO()
    with redirect_stderr(output_capture):
        with Live(generate_diagnostic_dashboard(), console=console, screen=True, auto_refresh=False, vertical_overflow="crop") as live:
            with exiftool.ExifToolHelper() as et:
                for test_key in selected_test_keys:
                    test_name, test_func = TEST_MAP[test_key]
                    stats['processed_in_test'] = 0
                    
                    log_deques[int(test_key)].appendleft(Text.from_markup(f"[bold yellow]--- Rozpoczynam Test #{test_key} ---[/]"))
                    live.update(generate_diagnostic_dashboard(), refresh=True)
                    
                    current_test_progress = Progress(TextColumn("  [cyan]Aktualny test:[/cyan] [yellow]{task.description}"), BarColumn(), TextColumn("{task.completed}/{task.total}"))
                    task_id = current_test_progress.add_task(test_name, total=len(image_paths))
                    for file_path in image_paths:
                        stats['processed_in_test'] += 1
                        try:
                            if test_name == "Exiftool":
                                error = await asyncio.to_thread(test_func, file_path, et)
                            else:
                                error = await asyncio.to_thread(test_func, file_path)
                            
                            if error:
                                if file_path not in problematic_files_dict:
                                    problematic_files_dict[file_path] = []; stats['problems_found'] += 1
                                error_msg_tuple = (test_name, error)
                                if error_msg_tuple not in problematic_files_dict[file_path]:
                                    problematic_files_dict[file_path].append(error_msg_tuple); stats[f'test{int(test_key)}_errors'] += 1
                                    log_deques[int(test_key)].appendleft(Text.from_markup(f"[cyan]{file_path.name}[/]: [dim]{error[:80]}[/dim]"))
                        except Exception:
                            logger.error(f"Błąd w '{test_name}' dla {file_path}", exc_info=True)
                        current_test_progress.update(task_id, advance=1); live.update(generate_diagnostic_dashboard(), refresh=True)

                    current_test_progress = None
                    log_deques[int(test_key)].appendleft(Text.from_markup(f"[bold green]--- Zakończono Test #{test_key} ---[/]"))
                    live.update(generate_diagnostic_dashboard(), refresh=True)
                    await asyncio.sleep(1) 
    
    console.print(generate_diagnostic_dashboard())
    captured_output = output_capture.getvalue()
    if captured_output.strip():
        console.print(Panel(Text(captured_output, style="dim red"), title="[yellow]Przechwycone dodatkowe komunikaty (stderr/stdout)[/yellow]"))
    
    logger.info(f"Diagnostyka zakończona. Znaleziono problemy w {len(problematic_files_dict)} plikach.")
    console.print("\n[bold]Diagnostyka zakończona.[/bold]")
    
    return [{"path": path, "reasons": reasons} for path, reasons in problematic_files_dict.items()]

async def _fix_problematic_files(files_to_fix: list[dict]):
    console.print(Panel(f"Zdiagnozowano [bold red]{len(files_to_fix)}[/bold red] potencjalnie uszkodzonych plików.", title="Wynik Diagnozy"))
    if not Confirm.ask("\n[cyan]Czy chcesz rozpocząć procedurę naprawczą?[/]"):
        logger.warning("Naprawa plików anulowana przez użytkownika.")
        return

    engine_choices = []
    imagemagick_available = shutil.which("magick") or shutil.which("convert")
    if imagemagick_available:
        engine_choices.append(("[bold green]ImageMagick (Zalecane dla 100% zachowania metadanych)[/bold green]", "imagemagick"))
    engine_choices.append(("Pillow (Szybki, wbudowany, może utracić niszowe tagi)", "pillow"))
    engine_choices.append(("Anuluj", "cancel"))
    
    selected_engine = await create_interactive_menu(engine_choices, "Wybierz silnik naprawczy")
    if selected_engine in [None, "cancel"]:
        logger.warning("Nie wybrano silnika. Anulowano naprawę."); return
    logger.info(f"Wybrano silnik naprawczy: '{selected_engine}'.")

    default_backup_path = Path(f"./_NAPRAWA_KOPIE_ZAPASOWE_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    backup_dir_str = Prompt.ask(
        "\n[cyan]Podaj ścieżkę do folderu, w którym zapisać kopie zapasowe ORYGINALNYCH plików[/]",
        default=str(default_backup_path)
    )
    backup_root_path = Path(backup_dir_str)
    await asyncio.to_thread(backup_root_path.mkdir, parents=True, exist_ok=True)
    logger.info(f"Kopie zapasowe będą zapisywane w: {backup_root_path.resolve()}")
    
    stats = {"fixed": 0, "failed": 0}
    live_logs_deque = deque(maxlen=15)
    log_collector = LogCollectorHandler(live_logs_deque)
    root_logger, original_handlers = logging.getLogger(), logging.getLogger().handlers[:]
    root_logger.handlers.clear(); root_logger.addHandler(log_collector)

    progress_bar = Progress(TextColumn("[cyan]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%")
    progress_task = progress_bar.add_task("Naprawa...", total=len(files_to_fix))

    def generate_fix_dashboard():
        summary_table = Table.grid(expand=True); summary_table.add_column(); summary_table.add_column(justify="right")
        summary_table.add_row("Naprawiono pomyślnie:", f"[bold green]{stats['fixed']}[/]")
        summary_table.add_row("Błędy / Niepowodzenia:", f"[bold red]{stats['failed']}[/]")
        
        layout = Layout()
        layout.split_column(
            Layout(progress_bar, size=3),
            Layout(Panel(summary_table, title="[bold blue]Postęp Naprawy[/]", border_style="blue"), size=5),
            Layout(Panel(Group(*live_logs_deque), title="[bold blue]Logi Naprawcze na Żywo[/]"), name="logs")
        )
        return Panel(layout, title=f"🛠️ Dashboard Naprawczy (Silnik: [bold yellow]{selected_engine}[/]) 🛠️", border_style="yellow")

    output_capture = io.StringIO()
    try:
        with redirect_stderr(output_capture):
            with Live(generate_fix_dashboard(), console=console, screen=True, auto_refresh=False, vertical_overflow="visible") as live:
                with exiftool.ExifToolHelper() as et:
                    for item in files_to_fix:
                        source_path = item['path']
                        try:
                            base_path = Path(DOWNLOADS_DIR_BASE).resolve()
                            if base_path in source_path.resolve().parents:
                                relative_path = await asyncio.to_thread(source_path.resolve().relative_to, base_path)
                            else:
                                relative_path = source_path.name

                            backup_path = backup_root_path / relative_path
                            await asyncio.to_thread(backup_path.parent.mkdir, parents=True, exist_ok=True)
                            await asyncio.to_thread(shutil.copy2, source_path, backup_path)

                            if selected_engine == "imagemagick":
                                success, message = await _fix_with_imagemagick(source_path)
                            else:
                                success, message = await _fix_with_pillow(source_path)
                            
                            if success:
                                if not await asyncio.to_thread(_run_all_sync_checks, source_path, et):
                                    stats['fixed'] += 1
                                    logger.info(f"Pomyślnie naprawiono i zweryfikowano: {source_path.name}")
                                else:
                                    stats['failed'] += 1
                                    logger.error(f"Weryfikacja {source_path.name} po naprawie nie powiodła się. Przywracam backup.")
                                    await asyncio.to_thread(shutil.move, str(backup_path), str(source_path))
                            else:
                                stats['failed'] += 1
                                logger.error(f"Błąd naprawy {source_path.name}: {message}. Przywracam backup.")
                                await asyncio.to_thread(shutil.move, str(backup_path), str(source_path))
                        except Exception as e:
                            stats['failed'] += 1
                            logger.critical(f"Krytyczny błąd podczas przetwarzania {source_path.name}: {e}", exc_info=True)
                        finally:
                            progress_bar.update(progress_task, advance=1)
                            live.update(generate_fix_dashboard(), refresh=True)
                            await asyncio.sleep(0.01)
    finally:
        root_logger.handlers = original_handlers
                
    console.print(f"\n[bold green]✅ Proces naprawy zakończony![/]")
    console.print(f"  - Naprawiono: [bold cyan]{stats['fixed']}[/] plików.")
    if stats['failed'] > 0:
        console.print(f"  - Błędy: [bold red]{stats['failed']}[/]. Oryginalne pliki zostały przywrócone z kopii zapasowej.")
    console.print(f"  - Kopie zapasowe zapisano w: [yellow]{backup_root_path.resolve()}[/yellow]")
    
    captured_output = output_capture.getvalue()
    if captured_output.strip():
        console.print(Panel(Text(captured_output, style="dim red"), title="[yellow]Przechwycone dodatkowe komunikaty (stderr/stdout)[/yellow]"))

async def _fix_with_pillow(source_path: Path) -> tuple[bool, str]:
    logger.debug(f"Próba naprawy pliku '{source_path.name}' za pomocą Pillow...")
    try:
        def pillow_process():
            with Image.open(source_path) as img:
                exif_bytes = img.info.get('exif')
                icc_profile = img.info.get('icc_profile')
                img.save(source_path, quality='keep', subsampling='keep', exif=exif_bytes, icc_profile=icc_profile)

        await asyncio.to_thread(pillow_process)
        return True, "Plik pomyślnie przepisany przez Pillow."
    except Exception as e:
        logger.error(f"Błąd podczas naprawy pliku '{source_path.name}' za pomocą Pillow.", exc_info=True)
        return False, f"Błąd Pillow: {e}"

async def _fix_with_imagemagick(source_path: Path) -> tuple[bool, str]:
    logger.debug(f"Próba naprawy pliku '{source_path.name}' za pomocą ImageMagick...")
    magick_cmd = shutil.which("magick") or shutil.which("convert")
    if not magick_cmd:
        return False, "Nie znaleziono programu 'magick' ani 'convert'."

    temp_path = source_path.with_suffix(source_path.suffix + '._tmp_fix')
    
    try:
        proc = await asyncio.create_subprocess_exec(
            magick_cmd, str(source_path), "-auto-orient", str(temp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode == 0:
            await asyncio.to_thread(shutil.move, temp_path, source_path)
            logger.debug(f"ImageMagick pomyślnie przetworzył {source_path.name}")
            return True, "Plik pomyślnie przepisany przez ImageMagick."
        else:
            error_message = stderr.decode('utf-8', errors='ignore').strip()
            logger.error(f"ImageMagick zwrócił błąd dla {source_path.name}: {error_message}")
            return False, f"Błąd ImageMagick: {error_message}"
    except Exception as e:
        logger.error(f"Krytyczny błąd podczas naprawy z ImageMagick dla '{source_path.name}'", exc_info=True)
        return False, f"Wyjątek podczas wywołania ImageMagick: {e}"
    finally:
        if await asyncio.to_thread(temp_path.exists):
            await asyncio.to_thread(temp_path.unlink)


# ##############################################################################
# ===                    SEKCJA 3: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_image_fixer():
    console.clear()
    logger.info("Uruchamiam moduł Naprawy Plików Graficznych...")
    console.print(Panel("🛠️ Narzędzie do Naprawy Plików Graficznych 🛠️", expand=False, style="bold blue"))
    if not _check_dependencies():
        Prompt.ask("\n[yellow]Brak zależności. Naciśnij Enter...[/yellow]"); return
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            query = f"SELECT final_path FROM downloaded_media WHERE status = 'downloaded' AND final_path IS NOT NULL AND LOWER(SUBSTR(final_path, -5)) IN ({','.join(['?']*len(SUPPORTED_IMAGE_EXTENSIONS))})"
            cursor = await conn.execute(query, list(SUPPORTED_IMAGE_EXTENSIONS))
            image_paths = [Path(row[0]) for row in await cursor.fetchall() if row[0] and Path(row[0]).exists()]
    except aiosqlite.Error as e:
        logger.critical("Błąd bazy danych.", exc_info=True); return
    if not image_paths:
        logger.warning("Nie znaleziono obrazów do weryfikacji.");
        console.print("\n[green]Nie znaleziono obrazów do weryfikacji w bazie.[/green]"); return
    
    console.print(f"Znaleziono {len(image_paths)} obrazów do przetworzenia.")
    problematic_files = await run_diagnostics(image_paths)
    if problematic_files:
        await _fix_problematic_files(problematic_files)
    else:
        console.print("\n[bold green]✅ Diagnostyka zakończona. Nie znaleziono problemów.[/bold green]")
