# plik: core/image_fixer_logic.py (Wersja 4.1 - Bez `suppress_c_stderr`)
# Usunięto problematyczną funkcję `suppress_c_stderr` i zastosowano prostsze
# przekierowanie, które nie powoduje błędu `fileno`.
# -*- coding: utf-8 -*-

import asyncio
import logging
import shutil
import subprocess
import re
from pathlib import Path
from datetime import datetime
from collections import deque
import io
import os
import sys
from contextlib import contextmanager, redirect_stderr
from typing import List, Dict, Optional, Tuple, Generator
import time

# ... (reszta importów bez zmian)
try:
    from PIL import Image, UnidentifiedImageError; Image.MAX_IMAGE_PIXELS = None
    import cv2
    import exiftool
except ImportError:
    pass
from rich.console import Console, Group
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.layout import Layout
from rich.live import Live
from rich.table import Table
from rich.text import Text
from .config import DOWNLOADS_DIR_BASE
from .utils import create_interactive_menu, check_dependency, LogCollectorHandler
from .database import get_image_paths_for_analysis

console = Console(record=True)
logger = logging.getLogger(__name__)
SUPPORTED_IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp', '.tiff', '.tif', '.bmp'}

# ##############################################################################
# ===                     SEKCJA 1: MODUŁY DIAGNOSTYCZNE                       ===
# ##############################################################################

# Usunięto problematyczną funkcję suppress_c_stderr. Będziemy polegać tylko na redirect_stderr.

def _test_with_exiftool(path: Path, et_helper: exiftool.ExifToolHelper) -> Optional[str]:
    # Ta funkcja nie używała suppress_c_stderr, więc jest OK
    try:
        output = et_helper.execute("-validate", "-warning", "-error", "-a", "-m", str(path))
        if not output: return None
        if "Validate                      : OK" in output: return None
        validate_line = next((line for line in output.splitlines() if "Validate" in line), None)
        if validate_line:
            match = re.search(r'Validate\s*:\s*(\d+)\s+\d+\s+\d+', validate_line)
            if match and int(match.group(1)) == 0:
                return None
        error_line = next((line for line in output.strip().splitlines() if line.strip()), "Nieznany błąd walidacji")
        return f"Exiftool: {error_line}"
    except Exception as e:
        return f"Exiftool: Krytyczny błąd ({e})"

def _test_with_pillow_verify(path: Path) -> Optional[str]:
    error_capture = io.StringIO()
    try:
        # Używamy tylko redirect_stderr
        with redirect_stderr(error_capture):
            with Image.open(path) as img: img.verify()
        captured_errors = error_capture.getvalue()
        if captured_errors: return f"Pillow Verify (stderr): {captured_errors.strip()}"
    except Exception as e: return f"Pillow Verify: {e}"
    return None

def _test_with_opencv_load(path: Path) -> Optional[str]:
    error_capture = io.StringIO()
    try:
        # Używamy tylko redirect_stderr
        with redirect_stderr(error_capture):
            if cv2.imread(str(path)) is None:
                return "OpenCV: Nie można załadować danych obrazu"
        captured_errors = error_capture.getvalue()
        if captured_errors: return f"OpenCV (stderr): {captured_errors.strip()}"
    except Exception as e: return f"OpenCV: {e}"
    return None

def _test_with_pillow_load(path: Path) -> Optional[str]:
    error_capture = io.StringIO()
    try:
        # Używamy tylko redirect_stderr
        with redirect_stderr(error_capture):
            with Image.open(path) as img: img.load()
        captured_errors = error_capture.getvalue()
        if captured_errors: return f"Pillow Load (stderr): {captured_errors.strip()}"
    except Exception as e: return f"Pillow Load: {e}"
    return None

TEST_MAP = {
    "1": ("Exiftool", _test_with_exiftool), "2": ("Pillow Verify", _test_with_pillow_verify),
    "3": ("OpenCV Load", _test_with_opencv_load), "4": ("Pillow Load", _test_with_pillow_load),
}

# --- Funkcje weryfikacyjne również uproszczone ---
def _lenient_pillow_test(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size == 0: return False
        with redirect_stderr(io.StringIO()):
            with Image.open(path) as img: img.load()
        return True
    except Exception: return False

def _lenient_opencv_test(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size == 0: return False
        with redirect_stderr(io.StringIO()):
            return cv2.imread(str(path)) is not None
    except Exception: return False

def _verify_fix_is_successful(path: Path) -> bool:
    return _lenient_pillow_test(path) and _lenient_opencv_test(path)

# ##############################################################################
# ===                  SEKCJA 2: GŁÓWNA LOGIKA MODUŁU                        ===
# ##############################################################################
# Pozostała część kodu jest w większości taka sama, ale dla pewności wklejam całość

def _check_dependencies() -> bool:
    deps_ok = [
        check_dependency("PIL", "Pillow", "Pillow"),
        check_dependency("cv2", "opencv-python", "OpenCV"),
        check_dependency("exiftool", "pyexiftool", "PyExifTool")
    ]
    return all(deps_ok)

async def _run_fixer_process(scan_target: str):
    try:
        target_name_map = {'local_import': "plików importowanych", 'all': "wszystkich plików", 'downloaded': "plików pobranych"}
        target_name = target_name_map.get(scan_target, "plików")
        with console.status(f"[cyan]Wczytywanie {target_name} z bazy danych...[/]"):
            image_paths = await get_image_paths_for_analysis(tuple(SUPPORTED_IMAGE_EXTENSIONS), source_filter=scan_target)
    except Exception as e:
        logger.critical(f"Błąd podczas pobierania ścieżek obrazów: {e}", exc_info=True)
        console.print(f"[bold red]Błąd bazy danych. Sprawdź logi.[/bold red]")
        return
    if not image_paths:
        console.print(f"\n[green]Nie znaleziono obrazów do weryfikacji w grupie '{target_name}'.[/green]")
        return
    console.print(f"Znaleziono {len(image_paths)} obrazów do przetworzenia.")
    problematic_files = await run_diagnostics(image_paths)
    if problematic_files:
        await _fix_problematic_files(problematic_files)
    else:
        console.print("\n[bold green]✅ Diagnostyka zakończona. Nie znaleziono żadnych problemów.[/bold green]")

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
                            error = await asyncio.to_thread(test_func, file_path, et) if test_name == "Exiftool" else await asyncio.to_thread(test_func, file_path)
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
        logger.warning("Naprawa plików anulowana przez użytkownika."); return
    engine_choices = []
    if shutil.which("magick") or shutil.which("convert"):
        engine_choices.append(("[bold green]ImageMagick (Zalecane)[/bold green]", "imagemagick"))
    engine_choices.append(("Pillow (Wbudowany, szybki)", "pillow"))
    engine_choices.append(("Anuluj", "cancel"))
    selected_engine = await create_interactive_menu(engine_choices, "Wybierz silnik naprawczy")
    if selected_engine in [None, "cancel"]:
        logger.warning("Nie wybrano silnika. Anulowano naprawę."); return
    logger.info(f"Wybrano silnik naprawczy: '{selected_engine}'.")
    default_backup_path = Path(f"./_NAPRAWA_KOPIE_ZAPASOWE_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    backup_dir_str = Prompt.ask("\n[cyan]Podaj ścieżkę do folderu na kopie zapasowe[/]", default=str(default_backup_path))
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
                for item in files_to_fix:
                    source_path = item['path']
                    try:
                        base_path = Path(DOWNLOADS_DIR_BASE).resolve()
                        relative_path = await asyncio.to_thread(source_path.resolve().relative_to, base_path) if base_path in source_path.resolve().parents else source_path.name
                        backup_path = backup_root_path / relative_path
                        await asyncio.to_thread(backup_path.parent.mkdir, parents=True, exist_ok=True)
                        await asyncio.to_thread(shutil.copy2, source_path, backup_path)
                        success, message = await _fix_with_imagemagick(source_path) if selected_engine == "imagemagick" else await _fix_with_pillow(source_path)
                        if success:
                            if await asyncio.to_thread(_verify_fix_is_successful, source_path):
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
    logger.info(f"Pillow: Rozpoczynam naprawę '{source_path.name}'")
    try:
        def pillow_process():
            temp_path = source_path.with_suffix(f"{source_path.suffix}.tmp")
            with Image.open(source_path) as img:
                original_format = img.format
                exif_bytes, icc_profile = img.info.get('exif'), img.info.get('icc_profile')
                img.load()
                if img.mode in ('RGBA', 'LA', 'P'):
                    if img.mode == 'P': img = img.convert('RGBA')
                    background = Image.new('RGBA', img.size, (255, 255, 255))
                    background.alpha_composite(img)
                    img = background.convert('RGB')
                elif img.mode != 'RGB':
                    img = img.convert('RGB')
                img.save(temp_path, format=original_format, quality=95, subsampling='keep', exif=exif_bytes, icc_profile=icc_profile)
            shutil.move(str(temp_path), str(source_path))
        await asyncio.to_thread(pillow_process)
        return True, "Plik pomyślnie przepisany przez Pillow."
    except Exception as e:
        logger.error(f"Błąd podczas naprawy pliku '{source_path.name}' za pomocą Pillow.", exc_info=True)
        return False, f"Błąd Pillow: {e}"

async def _fix_with_imagemagick(source_path: Path) -> tuple[bool, str]:
    logger.info(f"ImageMagick: Rozpoczynam naprawę '{source_path.name}'")
    magick_path, convert_path = shutil.which("magick"), shutil.which("convert")
    if not magick_path and not convert_path:
        return False, "Nie znaleziono programu 'magick' ani 'convert'."
    def magick_process():
        cmd_args = [magick_path, 'convert'] if magick_path else [convert_path]
        temp_path = source_path.with_suffix(f"{source_path.suffix}.tmp")
        cmd_args.extend([str(source_path), "-auto-orient", str(temp_path)])
        result = subprocess.run(cmd_args, capture_output=True, text=True, check=False)
        if result.stderr:
            logger.info(f"ImageMagick: Komunikat z stderr dla '{source_path.name}': {result.stderr.strip()}")
        if result.returncode == 0:
            shutil.move(str(temp_path), str(source_path))
            return True, "Plik pomyślnie przepisany przez ImageMagick."
        else:
            error_message = result.stderr.strip()
            logger.error(f"ImageMagick zwrócił błąd dla {source_path.name}: {error_message}")
            if temp_path.exists(): temp_path.unlink()
            return False, f"Błąd ImageMagick: {error_message}"
    try:
        success, message = await asyncio.to_thread(magick_process)
        return success, message
    except Exception as e:
        logger.error(f"Krytyczny błąd podczas wywołania ImageMagick dla '{source_path.name}'", exc_info=True)
        return False, f"Wyjątek podczas wywołania ImageMagick: {e}"

# ##############################################################################
# ===                 SEKCJA 3: GŁÓWNA FUNKCJA URUCHOMIENIOWA                ===
# ##############################################################################

async def run_image_fixer():
    if not _check_dependencies(): return
    menu_items = [
        ("Analizuj pliki POBRANE z Google Photos", "downloaded"),
        ("Analizuj pliki IMPORTOWANE z dysku", "local_import"),
        ("Analizuj WSZYSTKIE pliki w bazie", "all"),
        ("Wróć do menu głównego", "exit")
    ]
    while True:
        console.clear()
        console.print(Panel("🛠️ Narzędzie do Naprawy Plików Graficznych 🛠️", expand=False, style="bold blue"))
        selected_action = await create_interactive_menu(menu_items, "Wybierz grupę plików do analizy")
        if selected_action in ["exit", None]: break
        if selected_action in ["downloaded", "local_import", "all"]:
            await _run_fixer_process(scan_target=selected_action)
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter, aby wrócić...[/]")
