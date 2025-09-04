# plik: core/smart_archiver_logic.py (Wersja z dedykowaną funkcją dla plików lokalnych)
# -*- coding: utf-8 -*-

import asyncio
import logging
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

try:
    from PIL import Image, UnidentifiedImageError
    import cv2
    import numpy as np
except ImportError:
    Image, UnidentifiedImageError, cv2, np = None, None, None, None

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

from .database import setup_database, get_image_paths_for_analysis, get_imported_image_paths_for_analysis
from .config import DOWNLOADS_DIR_BASE
from .utils import check_dependency, create_interactive_menu, open_image_viewer

console = Console(record=True)
logger = logging.getLogger(__name__)

# ##############################################################################
# ===                   SEKCJA 1: FUNKCJE ANALIZUJĄCE OBRAZ                  ===
# ##############################################################################

def is_blurry(image_path: Path, threshold: int = 100) -> bool:
    try:
        image = cv2.imread(str(image_path))
        if image is None: return False
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        laplacian_var = cv2.Laplacian(gray, cv2.CV_64F).var()
        return laplacian_var < threshold
    except Exception:
        return False

def is_dark(image_path: Path, threshold: int = 60) -> bool:
    try:
        image = cv2.imread(str(image_path))
        if image is None: return False
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        return np.mean(gray) < threshold
    except Exception:
        return False

def is_too_small(image_path: Path, size_kb: int = 50) -> bool:
    try:
        return image_path.stat().st_size < (size_kb * 1024)
    except (OSError, FileNotFoundError):
        return False

def is_corrupted(image_path: Path) -> bool:
    try:
        with Image.open(image_path) as img:
            img.verify()
        return False
    except (UnidentifiedImageError, IOError, SyntaxError):
        return True

# ##############################################################################
# ===        SEKCJA 2: FUNKCJA ROBOCZA I GŁÓWNY SILNIK ASYSTENTA             ===
# ##############################################################################

def analyze_single_image(image_path: Path) -> dict:
    """Funkcja robocza, która wykonuje wszystkie analizy dla jednego obrazu."""
    if not all([Image, cv2, np]):
        return {'path': image_path}
    return {
        'path': image_path,
        'is_blurry': is_blurry(image_path),
        'is_dark': is_dark(image_path),
        'is_small': is_too_small(image_path),
        'is_corrupted': is_corrupted(image_path)
    }

async def _run_analysis_process(scan_target: str):
    """
    Główna logika robocza Asystenta Porządkowania. Skanuje, analizuje
    i pozwala na interaktywne zarządzanie problematycznymi plikami.
    """
    await setup_database()
    
    image_extensions = ('.jpg', '.jpeg', '.png')
    target_name = "plików importowanych" if scan_target == 'imported' else "plików pobranych"

    with console.status(f"[cyan]Wczytywanie ścieżek {target_name} z bazy danych...[/]"):
        if scan_target == 'imported':
            image_paths = await get_imported_image_paths_for_analysis(image_extensions)
        else:
            image_paths = await get_image_paths_for_analysis(image_extensions)

    if not image_paths:
        console.print(f"\n[bold green]✅ Nie znaleziono żadnych {target_name} do przeanalizowania.[/bold green]")
        return

    blurry_files, dark_files, small_files, corrupted_files = [], [], [], []
    
    loop = asyncio.get_running_loop()
    with Progress(TextColumn("[cyan]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", TimeRemainingColumn(), transient=True) as progress:
        task = progress.add_task(f"Analizuję {target_name} (na wielu rdzeniach)...", total=len(image_paths))
        with ProcessPoolExecutor() as executor:
            futures = [loop.run_in_executor(executor, analyze_single_image, path) for path in image_paths]
            for future in asyncio.as_completed(futures):
                try:
                    result = await future
                    if result.get('is_blurry'): blurry_files.append(result['path'])
                    if result.get('is_dark'): dark_files.append(result['path'])
                    if result.get('is_small'): small_files.append(result['path'])
                    if result.get('is_corrupted'): corrupted_files.append(result['path'])
                except Exception as e:
                    logger.error(f"Błąd podczas analizy obrazu w podprocesie: {e}")
                finally:
                    progress.update(task, advance=1)

    while True:
        console.clear()
        files_map = {
            "blurry": list(blurry_files), "dark": list(dark_files),
            "small": list(small_files), "corrupted": list(corrupted_files)
        }
        
        menu_items = []
        if files_map["blurry"]: menu_items.append((f"Przeglądaj nieostre zdjęcia ({len(files_map['blurry'])})", "blurry"))
        if files_map["dark"]: menu_items.append((f"Przeglądaj ciemne zdjęcia ({len(files_map['dark'])})", "dark"))
        if files_map["small"]: menu_items.append((f"Przeglądaj małe pliki (<50KB) ({len(files_map['small'])})", "small"))
        if files_map["corrupted"]: menu_items.append((f"Przeglądaj uszkodzone pliki ({len(files_map['corrupted'])})", "corrupted"))

        if not menu_items:
            console.print(Panel("\n[bold green]✅ Gratulacje! Nie znaleziono żadnych problematycznych zdjęć.[/bold green]", expand=False))
            break
        menu_items.append(("Zakończ i wróć do menu głównego", "exit"))

        selected_category = await create_interactive_menu(menu_items, f"Asystent Porządkowania - Wyniki ({target_name})", border_style="blue")
        if selected_category in ["exit", None]: break

        files_to_review = files_map[selected_category]
        i = 0
        while i < len(files_to_review):
            file_path = files_to_review[i]
            console.clear()
            console.print(Panel(f"Przeglądanie: {selected_category.capitalize()} | Plik {i+1}/{len(files_to_review)}\n[cyan]{file_path.name}[/]", title="Weryfikacja"))
            await asyncio.to_thread(open_image_viewer, file_path)
            action = Prompt.ask("\nAkcja dla tego pliku: ([A]rchiwizuj / [U]suń / [P]omiń / [W]yjście z przeglądania)", choices=["a", "u", "p", "w"], default="p").lower()

            if action == 'w': break
            elif action == 'p': i += 1; continue
            
            target_dir = None
            if action == "a":
                target_dir = Path(DOWNLOADS_DIR_BASE) / "_ARCHIWUM_Asystenta" / selected_category
                await asyncio.to_thread(target_dir.mkdir, parents=True, exist_ok=True)
            try:
                if action == "a": await asyncio.to_thread(file_path.rename, target_dir / file_path.name)
                elif action == "u": await asyncio.to_thread(os.remove, file_path)
                
                if selected_category == "blurry": blurry_files.remove(file_path)
                if selected_category == "dark": dark_files.remove(file_path)
                if selected_category == "small": small_files.remove(file_path)
                if selected_category == "corrupted": corrupted_files.remove(file_path)
                
                files_to_review.pop(i)
            except Exception as e:
                logger.error(f"Błąd operacji '{action}' na pliku {file_path.name}", exc_info=True)
                i += 1

# ##############################################################################
# ===                   SEKCJA 3: GŁÓWNA FUNKCJA URUCHOMIENIOWA                ===
# ##############################################################################

async def run_smart_archiver():
    """
    Wyświetla i zarządza interaktywnym menu dla Asystenta Porządkowania.
    """
    if not check_dependency("PIL", "Pillow", "Pillow"): return
    if not check_dependency("cv2", "opencv-python", "OpenCV"): return
        
    menu_items = [
        ("Analizuj pliki POBRANE z Google Photos", "downloaded"),
        ("Analizuj pliki IMPORTOWANE z dysku", "imported"),
        ("Wróć do menu głównego", "exit")
    ]
    
    while True:
        console.clear()
        console.print(Panel("🧹 Asystent Porządkowania Zdjęć 🧹", expand=False, style="bold blue"))
        
        selected_action = await create_interactive_menu(menu_items, "Wybierz grupę plików do analizy")
        
        if selected_action in ["exit", None]:
            break
        
        if selected_action in ["downloaded", "imported"]:
            await _run_analysis_process(scan_target=selected_action)
        
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter, aby wrócić...[/]")
