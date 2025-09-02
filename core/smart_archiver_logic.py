# -*- coding: utf-8 -*-

# plik: core/smart_archiver_logic.py
# Wersja 4.2 - Dodano interaktywny podgląd plików
#
# ##############################################################################
# ===                        JAK TO DZIAŁA (PROSTE WYJAŚNIENIE)                ===
# ##############################################################################
#
# "Asystent Porządkowania" to narzędzie, które pomaga w utrzymaniu jakości
# kolekcji. Wykorzystuje techniki przetwarzania obrazów, aby
# automatycznie zidentyfikować potencjalnie problematyczne pliki, takie jak:
#
#  - Zdjęcia nieostre (rozmyte).
#  - Zdjęcia zbyt ciemne (niedoświetlone).
#  - Pliki o bardzo małym rozmiarze lub uszkodzone.
#
# Po analizie, prezentuje użytkownikowi listy podejrzanych plików i pozwala
# na ich interaktywne przejrzenie i podjęcie decyzji dla każdego z osobna.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import os
import logging
from pathlib import Path
from typing import List

# --- Importy asynchroniczne ---
import aiosqlite

# --- Zależności zewnętrzne (opcjonalne) ---
try:
    import cv2
    import numpy as np
except ImportError:
    cv2, np = None, None

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import DATABASE_FILE, DOWNLOADS_DIR_BASE
from .utils import create_interactive_menu, _interactive_file_selector, check_dependency, open_image_viewer
from .database import setup_database

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


# ##############################################################################
# ===                    SEKCJA 1: FUNKCJE ANALITYCZNE                       ===
# ##############################################################################

def is_blurry(image_path: Path, threshold: int = 100) -> bool:
    """
    Sprawdza, czy obraz jest prawdopodobnie nieostry.
    """
    try:
        image = cv2.imread(str(image_path), cv2.IMREAD_GRAYSCALE)
        if image is None:
            logger.warning(f"Nie można odczytać obrazu do analizy ostrości: {image_path.name}")
            return False
        variance = cv2.Laplacian(image, cv2.CV_64F).var()
        return variance < threshold
    except Exception as e:
        logger.error(f"Błąd analizy ostrości dla {image_path.name}: {e}", exc_info=True)
        return False

def is_dark(image_path: Path, threshold: int = 70) -> bool:
    """
    Sprawdza, czy obraz jest prawdopodobnie zbyt ciemny.
    """
    try:
        gray_image = cv2.imread(str(image_path), cv2.IMREAD_GRAYSCALE)
        if gray_image is None:
            logger.warning(f"Nie można odczytać obrazu do analizy jasności: {image_path.name}")
            return False
        mean_brightness = np.mean(gray_image)
        return mean_brightness < threshold
    except Exception as e:
        logger.error(f"Błąd analizy jasności dla {image_path.name}: {e}", exc_info=True)
        return False

def is_corrupted(image_path: Path) -> bool:
    """
    Sprawdza, czy plik obrazu jest prawdopodobnie uszkodzony.
    """
    try:
        if image_path.stat().st_size == 0:
            return True
    except FileNotFoundError:
        return False
    try:
        from PIL import Image, UnidentifiedImageError
        with Image.open(image_path) as img:
            img.verify()
        return False
    except (IOError, SyntaxError, UnidentifiedImageError, ValueError):
        return True
    except Exception as e:
        logger.error(f"Nieoczekiwany błąd sprawdzania uszkodzenia {image_path.name}: {e}", exc_info=True)
        return False

def is_too_small(file_path: Path, size_threshold_kb: int = 50) -> bool:
    """
    Sprawdza, czy plik jest podejrzanie mały.
    """
    try:
        file_size_bytes = file_path.stat().st_size
        return file_size_bytes < (size_threshold_kb * 1024)
    except (FileNotFoundError, Exception):
        return False

# ##############################################################################
# ===                    SEKCJA 2: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_smart_archiver():
    """
    Uruchamia pełny, interaktywny proces Asystenta Porządkowania.
    """
    console.clear()
    logger.info("Uruchamiam Asystenta Porządkowania...")
    console.print(Panel("🧹 Asystent Porządkowania Zdjęć 🧹", expand=False, style="bold blue"))

    if not check_dependency("PIL", "Pillow", "Pillow"): return
    if not check_dependency("cv2", "opencv-python", "OpenCV"): return

    await setup_database()
    image_paths = []
    with console.status("[cyan]Wczytywanie ścieżek do zdjęć z bazy danych...[/]"):
        try:
            async with aiosqlite.connect(DATABASE_FILE) as conn:
                query = "SELECT final_path FROM downloaded_media WHERE status = 'downloaded' AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')"
                cursor = await conn.execute(query)
                image_paths_tuples = await cursor.fetchall()
                image_paths = [Path(row[0]) for row in image_paths_tuples if row[0] and Path(row[0]).exists()]
        except aiosqlite.Error:
            return

    if not image_paths:
        console.print("\n[bold green]✅ Nie znaleziono żadnych zdjęć do przeanalizowania.[/bold green]")
        return

    blurry_files, dark_files, small_files, corrupted_files = [], [], [], []
    with Progress(TextColumn("[cyan]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", TimeRemainingColumn()) as progress:
        task = progress.add_task("Analizuję zdjęcia...", total=len(image_paths))
        for image_path in image_paths:
            results = await asyncio.gather(
                asyncio.to_thread(is_blurry, image_path), asyncio.to_thread(is_dark, image_path),
                asyncio.to_thread(is_too_small, image_path), asyncio.to_thread(is_corrupted, image_path)
            )
            if results[0]: blurry_files.append(image_path)
            if results[1]: dark_files.append(image_path)
            if results[2]: small_files.append(image_path)
            if results[3]: corrupted_files.append(image_path)
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

        selected_category = await create_interactive_menu(menu_items, "Asystent Porządkowania - Wyniki Analizy", border_style="blue")
        if selected_category in ["exit", None]:
            break

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
