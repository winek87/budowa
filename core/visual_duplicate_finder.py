# -*- coding: utf-8 -*-

# plik: core/visual_duplicate_finder.py
# Wersja 4.1 - Scentralizowana walidacja zależności
#
# ##############################################################################
# ===                MODUŁ WYSZUKIWANIA DUPLIKATÓW WIZUALNYCH                ===
# ##############################################################################
# ... (reszta nagłówka bez zmian)

# --- GŁÓWNE IMPORTY ---
import asyncio
import os
import json
import logging
from pathlib import Path
from typing import List, Dict
from datetime import datetime, timedelta

# --- Zależności zewnętrzne (opcjonalne) ---
# ZMIANA: Usunięto flagę LIBS_AVAILABLE.
try:
    from PIL import Image, UnidentifiedImageError
    import imagehash
except ImportError:
    Image, UnidentifiedImageError, imagehash = None, None, None

# --- Importy asynchroniczne ---
import aiosqlite

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.table import Table
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from rich.align import Align

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import DATABASE_FILE, DOWNLOADS_DIR_BASE
# ZMIANA: Dodajemy import `check_dependency`.
from .utils import create_interactive_menu, _parse_metadata_for_display, check_dependency
from .config_editor_logic import get_key
from .database import setup_database

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)

# ##############################################################################
# ===                   SEKCJA 1: FUNKCJE POMOCNICZE UI                      ===
# ##############################################################################

async def _resolve_similar_pair_interactively(pair: List[Dict], group_info: str) -> Dict:
    """
    Wyświetla ulepszony, bogaty w informacje interfejs do rozwiązania
    pary wizualnie podobnych obrazów.
    """
    selected_to_keep = 0
    try:
        size_a_str = pair[0].get('size', '0 MB').split(' ')[0].replace(',', '.')
        size_b_str = pair[1].get('size', '0 MB').split(' ')[0].replace(',', '.')
        if float(size_b_str) > float(size_a_str): selected_to_keep = 1
    except (ValueError, IndexError): pass

    def generate_layout() -> Layout:
        layout = Layout(); layout.split_row(Layout(name="left"), Layout(name="right"))
        for i, file_info in enumerate(pair):
            is_kept = (i == selected_to_keep)
            status_text = Text("⭐ ZACHOWAJ", style="bold green") if is_kept else Text("🗑️ Usuń", style="dim")
            border_style = "green" if is_kept else "default"
            try: relative_path = str(file_info['path'].relative_to(DOWNLOADS_DIR_BASE))
            except ValueError: relative_path = str(file_info['path'])

            table = Table.grid(expand=True, padding=(0, 1))
            table.add_column(style="cyan", justify="right", width=15); table.add_column()
            table.add_row("ID w Bazie:", str(file_info.get('id', 'Brak')))
            table.add_row("URL:", f"[link={file_info.get('url', '')}]{str(file_info.get('url', ''))[:50]}\[/...][/link]")
            table.add_row("Ścieżka:", relative_path)
            table.add_row("─" * 15, "─" * 30)
            table.add_row("Data:", file_info.get('date', 'Brak'))
            table.add_row("Rozmiar:", file_info.get('size', 'Brak'))
            table.add_row("Wymiary:", file_info.get('dimensions', 'Brak'))
            table.add_row("Typ Pliku:", file_info.get('type', 'Brak'))
            table.add_row("─" * 15, "─" * 30)
            table.add_row("Aparat:", file_info.get('camera', 'Brak'))
            table.add_row("Ekspozycja:", file_info.get('exposure', 'Brak'))
            table.add_row("GPS:", file_info.get('gps', 'Brak'))

            panel_content = Group(Align.center(status_text), table)
            layout["left" if i == 0 else "right"].update(Panel(panel_content, title=f"Plik {'A' if i == 0 else 'B'}", border_style=border_style))

        footer = Align.center(Text("[bold]L/P[/](wybierz)•[bold]ENTER[/](zatwierdź)•[bold]P[/](pomiń)•[bold]Q[/](zakończ)"))
        main_layout = Layout()
        main_layout.split_column(Layout(Align.center(Text(group_info))), layout, Layout(footer, size=1))
        return main_layout

    with Live(generate_layout(), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_layout(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue

            if key.upper() in ["Q", "ESC"]: return {"action": "quit"}
            if key.upper() == "P": return {"action": "skip"}
            if key in ["LEFT", "RIGHT"]: selected_to_keep = 1 - selected_to_keep
            if key == "ENTER":
                files_to_delete = [f for i, f in enumerate(pair) if i != selected_to_keep]
                return {"action": "resolve", "delete": files_to_delete}

# ##############################################################################
# ===                     SEKCJA 2: GŁÓWNE FUNKCJE NARZĘDZIA                 ===
# ##############################################################################

async def _calculate_and_save_hashes():
    """
    Skanuje kolekcję obrazów, oblicza dla nich hashe percepcyjne (pHash)
    i zapisuje je w bazie danych.
    """
    console.clear()
    console.print(Panel("‍🔬 Krok 1: Obliczanie Haszy Percepcyjnych (pHash) 🔬", expand=False, style="bold cyan"))

    await setup_database()

    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            query = """
                SELECT id, final_path FROM downloaded_media 
                WHERE (perceptual_hash IS NULL OR perceptual_hash = '') AND status = 'downloaded'
                AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')
            """
            cursor = await conn.execute(query)
            images_to_process = await cursor.fetchall()

            if not images_to_process:
                console.print("\n[bold green]✅ Wszystkie obrazy w bazie mają już obliczone hashe percepcyjne.[/]")
                return

            updates_batch = []
            BATCH_SIZE = 100

            with Progress(TextColumn("[cyan]Przetwarzam obrazy...[/]"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", TimeRemainingColumn(), transient=True) as progress:
                task = progress.add_task("Hashing...", total=len(images_to_process))
                for img_id, img_path_str in images_to_process:
                    if not img_path_str or not await asyncio.to_thread(os.path.exists, img_path_str):
                        progress.update(task, advance=1); continue
                    try:
                        def calculate_hash():
                            with Image.open(img_path_str) as img: return imagehash.phash(img)
                        p_hash = await asyncio.to_thread(calculate_hash)
                        updates_batch.append((str(p_hash), img_id))
                    except (UnidentifiedImageError, Exception):
                        pass
                    if len(updates_batch) >= BATCH_SIZE:
                        await conn.executemany("UPDATE downloaded_media SET perceptual_hash = ? WHERE id = ?", updates_batch); await conn.commit()
                        updates_batch = []
                    progress.update(task, advance=1)
            if updates_batch:
                await conn.executemany("UPDATE downloaded_media SET perceptual_hash = ? WHERE id = ?", updates_batch); await conn.commit()
            console.print("\n[bold green]✅ Zakończono obliczanie hashy percepcyjnych.[/]")
    except aiosqlite.Error as e:
        console.print(f"[bold red]Błąd bazy danych: {e}[/bold red]")

async def _find_and_resolve_duplicates(quick_scan: bool = False):
    """
    Znajduje wizualnie podobne obrazy i uruchamia interaktywny proces
    rozwiązywania duplikatów.
    """
    scan_mode_str = "Szybkiego Skanu" if quick_scan else "Pełnego Skanu"
    console.clear()
    console.print(Panel(f"🔎 Krok 2: Wyszukiwanie Duplikatów ({scan_mode_str}) 🔎", expand=False, style="bold cyan"))

    info_table = Table(title="Co oznacza próg podobieństwa (dystans Hamminga)?", header_style="bold magenta")
    info_table.add_column("Próg", justify="center"); info_table.add_column("Znaczenie")
    info_table.add_row("0", "Obrazy niemal identyczne."); info_table.add_row("1-4", "[green]Bardzo podobne (zalecane)[/].");
    info_table.add_row("5-7", "[yellow]Dość podobne[/]."); info_table.add_row("8+", "[red]Luźno powiązane[/].")
    console.print(info_table)
    
    try: threshold = int(Prompt.ask("\nPodaj próg podobieństwa", default="4"))
    except ValueError: threshold = 4

    all_hashes_list = []
    with console.status("[cyan]Wczytywanie hashy z bazy danych...[/]"):
        await setup_database()
        try:
            async with aiosqlite.connect(DATABASE_FILE) as conn:
                conn.row_factory = aiosqlite.Row
                query = "SELECT id, url, final_path, perceptual_hash, json_extract(metadata_json, '$.DateTime') as dt_str FROM downloaded_media WHERE perceptual_hash IS NOT NULL AND perceptual_hash != '' AND status = 'downloaded'"
                cursor = await conn.execute(query)
                async for rec in cursor:
                    try:
                        all_hashes_list.append({
                            "id": rec['id'], "url": rec['url'], "path": Path(rec['final_path']),
                            "hash": imagehash.hex_to_hash(rec['perceptual_hash']),
                            "datetime": datetime.fromisoformat(rec['dt_str'].replace('Z', '+00:00')) if rec['dt_str'] else None
                        })
                    except (ValueError, TypeError): pass
        except aiosqlite.Error: return

    similar_groups, processed_ids = [], set()
    with Progress(TextColumn("[cyan]Porównuję hashe...[/]"), BarColumn(), TimeRemainingColumn(), transient=True) as progress:
        task = progress.add_task("Postęp", total=len(all_hashes_list))
        all_hashes_list.sort(key=lambda x: x.get('datetime') or datetime.min)
        for i, img1 in enumerate(all_hashes_list):
            if img1['id'] in processed_ids: progress.update(task, advance=1); continue
            current_group = [img1]
            for j in range(i + 1, len(all_hashes_list)):
                img2 = all_hashes_list[j]
                if img2['id'] in processed_ids: continue
                if quick_scan and img1['datetime'] and img2['datetime'] and (img2['datetime'] - img1['datetime']) > timedelta(minutes=1): break
                if (img1["hash"] - img2["hash"]) <= threshold:
                    current_group.append(img2); processed_ids.add(img2['id'])
            if len(current_group) > 1: similar_groups.append(current_group)
            processed_ids.add(img1['id']); progress.update(task, advance=1)

    if not similar_groups:
        console.print(f"\n[bold green]✅ Nie znaleziono podobnych obrazów przy progu <= {threshold}.[/]"); return
    
    all_files_to_delete = []
    async with aiosqlite.connect(DATABASE_FILE) as conn:
        for i, group in enumerate(similar_groups):
            pair_to_compare = group[:2]; pair_details = []
            for item in pair_to_compare:
                cursor = await conn.execute("SELECT metadata_json FROM downloaded_media WHERE id = ?", (item['id'],))
                rec = await cursor.fetchone()
                metadata = json.loads(rec[0] or '{}')
                display_info = _parse_metadata_for_display(metadata, item['path'])
                pair_details.append({**item, **display_info})
            group_info = f"Grupa {i + 1}/{len(similar_groups)}\n[dim]Dystans: {pair_details[0]['hash'] - pair_details[1]['hash']} (próg: {threshold})[/dim]"
            resolution = await _resolve_similar_pair_interactively(pair_details, group_info)
            if resolution.get("action") == "quit": break
            if resolution.get("action") == "resolve": all_files_to_delete.extend(resolution['delete'])

    if not all_files_to_delete: return
    if Confirm.ask(f"\nWybrano [cyan]{len(all_files_to_delete)}[/cyan] plików. [bold red]Czy na pewno chcesz je trwale usunąć?[/]", default=False):
        ids_to_delete = [f['id'] for f in all_files_to_delete]
        with Progress(console=console, transient=True) as progress:
            task_del = progress.add_task("[red]Usuwam pliki...", total=len(all_files_to_delete))
            for file_info in all_files_to_delete:
                try:
                    if await asyncio.to_thread(file_info['path'].exists): await asyncio.to_thread(os.remove, file_info['path'])
                except OSError: pass
                progress.update(task_del, advance=1)
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            placeholders = ','.join(['?'] * len(ids_to_delete))
            await conn.execute(f"DELETE FROM downloaded_media WHERE id IN ({placeholders})", ids_to_delete)
            await conn.commit()


# ##############################################################################
# ===                    SEKCJA 3: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_visual_duplicate_finder():
    """
    Wyświetla i zarządza interaktywnym menu dla narzędzia wyszukiwania
    duplikatów wizualnych.
    """
    console.clear()
    
    # --- POCZĄTEK ZMIANY ---
    # Używamy nowej, scentralizowanej funkcji do sprawdzania zależności.
    dependencies_ok = all([
        check_dependency("PIL", "Pillow", "Pillow"),
        check_dependency("imagehash", "imagehash", "ImageHash")
    ])
    if not dependencies_ok:
        Prompt.ask("\n[yellow]Brak kluczowych zależności. Naciśnij Enter, aby wrócić...[/yellow]")
        return
    # --- KONIEC ZMIANY ---
        
    menu_items = [
        ("--- GŁÓWNE OPERACJE ---", None),
        ("🚀 Uruchom Szybki Skan (zalecane)", "quick_scan"),
        ("🔬 Uruchom Pełny Skan (cała kolekcja)", "full_scan"),
        ("--- KROKI POJEDYNCZE ---", None),
        ("Krok 1: Tylko oblicz/zaktualizuj hashe percepcyjne", "calculate_hashes"),
        ("Krok 2: Tylko znajdź duplikaty (na podstawie istniejących hashy)", "resolve_duplicates"),
        ("Wróć do menu głównego", "exit")
    ]
    
    while True:
        console.clear()
        console.print(Panel("🧩 Wyszukiwanie Duplikatów Wizualnych (pHash) 🧩", expand=False, style="bold magenta"))
        
        selected_action = await create_interactive_menu(
            menu_items, "Wybierz operację", border_style="blue"
        )
        
        if selected_action in ["exit", None]:
            break
        
        if selected_action == "calculate_hashes":
            await _calculate_and_save_hashes()
        elif selected_action == "resolve_duplicates":
            await _find_and_resolve_duplicates(quick_scan=False)
        elif selected_action == "full_scan":
            await _calculate_and_save_hashes()
            await _find_and_resolve_duplicates(quick_scan=False)
        elif selected_action == "quick_scan":
            await _calculate_and_save_hashes()
            await _find_and_resolve_duplicates(quick_scan=True)
        
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter, aby wrócić...[/]")
