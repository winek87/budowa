# plik: core/visual_duplicate_finder.py
# Wersja 4.2 - Scentralizowana logika bazy danych (Refaktoryzacja Fazy 1)

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
try:
    from PIL import Image, UnidentifiedImageError
    import imagehash
except ImportError:
    Image, UnidentifiedImageError, imagehash = None, None, None

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
from .config import DOWNLOADS_DIR_BASE
from .utils import create_interactive_menu, check_dependency
from .config_editor_logic import get_key
# NOWE, SCENTRALIZOWANE IMPORTY Z MODUŁU BAZY DANYCH
from .database import (
    setup_database,
    get_images_without_perceptual_hash,
    update_perceptual_hash_batch,
    get_all_perceptual_hashes,
    get_metadata_for_display,
    delete_entries_by_ids,
    clear_all_perceptual_hashes,
    get_all_images_for_phash_recalculation,
    get_imported_images_without_perceptual_hash
)

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)

# ##############################################################################
# ===                   SEKCJA 1: FUNKCJE POMOCNICZE UI                      ===
# ##############################################################################

async def _resolve_similar_pair_interactively(pair: List[Dict], group_info: str) -> Dict:
    """
    Wyświetla ulepszony interfejs do rozwiązania pary podobnych obrazów,
    korzystając z uniwersalnego komponentu UI.
    """
    from .utils import create_side_by_side_comparison_panel # Importujemy nasz nowy komponent

    selected_to_keep_index = 0
    try:
        # Prosta logika do wstępnego wyboru większego pliku
        size_a_str = pair[0].get('size', '0 MB').split(' ')[0].replace(',', '.')
        size_b_str = pair[1].get('size', '0 MB').split(' ')[0].replace(',', '.')
        if float(size_b_str) > float(size_a_str):
            selected_to_keep_index = 1
    except (ValueError, IndexError):
        pass

    def generate_main_layout() -> Layout:
        """Wewnętrzna funkcja renderująca cały widok, włączając nagłówek i stopkę."""
        
        # Krok 1: Przygotuj dane dla uniwersalnego komponentu
        # Tworzymy słowniki z danymi w formacie, jakiego oczekuje nowa funkcja
        item_a_details = {
            "ID w Bazie": pair[0].get('id', 'Brak'),
            "URL": f"[link={pair[0].get('url', '')}]{str(pair[0].get('url', ''))[:50]}\[/...][/link]",
            "Ścieżka": pair[0].get('relative_path', 'Brak'),
            "separator_1": "", # Pusty klucz dla separatora
            "Data": pair[0].get('date', 'Brak'),
            "Rozmiar": pair[0].get('size', 'Brak'),
            "Wymiary": pair[0].get('dimensions', 'Brak'),
            "Typ Pliku": pair[0].get('type', 'Brak'),
            "separator_2": "",
            "Aparat": pair[0].get('camera', 'Brak'),
            "Ekspozycja": pair[0].get('exposure', 'Brak'),
            "GPS": pair[0].get('gps', 'Brak'),
        }
        item_b_details = {
            "ID w Bazie": pair[1].get('id', 'Brak'),
            "URL": f"[link={pair[1].get('url', '')}]{str(pair[1].get('url', ''))[:50]}\[/...][/link]",
            "Ścieżka": pair[1].get('relative_path', 'Brak'),
            "separator_1": "",
            "Data": pair[1].get('date', 'Brak'),
            "Rozmiar": pair[1].get('size', 'Brak'),
            "Wymiary": pair[1].get('dimensions', 'Brak'),
            "Typ Pliku": pair[1].get('type', 'Brak'),
            "separator_2": "",
            "Aparat": pair[1].get('camera', 'Brak'),
            "Ekspozycja": pair[1].get('exposure', 'Brak'),
            "GPS": pair[1].get('gps', 'Brak'),
        }
        
        # Krok 2: Wywołaj uniwersalny komponent, aby wygenerował panel porównawczy
        comparison_panel = create_side_by_side_comparison_panel(
            item_a_details,
            item_b_details,
            is_a_selected=(selected_to_keep_index == 0)
        )

        # Krok 3: Dodaj nagłówek i stopkę
        footer = Align.center(Text.from_markup("[bold]L/P[/](wybierz)•[bold]ENTER[/](zatwierdź)•[bold]P[/](pomiń)•[bold]Q[/](zakończ)"))
        main_layout = Layout()
        main_layout.split_column(
            Layout(Align.center(Text(group_info))),
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
                files_to_delete = [f for i, f in enumerate(pair) if i != selected_to_keep_index]
                return {"action": "resolve", "delete": files_to_delete}

# ##############################################################################
# ===                     SEKCJA 2: GŁÓWNE FUNKCJE NARZĘDZIA                 ===
# ##############################################################################

async def _calculate_and_save_hashes(scan_target: str, force_rehash: bool = False):
    """
    Skanuje obrazy, oblicza hashe percepcyjne i zapisuje je w bazie.
    """
    console.clear()
    scan_type = "Wymuszone Przeliczanie" if force_rehash else "Obliczanie Brakujących"
    target_name = "Plików Importowanych" if scan_target == 'imported' else "Plików Pobranych"
    console.print(Panel(f"‍🔬 {scan_type} Haszy dla {target_name} 🔬", expand=False, style="bold cyan"))

    await setup_database()
    try:
        if force_rehash:
            images_to_process = await get_all_images_for_phash_recalculation()
        elif scan_target == 'imported':
            images_to_process = await get_imported_images_without_perceptual_hash()
        else:
            images_to_process = await get_images_without_perceptual_hash()

        if not images_to_process:
            message = "Wszystkie obrazy w tej grupie są już przetworzone."
            console.print(f"\n[bold green]✅ {message}[/]")
            return

        updates_batch = []
        BATCH_SIZE = 100
        with Progress(TextColumn("[cyan]Przetwarzam obrazy...[/]"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", TimeRemainingColumn(), transient=True) as progress:
            task = progress.add_task("Hashing...", total=len(images_to_process))
            for record in images_to_process:
                img_path = Path(record['final_path'])
                if not await asyncio.to_thread(img_path.exists):
                    progress.update(task, advance=1)
                    continue
                try:
                    def calculate_hash():
                        with Image.open(img_path) as img:
                            return imagehash.phash(img)
                    p_hash = await asyncio.to_thread(calculate_hash)
                    updates_batch.append((str(p_hash), record['id']))
                except (UnidentifiedImageError, Exception) as e:
                    logger.warning(f"Nie udało się obliczyć hasha dla {img_path.name}: {e}")
                
                if len(updates_batch) >= BATCH_SIZE:
                    await update_perceptual_hash_batch(updates_batch)
                    updates_batch = []
                
                progress.update(task, advance=1)
        if updates_batch:
            await update_perceptual_hash_batch(updates_batch)
        console.print("\n[bold green]✅ Zakończono obliczanie hashy percepcyjnych.[/]")
    
    except Exception as e:
        logger.error(f"Wystąpił krytyczny błąd podczas obliczania hashy: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/bold red]")


async def _find_and_resolve_duplicates(quick_scan: bool = False):
    """
    Znajduje wizualnie podobne obrazy i uruchamia interaktywny proces
    rozwiązywania duplikatów.
    """
    scan_mode_str = "Szybkiego Skanu" if quick_scan else "Pełnego Skanu"
    console.clear()
    console.print(Panel(f"🔎 Krok 2: Wyszukiwanie Duplikatów ({scan_mode_str}) 🔎", expand=False, style="bold cyan"))

    info_table = Table(title="Co oznacza próg podobieństwa (dystans Hamminga)?", header_style="bold magenta")
    info_table.add_column("Próg", justify="center")
    info_table.add_column("Znaczenie")
    info_table.add_row("0", "Obrazy niemal identyczne.")
    info_table.add_row("1-4", "[green]Bardzo podobne (zalecane)[/].")
    info_table.add_row("5-7", "[yellow]Dość podobne[/].")
    info_table.add_row("8+", "[red]Luźno powiązane[/].")
    console.print(info_table)
    
    try:
        threshold = int(Prompt.ask("\nPodaj próg podobieństwa", default="4"))
    except ValueError:
        threshold = 4

    with console.status("[cyan]Wczytywanie hashy z bazy danych...[/]"):
        await setup_database()
        all_hashes_list = await get_all_perceptual_hashes()

    similar_groups, processed_ids = [], set()
    with Progress(TextColumn("[cyan]Porównuję hashe...[/]"), BarColumn(), TimeRemainingColumn(), transient=True) as progress:
        task = progress.add_task("Postęp", total=len(all_hashes_list))
        all_hashes_list.sort(key=lambda x: x.get('datetime') or datetime.min)
        
        for i, img1 in enumerate(all_hashes_list):
            if img1['id'] in processed_ids:
                progress.update(task, advance=1)
                continue
            
            current_group = [img1]
            for j in range(i + 1, len(all_hashes_list)):
                img2 = all_hashes_list[j]
                if img2['id'] in processed_ids:
                    continue
                
                if quick_scan and img1['datetime'] and img2['datetime'] and (img2['datetime'] - img1['datetime']) > timedelta(minutes=1):
                    break
                
                if (img1["hash"] - img2["hash"]) <= threshold:
                    current_group.append(img2)
                    processed_ids.add(img2['id'])

            if len(current_group) > 1:
                similar_groups.append(current_group)
            
            processed_ids.add(img1['id'])
            progress.update(task, advance=1)

    if not similar_groups:
        console.print(f"\n[bold green]✅ Nie znaleziono podobnych obrazów przy progu <= {threshold}.[/]")
        return
    
    all_files_to_delete = []
    for i, group in enumerate(similar_groups):
        pair_to_compare = group[:2]
        pair_details = []
        for item in pair_to_compare:
            display_info = await get_metadata_for_display(item['id'], item['path'])
            pair_details.append({**item, **display_info})
            
        group_info = f"Grupa {i + 1}/{len(similar_groups)}\n[dim]Dystans: {pair_details[0]['hash'] - pair_details[1]['hash']} (próg: {threshold})[/dim]"
        resolution = await _resolve_similar_pair_interactively(pair_details, group_info)
        
        if resolution.get("action") == "quit":
            break
        if resolution.get("action") == "resolve":
            all_files_to_delete.extend(resolution['delete'])

    if not all_files_to_delete:
        return
        
    if Confirm.ask(f"\nWybrano [cyan]{len(all_files_to_delete)}[/cyan] plików. [bold red]Czy na pewno chcesz je trwale usunąć?[/]", default=False):
        ids_to_delete = [f['id'] for f in all_files_to_delete]
        
        with Progress(console=console, transient=True) as progress:
            task_del = progress.add_task("[red]Usuwam pliki z dysku...", total=len(all_files_to_delete))
            for file_info in all_files_to_delete:
                try:
                    if await asyncio.to_thread(file_info['path'].exists):
                        await asyncio.to_thread(os.remove, file_info['path'])
                except OSError as e:
                    logger.warning(f"Nie udało się usunąć pliku {file_info['path']}: {e}")
                progress.update(task_del, advance=1)
        
        await delete_entries_by_ids(ids_to_delete)
        console.print(f"\n[bold green]✅ Usunięto {len(ids_to_delete)} duplikatów z bazy danych.[/bold green]")

# ##############################################################################
# ===                    SEKCJA 3: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_visual_duplicate_finder():
    """
    Wyświetla i zarządza interaktywnym menu dla narzędzia wyszukiwania
    duplikatów wizualnych.
    """
    console.clear()
    
    dependencies_ok = all([
        check_dependency("PIL", "Pillow", "Pillow"),
        check_dependency("imagehash", "imagehash", "ImageHash")
    ])
    if not dependencies_ok:
        Prompt.ask("\n[yellow]Brak kluczowych zależności. Naciśnij Enter, aby wrócić...[/yellow]")
        return
        
    menu_items = [
        ("--- GŁÓWNE OPERACJE ---", None),
        ("🚀 Uruchom Szybki Skan (pliki pobrane)", "quick_scan"),
        ("🔬 Uruchom Pełny Skan (pliki pobrane)", "full_scan"),
        ("--- KROKI POJEDYNCZE ---", None),
        ("Oblicz hashe dla plików POBRANYCH", "calculate_hashes_downloaded"),
        ("Oblicz hashe dla plików IMPORTOWANYCH z dysku", "calculate_hashes_imported"),
        ("Znajdź duplikaty (na podstawie istniejących hashy)", "resolve_duplicates"),
        ("--- ZAAWANSOWANE ---", None),
        ("[bold red]! Wymuś ponowne przeliczenie WSZYSTKICH hashy[/]", "force_rehash"),
        ("Wróć do menu głównego", "exit")
    ]
#    menu_items = [
#        ("--- GŁÓWNE OPERACJE ---", None),
#        ("🚀 Uruchom Szybki Skan (zalecane)", "quick_scan"),
#        ("🔬 Uruchom Pełny Skan (cała kolekcja)", "full_scan"),
#        ("--- KROKI POJEDYNCZE ---", None),
#        ("Krok 1: Tylko oblicz/zaktualizuj hashe percepcyjne", "calculate_hashes"),
#        ("Krok 2: Tylko znajdź duplikaty (na podstawie istniejących hashy)", "resolve_duplicates"),
#        ("--- ZAAWANSOWANE ---", None),
#        ("[bold red]! Wymuś ponowne przeliczenie WSZYSTKICH hashy[/]", "force_rehash"),
#        ("Wróć do menu głównego", "exit")
#    ]
#    menu_items = [
#        ("--- GŁÓWNE OPERACJE ---", None),
#        ("🚀 Uruchom Szybki Skan (zalecane)", "quick_scan"),
#        ("🔬 Uruchom Pełny Skan (cała kolekcja)", "full_scan"),
#        ("--- KROKI POJEDYNCZE ---", None),
#        ("Krok 1: Tylko oblicz/zaktualizuj hashe percepcyjne", "calculate_hashes"),
#        ("Krok 2: Tylko znajdź duplikaty (na podstawie istniejących hashy)", "resolve_duplicates"),
#        ("Wróć do menu głównego", "exit")
#    ]
    
    while True:
        console.clear()
        console.print(Panel("🧩 Wyszukiwanie Duplikatów Wizualnych (pHash) 🧩", expand=False, style="bold magenta"))
        
        selected_action = await create_interactive_menu(menu_items, "Wybierz operację", border_style="blue")
        
        if selected_action in ["exit", None]:
            break
        
        if selected_action == "calculate_hashes_downloaded":
            await _calculate_and_save_hashes(scan_target='downloaded', force_rehash=False)
        elif selected_action == "calculate_hashes_imported":
            await _calculate_and_save_hashes(scan_target='imported', force_rehash=False)
        elif selected_action == "resolve_duplicates":
            await _find_and_resolve_duplicates(quick_scan=False)
        elif selected_action == "full_scan":
            await _calculate_and_save_hashes(scan_target='downloaded', force_rehash=False)
            await _find_and_resolve_duplicates(quick_scan=False)
        elif selected_action == "quick_scan":
            await _calculate_and_save_hashes(scan_target='downloaded', force_rehash=False)
            await _find_and_resolve_duplicates(quick_scan=True)
        elif selected_action == "force_rehash":
            warning_text = "Ta operacja usunie wszystkie istniejące hashe percepcyjne i przeliczy je od nowa dla całej kolekcji. Może to potrwać bardzo długo."
            console.print(Panel(warning_text, title="[bold yellow]OSTRZEŻENIE[/]", border_style="red"))
            if Confirm.ask("\n[bold red]Czy na pewno chcesz kontynuować?[/]", default=False):
                with console.status("[bold red]Czyszczenie starych hashy z bazy danych...[/]"):
                    cleared_count = await clear_all_perceptual_hashes()
                console.print(f"[yellow]Usunięto {cleared_count} starych hashy. Rozpoczynam ponowne przeliczanie...[/]")
                await _calculate_and_save_hashes(scan_target='all', force_rehash=True)
        
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter, aby wrócić...[/]")

