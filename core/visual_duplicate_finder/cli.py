# plik: core/visual_duplicate_finder/cli.py
# Wersja 1.2 - Wprowadzono pełne, interaktywne menu i poprawiono błąd TypeError.
# Opis: Moduł zarządzający menu, obliczaniem hashy i interaktywnym
#       rozwiązywaniem duplikatów wizualnych.
# -*- coding: utf-8 -*-

import asyncio
import logging
import os
from pathlib import Path
from typing import Dict, List, Any

from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.table import Table
from rich.text import Text

from ..config import DOWNLOADS_DIR_BASE
from ..database import (
    setup_database, delete_entries_by_ids, get_all_images_for_phash_recalculation,
    get_images_without_perceptual_hash, get_imported_images_without_perceptual_hash,
    get_metadata_for_display, clear_all_perceptual_hashes
)
from ..utils import check_dependency, create_side_by_side_comparison_panel, get_key
from .tasks import calculate_and_save_hashes, find_similar_images
from .ui.live_display import HashCalculationLiveDisplay

console = Console()
logger = logging.getLogger(__name__)


class DuplicateFinderMenuApp:
    """Klasa zarządzająca całym przepływem pracy Wyszukiwarki Duplikatów."""

    async def _resolve_similar_pair(self, pair: List[Dict], group_info: str) -> Dict:
        """Wyświetla interaktywny, dwukolumnowy interfejs do rozwiązania pary podobnych obrazów."""
        selected_index = 0
        try:
            if pair[1]['size_bytes'] > pair[0]['size_bytes']: selected_index = 1
        except (KeyError, TypeError): pass

        def generate_layout() -> Layout:
            details1 = {k: v for k, v in pair[0].items() if k not in ['id', 'path', 'hash', 'datetime', 'size_bytes']}
            details2 = {k: v for k, v in pair[1].items() if k not in ['id', 'path', 'hash', 'datetime', 'size_bytes']}
            comparison_panel = create_side_by_side_comparison_panel(details1, details2, is_a_selected=(selected_index == 0))
            footer = Align.center(Text.from_markup("[bold]◀/▶[/](wybierz)•[bold]ENTER[/](zatwierdź)•[bold]P[/](pomiń)•[bold]Q[/](zakończ)"))
            layout = Layout(); layout.split_column(Layout(Align.center(Text(group_info))), comparison_panel, Layout(footer, size=1))
            return layout

        with Live(generate_layout(), screen=True, auto_refresh=False, transient=True) as live:
            while True:
                live.update(generate_layout(), refresh=True)
                key = await asyncio.to_thread(get_key)
                if not key: continue
                if key.upper() == "Q": return {"action": "quit"}
                if key.upper() == "P": return {"action": "skip"}
                if key in ["LEFT", "RIGHT"]: selected_index = 1 - selected_index
                if key == "ENTER":
                    return {"action": "resolve", "delete": [p for i, p in enumerate(pair) if i != selected_index]}

    async def _run_hash_calculation(self, scan_target: str, force: bool = False):
        """Uruchamia proces obliczania hashy z dedykowanym dashboardem na żywo."""
        title = "Wymuszone Przeliczanie Haszy" if force else f"Obliczanie Brakujących Haszy ({scan_target})"
        
        get_func_map = {
            'all': get_all_images_for_phash_recalculation,
            'imported': get_imported_images_without_perceptual_hash,
            'downloaded': get_images_without_perceptual_hash
        }
        images = await get_func_map.get(scan_target, get_images_without_perceptual_hash)() if not force else await get_all_images_for_phash_recalculation()
        
        if not images:
            console.print(f"\n[green]✅ Wszystkie obrazy w tej grupie mają już obliczone hashe.[/]"); return
        
        with HashCalculationLiveDisplay(total_items=len(images), title=title, console=console) as display:
            await calculate_and_save_hashes(images, display)

    async def _run_duplicate_finder(self, quick_scan: bool):
        """Uruchamia proces znajdowania i rozwiązywania duplikatów."""
        scan_mode = "Szybki Skan" if quick_scan else "Pełny Skan"
        console.clear()
        console.print(Panel(f"🔎 Wyszukiwanie Duplikatów ({scan_mode}) 🔎", style="cyan"))
        try:
            threshold = int(Prompt.ask("Podaj próg podobieństwa (0-4 zalecane)", default="4"))
        except ValueError:
            threshold = 4

        with console.status("[cyan]Porównywanie hashy w bazie danych... (może potrwać)[/]"):
            similar_groups_data = await find_similar_images(threshold, quick_scan)
        
        if not similar_groups_data:
            console.print(f"\n[green]✅ Nie znaleziono podobnych obrazów przy progu <= {threshold}.[/]"); return

        files_to_delete = []
        for i, data in enumerate(similar_groups_data):
            group = data['group']
            distance = data['distance']
            
            pair = group[:2]
            pair_details = [await get_metadata_for_display(item['id'], Path(item['path'])) for item in pair]
            
            group_info = f"Grupa {i + 1}/{len(similar_groups_data)} | Dystans: {distance}"
            
            resolution = await self._resolve_similar_pair(pair_details, group_info)
            if resolution.get("action") == "quit": break
            if resolution.get("action") == "resolve": files_to_delete.extend(resolution['delete'])
        
        if files_to_delete and Confirm.ask(f"\nWybrano [cyan]{len(files_to_delete)}[/] plików. [bold red]Czy na pewno chcesz je trwale usunąć?[/]", default=False):
            ids_to_delete = [f['id'] for f in files_to_delete]
            with Progress(transient=True) as progress:
                for f in progress.track(files_to_delete, description="[red]Usuwanie plików...[/]"):
                    try:
                        await asyncio.to_thread(os.remove, f['path'])
                    except OSError as e:
                        logger.warning(f"Nie udało się usunąć {f['path']}: {e}")
            await delete_entries_by_ids(ids_to_delete)
            console.print(f"\n[green]✅ Usunięto {len(ids_to_delete)} duplikatów.[/]")
    
    async def run(self):
        """Główna pętla menu Wyszukiwarki Duplikatów."""
        menu_items = [
            {"icon": "🚀", "text": "Uruchom Szybki Skan (pliki pobrane)", "action": "quick_scan", "description": "Oblicza brakujące hashe i szuka duplikatów tylko wśród plików pobranych w bliskim sąsiedztwie czasowym. Najszybsza opcja."},
            {"icon": "🔬", "text": "Uruchom Pełny Skan (pliki pobrane)", "action": "full_scan", "description": "Oblicza brakujące hashe i dokładnie porównuje każdy pobrany plik z każdym innym. Wolniejsze, ale dokładniejsze."},
            {"icon": "---", "text": "--- KROKI POJEDYNCZE ---", "action": None, "description": ""},
            {"icon": "🔢", "text": "Oblicz hashe dla POBRANYCH", "action": "calculate_hashes_downloaded", "description": "Tylko oblicza i zapisuje brakujące hashe dla plików pobranych. Nie szuka duplikatów."},
            {"icon": "🔢", "text": "Oblicz hashe dla IMPORTOWANYCH", "action": "calculate_hashes_imported", "description": "Tylko oblicza i zapisuje brakujące hashe dla plików importowanych. Nie szuka duplikatów."},
            {"icon": "🧩", "text": "Znajdź duplikaty (na istniejących haszach)", "action": "resolve_duplicates", "description": "Uruchamia tylko interfejs do rozwiązywania duplikatów na podstawie już obliczonych hashy."},
            {"icon": "---", "text": "--- ZAAWANSOWANE ---", "action": None, "description": ""},
            {"icon": "💥", "text": "Wymuś ponowne przeliczenie WSZYSTKICH hashy", "action": "force_rehash", "description": "USUWA wszystkie istniejące hashe i oblicza je od nowa dla całej bazy. Bardzo czasochłonne."},
            {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "description": "Zamyka to narzędzie."},
        ]
        selected_index = 0
        
        while True:
            console.clear()
            def build_layout() -> Layout:
                table = Table.grid(expand=True, padding=(0, 2))
                for i, item in enumerate(menu_items):
                    style = "bold white on magenta" if i == selected_index else ""
                    prefix = "» " if i == selected_index else "  "
                    label_text = Text(f"{prefix}{item['icon']} {item['text']}")
                    if not item['action']: label_text.stylize("dim")
                    table.add_row(label_text, style=style)
                
                menu_panel = Panel(table, title="[bold magenta]🧩 Wyszukiwarka Duplikatów Wizualnych[/]", border_style="magenta")
                info_panel = Panel(Align.center(f"[italic]{menu_items[selected_index]['description']}[/]", vertical="middle"), title="Opis Opcji", border_style="dim")
                layout = Layout(); body = Layout(); body.split_row(menu_panel, info_panel)
                footer = Text.from_markup("Nawigacja: ▲/▼ | Wybór: Enter | Powrót: Q", justify="center")
                layout.split_column(body, Layout(footer, size=1)); return layout
            
            with Live(build_layout(), transient=True, auto_refresh=False) as live:
                while True:
                    live.update(build_layout(), refresh=True); key = await asyncio.to_thread(get_key)
                    if not key: continue
                    if key == "UP":
                        original_index = selected_index
                        while True:
                            selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                            if menu_items[selected_index].get("action"): break
                            if selected_index == original_index: break
                    elif key == "DOWN":
                        original_index = selected_index
                        while True:
                            selected_index = (selected_index + 1) % len(menu_items)
                            if menu_items[selected_index].get("action"): break
                            if selected_index == original_index: break
                    elif key == "ENTER": break
                    elif key.upper() == 'Q': selected_index = -1; break

            if selected_index == -1 or menu_items[selected_index]['action'] == 'back': break
            
            selected_action = menu_items[selected_index]['action']
            console.clear()

            if selected_action == "calculate_hashes_downloaded": await self._run_hash_calculation('downloaded')
            elif selected_action == "calculate_hashes_imported": await self._run_hash_calculation('imported')
            elif selected_action == "resolve_duplicates": await self._run_duplicate_finder(False)
            elif selected_action == "full_scan":
                await self._run_hash_calculation('downloaded'); await self._run_duplicate_finder(False)
            elif selected_action == "quick_scan":
                await self._run_hash_calculation('downloaded'); await self._run_duplicate_finder(True)
            elif selected_action == "force_rehash":
                if Confirm.ask("[bold red]Czy na pewno usunąć i przeliczyć WSZYSTKIE hashe?[/]", default=False):
                    with console.status("[red]Czyszczenie starych hashy...[/]"): await clear_all_perceptual_hashes()
                    await self._run_hash_calculation('all', force=True)

            Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić do menu...[/]")

async def run_visual_duplicate_finder():
    """Główny punkt wejścia do modułu."""
    if not all([check_dependency("PIL", "Pillow", "Pillow"), check_dependency("imagehash", "imagehash", "ImageHash")]):
        return
    await setup_database()
    app = DuplicateFinderMenuApp()
    await app.run()
