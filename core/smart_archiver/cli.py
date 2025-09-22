# plik: core/smart_archiver/cli.py
# Wersja 1.4 - Naprawiono błąd logiczny znikających wyników analizy.
# Opis: Moduł zarządzający interfejsem użytkownika, analizą wieloprocesową
#       i interaktywnym przeglądaniem wyników dla narzędzia do porządkowania zdjęć.
# -*- coding: utf-8 -*-

import asyncio
import logging
import os
from concurrent.futures import ProcessPoolExecutor, CancelledError
from pathlib import Path
from typing import Dict, List, Any

from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

# Importy z modułów projektu `core`
from ..config import DOWNLOADS_DIR_BASE
from ..database import get_image_paths_for_analysis, get_analysis_results_from_db
from ..utils import check_dependency, get_key, open_image_viewer

# Importy z wewnętrznych modułów pakietu
from .tasks import analyze_single_image
from .ui.live_display import ArchiverAnalysisLiveDisplay
from .utils import stop_event, setup_signal_handlers, silence_loggers

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)


class SmartArchiverApp:
    """Klasa zarządzająca całym przepływem pracy Asystenta Porządkowania."""

    def __init__(self):
        """Inicjalizuje stan aplikacji."""
        self.results: Dict[str, List[Path]] = {}
        self.scan_target = 'downloader'

    async def _run_analysis(self):
        """Uruchamia nową analizę lub odświeża istniejącą z dashboardem na żywo."""
        setup_signal_handlers()
        stop_event.clear()
        
        target_name_map = {'downloader': 'pobranych', 'local_importer': 'importowanych'}
        target_name = target_name_map.get(self.scan_target, "nieznanych")

        with console.status(f"[cyan]Wczytywanie ścieżek {target_name} plików z bazy...[/]"):
            image_entries = await get_image_paths_for_analysis(('.jpg', '.jpeg', '.png'), self.scan_target)

        if not image_entries:
            console.print(f"\n[bold green]✅ Nie znaleziono obrazów do analizy w kategorii '{target_name}'.[/]")
            await asyncio.sleep(2)
            return

        # Resetujemy wyniki w pamięci przed rozpoczęciem nowej analizy
        self.results = {"blurry": [], "dark": [], "small": [], "corrupted": []}
        
        loggers_to_silence = ['core.smart_archiver.tasks']
        with silence_loggers(loggers_to_silence):
            with ArchiverAnalysisLiveDisplay(total_items=len(image_entries), console=console) as display:
                loop = asyncio.get_running_loop()
                with ProcessPoolExecutor() as executor:
                    futures = [loop.run_in_executor(executor, analyze_single_image, entry['id'], str(entry['path'])) for entry in image_entries]
                    for future in asyncio.as_completed(futures):
                        if stop_event.is_set():
                            for f in futures:
                                if not f.done(): f.cancel()
                            break
                        try:
                            result = await future
                            if result:
                                display.update_with_result(result)
                                # Zbieramy wyniki w pamięci na bieżąco, aby były dostępne od razu po analizie
                                if result.get('is_blurry'): self.results["blurry"].append(result['path'])
                                if result.get('is_dark'): self.results["dark"].append(result['path'])
                                if result.get('is_small'): self.results["small"].append(result['path'])
                                if result.get('is_corrupted'): self.results["corrupted"].append(result['path'])
                        except CancelledError:
                            logger.warning("Zadanie analizy zostało anulowane.")
                            break
                        except Exception as e:
                            logger.error(f"Błąd w podprocesie analizy: {e}", exc_info=True)

    async def _review_files(self, category: str, files: List[Path]):
        """Uruchamia interaktywną pętlę przeglądania i zarządzania plikami."""
        i = 0
        while i < len(files):
            file_path = files[i]
            console.clear()
            console.print(Panel(f"Przeglądanie: {category.capitalize()} | Plik {i+1}/{len(files)}\n[cyan]{file_path}[/]", title="Weryfikacja"))
            await asyncio.to_thread(open_image_viewer, file_path)
            
            action = Prompt.ask("\nAkcja: ([A]rchiwizuj / [U]suń / [P]omiń / [W]yjście)", choices=["a", "u", "p", "w"], default="p").lower()
            if action == 'w': break
            if action == 'p': i += 1; continue

            try:
                if action == "a":
                    target_dir = Path(DOWNLOADS_DIR_BASE) / "_ARCHIWUM_Asystenta" / category
                    target_dir.mkdir(parents=True, exist_ok=True)
                    await asyncio.to_thread(file_path.rename, target_dir / file_path.name)
                    console.print(f"[green]Przeniesiono do archiwum.[/]")
                elif action == "u":
                    await asyncio.to_thread(os.remove, file_path)
                    console.print(f"[yellow]Usunięto plik.[/]")
                
                files.pop(i)
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Błąd operacji '{action}' na {file_path.name}", exc_info=True)
                console.print(f"[red]Błąd operacji: {e}[/]"); await asyncio.sleep(1.5)
                i += 1

    async def _show_results_menu(self):
        """Wyświetla interaktywne menu kategorii na podstawie wyników w `self.results`."""
        target_name_map = {'downloader': 'pobranych', 'local_importer': 'importowanych'}
        target_name = target_name_map.get(self.scan_target, "nieznanych")
        
        selected_index = 0
        while True:
            menu_items: List[Dict[str, Any]] = []
            categories = [
                ("blurry", "Nieostre zdjęcia", self.results.get("blurry", [])),
                ("dark", "Ciemne zdjęcia", self.results.get("dark", [])),
                ("small", "Małe pliki (<50KB)", self.results.get("small", [])),
                ("corrupted", "Uszkodzone pliki", self.results.get("corrupted", [])),
            ]
            for key, text, files in categories:
                if files: menu_items.append({"icon": "🖼️", "text": f"{text} ({len(files)})", "action": key, "files": files})
            
            if not menu_items:
                console.print(Panel("[bold green]✅ Gratulacje! Nie znaleziono żadnych problematycznych zdjęć w tej kategorii.[/]", expand=False))
                break
            
            menu_items.append({"icon": "🚪", "text": "Zakończ przeglądanie", "action": "back"})
            
            def build_results_layout() -> Panel:
                table = Table.grid(padding=(0, 2))
                for i, item in enumerate(menu_items):
                    style = "bold white on blue" if i == selected_index else ""
                    prefix = "» " if i == selected_index else "  "
                    table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
                return Panel(table, title=f"Asystent Porządkowania - Wyniki dla {target_name} plików")

            console.clear()
            with Live(build_results_layout(), transient=True, auto_refresh=False) as live:
                while True:
                    live.update(build_results_layout(), refresh=True)
                    key = await asyncio.to_thread(get_key)
                    if not key: continue
                    if key == "UP": selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                    elif key == "DOWN": selected_index = (selected_index + 1) % len(menu_items)
                    elif key == "ENTER": break
                    elif key.upper() == 'Q': selected_index = -1; break
            
            if selected_index == -1 or menu_items[selected_index]['action'] == 'back': break
            
            selected = menu_items[selected_index]
            await self._review_files(selected['action'], selected['files'])

    async def run(self):
        """Główna, przebudowana pętla menu Asystenta Porządkowania."""
        menu_items = [
            {"icon": "📊", "text": "Pokaż ostatnie wyniki dla POBRANYCH", "action": "show_downloaded", "description": "Wyświetla ostatnio zapisane wyniki analizy dla plików pobranych z Google Photos."},
            {"icon": "🔄", "text": "Odśwież analizę dla POBRANYCH", "action": "run_downloaded", "description": "Uruchamia od nowa pełną analizę wszystkich pobranych plików. Może to zająć dużo czasu."},
            {"icon": "📊", "text": "Pokaż ostatnie wyniki dla IMPORTOWANYCH", "action": "show_imported", "description": "Wyświetla ostatnio zapisane wyniki analizy dla plików importowanych lokalnie."},
            {"icon": "🔄", "text": "Odśwież analizę dla IMPORTOWANYCH", "action": "run_imported", "description": "Uruchamia od nowa pełną analizę wszystkich importowanych plików."},
            {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "description": "Zamyka Asystenta Porządkowania."},
        ]
        selected_index = 0

        while True:
            def build_main_menu() -> Layout:
                table = Table.grid(expand=True, padding=(0, 2))
                for i, item in enumerate(menu_items):
                    style = "bold white on blue" if i == selected_index else ""
                    prefix = "» " if i == selected_index else "  "
                    table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
                
                menu_panel = Panel(table, title="[bold blue]🧹 Asystent Porządkowania 🧹[/]", border_style="blue")
                info_panel = Panel(Align.center(f"[italic]{menu_items[selected_index]['description']}[/]", vertical="middle"), title="[bold]Opis Opcji[/]", border_style="dim")
                layout = Layout(); body = Layout(name="body"); body.split_row(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1))
                footer = Text.from_markup("Nawigacja: ▲/▼ | Wybór: Enter | Powrót: Q", justify="center")
                layout.split_column(body, Layout(footer, size=1))
                return layout

            console.clear()
            with Live(build_main_menu(), transient=True, auto_refresh=False) as live:
                while True:
                    live.update(build_main_menu(), refresh=True)
                    key = await asyncio.to_thread(get_key)
                    if not key: continue
                    if key == "UP": selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                    elif key == "DOWN": selected_index = (selected_index + 1) % len(menu_items)
                    elif key == "ENTER": break
                    elif key.upper() == 'Q': selected_index = -1; break

            if selected_index == -1 or menu_items[selected_index]['action'] == 'back': break
            
            action = menu_items[selected_index]['action']
            
            if 'downloaded' in action: self.scan_target = 'downloader'
            elif 'imported' in action: self.scan_target = 'local_importer'

            if action.startswith("run_"):
                await self._run_analysis()
                # Po zakończeniu analizy, `self.results` jest już w pamięci, więc od razu je pokazujemy
                await self._show_results_menu()
            elif action.startswith("show_"):
                # Pobieramy najświeższe wyniki z bazy
                with console.status("[cyan]Pobieranie wyników analizy z bazy danych...[/]"):
                    self.results = await get_analysis_results_from_db(self.scan_target)
                await self._show_results_menu()
            
            Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić do menu Asystenta...[/]")

def _check_deps():
    """Sprawdza kluczowe zależności przed uruchomieniem."""
    if not check_dependency("PIL", "Pillow", "Pillow"): return False
    if not check_dependency("cv2", "opencv-python", "OpenCV"): return False
    return True

async def run_smart_archiver():
    """Główny punkt wejścia do modułu."""
    if not _check_deps(): return
    
    app = SmartArchiverApp()
    await app.run()
