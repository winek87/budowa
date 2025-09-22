# plik: core/image_fixer/cli.py
# Wersja 1.4 - Dodano silnik naprawczy ExifTool do menu wyboru.
# Opis: Moduł zarządzający menu, diagnozą i naprawą plików graficznych.
# -*- coding: utf-8 -*-

import asyncio
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table
from rich.text import Text

# Importy z modułów projektu `core`
from ..config import DOWNLOADS_DIR_BASE
from ..database import get_image_paths_for_analysis
from ..utils import check_dependency, get_key, open_image_viewer

# Importy z wewnętrznych modułów pakietu
from .tasks import run_diagnostics, fix_with_pillow, fix_with_imagemagick, fix_with_exiftool, TEST_MAP
from .ui import ImageFixerLiveDisplay

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)

SUPPORTED_IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp', '.tiff', '.tif', '.bmp'}


class ImageFixerMenuApp:
    """Klasa zarządzająca całym przepływem pracy Naprawiacza Obrazów."""

    def _check_dependencies(self) -> bool:
        """Sprawdza, czy wszystkie zależności są spełnione."""
        deps_ok = [
            check_dependency("PIL", "Pillow", "Pillow"),
            check_dependency("cv2", "opencv-python", "OpenCV"),
            check_dependency("exiftool", "pyexiftool", "PyExifTool")
        ]
        return all(deps_ok)
    
    async def _select_diagnostic_tests(self) -> List[str] | None:
        """Wyświetla interaktywne, dwukolumnowe menu wyboru testów diagnostycznych."""
        selected_index = 0
        menu_items = [
            {"icon": "🔧", "text": "Exiftool", "action": "Exiftool", "description": "Sprawdza integralność metadanych EXIF i strukturę pliku. Najszybszy test."},
            {"icon": "🖼️", "text": "Pillow Verify", "action": "Pillow Verify", "description": "Wykonuje szybką weryfikację nagłówków i podstawowej struktury pliku za pomocą biblioteki Pillow."},
            {"icon": "👁️", "text": "OpenCV Load", "action": "OpenCV Load", "description": "Próbuje w pełni załadować dane obrazu do pamięci za pomocą OpenCV. Dokładny, ale wolniejszy."},
            {"icon": "📦", "text": "Pillow Load", "action": "Pillow Load", "description": "Próbuje w pełni zdekodować i załadować dane obrazu za pomocą Pillow. Dobry test na uszkodzenia kompresji."},
            {"icon": "🚀", "text": "Uruchom wszystkie testy", "action": "all", "description": "Przeprowadza pełną, kompleksową diagnostykę przy użyciu wszystkich dostępnych metod."},
            {"icon": "🚪", "text": "Anuluj", "action": None, "description": "Wróć do poprzedniego menu bez uruchamiania diagnostyki."},
        ]
        
        def build_layout() -> Layout:
            table = Table.grid(expand=True, padding=(0, 2))
            for i, item in enumerate(menu_items):
                style = "bold white on blue" if i == selected_index else ""
                prefix = "» " if i == selected_index else "  "
                table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
            
            menu_panel = Panel(table, title="[bold]Wybierz testy diagnostyczne do uruchomienia[/]", border_style="blue")
            info_panel = Panel(Align.center(f"[italic]{menu_items[selected_index]['description']}[/]", vertical="middle"), title="[bold]Opis Opcji[/]", border_style="dim")
            layout = Layout(); body = Layout(); body.split_row(menu_panel, info_panel)
            footer = Text.from_markup("Nawigacja: ▲/▼ | Wybór: Enter | Powrót: Q", justify="center")
            layout.split_column(body, Layout(footer, size=1))
            return layout
        
        with Live(build_layout(), screen=True, transient=True, auto_refresh=False) as live:
            while True:
                live.update(build_layout(), refresh=True)
                key = await asyncio.to_thread(get_key)
                if not key: continue
                if key == "UP": selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                elif key == "DOWN": selected_index = (selected_index + 1) % len(menu_items)
                elif key == "ENTER": break
                elif key.upper() == 'Q': return None
        
        choice = menu_items[selected_index]['action']
        if choice == "all": return list(TEST_MAP.keys())
        return [choice] if choice else None

    async def _select_fix_engine(self) -> str | None:
        """Wyświetla interaktywne menu wyboru silnika naprawczego."""
        selected_index = 0
        engine_choices = [
            {"icon": "🔧", "text": "ExifTool (Naprawa Metadanych)", "action": "exiftool", "description": "Najlepsze dla błędów walidacji (np. 'Bad InteropOffset'). Przepisuje metadane bez ruszania pikseli."}
        ]
        if shutil.which("magick") or shutil.which("convert"):
            engine_choices.append({"icon": "🪄", "text": "ImageMagick (Naprawa Pikseli)", "action": "imagemagick", "description": "Potężne, zewnętrzne narzędzie do naprawy uszkodzonych danych obrazu (pikseli). Może być wolniejsze."})
        engine_choices.extend([
            {"icon": "🐍", "text": "Pillow (Naprawa Pikseli)", "action": "pillow", "description": "Szybki silnik wbudowany w Pythonie. Dobry dla prostszych uszkodzeń danych obrazu."},
            {"icon": "🚪", "text": "Anuluj", "action": None, "description": "Wróć bez naprawiania plików."}
        ])

        def build_layout() -> Layout:
            table = Table.grid(expand=True, padding=(0,2))
            for i, item in enumerate(engine_choices):
                style = "bold white on blue" if i == selected_index else ""
                prefix = "» " if i == selected_index else "  "
                table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
            menu_panel = Panel(table, title="[bold]Wybierz silnik naprawczy[/]")
            info_panel = Panel(Align.center(f"[italic]{engine_choices[selected_index]['description']}[/]", vertical="middle"), title="Opis")
            layout = Layout(); layout.split_column(menu_panel, info_panel)
            return layout
        
        with Live(build_layout(), screen=True, transient=True, auto_refresh=False) as live:
            while True:
                live.update(build_layout(), refresh=True)
                key = await asyncio.to_thread(get_key)
                if not key: continue
                if key == "UP": selected_index = (selected_index - 1 + len(engine_choices)) % len(engine_choices)
                elif key == "DOWN": selected_index = (selected_index + 1) % len(engine_choices)
                elif key == "ENTER": break
                elif key.upper() == 'Q': return None
        
        return engine_choices[selected_index]['action']

    async def _run_fixer_process(self, scan_target: str):
        """Uruchamia pełny cykl: diagnoza, a następnie opcjonalna naprawa."""
        target_name_map = {'local_importer': "importowanych", 'all': "wszystkich", 'downloader': "pobranych"}
        target_name = target_name_map.get(scan_target, "nieznanych")
        
        with console.status(f"[cyan]Wczytywanie ścieżek {target_name} plików...[/]"):
            image_entries = await get_image_paths_for_analysis(extensions=tuple(SUPPORTED_IMAGE_EXTENSIONS), scan_target=scan_target)

        if not image_entries:
            console.print(f"\n[green]Nie znaleziono obrazów do weryfikacji w grupie '{target_name}'.[/]"); return

        selected_tests = await self._select_diagnostic_tests()
        if not selected_tests: return

        problematic_files = await run_diagnostics(image_entries, selected_tests)
        
        if not problematic_files:
            Prompt.ask("\n[bold green]✅ Diagnostyka zakończona. Nie znaleziono problemów. Naciśnij Enter...[/]"); return

        console.print(Panel(f"Zdiagnozowano [bold red]{len(problematic_files)}[/bold red] potencjalnie uszkodzonych plików.", title="Wynik Diagnozy"))
        if not Confirm.ask("\n[cyan]Czy chcesz rozpocząć procedurę naprawczą?[/]"): return

        engine = await self._select_fix_engine()
        if not engine: return

        backup_dir = Path(f"./_NAPRAWA_OBRAZOW_KOPIE_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        fix_func_map = {
            "imagemagick": fix_with_imagemagick,
            "pillow": fix_with_pillow,
            "exiftool": fix_with_exiftool
        }
        fix_func = fix_func_map[engine]
        
        with ImageFixerLiveDisplay(len(problematic_files), engine, backup_dir, console) as display:
            for item in problematic_files:
                source_path = item['path']
                success, message = await fix_func(source_path)
                display.update("success" if success else "error", source_path.name, message)

    async def run(self):
        """Główna pętla menu Naprawiacza Obrazów."""
        if not self._check_dependencies():
            Prompt.ask("\n[bold]Naciśnij Enter...[/]"); return

        menu_items = [
            {"icon": "📥", "text": "Analizuj pliki POBRANE z Google Photos", "action": "downloader", "description": "Skanuje tylko pliki graficzne, które zostały pobrane za pomocą tej aplikacji."},
            {"icon": "📁", "text": "Analizuj pliki IMPORTOWANE z dysku", "action": "local_importer", "description": "Skanuje tylko pliki graficzne, które zostały zaimportowane z lokalnych folderów."},
            {"icon": "🌐", "text": "Analizuj WSZYSTKIE pliki w bazie", "action": "all", "description": "Skanuje wszystkie pliki graficzne w bazie danych, niezależnie od ich pochodzenia."},
            {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "description": "Zamyka to narzędzie i wraca do menu głównego."},
        ]
        selected_index = 0

        while True:
            console.clear()
            
            def build_layout() -> Layout:
                table = Table.grid(expand=True, padding=(0, 2))
                for i, item in enumerate(menu_items):
                    style = "bold white on blue" if i == selected_index else ""
                    prefix = "» " if i == selected_index else "  "
                    table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
                
                menu_panel = Panel(table, title="[bold blue]🛠️ Naprawiacz Plików Graficznych[/]", border_style="blue")
                info_panel = Panel(Align.center(f"[italic]{menu_items[selected_index]['description']}[/]", vertical="middle"), title="[bold]Opis Opcji[/]", border_style="dim")
                
                layout = Layout(); body = Layout(name="body"); body.split_row(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1))
                footer = Text.from_markup("Nawigacja: ▲/▼ | Wybór: Enter | Powrót: Q", justify="center")
                layout.split_column(body, Layout(footer, size=1)); return layout
            
            with Live(build_layout(), transient=True, auto_refresh=False) as live:
                while True:
                    live.update(build_layout(), refresh=True)
                    key = await asyncio.to_thread(get_key)
                    if not key: continue
                    if key == "UP": selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                    elif key == "DOWN": selected_index = (selected_index + 1) % len(menu_items)
                    elif key == "ENTER": break
                    elif key.upper() == 'Q': selected_index = -1; break
            
            if selected_index == -1 or menu_items[selected_index]['action'] == 'back': break
            
            selected_action = menu_items[selected_index]['action']
            console.clear()
            await self._run_fixer_process(scan_target=selected_action)
            Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter, aby wrócić do menu Naprawiacza...[/]")

async def run_image_fixer():
    """Główny punkt wejścia do modułu."""
    app = ImageFixerMenuApp()
    await app.run()
