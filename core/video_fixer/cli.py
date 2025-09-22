# plik: core/video_fixer/cli.py
# Wersja 1.2 - Dodano pauzę po zakończeniu diagnostyki dla lepszego UX.
# Opis: Moduł zarządzający menu i przepływem pracy dla diagnozy i naprawy
#       plików wideo za pomocą FFmpeg.
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
from ..database import get_video_paths_for_analysis
from ..utils import get_key

# Importy z wewnętrznych modułów pakietu
from .tasks import run_diagnostics, fix_single_video
from .ui import VideoFixerLiveDisplay

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)

SUPPORTED_VIDEO_EXTENSIONS = {'.mp4', '.mov', '.avi', '.mkv', '.webm', '.3gp', '.mpg', '.mpeg'}


class VideoFixerMenuApp:
    """Klasa zarządzająca całym przepływem pracy Naprawiacza Wideo."""

    def _check_dependencies(self) -> bool:
        """Sprawdza, czy FFmpeg jest zainstalowany i dostępny w systemie."""
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            console.print(Panel(
                "[bold red]Błąd: Nie znaleziono FFmpeg![/]\n\n"
                "Zainstaluj FFmpeg i upewnij się, że jest dodany do systemowej zmiennej PATH.",
                title="Brak Zależności"
            ))
            return False
        return True

    async def _run_fixer_process(self, scan_target: str):
        """
        Uruchamia pełny cykl: pobiera listę plików, diagnozuje, a następnie
        oferuje interaktywną naprawę.
        """
        target_name_map = {'local_import': "plików importowanych", 'all': "wszystkich plików", 'downloaded': "plików pobranych"}
        target_name = target_name_map.get(scan_target, "nieznanych")

        with console.status(f"[cyan]Wczytywanie ścieżek {target_name} plików wideo z bazy danych...[/]"):
            video_paths = await get_video_paths_for_analysis(tuple(SUPPORTED_VIDEO_EXTENSIONS), source_filter=scan_target)

        if not video_paths:
            console.print(f"\n[green]Nie znaleziono plików wideo do weryfikacji w grupie '{target_name}'.[/]")
            return

        with VideoFixerLiveDisplay(console=console) as display:
            # Etap 1: Diagnostyka
            problematic_files = await run_diagnostics(video_paths, display)
        
            # === POCZĄTEK POPRAWKI: Nowa, niezawodna logika ===
            # Dashboard diagnostyczny właśnie zniknął. Teraz my przejmujemy kontrolę.

            if not problematic_files:
                console.print(Panel("[bold green]✅ Gratulacje! Diagnostyka nie wykryła żadnych problemów.[/]", expand=False))
                display.update()
                Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić...[/]")
                return 

            display.error_logs.appendleft(Text.from_markup("[bold yellow]Diagnostyka zakończona. Sprawdź wyniki.[/]"))
            display.update()
        
            # Zapisujemy szczegółowe wyniki do pliku logu
            log_dir = Path("app_data/video_fixer_logs")
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / f"diagnostics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            with open(log_file, "w", encoding="utf-8") as f:
                f.write(f"Wyniki diagnostyki wideo z {datetime.now()}:\n\n")
                for item in problematic_files:
                    f.write(f"Plik: {item['path']}\n")
                    f.write(f"  Problem: {item['reason']}\n\n")

            # Wyświetlamy podsumowanie i informację o pliku
            console.print(Panel(
                f"Wykryto [bold yellow]{len(problematic_files)}[/] potencjalnie uszkodzonych plików.\n\n"
                f"Szczegółowy raport został zapisany w:\n[cyan]{log_file.resolve()}[/]",
                title="[bold]Wynik Diagnostyki[/]"
            ))
        
            if not Confirm.ask("\n[cyan]Czy chcesz spróbować je naprawić?[/]", default=True):
                return
        # === KONIEC POPRAWKI ===
        
        # Etap 2: Podsumowanie diagnostyki i potwierdzenie
        console.print(Panel(
            f"[bold yellow]Wykryto {len(problematic_files)} potencjalnie uszkodzonych plików wideo.[/]",
            title="Wynik Diagnostyki"
        ))
        
            # Dodajemy pauzę, aby użytkownik mógł przejrzeć listę błędów,
            # zanim zdecyduje, co robić dalej.
        Prompt.ask("[bold]Naciśnij Enter, aby kontynuować do etapu naprawy...[/]")

        if not Confirm.ask("\n[cyan]Czy chcesz spróbować je naprawić (proces polega na ponownym zakodowaniu)?", default=True):
            return

            # Etap 3: Naprawa
            backup_dir = Path(f"./_NAPRAWA_WIDEO_KOPIE_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            backup_dir.mkdir(parents=True, exist_ok=True)

            with VideoFixerLiveDisplay(total_items=len(problematic_files), backup_dir=backup_dir, console=console) as display:
                for item in problematic_files:
                    source_path = item['path']
                    try:
                        await fix_single_video(source_path, backup_dir, display)
                        display.update("success", source_path.name)
                    except Exception as e:
                        logger.error(f"Nie udało się naprawić pliku {source_path.name}: {e}", exc_info=True)
                        display.update("error", source_path.name, str(e))
                    finally:
                        display.update()
    
    async def run(self):
        """Główna pętla menu Naprawiacza Wideo."""
        if not self._check_dependencies():
            Prompt.ask("\n[yellow]Naciśnij Enter, aby wrócić...[/]"); return

        menu_items = [
            {"icon": "📥", "text": "Analizuj pliki POBRANE z Google Photos", "action": "downloaded", "description": "Skanuje tylko pliki wideo, które zostały pobrane za pomocą tej aplikacji."},
            {"icon": "📁", "text": "Analizuj pliki IMPORTOWANE z dysku", "action": "local_import", "description": "Skanuje tylko pliki wideo, które zostały zaimportowane z lokalnych folderów."},
            {"icon": "🌐", "text": "Analizuj WSZYSTKIE pliki w bazie", "action": "all", "description": "Skanuje wszystkie pliki wideo w bazie danych, niezależnie od ich pochodzenia."},
            {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "description": "Zamyka to narzędzie i wraca do menu głównego."},
        ]
        selected_index = 0

        while True:
            def build_layout() -> Layout:
                table = Table.grid(expand=True, padding=(0, 2))
                for i, item in enumerate(menu_items):
                    style = "bold white on blue" if i == selected_index else ""
                    prefix = "» " if i == selected_index else "  "
                    table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
                
                menu_panel = Panel(table, title="[bold blue]📹 Naprawiacz Plików Wideo (FFmpeg)[/]", border_style="blue")
                info_panel = Panel(Align.center(f"[italic]{menu_items[selected_index]['description']}[/]", vertical="middle"), title="[bold]Opis Opcji[/]", border_style="dim")
                
                layout = Layout(); body = Layout(name="body"); body.split_row(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1))
                footer = Text.from_markup("Nawigacja: ▲/▼ | Wybór: Enter | Powrót: Q", justify="center")
                layout.split_column(body, Layout(footer, size=1)); return layout
            
            console.clear()
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

async def run_video_fixer():
    """Główny punkt wejścia do modułu."""
    app = VideoFixerMenuApp()
    await app.run()
