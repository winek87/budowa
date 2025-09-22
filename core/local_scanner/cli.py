# plik: core/local_scanner/cli.py
# Wersja 1.7 - Naprawiono działanie opcji "Wróć do menu głównego".
# Opis: Moduł zarządzający interaktywnym menu Lokalnego Importera.
# -*- coding: utf-8 -*-

import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Any

from rich.align import Align
from rich.console import Console, Group
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table
from rich.text import Text

try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False
from .config import LOCAL_SCANNER_DIRECTORIES, add_local_scanner_directory
from ..utils import get_key
from .tasks import process_folder
from .ui.live_display import LocalScannerLiveDisplay

console = Console()
logger = logging.getLogger(__name__)

class LocalScannerMenuApp:
    def __init__(self):
        self.selected_index: int = 0
        self.menu_items: List[Dict[str, Any]] = []
        self.refresh_menu_items()

    def refresh_menu_items(self):
        items = [{"type": "header", "text": "ZAPISANE LOKALIZACJE"}]
        if not LOCAL_SCANNER_DIRECTORIES:
            items.append({"icon": "⚫", "text": "Brak zapisanych lokalizacji", "action": None, "description": "Użyj opcji poniżej, aby dodać stałe foldery do szybkiego dostępu."})
        for path_str in sorted(LOCAL_SCANNER_DIRECTORIES):
            items.append({"icon": "📁", "text": f"Przetwórz: {path_str}", "action": Path(path_str), "description": f"Rozpoczyna skanowanie plików w folderze '{Path(path_str).name}' i wszystkich jego podfolderach."})
        items.extend([{"type": "header", "text": "INNE OPCJE"}, {"icon": "➕", "text": "Przetwórz jednorazowy folder", "action": "scan_new", "description": "Pozwala na podanie ścieżki do folderu, który nie zostanie zapisany w konfiguracji."}, {"icon": "💾", "text": "Dodaj nowy folder do zapisanych", "action": "add_new_path", "description": "Dodaje nową, stałą ścieżkę do listy szybkiego wyboru i zapisuje ją w pliku konfiguracyjnym."}, {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "description": "Zamyka to menu i wraca do menu głównego aplikacji."}])
        self.menu_items = items
        self.selected_index = next((i for i, item in enumerate(self.menu_items) if item.get("action") is not None), 0)

    def _build_layout(self) -> Layout:
        menu_table = Table.grid(expand=True, padding=(0, 2))
        for i, item in enumerate(self.menu_items):
            if item.get("type") == "header":
                menu_table.add_row(); menu_table.add_row(Text(f" {item['text']} ", style="bold underline gold3"))
            else:
                style = "bold white on dark_blue" if i == self.selected_index else ""
                prefix = "» " if i == self.selected_index else "  "
                label_text = f"{prefix}{item['icon']} {item['text']}"
                menu_table.add_row(Text.from_markup(label_text, style=style))
        menu_panel = Panel(menu_table, title="[bold blue]📂 Lokalny Importer i Indekser[/]", border_style="blue")
        selected_item = self.menu_items[self.selected_index]
        info_panel = Panel(Align.center(f"[bold underline bright_blue]{selected_item['icon']} {selected_item['text']}[/]\n\n" f"[italic]{selected_item['description']}[/]", vertical="middle"), title="[bold]Opis Opcji[/]", border_style="dim")
        layout = Layout()
        body_layout = Layout(name="body"); body_layout.split_row(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1))
        footer = Text.from_markup(" Nawigacja: [on bright_black] ▲ ▼ [/] | Wybór: [on bright_black] Enter [/] | Powrót: [on bright_black] Q [/] ", style="white")
        layout.split_column(body_layout, Layout(Align.center(footer), size=1))
        return layout

    async def _handle_input(self) -> str:
        key = await asyncio.to_thread(get_key);
        if not key: return 'CONTINUE'
        if key == "UP":
            original_index = self.selected_index
            while True:
                self.selected_index = (self.selected_index - 1 + len(self.menu_items)) % len(self.menu_items)
                if self.menu_items[self.selected_index].get("action") is not None: break
                if self.selected_index == original_index: break
        elif key == "DOWN":
            original_index = self.selected_index
            while True:
                self.selected_index = (self.selected_index + 1) % len(self.menu_items)
                if self.menu_items[self.selected_index].get("action") is not None: break
                if self.selected_index == original_index: break
        elif key.upper() == 'Q': return 'EXIT_MENU'
        elif key == "ENTER": return 'EXECUTE_ACTION'
        return 'CONTINUE'

    async def _get_import_mode_from_user(self, folder_path: Path) -> str | None:
        selected_index = 0
        menu_items = [{"icon": "📥", "text": "Importuj i Organizuj", "description": "Pliki zostaną [bold green]SKOPIOWANE[/] do Twojej głównej biblioteki i posortowane w folderach ROK/MIESIĄC.", "action": "copy"}, {"icon": "🔎", "text": "Skanuj i Indeksuj", "description": "Pliki [bold yellow]POZOSTANĄ[/] w swojej oryginalnej lokalizacji. Do bazy zostanie dodany tylko wpis wskazujący na plik.", "action": "index"}, {"icon": "🚪", "text": "Wróć", "description": "Wróć do poprzedniego menu bez wykonywania żadnej akcji.", "action": "back"}]
        def build_choice_layout() -> Layout:
            table = Table.grid(expand=True, padding=(0, 2))
            for i, item in enumerate(menu_items):
                style = "bold white on dark_blue" if i == selected_index else ""
                prefix = "» " if i == selected_index else "  "
                table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
            menu_panel = Panel(table, title=f"[bold]Wybierz tryb dla: [cyan]{folder_path.name}[/][/bold]", border_style="blue")
            info_panel = Panel(Align.center(f"[italic]{menu_items[selected_index]['description']}[/]", vertical="middle"), title="[bold]Opis trybu[/]", border_style="dim", height=7)
            return Layout(Group(menu_panel, info_panel))
        console.clear()
        with Live(build_choice_layout(), screen=True, transient=True, auto_refresh=False) as live:
            while True:
                live.update(build_choice_layout(), refresh=True)
                key = await asyncio.to_thread(get_key)
                if key == "UP": selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                elif key == "DOWN": selected_index = (selected_index + 1) % len(menu_items)
                elif key == "ENTER": return menu_items[selected_index]['action']
                elif key and key.upper() == 'Q': return 'back'
    
    async def _execute_action(self, selected_action: Any):
        # Ta funkcja jest teraz prostsza, nie musi już obsługiwać akcji 'back'
        folder_to_process = None
        if isinstance(selected_action, Path):
            folder_to_process = selected_action
        elif selected_action == "scan_new":
            console.clear(); console.print(Panel("Wybrano opcję przetwarzania jednorazowego folderu.", style="blue"))
            path_str = Prompt.ask("[cyan]Podaj pełną ścieżkę do folderu[/]")
            if path_str: folder_to_process = Path(path_str.strip()).expanduser().resolve()
        elif selected_action == "add_new_path":
            console.clear(); console.print(Panel("Dodawanie nowej, stałej ścieżki do konfiguracji.", style="green"))
            path_str = Prompt.ask("[cyan]Podaj pełną ścieżkę do folderu, który chcesz zapisać[/]")
            if path_str:
                new_path = Path(path_str.strip()).expanduser().resolve()
                if not await asyncio.to_thread(new_path.is_dir):
                    console.print(f"\n[bold red]Błąd: Ścieżka '{new_path}' nie jest prawidłowym folderem.[/bold red]")
                elif add_local_scanner_directory(str(new_path)):
                    console.print(f"\n[bold green]✅ Sukces! Ścieżka została zapisana w konfiguracji.[/bold green]")
                    self.refresh_menu_items()
                else:
                    console.print(f"\n[bold yellow]Informacja: Ta ścieżka już istnieje w konfiguracji.[/bold yellow]")
            await asyncio.sleep(2.5)
            return

        if not folder_to_process or not await asyncio.to_thread(folder_to_process.is_dir):
            if folder_to_process is not None:
                console.print(f"\n[bold red]Błąd: Ścieżka '{folder_to_process}' nie jest prawidłowym folderem.[/bold red]")
                await asyncio.sleep(2.5)
            return

        import_mode = await self._get_import_mode_from_user(folder_to_process)
        if import_mode == 'back' or import_mode is None:
            return

        title = f"Import i Organizacja z: {folder_to_process.name}" if import_mode == 'copy' else f"Indeksowanie Folderu: {folder_to_process.name}"
        
        try:
            with LocalScannerLiveDisplay(total_items=0, title=title, console=console) as display:
                await process_folder(folder_to_process, import_mode, display)
        except Exception as e:
            logger.critical(f"Wystąpił krytyczny błąd podczas przetwarzania folderu: {e}", exc_info=True)
            console.print(f"[bold red]Wystąpił nieoczekiwany błąd. Sprawdź logi.[/]")
        
        Prompt.ask("\n[green]✅[/] [bold]Operacja zakończona. Naciśnij Enter, aby wrócić do menu...[/]")

    async def run(self):
        """Główna pętla menu."""
        while True:
            console.clear()
            with Live(self._build_layout(), screen=True, auto_refresh=False, transient=True) as live:
                while True:
                    live.update(self._build_layout(), refresh=True)
                    action = await self._handle_input()
                    if action != 'CONTINUE': break
            
            if action == 'EXIT_MENU': break
            
            # === POCZĄTEK POPRAWKI: Sprawdzamy akcję 'back' tutaj ===
            if action == 'EXECUTE_ACTION':
                selected_action = self.menu_items[self.selected_index].get("action")
                if selected_action == 'back':
                    break # Przerywamy pętlę i wracamy do menu głównego
                
                await self._execute_action(selected_action)
            # === KONIEC POPRAWKI ===

def _check_dependencies() -> bool:
    if not EXIFTOOL_AVAILABLE:
        console.print(Panel("[bold red]Błąd: Brak 'pyexiftool'![/bold red]\nUruchom: [cyan]pip install pyexiftool[/cyan]", title="Brak Zależności"))
        return False
    return True

async def run_local_scanner_menu():
    if not _check_dependencies(): return
    app = LocalScannerMenuApp()
    await app.run()
