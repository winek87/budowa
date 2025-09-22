# plik: core/backup_manager/cli.py
# Wersja 1.6 - Ulepszono ostrzeżenia przy przywracaniu bazy MariaDB i dodano stronicowanie.
# Opis: Moduł zarządza menu i przepływem pracy dla tworzenia
#       i przywracania kopii zapasowych.
# -*- coding: utf-8 -*-

import asyncio
import logging
import math
import os
import tempfile
from datetime import datetime
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
from rich.filesize import decimal

from ..config import BACKUP_DIR, PROJECT_BACKUP_CONFIG, DB_TYPE, DB_CONFIG_MARIADB
from ..utils import get_key
from ..database import setup_database
from .tasks import (
    create_archive, get_files_for_data_backup, get_files_for_core_app_backup,
    get_files_for_full_project_backup, restore_data_from_zip
)
from .ui.live_display import BackupLiveDisplay

console = Console()
logger = logging.getLogger(__name__)

class BackupManagerMenuApp:
    """Klasa hermetyzująca logikę i stan menu Menedżera Kopii Zapasowych."""

    def __init__(self):
        self.selected_index: int = 0
        self.menu_items: List[Dict[str, Any]] = self._define_menu_items()

    def _define_menu_items(self) -> List[Dict[str, Any]]:
        """Definiuje strukturę menu."""
        return [
            {"icon": "💾", "text": "Utwórz kopię DANYCH (baza, sesja, config)", "action": self._run_create_data_backup, "description": "Najważniejsza opcja. Tworzy małe archiwum .zip z Twoimi danymi, które można łatwo przenieść i przywrócić."},
            {"icon": "📦", "text": "Utwórz kopię RDZENIA APLIKACJI (kod)", "action": self._run_create_core_backup, "description": "Tworzy archiwum .tar.gz z kluczowymi plikami kodu źródłowego aplikacji. Użyteczne przed wprowadzeniem dużych zmian."},
            {"icon": "📚", "text": "Utwórz PEŁNĄ kopię PROJEKTU (.gitignore)", "action": self._run_create_full_backup, "description": "Tworzy duże archiwum .tar.gz z całym folderem projektu, respektując reguły z pliku .gitignore."},
            {"icon": "📥", "text": "Przywróć DANE z kopii zapasowej", "action": self._run_restore_backup, "description": "Pozwala wybrać archiwum .zip i przywrócić z niego dane, nadpisując obecne pliki."},
            {"icon": "🚪", "text": "Wróć do menu głównego", "action": "back", "description": "Zamyka Menedżera Kopii Zapasowych."},
        ]

    def _build_layout(self) -> Layout:
        """Tworzy dynamiczny, dwukolumnowy layout menu."""
        table = Table.grid(expand=True, padding=(0, 2))
        for i, item in enumerate(self.menu_items):
            style = "bold white on green" if i == self.selected_index else ""
            prefix = "» " if i == self.selected_index else "  "
            table.add_row(Text(f"{prefix}{item['icon']} {item['text']}", style=style))
        
        menu_panel = Panel(table, title="[bold green]💾 Menedżer Kopii Zapasowych[/]", border_style="green")
        
        selected_item = self.menu_items[self.selected_index]
        info_panel = Panel(Align.center(f"[bold underline bright_green]{selected_item['icon']} {selected_item['text']}[/]\n\n[italic]{selected_item['description']}[/]", vertical="middle"), title="[bold]Opis Opcji[/]", border_style="dim")
        
        layout = Layout()
        body = Layout(name="body"); body.split_row(Layout(menu_panel, ratio=2), Layout(info_panel, ratio=1))
        footer_text = Text.from_markup("Nawigacja: [on bright_black] ▲/▼ [/] | Wybór: [on bright_black] Enter [/] | Powrót: [on bright_black] Q [/]", justify="center")
        layout.split_column(body, Layout(footer_text, size=1))
        return layout

    async def _handle_input(self) -> str:
        key = await asyncio.to_thread(get_key);
        if not key: return 'CONTINUE'
        if key == "UP": self.selected_index = (self.selected_index - 1 + len(self.menu_items)) % len(self.menu_items)
        elif key == "DOWN": self.selected_index = (self.selected_index + 1) % len(self.menu_items)
        elif key.upper() == 'Q': return 'EXIT_MENU'
        elif key == "ENTER": return 'EXECUTE_ACTION'
        return 'CONTINUE'
    
    async def _run_create_backup(self, backup_type: str):
        """Uogólniona metoda do tworzenia różnych typów kopii zapasowych."""
        temp_dir = None
        try:
            files_func_map = {'core': get_files_for_core_app_backup, 'full': get_files_for_full_project_backup}
            title_map = {'data': "📦 Tworzenie Kopii Zapasowej Danych", 'core': "📦 Tworzenie Kopii Rdzenia Aplikacji", 'full': "📦 Tworzenie Pełnej Kopii Projektu"}
            archive_ext_map = {'data': 'zip', 'core': 'tar', 'full': 'tar'}

            with console.status("[cyan]Zbieranie listy plików...[/]"):
                if backup_type == 'data':
                    temp_dir = tempfile.TemporaryDirectory()
                    temp_dir_path = Path(temp_dir.name)
                    files_to_pack = await get_files_for_data_backup(temp_dir_path)
                else:
                    files_to_pack = await files_func_map[backup_type]()
            
            if not files_to_pack:
                console.print(Panel("[bold yellow]Nie znaleziono żadnych plików do archiwizacji.[/]", title="Informacja")); await asyncio.sleep(2)
                return

            archive_dir = Path(BACKUP_DIR if backup_type == 'data' else PROJECT_BACKUP_CONFIG['ARCHIVE_DIR']); archive_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"backup_{backup_type}_{timestamp}.{archive_ext_map[backup_type]}" + (".gz" if archive_ext_map[backup_type] == 'tar' else "")
            archive_path = archive_dir / archive_name
            
            with BackupLiveDisplay(title=title_map[backup_type], console=console) as display:
                temp_dir_to_pass = Path(temp_dir.name) if temp_dir else None
                await create_archive(archive_ext_map[backup_type], files_to_pack, archive_path, display, temp_dir_to_pass)

            console.print(Panel(f"Kopia zapasowa pomyślnie utworzona.\n\nLokalizacja:\n[cyan]{archive_path.resolve()}[/]", title="[green]✅ Sukces![/]", border_style="green"))
        
        except Exception as e:
            logger.critical(f"Krytyczny błąd podczas tworzenia kopii {backup_type}: {e}", exc_info=True)
            console.print(Panel(f"[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/]\n\n{e}", title="Błąd", border_style="red"))
        
        finally:
            if temp_dir:
                temp_dir.cleanup()
        
        Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić...[/]")

    async def _run_create_data_backup(self): await self._run_create_backup('data')
    async def _run_create_core_backup(self): await self._run_create_backup('core')
    async def _run_create_full_backup(self): await self._run_create_backup('full')

    async def _run_restore_backup(self):
        """Interaktywny proces przywracania danych z menu i stronicowaniem."""
        console.clear()
        backup_dirs = [Path(BACKUP_DIR), Path(PROJECT_BACKUP_CONFIG['ARCHIVE_DIR'])]
        all_backups = []
        for d in set(backup_dirs):
            d.mkdir(parents=True, exist_ok=True)
            all_backups.extend(d.glob("backup_*.zip"))
            all_backups.extend(d.glob("backup_*.tar.gz"))
        
        backup_items = []
        for p in set(all_backups):
            is_restorable = "backup_data" in p.name and p.suffix == ".zip"
            backup_items.append({"path": p, "restorable": is_restorable})
            
        backup_items.sort(key=lambda x: x['path'].stat().st_mtime, reverse=True)

        if not backup_items:
            console.print(Panel("[bold yellow]Nie znaleziono żadnych plików kopii zapasowych w skonfigurowanych folderach.[/]", title="Informacja")); await asyncio.sleep(3)
            return
        
        ITEMS_PER_PAGE = 20
        selected_index_on_page = 0
        current_page = 0
        total_pages = math.ceil(len(backup_items) / ITEMS_PER_PAGE)

        def build_restore_layout() -> Layout:
            start_index = current_page * ITEMS_PER_PAGE
            end_index = start_index + ITEMS_PER_PAGE
            page_items = backup_items[start_index:end_index]
            
            table = Table(title=f"[bold]Wybierz kopię do przywrócenia (Strona {current_page + 1}/{total_pages})[/]", show_header=True, header_style="bold green", border_style="dim")
            table.add_column("Typ", style="magenta", width=12)
            table.add_column("Nazwa Pliku", style="yellow", ratio=2)
            table.add_column("Data Utworzenia", style="cyan", justify="center", ratio=1)
            table.add_column("Rozmiar", style="magenta", justify="right", ratio=1)
            
            for i, item in enumerate(page_items):
                p = item['path']
                stat = p.stat()
                date_str = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M")
                row_style = "bold white on green" if i == selected_index_on_page else ""
                
                prefix = "» " if i == selected_index_on_page else "  "
                filename_text = Text(f"{prefix}{p.name}")
                if not item['restorable']:
                    filename_text.stylize("dim")
                    row_style = "dim" if not row_style else f"dim {row_style}"

                backup_type = "Dane" if "data" in p.name else "Rdzeń" if "core" in p.name else "Projekt" if "full" in p.name else "Nieznany"
                type_text = Text(backup_type, style="green" if item['restorable'] else "dim")

                table.add_row(type_text, filename_text, date_str, decimal(stat.st_size), style=row_style)
            
            footer_nav = "▲/▼ - Nawigacja | ◀/▶ - Zmień stronę"
            footer_actions = "Enter - Wybierz | Q - Anuluj"
            footer_text = Text(f"{footer_nav.center(40)} • {footer_actions.center(30)}", justify="center")
            
            layout = Layout()
            layout.split_column(Layout(Align.center(table)), Layout(footer_text, size=1))
            return layout

        with Live(build_restore_layout(), screen=True, transient=True, auto_refresh=False) as live:
            while True:
                live.update(build_restore_layout(), refresh=True)
                key = await asyncio.to_thread(get_key)
                if not key: continue
                items_on_current_page = len(backup_items[current_page * ITEMS_PER_PAGE : (current_page * ITEMS_PER_PAGE) + ITEMS_PER_PAGE])
                if key == "UP": selected_index_on_page = (selected_index_on_page - 1 + items_on_current_page) % items_on_current_page
                elif key == "DOWN": selected_index_on_page = (selected_index_on_page + 1) % items_on_current_page
                elif key == "LEFT": current_page = (current_page - 1 + total_pages) % total_pages; selected_index_on_page = 0
                elif key == "RIGHT": current_page = (current_page + 1) % total_pages; selected_index_on_page = 0
                elif key == "ENTER": break
                elif key.upper() == 'Q': return
        
        final_selected_index = (current_page * ITEMS_PER_PAGE) + selected_index_on_page
        selected_item = backup_items[final_selected_index]
        
        if not selected_item['restorable']:
            console.print(Panel("[bold yellow]Ta kopia zapasowa nie może być automatycznie przywrócona z tego menu.[/]\n[dim]Obecnie obsługiwane jest tylko przywracanie kopii 'Danych'.[/dim]", title="Informacja")); await asyncio.sleep(4)
            return
        
        backup_to_restore = selected_item['path']

        warning_message = (
            f"\n[bold red]UWAGA![/] Spowoduje to [bold]NADISANIE[/] obecnych plików (sesja, config).\n"
        )
        if DB_TYPE == "mariadb":
            warning_message += f"[bold red]Co więcej, cała baza danych '{DB_CONFIG_MARIADB['db']}' zostanie WYCZYSZCZONA i zastąpiona danymi z kopii![/]\n"
        warning_message += f"\nCzy na pewno chcesz przywrócić [cyan]{backup_to_restore.name}[/]?"

        if not Confirm.ask(warning_message, default=False): return

        try:
            with console.status("[yellow]Przywracanie plików...[/]") as status:
                await restore_data_from_zip(backup_to_restore, lambda msg: status.update(f"[yellow]{msg}[/]"))
            console.print(Panel(f"Kopia zapasowa [cyan]{backup_to_restore.name}[/cyan] została przywrócona.", title="[green]✅ Sukces![/]", border_style="green"))
        except Exception as e:
            logger.critical(f"Krytyczny błąd podczas przywracania kopii: {e}", exc_info=True)
            console.print(Panel(f"[bold red]Błąd krytyczny. Sprawdź logi.[/]\n\n{e}", title="Błąd", border_style="red"))
        
        Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić...[/]")
        
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
            if action == 'EXECUTE_ACTION':
                selected_action = self.menu_items[self.selected_index].get("action")
                if selected_action == 'back': break
                if selected_action: await selected_action()

async def run_backup_manager():
    await setup_database()
    app = BackupManagerMenuApp()
    await app.run()
