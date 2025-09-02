# -*- coding: utf-8 -*-

# plik: core/db_editor_logic.py
# Wersja 8.0 - Sfinalizowana z Ujednoliconym UI i Pełną Dokumentacją
#
# ##############################################################################
# ===                        JAK TO DZIAŁA (PROSTE WYJAŚNIENIE)                ===
# ##############################################################################
#
# Ten plik zawiera logikę dla w pełni interaktywnego, terminalowego edytora
# bazy danych. Działa on podobnie do prostego menedżera bazy danych,
# pozwalając na:
#
#  - Przeglądanie zawartości tabeli `downloaded_media` z paginacją.
#  - Sortowanie i filtrowanie wyników po różnych kolumnach.
#  - Masowe zaznaczanie i usuwanie wpisów.
#  - Masową zmianę statusu dla zaznaczonych wpisów.
#  - Podgląd szczegółów pojedynczego rekordu, w tym pełnego JSON-a z metadanymi.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import sqlite3
import math
import json
import logging
from pathlib import Path
import asyncio

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text
from rich.live import Live
from rich.layout import Layout
from rich.table import Table
from rich.align import Align
from rich.json import JSON
from rich.prompt import Prompt, Confirm

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config_editor_logic import get_key
from .utils import create_interactive_menu
from .database import setup_database

# --- Inicjalizacja i Konfiguracja Modułu ---
#console = Console(record=True)
console = Console()
logger = logging.getLogger(__name__)


class RichDbEditor:
    """
    Klasa enkapsulująca całą logikę interaktywnego edytora bazy danych.

    Zarządza stanem (bieżąca strona, sortowanie, filtry, zaznaczenia),
    wczytywaniem danych z bazy, dynamicznym renderowaniem interfejsu
    oraz obsługą wszystkich interakcji z użytkownikiem.
    """
    def __init__(self, db_file: str):
        """
        Inicjalizuje stan edytora.

        Args:
            db_file (str): Ścieżka do pliku bazy danych.
        """
        self.db_file = Path(db_file)
        self.entries: list = []
        self.selected_index_on_page: int = 0
        self.selected_ids: set = set()
        self.page_size: int = 30
        self.current_page: int = 0
        self.total_pages: int = 0
        self.total_entries: int = 0
        self.sort_options: list = [
            ('id', 'DESC', 'ID (malejąco)'), ('id', 'ASC', 'ID (rosnąco)'),
            ('status', 'ASC', 'Status (A-Z)'), ('filename', 'ASC', 'Nazwa pliku (A-Z)'),
            ('retry_count', 'DESC', 'Próby (malejąco)'), ('timestamp', 'DESC', 'Data dodania (najnowsze)')
        ]
        self.current_sort_index: int = 0
        self.filter_text: str = ""
        self.running: bool = False

    def _get_connection(self) -> sqlite3.Connection | None:
        """Nawiązuje połączenie z bazą danych i zwraca obiekt połączenia."""
        try:
            conn = sqlite3.connect(self.db_file, detect_types=sqlite3.PARSE_DECLTYPES)
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.Error as e:
            logger.critical("Błąd krytyczny połączenia z bazą danych.", exc_info=True)
            return None

    def _load_data(self):
        """
        Wczytuje dane z bazy dla bieżącej strony, uwzględniając sortowanie i filtrowanie.
        """
        conn = self._get_connection()
        if not conn: self.running = False; return
        with conn:
            where_clause = "WHERE status LIKE ?" if self.filter_text else ""
            params = [f"%{self.filter_text}%"] if self.filter_text else []
            count_query = f"SELECT COUNT(id) FROM downloaded_media {where_clause}"
            self.total_entries = conn.execute(count_query, params).fetchone()[0]
            self.total_pages = math.ceil(self.total_entries / self.page_size) if self.page_size > 0 else 1
            self.current_page = max(0, min(self.current_page, self.total_pages - 1))
            sort_col, sort_ord, _ = self.sort_options[self.current_sort_index]
            offset = self.current_page * self.page_size
            query = f"SELECT id, url, status, retry_count, final_path, expected_path, metadata_json FROM downloaded_media {where_clause} ORDER BY {sort_col} {sort_ord} LIMIT ? OFFSET ?"
            params.extend([self.page_size, offset])
            self.entries = conn.execute(query, params).fetchall()
            if self.entries and self.selected_index_on_page >= len(self.entries): self.selected_index_on_page = len(self.entries) - 1
            elif not self.entries: self.selected_index_on_page = 0
        logger.debug(f"Wczytano {len(self.entries)} wpisów. Strona {self.current_page + 1}/{self.total_pages}.")

    def _generate_layout(self) -> Layout:
        """Tworzy i zwraca pełen układ interfejsu edytora."""
        header = Panel(Align.center(Text("🗂️ Edytor Bazy Danych 🗂️", style="bold white on blue")), border_style="blue")
        table = Table(expand=True, border_style="green", title="Wpisy w bazie danych")
        table.add_column("✓", width=1); table.add_column("S", width=1); table.add_column("M", width=1)
        table.add_column("ID", style="dim", width=5); table.add_column("Prób", width=5, justify="right")
        table.add_column("Ścieżka Pliku (final_path)", style="cyan"); table.add_column("Ścieżka Oczekiwana (expected_path)", style="yellow")
        status_colors = {"downloaded": "green", "skipped": "yellow", "failed": "red", "pending": "cyan", "scanned": "blue"}
        for i, entry in enumerate(self.entries):
            check = "✓" if entry['id'] in self.selected_ids else ""
            status = Text("●", style=status_colors.get(entry['status'], "white"))
            meta = Text("✓", style="bold green") if entry['metadata_json'] and entry['metadata_json'] != 'null' else ""
            mismatch = entry['final_path'] and entry['expected_path'] and Path(entry['final_path']).resolve() != Path(entry['expected_path']).resolve()
            path_text = Text(str(entry['final_path'] or "Brak"), style="red" if mismatch else "cyan")
            table.add_row(check, status, meta, str(entry['id']), str(entry['retry_count']), path_text, str(entry['expected_path'] or "Brak"), style="black on white" if i == self.selected_index_on_page else "")
        _, _, sort_desc = self.sort_options[self.current_sort_index]
        footer = f"Strona: {self.current_page + 1}/{self.total_pages or 1} ({self.total_entries} wpisów) | Zaznaczono: {len(self.selected_ids)}"
        if self.filter_text: footer += f" | Filtr: '{self.filter_text}'"
        footer += f" | Sortowanie: {sort_desc}"
        help1 = "Nawigacja: Strzałki | Zaznacz: Spacja/A | Zmień Status: S | Usuń: D | Filtr: F | Sort: T | Szczegóły: Enter | Wyjdź: Q"
        help2 = Text("Status: ").append("●", style="green").append(" Pobrane ").append("●", style="yellow").append(" Pominięte ").append("●", style="red").append(" Błąd ").append("●", style="blue").append(" Zeskanowane | ").append("✓", style="green").append(" Posiada Metadane")
        layout = Layout(); layout.split(Layout(header, size=3), Layout(table), Layout(Panel(footer, title="Status"), size=3), Layout(Align.center(Group(Text(help1, style="dim"), help2)), size=2))
        return layout

    async def _handle_keypress(self, key: str):
        """Obsługuje wszystkie akcje użytkownika w głównym oknie edytora."""
        if key == "UP": self.selected_index_on_page = max(0, self.selected_index_on_page - 1)
        elif key == "DOWN": self.selected_index_on_page = min(len(self.entries) - 1, self.selected_index_on_page + 1) if self.entries else 0
        elif key == "LEFT": self.current_page = max(0, self.current_page - 1); self._load_data()
        elif key == "RIGHT": self.current_page = min(self.total_pages - 1, self.current_page + 1); self._load_data()
        elif key == ' ': self._toggle_selection()
        elif key.upper() == 'A': self._toggle_all_on_page()
        elif key.upper() == 'T': self.current_sort_index = (self.current_sort_index + 1) % len(self.sort_options); self._load_data()
        elif key.upper() == 'Q': self.running = False
        elif key.upper() == 'F': await self._handle_filter()
        elif key.upper() == 'S': await self._handle_status_change()
        elif key.upper() == 'D': await self._handle_delete()
        elif key == 'ENTER' and self.entries: await self._show_details()

    def _toggle_selection(self):
        """Zaznacza lub odznacza pojedynczy, podświetlony wpis."""
        if not self.entries: return
        entry_id = self.entries[self.selected_index_on_page]['id']
        if entry_id in self.selected_ids: self.selected_ids.remove(entry_id)
        else: self.selected_ids.add(entry_id)

    def _toggle_all_on_page(self):
        """Zaznacza lub odznacza wszystkie wpisy na bieżącej stronie."""
        if not self.entries: return
        page_ids = {entry['id'] for entry in self.entries}
        if page_ids.issubset(self.selected_ids): self.selected_ids.difference_update(page_ids)
        else: self.selected_ids.update(page_ids)

    async def _handle_filter(self):
        """Obsługuje logikę filtrowania wyników po statusie."""
        options = ["pending", "downloaded", "skipped", "failed", "scanned", "Wyczyść Filtr"]
        choice = await create_interactive_menu(
            [(opt.capitalize(), opt) for opt in options] + [("Anuluj", "cancel")],
            "Filtruj wg statusu", border_style="yellow"
        )
        if choice and choice != "cancel":
            self.filter_text = "" if choice == "Wyczyść Filtr" else choice
            self.current_page = 0; self._load_data()
            logger.info(f"Zastosowano filtr: '{self.filter_text}'") if self.filter_text else logger.info("Filtr wyczyszczony.")

    async def _handle_delete(self):
        """Obsługuje logikę usuwania zaznaczonych wpisów z bazy."""
        if not self.selected_ids: logger.warning("Najpierw zaznacz wpisy do usunięcia (spacją)."); await asyncio.sleep(1.5); return
        if Confirm.ask(f"[bold red]Czy na pewno usunąć {len(self.selected_ids)} zaznaczonych wpisów z bazy danych?[/]"):
            conn = self._get_connection()
            if not conn: return
            with conn:
                placeholders = ','.join('?' * len(self.selected_ids))
                cursor = conn.execute(f"DELETE FROM downloaded_media WHERE id IN ({placeholders})", list(self.selected_ids))
                logger.info(f"Usunięto {cursor.rowcount} wpisów z bazy danych.")
            self.selected_ids.clear(); self._load_data()

    async def _handle_status_change(self):
        """Obsługuje logikę masowej zmiany statusu dla zaznaczonych wpisów."""
        if not self.selected_ids: logger.warning("Najpierw zaznacz wpisy do zmiany statusu (spacją)."); await asyncio.sleep(1.5); return
        options = ["pending", "downloaded", "skipped", "failed"]
        new_status = await create_interactive_menu(
            [(opt.capitalize(), opt) for opt in options] + [("Anuluj", "cancel")],
            f"Wybierz nowy status dla {len(self.selected_ids)} wpisów", border_style="yellow"
        )
        if new_status and new_status != "cancel":
            conn = self._get_connection()
            if not conn: return
            with conn:
                placeholders = ','.join('?' * len(self.selected_ids))
                cursor = conn.execute(f"UPDATE downloaded_media SET status = ?, retry_count = 0 WHERE id IN ({placeholders})", [new_status] + list(self.selected_ids))
                logger.info(f"Zmieniono status dla {cursor.rowcount} wpisów na '{new_status}'.")
            self.selected_ids.clear(); self._load_data()

    async def _show_details(self):
        """Wyświetla szczegółowy widok dla pojedynczego wpisu, włączając dane JSON."""
        entry = self.entries[self.selected_index_on_page]
        text_content = Text(); text_content.append(f"ID: {entry['id']}\n", style="bold"); text_content.append(f"Status: {entry['status']}\n")
        text_content.append(f"Próby: {entry['retry_count']}\n"); text_content.append(f"Ścieżka: {entry['final_path']}\n")
        text_content.append(f"Oczekiwana: {entry['expected_path']}\n"); text_content.append(f"URL: {entry['url']}\n\n")
        details_panel = Panel(text_content, title=f"Szczegóły wpisu #{entry['id']}", border_style="yellow")
        console.clear(); console.print(details_panel)
        if entry['metadata_json']:
            console.print(Panel(JSON(entry['metadata_json']), title="Metadane (JSON)", border_style="cyan"))
        Prompt.ask("\n[bold]Naciśnij Enter, aby zamknąć szczegóły...[/]")
        console.clear()

    async def run(self):
        """Główna pętla, która uruchamia i zarządza edytorem."""
        if not self.db_file.exists():
            logger.error(f"Plik bazy danych '{self.db_file}' nie istnieje!"); return
        logger.info("Uruchamiam Edytor Bazy Danych...")
        self.running = True; self._load_data()
        with Live(self._generate_layout(), screen=True, auto_refresh=False, transient=True) as live:
            while self.running:
                live.update(self._generate_layout(), refresh=True)
                key = await asyncio.to_thread(get_key)
                if key:
                    if key.upper() in ('F', 'S', 'D') or key == 'ENTER':
                        live.stop()
                        await self._handle_keypress(key)
                        if self.running: live.start()
                    else:
                        await self._handle_keypress(key)

# ##############################################################################
# ===                    SEKCJA 3: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_db_editor(db_file: str):
    """
    Inicjalizuje i uruchamia instancję klasy `RichDbEditor`.
    """
    await setup_database()
    editor = RichDbEditor(db_file)
    await editor.run()

