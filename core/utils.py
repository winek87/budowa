# -*- coding: utf-8 -*-

# plik: core/utils.py
# Wersja 9.1 - Finalna wersja z rozbudowanym, konfigurowalnym modułem wyświetlania obrazów
#
# ##############################################################################
# ===                     MODUŁ Z UNIWERSALNYMI NARZĘDZIAMI                  ===
# ##############################################################################
#
# Ten plik jest "skrzynką z narzędziami" dla całej aplikacji. Zawiera zbiór
# małych, niezależnych funkcji, które wykonują specyficzne zadania, takie jak
# obsługa sygnału Ctrl+C, parsowanie daty, tworzenie unikalnych nazw plików
# oraz zaawansowane, konfigurowalne wyświetlanie plików.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import math
import logging
import sys
import os
from collections import deque
from typing import List, Set, Dict, Any
from pathlib import Path
from datetime import datetime
import webbrowser
import http.server
import socketserver
import threading
import subprocess
import platform
import socket
import shutil

# --- Importy specyficzne dla systemu (dla get_key) ---
try:
    import termios
    import tty
    IS_POSIX = True
except ImportError:
    IS_POSIX = False

# --- Playwright (dla type hintów) ---
from playwright.async_api import Page

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text
from rich.live import Live
from rich.align import Align
from rich.table import Table
from rich.layout import Layout

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import ENABLE_HEADLESS_CURSOR, DOWNLOADS_DIR_BASE, IMAGE_VIEWER_MODE

# --- Inicjalizacja i Konfiguracja Modułu ---
logger = logging.getLogger(__name__)
stop_event = asyncio.Event()
console = Console()

# ##############################################################################
# ===           SEKCJA 1: UNIWERSALNA FUNKCJA ODCZYTU KLAWISZY               ===
# ##############################################################################

def get_key() -> str | None:
    """
    Odczytuje pojedyncze naciśnięcie klawisza w terminalu.
    """
    if not IS_POSIX:
        try:
            line = input()
            return line[0].upper() if line else "ENTER"
        except Exception: return None
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        char = sys.stdin.read(1)
        if char == '\x1b':
            seq = sys.stdin.read(2)
            if seq == '[A': return "UP"
            if seq == '[B': return "DOWN"
            if seq == '[C': return "RIGHT"
            if seq == '[D': return "LEFT"
            return None
        elif char in ('\r', '\n'): return "ENTER"
        elif char == ' ': return ' '
        elif char.isalpha(): return char.upper()
        return char
    except Exception: return None
    finally: termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

# ##############################################################################
# ===                   SEKCJA 2: OBSŁUGA APLIKACJI I SYGNAŁÓW               ===
# ##############################################################################

def handle_shutdown_signal(sig, frame):
    """
    Obsługuje sygnał przerwania (Ctrl+C) w sposób bezpieczny dla `asyncio`.
    """
    if not stop_event.is_set():
        logger.warning("Otrzymano sygnał przerwania (Ctrl+C). Aplikacja zakończy pracę...")
        stop_event.set()

# ##############################################################################
# ===                   SEKCJA 3: INTERAKCJA Z PRZEGLĄDARKĄ                  ===
# ##############################################################################

async def move_cursor_in_circles(page: Page, stop_event: asyncio.Event, headless_mode: bool):
    """
    Asynchroniczne zadanie, które w tle porusza kursorem myszy po okręgu.
    """
    if headless_mode and not ENABLE_HEADLESS_CURSOR: return
    try:
        viewport_size = page.viewport_size
        if not viewport_size: return
        center_x, center_y = viewport_size['width'] / 2, viewport_size['height'] / 2
        radius = min(center_x, center_y) / 4; angle = 0
        while not stop_event.is_set():
            rad = math.radians(angle)
            x = center_x + radius * math.cos(rad)
            y = center_y + radius * math.sin(rad)
            try:
                await page.mouse.move(x, y, steps=5)
            except Exception:
                break
            angle = (angle + 10) % 360
            await asyncio.sleep(1)
    except Exception as e:
        if "Target page, context or browser has been closed" not in str(e):
            logger.debug(f"Zadanie kursora zatrzymane: {e}")

# ##############################################################################
# ===              SEKCJA 4: WYŚWIETLANIE OBRAZÓW I WIDEO (NOWA)             ===
# ##############################################################################
def _open_with_system_viewer(path: Path):
    """Metoda 1: Używa domyślnej przeglądarki systemowej (np. xdg-open)."""
    logger.info(f"Próba otwarcia pliku '{path.name}' za pomocą przeglądarki systemowej...")
    system = platform.system()
    
    try:
        if system == "Windows":
            os.startfile(path)
        elif system == "Darwin":
            subprocess.run(["open", path], check=True, capture_output=True)
        else:  # Linux i inne
            # --- POCZĄTEK KLUCZOWEJ POPRAWKI ---
            # Krok 1: Sprawdź, czy środowisko graficzne jest dostępne PRZED próbą otwarcia.
            if not os.environ.get('DISPLAY'):
                logger.warning("Brak środowiska graficznego (DISPLAY). Wyświetlanie niemożliwe w tym trybie.")
                console.print(f"\n[bold yellow]Ostrzeżenie:[/] Brak środowiska graficznego (sesja SSH?).")
                console.print(f"[dim]Aby zobaczyć plik, zmień tryb wyświetlania w 'core/config.py' na 'server'.[/dim]")
                console.print(f"[dim]Ścieżka do pliku: {path}[/dim]")
                return # Zakończ funkcję, nie próbuj nawet uruchamiać xdg-open

            # Krok 2: Jeśli jest DISPLAY, spróbuj otworzyć plik.
            subprocess.run(["xdg-open", path], check=True, capture_output=True, text=True)
            # --- KONIEC KLUCZOWEJ POPRAWKI ---

        console.print(f"[green]Wysłano polecenie otwarcia pliku [cyan]{path.name}[/cyan]. Sprawdź okna na swoim pulpicie.[/green]")

    except FileNotFoundError:
        logger.error(f"Polecenie systemowe do otwierania plików nie zostało znalezione dla systemu {system}.")
        console.print(f"[bold red]Błąd: Nie znaleziono polecenia do otwierania plików dla Twojego systemu.[/bold red]")
    
    except subprocess.CalledProcessError as e:
        error_output = e.stderr or e.stdout or "Brak dodatkowych informacji."
        logger.error(f"Polecenie otwarcia pliku zakończyło się błędem. stderr: {error_output.strip()}")
        console.print(f"\n[bold yellow]Ostrzeżenie:[/] Nie udało się otworzyć pliku za pomocą przeglądarki systemowej.")
        console.print(f"[dim]Szczegóły błędu: {error_output.strip()}[/dim]")
        
    except Exception as e:
        logger.error(f"Nie udało się otworzyć pliku: {e}", exc_info=True)
        console.print(f"[bold red]Błąd: Wystąpił nieoczekiwany problem z otwarciem przeglądarki systemowej.[/bold red]")

def _open_with_eog_unsafe(path: Path):
    """
    Metoda 4 (specjalna): Uruchamia przeglądarkę 'eog' z flagą --no-sandbox.
    Przydatne do pracy jako root w sesji SSH z przekierowaniem X11.
    """
    logger.info(f"Próba otwarcia pliku '{path.name}' za pomocą eog --no-sandbox...")
    try:
        # Sprawdzamy, czy polecenie 'eog' jest dostępne
        if not shutil.which("eog"):
            console.print(Panel(
                "[bold red]Błąd: Brak programu 'eog' (Eye of GNOME)![/bold red]\n\n"
                "Aby użyć tego trybu, zainstaluj go:\n"
                "[cyan]sudo apt-get update && sudo apt-get install eog[/cyan]",
                title="Brak Zależności", border_style="red"
            ))
            return

        # Uruchamiamy eog z flagą --no-sandbox.
        # Używamy Popen, aby nie czekać na zamknięcie programu i od razu kontynuować.
        subprocess.Popen(["eog", "--disable-gallery", "--single-window", str(path)])
        
        console.print(f"[green]Wysłano polecenie otwarcia pliku [cyan]{path.name}[/cyan] w trybie 'eog-unsafe'.[/green]")
        console.print("[dim]Sprawdź okna na swoim pulpicie.[/dim]")

    except Exception as e:
        logger.error(f"Nie udało się otworzyć pliku za pomocą 'eog --no-sandbox': {e}", exc_info=True)
        console.print(f"[bold red]Błąd: Wystąpił nieoczekiwany problem z uruchomieniem 'eog'.[/bold red]")

##
def _open_with_web_server(path: Path):
    """Metoda 2: Uruchamia tymczasowy serwer WWW i wyświetla link."""
    logger.info(f"Udostępnianie pliku '{path.name}' przez tymczasowy serwer WWW...")
    
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(path.parent), **kwargs)

    port = 8000 + os.getpid() % 1000
    httpd = socketserver.TCPServer(("", port), Handler)
    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    
    local_ip = "127.0.0.1"
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80)); local_ip = s.getsockname()[0]
    except Exception: pass

    file_url = f"http://{local_ip}:{port}/{path.name.replace(' ', '%20')}"
    console.print(Panel(
        f"Skopiuj ten link do przeglądarki na swoim komputerze:\n\n[bold cyan link={file_url}]{file_url}[/bold cyan link]\n\n[dim]Serwer zostanie automatycznie zamknięty po naciśnięciu dowolnego klawisza w tym oknie.[/dim]",
        title="🔗 Plik udostępniony przez Serwer WWW", border_style="yellow"
    ))
    
    get_key()
    
    httpd.shutdown(); httpd.server_close()
    logger.info("Tymczasowy serwer WWW został zamknięty.")
#####
def _open_with_sixel(path: Path):
    """Metoda 3: Próbuje wyświetlić obraz bezpośrednio w terminalu (Sixel/iTerm)."""
    logger.info(f"Próba wyświetlenia obrazu '{path.name}' bezpośrednio w terminalu...")
    
    # Sprawdzenie zależności pozostaje bez zmian
    if not check_dependency("term_image", "term-image", "term-image (do podglądu w terminalu)"):
        return
    
    import term_image
#    import termvisage
 
    try:
        # --- POCZĄTEK ZMIANY ---
        # Stara składnia: term_image.show(str(path))
        
        # Nowa, poprawna składnia:
        # 1. Tworzymy obiekt obrazu z pliku.
        image = term_image.image.from_file(str(path))
        # 2. "Drukujemy" obiekt do terminala.
        image.draw()
        # --- KONIEC ZMIANY ---
        
    except Exception as e:
        logger.error(f"Błąd podczas wyświetlania obrazu w terminalu: {e}", exc_info=True)
        console.print(f"[red]Nie udało się wyświetlić obrazu. Upewnij się, że Twój terminal (np. Kitty, iTerm2) to obsługuje.[/red]")

def open_image_viewer(path: Path):
    """
    Główna funkcja-dyspozytor do otwierania obrazów.

    Na podstawie zmiennej `IMAGE_VIEWER_MODE` w `config.py`, wybiera
    i uruchamia odpowiednią metodę wyświetlania pliku.

    Args:
        path (Path): Ścieżka do pliku obrazu lub wideo.
    """

    if not path.exists():
        logger.error(f"Próba otwarcia nieistniejącego pliku: {path}")
        console.print(f"[bold red]Błąd: Plik '{path}' nie istnieje na dysku.[/bold red]")
        return

    logger.info(f"Wybrano tryb wyświetlania: '{IMAGE_VIEWER_MODE}'. Otwieram plik: {path.name}")

    if IMAGE_VIEWER_MODE == 'server':
        _open_with_web_server(path)
    elif IMAGE_VIEWER_MODE == 'sixel':
        _open_with_sixel(path)
    elif IMAGE_VIEWER_MODE == 'eog-unsafe':
        _open_with_eog_unsafe(path)
    else:  # 'system' jest domyślny
        _open_with_system_viewer(path)

# ##############################################################################
# ===                    SEKCJA 5: PRZETWARZANIE DANYCH I PLIKÓW             ===
# ##############################################################################
async def get_date_from_metadata(metadata: dict) -> datetime | None:
    date_tags_priority = ['DateTime', 'EXIF:DateTimeOriginal', 'EXIF:CreateDate', 'QuickTime:CreateDate', 'XMP:CreateDate', 'XMP:DateCreated', 'File:FileModifyDate']
    for tag in date_tags_priority:
        if date_str := metadata.get(tag):
            try:
                cleaned_str = str(date_str).split('+')[0].split('.')[0].strip()
                if ":" in cleaned_str[0:10] and 'T' not in cleaned_str: return datetime.strptime(cleaned_str, '%Y:%m:%d %H:%M:%S')
                else: return datetime.fromisoformat(cleaned_str.replace('Z', '+00:00'))
            except (ValueError, TypeError): continue
    return None

def create_unique_filepath(dest_dir: Path, original_filename: str, current_path: Path = None) -> Path:
    base_path = dest_dir / original_filename
    if not base_path.exists() or (current_path and base_path.samefile(current_path)): return base_path
    counter = 1
    while True:
        new_path = dest_dir / f"{base_path.stem}_{counter}{base_path.suffix}"
        if not new_path.exists() or (current_path and new_path.samefile(current_path)): return new_path
        counter += 1

def _parse_metadata_for_display(metadata: dict, file_path: Path) -> dict:
    date_tags = ['EXIF:DateTimeOriginal', 'EXIF:CreateDate', 'QuickTime:CreateDate', 'DateTime', 'XMP:CreateDate', 'File:FileModifyDate']
    date_str = "Brak";
    for tag in date_tags:
        if tag in metadata: date_str = str(metadata[tag]).split('+')[0].strip(); break
    dimensions_str = metadata.get('File:ImageSize') or (f"{metadata.get('EXIF:ImageWidth')}x{metadata.get('EXIF:ImageHeight')}" if 'EXIF:ImageWidth' in metadata else "Brak")
    size_str = "Brak pliku"
    try:
        if file_path.exists(): size_bytes = file_path.stat().st_size; size_str = format_size_for_display(size_bytes)
    except (OSError, FileNotFoundError): pass
    file_type = metadata.get('File:FileType', "Brak"); camera_model = metadata.get('EXIF:Model') or metadata.get('Camera', "Brak")
    f_number, exposure_time, iso = metadata.get('EXIF:FNumber'), metadata.get('EXIF:ExposureTime'), metadata.get('EXIF:ISO')
    exposure_str = f"f/{f_number}, {exposure_time}s, ISO {iso}" if all([f_number, exposure_time, iso]) else "Brak"
    lat, lon = metadata.get('EXIF:GPSLatitude'), metadata.get('EXIF:GPSLongitude'); gps_str = f"{lat}, {lon}" if all([lat, lon]) else "Brak"
    return {"date": date_str, "dimensions": dimensions_str, "size": size_str, "type": file_type, "camera": camera_model, "exposure": exposure_str, "gps": gps_str}
# ##############################################################################

def format_size_for_display(size_bytes: int) -> str:
    """Konwertuje rozmiar w bajtach na czytelny format (KB, MB, GB)."""
    if size_bytes is None or not isinstance(size_bytes, (int, float)):
        return "B/D" # POPRAWKA: Zwraca "B/D" (Brak Danych)
    if size_bytes == 0:
        return "0.0 B" # POPRAWKA: Zwraca z jednym miejscem po przecinku
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 1) # POPRAWKA: Zaokrągla do 1 miejsca
    return f"{s:.1f} {size_name[i]}" # POPRAWKA: Formatowanie do 1 miejsca

# ##############################################################################
def format_size_for_display_old(size_bytes: int | None) -> str:
    if size_bytes is None: return "Brak"
    if not isinstance(size_bytes, (int, float)) or size_bytes < 0: return "Błąd"
    if size_bytes == 0: return "0.00 KB"
    size_units = ("B", "KB", "MB", "GB", "TB", "PB")
    power = int(math.log(size_bytes, 1024)) if size_bytes > 0 else 0
    power = min(power, len(size_units) - 1)
    value_in_unit = size_bytes / (1024 ** power); unit = size_units[power]
    return f"{value_in_unit:.2f} {unit}"
# ##############################################################################
# ##############################################################################
# ===                  SEKCJA 6: UNIWERSALNE KOMPONENTY UI                   ===
# ##############################################################################
async def _interactive_file_selector(items: list, title: str) -> list:
    """
    Uniwersalny, interaktywny selektor listy elementów (plików lub stringów),
    pozwalający na wielokrotny wybór.
    """
    if not items:
        return []

    selected_indices = set()
    current_index = 0

    def generate_layout() -> Layout:
        """Wewnętrzna funkcja renderująca interfejs listy."""
        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column(width=4)  # Na checkbox i strzałkę
        table.add_column()

        # Dynamicznie dostosuj widoczny fragment listy
        start_index = max(0, current_index - 10)
        end_index = min(len(items), start_index + 20)
        
        for i in range(start_index, end_index):
            item = items[i]
            # Sprawdzamy, czy element jest obiektem Path, aby poprawnie wyświetlić nazwę
            display_name = item.name if isinstance(item, Path) else str(item)
            
            cursor = "»" if i == current_index else " "
            checkbox = "[bold green]✓[/]" if i in selected_indices else "[dim]o[/]"
            
            style = "bold black on white" if i == current_index else ("bold green" if i in selected_indices else "")
            table.add_row(f"{cursor} {checkbox}", Text(display_name, style=style))

        footer_text = (
            "[bold]G/D[/](nawigacja) • [bold]SPACJA[/](zaznacz) • "
            "[bold]A[/](zaznacz wszystko) • [bold]N[/](odznacz wszystko) • "
            "[bold]ENTER[/](zatwierdź) • [bold]Q[/](anuluj)"
        )
        
        layout = Layout()
        layout.split_column(
            Layout(Panel(table, title=f"{title} ({len(selected_indices)}/{len(items)})"), name="main"),
            Layout(Align.center(Text(footer_text)), name="footer", size=1)
        )
        return layout

    with Live(generate_layout(), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_layout(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key:
                continue

            if key == "UP":
                current_index = (current_index - 1 + len(items)) % len(items)
            elif key == "DOWN":
                current_index = (current_index + 1) % len(items)
            elif key == ' ':
                if current_index in selected_indices:
                    selected_indices.remove(current_index)
                else:
                    selected_indices.add(current_index)
            elif key.upper() == 'A':
                selected_indices = set(range(len(items)))
            elif key.upper() == 'N':
                selected_indices.clear()
            elif key.upper() == 'Q' or key == "ESC":
                return []
            elif key == "ENTER":
                return [items[i] for i in sorted(list(selected_indices))]

async def _interactive_file_selector_bak(all_files: List[Path], title: str) -> List[Path]:
    FILES_PER_PAGE, selected_paths, current_page, selected_index_on_page = 20, set(), 0, 0
    base_download_path = Path(DOWNLOADS_DIR_BASE)
    def generate_panel() -> Panel:
        nonlocal selected_index_on_page, current_page
        total_pages = max(1, (len(all_files) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
        current_page = max(0, min(current_page, total_pages - 1))
        start_idx, end_idx = current_page * FILES_PER_PAGE, (current_page + 1) * FILES_PER_PAGE
        files_on_page = all_files[start_idx:end_idx]
        if selected_index_on_page >= len(files_on_page): selected_index_on_page = max(0, len(files_on_page) - 1)
        table_title = f"{title}\n[dim]Strona {current_page + 1}/{total_pages} | Zaznaczono: {len(selected_paths)}[/dim]"
        table = Table(title=table_title, title_justify="left"); table.add_column("✓", width=3); table.add_column("Ścieżka", style="cyan")
        for i, file_path in enumerate(files_on_page):
            selector = "[bold green][✔][/]" if file_path in selected_paths else "[dim][ ][/]"
            try: display_path = file_path.relative_to(base_download_path)
            except ValueError: display_path = file_path
            table.add_row(selector, str(display_path), style="black on white" if i == selected_index_on_page else "")
        nav_text = "[bold]G/D[/]•[bold]L/P[/](strony)•[bold]SPACJA[/](zaznacz)•[bold]ENTER[/](zatwierdź)•[bold]Q[/](anuluj)"
        return Panel(Group(table, Text(f"[dim]{nav_text}[/dim]", justify="center")), border_style="green")
    with Live(generate_panel(), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_panel(), refresh=True); key = await asyncio.to_thread(get_key)
            if not key: continue
            total_pages = max(1, (len(all_files) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
            files_on_page = all_files[current_page * FILES_PER_PAGE : (current_page + 1) * FILES_PER_PAGE]
            if key.upper() in ["Q", "ESC"]: return []
            elif key == "UP":
                if selected_index_on_page > 0: selected_index_on_page -= 1
                elif current_page > 0: current_page -= 1; selected_index_on_page = FILES_PER_PAGE - 1
            elif key == "DOWN":
                if selected_index_on_page < len(files_on_page) - 1: selected_index_on_page += 1
                elif current_page < total_pages - 1: current_page += 1; selected_index_on_page = 0
            elif key == "LEFT":
                if current_page > 0: current_page -= 1; selected_index_on_page = 0
            elif key == "RIGHT":
                if current_page < total_pages - 1: current_page += 1; selected_index_on_page = 0
            elif key == " ":
                if files_on_page:
                    path_to_toggle = files_on_page[selected_index_on_page]
                    if path_to_toggle in selected_paths: selected_paths.remove(path_to_toggle)
                    else: selected_paths.add(path_to_toggle)
            elif key == "ENTER": return list(selected_paths)

class LogCollectorHandler(logging.Handler):
    """
    Handler logowania, który przechwytuje komunikaty i dodaje je
    do obiektu deque, zamiast wyświetlać je bezpośrednio w konsoli.
    Jest to kluczowe do integracji logów z interfejsami Rich.Live.
    """
    def __init__(self, target_deque: deque):
        super().__init__()
        self.target_deque = target_deque

    def emit(self, record: logging.LogRecord):
        """Przechwytuje log i formatuje go jako obiekt Rich.Text."""
        msg = self.format(record)
        level_colors = {
            "INFO": "dim cyan",
            "WARNING": "yellow",
            "ERROR": "bold red",
            "CRITICAL": "bold white on red",
            "DEBUG": "dim green"
        }
        color = level_colors.get(record.levelname, "white")
        log_text = Text.from_markup(f"[{color}]{msg}[/{color}]")
        self.target_deque.appendleft(log_text)

class LogCollectorHandler_bak(logging.Handler):
    def __init__(self, target_deque: deque):
        super().__init__(); self.target_deque = target_deque
    def emit(self, record: logging.LogRecord):
        msg = record.getMessage(); level_colors = {"INFO": "dim cyan", "WARNING": "yellow", "ERROR": "bold red", "CRITICAL": "bold white on red", "DEBUG": "dim green"}
        color = level_colors.get(record.levelname, "white"); log_text = Text.from_markup(f"[{color}]{msg}[/{color}]")
        self.target_deque.appendleft(log_text)

async def create_interactive_menu(
    menu_items: list,
    title: str,
    subtitle: str = "",
    border_style: str = "blue"
) -> Any:
    """
    Tworzy i zarządza uniwersalnym, interaktywnym menu w terminalu.
    """
    try:
        selected_index = next(i for i, item in enumerate(menu_items) if item[1] is not None)
    except StopIteration:
        selected_index = 0

    def generate_panel(sel_idx: int) -> Panel:
        menu_text = Text(justify="center")
        for i, (text, action) in enumerate(menu_items):
            if action is None:
                menu_text.append(f"\n[dim bold]{text}[/dim bold]\n\n")
                continue
            style = "bold black on white" if i == sel_idx else ""
            prefix = "» " if i == sel_idx else "  "
            menu_text.append(Text.from_markup(f"{prefix}{text}\n", style=style))

        if subtitle:
            final_content = Group(
                Align.center(menu_text, vertical="middle"),
                Align.center(Text.from_markup(f"\n{subtitle}", style="dim"))
            )
        else:
            final_content = Align.center(menu_text, vertical="middle")
        return Panel(final_content, title=f"[bold]{title}[/bold]", border_style=border_style)

    with Live(generate_panel(selected_index), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_panel(selected_index), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue

            if key == "UP":
                original_index = selected_index
                # --- POCZĄTEK POPRAWKI ---
                while True:
                    selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                    if menu_items[selected_index][1] is not None:
                        break
                    if selected_index == original_index:
                        break
                # --- KONIEC POPRAWKI ---
            elif key == "DOWN":
                original_index = selected_index
                # --- POCZĄTEK POPRAWKI ---
                while True:
                    selected_index = (selected_index + 1) % len(menu_items)
                    if menu_items[selected_index][1] is not None:
                        break
                    if selected_index == original_index:
                        break
                # --- KONIEC POPRAWKI ---
            elif key.upper() in ["Q", "ESC"]:
                return None
            elif key == "ENTER":
                _, selected_value = menu_items[selected_index]
                return selected_value

# ##############################################################################
# ===            SEKCJA 7: UNIWERSALNE NARZĘDZIA WALIDACYJNE                 ===
# ##############################################################################
def check_dependency(module_name: str, package_name: str, friendly_name: str) -> bool:
    try: __import__(module_name); return True
    except ImportError:
        console = Console()
        console.print(Panel(f"[bold red]Błąd: Brak zależności '{friendly_name}'![/bold red]\n\nUruchom: [cyan]pip install {package_name}[/cyan]", title="Brak Zależności", border_style="red"))
        return False

def create_side_by_side_comparison_panel(
    item_a_details: dict,
    item_b_details: dict,
    title_a: str = "Plik A",
    title_b: str = "Plik B",
    is_a_selected: bool = True
) -> Panel:
    """
    Tworzy uniwersalny panel Rich do porównywania dwóch elementów obok siebie.

    Args:
        item_a_details (dict): Słownik z danymi do wyświetlenia dla elementu A.
        item_b_details (dict): Słownik z danymi do wyświetlenia dla elementu B.
        title_a (str): Tytuł dla panelu A.
        title_b (str): Tytuł dla panelu B.
        is_a_selected (bool): Wskazuje, który element jest aktualnie "wybrany".

    Returns:
        Panel: Gotowy do wyświetlenia obiekt Panel z porównaniem.
    """
    from rich.layout import Layout
    from rich.align import Align
    from rich.text import Text

    layout = Layout()
    layout.split_row(Layout(name="left"), Layout(name="right"))

    items = [(item_a_details, title_a), (item_b_details, title_b)]
    
    for i, (details, title) in enumerate(items):
        is_selected = (i == 0 and is_a_selected) or (i == 1 and not is_a_selected)
        
        status_text = Text("⭐ ZACHOWAJ", style="bold green") if is_selected else Text("🗑️ Usuń", style="dim")
        border_style = "green" if is_selected else "default"

        table = Table.grid(expand=True, padding=(0, 1))
        table.add_column(style="cyan", justify="right", width=15)
        table.add_column()
        
        # Iterujemy po słowniku, aby dynamicznie budować tabelę
        for key, value in details.items():
            # Prosta logika formatowania dla czytelności
            if key.lower() == "separator":
                table.add_row("─" * 15, "─" * 30)
            else:
                table.add_row(f"{key}:", str(value))

        panel_content = Group(Align.center(status_text), table)
        
        target_layout = layout["left" if i == 0 else "right"]
        target_layout.update(Panel(panel_content, title=title, border_style=border_style))

    return Panel(layout)

