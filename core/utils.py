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
from typing import List, Set, Dict, Any, Optional, Tuple
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
import tempfile
from io import StringIO, BytesIO
from contextlib import redirect_stdout, suppress

# --- NOWE IMPORTY DLA NIEBLOKUJĄCEJ WERSJI ---
try:
    import termios
    import tty
    import select
    IS_POSIX = True
except ImportError:
    IS_POSIX = False
    import msvcrt # Dla Windows

# --- Playwright (dla type hintów) ---
from playwright.async_api import Page

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, ConsoleOptions, RenderResult, Group
#from rich.console import Console, Group
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


# --- Funkcja 2: Nowa, NIEBLOKUJĄCA (dla skanera) ---
_original_terminal_settings = None

def setup_raw_terminal():
    """Przełącza terminal w tryb surowy. Tylko dla skanera."""
    if not IS_POSIX: return
    global _original_terminal_settings
    try:
        _original_terminal_settings = termios.tcgetattr(sys.stdin)
        tty.setraw(sys.stdin.fileno())
    except termios.error:
        _original_terminal_settings = None

def restore_terminal():
    """Przywraca oryginalne ustawienia terminala. Tylko dla skanera."""
    if IS_POSIX and _original_terminal_settings:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, _original_terminal_settings)

def get_key_nonblocking() -> str | None:
    """
    Nieblokująca funkcja do odczytu klawisza. Zwraca znak lub None, jeśli nic nie naciśnięto.
    Używana WYŁĄCZNIE przez interaktywny dashboard skanera.
    """
    if IS_POSIX:
        if select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
            return sys.stdin.read(1).upper()
    else: # Windows
        if msvcrt.kbhit():
            return msvcrt.getch().decode().upper()
    return None

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

from io import BytesIO

try:
    import timg
    TIMG_AVAILABLE = True
except ImportError:
    TIMG_AVAILABLE = False

class ThumbnailRendererWersjaPierwsza:
    """
    Klasa renderująca miniaturę obrazu, kompatybilna z `rich.Live`.
    Używa `timg` do wyświetlania grafiki, z fallbackiem do tekstu, jeśli
    biblioteka jest niedostępna lub terminal nie wspiera grafiki.
    """
    def __init__(self, image_path: Optional[Path], max_size: Tuple[int, int] = (60, 30)):
        self.image_path = image_path
        self.max_size = max_size

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        if not TIMG_AVAILABLE or not self.image_path or not self.image_path.exists():
            yield Text("[Podgląd niedostępny]", justify="center", style="dim")
            return

        # Timg renderuje do stdout, więc musimy to przechwycić
        original_stdout = sys.stdout
        captured_output = BytesIO()
        sys.stdout = captured_output  # Przekieruj stdout

        try:
            # Stwórz obiekt renderujący
            obj = timg.Renderer()
            # Ustaw maksymalny rozmiar, aby nie zalać terminala
            obj.set_size(*self.max_size)
            # Renderuj obraz do naszego bufora
            obj.render_from_file(str(self.image_path))
        except Exception as e:
            # W razie błędu (np. nieobsługiwany terminal), przywróć stdout i zwróć tekst
            sys.stdout = original_stdout
            logger.warning(f"Błąd renderowania miniatury: {e}")
            yield Text("[Podgląd niedostępny]", justify="center", style="dim")
            return
        finally:
            # Zawsze przywracaj oryginalny stdout!
            sys.stdout = original_stdout

        # Pobierz wyrenderowany obraz jako tekst z escape codami
        output_str = captured_output.getvalue().decode('utf-8', errors='ignore')
        yield Text(output_str, justify="center")

async def run_exiftool_command(params: List[str]) -> Tuple[bool, str]:
    """
    Asynchronicznie uruchamia polecenie exiftool z podanymi parametrami.

    Funkcja jest zabezpieczona przed błędami, takimi jak brak zainstalowanego
    narzędzia ExifTool. Przechwytuje standardowe wyjście oraz wyjście błędu.

    Args:
        params (List[str]): Lista argumentów do przekazania do exiftool.
                            Nie dołączaj "exiftool" na początku listy.
                            Przykład: ['-overwrite_original', '-Subject=tag', 'sciezka/do/pliku.jpg']

    Returns:
        Tuple[bool, str]: Krotka zawierająca:
                          - bool: True, jeśli polecenie zakończyło się sukcesem (kod wyjścia 0), w przeciwnym razie False.
                          - str: Przechwycone standardowe wyjście (w przypadku sukcesu)
                                 lub wyjście błędu (w przypadku porażki).
    """
    command_str = ' '.join(['exiftool'] + params)
    logger.debug(f"Uruchamiam Exiftool z poleceniem: {command_str}")

    try:
        process = await asyncio.create_subprocess_exec(
            "exiftool",
            *params,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout_bytes, stderr_bytes = await process.communicate()
        
        stdout = stdout_bytes.decode('utf-8', errors='ignore').strip()
        stderr = stderr_bytes.decode('utf-8', errors='ignore').strip()

        if process.returncode == 0:
            # Polecenie zakończyło się sukcesem
            if stderr:
                # Czasem ExifTool zwraca ostrzeżenia na stderr, nawet przy sukcesie
                logger.warning(f"Exiftool zwrócił ostrzeżenie: {stderr}")
            logger.debug(f"Exiftool zakończył pomyślnie. Wyjście: {stdout[:250]}...")
            return True, stdout
        else:
            # Polecenie zakończyło się błędem
            error_message = stderr if stderr else stdout
            logger.error(f"Błąd wykonania Exiftool (kod wyjścia: {process.returncode}): {error_message}")
            return False, error_message

    except FileNotFoundError:
        error_msg = "BŁĄD KRYTYCZNY: Program 'exiftool' nie został znaleziony."
        logger.critical(error_msg)
        console.print(f"[bold red]{error_msg}[/bold red]")
        console.print("Upewnij się, że ExifTool jest zainstalowany i dostępny w ścieżce systemowej (PATH).")
        console.print("Instrukcje instalacji: https://exiftool.org/install.html")
        return False, "Exiftool nie jest zainstalowany."
    except Exception as e:
        logger.critical("Nieoczekiwany błąd podczas uruchamiania Exiftool.", exc_info=True)
        return False, f"Wystąpił nieoczekiwany błąd: {e}"

async def run_exiftool_command(params: List[str]) -> Tuple[bool, str]:
    command_str = ' '.join(['exiftool'] + params)
    logger.debug(f"Uruchamiam Exiftool z poleceniem: {command_str}")
    try:
        process = await asyncio.create_subprocess_exec("exiftool", *params, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout_bytes, stderr_bytes = await process.communicate()
        stdout = stdout_bytes.decode('utf-8', errors='ignore').strip()
        stderr = stderr_bytes.decode('utf-8', errors='ignore').strip()
        if process.returncode == 0:
            if stderr: logger.warning(f"Exiftool zwrócił ostrzeżenie: {stderr}")
            logger.debug(f"Exiftool zakończył pomyślnie. Wyjście: {stdout[:250]}...")
            return True, stdout
        else:
            error_message = stderr if stderr else stdout
            logger.error(f"Błąd wykonania Exiftool (kod wyjścia: {process.returncode}): {error_message}")
            return False, error_message
    except FileNotFoundError:
        error_msg = "BŁĄD KRYTYCZNY: Program 'exiftool' nie został znaleziony."
        logger.critical(error_msg); console.print(f"[bold red]{error_msg}[/bold red]"); console.print("Upewnij się, że ExifTool jest zainstalowany i dostępny w ścieżce systemowej (PATH)."); return False, "Exiftool nie jest zainstalowany."
    except Exception as e:
        logger.critical("Nieoczekiwany błąd podczas uruchamiania Exiftool.", exc_info=True); return False, f"Wystąpił nieoczekiwany błąd: {e}"


# Zależność do sprawdzania wymiarów obrazu
try:
    from PIL import Image, UnidentifiedImageError
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# Sprawdzamy, czy polecenie 'timg' jest dostępne w systemie
TIMG_AVAILABLE = shutil.which("timg") is not None

class ThumbnailRenderer:
    """
    Renderuje miniaturę, inteligentnie skalując ją tak, aby zmieściła się
    w zdefiniowanej ramce (bounding box), zachowując proporcje obrazu.
    Używa Text.from_ansi() dla poprawnego renderowania w rich.Live.
    """
    def __init__(self, image_path: Optional[Path], max_size: Tuple[int, int] = (60, 30)):
        self.image_path = image_path
        self.max_size_chars = max_size

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        if not TIMG_AVAILABLE:
            yield Text("\n[dim]Podgląd niedostępny: Polecenie 'timg' nie jest zainstalowane.[/dim]", justify="center"); return
        if not PIL_AVAILABLE:
            yield Text("\n[dim]Podgląd niedostępny: Biblioteka 'Pillow' nie jest zainstalowana.[/dim]", justify="center"); return
        if not self.image_path or not self.image_path.exists():
            yield Text("\n[dim]Podgląd niedostępny: Nieprawidłowa ścieżka do pliku.[/dim]", justify="center"); return

        temp_file_path = None
        try:
            with Image.open(self.image_path) as img:
                pixel_box = (self.max_size_chars[0] * 8, self.max_size_chars[1] * 16)
                img.thumbnail(pixel_box, Image.Resampling.LANCZOS)
                
                with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                    temp_file_path = tmp.name
                    img.convert("RGB").save(tmp, format="JPEG", quality=90)

#            command = ["timg", "-r", "256", "-m", "a24h", "-s", str(self.max_size_chars[0]), temp_file_path]
            command = ["timg", "-s", str(self.max_size_chars[0]), temp_file_path]
            result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
            output_str = result.stdout
            
        except UnidentifiedImageError:
             yield Text(f"\n[bold red]Błąd podglądu:[/]\n[yellow]Plik '{self.image_path.name}' jest uszkodzony lub nieobsługiwany.[/yellow]", justify="center")
             return
        except Exception as e:
            logger.warning(f"Błąd renderowania miniatury: {e}")
            error_details = e.stderr if isinstance(e, subprocess.CalledProcessError) else str(e)
            yield Text(f"\n[bold red]Błąd renderowania podglądu:[/]\n[yellow]{error_details}[/yellow]", justify="center")
            return
        finally:
            if temp_file_path:
                with suppress(FileNotFoundError):
                    os.unlink(temp_file_path)
            
        # >>> OSTATECZNA POPRAWKA: Używamy Text.from_ansi() <<<
        # To gwarantuje, że kody sterujące z timg zostaną poprawnie zinterpretowane.
        yield Text.from_ansi(output_str, no_wrap=True)

