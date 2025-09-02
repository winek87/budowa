# -*- coding: utf-8 -*-

# plik: core/doctor_logic.py
# Wersja 8.3 - Scentralizowana walidacja zależności
# Wersja 8.2 - Stabilna i Niezawodna Logika Wywoływania Testów
#
# ##############################################################################
# ===                        MODUŁ DIAGNOSTYKI SYSTEMU                       ===
# ##############################################################################
#
# "Doktor" to zaawansowane narzędzie diagnostyczne, które przeprowadza
# kompleksowy "bilans zdrowia" całej aplikacji. Sprawdza wszystko: od
# poprawności struktury plików, przez dostępność zależności i połączenie
# z internetem, aż po integralność bazy danych i ważność sesji logowania.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import os
import sys
import subprocess
import socket
import logging
from pathlib import Path
from collections import deque
from functools import partial

# --- Importy asynchroniczne ---
import aiosqlite
from playwright.async_api import async_playwright

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text
from rich.live import Live
from rich.layout import Layout
from rich.align import Align
from rich.table import Table

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from . import config as core_config
from .utils import LogCollectorHandler, check_dependency
from .config_editor_logic import get_key

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)

# ##############################################################################
# ===                     SEKCJA 1: DEFINICJE KLAS I WYJĄTKÓW                ===
# ##############################################################################

class DoctorCheckError(Exception):
    """
    Wyjątek używany, gdy test diagnostyczny zakończy się niepowodzeniem
    wskazującym na krytyczny błąd.

    Jego przechwycenie przez główną pętlę "Doktora" jest sygnałem, że
    aplikacja może nie działać poprawnie i problem powinien być oznaczony
    jako "BŁĄD".
    """
    pass

class DoctorInfo(Exception):
    """
    Wyjątek używany, gdy test diagnostyczny chce zwrócić ważną informację
    lub ostrzeżenie, które nie jest błędem krytycznym.

    Jego przechwycenie jest sygnałem, że aplikacja będzie działać, ale
    użytkownik powinien zwrócić uwagę na pewien aspekt (np. brak opcjonalnej
    zależności, pusta sesja).
    """
    pass

# ##############################################################################
# ===                     SEKCJA 2: INDYWIDUALNE TESTY DIAGNOSTYCZNE         ===
# ##############################################################################

def check_project_structure():
    """
    Sprawdza, czy struktura plików projektu jest zgodna z architekturą.
    """
    logger.info("Uruchamiam test struktury projektu...")
    core_path = Path("core")

    if not core_path.is_dir():
        raise DoctorCheckError(f"Krytyczny błąd: Nie znaleziono folderu '{core_path}'. Aplikacja nie może działać.")

    required_files = ["config.py", "database.py", "utils.py", "menu_logic.py", "master_logic.py"]
    missing_files = [f for f in required_files if not (core_path / f).is_file()]
    if missing_files:
        raise DoctorCheckError(f"Brak kluczowych plików w folderze 'core': {', '.join(missing_files)}")

    obsolete_files = ["logic.py", "unified_logic.py", "robust_logic.py"]
    found_obsolete = [f for f in obsolete_files if (core_path / f).is_file()]
    if found_obsolete:
        raise DoctorInfo(f"Wykryto przestarzałe pliki silników. Zalecane usunięcie: {', '.join(found_obsolete)}")

    logger.info("Test struktury projektu zakończony pomyślnie.")
    return "Struktura folderów i kluczowych plików jest poprawna."

def check_dependencies():
    """
    Sprawdza, czy wszystkie kluczowe biblioteki Python są zainstalowane.
    Używa scentralizowanej funkcji z `utils.py`.
    """
    logger.info("Uruchamiam test zależności Python...")

    # ZMIANA: Cała stara logika została zastąpiona.
    
    # Podstawowe, wymagane zależności
    core_deps = [
        ('rich', 'rich', 'Rich'),
        ('playwright', 'playwright', 'Playwright'),
        ('aiosqlite', 'aiosqlite', 'aiosqlite')
    ]
    for module, package, name in core_deps:
        if not check_dependency(module, package, name):
            # check_dependency sama wyświetli błąd, tutaj tylko go rzucamy, aby zatrzymać Doktora
            raise DoctorCheckError(f"Brak kluczowej biblioteki: '{name}'.")

    # Opcjonalne zależności
    optional_deps = [
        ('geopy', 'geopy', 'Geopy'),
        ('imagehash', 'imagehash', 'ImageHash'),
        ('cv2', 'opencv-python', 'OpenCV'),
        ('wordcloud', 'wordcloud', 'WordCloud'),
        ('transformers', 'transformers', 'Transformers (Hugging Face)')
    ]
    missing_optional = [name for module, package, name in optional_deps if not check_dependency(module, package, name)]

    if missing_optional:
        # Zmieniamy DoctorInfo, aby nie wyświetlać panelu z błędem, bo check_dependency już to zrobiło.
        # Wystarczy informacja tekstowa w tabeli Doktora.
        raise DoctorInfo(f"Brak opcjonalnych bibliotek: {', '.join(missing_optional)}. Niektóre narzędzia mogą nie działać.")

    return "Wszystkie kluczowe biblioteki Python są dostępne."

def check_dependencies_bak():
    """
    Sprawdza, czy wszystkie kluczowe biblioteki Python są zainstalowane.
    """
    logger.info("Uruchamiam test zależności Python...")

    core_deps = [('rich', 'rich'), ('playwright', 'playwright'), ('aiosqlite', 'aiosqlite')]
    for module_name, package_name in core_deps:
        try:
            __import__(module_name)
        except ImportError:
            raise DoctorCheckError(f"Brak biblioteki: '{package_name}'. Uruchom 'pip install {package_name}'.")

    optional_deps = {
        'geopy': 'geopy', 'imagehash': 'imagehash', 'cv2': 'opencv-python',
        'wordcloud': 'wordcloud', 'transformers': 'transformers'
    }

    missing_optional = []
    for module_name, package_name in optional_deps.items():
        try:
            __import__(module_name)
        except ImportError:
            missing_optional.append(package_name)
            
    if missing_optional:
        raise DoctorInfo(f"Brak opcjonalnych bibliotek: {', '.join(missing_optional)}. Niektóre narzędzia mogą nie działać.")
        
    logger.info("Test zależności Python zakończony pomyślnie.")
    return "Wszystkie kluczowe biblioteki Python są dostępne."


def check_network_connectivity():
    """
    Sprawdza, czy istnieje aktywne połączenie z kluczowymi serwerami Google.
    """
    logger.info("Uruchamiam test połączenia sieciowego z serwerami Google...")
    
    hosts_to_check = ["photos.google.com", "googleusercontent.com", "accounts.google.com"]
    
    for host in hosts_to_check:
        try:
            socket.create_connection((host, 443), timeout=5)
        except (socket.timeout, socket.gaierror, OSError) as e:
            error_message = f"Brak połączenia z '{host}'. Sprawdź połączenie z internetem lub ustawienia firewall. Błąd: {e}"
            raise DoctorCheckError(error_message)
            
    logger.info("Test połączenia sieciowego zakończony pomyślnie.")
    return "Połączenie z serwerami Google jest aktywne."


def check_playwright_browsers(config_module):
    """
    Sprawdza, czy przeglądarka zdefiniowana w `config.py` jest zainstalowana.
    """
    browser_type = getattr(config_module, 'BROWSER_TYPE', 'chromium')
    logger.info(f"Uruchamiam weryfikację instalacji przeglądarki Playwright: '{browser_type}'...")
    
    try:
        command = [sys.executable, "-m", "playwright", "install", "--with-deps", browser_type]
        proc = subprocess.run(command, capture_output=True, text=True, timeout=180, check=False, encoding='utf-8')
        
        if proc.returncode != 0 and "is already installed" not in proc.stdout.lower():
            raise DoctorCheckError(f"Nie udało się zainstalować/zweryfikować przeglądarki '{browser_type}'. Błąd: {proc.stderr[:250]}")
            
        return f"Przeglądarka '{browser_type}' jest zainstalowana."
        
    except FileNotFoundError:
        raise DoctorCheckError("Nie można uruchomić Playwright. Czy na pewno jest zainstalowany (`pip install playwright`)?")
    except subprocess.TimeoutExpired:
        raise DoctorCheckError("Instalacja przeglądarki trwała zbyt długo (> 3 min). Sprawdź połączenie z internetem.")
    except Exception as e:
        raise DoctorCheckError(f"Wystąpił nieoczekiwany błąd podczas weryfikacji instalacji Playwright: {e}")


def check_exiftool_program():
    """
    Sprawdza, czy program ExifTool jest zainstalowany i dostępny w systemie.
    """
    logger.info("Uruchamiam weryfikację programu ExifTool...")
    try:
        startupinfo = None
        if os.name == 'nt':
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            
        result = subprocess.run(['exiftool', '-ver'], capture_output=True, text=True, check=True, startupinfo=startupinfo, encoding='utf-8')
        version = result.stdout.strip()
        return f"Program ExifTool jest zainstalowany (wersja: {version})."
        
    except FileNotFoundError:
        raise DoctorCheckError("Program 'exiftool' nie został znaleziony. Zainstaluj go i upewnij się, że jest dodany do systemowej zmiennej PATH.")
    except subprocess.CalledProcessError as e:
        raise DoctorCheckError(f"Polecenie 'exiftool' zakończyło się błędem. Błąd: {e.stderr}")
    except Exception as e:
        raise DoctorCheckError(f"Wystąpił nieoczekiwany błąd podczas uruchamiania 'exiftool': {e}")


def check_config_completeness_and_sanity(config_module):
    """
    Sprawdza kompletność i podstawową poprawność wartości w `core/config.py`.
    """
    logger.info("Uruchamiam test kompletności i poprawności konfiguracji...")
    
    required_vars = ['SESSION_DIR', 'START_URL', 'DATABASE_FILE', 'DOWNLOADS_DIR_BASE']
    missing_vars = [var for var in required_vars if not hasattr(config_module, var) or not getattr(config_module, var)]
    
    if missing_vars:
        raise DoctorCheckError(f"Brakujące lub puste kluczowe zmienne w config.py: {', '.join(missing_vars)}. Uzupełnij je.")
        
    if not config_module.START_URL.startswith("https://photos.google.com/photo/"):
        raise DoctorCheckError("Zmienna START_URL w config.py nie wygląda na prawidłowy link do pojedynczego zdjęcia.")
        
    return "Plik konfiguracyjny jest kompletny i poprawny."


def check_permissions_and_performance(config_module):
    """
    Sprawdza uprawnienia do zapisu w kluczowych folderach.
    """
    logger.info("Uruchamiam test uprawnień do zapisu w kluczowych folderach...")
    
    dirs_to_check = [
        config_module.SESSION_DIR,
        config_module.DOWNLOADS_DIR_BASE,
        Path(config_module.DATABASE_FILE).parent
    ]
    
    for dir_path in dirs_to_check:
        try:
            p = Path(dir_path)
            p.mkdir(parents=True, exist_ok=True)
            test_file = p / f"doctor_permission_test_{os.getpid()}.tmp"
            with open(test_file, "wb") as f:
                f.write(os.urandom(128))
            test_file.unlink()
        except Exception as e:
            raise DoctorCheckError(f"Brak uprawnień do zapisu w folderze '{p}'. Sprawdź uprawnienia systemu plików. Błąd: {e}")
            
    return "Program ma uprawnienia do zapisu w kluczowych folderach."

async def check_database_schema_and_integrity(config_module):
    """
    Sprawdza integralność i strukturę bazy danych.
    """
    logger.info("Uruchamiam test integralności i schematu bazy danych...")
    db_path = Path(config_module.DATABASE_FILE)
    
    if not await asyncio.to_thread(db_path.exists):
        raise DoctorInfo("Plik bazy danych jeszcze nie istnieje. Zostanie utworzony przy pierwszym skanie.")
    
    try:
        async with aiosqlite.connect(db_path) as conn:
            cursor = await conn.execute("PRAGMA integrity_check;")
            integrity_result = await cursor.fetchone()
            if not integrity_result or integrity_result[0].lower() != 'ok':
                raise DoctorCheckError(f"Baza danych jest uszkodzona! Wynik: {integrity_result[0] if integrity_result else 'Brak'}.")
            
            required_cols = {'id', 'url', 'status', 'final_path', 'expected_path', 'metadata_json', 'source'}
            cursor = await conn.execute("PRAGMA table_info(downloaded_media)")
            existing_columns = {info[1] for info in await cursor.fetchall()}
            
            if missing_cols := required_cols - existing_columns:
                raise DoctorCheckError(f"Struktura bazy danych jest nieaktualna. Brakuje kolumn: {', '.join(missing_cols)}.")

        return "Baza danych jest spójna i ma aktualną strukturę."
    except aiosqlite.Error as e:
        raise DoctorCheckError(f"Błąd bazy danych: {e}")


async def check_session_validity(config_module):
    """
    Sprawdza, czy zapisana sesja logowania do Google jest wciąż aktywna.
    """
    logger.info("Uruchamiam test ważności sesji logowania...")
    session_path = Path(config_module.SESSION_DIR)

    if not await asyncio.to_thread(session_path.exists) or not any(await asyncio.to_thread(os.listdir, session_path)):
        raise DoctorInfo(f"Folder sesji '{config_module.SESSION_DIR}' jest pusty. Odśwież sesję w menu głównym.")

    try:
        # "async with" zarządza teraz całym cyklem życia przeglądarki i kontekstu.
        async with async_playwright() as p:
            context = await getattr(p, config_module.BROWSER_TYPE).launch_persistent_context(
                session_path.expanduser(),
                headless=False,
                args=config_module.BROWSER_ARGS.get(config_module.BROWSER_TYPE)
            )
            page = await context.new_page()
            await page.goto("https://photos.google.com/", timeout=20000)
            await page.wait_for_selector("input[aria-label^='Wyszukaj']", timeout=15000)
            
            # Nie ma potrzeby ręcznego zamykania `context`, `async with` zrobi to za nas.
            
            logger.info("Test sesji zakończony pomyślnie.")
            return "Zapisana sesja logowania jest aktywna."

    except Exception as e:
        # Przechwyć każdy błąd (Timeout, błąd Playwright, itp.) jako dowód nieprawidłowej sesji
        error_message = f"Sesja logowania wygasła lub jest nieprawidłowa. Odśwież ją w menu głównym. Błąd: {str(e)[:150]}"
        logger.error(error_message)
        raise DoctorCheckError(error_message)

# ##############################################################################
# ===                   SEKCJA 3: GŁÓWNA FUNKCJA URUCHOMIENIOWA              ===
# ##############################################################################

async def run_doctor():
    """
    Tworzy i uruchamia interaktywny interfejs diagnostyczny "Doktor".
    """
    console.clear()
    logger.info("Uruchamiam Diagnostykę Systemu (Doktor)...")
    console.print(Panel("🩺 Diagnostyka Systemu (Doktor) 🩺", expand=False, style="bold blue"))

    test_definitions = [
        ("Struktura projektu", check_project_structure, False),
        ("Zależności (Python)", check_dependencies, False),
        ("Połączenie sieciowe", check_network_connectivity, False),
        ("Zależności (ExifTool)", check_exiftool_program, False),
        ("Zależności (Playwright)", partial(check_playwright_browsers, core_config), False),
        ("Kompletność Konfiguracji", partial(check_config_completeness_and_sanity, core_config), False),
        ("Uprawnienia i wydajność I/O", partial(check_permissions_and_performance, core_config), False),
        ("Struktura Bazy Danych", partial(check_database_schema_and_integrity, core_config), True),
        ("Ważność sesji logowania", partial(check_session_validity, core_config), True),
    ]

    live_logs = deque(maxlen=15)
    log_collector = LogCollectorHandler(live_logs)
    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]
    root_logger.handlers.clear()
    root_logger.addHandler(log_collector)

    step_statuses = {name: "[dim]Oczekuje...[/dim]" for name, _, _ in test_definitions}
    overall_ok = True
    current_step_name = ""

    def generate_live_layout() -> Layout:
        """Dynamicznie tworzy layout interfejsu na żywo."""
        status_table = Table.grid(padding=(0, 2))
        status_table.add_column("Test Diagnostyczny", style="cyan", no_wrap=True, width=35)
        status_table.add_column("Status / Wynik", style="white")

        for name, _, _ in test_definitions:
            status_text = step_statuses[name]
            if name == current_step_name:
                status_text = f"[bold yellow][ Sprawdzam... ][/bold yellow] {console.render_str(':hourglass:')}"
            status_table.add_row(name, Text.from_markup(status_text))

        logs_panel = Panel(Group(*live_logs), title="Logi na Żywo", border_style="green")

        layout = Layout(name="root")
        layout.split(
            Layout(Panel(Align.center(status_table)), name="header", size=len(test_definitions) + 2),
            Layout(logs_panel, name="body", ratio=1),
        )
        return layout

    # --- Uruchomienie testów z `rich.Live` ---
    with Live(generate_live_layout(), screen=True, auto_refresh=False, transient=True, vertical_overflow="visible") as live:
        for name, func, is_async in test_definitions:
            current_step_name = name
            live.update(generate_live_layout(), refresh=True)
            await asyncio.sleep(0.2)

            try:
                if is_async:
                    details = await func()
                else:
                    details = await asyncio.to_thread(func)

                step_statuses[name] = f"[bold green]✅ OK[/]\n[dim]{details}[/dim]"
            except DoctorInfo as e:
                step_statuses[name] = f"[bold cyan]ℹ️ INFO[/]\n[dim]{e}[/dim]"
            except DoctorCheckError as e:
                step_statuses[name] = f"[bold red]❌ BŁĄD[/]\n[dim]{e}[/dim]"
                overall_ok = False
            except Exception as e:
                step_statuses[name] = f"[bold white on red]💥 KRYTYCZNY BŁĄD[/]\n[dim]{e}[/dim]"
                overall_ok = False

            live.update(generate_live_layout(), refresh=True)

        current_step_name = ""
        live.update(generate_live_layout(), refresh=True)

        logger.info("Diagnostyka zakończona. Oczekuję na interakcję użytkownika.")
        await asyncio.to_thread(get_key)

    # --- Sprzątanie i podsumowanie ---
    root_logger.removeHandler(log_collector)
    for h in original_handlers:
        root_logger.addHandler(h)

    console.clear()
    if overall_ok:
        console.print(Panel("✅ [bold green]Diagnostyka zakończona. System w pełni sprawny![/]", border_style="green"))
    else:
        console.print(Panel("⚠️ [bold red]Diagnostyka wykryła problemy! Sprawdź szczegóły w logach i powyższej tabeli.[/]", border_style="red"))

