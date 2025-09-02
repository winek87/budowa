#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# plik: uruchom.py
# Wersja 8.0 - Dodano dedykowaną komendę backupu rdzenia aplikacji
# Wersja 7.0 - Rozdzielenie komend backupu na dane i projekt
#
# ##############################################################################
# ===                     GŁÓWNY PLIK URUCHOMIAJĄCY (LAUNCHER)               ===
# ##############################################################################

# --- GŁÓWNE IMPORTY ---
import os
import sys
import importlib
import argparse
import json
from pathlib import Path
import asyncio
import logging
import shutil

# --- IMPORTY Z BIBLIOTEKI `rich` ---
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.traceback import Traceback
    from rich.prompt import Confirm
    from rich.logging import RichHandler
except ImportError:
    print("BŁĄD KRYTYCZNY: Biblioteka 'rich' nie jest zainstalowana.")
    print("Proszę ją zainstalować za pomocą komendy: pip install rich")
    sys.exit(1)

# --- IMPORTY Z MODUŁÓW APLIKACJI ---
try:
    from core import backup_logic
    from core.database import setup_database
except ImportError:
    print("BŁĄD KRYTYCZNY: Nie można zaimportować modułów z folderu 'core'.")
    print("Upewnij się, że uruchamiasz skrypt z głównego folderu projektu.")
    sys.exit(1)

# --- INICJALIZACJA I KONFIGURACJA MODUŁU ---
console = Console(highlight=True)
PROJECT_ROOT = Path(__file__).resolve().parent
APP_DATA_DIR = PROJECT_ROOT / "app_data"
SETTINGS_FILE = APP_DATA_DIR / "settings.json"
LOG_DIR = APP_DATA_DIR / "dziennik"
APP_DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level="INFO",
    format="%(asctime)s - [%(levelname)s] - (%(module)s) - %(message)s",
    handlers=[
        RichHandler(console=console, rich_tracebacks=True, show_path=False, markup=True),
        logging.FileHandler(LOG_DIR / "launcher.log", mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

SOURCE_DIRECTORIES = ['core']
APP_ENTRY_POINT_MODULE = 'start'
APP_ENTRY_POINT_FUNCTION = 'main'

# ##############################################################################
# ===                   SEKCJA 1: ZARZĄDZANIE USTAWIENIAMI                   ===
# ##############################################################################

def _read_settings() -> dict:
    if not SETTINGS_FILE.exists():
        logger.warning(f"Plik '{SETTINGS_FILE.name}' nie istnieje. Tworzę go z domyślnymi wartościami.")
        default_settings = {"LOG_ENABLED": True, "AUTO_BACKUP_ON_START": False}
        _write_settings(default_settings)
        return default_settings
    try:
        with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.critical(f"Błąd odczytu pliku '{SETTINGS_FILE.name}': {e}. Używam domyślnych ustawień.", exc_info=True)
        return {}

def _write_settings(settings: dict) -> bool:
    try:
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(settings, f, indent=4, ensure_ascii=False)
        logger.info(f"Pomyślnie zaktualizowano plik ustawień '{SETTINGS_FILE.name}'.")
        return True
    except IOError as e:
        logger.critical(f"Błąd zapisu do pliku '{SETTINGS_FILE.name}': {e}", exc_info=True)
        return False

def toggle_setting(key: str, enable: bool, title: str, on_text: str, off_text: str, color: str):
    action_text = "Włączam" if enable else "Wyłączam"
    logger.info(f"Próba zmiany ustawienia '{key}' na '{enable}'.")
    console.print(Panel(f"[bold {color}]⚙️ {action_text} {title} ⚙️[/]", border_style=color))
    settings = _read_settings()
    settings[key] = enable
    if _write_settings(settings):
        console.print(f"[green]✅ {on_text}[/]" if enable else f"[green]✅ {off_text}[/]")
    else:
        console.print(f"[bold red]Błąd: Nie udało się zapisać zmian w pliku '{SETTINGS_FILE.name}'.[/]")

def toggle_logging(enable: bool):
    toggle_setting("LOG_ENABLED", enable, "Globalne Logowanie", "Logowanie zostało [bold]WŁĄCZONE[/].", "Logowanie zostało [bold]WYŁĄCZONE[/].", "yellow")

def toggle_auto_backup(enable: bool):
    toggle_setting("AUTO_BACKUP_ON_START", enable, "Automatyczny Backup Danych", "Automatyczny backup danych przy starcie został [bold]WŁĄCZONY[/].", "Automatyczny backup danych przy starcie został [bold]WYŁĄCZONY[/].", "blue")

# ##############################################################################
# ===                   SEKCJA 2: NARZĘDZIA PROJEKTU                         ===
# ##############################################################################

def create_project_backup():
    """Wywołuje logikę tworzenia kopii zapasowej KODU PROJEKTU."""
    try:
        asyncio.run(backup_logic.create_full_project_backup())
    except Exception as e:
        logger.critical(f"Wystąpił błąd podczas tworzenia kopii zapasowej projektu: {e}", exc_info=True)
        console.print(f"[bold red]Błąd: Nie udało się uruchomić procesu tworzenia kopii zapasowej projektu.[/]")

def create_data_backup():
    """Wywołuje logikę tworzenia kopii zapasowej DANYCH UŻYTKOWNIKA."""
    try:
        asyncio.run(backup_logic.create_data_backup())
    except Exception as e:
        logger.critical(f"Wystąpił błąd podczas tworzenia kopii zapasowej danych: {e}", exc_info=True)
        console.print(f"[bold red]Błąd: Nie udało się uruchomić procesu tworzenia kopii zapasowej danych.[/]")

def create_core_backup():
    """Wywołuje logikę tworzenia kopii zapasowej RDZENIA APLIKACJI."""
    try:
        asyncio.run(backup_logic.create_core_app_backup())
    except Exception as e:
        logger.critical(f"Błąd podczas tworzenia kopii rdzenia aplikacji: {e}", exc_info=True)

def clean_project(confirm: bool = True):
    logger.info("Rozpoczynam proces sprzątania projektu z katalogów __pycache__.")
    console.print(Panel("[bold yellow]🧹 Sprzątanie Projektu 🧹[/bold yellow]", border_style="yellow"))
    pycache_dirs = list(PROJECT_ROOT.rglob('__pycache__'))
    if not pycache_dirs:
        console.print("[green]✅ Projekt jest już czysty.[/green]")
        return
    if confirm and not Confirm.ask(f"[bold yellow]Znaleziono {len(pycache_dirs)} katalogów __pycache__. Czy na pewno chcesz je usunąć?[/]", default=True):
        console.print("[red]Przerwano operację.[/red]")
        return
    deleted_count = 0
    with console.status("[bold yellow]Usuwanie...[/]"):
        for d in pycache_dirs:
            try:
                shutil.rmtree(d)
                deleted_count += 1
            except OSError as e:
                logger.error(f"Błąd podczas usuwania katalogu {d}: {e}", exc_info=True)
    console.print(f"\n[green]✅ Usunięto [bold cyan]{deleted_count}[/bold cyan] z {len(pycache_dirs)} katalogów.[/green]")

# ##############################################################################
# ===                   SEKCJA 3: WERYFIKACJA I URUCHAMIANIE APLIKACJI       ===
# ##############################################################################

def verify_imports_and_dependencies():
    logger.info("Rozpoczynam weryfikację integralności modułów.")
    console.print("\n[bold]Weryfikacja integralności modułów...[/bold]")
    
    def path_to_module_name(file_path: Path) -> str:
        return str(file_path.relative_to(PROJECT_ROOT).with_suffix('')).replace(os.sep, '.')

    modules_to_check = []
    for dir_name in SOURCE_DIRECTORIES:
        for path in (PROJECT_ROOT / dir_name).rglob('*.py'):
            if path.name != '__init__.py':
                modules_to_check.append(path_to_module_name(path))
    modules_to_check.append(APP_ENTRY_POINT_MODULE)

    with console.status("[green]Sprawdzanie...[/]"):
        for module_name in modules_to_check:
            try:
                importlib.import_module(module_name)
            except Exception:
                console.print(f"\n❌ [bold red]Błąd krytyczny podczas importu modułu:[/bold red] [cyan]{module_name}[/cyan]")
                console.print(Traceback(width=120))
                sys.exit(1)
    console.print("✅ [bold green]Wszystkie moduły i zależności zweryfikowane pomyślnie.[/bold green]")

def run_main_application():
    logger.info(f"Uruchamiam główny punkt wejścia: {APP_ENTRY_POINT_MODULE}.{APP_ENTRY_POINT_FUNCTION}()")
    try:
        app_module = importlib.import_module(APP_ENTRY_POINT_MODULE)
        start_function = getattr(app_module, APP_ENTRY_POINT_FUNCTION)
        asyncio.run(start_function())
    except (ImportError, AttributeError):
        logger.critical("Błąd krytyczny: nie można znaleźć punktu wejścia aplikacji.", exc_info=True)
        console.print(f"\n[bold red]BŁĄD KRYTYCZNY: Nie można uruchomić aplikacji![/bold red]")
        console.print(f"Problem z punktem wejścia: [cyan]{APP_ENTRY_POINT_MODULE}.{APP_ENTRY_POINT_FUNCTION}[/cyan]")
        console.print(Traceback(width=120))
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("Główna aplikacja przerwana przez KeyboardInterrupt.")

def run_verification_and_app():
    logger.info("Rozpoczynam procedurę uruchomienia aplikacji ('run').")
    settings = _read_settings()
    if settings.get("AUTO_BACKUP_ON_START", False):
        logger.info("Wykonywanie automatycznej kopii zapasowej DANYCH przy starcie.")
        create_core_backup()
        create_data_backup()
    verify_imports_and_dependencies()
    run_main_application()

# ##############################################################################
# ===                   SEKCJA 4: GŁÓWNY PUNKT WEJŚCIA I OBSŁUGA KOMEND      ===
# ##############################################################################

def main():
    parser = argparse.ArgumentParser(description="Launcher dla Google Photos Toolkit.", formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest='command', help='Dostępne komendy', required=False)

    subparsers.add_parser('run', help='Weryfikuje i uruchamia główną aplikację (domyślna akcja).')
    subparsers.add_parser('clean', help='Usuwa wszystkie katalogi __pycache__ z projektu.')
    
    # --- POCZĄTEK ZMIAN ---
    subparsers.add_parser('backup-core', help='Tworzy kopię zapasową RDZENIA APLIKACJI (core, start.py, uruchom.py).')
    subparsers.add_parser('backup-project', help='Tworzy kopię zapasową KODU ŹRÓDŁOWEGO projektu (.tar.gz).')
    subparsers.add_parser('backup-data', help='Tworzy kopię zapasową DANYCH UŻYTKOWNIKA (baza, sesja) (.zip).')
    # --- KONIEC ZMIAN ---

    subparsers.add_parser('backup-on', help='Włącza automatyczny backup DANYCH przy starcie.')
    subparsers.add_parser('backup-off', help='Wyłącza automatyczny backup DANYCH przy starcie.')
    subparsers.add_parser('logs-on', help='Włącza globalnie system logowania.')
    subparsers.add_parser('logs-off', help='Wyłącza globalnie system logowania.')

    args = parser.parse_args()
    command = args.command or 'run'
    logger.info(f"Otrzymano polecenie: '{command}'")

    if command != 'run':
        logger.debug("Uruchamianie wstępnej inicjalizacji bazy danych...")
        asyncio.run(setup_database())

    try:
        action_map = {
            'run': run_verification_and_app,
            'clean': clean_project,
            'backup-core': create_core_backup,
            'backup-project': create_project_backup,
            'backup-data': create_data_backup,
            'logs-on': lambda: toggle_logging(True),
            'logs-off': lambda: toggle_logging(False),
            'backup-on': lambda: toggle_auto_backup(True),
            'backup-off': lambda: toggle_auto_backup(False),
        }
        if command in action_map:
            action_map[command]()
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Przerwano przez użytkownika.[/bold yellow]")
        logger.warning("Launcher przerwany przez KeyboardInterrupt.")
        sys.exit(0)
    finally:
        if command == 'run':
            logger.info("Aplikacja zakończyła działanie. Rozpoczynam automatyczne sprzątanie.")
            clean_project(confirm=False)

if __name__ == "__main__":
    main()
