# plik: core/doctor/tasks.py
# Wersja 1.0 - Czysta logika biznesowa dla modułu Diagnostyki.
# Opis: Ten moduł zawiera wszystkie indywidualne testy diagnostyczne
#       wykonywane przez narzędzie "Doktor".
# -*- coding: utf-8 -*-

import asyncio
import os
import socket
import subprocess
import sys
import logging
from pathlib import Path

import aiosqlite
from playwright.async_api import async_playwright

from .utils import check_dependency

# Inicjalizacja
logger = logging.getLogger(__name__)

# Definicje wyjątków, które będą używane w cli.py
class DoctorCheckError(Exception): pass
class DoctorInfo(Exception): pass

# --- INDYWIDUALNE TESTY DIAGNOSTYCZNE ---

def check_project_structure():
    """Sprawdza, czy struktura plików projektu jest zgodna z architekturą."""
    logger.info("Uruchamiam test struktury projektu...")
    core_path = Path("core")
    if not core_path.is_dir():
        raise DoctorCheckError(f"Krytyczny błąd: Nie znaleziono folderu '{core_path}'.")
    
    required_files = ["config.py", "database.py", "utils.py", "menu_logic.py"]
    missing_files = [f for f in required_files if not (core_path / f).is_file()]
    if missing_files:
        raise DoctorCheckError(f"Brak kluczowych plików w 'core': {', '.join(missing_files)}")
    
    return "Struktura kluczowych plików jest poprawna."

def check_dependencies():
    """Sprawdza, czy wszystkie kluczowe biblioteki Python są zainstalowane."""
    logger.info("Uruchamiam test zależności Python...")
    core_deps = [('rich', 'rich', 'Rich'), ('playwright', 'playwright', 'Playwright'), ('aiosqlite', 'aiosqlite', 'aiosqlite')]
    for module, package, name in core_deps:
        if not check_dependency(module, package, name, silent=True):
            raise DoctorCheckError(f"Brak biblioteki: '{name}'. Uruchom 'pip install {package}'.")
            
    optional_deps = [('exiftool', 'pyexiftool', 'PyExifTool')]
    missing_optional = [name for module, package, name in optional_deps if not check_dependency(module, package, name, silent=True)]
    if missing_optional:
        raise DoctorInfo(f"Brak opcjonalnych bibliotek: {', '.join(missing_optional)}. Niektóre narzędzia mogą nie działać.")
        
    return "Wszystkie kluczowe biblioteki Python są dostępne."

def check_network_connectivity():
    """Sprawdza, czy istnieje aktywne połączenie z serwerami Google."""
    logger.info("Uruchamiam test połączenia sieciowego...")
    hosts_to_check = ["photos.google.com", "googleusercontent.com", "accounts.google.com"]
    for host in hosts_to_check:
        try:
            socket.create_connection((host, 443), timeout=5)
        except Exception as e:
            raise DoctorCheckError(f"Brak połączenia z '{host}'. Sprawdź internet lub firewall. Błąd: {e}")
    return "Połączenie z serwerami Google jest aktywne."

def check_playwright_browsers(config_module):
    """Sprawdza, czy przeglądarka zdefiniowana w `config.py` jest zainstalowana."""
    browser_type = getattr(config_module, 'BROWSER_TYPE', 'chromium')
    logger.info(f"Weryfikacja instalacji przeglądarki Playwright: '{browser_type}'...")
    try:
        command = [sys.executable, "-m", "playwright", "install", "--with-deps", browser_type]
        proc = subprocess.run(command, capture_output=True, text=True, timeout=180, check=False, encoding='utf-8')
        if proc.returncode != 0 and "is already installed" not in proc.stdout.lower():
            raise DoctorCheckError(f"Nie udało się zainstalować/zweryfikować '{browser_type}'. Błąd: {proc.stderr[:250]}")
        return f"Przeglądarka '{browser_type}' jest zainstalowana."
    except Exception as e:
        raise DoctorCheckError(f"Błąd podczas weryfikacji instalacji Playwright: {e}")

def check_exiftool_program():
    """Sprawdza, czy program ExifTool jest zainstalowany i dostępny w systemie."""
    logger.info("Weryfikacja programu ExifTool...")
    try:
        startupinfo = subprocess.STARTUPINFO() if os.name == 'nt' else None
        if os.name == 'nt': startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        result = subprocess.run(['exiftool', '-ver'], capture_output=True, text=True, check=True, startupinfo=startupinfo, encoding='utf-8')
        return f"Program ExifTool jest zainstalowany (wersja: {result.stdout.strip()})."
    except FileNotFoundError:
        raise DoctorInfo("Program 'exiftool' nie znaleziony. Niektóre funkcje (np. skaner offline) nie będą działać.")
    except Exception as e:
        raise DoctorCheckError(f"Błąd podczas uruchamiania 'exiftool': {e}")

def check_config_completeness(config_module):
    """Sprawdza kompletność i podstawową poprawność wartości w `core/config.py`."""
    logger.info("Test kompletności i poprawności konfiguracji...")
    required_vars = ['SESSION_DIR', 'START_URL', 'DOWNLOADS_DIR_BASE']
    missing_vars = [var for var in required_vars if not hasattr(config_module, var) or not getattr(config_module, var)]
    if missing_vars:
        raise DoctorCheckError(f"Brakujące zmienne w config.py: {', '.join(missing_vars)}.")
    if not config_module.START_URL.startswith("https://photos.google.com/photo/"):
        raise DoctorCheckError("Zmienna START_URL w config.py nie jest prawidłowym linkiem.")
    return "Plik konfiguracyjny jest kompletny i poprawny."

def check_permissions(config_module):
    """Sprawdza uprawnienia do zapisu w kluczowych folderach."""
    logger.info("Test uprawnień do zapisu...")
    db_path = getattr(config_module, 'DATABASE_FILE', 'app_data/database/photos.db')
    dirs_to_check = [config_module.SESSION_DIR, config_module.DOWNLOADS_DIR_BASE, Path(db_path).parent]
    for dir_path in dirs_to_check:
        try:
            p = Path(dir_path); p.mkdir(parents=True, exist_ok=True)
            test_file = p / f"doctor_test_{os.getpid()}.tmp"
            test_file.write_bytes(os.urandom(128)); test_file.unlink()
        except Exception as e:
            raise DoctorCheckError(f"Brak uprawnień do zapisu w '{p}'. Błąd: {e}")
    return "Program ma uprawnienia do zapisu w kluczowych folderach."

async def check_database_integrity(config_module):
    """Sprawdza integralność i strukturę bazy danych."""
    logger.info("Test integralności i schematu bazy danych...")
    db_type = getattr(config_module, 'DB_TYPE', 'sqlite')
    if db_type != 'sqlite':
        return "Pominięto (test dotyczy tylko bazy SQLite)."
        
    db_path = Path(getattr(config_module, 'DATABASE_FILE', 'app_data/database/photos.db'))
    if not await asyncio.to_thread(db_path.exists):
        raise DoctorInfo("Plik bazy danych nie istnieje (zostanie utworzony).")
    try:
        async with aiosqlite.connect(db_path) as conn:
            cursor = await conn.execute("PRAGMA integrity_check;")
            if (await cursor.fetchone())[0].lower() != 'ok':
                raise DoctorCheckError("Baza danych jest uszkodzona!")
            
            required_cols = {'id', 'url', 'status', 'final_path'}
            cursor = await conn.execute("PRAGMA table_info(downloaded_media)")
            if not required_cols.issubset({info[1] for info in await cursor.fetchall()}):
                raise DoctorCheckError("Struktura bazy jest nieaktualna (brakuje kolumn).")
        return "Baza danych jest spójna i ma aktualną strukturę."
    except Exception as e:
        raise DoctorCheckError(f"Błąd bazy danych: {e}")

async def check_session_validity(config_module):
    """Sprawdza, czy zapisana sesja logowania do Google jest wciąż aktywna."""
    logger.info("Test ważności sesji logowania...")
    session_path = Path(config_module.SESSION_DIR)
    if not await asyncio.to_thread(session_path.exists) or not any(await asyncio.to_thread(os.listdir, session_path)):
        raise DoctorInfo(f"Folder sesji '{session_path}' jest pusty. Odśwież sesję.")
    try:
        async with async_playwright() as p:
            context = await getattr(p, config_module.BROWSER_TYPE).launch_persistent_context(session_path.expanduser(), headless=True)
            page = await context.new_page()
            await page.goto("https://photos.google.com/", timeout=20000)
            await page.wait_for_selector("input[aria-label^='Wyszukaj']", timeout=15000)
            await context.close()
        return "Zapisana sesja logowania jest aktywna."
    except Exception as e:
        raise DoctorCheckError(f"Sesja wygasła lub jest nieprawidłowa. Odśwież ją. Błąd: {type(e).__name__}")
