#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# plik: start.py
# Wersja 5.0 - Dynamiczna Konfiguracja Logowania i Ulepszona Struktura
#
# ############################################################################
# ===                  GŁÓWNY PLIK URUCHOMIENIOWY APLIKACJI                ===
# ############################################################################
#
# To jest główny plik, który inicjalizuje i uruchamia całą aplikację.
# Jego jedynym zadaniem jest przygotowanie środowiska i przekazanie
# kontroli do głównego, interaktywnego menu.
#
# Kolejność działań:
#  1. Dynamiczne skonfigurowanie centralnego systemu logowania na podstawie
#     ustawień w pliku `app_data/settings.json`.
#  2. Zainicjowanie bazy danych (utworzenie tabel, jeśli nie istnieją).
#  3. Ustawienie obsługi sygnału przerwania (Ctrl+C) dla "grzecznego" zamykania.
#  4. Uruchomienie głównego menu aplikacji zdefiniowanego w `core/menu_logic.py`.
#
##############################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import signal
import sys
import logging
import json
from pathlib import Path
from logging.handlers import RotatingFileHandler

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.logging import RichHandler

# --- IMPORTY Z WŁASNYCH MODUŁÓW `core` ---
try:
    from core.database import setup_database
    from core.menu_logic import run_main_menu
    from core.utils import handle_shutdown_signal
    # Importujemy cały moduł config, aby mieć dostęp do wszystkich stałych ustawień.
    from core import config
except ImportError as e:
    # Ten błąd jest krytyczny, więc logujemy go i kończymy działanie
    logging.critical(f"Nie można zaimportować modułu z folderu 'core': {e}", exc_info=True)
    print(f"[BŁĄD KRYTYCZNY] Nie można zaimportować modułu z folderu 'core': {e}")
    print("Upewnij się, że uruchamiasz ten skrypt z głównego folderu projektu,")
    print("a struktura folderów jest poprawna (powinien istnieć folder 'core').")
    sys.exit(1)

# --- INICJALIZACJA I KONFIGURACJA MODUŁU ---
logger = logging.getLogger(__name__)
console = Console()
SETTINGS_FILE = Path("app_data/settings.json")


# ############################################################################
# ===               SEKCJA 1: KONFIGURACJA SYSTEMU LOGOWANIA               ===
# ############################################################################

def setup_global_logging():
    """
    Konfiguruje centralny system logowania dla całej aplikacji.

    Funkcja ta odczytuje ustawienie `LOG_ENABLED` z pliku `settings.json`.
    Jeśli logowanie jest włączone, konfiguruje profesjonalny logger z `RichHandler`
    dla pięknych logów w konsoli oraz opcjonalnie `RotatingFileHandler`
    do zapisu logów w pliku. Jeśli jest wyłączone, całkowicie wycisza logowanie.
    """
    # Odczytaj ustawienia użytkownika z pliku JSON
    try:
        with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
            user_settings = json.load(f)
        log_enabled = user_settings.get("LOG_ENABLED", True)
    except (FileNotFoundError, json.JSONDecodeError):
        # Jeśli plik nie istnieje lub jest uszkodzony, włącz logowanie domyślnie
        log_enabled = True
        logger.warning(f"Nie można odczytać pliku '{SETTINGS_FILE}'. Używam domyślnych ustawień logowania.")

    if not log_enabled:
        # Całkowicie wyłącz logowanie, ustawiając poziom na wyższy niż CRITICAL
        logging.basicConfig(level=logging.CRITICAL + 1)
        # Informujemy użytkownika jednorazowym `printem`, ponieważ logger jest wyciszony
        console.print("[dim]Logowanie jest wyłączone w pliku `settings.json`.[/dim]")
        return

    # --- Konfiguracja dla włączonego logowania ---
    log_handlers = [
        RichHandler(
            rich_tracebacks=True,
            console=console,
            markup=True,
            show_path=False # Ukrywamy ścieżkę do pliku w logach dla większej czytelności
        )
    ]
    
    # Sprawdzamy, czy logi mają być również zapisywane do pliku, na podstawie `config.py`
    if config.LOG_SAVE_TO_FILE:
        try:
            log_path = Path(config.LOG_FILENAME)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = RotatingFileHandler(
                log_path,
                maxBytes=5*1024*1024,  # 5 MB na plik
                backupCount=2,        # Zachowaj 2 ostatnie archiwalne pliki logów
                encoding='utf-8'
            )
            file_handler.setFormatter(
                logging.Formatter("%(asctime)s - [%(levelname)s] - %(name)s - %(message)s")
            )
            log_handlers.append(file_handler)
        except Exception as e:
            # Informujemy o problemie, ale nie przerywamy działania aplikacji
            logging.error(f"Nie udało się skonfigurować zapisu logów do pliku: {e}", exc_info=True)

    # Finalna konfiguracja loggera
    logging.basicConfig(
        level=config.LOG_LEVEL.upper(),
        format="%(message)s",
        datefmt="[%X]",
        handlers=log_handlers
    )
    logger.info("System logowania został pomyślnie skonfigurowany.")


# ##############################################################################
# ===                   SEKCJA 2: GŁÓWNA FUNKCJA APLIKACJI                   ===
# ##############################################################################

async def main():
    """
    Główna funkcja asynchroniczna, która inicjalizuje i uruchamia aplikację.

    Wykonuje kluczowe kroki startowe w odpowiedniej kolejności i zawiera
    główną obsługę błędów na poziomie całej aplikacji.
    """
    try:
        # Krok 1: Skonfiguruj system logowania jako pierwszą czynność
        setup_global_logging()
        logger.info("[bold green]Aplikacja Google Photos Toolkit startuje...[/]", extra={"markup": True})

        # Krok 2: Przygotuj bazę danych (utwórz tabele, jeśli nie istnieją)
        logger.debug("Inicjalizacja bazy danych...")
        setup_database()
        logger.info("Baza danych została pomyślnie zainicjalizowana.")

        # Krok 3: Ustaw nasłuchiwanie na sygnał Ctrl+C
        logger.debug("Ustawianie obsługi sygnału SIGINT (Ctrl+C)...")
        signal.signal(signal.SIGINT, handle_shutdown_signal)
        logger.info("Obsługa sygnału przerwania została ustawiona.")

        # Krok 4: Uruchom główne, interaktywne menu aplikacji
        logger.info("Uruchamianie głównego menu aplikacji...")
        await run_main_menu()

        # Krok 5: Wyświetlenie komunikatu pożegnalnego po normalnym zamknięciu
        logger.info("[bold green]Aplikacja została zamknięta przez użytkownika. Do widzenia![/]", extra={"markup": True})

    except Exception:
        # Ten blok `except` jest "ostatnią deską ratunku" - łapie wszelkie
        # nieoczekiwane błędy, które mogłyby wystąpić w głównej logice.
        logger.critical("Wystąpił nieoczekiwany, krytyczny błąd w głównej funkcji aplikacji.", exc_info=True)
        # W przypadku krytycznego błędu, zwracamy kod błędu do systemu operacyjnego.
        sys.exit(1)


# ##############################################################################
# ===                    SEKCJA 3: PUNKT WEJŚCIA SKRYPTU                     ===
# ##############################################################################

if __name__ == "__main__":
    """
    Ten blok jest wykonywany tylko wtedy, gdy plik `start.py` jest
    uruchamiany bezpośrednio (np. `python start.py`), a nie importowany.
    """
    try:
        # `asyncio.run()` tworzy nową pętlę zdarzeń i uruchamia w niej
        # naszą główną funkcję `main()` aż do jej zakończenia.
        asyncio.run(main())

    except KeyboardInterrupt:
        # Ta obsługa jest na wszelki wypadek, gdyby sygnał SIGINT nie został
        # poprawnie obsłużony na wyższym poziomie. Zapewnia ciche wyjście.
        logger.warning("Program został przerwany przez użytkownika (KeyboardInterrupt) na najwyższym poziomie.")
    except Exception:
        # Ostateczna ochrona przed awarią.
        logger.critical("Wystąpił nieprzewidziany błąd na najwyższym poziomie wykonania.", exc_info=True)
        sys.exit(1)
