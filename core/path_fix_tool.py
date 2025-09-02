# -*- coding: utf-8 -*-

# plik: core/path_fix_tool.py
# Wersja 7.0 - W pełni asynchroniczny i udokumentowany
#
# ##############################################################################
# ===                     MODUŁ NAPRAWY ŚCIEŻEK W BAZIE                      ===
# ##############################################################################
#
# To narzędzie jest "chirurgiem" dla ścieżek plików w bazie danych. Jego
# głównym zadaniem jest naprawa bazy po tym, jak użytkownik ręcznie
# przeniósł cały folder z pobranymi plikami (`DOWNLOADS_DIR_BASE`) w inne
# miejsce na dysku.
#
# Narzędzie inteligentnie wykrywa starą, nieaktualną część ścieżki i
# automatycznie zamienia ją na nową, poprawną ścieżkę z pliku `config.py`.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import re
import logging
from pathlib import Path
import asyncio

# --- Importy asynchroniczne ---
import aiosqlite

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import DATABASE_FILE, DOWNLOADS_DIR_BASE

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


async def find_path_issue() -> tuple[str | None, str | None]:
    """
    Asynchronicznie i inteligentnie analizuje bazę danych w poszukiwaniu
    nieaktualnych prefiksów ścieżek plików.

    Funkcja ta:
    1.  Pobiera aktualną, poprawną ścieżkę bazową (`DOWNLOADS_DIR_BASE`)
        z pliku konfiguracyjnego.
    2.  Szuka w bazie danych pierwszego rekordu, którego ścieżka `final_path`
        nie zaczyna się od poprawnego prefiksu.
    3.  Jeśli znajdzie taki rekord, próbuje automatycznie zidentyfikować,
        która część ścieżki jest nieaktualna. Robi to, szukając w ścieżce
        pierwszej struktury folderu daty (np. `/2023/10/`) i zakładając,
        że wszystko *przed* tą strukturą jest starym, niepoprawnym prefiksem.

    Returns:
        tuple[str | None, str | None]: Krotka zawierająca:
            (nieaktualny_prefiks, nowy_poprawny_prefiks).
            Zwraca (None, None), jeśli nie znaleziono problemów lub
            wystąpił błąd.
    """
    logger.info("Rozpoczynam analizę bazy danych w poszukiwaniu problemów ze ścieżkami...")
    try:
        correct_base_path = str(Path(DOWNLOADS_DIR_BASE).resolve())
        logger.debug(f"Oczekiwany prefiks ścieżki: '{correct_base_path}'")
        
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            # Szukamy JEDNEGO przykładowego wiersza, który ma problem.
            # aiosqlite nie wspiera SUBSTR, więc musimy użyć LIKE z negacją.
            query = """
                SELECT final_path FROM downloaded_media 
                WHERE final_path IS NOT NULL AND final_path != '' 
                AND final_path NOT LIKE ? 
                LIMIT 1
            """
            # Wzorzec LIKE: ścieżka MUSI zaczynać się od...
            pattern = f"{correct_base_path}%"
            cursor = await conn.execute(query, (pattern,))
            sample_path_tuple = await cursor.fetchone()

            if not sample_path_tuple:
                logger.info("Analiza zakończona. Nie znaleziono żadnych nieprawidłowych ścieżek.")
                return None, None
            
            sample_path = sample_path_tuple[0]
            logger.warning(f"Wykryto potencjalny problem. Przykładowa błędna ścieżka: '{sample_path}'")

            # Inteligentne wykrywanie części do zamiany
            match = re.search(r'([/\\]\d{4}[/\\]\d{2}[/\\])', sample_path)
            if not match:
                logger.error(f"Nie można automatycznie zidentyfikować struktury daty (ROK/MIESIĄC) w ścieżce '{sample_path}'.")
                return None, None
            
            bad_part_end_index = match.start()
            bad_part = sample_path[:bad_part_end_index]
            
            logger.info(f"Automatycznie zidentyfikowano nieaktualny prefiks do zamiany: '{bad_part}'")
            return bad_part, correct_base_path
            
    except aiosqlite.Error as e:
        logger.critical("Wystąpił błąd bazy danych podczas analizy ścieżek.", exc_info=True)
        return None, None
    except Exception as e:
        logger.critical("Wystąpił nieoczekiwany błąd podczas analizy ścieżek.", exc_info=True)
        return None, None

# plik: core/path_fix_tool.py

async def _update_paths_in_db(bad_part: str, good_part: str) -> tuple[int, int]:
    """
    Asynchronicznie wykonuje operację UPDATE na bazie danych, zastępując
    nieaktualny prefiks ścieżki nowym.

    Args:
        bad_part (str): Stary, niepoprawny prefiks ścieżki do usunięcia.
        good_part (str): Nowy, poprawny prefiks ścieżki do wstawienia.

    Returns:
        tuple[int, int]: Krotka zawierająca (liczba_zaktualizowanych_final_path,
                                          liczba_zaktualizowanych_expected_path).
    """
    async with aiosqlite.connect(DATABASE_FILE) as conn:
        logger.info("Aktualizuję kolumnę 'final_path'...")
        cursor = await conn.execute(
            "UPDATE downloaded_media SET final_path = REPLACE(final_path, ?, ?)",
            (bad_part, good_part)
        )
        updated_final = cursor.rowcount
        logger.info(f"Zaktualizowano {updated_final} wpisów w 'final_path'.")
        
        logger.info("Aktualizuję kolumnę 'expected_path'...")
        cursor = await conn.execute(
            "UPDATE downloaded_media SET expected_path = REPLACE(expected_path, ?, ?)",
            (bad_part, good_part)
        )
        updated_expected = cursor.rowcount
        logger.info(f"Zaktualizowano {updated_expected} wpisów w 'expected_path'.")

        await conn.commit()
        return updated_final, updated_expected

async def run_path_fixer():
    """
    Uruchamia główne narzędzie do resynchronizacji ścieżek w bazie danych.

    Proces:
    1.  Wywołuje `find_path_issue` do zdiagnozowania problemu.
    2.  Jeśli problem zostanie znaleziony, prezentuje użytkownikowi czytelny
        plan naprawy, pokazując, co i na co zostanie zamienione.
    3.  Po uzyskaniu potwierdzenia, wywołuje `_update_paths_in_db` do
        wykonania operacji `UPDATE` na bazie.
    """
    console.clear()
    logger.info("Uruchamiam Narzędzie do Naprawy Ścieżek Plików...")
    console.print(Panel("🔧 Narzędzie do Naprawy Ścieżek Plików w Bazie Danych 🔧", expand=False, border_style="cyan"))

    # Krok 1: Zdiagnozuj problem
    with console.status("[cyan]Analizuję bazę danych...[/]"):
        bad_part, good_part = await find_path_issue()

    if bad_part is None:
        console.print("\n[bold green]✅ Analiza zakończona. Wygląda na to, że wszystkie ścieżki w bazie są już poprawne.[/bold green]")
        return

    # Krok 2: Wyświetl plan naprawy i poproś o potwierdzenie
    plan_text = (
        "Narzędzie zamierza zastąpić nieaktualny prefiks ścieżki:\n\n"
        f"[bold red]'{bad_part}'[/]\n\n"
        "na aktualny prefiks z Twojej konfiguracji:\n\n"
        f"[bold green]'{good_part}'[/]\n\n"
        "Operacja zostanie wykonana dla kolumn `final_path` oraz `expected_path`."
    )
    console.print(f"\n[cyan]Wykryto problem do naprawienia. Planowane działanie:[/cyan]")
    console.print(Panel(plan_text, title="[bold yellow]PLAN NAPRAWY[/]", border_style="yellow"))

    if not Confirm.ask("\n[bold]Czy na pewno chcesz kontynuować?[/bold]", default=False):
        logger.warning("Operacja naprawy ścieżek anulowana przez użytkownika.")
        return

    # Krok 3: Wykonaj operację na bazie danych
    try:
        with console.status("[bold yellow]Aktualizuję bazę danych... To może potrwać kilka minut.[/]", spinner="earth"):
            updated_final, updated_expected = await _update_paths_in_db(bad_part, good_part)

        console.print(f"\n[bold green]✅ Sukces! Zaktualizowano [cyan]{updated_final}[/cyan] ścieżek rzeczywistych (`final_path`).[/bold green]")
        console.print(f"[bold green]✅ Sukces! Zaktualizowano [cyan]{updated_expected}[/cyan] ścieżek oczekiwanych (`expected_path`).[/bold green]")
        logger.info("Operacja naprawy ścieżek zakończona pomyślnie.")

    except aiosqlite.Error as e:
        logger.critical("Wystąpił krytyczny błąd podczas aktualizacji bazy danych.", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd bazy danych: {e}[/bold red]")
    except Exception as e:
        logger.critical("Wystąpił nieoczekiwany błąd podczas aktualizacji bazy danych.", exc_info=True)
        console.print(f"[bold red]Wystąpił nieoczekiwany błąd. Sprawdź plik logu.[/bold red]")
