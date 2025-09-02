# -*- coding: utf-8 -*-

# plik: core/code_analyzer_logic.py
# Wersja 5.0 - W pełni asynchroniczny i zrefaktoryzowany Audytor Kodu
#
# ##############################################################################
# ===                       MODUŁ AUDYTORA KODU                              ===
# ##############################################################################
#
# "Audytor Kodu" to narzędzie deweloperskie służące do utrzymania wysokiej
# jakości i spójności kodu źródłowego projektu. Wykonuje dwie analizy:
#
#  1. ANALIZA STATYCZNA (LINTING): Używa biblioteki `flake8` do
#     przeskanowania kodu w poszukiwaniu potencjalnych błędów,
#     niespójności stylistycznych i złych praktyk programistycznych.
#
#  2. TESTY JEDNOSTKOWE: Uruchamia wszystkie testy jednostkowe zdefiniowane
#     w projekcie (zwykle w folderze `tests/`), aby zweryfikować, czy
#     poszczególne funkcje działają zgodnie z oczekiwaniami.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import unittest
import io
import logging
import asyncio
from pathlib import Path
from typing import List, Tuple
from contextlib import redirect_stdout

# --- Sprawdzenie dostępności zależności ---
try:
    # Ten import nie jest używany bezpośrednio, ale służy do sprawdzenia,
    # czy flake8 jest zainstalowany.
    import flake8
    FLAKE8_AVAILABLE = True
except ImportError:
    FLAKE8_AVAILABLE = False

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.prompt import Prompt

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .utils import create_interactive_menu

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


# ##############################################################################
# ===                     SEKCJA 1: FUNKCJE AUDYTUJĄCE                       ===
# ##############################################################################

async def run_linter_async(paths_to_check: List[str]) -> Tuple[bool, List[str]]:
    """
    Uruchamia analizę statyczną kodu za pomocą Flake8 jako asynchronicznego
    procesu potomnego.

    Args:
        paths_to_check (List[str]): Lista ścieżek do folderów/plików z kodem
                                    do analizy statycznej.

    Returns:
        Tuple[bool, List[str]]: Krotka zawierająca:
            - `bool`: True, jeśli analiza przebiegła pomyślnie, False w
                      przypadku błędu.
            - `List[str]`: Lista linii z wynikami zwróconymi przez Flake8.
    """
    if not FLAKE8_AVAILABLE:
        logger.warning("Biblioteka 'flake8' nie jest zainstalowana. Analiza statyczna kodu została pominięta.")
        return True, ["SKIPPED: Biblioteka 'flake8' nie jest zainstalowana."]

    logger.info(f"Uruchamiam asynchroniczną analizę statyczną kodu (flake8) w: {', '.join(paths_to_check)}")

    command = ["flake8", *paths_to_check, "--ignore=E501,W503", "--max-line-length=120"]
    
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if stderr:
            error_message = stderr.decode('utf-8').strip()
            logger.error(f"Flake8 zwrócił błąd: {error_message}")
            return False, [f"BŁĄD KRYTYCZNY: {error_message}"]
            
        results = [line for line in stdout.decode('utf-8').strip().split('\n') if line]
        logger.info(f"Analiza Flake8 zakończona. Znaleziono {len(results)} potencjalnych problemów.")
        return True, results

    except FileNotFoundError:
        error_message = "Polecenie 'flake8' nie zostało znalezione. Upewnij się, że jest zainstalowane i w ścieżce PATH."
        logger.error(error_message)
        return False, [f"BŁĄD KRYTYCZNY: {error_message}"]
    except Exception as e:
        logger.critical("Wystąpił nieoczekiwany błąd podczas uruchamiania Flake8.", exc_info=True)
        return False, [f"BŁĄD KRYTYCZNY: {e}"]


async def run_tests_async(test_path: str) -> Tuple[bool, str]:
    """
    Uruchamia wszystkie testy jednostkowe zdefiniowane w projekcie w osobnym
    wątku, aby nie blokować pętli asyncio.

    Args:
        test_path (str): Ścieżka do folderu z testami jednostkowymi.

    Returns:
        Tuple[bool, str]: Krotka zawierająca:
            - `bool`: True, jeśli testy zakończyły się bez błędów i niepowodzeń.
            - `str`: Sformatowane wyjście z przebiegu testów do wyświetlenia.
    """
    if not await asyncio.to_thread(Path(test_path).exists):
        logger.warning(f"Folder z testami '{test_path}' nie istnieje. Pomijam testy jednostkowe.")
        return True, f"SKIPPED: Folder z testami '{test_path}' nie istnieje."

    logger.info(f"Uruchamiam asynchroniczne testy jednostkowe z folderu '{test_path}'...")

    def run_unittest_suite():
        """
        Wewnętrzna, synchroniczna funkcja, która wykonuje blokujący
        proces uruchamiania testów.
        """
        try:
            suite = unittest.TestLoader().discover(test_path, pattern="test_*.py")
            if suite.countTestCases() == 0:
                logger.info("Nie znaleziono żadnych testów jednostkowych do uruchomienia.")
                return True, "INFO: Nie znaleziono żadnych testów jednostkowych do uruchomienia."

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                runner = unittest.TextTestRunner(stream=buffer, verbosity=2)
                result = runner.run(suite)
            
            output = buffer.getvalue()
            is_successful = result.wasSuccessful()
            logger.info(f"Testy jednostkowe zakończone. Sukces: {is_successful}")
            return is_successful, output
            
        except Exception as e:
            logger.critical("Wystąpił nieoczekiwany błąd podczas uruchamiania testów jednostkowych.", exc_info=True)
            return False, f"BŁĄD KRYTYCZNY podczas uruchamiania testów:\n{e}"

    # Uruchom blokującą funkcję w osobnym wątku
    return await asyncio.to_thread(run_unittest_suite)


def display_summary(linter_results: List[str] | None, test_successful: bool | None, test_output: str | None):
    """
    Wyświetla w konsoli czytelne podsumowanie wyników obu audytów.

    Prezentuje osobne panele dla analizy statycznej (Flake8) i testów
    jednostkowych (Unittest), a w przypadku problemów, wyświetla
    szczegółowe raporty lub logi.

    Args:
        linter_results (List[str] | None): Wyniki z `run_linter_async`.
        test_successful (bool | None): Flaga sukcesu z `run_tests_async`.
        test_output (str | None): Surowe wyjście tekstowe z `run_tests_async`.
    """
    console.clear()
    console.print(Panel("📋 Kompleksowy Audyt Kodu Aplikacji 📋", expand=False, style="bold green"))

    # --- Podsumowanie Lintera (Flake8) ---
    if linter_results is not None:
        if not linter_results:
            console.print(Panel("[bold green]✅ Brak problemów w kodzie.[/]", title="Wynik Analizy Statycznej (Flake8)", border_style="green"))
        elif "SKIPPED:" in linter_results[0]:
             console.print(Panel(f"[bold yellow]{linter_results[0]}[/]", title="Analiza Statyczna (Flake8)", border_style="yellow"))
        elif "BŁĄD KRYTYCZNY:" in linter_results[0]:
             console.print(Panel(f"[bold red]{linter_results[0]}[/]", title="Błąd Analizy Statycznej (Flake8)", border_style="red"))
        else:
            error_count = len(linter_results)
            linter_title = f"Wynik Analizy Statycznej (Flake8) - Znaleziono {error_count} problemów"
            
            table = Table(title="Szczegółowy Raport Flake8", show_lines=True)
            table.add_column("Plik", style="cyan", width=30); table.add_column("Linia", style="magenta");
            table.add_column("Kol.", style="yellow"); table.add_column("Opis")
            
            for line in linter_results:
                parts = line.split(':', 3)
                if len(parts) == 4:
                    file, line_num, col_num, message = parts
                    table.add_row(Path(file).name, line_num, col_num, message.strip())
            
            console.print(Panel(table, title=linter_title, border_style="red"))

    # --- Podsumowanie Testów Jednostkowych (Unittest) ---
    if test_output is not None:
        if test_successful is None: # Oznacza, że testy zostały pominięte
            console.print(Panel(f"[bold yellow]{test_output}[/]", title="Testy Jednostkowe (Unittest)", border_style="yellow"))
        else:
            test_panel_style = "green" if test_successful else "red"
            test_title = "Wynik Testów Jednostkowych"
            
            summary_content = Text(test_output, overflow="fold")
            console.print(Panel(summary_content, title=test_title, border_style=test_panel_style))


# ##############################################################################
# ===                    SEKCJA 2: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_code_analyzer():
    """
    Wyświetla interaktywne menu i zarządza całym procesem audytu kodu.
    """
    logger.info("Uruchomiono moduł Audytora Kodu.")
    
    PATHS_TO_LINT = ["core", "uruchom.py", "start.py"]
    TESTS_PATH = "tests"

    menu_items = [
        ("Uruchom pełny audyt (Flake8 + Unittest)", "full"),
        ("Tylko analiza statyczna (Flake8)", "linter"),
        ("Tylko testy jednostkowe (Unittest)", "tests"),
        ("Wróć do menu głównego", "exit")
    ]

    while True:
        console.clear()
        
        selected_action = await create_interactive_menu(
            menu_items,
            "Audytor Kodu",
            border_style="yellow"
        )

        if selected_action == "exit" or selected_action is None:
            logger.info("Zamykanie Audytora Kodu.")
            break

        linter_results, test_successful, test_output = None, None, None
        
        with console.status("[cyan]Uruchamianie analizy kodu...[/]", spinner="dots") as status:
            if selected_action in ["full", "linter"]:
                status.update("[cyan]Uruchamianie analizy statycznej (Flake8)...[/]")
                _, linter_results = await run_linter_async(PATHS_TO_LINT)
            
            if selected_action in ["full", "tests"]:
                status.update("[cyan]Uruchamianie testów jednostkowych (Unittest)...[/]")
                test_successful, test_output = await run_tests_async(TESTS_PATH)
        
        display_summary(linter_results, test_successful, test_output)
        
        Prompt.ask("\n[bold]Audyt zakończony. Naciśnij Enter, aby wrócić do menu audytora...[/]")
