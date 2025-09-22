# plik: core/code_analyzer/tasks.py
# Wersja 1.0 - Czysta logika biznesowa dla Audytora Kodu.
# Opis: Ten moduł zawiera funkcje odpowiedzialne za uruchamianie
#       analizy statycznej (flake8) i testów jednostkowych (unittest).
# -*- coding: utf-8 -*-

import asyncio
import io
import logging
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from typing import List, Tuple

# Sprawdzenie dostępności zależności
try:
    import flake8
    FLAKE8_AVAILABLE = True
except ImportError:
    FLAKE8_AVAILABLE = False

# Inicjalizacja
logger = logging.getLogger(__name__)


async def run_linter_async(paths_to_check: List[str]) -> Tuple[bool, List[str]]:
    """
    Uruchamia analizę statyczną kodu za pomocą Flake8 jako proces potomny.
    """
    if not FLAKE8_AVAILABLE:
        logger.warning("Biblioteka 'flake8' nie jest zainstalowana. Pomijam.")
        return True, ["SKIPPED: Biblioteka 'flake8' nie jest zainstalowana."]

    logger.info(f"Uruchamiam analizę flake8 w: {', '.join(paths_to_check)}")
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
        logger.info(f"Analiza Flake8 zakończona. Znaleziono {len(results)} problemów.")
        return True, results
    except FileNotFoundError:
        error_message = "Polecenie 'flake8' nie znalezione. Upewnij się, że jest w ścieżce PATH."
        logger.error(error_message)
        return False, [f"BŁĄD KRYTYCZNY: {error_message}"]
    except Exception as e:
        logger.critical("Błąd podczas uruchamiania Flake8.", exc_info=True)
        return False, [f"BŁĄD KRYTYCZNY: {e}"]


async def run_tests_async(test_path: str) -> Tuple[bool, str]:
    """
    Uruchamia testy jednostkowe w osobnym wątku.
    """
    if not await asyncio.to_thread(Path(test_path).exists):
        logger.warning(f"Folder testów '{test_path}' nie istnieje. Pomijam.")
        return True, f"SKIPPED: Folder testów '{test_path}' nie istnieje."

    logger.info(f"Uruchamiam testy jednostkowe z folderu '{test_path}'...")
    
    def run_unittest_suite():
        """Wewnętrzna, synchroniczna funkcja wykonująca testy."""
        try:
            suite = unittest.TestLoader().discover(test_path, pattern="test_*.py")
            if suite.countTestCases() == 0:
                logger.info("Nie znaleziono testów jednostkowych.")
                return True, "INFO: Nie znaleziono żadnych testów jednostkowych."
            
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                runner = unittest.TextTestRunner(stream=buffer, verbosity=2)
                result = runner.run(suite)

            output = buffer.getvalue()
            is_successful = result.wasSuccessful()
            logger.info(f"Testy jednostkowe zakończone. Sukces: {is_successful}")
            return is_successful, output
        except Exception as e:
            logger.critical("Błąd podczas uruchamiania testów.", exc_info=True)
            return False, f"BŁĄD KRYTYCZNY podczas uruchamiania testów:\n{e}"

    return await asyncio.to_thread(run_unittest_suite)
