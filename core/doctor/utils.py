# plik: core/doctor/utils.py
# Wersja 1.0 - Narzędzia pomocnicze dla modułu Diagnostyki.
# Opis: Ten plik zawiera funkcje pomocnicze specyficzne dla modułu "Doktor",
#       takie jak menedżer do wyciszania logów.
# -*- coding: utf-8 -*-

import logging
from contextlib import contextmanager
from typing import List

from rich.console import Console
from rich.panel import Panel

console_utils = Console() # Używamy osobnej konsoli, aby nie mieszać z główną

def check_dependency(module_name: str, package_name: str, friendly_name: str, silent: bool = False) -> bool:
    """
    Sprawdza dostępność biblioteki i wyświetla pomoc, jeśli jej brakuje.
    
    Args:
        module_name (str): Nazwa modułu do importu (np. 'cv2').
        package_name (str): Nazwa pakietu do instalacji (np. 'opencv-python').
        friendly_name (str): Przyjazna nazwa do wyświetlenia użytkownikowi.
        silent (bool): Jeśli True, nie wyświetla panelu z błędem, tylko zwraca False.

    Returns:
        bool: True, jeśli biblioteka jest dostępna, w przeciwnym razie False.
    """
    try:
        __import__(module_name)
        return True
    except ImportError:
        if not silent:
            console_utils.print(Panel(
                f"[bold red]Brak zależności: {friendly_name}[/]\n\n"
                f"Aby korzystać z tej funkcji, zainstaluj wymaganą bibliotekę:\n"
                f"[cyan]pip install {package_name}[/]",
                title="Brak Zależności", border_style="red"
            ))
        return False
# === KONIEC POPRAWKI ===

@contextmanager
def silence_loggers(logger_names: List[str]):
    """
    Menedżer kontekstu, który tymczasowo wyłącza propagację dla wskazanych
    loggerów, aby ich komunikaty nie zakłócały pracy interfejsu `rich.Live`.

    Po wyjściu z bloku 'with', przywraca ich oryginalny stan, dzięki czemu
    logowanie do pliku działa bez zmian.

    Args:
        logger_names (List[str]): Lista nazw loggerów do wyciszenia,
                                  np. ['core.doctor.tasks'].
    """
    # Pobieramy instancje loggerów na podstawie ich nazw
    loggers = [logging.getLogger(name) for name in logger_names]
    
    # Zapisujemy oryginalny stan atrybutu 'propagate' dla każdego loggera
    original_states = {logger: logger.propagate for logger in loggers}
    
    try:
        # Przed wejściem do bloku 'with', wyłączamy propagację
        for logger in loggers:
            logger.propagate = False
        
        # 'yield' przekazuje kontrolę z powrotem do bloku 'with' w cli.py
        yield
    
    finally:
        # Po wyjściu z bloku 'with' (nawet z powodu błędu),
        # przywracamy oryginalny stan każdego loggera
        for logger, original_state in original_states.items():
            logger.propagate = original_state
