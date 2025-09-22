# plik: core/takeout_file_importer/utils.py
# Wersja 1.0 - Narzędzia pomocnicze dla Importera Plików z Takeout.
# -*- coding: utf-8 -*-

import asyncio
import signal
import sys
import logging
from contextlib import contextmanager
from typing import List

@contextmanager
def silence_loggers(logger_names: List[str]):
    """
    Menedżer kontekstu, który tymczasowo wyłącza propagację dla wskazanych
    loggerów, aby ich komunikaty nie zakłócały pracy `rich.Live`.
    """
    loggers = [logging.getLogger(name) for name in logger_names]
    original_states = {logger: logger.propagate for logger in loggers}
    
    try:
        for logger in loggers:
            logger.propagate = False
        yield
    finally:
        for logger, original_state in original_states.items():
            logger.propagate = original_state

# Ten event będzie współdzielony przez moduły wewnątrz tego pakietu
stop_event = asyncio.Event()

def handle_stop_signal(*args):
    """Callback wywoływany po otrzymaniu sygnału przerwania (Ctrl+C)."""
    print("\nOtrzymano sygnał zatrzymania. Kończenie pracy po bieżącym pliku...")
    stop_event.set()

def setup_signal_handlers():
    """Ustawia lokalną obsługę sygnałów na czas działania modułu."""
    try:
        loop = asyncio.get_running_loop()
        if sys.platform != "win32":
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, handle_stop_signal, sig)
    except Exception:
        pass
