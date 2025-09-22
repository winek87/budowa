# plik: core/smart_archiver/utils.py
# Wersja 1.0 - Narzędzia pomocnicze dla Asystenta Porządkowania.
# -*- coding: utf-8 -*-

import asyncio
import signal
import sys
import logging
from contextlib import contextmanager
from typing import List

logger = logging.getLogger(__name__)

@contextmanager
def silence_loggers(logger_names: List[str]):
    """Tymczasowo wyłącza propagację dla wskazanych loggerów."""
    loggers = [logging.getLogger(name) for name in logger_names]
    original_states = {logger: logger.propagate for logger in loggers}
    try:
        for logger in loggers: logger.propagate = False
        yield
    finally:
        for logger, original_state in original_states.items():
            logger.propagate = original_state

stop_event = asyncio.Event()

def handle_stop_signal(*args):
    """Callback wywoływany po otrzymaniu sygnału przerwania (Ctrl+C)."""
    if not stop_event.is_set():
        logger.warning("Otrzymano sygnał przerwania (Ctrl+C). Kończenie po bieżących zadaniach...")
        stop_event.set()

def setup_signal_handlers():
    """Ustawia lokalną obsługę sygnałów na czas działania modułu."""
    try:
        loop = asyncio.get_running_loop()
        if sys.platform != "win32":
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, handle_stop_signal)
    except Exception as e:
        logger.debug(f"Nie można ustawić signal handlera: {e}")
