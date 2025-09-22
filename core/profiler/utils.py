# plik: core/profiler/utils.py
# Wersja 1.0 - Narzędzia pomocnicze dla modułu Profiler.
# Opis: Ten moduł zawiera funkcje pomocnicze, w tym menedżer
#       do wyciszania logów na czas pracy dashboardu.
# -*- coding: utf-8 -*-

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
