# plik: core/exif_writer/utils.py
# Wersja 1.0 - Dedykowane narzędzia dla Zapisywarki EXIF.
# Opis: Ten moduł zawiera logikę do obsługi przerwania operacji (Ctrl+C).
# -*- coding: utf-8 -*-

import asyncio
import signal
import sys
from typing import List

# Ten event będzie współdzielony przez moduły wewnątrz pakietu exif_writer
stop_event = asyncio.Event()

def handle_stop_signal(*args):
    """Callback wywoływany po otrzymaniu sygnału przerwania (Ctrl+C)."""
    # Używamy print zamiast loggera, aby komunikat był widoczny od razu
    print("\nOtrzymano sygnał zatrzymania. Kończenie pracy po bieżącym pliku...")
    stop_event.set()

def setup_signal_handlers():
    """
    Ustawia lokalną obsługę sygnałów (np. Ctrl+C) na czas działania
    modułu Zapisywarki EXIF.
    """
    try:
        loop = asyncio.get_running_loop()
        if sys.platform != "win32":
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, handle_stop_signal, sig)
    except Exception:
        # Ignorujemy błędy, jeśli pętla nie jest jeszcze w pełni uruchomiona
        # lub jeśli sygnały nie mogą być ustawione.
        pass
