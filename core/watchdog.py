# -*- coding: utf-8 -*-
"""
Moduł "Watchdog" (Stróż) do monitorowania i restartowania długotrwałych zadań.
Wersja 2.0 - Poprawiona logika asyncio, aby zapobiec zawieszaniu się.
"""

import asyncio
import logging
from typing import Callable, Any
from rich.console import Console
from rich.panel import Panel

logger = logging.getLogger(__name__)
console = Console()

# Domyślne wartości
DEFAULT_MAX_RESTARTS = 3
DEFAULT_RESTART_DELAY_S = 15
DEFAULT_HEARTBEAT_TIMEOUT_S = 300  # 5 minut

class WatchdogError(Exception):
    """Wyjątek rzucany, gdy zadanie ostatecznie zawiedzie po wszystkich próbach."""
    pass

async def monitored_task(
    target_func: Callable[..., Any],
    *args,
    max_restarts: int = DEFAULT_MAX_RESTARTS,
    restart_delay: int = DEFAULT_RESTART_DELAY_S,
    heartbeat_timeout: int = DEFAULT_HEARTBEAT_TIMEOUT_S,
    **kwargs
) -> Any:
    """
    "Nieśmiertelny" opakowujący (wrapper), który uruchamia, monitoruje i restartuje
    długotrwałe zadanie asynchroniczne. Wersja 2.0.

    Args:
        target_func (Callable): Funkcja asynchroniczna do wykonania.
        max_restarts (int): Maksymalna liczba prób restartu.
        restart_delay (int): Czas oczekiwania (w sekundach) między restartami.
        heartbeat_timeout (int): Maksymalny czas (w sekundach) bez "pulsu" od zadania.
        *args, **kwargs: Argumenty do przekazania do `target_func`.

    Returns:
        Any: Wynik zwrócony przez `target_func`, jeśli zakończy się sukcesem.

    Raises:
        WatchdogError: Jeśli zadanie zawiedzie po wszystkich próbach restartu.
    """
    restart_count = 0
    heartbeat_event = asyncio.Event()
    kwargs['heartbeat_event'] = heartbeat_event

    while restart_count <= max_restarts:
        main_task = None
        monitor_task = None
        try:
            logger.info(f"Watchdog: Uruchamiam zadanie '{target_func.__name__}' (próba {restart_count + 1}/{max_restarts + 1}).")

            # Krok 1: Stwórz zadanie główne (np. index_faces)
            main_task = asyncio.create_task(target_func(*args, **kwargs))

            # Krok 2: Stwórz zadanie monitorujące puls w pętli
            async def _heartbeat_monitor():
                while True:
                    try:
                        await asyncio.wait_for(heartbeat_event.wait(), timeout=heartbeat_timeout)
                        heartbeat_event.clear()
                        logger.debug(f"Watchdog: Otrzymano puls z '{target_func.__name__}'.")
                    except asyncio.TimeoutError:
                        logger.warning(f"Watchdog: Timeout! Brak pulsu od '{target_func.__name__}' przez {heartbeat_timeout}s.")
                        # Rzucenie wyjątku w tym zadaniu spowoduje, że asyncio.wait go przechwyci
                        raise TimeoutError("Heartbeat timeout")

            monitor_task = asyncio.create_task(_heartbeat_monitor())

            # Krok 3: Czekaj na ZAKOŃCZENIE PIERWSZEGO z dwóch zadań
            done, pending = await asyncio.wait(
                [main_task, monitor_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            # Krok 4: Sprzątanie
            # Anuluj zadanie, które wciąż działa (jeśli jest)
            for task in pending:
                task.cancel()
            
            # Sprawdź, co się zakończyło
            if main_task in done:
                # Jeśli zadanie główne zakończyło się pierwsze, wszystko jest OK.
                # Zwróć jego wynik. To automatycznie zakończy watchdoga.
                return main_task.result()
            
            # Jeśli monitor zakończył się pierwszy, to znaczy, że był timeout.
            # Rzuć wyjątek, aby pętla `while` mogła go złapać i zrestartować.
            raise TimeoutError(f"Zadanie '{target_func.__name__}' nie odpowiedziało w ciągu {heartbeat_timeout}s.")

        except Exception as e:
            # Anuluj oba zadania, jeśli jeszcze działają, na wypadek błędu
            if main_task and not main_task.done():
                main_task.cancel()
            if monitor_task and not monitor_task.done():
                monitor_task.cancel()
            # Czekamy chwilę, aby dać zadaniom czas na anulowanie
            await asyncio.gather(main_task, monitor_task, return_exceptions=True)

            restart_count += 1
            logger.error(f"Watchdog: Zadanie '{target_func.__name__}' zawiodło. Błąd: {e}", exc_info=False)
            
            if restart_count > max_restarts:
                error_msg = f"Watchdog: Zadanie '{target_func.__name__}' ostatecznie zawiodło po {max_restarts} próbach."
                logger.critical(error_msg)
                console.print(Panel(f"🔥 [bold red]BŁĄD KRYTYCZNY[/]\n\n{error_msg}", border_style="red"))
                raise WatchdogError(error_msg) from e

            logger.warning(f"Watchdog: Restart za {restart_delay}s... (Próba {restart_count}/{max_restarts})")
            await asyncio.sleep(restart_delay)

    # Ten kod nie powinien być nigdy osiągnięty
    raise WatchdogError("Watchdog zakończył pętlę w nieoczekiwany sposób.")
