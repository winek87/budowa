# plik: core/doctor/cli.py
# Wersja 1.3 - Poprawiono strukturę importów UI.
# Opis: Moduł zarządzający przepływem pracy dla narzędzia "Doktor".
# -*- coding: utf-8 -*-

import asyncio
import logging
from functools import partial

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

# Importy z modułów projektu `core`
from .. import config as core_config

# Importy z wewnętrznych modułów pakietu
from .utils import silence_loggers
from .tasks import (
    DoctorCheckError, DoctorInfo,
    check_project_structure, check_dependencies, check_network_connectivity,
    check_playwright_browsers, check_exiftool_program, check_config_completeness,
    check_permissions, check_database_integrity, check_session_validity
)
from .ui.live_display import DoctorLiveDisplay

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)


async def run_doctor():
    """
    Uruchamia interaktywny interfejs diagnostyczny "Doktor".
    """
    console.clear()
    logger.info("Uruchamiam Diagnostykę Systemu (Doktor)...")

    # Definicja wszystkich testów do wykonania
    test_definitions = [
        {'name': "Struktura projektu", 'func': check_project_structure, 'async': False},
        {'name': "Zależności (Python)", 'func': check_dependencies, 'async': False},
        {'name': "Połączenie sieciowe", 'func': check_network_connectivity, 'async': False},
        {'name': "Zależności (ExifTool)", 'func': check_exiftool_program, 'async': False},
        {'name': "Zależności (Playwright)", 'func': partial(check_playwright_browsers, core_config), 'async': False},
        {'name': "Kompletność Konfiguracji", 'func': partial(check_config_completeness, core_config), 'async': False},
        {'name': "Uprawnienia do zapisu", 'func': partial(check_permissions, core_config), 'async': False},
        {'name': "Integralność Bazy Danych", 'func': partial(check_database_integrity, core_config), 'async': True},
        {'name': "Ważność sesji logowania", 'func': partial(check_session_validity, core_config), 'async': True},
    ]

    overall_ok = True
    
    # Definiujemy, które loggery chcemy wyciszyć na czas działania dashboardu
    loggers_to_silence = ['core.doctor.tasks', 'core.utils']

    with silence_loggers(loggers_to_silence):
        try:
            with DoctorLiveDisplay(tests_to_run=test_definitions, console=console) as display:
                for test in test_definitions:
                    test_name = test['name']
                    display.start_test(test_name)
                    
                    try:
                        if test['async']:
                            details = await test['func']()
                        else:
                            details = await asyncio.to_thread(test['func'])
                        
                        display.end_test(test_name, "✅ OK", details, "green")
                    
                    except DoctorInfo as e:
                        display.end_test(test_name, "ℹ️ INFO", str(e), "cyan")
                    
                    except DoctorCheckError as e:
                        display.end_test(test_name, "❌ BŁĄD", str(e), "red")
                        overall_ok = False
                    
                    except Exception as e:
                        logger.critical(f"Krytyczny błąd w teście '{test_name}'", exc_info=True)
                        display.end_test(test_name, "💥 KRYTYCZNY", str(e), "bold white on red")
                        overall_ok = False
                    
                    await asyncio.sleep(0.3) # Krótka pauza dla lepszego efektu wizualnego
                
                display.finish()
                await asyncio.sleep(1)

        except Exception as e:
            # Ten blok łapie błędy, które mogłyby wystąpić poza pętlą testów,
            # np. podczas inicjalizacji DoctorLiveDisplay.
            logger.critical("Wystąpił nieoczekiwany błąd w module Doktor.", exc_info=True)
            console.print(Panel(f"[bold red]Wystąpił błąd krytyczny: {e}[/]", title="Błąd", border_style="red"))

    # Wyświetlenie finalnego podsumowania
    if overall_ok:
        console.print(Panel("✅ [bold green]Diagnostyka zakończona pomyślnie. System w pełni sprawny![/]", title="[bold]Wynik Końcowy[/bold]", border_style="green"))
    else:
        console.print(Panel("⚠️ [bold red]Diagnostyka wykryła problemy![/]\n\n[dim]Sprawdź szczegóły w powyższej tabeli i plikach logów, aby uzyskać więcej informacji.[/dim]", title="[bold]Wynik Końcowy[/bold]", border_style="red"))

    Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić do menu...[/]")
