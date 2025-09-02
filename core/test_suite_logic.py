# -*- coding: utf-8 -*-

# plik: core/test_suite_logic.py
# Wersja 6.0 - W pełni asynchroniczny i zintegrowany Pakiet Testowy
#
# ##############################################################################
# ===                     MODUŁ PAKIETU TESTOWEGO                            ===
# ##############################################################################
#
# "Pakiet Testowy" to zestaw narzędzi deweloperskich, które pozwalają
# zweryfikować, czy poszczególne komponenty programu działają poprawnie.
# Każdy test posiada zaawansowany interfejs na żywo, który w czasie
# rzeczywistym pokazuje postęp oraz przechwytuje i wyświetla logi
# w dedykowanym panelu.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import logging
import shutil
from pathlib import Path
from collections import deque

# --- Playwright ---
from playwright.async_api import async_playwright

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text
from rich.prompt import Prompt
from rich.table import Table
from rich.json import JSON
from rich.live import Live
from rich.layout import Layout
from rich.align import Align

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import (
    SESSION_DIR, BROWSER_TYPE, BROWSER_ARGS, WAIT_FOR_PAGE_LOAD, DOWNLOADS_DIR_BASE
)
from .utils import (
    create_interactive_menu, get_date_from_metadata,
    create_unique_filepath, LogCollectorHandler
)
from .master_logic import process_single_photo_page, unstoppable_navigate
from .database import get_state
from .config_editor_logic import get_key

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


# ##############################################################################
# ===                     SEKCJA 1: FUNKCJE TESTUJĄCE                        ===
# ##############################################################################

async def run_single_url_test():
    """
    Uruchamia test dla jednego URL, używając `process_single_photo_page`
    z silnika "Master", z interfejsem statusu i logów na żywo.
    """
    console.clear()
    logger.info("Uruchamiam Test Pojedynczego URL...")
    console.print(Panel("[bold yellow]Test Pojedynczego URL na Silniku Master[/]", expand=False))

    url = Prompt.ask("\n[cyan]Wklej adres URL, który chcesz przetestować[/]")
    if not url.strip().startswith("http"):
        logger.error(f"Wprowadzono nieprawidłowy URL: {url}"); return

    logger.info(f"Rozpoczynam test dla URL: {url}")
    
    live_logs = deque(maxlen=20)
    log_collector = LogCollectorHandler(live_logs)
    root_logger = logging.getLogger(); original_handlers = root_logger.handlers[:]
    root_logger.handlers.clear(); root_logger.addHandler(log_collector)

    steps = ["Uruchamianie przeglądarki", "Nawigacja do strony", "Przetwarzanie (pobieranie/analiza)"]
    step_statuses = {step: "[dim]Oczekuje...[/dim]" for step in steps}
    current_step_index = -1; final_metadata = {}; test_passed = False; error_message = ""

    def generate_live_layout() -> Layout:
        # Logika renderowania UI pozostaje bez zmian
        status_table = Table.grid(padding=(0, 2))
        status_table.add_column("Krok Testu", style="cyan", no_wrap=True, width=45)
        status_table.add_column("Status", justify="left")
        for i, step in enumerate(steps):
            status_text = step_statuses[step]
            if i == current_step_index: status_text = f"[bold yellow][ Działa... ][/bold yellow] {console.render_str(':hourglass:')}"
            status_table.add_row(f"Krok {i+1}/{len(steps)}: {step}", Text.from_markup(status_text))
        
        bottom_panel_content = Group(*live_logs)
        bottom_panel_title = "Logi na Żywo"
        if test_passed or (error_message and final_metadata):
            bottom_panel_content = JSON.from_data(final_metadata)
            bottom_panel_title = "Zebrane Metadane (JSON)"
        
        logs_panel = Panel(bottom_panel_content, title=bottom_panel_title, border_style="green")
        layout = Layout(name="root")
        layout.split(Layout(Panel(Align.center(status_table)), size=len(steps) + 2), Layout(logs_panel, ratio=1))
        return layout

    async with async_playwright() as p:
        browser = None
        with Live(generate_live_layout(), screen=True, auto_refresh=False, transient=True, vertical_overflow="visible") as live:
            async def update_step(index, error_msg=None):
                nonlocal current_step_index
                if index > 0: step_statuses[steps[index-1]] = "[bold green][ OK ] ✓[/bold green]"
                current_step_index = index
                live.update(generate_live_layout(), refresh=True)
                if error_msg:
                    step_statuses[steps[index]] = f"[bold red][ BŁĄD ] ✗[/bold red]"; raise AssertionError(error_msg)
            
            try:
                await update_step(0)
                browser = await getattr(p, BROWSER_TYPE).launch_persistent_context(Path(SESSION_DIR).expanduser(), headless=False, args=BROWSER_ARGS.get(BROWSER_TYPE))
                page = await browser.new_page()
                await update_step(1)
                await page.goto(url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
                await update_step(2)
                
                # ZMIANA: Usunięto niepotrzebne argumenty `deque` i `Text`
                success, status, metadata = await process_single_photo_page(
                    page, page.url, Path(DOWNLOADS_DIR_BASE), 'main'
                )
                
                final_metadata = metadata
                if not success:
                    await update_step(2, f"`process_single_photo_page` zwróciło błąd (status: {status}).")

                step_statuses[steps[-1]] = "[bold green][ OK ] ✓[/bold green]"
                current_step_index = -1; test_passed = True
            except AssertionError as e: error_message = str(e)
            except Exception as e: error_message = f"Wystąpił nieoczekiwany wyjątek: {str(e)[:150]}"
            finally:
                if browser: await browser.close()
                root_logger.removeHandler(log_collector)
                for h in original_handlers: root_logger.addHandler(h)
            
            live.update(generate_live_layout(), refresh=True)
            await asyncio.to_thread(get_key)
    
    console.clear()
    if test_passed:
        console.print(Panel("✅ [bold green]Test pojedynczego URL zakończony SUKCESEM.[/]", border_style="green"))
    else:
        console.print(Panel(f"❌ [bold red]Test pojedynczego URL zakończony BŁĘDEM.[/]\n[yellow]   Przyczyna: {error_message}[/yellow]", border_style="red"))


async def _test_get_date_from_metadata_async():
    """
    Testuje logikę parsowania daty z metadanych z `utils.py`.

    Sprawdza, czy funkcja `get_date_from_metadata` poprawnie obsługuje
    różne formaty dat, hierarchię ważności tagów oraz brakujące dane.

    Raises:
        AssertionError: Jeśli wynik działania funkcji jest niezgodny z oczekiwaniami.
    """
    # Test 1: Idealny przypadek z DateTimeOriginal
    meta1 = {'EXIF:DateTimeOriginal': '2023:01:01 12:00:00'}
    date1 = await get_date_from_metadata(meta1)
    assert date1 == datetime(2023, 1, 1, 12, 0, 0), "Błąd 1: Niepoprawna data z DateTimeOriginal"

    # Test 2: Fallback na CreateDate
    meta2 = {'EXIF:CreateDate': '2022:02:02 13:30:00'}
    date2 = await get_date_from_metadata(meta2)
    assert date2 == datetime(2022, 2, 2, 13, 30, 0), "Błąd 2: Niepoprawna data z CreateDate"

    # Test 3: Fallback na datę pliku wideo (QuickTime)
    meta3 = {'QuickTime:CreateDate': '2021:03:03 14:45:15'}
    date3 = await get_date_from_metadata(meta3)
    assert date3 == datetime(2021, 3, 3, 14, 45, 15), "Błąd 3: Niepoprawna data z QuickTime:CreateDate"

    # Test 4: Brak daty, powinien zwrócić None
    meta4 = {'SourceFile': 'test.jpg'}
    date4 = await get_date_from_metadata(meta4)
    assert date4 is None, "Błąd 4: Zwrócono datę, chociaż nie powinno"

    # Test 5: Dane ze strefą czasową, powinna zostać zignorowana
    meta5 = {'File:FileModifyDate': '2020:04:04 10:00:00+02:00'}
    date5 = await get_date_from_metadata(meta5)
    assert date5 == datetime(2020, 4, 4, 10, 0, 0), "Błąd 5: Niepoprawnie obsłużono strefę czasową"


async def run_unit_tests():
    """
    Uruchamia zestaw szybkich, wewnętrznych testów jednostkowych dla kluczowych
    funkcji pomocniczych, z interfejsem statusu i logów na żywo.
    """
    console.clear()
    logger.info("Uruchamiam Testy Jednostkowe...")
    console.print(Panel("🧪 Uruchamianie Szybkich Testów Jednostkowych...", expand=False, style="yellow"))

    # Utwórz tymczasowy folder do testów
    test_dir = Path("./_temp_test_dir_suite")
    if await asyncio.to_thread(test_dir.exists):
        logger.warning(f"Znaleziono stary folder testowy '{test_dir}'. Usuwam go.")
        await asyncio.to_thread(shutil.rmtree, test_dir)
    await asyncio.to_thread(test_dir.mkdir, exist_ok=True)
    
    # Lista testów do wykonania: (Nazwa, Funkcja, Czy jest async)
    tests = [
        ("Test unikalności nazw plików", _test_create_unique_filepath, False),
        ("Test parsowania daty z metadanych", _test_get_date_from_metadata_async, True),
    ]
    
    live_logs = deque(maxlen=15)
    log_collector = LogCollectorHandler(live_logs)
    root_logger = logging.getLogger(); original_handlers = root_logger.handlers[:]
    root_logger.handlers.clear(); root_logger.addHandler(log_collector)

    step_statuses = {name: "[dim]Oczekuje...[/dim]" for name, _, _ in tests}
    overall_success = True
    current_step_name = ""

    def generate_live_layout() -> Layout:
        # Logika renderowania pozostaje bez zmian
        status_table = Table.grid(padding=(0, 2))
        status_table.add_column("Test Jednostkowy", style="cyan", no_wrap=True, width=45)
        status_table.add_column("Status", justify="left")
        for name, _, _ in tests:
            status_text = step_statuses[name]
            if name == current_step_name:
                status_text = f"[bold yellow][ Działa... ][/bold yellow] {console.render_str(':hourglass:')}"
            status_table.add_row(name, Text.from_markup(status_text))
        
        logs_panel = Panel(Group(*live_logs), title="Logi na Żywo", border_style="green")
        layout = Layout(name="root")
        layout.split(Layout(Panel(Align.center(status_table)), size=len(tests) + 2), Layout(logs_panel, ratio=1))
        return layout

    with Live(generate_live_layout(), screen=True, auto_refresh=False, transient=True, vertical_overflow="visible") as live:
        for name, test_func, is_async in tests:
            current_step_name = name
            live.update(generate_live_layout(), refresh=True)
            await asyncio.sleep(0.5)
            
            try:
                if is_async:
                    await test_func()
                else:
                    await asyncio.to_thread(test_func, test_dir)

                step_statuses[name] = "[bold green]✅ ZALICZONY[/]"
                logger.info(f"Test jednostkowy '{name}' zaliczony.")
            except AssertionError as e:
                error_details = Text(str(e))
                error_details.highlight_regex(r"'(.*?)'", "bold yellow")
                step_statuses[name] = Text.from_markup(f"[bold red]❌ NIEZALICZONY[/]\n") + error_details
                logger.error(f"Test jednostkowy '{name}' niezliczony. Błąd: {e}")
                overall_success = False
            
            live.update(generate_live_layout(), refresh=True)

        current_step_name = ""
        live.update(generate_live_layout(), refresh=True)
        await asyncio.to_thread(get_key)

    # Przywróć oryginalne handlery i posprzątaj
    root_logger.removeHandler(log_collector)
    for h in original_handlers: root_logger.addHandler(h)
    
    console.clear()
    if overall_success:
        console.print(Panel("✅ [bold green]Wszystkie testy jednostkowe zakończone sukcesem![/]", border_style="green"))
    else:
        console.print(Panel("⚠️ [bold red]Wykryto błędy w co najmniej jednym teście jednostkowym.[/]", border_style="red"))

    await asyncio.to_thread(shutil.rmtree, test_dir)
    logger.debug("Usunięto tymczasowy folder testowy.")


async def run_engine_integration_tests():
    """
    Uruchamia test integracyjny silnika "Master" z zaawansowanym interfejsem.

    Test ten symuluje podstawowy cykl pracy silnika:
    1.  Uruchamia przeglądarkę.
    2.  Nawiguje do strony startowej.
    3.  Przetwarza jedną stronę (pobiera plik i metadane).
    4.  Nawiguje do następnego elementu.
    5.  Weryfikuje, czy adres URL uległ zmianie.
    """
    console.clear()
    logger.info("Uruchamiam Testy Integracyjne Silnika Master...")
    console.print(Panel("🚀 Uruchamianie Testów Integracyjnych Silnika Master...", expand=False, style="yellow"))

    start_url = await get_state('last_scan_url') or "https://photos.google.com/photo/AF1QipMnTUIRsS1Kc93fWxJHIegjVRplzs7RuUtXs5nQ"
    
    live_logs = deque(maxlen=15)
    log_collector = LogCollectorHandler(live_logs)
    root_logger = logging.getLogger(); original_handlers = root_logger.handlers[:]
    root_logger.handlers.clear(); root_logger.addHandler(log_collector)

    steps = ["Uruchamianie przeglądarki", "Nawigacja do strony startowej", "Przetwarzanie strony", "Nawigacja do następnego elementu", "Weryfikacja zmiany URL"]
    step_statuses = {step: "[dim]Oczekuje...[/dim]" for step in steps}
    current_step_index = -1; test_passed = False; error_message = ""

    def generate_live_layout() -> Layout:
        # Logika renderowania UI pozostaje bez zmian
        status_table = Table.grid(padding=(0, 2))
        status_table.add_column("Krok", style="cyan", no_wrap=True, width=45)
        status_table.add_column("Status", justify="left")
        for i, step in enumerate(steps):
            status_text = step_statuses[step]
            if i == current_step_index: status_text = f"[bold yellow][ Działa... ][/bold yellow] {console.render_str(':hourglass:')}"
            status_table.add_row(f"Krok {i+1}/{len(steps)}: {step}", Text.from_markup(status_text))
        logs_panel = Panel(Group(*live_logs), title="Logi na Żywo", border_style="green")
        layout = Layout(name="root")
        layout.split(Layout(Panel(Align.center(status_table)), size=len(steps) + 2), Layout(logs_panel, ratio=1))
        return layout

    async with async_playwright() as p:
        browser = None
        with Live(generate_live_layout(), screen=True, auto_refresh=False, transient=True, vertical_overflow="visible") as live:
            async def update_step(index, error_msg=None):
                nonlocal current_step_index
                if index > 0: step_statuses[steps[index-1]] = "[bold green][ OK ] ✓[/bold green]"
                current_step_index = index
                live.update(generate_live_layout(), refresh=True)
                if error_msg:
                    step_statuses[steps[index]] = f"[bold red][ BŁĄD ] ✗[/bold red]"; raise AssertionError(error_msg)
            
            try:
                await update_step(0)
                browser = await getattr(p, BROWSER_TYPE).launch_persistent_context(Path(SESSION_DIR).expanduser(), headless=True, args=BROWSER_ARGS.get(BROWSER_TYPE))
                page = await browser.new_page()
                await update_step(1)
                await page.goto(start_url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
                url_before_processing = page.url
                await update_step(2)
                success, _, _ = await process_single_photo_page(page, url_before_processing, Path(DOWNLOADS_DIR_BASE), 'main')
                if not success: await update_step(2, "`process_single_photo_page` zwróciło błąd.")
                await update_step(3)
                nav_success = await unstoppable_navigate(page, "ArrowLeft", url_before_processing, Text())
                if not nav_success: await update_step(3, "`unstoppable_navigate` nie powiodło się.")
                await update_step(4)
                await asyncio.sleep(1)
                url_after_navigation = page.url
                if url_after_navigation == url_before_processing: await update_step(4, "URL nie zmienił się po nawigacji.")
                
                step_statuses[steps[-1]] = "[bold green][ OK ] ✓[/bold green]"
                current_step_index = -1; test_passed = True
            except AssertionError as e: error_message = str(e)
            except Exception as e: error_message = f"Wystąpił nieoczekiwany wyjątek: {str(e)[:150]}"
            finally:
                if browser: await browser.close()
                root_logger.removeHandler(log_collector)
                for h in original_handlers: root_logger.addHandler(h)
            
            live.update(generate_live_layout(), refresh=True)
            await asyncio.to_thread(get_key)

    console.clear()
    if test_passed:
        console.print(Panel("✅ [bold green]Test integracyjny silnika ZALICZONY.[/]", border_style="green"))
    else:
        console.print(Panel(f"❌ [bold red]Test integracyjny silnika NIEZALICZONY.[/]\n[yellow]   Błąd w kroku: {error_message}[/yellow]", border_style="red"))


# ##############################################################################
# ===                    SEKCJA 2: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_test_suite():
    """
    Wyświetla i zarządza interaktywnym menu dla "Pakietu Testowego".
    """
    logger.info("Uruchamiam Pakiet Testowy...")
    
    menu_items = [
        ("Uruchom szybkie testy jednostkowe", run_unit_tests),
        ("Uruchom testy integracyjne silnika", run_engine_integration_tests),
        ("Przetestuj pojedynczy, ręcznie wpisany URL", run_single_url_test),
        ("Wróć do menu głównego", "exit")
    ]

    while True:
        console.clear()
        selected_action = await create_interactive_menu(
            menu_items,
            "🛡️ Pakiet Testowy 🛡️",
            border_style="green"
        )
        
        if selected_action == "exit" or selected_action is None:
            logger.info("Zamykanie Pakietu Testowego.")
            break
        
        await selected_action()
        
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter, aby wrócić do menu testów...[/]")
