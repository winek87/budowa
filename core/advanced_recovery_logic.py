# -*- coding: utf-8 -*-

# plik: core/advanced_recovery_logic.py
# Wersja 4.0 - W pełni asynchroniczny i zintegrowany Zaawansowany Silnik Naprawy
#
# ##############################################################################
# ===                MODUŁ ZAAWANSOWANEGO SILNIKA NAPRAWY                    ===
# ##############################################################################
#
# "Zaawansowany Silnik Naprawy" to narzędzie do rozwiązywania najbardziej
# uporczywych błędów pobierania. Dla każdego pliku oznaczonego jako błąd,
# wykonuje unikalną sekwencję "potrząśnięcia" stroną:
#
#  1. Nawiguje do strony z błędem.
#  2. Przechodzi do następnego lub poprzedniego zdjęcia, aby zmusić
#     przeglądarkę do odświeżenia swojego stanu.
#  3. Wraca na oryginalną stronę z błędem.
#  4. Uruchamia standardową procedurę pobierania z fallbackiem.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import shutil
import logging
from pathlib import Path
from collections import deque

# --- Zależności zewnętrzne (opcjonalne) ---
try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False
    
# --- Playwright ---
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.live import Live
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.prompt import Prompt, Confirm
from rich.layout import Layout

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import (
    BROWSER_TYPE, SESSION_DIR, DEFAULT_HEADLESS_MODE, BROWSER_ARGS,
    WAIT_FOR_PAGE_LOAD, NAV_ARROW_RIGHT_SELECTOR, NAV_ARROW_LEFT_SELECTOR,
    THREE_DOTS_MENU_SELECTOR, DOWNLOAD_OPTION_SELECTOR, WAIT_FOR_SELECTOR,
    WAIT_FOR_DOWNLOAD_START, DOWNLOADS_DIR_BASE
)
from .database import get_failed_urls_from_db, add_google_photo_entry
from .utils import get_date_from_metadata, create_unique_filepath, stop_event, LogCollectorHandler
from .config_editor_logic import get_key

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


async def run_advanced_recovery():
    """
    Uruchamia zaawansowany proces "ratowania" plików z logiką "potrząśnięcia".

    Proces:
    1.  Asynchronicznie pobiera z bazy listę nieudanych URL-i.
    2.  Dla każdego URL-a:
        a) Wykonuje sekwencję "potrząśnięcia" (nawigacja przód/tył).
        b) Próbuje pobrać plik (z fallbackiem na menu).
        c) Analizuje pobrany plik lokalnie za pomocą ExifTool w osobnym wątku.
        d) Zapisuje plik we właściwym folderze i asynchronicznie aktualizuje bazę.
    3.  Prezentuje finalne podsumowanie operacji.
    """
    console.clear()
    logger.info("Uruchamiam Zaawansowany Silnik Naprawy Błędów...")
    console.print(Panel("🌀 Zaawansowany Silnik Naprawy (Shake & Retry) 🌀", expand=False, style="bold magenta"))
    
    if not EXIFTOOL_AVAILABLE:
        logger.critical("Brak 'pyexiftool'. Operacja została przerwana.")
        console.print(Panel("[bold red]Błąd: 'pyexiftool'![/bold red]\nUruchom: [cyan]pip install pyexiftool[/cyan]", title="Instrukcja Instalacji"))
        return

    failed_urls = await get_failed_urls_from_db()
    if not failed_urls:
        logger.info("Nie znaleziono żadnych plików z błędami do naprawy.")
        console.print("\n[bold green]✅ Nie znaleziono żadnych plików z błędami do naprawy.[/bold green]")
        return

    console.print(f"\nZnaleziono [bold red]{len(failed_urls)}[/bold red] plików z błędami w bazie danych.", highlight=False)
    if not Confirm.ask("[cyan]Czy chcesz uruchomić zaawansowaną procedurę naprawczą?[/]"):
        logger.warning("Zaawansowana naprawa anulowana przez użytkownika."); return

    live_logs = deque(maxlen=15)
    log_collector = LogCollectorHandler(live_logs)
    root_logger = logging.getLogger(); original_handlers = root_logger.handlers[:]
    root_logger.handlers.clear(); root_logger.addHandler(log_collector)

    progress = Progress(TextColumn("[progress.description]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.1f}%",
                        "•", "Postęp: {task.completed}/{task.total}", "•", "Sukcesy: [green]{task.fields[succeeded]}",
                        "•", "Porażki: [red]{task.fields[failed]}")
    
    layout = Layout(name="root")
    layout.split_column(Layout(progress, name="header", size=3), Layout(Panel(Group(*live_logs), title="Logi na Żywo"), name="body"))

    context, page = None, None
    succeeded_count, failed_count = 0, 0
    try:
        async with async_playwright() as p:
            context = await getattr(p, BROWSER_TYPE).launch_persistent_context(Path(SESSION_DIR).expanduser(), headless=DEFAULT_HEADLESS_MODE,
                                                                                accept_downloads=True, args=BROWSER_ARGS.get(BROWSER_TYPE))
            page = await context.new_page()
            loop = asyncio.get_running_loop()

            with Live(layout, screen=True, auto_refresh=False, transient=False, vertical_overflow="visible") as live:
                task_id = progress.add_task("Naprawianie...", total=len(failed_urls), succeeded=0, failed=0)

                for url in failed_urls:
                    if stop_event.is_set(): logger.warning("Przerwano przez użytkownika."); break
                    
                    temp_path = None
                    try:
                        progress.update(task_id, description=f"Potrząsanie: ...{url[-40:]}"); live.refresh()
                        logger.info(f"Rozpoczynam sekwencję 'potrząśnięcia' dla ...{url[-40:]}")
                        await page.goto(url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
                        try:
                            await page.locator(NAV_ARROW_RIGHT_SELECTOR).click(timeout=5000); logger.debug("Nawigacja w prawo (shake) udana.")
                        except Exception:
                            await page.locator(NAV_ARROW_LEFT_SELECTOR).click(timeout=5000); logger.debug("Nawigacja w lewo (shake) udana.")
                        
                        await asyncio.sleep(2)
                        await page.go_back(wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
                        await asyncio.sleep(2)
                        logger.info("Sekwencja 'potrząśnięcia' zakończona.")

                        progress.update(task_id, description=f"Pobieranie: ...{url[-40:]}"); live.refresh()
                        try:
                            async with page.expect_download(timeout=WAIT_FOR_DOWNLOAD_START * 1000) as download_info:
                                await page.keyboard.press('Shift+D')
                            download = await download_info.value
                        except PlaywrightTimeoutError:
                            logger.warning("Pobieranie przez Shift+D nie powiodło się. Próbuję przez menu...")
                            async with page.expect_download(timeout=WAIT_FOR_DOWNLOAD_START * 1000) as download_info:
                                await page.click(THREE_DOTS_MENU_SELECTOR, timeout=WAIT_FOR_SELECTOR * 1000)
                                await page.click(DOWNLOAD_OPTION_SELECTOR, timeout=WAIT_FOR_SELECTOR * 1000)
                            download = await download_info.value

                        if failure_reason := await download.failure():
                            raise Exception(f"Pobieranie nie powiodło się: {failure_reason}")

                        temp_path = Path(await download.path())
                        original_filename = download.suggested_filename
                        with exiftool.ExifToolHelper() as et:
                            metadata_list = await loop.run_in_executor(None, et.get_metadata, str(temp_path))
                        
                        if not metadata_list: raise Exception("Nie udało się odczytać metadanych Exif.")
                        metadata = metadata_list[0]
                        
                        creation_date = await get_date_from_metadata(metadata)
                        if not creation_date: raise Exception("Nie udało się odczytać daty z metadanych pliku.")
                        
                        dest_dir = Path(DOWNLOADS_DIR_BASE) / str(creation_date.year) / f"{creation_date.month:02d}"
                        await asyncio.to_thread(dest_dir.mkdir, parents=True, exist_ok=True)
                        final_path = create_unique_filepath(dest_dir, original_filename)
                        await asyncio.to_thread(shutil.move, temp_path, final_path)

                        await add_google_photo_entry(url, final_path.name, final_path, metadata, 'downloaded', 0, None)
                        succeeded_count += 1
                        logger.info(f"Sukces: Pomyślnie naprawiono i pobrano plik: {final_path.name}")

                    except Exception as e:
                        logger.error(f"BŁĄD dla ...{url[-40:]}: {str(e)[:150]}...", exc_info=True)
                        failed_count += 1
                    finally:
                        if temp_path and await asyncio.to_thread(temp_path.exists):
                            try: await asyncio.to_thread(temp_path.unlink)
                            except OSError: pass
                        
                        progress.update(task_id, advance=1, succeeded=succeeded_count, failed=failed_count)
                        live.refresh()
                
                logger.info("Przetwarzanie zakończone. Naciśnij dowolny klawisz, aby zobaczyć podsumowanie.")
                await asyncio.to_thread(get_key)

    except Exception as e:
        if not stop_event.is_set():
            logger.critical("Wystąpił krytyczny błąd w Zaawansowanym Silniku Naprawczym.", exc_info=True)
    finally:
        if context: await context.close()
        root_logger.removeHandler(log_collector)
        for h in original_handlers: root_logger.addHandler(h)
            
        logger.info("Zaawansowana Naprawa zakończyła pracę.")
        console.clear()
        console.print(Panel(f"Operacja zaawansowanej naprawy zakończona.\n\n  - [green]Sukcesy:[/] {succeeded_count}\n  - [red]Porażki:[/] {failed_count}", title="Podsumowanie"))
