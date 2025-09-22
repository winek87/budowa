# plik: core/downloader/runner.py
# Wersja 1.2 - Zintegrowano dynamiczne raportowanie statusu za pomocą callbacku.
# Opis: Ten moduł zawiera główną pętlę pobierania, logikę ponawiania
#       po awarii oraz zarządza dashboardem na żywo.
# -*- coding: utf-8 -*-

import asyncio
import logging
from pathlib import Path

from playwright.async_api import async_playwright

from rich.console import Console
from rich.panel import Panel

from ..config import (
    START_URL, ENABLE_SHAKE_THE_SCAN, SESSION_DIR, MAX_RESTARTS_ON_FAILURE,
    RESTART_DELAY_SECONDS, BROWSER_TYPE, BROWSER_ARGS, ENABLE_RESOURCE_BLOCKING,
    ENABLE_PAUSE_AFTER_REPAIR, DIRECTION_KEY
)
from ..database import (
    set_state, get_state, get_url_status_from_db, get_failed_urls_from_db
)
from ..utils import move_cursor_in_circles, stop_event
from .utils import silence_loggers
from .playwright_manager import block_unwanted_resources
from .page_navigator import unstoppable_navigate
from .page_processor import process_single_photo_page
from .ui.live_display import DownloaderLiveDisplay

console = Console()
logger = logging.getLogger(__name__)


async def run_master_downloader(scan_mode: str, retry_failed: bool, headless_mode: bool, single_url_to_process: str = None) -> bool:
    """
    Główna, kompleksowa pętla robocza dla Silnika Master z ujednoliconym UI.
    """
    if scan_mode == 'single_retry': start_url = single_url_to_process
    elif scan_mode == 'main': start_url = await get_state('last_scan_url') or START_URL
    else: await set_state('last_forced_scan_url', START_URL); start_url = START_URL

    clean_exit = False
    context, page, cursor_task = None, None, None
    
    loggers_to_silence = [
        'core.scanner.online.page_parser', 'core.downloader.page_processor',
        'core.downloader.file_processor', 'core.downloader.page_navigator'
    ]

    try:
        async with async_playwright() as p:
            with silence_loggers(loggers_to_silence):
                failed_urls_count = len(await get_failed_urls_from_db()) if retry_failed else 0
                title = "Silnik Pobierania: Naprawa Błędów" if retry_failed and failed_urls_count > 0 else "Silnik Pobierania Master"

                with DownloaderLiveDisplay(total_items=failed_urls_count, title=title, console=console) as display:
                    # Definiujemy nasz callback, który będzie przekazywany dalej
                    status_callback = display.update_status_message

                    context = await getattr(p, BROWSER_TYPE).launch_persistent_context(Path(SESSION_DIR).expanduser(), headless=headless_mode, accept_downloads=True, args=BROWSER_ARGS.get(BROWSER_TYPE))
                    page = await context.new_page()
                    await page.bring_to_front()
                    cursor_task = asyncio.create_task(move_cursor_in_circles(page, stop_event, headless_mode))
                    if ENABLE_RESOURCE_BLOCKING: await page.route("**/*", block_unwanted_resources)

                    if retry_failed:
                        failed_urls = await get_failed_urls_from_db()
                        if failed_urls:
                            for url in failed_urls:
                                if stop_event.is_set(): break
                                
                                display.update_current_url(url)
                                status_callback(f"Nawigacja do URL do naprawy...")
                                await page.goto(url, wait_until='load')
                                
                                success, status, metadata = await process_single_photo_page(
                                    page, page.url, scan_mode, status_callback
                                )
                                
                                if status == 'downloaded': display.update_downloaded(url, metadata)
                                elif status == 'skipped': display.update_skipped(url, metadata)
                                else: display.update_failed(url, metadata)
                            
                            if not stop_event.is_set() and ENABLE_PAUSE_AFTER_REPAIR:
                                status_callback("Zakończono naprawę. Naciśnij Enter...")
                                await asyncio.to_thread(input)

                    display.progress_bar.update(display._task_id, total=None, description="[bold green]Główny skan[/]", completed=0, pobrane=0, pominiete=0, bledy=0)
                    status_callback("Nawigacja do adresu startowego...")
                    await page.goto(start_url, wait_until='load')

                    if scan_mode == 'single_retry':
                        success, status, metadata = await process_single_photo_page(page, start_url, scan_mode, status_callback)
                        if status == 'downloaded': display.update_downloaded(start_url, metadata)
                        else: display.update_failed(start_url, metadata)
                        await asyncio.sleep(1); return True

                    while not stop_event.is_set():
                        current_url = page.url
                        display.update_current_url(current_url)

                        status_in_db = await get_url_status_from_db(current_url)
                        if scan_mode == 'forced' or status_in_db not in ('downloaded', 'skipped'):
                            success, status, metadata = await process_single_photo_page(
                                page, current_url, scan_mode, status_callback
                            )
                            if status == 'downloaded': display.update_downloaded(current_url, metadata)
                            elif status == 'skipped': display.update_skipped(current_url, metadata)
                            else: display.update_failed(current_url, metadata)
                        else:
                            display.update_skipped(current_url, {})

                        if not await unstoppable_navigate(page, current_url, status_callback):
                            break

                        if scan_mode == 'main': await set_state('last_scan_url', page.url)
                        elif scan_mode == 'forced': await set_state('last_forced_scan_url', page.url)
        clean_exit = True
    except Exception as e:
        if not stop_event.is_set(): logger.critical("Wystąpił nieobsługiwany błąd w głównej pętli.", exc_info=True)
    finally:
        if cursor_task and not cursor_task.done(): cursor_task.cancel()
        if context:
            try: await context.close()
            except Exception: pass
    return clean_exit


async def run_with_restarts(scan_mode: str, retry_failed: bool = False, headless_mode: bool = False, single_url_to_process: str = None, restart_manager: dict = None):
    if restart_manager is None: restart_manager = {'count': 0}
    while restart_manager['count'] <= MAX_RESTARTS_ON_FAILURE:
        is_clean_exit = await run_master_downloader(scan_mode, retry_failed, headless_mode, single_url_to_process)
        if is_clean_exit:
            if scan_mode != 'single_retry' and not stop_event.is_set(): logger.info("Sesja Master zakończona czysto.")
            break
        restart_manager['count'] += 1
        if restart_manager['count'] > MAX_RESTARTS_ON_FAILURE:
            console.print(Panel("🔥 [bold red]BŁĄD KRYTYCZNY[/]\n\nOsiągnięto maksymalną liczbę restartów.", border_style="red"))
            break
        logger.error(f"Krytyczna awaria silnika. Restart za {RESTART_DELAY_SECONDS}s... (Próba {restart_manager['count']}/{MAX_RESTARTS_ON_FAILURE})")
        await asyncio.sleep(RESTART_DELAY_SECONDS)
        retry_failed = False
