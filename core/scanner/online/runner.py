# plik: core/scanner/online/runner.py
# Wersja 2.1 (Ostateczna) - Poprawiono wywołanie konstruktora ScannerLiveDisplay
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
from pathlib import Path
from contextlib import contextmanager
from typing import List

from rich.console import Console
from rich.logging import RichHandler

from ...config import WAIT_FOR_PAGE_LOAD, BATCH_SIZE
from ...database import get_urls_for_online_scan, update_scanned_entries_batch
from ...utils import stop_event
from .playwright_manager import PlaywrightManager
from .page_parser import get_advanced_photo_details_from_page
from ..ui.live_display import ScannerLiveDisplay

logger = logging.getLogger(__name__)

@contextmanager
def silence_loggers(logger_names: List[str]):
    """
    Tymczasowo wyłącza propagację dla wskazanych loggerów, aby nie
    zakłócały pracy `rich.Live`.
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

def _log_to_file(url: str, details: dict | None, status: str):
    log_path = Path("app_data/dziennik/advanced_scanner.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"--- {__import__('datetime').datetime.now().isoformat()} ---\nURL: {url}\nStatus: {status}\n")
        if details: f.write(json.dumps(details, ensure_ascii=False, indent=2))
        f.write("\n\n")

async def run_online_scanner(process_mode: str, run_headless: bool, input_file: str):
    title_map = {
        'full_scan': "Skanowanie Online: Dokańczanie",
        'retry_errors': "Skanowanie Online: Ponawianie Błędów",
        'force_refresh': "Skanowanie Online: Pełne Odświeżanie",
        'scan_all': f"Skanowanie Online z Pliku: {Path(input_file).name}",
        'scan_fix_file': f"Skanowanie Online z Pliku Naprawczego: {Path(input_file).name}"
    }
    scan_title = title_map.get(process_mode, "Skanowanie Online")
    
    urls_to_process = await _get_urls_for_processing(process_mode, input_file)
    if not urls_to_process:
        Console().print("[bold green]✅ Brak pracy do wykonania.[/]")
        return
    
    loggers_to_silence = [
        'core.scanner.online.page_parser',
        'core.scanner.online.playwright_manager'
    ]
    
    # === POCZĄTEK POPRAWKI ===
    scan_console = Console()
    # === KONIEC POPRAWKI ===
    
    with silence_loggers(loggers_to_silence):
        # === POCZĄTEK POPRAWKI ===
        with ScannerLiveDisplay(total_urls=len(urls_to_process), title=scan_title, console=scan_console) as display:
        # === KONIEC POPRAWKI ===
            try:
                # Logi wewnątrz tego bloku będą w 100% bezpieczne
                logger.info(f"Uruchamiam skaner online w trybie: {process_mode}")
                logger.info(f"Znaleziono {len(urls_to_process)} URL-i do przetworzenia.")

                async with PlaywrightManager(headless_mode=run_headless) as pm:
                    page = await pm.new_page()
                    results_batch = []
                    
                    for url in urls_to_process:
                        if stop_event.is_set():
                            break

                        display.update_current_url(url)
                        
                        photo_details, final_error = None, "Nieznany błąd"
                        try:
                            await page.goto(url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
                            photo_details = await get_advanced_photo_details_from_page(page, url)
                        except Exception as e:
                            final_error = str(e)

                        if photo_details:
                            display.update_success(url, photo_details)
                            results_batch.append({"url": url, "metadata_json": json.dumps(photo_details, ensure_ascii=False), "processing_status": "Sukces", "expected_path": photo_details.get('expected_path')})
                        else:
                            display.update_error(url, final_error)
                            results_batch.append({"url": url, "metadata_json": json.dumps({"error": final_error}, ensure_ascii=False), "processing_status": "Błąd", "expected_path": None})
                        
                        _log_to_file(url, photo_details, "Sukces" if photo_details else "Błąd")
                        
                        if len(results_batch) >= BATCH_SIZE:
                            await update_scanned_entries_batch(results_batch)
                            results_batch.clear()
                            display.set_batch_save_time()
                    
                    if results_batch:
                        await update_scanned_entries_batch(results_batch)
            except Exception as e:
                scan_console.print(f"[bold red]Wystąpił krytyczny błąd. Sprawdź logi.[/]")
                logging.getLogger(__name__).critical("Krytyczny błąd w pętli skanera", exc_info=True)

async def _get_urls_for_processing(process_mode: str, input_file: str) -> list[str]:
    if process_mode in ['scan_all', 'scan_fix_file']:
        url_file = Path(input_file);
        if not url_file.exists(): return []
        with open(url_file, "r", encoding="utf-8") as f: return [l.strip() for l in f if l.strip().startswith("http")]
    scan_type = {'retry_errors': 'retry_errors', 'force_refresh': 'force_refresh'}.get(process_mode, 'new_only')
    return await get_urls_for_online_scan(scan_type)
