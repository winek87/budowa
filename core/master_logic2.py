# plik: core/master_logic.py (po Kroku 3)
# -*- coding: utf-8 -*-

# --- GŁÓWNE IMPORTY ---
import asyncio
import re
import logging
from pathlib import Path
from collections import deque
from typing import Tuple, Dict

# --- Playwright ---
from playwright.async_api import (
    async_playwright,
    Page,
    Download,
    TimeoutError as PlaywrightTimeoutError
)

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.progress import (
    Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn
)
from rich.live import Live
from rich.layout import Layout
from rich.table import Table
from rich.text import Text
from rich.prompt import Prompt

# --- IMPORTY Z WŁASNYCH MODUŁÓW `core` ---
from .advanced_scanner_logic import get_advanced_photo_details_from_page
from .config import (
    START_URL, METADATA_STRATEGY, ENABLE_SHAKE_THE_SCAN,
    SESSION_DIR, DOWNLOADS_DIR_BASE, MAX_RESTARTS_ON_FAILURE,
    RESTART_DELAY_SECONDS, BROWSER_TYPE, BROWSER_ARGS, BLOCKED_RESOURCE_TYPES,
    WAIT_FOR_SELECTOR, ENABLE_RESOURCE_BLOCKING, ENABLE_PAUSE_AFTER_REPAIR, DIRECTION_KEY
)
from .database import (
    set_state, get_state, add_google_photo_entry,
    get_url_status_from_db, get_retry_count_for_url, get_failed_urls_from_db
)
from .utils import (
    move_cursor_in_circles, stop_event,
    LogCollectorHandler, format_size_for_display
)
# NOWE IMPORTY Z NOWYCH MODUŁÓW W FOLDERZE `downloader`
from .downloader.page_navigator import unstoppable_navigate
from .downloader.file_processor import download_file_with_fallbacks, finalize_and_move_file


# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


# ##############################################################################
# ===                     SEKCJA 1: FUNKCJE POMOCNICZE SILNIKA                 ===
# ##############################################################################

async def block_unwanted_resources(route):
    """
    Przechwytuje i opcjonalnie blokuje żądania sieciowe strony.
    """
    resource_type = route.request.resource_type
    if resource_type in BLOCKED_RESOURCE_TYPES:
        await route.abort()
    else:
        await route.continue_()


def _create_summary_panel(url: str, status: str, metadata: Dict) -> Panel:
    """Tworzy estetyczny panel Rich podsumowujący wynik operacji dla jednego pliku."""
    status_map = {
        "downloaded": ("[bold green]✅ Pobrany Plik[/]", "green"),
        "skipped": ("[bold yellow]🟡 Pominięty (już w bazie)[/]", "yellow"),
        "failed": ("[bold red]❌ Błąd Przetwarzania[/]", "red")
    }
    title, border_style = status_map.get(status, (f"[bold]ℹ️ Status: {status}[/]", "default"))
    filename = Path(metadata.get('final_path') or metadata.get('FileName') or "Brak nazwy").name
    file_info_table = Table(box=None, show_header=False, padding=0)
    file_info_table.add_column(style="dim", width=15)
    file_info_table.add_column(style="cyan")
    file_info_table.add_row("Plik:", filename)
    if final_path := metadata.get('final_path'):
        try:
            display_path = Path(final_path).relative_to(DOWNLOADS_DIR_BASE)
        except ValueError:
            display_path = final_path
        file_info_table.add_row("Zapisano w:", f"{DOWNLOADS_DIR_BASE}/{display_path}")
    if dt := metadata.get('DateTime'):
        file_info_table.add_row("Data:", dt)
    if size := metadata.get('size'):
        file_info_table.add_row("Rozmiar:", format_size_for_display(size))
    if dims := metadata.get('Dimensions'):
        file_info_table.add_row("Wymiary:", dims)
    if camera := metadata.get('Camera'):
        file_info_table.add_row("Aparat:", camera)
    online_metadata_table = Table(box=None, show_header=False, padding=0)
    online_metadata_table.add_column(style="dim", width=15)
    online_metadata_table.add_column(style="cyan")
    has_online_meta = False
    if description := metadata.get('Description'):
        display_desc = (description[:40] + '...') if len(description) > 43 else description
        online_metadata_table.add_row("Opis:", display_desc.replace('\n', ' '))
        has_online_meta = True
    if people := metadata.get('TaggedPeople'):
        online_metadata_table.add_row("Osoby:", ", ".join(people))
        has_online_meta = True
    if albums := metadata.get('Albums'):
        online_metadata_table.add_row("Albumy:", ", ".join(albums))
        has_online_meta = True
    if location := metadata.get('Location'):
        online_metadata_table.add_row("Lokalizacja:", location)
        has_online_meta = True
    content_group = [file_info_table]
    if has_online_meta:
        content_group.append("\n[dim]-- Metadane ze strony --[/dim]")
        content_group.append(online_metadata_table)
    short_url = url[:45] + "..." + url[-15:] if len(url) > 60 else url
    return Panel(
        Group(*content_group),
        title=title,
        subtitle=f"[dim link={url}]{short_url}[/dim link]",
        border_style=border_style,
        subtitle_align="right"
    )

async def _confirm_page_loaded(page: Page, current_url: str) -> str:
    """Krok 1: Wizualnie potwierdza załadowanie strony i zwraca ID zdjęcia."""
    logger.info(f"Weryfikacja załadowania strony: ...{current_url[-40:]}")
    photo_id_match = re.search(r'(AF1Qip[\w-]+)', current_url)
    if not photo_id_match:
        raise ValueError("Nie można wyodrębnić ID zdjęcia z URL.")
    photo_id = photo_id_match.group(1)

    main_container_selector = f"[data-media-key='{photo_id}']"
    await page.wait_for_selector(main_container_selector, state='visible', timeout=WAIT_FOR_SELECTOR * 1000)
    await asyncio.sleep(1)
    logger.info(f"Wizualne potwierdzenie dla zdjęcia {photo_id[:10]}... OK.")
    return photo_id

# ##############################################################################
# ===               SEKCJA 2: GŁÓWNA FUNKCJA PROCESORA PLIKÓW                  ===
# ##############################################################################

async def process_single_photo_page(page: Page, current_url: str, scan_mode: str) -> tuple[bool, str, dict]:
    """Główny orkiestrator, który wywołuje poszczególne kroki przetwarzania strony."""
    metadata_from_page = {}
    try:
        await _confirm_page_loaded(page, current_url)

        if METADATA_STRATEGY in ['HYBRID', 'ONLINE_ONLY']:
            metadata_from_page = await get_advanced_photo_details_from_page(page, current_url) or {}

        # Wywołanie funkcji z nowego modułu
        download = await download_file_with_fallbacks(page)
        if await download.failure():
            raise IOError(f"Pobieranie nie powiodło się: {await download.failure()}")

        # Wywołanie funkcji z nowego modułu
        final_path, final_metadata = await finalize_and_move_file(
            download, metadata_from_page, scan_mode
        )

        final_metadata['final_path'] = str(final_path)
        try:
            final_metadata['size'] = (await asyncio.to_thread(final_path.stat)).st_size
        except (OSError, FileNotFoundError):
            pass

        expected_path_str = str(final_path.parent / final_metadata['FileName'])
        await add_google_photo_entry(
            url=current_url, filename=final_path.name, final_path=final_path,
            metadata=final_metadata, status='downloaded', retry_count=0,
            expected_path=expected_path_str, processing_status='Sukces'
        )
        return True, "downloaded", final_metadata

    except Exception as e:
        logger.error(f"Przetwarzanie strony ...{current_url[-40:]} nie powiodło się. Błąd: {e}", exc_info=False)
        status_to_save = 'skipped' if metadata_from_page else 'failed'
        retry_count = 0 if status_to_save == 'skipped' else (await get_retry_count_for_url(current_url)) + 1

        error_metadata = metadata_from_page.copy()
        error_metadata['error_message'] = str(e)

        await add_google_photo_entry(current_url, "processing_failed", Path(""), error_metadata, status_to_save, retry_count, None)
        return status_to_save == 'skipped', status_to_save, error_metadata

# ##############################################################################
# ===               SEKCJA 3: NARZĘDZIA DO RĘCZNEJ NAPRAWY BŁĘDÓW              ===
# ##############################################################################

async def interactive_retry_failed_files():
    """Uruchamia interaktywne narzędzie do ponawiania pobierania dla plików, które wcześniej zakończyły się błędem."""
    console.clear()
    console.print(Panel("[bold yellow]🛠️ Interaktywne Narzędzie do Ponawiania Błędów 🛠️[/]", border_style="yellow"))
    failed_urls = await get_failed_urls_from_db()
    if not failed_urls:
        console.print("\n[bold green]✅ Gratulacje! Nie znaleziono żadnych plików z błędami do naprawy.[/bold green]")
        return
    
    from .utils import _interactive_file_selector
    selected_urls = await _interactive_file_selector(failed_urls, "Wybierz URL-e do ponowienia")

    if not selected_urls:
        logger.warning("Nie wybrano żadnych URL-i do ponowienia.")
        return

    # Używamy menedżera restartów, aby obsłużyć ewentualne awarie przeglądarki
    shared_restart_manager = {'count': 0}
    for i, url in enumerate(selected_urls):
        if shared_restart_manager['count'] > MAX_RESTARTS_ON_FAILURE:
            console.print("[bold red]Osiągnięto globalny limit restartów. Przerywam pracę.[/bold red]"); break
        
        console.print(Panel(f"Praca nad plikiem [bold]{i + 1}/{len(selected_urls)}[/bold]\nURL: [dim]{url}[/dim]\n\n[cyan]Silnik zostanie uruchomiony w trybie widocznym (`headless=False`).[/cyan]", title="Ponawianie Błędu", style="yellow"))
        await run_with_restarts(scan_mode='single_retry', headless_mode=False, single_url_to_process=url, restart_manager=shared_restart_manager)
    
    console.print("\n[bold green]✅ Zakończono proces ponawiania błędów.[/bold green]")


async def run_single_file_download():
    """Uruchamia interaktywny proces pobierania pojedynczego pliku z podanego URL."""
    console.clear()
    console.print(Panel("[bold cyan]📥 Pobieranie Pojedynczego Pliku 📥[/bold cyan]", border_style="cyan"))
    url = Prompt.ask("\n[bold]Wklej adres URL zdjęcia lub filmu, który chcesz pobrać[/bold]")
    if not url.strip().startswith("http"):
        console.print("[bold red]To nie jest prawidłowy adres URL. Anuluję.[/bold red]"); return
    
    console.print(Panel(f"Rozpoczynam pracę nad URL: [dim]{url}[/dim]\n\n[cyan]Silnik zostanie uruchomiony w trybie widocznym (`headless=False`).[/cyan]", title="Pobieranie w toku...", style="green"))
    await run_with_restarts(scan_mode='single_retry', headless_mode=False, single_url_to_process=url)
    console.print("\n[bold green]✅ Pobieranie pojedynczego pliku zakończone.[/bold green]")


# ##############################################################################
# ===                     SEKCJA 4: GŁÓWNA PĘTLA WYKONAWCZA SILNIKA            ===
# ##############################################################################

async def run_master_downloader(scan_mode: str, retry_failed: bool, headless_mode: bool, single_url_to_process: str = None) -> bool:
    """
    Główna, kompleksowa pętla robocza dla Silnika Master.
    """
    if scan_mode == 'single_retry': start_url = single_url_to_process
    elif scan_mode == 'main': start_url = await get_state('last_scan_url') or START_URL
    else: await set_state('last_forced_scan_url', START_URL); start_url = START_URL
    
    log_collector_deque = deque(maxlen=20)
    log_collector_handler = LogCollectorHandler(log_collector_deque)
    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]
    root_logger.handlers.clear()
    root_logger.addHandler(log_collector_handler)

    recent_summaries = deque(maxlen=5)
    progress = Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", "•", TextColumn("Pobrane: [green]{task.fields[pobrane]}[/]"), "•", TextColumn("Pominięte: [yellow]{task.fields[pominiete]}[/]"), "•", TextColumn("Błędy: [red]{task.fields[bledy]}[/]"), "•", TimeRemainingColumn())
    status_text = Text("Inicjalizacja...", justify="center")

    summary_panel = Panel(Group(*recent_summaries), title="Podsumowanie Ostatnich Operacji", border_style="green")
    status_panel = Panel(status_text, title="Aktualny Status", border_style="cyan")
    layout = Layout(name="root")
    layout.split_column(
        Layout(progress, name="progress", size=3),
        Layout(summary_panel, name="summaries"),
        Layout(status_panel, name="footer", size=3)
    )

    clean_exit = False
    context, page, cursor_task = None, None, None
    try:
        async with async_playwright() as p:
            with Live(layout, screen=True, auto_refresh=False, transient=True, vertical_overflow="visible") as live:
                task_id = progress.add_task("Postęp", total=None, pobrane=0, pominiete=0, bledy=0)
                
                from .session_logic import check_login_status
                context = await getattr(p, BROWSER_TYPE).launch_persistent_context(Path(SESSION_DIR).expanduser(), headless=headless_mode, accept_downloads=True, args=BROWSER_ARGS.get(BROWSER_TYPE))
                page = await context.new_page()
                await check_login_status(page)
                
                await page.bring_to_front()
                cursor_task = asyncio.create_task(move_cursor_in_circles(page, stop_event, headless_mode))
                if ENABLE_RESOURCE_BLOCKING: await page.route("**/*", block_unwanted_resources)
                
                if retry_failed:
                    failed_urls = await get_failed_urls_from_db()
                    if failed_urls:
                        progress.update(task_id, total=len(failed_urls), description="[bold yellow]Naprawianie błędów[/]")
                        pobrane, pominiete, bledy = 0, 0, 0
                        for url in failed_urls:
                            if stop_event.is_set(): break
                            if ENABLE_SHAKE_THE_SCAN:
                                await page.goto(url); await page.keyboard.press(DIRECTION_KEY); await asyncio.sleep(2); await page.keyboard.press("ArrowRight" if DIRECTION_KEY == "ArrowLeft" else "ArrowLeft"); await asyncio.sleep(2)
                            else: await page.goto(url)
                            success, status, metadata = await process_single_photo_page(page, page.url, scan_mode)
                            recent_summaries.appendleft(_create_summary_panel(url, status, metadata))
                            if success:
                                if status == 'downloaded': pobrane += 1
                                elif status == 'skipped': pominiete += 1
                            else: bledy += 1
                            progress.update(task_id, advance=1, pobrane=pobrane, pominiete=pominiete, bledy=bledy)
                            layout["summaries"].update(Panel(Group(*recent_summaries), title="Podsumowanie Ostatnich Operacji", border_style="green"))
                            live.refresh()
                        if not stop_event.is_set() and ENABLE_PAUSE_AFTER_REPAIR: live.stop(); Prompt.ask("\n[bold green]✅ Zakończono naprawę błędów. Naciśnij Enter...[/]"); live.start(refresh=True)
                
                progress.update(task_id, total=None, description="[bold green]Główny skan[/]")
                await page.goto(start_url)
                
                if scan_mode == 'single_retry':
                    success, status, metadata = await process_single_photo_page(page, start_url, scan_mode)
                    live.stop()
                    console.print(_create_summary_panel(start_url, status, metadata))
                    return True
                
                pobrane, pominiete, bledy = 0, 0, 0
                while not stop_event.is_set():
                    current_url = page.url
                    status_in_db = await get_url_status_from_db(current_url)
                    if scan_mode == 'forced' or status_in_db not in ('downloaded', 'skipped'):
                        success, status, metadata = await process_single_photo_page(page, current_url, scan_mode)
                        recent_summaries.appendleft(_create_summary_panel(current_url, status, metadata))
                        if success:
                            if status == 'downloaded': pobrane += 1
                            elif status == 'skipped': pominiete += 1
                        else: bledy += 1
                    else:
                        pominiete += 1
                        recent_summaries.appendleft(_create_summary_panel(current_url, "skipped", {}))
                    
                    progress.update(task_id, pobrane=pobrane, pominiete=pominiete, bledy=bledy)
                    layout["summaries"].update(Panel(Group(*recent_summaries), title="Podsumowanie Ostatnich Operacji", border_style="green"))
                    live.refresh()
                    
                    if not await unstoppable_navigate(page, current_url, status_text): break
                    
                    if scan_mode == 'main': await set_state('last_scan_url', page.url)
                    elif scan_mode == 'forced': await set_state('last_forced_scan_url', page.url)
        clean_exit = True
    except Exception as e:
        if not stop_event.is_set(): logger.critical("Wystąpił nieobsługiwany błąd w głównej pętli.", exc_info=True)
        clean_exit = False
    finally:
        if cursor_task and not cursor_task.done(): cursor_task.cancel()
        if context:
            try: await context.close()
            except Exception: pass
        root_logger.removeHandler(log_collector_handler)
        for h in original_handlers:
            root_logger.addHandler(h)
    return clean_exit

# ##############################################################################
# ===           SEKCJA 5: "NIEŚMIERTELNY" MENEDŻER URUCHOMIENIA              ===
# ##############################################################################
async def run_with_restarts(
    scan_mode: str, retry_failed: bool = False, headless_mode: bool = False,
    single_url_to_process: str = None, restart_manager: dict = None
):
    """"Nieśmiertelny" menedżer, który restartuje główną pętlę roboczą w przypadku awarii."""
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
