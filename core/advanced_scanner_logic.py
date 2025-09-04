# plik: core/advanced_scanner_logic.py
# Wersja 7.0 - Scentralizowana logika bazy danych i ujednolicone ścieżki (Refaktoryzacja Fazy 1)

# -*- coding: utf-8 -*-

# plik: core/advanced_scanner_logic.py
# Wersja 6.0 - W pełni asynchroniczny, udokumentowany i zintegrowany z nowym modułem bazy danych
#
# ##############################################################################
# ===                        JAK TO DZIAŁA (PROSTE WYJAŚNIENIE)                ===
# ##############################################################################
#
# Ten plik zawiera logikę zaawansowanego, wielofunkcyjnego narzędzia do
# zarządzania metadanymi i spójnością kolekcji. Pełni trzy główne role:
#
#  1. SKANER ONLINE: Pobiera bogate metadane (opisy, albumy, tagi, GPS)
#     bezpośrednio ze strony Google Photos i oblicza OCZEKIWANĄ, idealną
#     lokalizację pliku (`expected_path`) na dysku.
#
#  2. KOREKTOR OFFLINE: Porównuje rzeczywistą lokalizację pobranych plików
#     z ich oczekiwaną lokalizacją i pozwala na ich automatyczną naprawę.
#
#  3. ZAPISYWARKA EXIF: Odczytuje metadane z bazy danych i zapisuje je
#     bezpośrednio w plikach na dysku.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import json
import re
import shutil
import logging
from collections import deque
from datetime import datetime
from pathlib import Path
import aiosqlite

# --- Zależności zewnętrzne (opcjonalne) ---
try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False
    
# --- Playwright ---
from playwright.async_api import async_playwright, Page

# --- Importy z `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.live import Live
from rich.layout import Layout
from rich.table import Table
from rich.text import Text
from rich.prompt import Confirm, Prompt

# --- IMPORTY Z WŁASNYCH MODUŁÓW `core` ---
# Jawne importy z pliku konfiguracyjnego
from .config import (
    DATABASE_FILE, SESSION_DIR, DOWNLOADS_DIR_BASE, URL_INPUT_FILE,
    WAIT_FOR_SELECTOR, WAIT_FOR_PAGE_LOAD, BROWSER_TYPE, BROWSER_ARGS,
    ENABLE_RESOURCE_BLOCKING, INFO_PANEL_BUTTON_SELECTOR,
    DEFAULT_HEADLESS_MODE, BLOCKED_RESOURCE_TYPES
)

# NOWE, SCENTRALIZOWANE IMPORTY Z MODUŁU BAZY DANYCH
from .database import (
    setup_database,
    get_urls_for_online_scan,
    update_scanned_entries_batch,
    get_urls_to_fix,
    get_all_urls_from_db,
    get_records_for_path_correction,
    update_final_path,
    get_records_for_filename_fix,
    update_entry_after_rename,
    get_records_for_metadata_completion,
    update_entry_with_completed_metadata,
    get_records_for_exif_writing
)

from .utils import stop_event, get_date_from_metadata, create_unique_filepath, create_interactive_menu
from .config_editor_logic import get_key

# --- INICJALIZACJA I KONFIGURACJA MODUŁU ---
console = Console(record=True)
logger = logging.getLogger(__name__)

# Definicje stałych dla plików logów i postępu
LOG_FILE = Path("app_data/dziennik/advanced_scanner.log")
BATCH_SIZE = 50 # Liczba wyników zapisywanych do bazy w jednej transakcji


# ##############################################################################
# ===            SEKCJA 1: FUNKCJE POMOCNICZE I NARZĘDZIA MENU               ===
# ##############################################################################


async def export_fix_needed_urls_to_file():
    """
    Eksportuje do pliku `urls_to_fix.txt` listę adresów URL, które wymagają
    ponownego skanowania w celu uzupełnienia brakujących metadanych.
    """
    FIX_URL_FILE = Path("urls_to_fix.txt")
    console.clear()
    logger.info(f"Rozpoczynam eksport URL-i wymagających naprawy do pliku '{FIX_URL_FILE.name}'...")
    console.print(Panel(f"📦 Eksport URL-i do Naprawy do Pliku '{FIX_URL_FILE.name}'", expand=False, style="blue"))

    try:
        urls_to_fix = await get_urls_to_fix()

        if not urls_to_fix:
            logger.info("Nie znaleziono żadnych wpisów wymagających naprawy metadanych.")
            console.print("\n[bold green]✅ Wygląda na to, że wszystkie metadany w bazie są kompletne.[/bold green]")
            return

        with open(FIX_URL_FILE, "w", encoding="utf-8") as f:
            for url in urls_to_fix:
                f.write(f"{url}\n")
        
        logger.info(f"Sukces! Wyeksportowano {len(urls_to_fix)} adresów URL do pliku.")
        console.print(f"\n[bold green]✅ Pomyślnie zapisano {len(urls_to_fix)} URL-i w pliku:[/bold green]")
        console.print(f"[cyan]{FIX_URL_FILE.resolve()}[/cyan]")

    except Exception as e:
        logger.critical(f"Wystąpił krytyczny błąd podczas eksportu URL-i do naprawy: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd. Sprawdź plik logu, aby uzyskać więcej informacji.[/bold red]")


async def block_unwanted_resources(route):
    """
    Przechwytuje i opcjonalnie blokuje żądania sieciowe strony.
    """
    resource_type = route.request.resource_type
    if resource_type in BLOCKED_RESOURCE_TYPES:
        logger.debug(f"Blokuję zasób typu '{resource_type}': {route.request.url[:80]}...")
        await route.abort()
    else:
        await route.continue_()


async def export_urls_from_db_to_file():
    """
    Eksportuje wszystkie adresy URL z tabeli `downloaded_media` w bazie
    danych do pliku tekstowego zdefiniowanego w `config.py`.
    """
    console.clear()
    logger.info(f"Rozpoczynam eksport wszystkich adresów URL do pliku '[bold cyan]{URL_INPUT_FILE}[/bold cyan]'...", extra={"markup": True})
    console.print(Panel(f"📦 Eksport Wszystkich URL-i z Bazy do Pliku", expand=False, style="blue"))

    try:
        urls = await get_all_urls_from_db()
        
        if not urls:
            logger.warning("Baza danych jest pusta lub nie zawiera żadnych adresów URL.")
            console.print("\n[bold yellow]Nie znaleziono żadnych adresów URL w bazie danych do wyeksportowania.[/bold yellow]")
            return
            
        output_file = Path(URL_INPUT_FILE)
        with open(output_file, "w", encoding="utf-8") as f:
            for url in urls:
                f.write(f"{url}\n")
                
        logger.info(f"Sukces! Wyeksportowano {len(urls)} adresów URL do pliku.")
        console.print(f"\n[bold green]✅ Pomyślnie zapisano {len(urls)} URL-i w pliku:[/bold green]")
        console.print(f"[cyan]{output_file.resolve()}[/cyan]")
        
    except Exception as e:
        logger.critical(f"Wystąpił krytyczny błąd podczas eksportu adresów URL: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd. Sprawdź plik logu, aby uzyskać więcej informacji.[/bold red]")


async def get_urls_for_processing(process_mode: str, input_file: str = URL_INPUT_FILE) -> list[str] | None:
    """
    Przygotowuje listę adresów URL do przetworzenia w zależności od trybu pracy.
    """
    logger.info(f"Przygotowuję listę URL-i do przetworzenia w trybie: [bold]{process_mode}[/bold]", extra={"markup": True})

    url_file = Path(input_file)
    if process_mode in ['scan_all', 'scan_fix_file']:
        try:
            if not url_file.exists():
                logger.warning(f"Nie znaleziono pliku wejściowego '{url_file}'.")
                console.print(f"\n[bold yellow]Plik '{url_file}' nie istnieje. Użyj opcji eksportu w menu, aby go utworzyć.[/bold yellow]")
                return []
            with open(url_file, "r", encoding="utf-8") as f:
                urls = [line.strip() for line in f if line.strip().startswith("http")]
            if not urls:
                logger.warning(f"Plik '{url_file}' jest pusty lub nie zawiera prawidłowych linków.")
            else:
                logger.info(f"Znaleziono {len(urls)} URL-i w pliku '{url_file.name}'.")
            return urls
        except Exception as e:
            logger.critical(f"BŁĄD: Nie można odczytać pliku '{url_file}': {e}", exc_info=True)
            return None

    try:
        scan_type_map = {
            'retry_errors': 'retry_errors', 'force_refresh': 'force_refresh',
            'full_scan': 'new_only'
        }
        scan_type = scan_type_map.get(process_mode, 'new_only')
        
        urls_from_db = await get_urls_for_online_scan(scan_type)
        logger.info(f"Znaleziono {len(urls_from_db)} URL-i w bazie danych pasujących do kryteriów.")
        return urls_from_db
    except Exception as e:
        logger.critical(f"BŁĄD: Nie można pobrać danych z bazy: {e}", exc_info=True)
        return None


def log_to_file(url: str, details: dict, status: str):
    """
    Zapisuje szczegółowy log pojedynczej operacji do pliku tekstowego.
    """
    try:
        log_path = Path(LOG_FILE)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"--- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n")
            f.write(f"URL: {url}\n")
            f.write(f"Status: {status}\n")
            if details:
                f.write(json.dumps(details, ensure_ascii=False, indent=4))
            f.write("\n\n")
            
        logger.debug(f"Zapisano wpis dla URL ...{url[-40:]} do pliku logu '{log_path.name}'.")

    except Exception as e:
        logger.error(f"Nie udało się zapisać do pliku logu '{LOG_FILE}': {e}", exc_info=True)


# ##############################################################################
# ===            SEKCJA 2: GŁÓWNA LOGIKA SKANERA I KOREKTORÓW                ===
# ##############################################################################

async def get_advanced_photo_details_from_page(page: Page, current_url: str) -> dict | None:
    """
    Skaner Online: Pobiera wszystkie zaawansowane metadane ze strony zdjęcia
    i oblicza OCZEKIWANĄ ścieżkę zapisu (`expected_path`).
    """
    # ... (kod tej funkcji pozostaje bez zmian, ponieważ nie zawiera zapytań SQL) ...
    months_map = {
        'sty': 1, 'lut': 2, 'mar': 3, 'kwi': 4, 'maj': 5, 'cze': 6,
        'lip': 7, 'sie': 8, 'wrz': 9, 'paź': 10, 'lis': 11, 'gru': 12
    }
    
    async def get_attribute_safely(locator, attribute='aria-label'):
        """Bezpiecznie pobiera atrybut, aby uniknąć błędów."""
        try:
            return await locator.get_attribute(attribute, timeout=1000)
        except Exception:
            return None

    async def _scan_page_content():
        """Wykonuje pojedynczą próbę skanowania zawartości strony."""
        logger.debug(f"Rozpoczynam skanowanie zawartości strony dla URL: ...{current_url[-40:]}")
        scan_results = {}
        
        photo_id_match = re.search(r'AF1Qip[\w-]+', current_url)
        if not photo_id_match:
            logger.warning(f"Nie udało się wyodrębnić ID zdjęcia z URL: {current_url}")
            return None
        photo_id = photo_id_match.group(0)

        # Krok 1: Otwórz panel boczny (jeśli jest zamknięty)
        info_panel_selector = f"c-wiz[jslog*='{photo_id}']"
        try:
            if not await page.is_visible(info_panel_selector):
                await page.click(INFO_PANEL_BUTTON_SELECTOR, timeout=WAIT_FOR_SELECTOR * 1000)
                await page.wait_for_selector(info_panel_selector, timeout=WAIT_FOR_SELECTOR * 1000, state="visible")
                logger.debug("Panel boczny został pomyślnie otwarty.")
        except Exception:
            logger.warning("Nie udało się otworzyć panelu bocznego. Metadane mogą być niekompletne.")

        wiz_element = page.locator(info_panel_selector).first

        # Krok 2: Ekstrakcja stabilnych danych
        
        # Data z panelu bocznego i atrybutu aria-label
        date_from_panel, date_from_aria = None, None
        # ... (ta logika pozostaje bez zmian) ...
        date_text_locator = wiz_element.locator(".R9U8ab")
        if await date_text_locator.count() > 0:
            date_text = await date_text_locator.first.inner_text()
            match = re.search(r'(\d{1,2})\s+([a-zA-Z]{3})\s+(\d{4}),\s+(\d{2}:\d{2})', date_text)
            if match:
                day, month_str, year, time_str = match.groups()
                if month := months_map.get(month_str.lower()):
                    hour, minute = map(int, time_str.split(':'))
                    date_from_panel = datetime(int(year), month, int(day), hour, minute)
                    scan_results["DateTime_Panel"] = date_from_panel.isoformat()
                    logger.debug(f"Znaleziono datę w panelu bocznym: {date_from_panel.isoformat()}")

        main_image_locator = page.locator("img.BiCYpc[aria-label]")
        if await main_image_locator.count() > 0:
            aria_label = await main_image_locator.first.get_attribute('aria-label')
            if aria_label:
                match_aria = re.search(r'(\d{1,2})\s+([a-zA-Z]{3})\s+(\d{4}),\s+(\d{2}:\d{2}:\d{2})', aria_label, re.IGNORECASE)
                if match_aria:
                    day, month_str, year, time_str = match_aria.groups()
                    if month := months_map.get(month_str.lower()):
                        h, m, s = map(int, time_str.split(':'))
                        date_from_aria = datetime(int(year), month, int(day), h, m, s)
                        scan_results["DateTime_AriaLabel"] = date_from_aria.isoformat()
                        logger.debug(f"Znaleziono datę w atrybucie aria-label: {date_from_aria.isoformat()}")

        if date_from_panel:
            scan_results["DateTime"] = date_from_panel.isoformat()
            scan_results["DateTimeSource"] = "Panel Boczny"
        elif date_from_aria:
            scan_results["DateTime"] = date_from_aria.isoformat()
            scan_results["DateTimeSource"] = "Atrybut 'aria-label'"
        
        # Pobieranie pozostałych metadanych
        pairs = {
            "FileName": "div.R9U8ab[aria-label^='Nazwa pliku:']",
            "Camera": "div.R9U8ab[aria-label^='Nazwa aparatu:']",
            "Location": "div.R9U8ab[aria-label='Lokalizacja']",
            "Dimensions": "span[aria-label^='Rozmiar w pikselach']",
            "FileSize": "span[aria-label^='Rozmiar pliku:']"
        }
        for key, selector in pairs.items():
            locator = wiz_element.locator(selector).first
            if await locator.count() > 0:
                if aria := await get_attribute_safely(locator):
                    value = aria.split(":", 1)[-1].strip() if ':' in aria else aria
                    scan_results[key] = value
                    logger.debug(f"Znaleziono '{key}': {value}")
        
        if description := await wiz_element.locator("textarea[aria-label='Opis']").input_value():
            scan_results["Description"] = description.strip()
            logger.debug(f"Znaleziono opis: '{description[:50]}...'")
        
        people_locators = await wiz_element.locator("a[aria-label^='Na zdjęciu:']").all()
        if tagged_people := [await get_attribute_safely(loc) for loc in people_locators]:
            scan_results["TaggedPeople"] = [p.replace("Na zdjęciu:", "").strip() for p in tagged_people if p]
            logger.debug(f"Znaleziono osoby: {scan_results['TaggedPeople']}")
        
        albums_section = wiz_element.locator("div.KlIBpb:has-text('Albumy')")
        if await albums_section.count() > 0:
            album_locators = await albums_section.locator("div.AJM7gb").all()
            scan_results["Albums"] = [await loc.inner_text() for loc in album_locators]
            logger.debug(f"Znaleziono albumy: {scan_results['Albums']}")

        # --- Krok 3: Ekstrakcja Danych Eksperymentalnych/Testowych ---
        # Ta sekcja zawiera selektory, które są mniej stabilne lub służą
        # do celów diagnostycznych. Dane z nich są dodawane do pod-słownika
        # 'Experimental_Details', aby nie mieszać ich z głównymi metadanymi.
        
        experimental_details = {}
        
        map_link_locator = wiz_element.locator("a.cFLCHe")
        if await map_link_locator.count() > 0:
            href = await map_link_locator.get_attribute('href')
            if href and (gps_match := re.search(r'(-?\d+\.\d+),(-?\d+\.\d+)', href)):
                experimental_details["GPS_Coords"] = {"latitude": float(gps_match.group(1)), "longitude": float(gps_match.group(2))}
        
        upload_source_locator = wiz_element.locator("div.ffq9nc:has-text('Przesłane z') dd.rCexAf")
        if await upload_source_locator.count() > 0:
            experimental_details["Upload_Source"] = await upload_source_locator.first.inner_text()
            
        album_info_locator = albums_section.locator(".rugHuc").first
        if await album_info_locator.count() > 0:
            album_text = await album_info_locator.inner_text()
            if match := re.search(r'(\d+)\s+element', album_text):
                experimental_details["Album_Element_Count"] = int(match.group(1))

        if experimental_details:
            scan_results["Experimental_Details"] = experimental_details
            logger.debug(f"Zebrano {len(experimental_details)} dodatkowych danych eksperymentalnych.")
            
        return scan_results
    
    # --- Główna logika wykonania z mechanizmem ponawiania ---
    try:
        details = await _scan_page_content()

        if details is not None and "DateTime" not in details:
            logger.warning(f"Brak kluczowej daty dla ...{current_url[-40:]}. Odświeżam i próbuję ponownie...", extra={"markup": True})
            await page.reload(wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
            await asyncio.sleep(2)
            details = await _scan_page_content()

        if details is None:
            return None

        # Oblicz OCZEKIWANĄ ścieżkę zapisu na podstawie zebranych metadanych
        if "DateTime" in details and "FileName" in details:
            try:
                dt = datetime.fromisoformat(details["DateTime"])
                dest_dir = Path(DOWNLOADS_DIR_BASE) / str(dt.year) / f"{dt.month:02d}"
                details['expected_path'] = str(dest_dir / details['FileName'])
                logger.debug(f"Obliczono oczekiwaną ścieżkę: {details['expected_path']}")
            except (ValueError, TypeError):
                details['expected_path'] = None
        else:
            details['expected_path'] = None
        
        logger.info(f"Skanowanie online dla ...{current_url[-40:]} zakończone pomyślnie.")
        return details

    except Exception as e:
        logger.error(f"Krytyczny błąd podczas analizy strony {current_url}: {e}", exc_info=True)
        return None


async def run_scanner_core(process_mode: str, run_headless: bool, input_file: str = URL_INPUT_FILE):
    """
    Główna pętla wykonawcza dla skanera działającego w trybie online.
    """
    # ... (kod tej funkcji pozostaje bez zmian, używa już update_scanned_entries_batch) ...
    title_map = {
        'full_scan': "Dokańczanie Skanowania",
        'retry_errors': "Ponawianie Błędów",
        'force_refresh': "Pełne Odświeżanie",
        'scan_all': f"Skanowanie z Pliku ({Path(input_file).name})",
        'scan_fix_file': f"Skanowanie z Pliku Naprawczego ({Path(input_file).name})"
    }
    logger.info(f"Uruchamiam Zaawansowany Skaner Online w trybie: [bold cyan]{title_map.get(process_mode)}[/bold cyan]", extra={"markup": True})

    # Krok 1: Pobierz listę URL-i do przetworzenia
    urls_to_process = await get_urls_for_processing(process_mode, input_file=input_file)
    if urls_to_process is None: # Krytyczny błąd odczytu
        console.print("[bold red]Wystąpił krytyczny błąd podczas przygotowywania listy URL-i. Sprawdź logi.[/bold red]")
        return
    if not urls_to_process:
        console.print("\n[bold green]✅ Brak pracy do wykonania dla wybranych kryteriów.[/bold green]")
        logger.info("Brak URL-i do przetworzenia. Kończę pracę.")
        return
        
    logger.info(f"Znaleziono [bold cyan]{len(urls_to_process)}[/bold cyan] URL-i do przetworzenia. Log: [yellow]{LOG_FILE.name}[/yellow]", extra={"markup": True})

    if process_mode in ['scan_all', 'scan_fix_file']:
        done_file_path = Path(input_file).with_suffix(f"{Path(input_file).suffix}_done")
        console.print(f"[dim]Przetworzone adresy będą przenoszone z '{Path(input_file).name}' do '{done_file_path.name}'.[/dim]")

    # Krok 2: Inicjalizacja zasobów (przeglądarka, interfejs)
    p, browser, results_batch = None, None, []
    try:
        p = await async_playwright().start()
        browser = await getattr(p, BROWSER_TYPE).launch_persistent_context(Path(SESSION_DIR).expanduser(), headless=run_headless, args=BROWSER_ARGS.get(BROWSER_TYPE))
        page = await browser.new_page()
        
        if ENABLE_RESOURCE_BLOCKING:
            logger.info("Blokowanie zbędnych zasobów sieciowych jest [bold green]WŁĄCZONE[/bold green].", extra={"markup": True})
            await page.route("**/*", block_unwanted_resources)

        # Inicjalizacja interfejsu Rich.Live
        counters = {"poprawne": 0, "błędy": 0}
        recent_logs = deque(maxlen=5)
        progress_bar = Progress(TextColumn("[bold blue]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.1f}%", "{task.completed}/{task.total}", TimeRemainingColumn())
        remaining_urls = list(urls_to_process)
        overall_task = progress_bar.add_task("[green]Postęp...", total=len(remaining_urls))
        layout = Layout()
        layout.split_column(Layout(progress_bar, name="progress", size=3), Layout(name="main_body"), Layout(name="footer", size=3))
        
        # Krok 3: Główna pętla przetwarzania
        with Live(layout, screen=True, transient=True, auto_refresh=False) as live:
            for url in list(remaining_urls):
                if stop_event.is_set():
                    logger.warning("Przerwanie przez użytkownika. Zatrzymuję skanowanie...")
                    break

                photo_details, final_error = None, "Nieznany błąd"
                # Pętla ponawiania prób dla pojedynczego URL
                for attempt in range(3):
                    if stop_event.is_set(): break
                    try:
                        await page.goto(url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
                        photo_details = await get_advanced_photo_details_from_page(page, url)
                        if photo_details: break # Sukces, przerywamy pętlę ponawiania
                    except Exception as e:
                        final_error = str(e)
                        logger.warning(f"Błąd podczas próby {attempt + 1} dla ...{url[-30:]}: {e}")
                
                if stop_event.is_set(): break
                
                # Przetwarzanie wyniku i aktualizacja UI
                if photo_details:
                    status = "Sukces"
                    counters["poprawne"] += 1
                    result_table = Table(show_header=False, box=None, padding=0, expand=True)
                    result_table.add_column(style="cyan", justify="right", width=25)
                    result_table.add_column()
                    for key, value in photo_details.items():
                        if isinstance(value, list): result_table.add_row(f"{key}:", "\n".join(f"- {item}" for item in value))
                        elif isinstance(value, dict): result_table.add_row(f"{key}:", json.dumps(value, indent=2, ensure_ascii=False))
                        else: result_table.add_row(f"{key}:", str(value))
                    recent_logs.appendleft(Panel(result_table, title=f"[bold green]✅ Sukces![/] ...{url[-30:]}"))
                else:
                    status = "Błąd"
                    photo_details = {"error": final_error}
                    counters["błędy"] += 1
                    recent_logs.appendleft(Panel(f"Nie udało się pobrać danych. Ostatni błąd: {final_error[:100]}...", title=f"[bold red]❌ Błąd![/] ...{url[-30:]}", border_style="red"))
                
                # Dodaj wynik do partii do zapisu w bazie
                results_batch.append({
                    "url": url,
                    "metadata_json": json.dumps(photo_details, ensure_ascii=False) if photo_details else None,
                    "processing_status": status,
                    "expected_path": photo_details.get('expected_path') if photo_details else None
                })
                log_to_file(url, photo_details, status)
                
                # Zapisz partię do bazy, jeśli osiągnęła odpowiedni rozmiar
                if len(results_batch) >= BATCH_SIZE:
                    await update_scanned_entries_batch(results_batch)
                    results_batch.clear()

                # Aktualizacja plików postępu w trybie skanowania z pliku
                if process_mode in ['scan_all', 'scan_fix_file']:
                    try:
                        remaining_urls.remove(url)
                        with open(done_file_path, "a", encoding="utf-8") as f_done: f_done.write(f"{url}\n")
                        with open(input_file, "w", encoding="utf-8") as f_input:
                            for rem_url in remaining_urls: f_input.write(f"{rem_url}\n")
                    except (IOError, ValueError) as e:
                        logger.error(f"Błąd podczas aktualizacji plików postępu: {e}")
                
                # Aktualizacja interfejsu
                progress_bar.update(overall_task, advance=1)
                layout["main_body"].update(Panel(Group(*recent_logs), title="Ostatnie Akcje"))
                counters_table = Table.grid(expand=True); counters_table.add_column(justify="center"); counters_table.add_column(justify="center")
                counters_table.add_row(f"[green]Poprawne: {counters['poprawne']}[/]", f"[red]Błędy: {counters['błędy']}[/]")
                layout["footer"].update(Panel(counters_table, title="Statystyki Sesji"))
                live.refresh()

        # Zapisz ostatnią partię danych, jeśli jakaś została
        if results_batch:
            await update_scanned_entries_batch(results_batch)
            
    except Exception as e:
        logger.critical(f"Wystąpił nieobsługiwany błąd w głównej pętli skanera: {e}", exc_info=True)
    finally:
        logger.info("Zamykanie zasobów skanera online...")
        if browser: await browser.close()
        if p: await p.stop()
        logger.info("Zasoby skanera zwolnione.")

# plik: core/advanced_scanner_logic.py

async def run_offline_file_corrector():
    """
    Skaner Offline z Samokorektą Lokalizacji Plików.
    """
    console.clear()
    logger.info("Uruchamiam Skaner Offline z Samokorektą Lokalizacji Plików...")
    console.print(Panel("🛰️  Korektor Lokalizacji Plików (Offline) 🛰️", expand=False, style="green"))
    
    try:
        # Krok 1: Pobierz dane do weryfikacji za pomocą nowej, dedykowanej funkcji
        records_to_check = await get_records_for_path_correction()
        
        if not records_to_check:
            logger.warning("Nie znaleziono plików do weryfikacji.")
            console.print("\n[bold yellow]Nie znaleziono plików do weryfikacji. Uruchom skaner online.[/bold yellow]")
            return

        logger.info(f"Znaleziono {len(records_to_check)} plików do weryfikacji lokalizacji.")
        moved_count, error_count, skipped_count = 0, 0, 0
        
        # Krok 2: Iteruj i naprawiaj niespójności
        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[green]Weryfikacja lokalizacji plików...", total=len(records_to_check))
            for record in records_to_check:
                try:
                    # Używamy pathlib.Path do obsługi ścieżek
                    final_path = Path(record['final_path'])
                    expected_path = Path(record['expected_path'])

                    if not final_path.name or not expected_path.name:
                        logger.warning(f"Pominięto rekord ID {record['id']} z powodu nieprawidłowej ścieżki.")
                        skipped_count += 1
                        continue

                    # Porównujemy rozwiązane, absolutne ścieżki
                    if final_path.resolve() != expected_path.resolve():
                        console.print(f"\n[yellow]Niespójność wykryta dla ID {record['id']}:[/]")
                        console.print(f"  [dim]Jest w:[/dim] {final_path}")
                        console.print(f"  [cyan]Powinien być w:[/cyan] {expected_path}")

                        if not await asyncio.to_thread(final_path.exists):
                            logger.error(f"BŁĄD: Plik źródłowy {final_path} nie istnieje. Pomijam.")
                            error_count += 1
                            continue

                        await asyncio.to_thread(expected_path.parent.mkdir, parents=True, exist_ok=True)
                        await asyncio.to_thread(shutil.move, str(final_path), str(expected_path))
                        
                        # Zaktualizuj wpis w bazie za pomocą nowej funkcji
                        await update_final_path(record['id'], str(expected_path))
                        
                        console.print(f"  [bold green]Sukces: Plik został przeniesiony.[/bold green]")
                        moved_count += 1
                except Exception as e:
                    logger.error(f"BŁĄD podczas przenoszenia pliku dla ID {record['id']}: {e}", exc_info=True)
                    error_count += 1
                finally:
                    progress.update(task, advance=1)

        # Krok 3: Wyświetl podsumowanie
        logger.info("Zakończono weryfikację lokalizacji plików.")
        console.print("\n[bold green]Zakończono weryfikację lokalizacji plików.[/bold green]")
        console.print(f"  - Przeniesiono plików: [cyan]{moved_count}[/cyan]")
        console.print(f"  - Pominięto (błędne dane): [yellow]{skipped_count}[/yellow]")
        console.print(f"  - Błędy: [red]{error_count}[/red]")

    except Exception as e:
        logger.critical(f"Błąd krytyczny w korektorze plików: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/bold red]")


async def run_filename_fixer_from_db():
    """
    Skaner Offline: Naprawia nazwy plików na dysku na podstawie metadanych.
    """
    console.clear()
    logger.info("Uruchamiam narzędzie do naprawy nazw plików na podstawie metadanych...")
    console.print(Panel("[bold yellow]Naprawa Nazw Plików z Pełną Synchronizacją[/]", expand=False))
    
    if not Confirm.ask("\n[bold red]UWAGA:[/bold red] Ta operacja zmieni nazwy plików na Twoim dysku. Czy na pewno chcesz kontynuować?", default=False):
        logger.warning("Naprawa nazw plików anulowana przez użytkownika.")
        return

    try:
        run_count = 0
        while True:
            run_count += 1
            logger.info(f"Rozpoczynam przebieg {run_count} weryfikacji nazw plików.")
            
            records_to_check = await get_records_for_filename_fix()
            
            if not records_to_check:
                logger.info("Nie znaleziono plików z metadanymi do weryfikacji nazw.")
                break

            mismatches_found_this_run = False
            with Progress(console=console, transient=True) as progress:
                task = progress.add_task(f"[green]Weryfikacja nazw (przebieg {run_count})...[/]", total=len(records_to_check))
                for record in records_to_check:
                    progress.update(task, advance=1)
                    try:
                        current_path = Path(record['final_path'])
                        metadata = json.loads(record['metadata_json'])
                        filename_from_meta = metadata.get('FileName')
                        
                        if not await asyncio.to_thread(current_path.exists):
                            continue

                        mismatches_found_this_run = True
                        
                        new_path = create_unique_filepath(current_path.parent, filename_from_meta)
                        new_filename = new_path.name

                        console.print(f"\n[yellow]Niespójność nazwy dla ID {record['id']}:[/]")
                        console.print(f"  [dim]Aktualna nazwa:[/dim] {current_path.name}")
                        console.print(f"  [cyan]Oczekiwana nazwa:[/cyan] {filename_from_meta}")
                        if new_filename != filename_from_meta:
                            console.print(f"  [magenta]Kolizja! Zmieniam nazwę na:[/magenta] {new_filename}")

                        await asyncio.to_thread(current_path.rename, new_path)
                        
                        if new_filename != filename_from_meta:
                            metadata['FileName'] = new_filename
                        
                        if 'DateTime' in metadata:
                            dt = datetime.fromisoformat(metadata["DateTime"])
                            dest_dir = Path(DOWNLOADS_DIR_BASE) / str(dt.year) / f"{dt.month:02d}"
                            new_expected_path = str(dest_dir / new_filename)
                            metadata['expected_path'] = new_expected_path
                        else:
                            new_expected_path = str(new_path)

                        await update_entry_after_rename(
                            record['id'], new_filename, str(new_path),
                            new_expected_path, json.dumps(metadata, ensure_ascii=False)
                        )
                        
                        logger.info(f"Zsynchronizowano plik ID {record['id']}. Nowa nazwa: '{new_filename}'.")
                        console.print("  [bold green]Sukces: Plik i wpis w bazie zostały w pełni zsynchronizowane.[/bold green]")
                    
                    except Exception as e:
                        logger.error(f"Błąd podczas naprawy nazwy dla pliku {record['final_path']}", exc_info=True)
            
            if not mismatches_found_this_run:
                logger.info("Brak dalszych niespójności. Kończę pętlę naprawczą.")
                break
                
        logger.info("Zakończono naprawę nazw plików.")
        console.print("\n[bold green]Zakończono. Wszystkie nazwy plików są teraz spójne z metadanymi.[/bold green]")
        
    except Exception as e:
        logger.critical(f"Błąd krytyczny podczas naprawy nazw plików: {e}", exc_info=True)


async def run_metadata_completer():
    """
    Skaner Offline: Uzupełnia brakujące dane i oblicza `expected_path`.
    """
    console.clear()
    logger.info("Uruchamiam narzędzie do uzupełniania metadanych...")
    console.print(Panel("[bold blue]Uzupełniacz Danych i Oczekiwanych Ścieżek[/]", expand=False))
    
    if not EXIFTOOL_AVAILABLE:
        # ... (obsługa braku exiftool bez zmian) ...
        return

    try:
        records_to_fix = await get_records_for_metadata_completion()
        
        if not records_to_fix:
            logger.info("Nie znaleziono plików wymagających uzupełnienia danych.")
            console.print("\n[bold green]✅ Wszystkie pobrane pliki mają już obliczoną oczekiwaną ścieżkę.[/bold green]")
            return

        logger.info(f"Znaleziono {len(records_to_fix)} plików do uzupełnienia danych.")
        if not Confirm.ask("\n[cyan]Czy chcesz kontynuować?[/]", default=True):
            logger.warning("Operacja uzupełniania danych anulowana."); return

        fixed_count, error_count = 0, 0
        loop = asyncio.get_running_loop()
        
        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[green]Uzupełnianie danych...", total=len(records_to_fix))
            for record in records_to_fix:
                try:
                    current_path = Path(record['final_path'])
                    if not await asyncio.to_thread(current_path.exists):
                        logger.warning(f"Plik {current_path} nie istnieje. Pomijam.")
                        continue

                    existing_metadata = json.loads(record['metadata_json'])
                    
                    with exiftool.ExifToolHelper() as et:
                        exif_metadata_list = await loop.run_in_executor(None, et.get_metadata, str(current_path))
                    
                    if not exif_metadata_list:
                         logger.warning(f"Nie odczytano EXIF dla {current_path.name}."); continue

                    merged_metadata = exif_metadata_list[0]
                    merged_metadata.update(existing_metadata)

                    if 'DateTime' not in merged_metadata or not merged_metadata['DateTime']:
                        date_obj = await get_date_from_metadata(merged_metadata)
                        if date_obj: merged_metadata['DateTime'] = date_obj.isoformat()
                    
                    if 'FileName' not in merged_metadata or not merged_metadata['FileName']:
                         merged_metadata['FileName'] = merged_metadata.get('File:FileName', current_path.name)
                         
                    if 'DateTime' in merged_metadata and 'FileName' in merged_metadata:
                        dt = datetime.fromisoformat(merged_metadata["DateTime"])
                        dest_dir = Path(DOWNLOADS_DIR_BASE) / str(dt.year) / f"{dt.month:02d}"
                        expected_path = str(dest_dir / merged_metadata['FileName'])
                        
                        await update_entry_with_completed_metadata(
                            record['id'], json.dumps(merged_metadata, ensure_ascii=False), expected_path
                        )
                        fixed_count += 1
                    else:
                        logger.error(f"Nie udało się ustalić daty/nazwy dla ID {record['id']}.")
                        error_count += 1

                except Exception as e:
                    logger.error(f"Błąd przetwarzania pliku {record['final_path']}", exc_info=True)
                    error_count += 1
                finally:
                    progress.update(task, advance=1)
                        
        logger.info("Zakończono uzupełnianie danych.")
        console.print(f"\n[bold green]Zakończono. Uzupełniono dane dla [cyan]{fixed_count}[/cyan] plików. Błędy: [red]{error_count}[/red].[/bold green]")

    except Exception as e:
        logger.critical(f"Błąd krytyczny podczas uzupełniania metadanych: {e}", exc_info=True)


# ##############################################################################
# ===                  SEKCJA 3: NARZĘDZIA ZARZĄDCZE I DIAGNOSTYCZNE         ===
# ##############################################################################

async def write_metadata_from_db_to_files():
    """
    Odczytuje metadane z bazy danych i zapisuje je do plików na dysku.
    """
    console.clear()
    logger.info("Uruchamiam narzędzie do zapisu metadanych w plikach (Exiftool)...")
    console.print(Panel("✍️ Zapisywanie Metadanych z Bazy do Plików (Exiftool)", expand=False, style="red"))
    
    if not EXIFTOOL_AVAILABLE:
        # ... (obsługa braku exiftool bez zmian) ...
        return
        
    console.print("\n[bold yellow]⚠️ UWAGA: Ta operacja nieodwracalnie zmodyfikuje pliki na dysku![/bold yellow]")
    if not Confirm.ask("Czy na pewno chcesz kontynuować? (Zalecana jest kopia zapasowa)", default=False):
        logger.warning("Operacja zapisu metadanych anulowana przez użytkownika.")
        return

    records_to_process = await get_records_for_exif_writing()

    if not records_to_process:
        logger.warning("Nie znaleziono plików z metadanymi do zapisu.")
        console.print("\n[bold yellow]Nie znaleziono plików z metadanymi do zapisu.[/bold yellow]")
        return

    success_count, error_count, skipped_count = 0, 0, 0
    loop = asyncio.get_running_loop()

    with Progress(console=console, transient=True) as progress:
        task = progress.add_task("[green]Zapisywanie tagów w plikach...", total=len(records_to_process))
        for record in records_to_process:
            try:
                file_path = Path(record['final_path'])
                if not await asyncio.to_thread(file_path.exists):
                    logger.warning(f"Pominięto: Plik nie istnieje {file_path}")
                    skipped_count += 1
                    continue

                data = json.loads(record['metadata_json'])
                
                tags_to_write = {}
                # ... (logika budowania tagów bez zmian) ...
                
                if not tags_to_write:
                    skipped_count += 1; continue

                params = []
                # ... (logika budowania parametrów bez zmian) ...
                
                with exiftool.ExifToolHelper() as et:
                    await loop.run_in_executor(None, et.execute, "-overwrite_original", "-m", *params, str(file_path))
                
                logger.debug(f"Pomyślnie zapisano {len(tags_to_write)} tagów do pliku {file_path.name}")
                success_count += 1

            except json.JSONDecodeError:
                logger.error(f"Błąd: Uszkodzony JSON dla pliku {record['final_path']}")
                error_count += 1
            except Exception as e:
                logger.error(f"Błąd zapisu do pliku {record['final_path']}: {e}", exc_info=True)
                error_count += 1
            finally:
                progress.update(task, advance=1)

    logger.info("Zakończono zapisywanie metadanych do plików.")
    console.print("\n[bold green]Zakończono zapisywanie metadanych do plików.[/bold green]")
    console.print(f"  - Zapisano pomyślnie: [cyan]{success_count}[/cyan]")
    console.print(f"  - Pominięto (brak danych/pliku): [yellow]{skipped_count}[/yellow]")
    console.print(f"  - Błędy: [red]{error_count}[/red]")


async def test_single_url_diagnostics(run_headless: bool):
    """
    Uruchamia pełny test diagnostyczny dla jednego, ręcznie podanego adresu URL.
    """
    # ... (kod tej funkcji pozostaje bez zmian) ...
    console.clear()
    console.print(Panel("[bold yellow]🔬 Test Skanera Online dla Pojedynczego URL 🔬[/]", expand=False))
    url = Prompt.ask("\n[cyan]Wklej adres URL zdjęcia, który chcesz przetestować[/]")
    if not url.strip().startswith("http"):
        logger.error("To nie jest prawidłowy adres URL.")
        return

    logger.info(f"Uruchamianie przeglądarki w trybie testowym (headless: {run_headless})...")
    
    async with async_playwright() as p:
        browser = None
        try:
            browser = await getattr(p, BROWSER_TYPE).launch_persistent_context(
                Path(SESSION_DIR).expanduser(), headless=run_headless, args=BROWSER_ARGS.get(BROWSER_TYPE)
            )
            page = await browser.new_page()

            if ENABLE_RESOURCE_BLOCKING:
                logger.info("Blokowanie zasobów włączone na czas testu.")
                await page.route("**/*", block_unwanted_resources)

            with console.status(f"[cyan]Nawigacja do: [dim]{url}[/dim]...[/]"):
                await page.goto(url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
            
            logger.info("Strona załadowana. Uruchamiam skaner główny...")
            with console.status("[cyan]Skanowanie metadanych ze strony...[/]"):
                metadata = await get_advanced_photo_details_from_page(page, url)
            
            console.clear()
            if metadata:
                console.print(Panel("[bold green]✅ SKANER ZAKOŃCZYŁ PRACĘ SUKCESEM[/]", title="Wynik Testu"))
                table = Table(title="Zebrane Metadane", show_header=False, box=None, padding=(0, 2))
                table.add_column(style="cyan", justify="right", width=25)
                table.add_column()
                for key, value in metadata.items():
                    if isinstance(value, list):
                        table.add_row(f"[bold]{key}:[/bold]", "\n".join(f"- {item}" for item in value))
                    elif isinstance(value, dict):
                        table.add_row(f"[bold]{key}:[/bold]", json.dumps(value, indent=2, ensure_ascii=False))
                    else:
                        table.add_row(f"[bold]{key}:[/bold]", str(value))
                console.print(Panel(table, border_style="green"))
            else:
                console.print(Panel("[bold red]❌ SKANER ZAKOŃCZYŁ PRACĘ BŁĘDEM LUB NIE ZNALAZŁ DANYCH[/]", title="Wynik Testu", border_style="red"))
                logger.error("Skaner nie zwrócił żadnych metadanych.")

        except Exception as e:
            logger.critical(f"Wystąpił krytyczny błąd podczas testu: {e}", exc_info=True)
            console.print(f"[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/bold red]")
        finally:
            if browser:
                await browser.close()
            logger.info("Test zakończony, przeglądarka zamknięta.")


# ##############################################################################
# ===                    SEKCJA 4: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_advanced_scanner():
    """
    Wyświetla i zarządza interaktywnym menu dla wszystkich funkcji
    dostępnych w module Zaawansowanego Skanera i Menedżera Kolekcji.
    """
    logger.info("Uruchamiam menu Zaawansowanego Skanera i Menedżera Kolekcji.")
    
    await setup_database()

    while True:
        console.clear()
        
        menu_items = [
            ("--- GŁÓWNY PRZEPŁYW PRACY (Zalecana kolejność) ---", None),
            ("Krok 1: Dokończ skanowanie metadanych z bazy (Online)", "full_scan"),
            ("Krok 2: Uzupełnij dane i ścieżki z plików (Offline)", "complete_metadata"),
            ("Krok 3: Sprawdź i napraw LOKALIZACJE plików (Offline)", "correct_paths"),
            ("Krok 4: Sprawdź i napraw NAZWY plików (Offline)", "fix_filenames"),

            ("--- ZAAWANSOWANE OPERACJE ONLINE ---", None),
            ("Ponów tylko te URL-e z bazy, które miały błąd", "retry_errors"),
            ("Odśwież metadane dla WSZYSTKICH wpisów w bazie", "force_refresh"),
            ("Skanuj wszystkie URL-e z pliku 'urls_to_scan.txt'", "scan_all"),
            ("Skanuj URL-e wymagające naprawy z pliku 'urls_to_fix.txt'", "scan_fix_file"),

            ("--- NARZĘDZIA POMOCNICZE I DIAGNOSTYKA ---", None),
            ("ZAPISZ metadane z bazy do plików (Exiftool)", "write_to_files"),
            ("Wygeneruj plik 'urls_to_scan.txt' z bazy", "export_urls"),
            ("Wygeneruj plik 'urls_to_fix.txt' (z brakującymi metadanymi)", "export_fix_urls"),
            ("Uruchom PEŁNY TEST (Skaner + Diagnostyka)", "single_url_test"),
            
            ("---", None),
            ("Wróć do menu głównego", "exit")
        ]

        selected_mode = await create_interactive_menu(
            menu_items,
            "Zaawansowany Skaner i Menedżer Kolekcji",
            border_style="magenta"
        )
        
        if selected_mode == "exit" or selected_mode is None:
            logger.info("Anulowano. Powrót do menu głównego.")
            break
        
        logger.info(f"Użytkownik wybrał opcję: '{selected_mode}'")
        
        online_modes = ['full_scan', 'retry_errors', 'force_refresh', 'scan_all', 'scan_fix_file']
        
        if selected_mode in online_modes:
            input_file_path = "urls_to_fix.txt" if selected_mode == 'scan_fix_file' else URL_INPUT_FILE
            run_headless = Confirm.ask("Uruchomić w trybie niewidocznym (headless)?", default=DEFAULT_HEADLESS_MODE)
            await run_scanner_core(process_mode=selected_mode, run_headless=run_headless, input_file=input_file_path)
        elif selected_mode == 'correct_paths':
            await run_offline_file_corrector()
        elif selected_mode == 'fix_filenames':
            await run_filename_fixer_from_db()
        elif selected_mode == 'complete_metadata':
            await run_metadata_completer()
        elif selected_mode == 'write_to_files':
            if not EXIFTOOL_AVAILABLE:
                console.print(Panel("[bold red]Błąd: Brak 'pyexiftool'![/bold red]\nUruchom: [cyan]pip install pyexiftool[/cyan]", title="Instrukcja Instalacji"))
            else:
                await write_metadata_from_db_to_files()
        elif selected_mode == 'export_urls':
            await export_urls_from_db_to_file()
        elif selected_mode == 'export_fix_urls':
            await export_fix_needed_urls_to_file()
        elif selected_mode == 'single_url_test':
            await test_single_url_diagnostics(run_headless=False)
        
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter, aby wrócić do menu skanera...[/]", console=console)
