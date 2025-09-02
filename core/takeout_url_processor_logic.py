# -*- coding: utf-8 -*-

# plik: core/takeout_url_processor_logic.py
# Wersja 1.0 - Nowy moduł do naprawy z URL-i z Takeout
#
# ##############################################################################
# ===           MODUŁ NAPRAWY Z UŻYCIEM URL-I Z GOOGLE TAKEOUT               ===
# ##############################################################################
#
# Ten moduł zawiera logikę dla narzędzia, które wykorzystuje unikalne adresy URL
# do zdjęć (pozyskane z archiwum Takeout) w celu ponownego przetworzenia
# i naprawy wpisów w bazie danych, które wcześniej zakończyły się błędem
# lub miały niekompletne dane.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import logging
from pathlib import Path

# --- Importy asynchroniczne ---
import aiosqlite
from playwright.async_api import async_playwright

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm
from rich.progress import Progress

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import (
    SESSION_DIR, BROWSER_TYPE, BROWSER_ARGS, DEFAULT_HEADLESS_MODE,
    DOWNLOADS_DIR_BASE, DATABASE_FILE
)
# Reużywamy głównego "mózgu" z silnika Master do przetwarzania stron
from .master_logic import process_single_photo_page

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


async def _get_urls_to_process_from_db() -> list[str]:
    """
    Pobiera z bazy danych listę adresów URL z Takeout dla plików,
    które wymagają ponownego przetworzenia.

    Szuka wpisów ze statusem 'failed' (błąd pobierania) lub 'skipped'
    (pominięty, potencjalnie z powodu braku metadanych online), które
    jednocześnie posiadają zapisany `google_photos_url`.

    :return: Lista adresów URL do przetworzenia.
    """
    logger.info("Pobieram z bazy listę URL-i z Takeout do naprawy...")
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            query = """
                SELECT google_photos_url FROM downloaded_media
                WHERE status IN ('failed', 'skipped')
                AND google_photos_url IS NOT NULL
                AND google_photos_url != ''
            """
            cursor = await conn.execute(query)
            urls = [row[0] for row in await cursor.fetchall()]
            logger.info(f"Znaleziono {len(urls)} URL-i do naprawy.")
            return urls
    except aiosqlite.Error as e:
        logger.error(f"Błąd podczas pobierania URL-i do naprawy: {e}", exc_info=True)
        return []

async def run_takeout_url_processor():
    """
    Uruchamia główny proces naprawy z użyciem URL-i zapisanych z Takeout.

    Proces:
    1. Pobiera listę problematycznych URL-i z bazy danych.
    2. Prosi użytkownika o potwierdzenie.
    3. Uruchamia przeglądarkę i iteruje po liście URL-i.
    4. Dla każdego URL-a, wywołuje potężną funkcję `process_single_photo_page`
       z modułu `master_logic`, która wykonuje pełen cykl pobierania,
       analizy i zapisu.
    """
    console.clear()
    logger.info("Uruchomiono narzędzie do naprawy z URL-i z Takeout.")
    console.print(Panel("🔧 Naprawa z Użyciem URL-i z Takeout 🔧", expand=False, style="bold green"))

    urls_to_process = await _get_urls_to_process_from_db()
    if not urls_to_process:
        console.print("\n[bold green]✅ Nie znaleziono plików, które można by naprawić za pomocą tej metody.[/bold green]")
        return

    console.print(f"\nZnaleziono [bold cyan]{len(urls_to_process)}[/bold cyan] plików, które można spróbować pobrać/przeskanować ponownie.")
    if not Confirm.ask("[cyan]Czy chcesz kontynuować?[/]"):
        logger.warning("Operacja naprawy anulowana przez użytkownika.")
        return

    # Uruchomienie przeglądarki i pętli przetwarzania
    try:
        async with async_playwright() as p:
            with console.status("[cyan]Uruchamianie przeglądarki...[/]"):
                context = await getattr(p, BROWSER_TYPE).launch_persistent_context(
                    Path(SESSION_DIR).expanduser(),
                    headless=DEFAULT_HEADLESS_MODE,
                    accept_downloads=True,
                    args=BROWSER_ARGS.get(BROWSER_TYPE)
                )
                page = await context.new_page()

            with Progress(console=console, transient=True) as progress:
                task = progress.add_task("[green]Przetwarzanie...", total=len(urls_to_process))
                for i, url in enumerate(urls_to_process):
                    short_url = url[:45] + "..." + url[-15:] if len(url) > 60 else url
                    progress.update(task, description=f"Praca nad [dim]{short_url}[/dim]")

                    # Wywołujemy główny procesor z silnika Master, przekazując mu URL z Takeout
                    success, status, metadata = await process_single_photo_page(
                        page, url, Path(DOWNLOADS_DIR_BASE), 'main'
                    )
                    
                    if success:
                        logger.info(f"Pomyślnie przetworzono URL: {url} (status: {status})")
                    else:
                        logger.warning(f"Nie udało się przetworzyć URL: {url} (status: {status})")

                    progress.update(task, advance=1)
            
            await context.close()

        console.print("\n[bold green]✅ Proces naprawy zakończony.[/bold green]")
        logger.info("Zakończono proces naprawy z użyciem URL-i z Takeout.")

    except Exception as e:
        logger.critical("Wystąpił krytyczny błąd podczas procesu naprawy.", exc_info=True)
        console.print(f"\n[bold red]Wystąpił nieoczekiwany błąd. Sprawdź plik logu.[/bold red]")
