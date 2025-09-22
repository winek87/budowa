# plik: core/downloader/tools.py
# Wersja 1.1 - Naprawiono błąd TypeError przez dodanie brakującego argumentu status_callback.
# Opis: Ten moduł zawiera samodzielne, interaktywne narzędzia,
#       takie jak pobieranie pojedynczego URL-a czy ponawianie błędów.
# -*- coding: utf-8 -*-

import asyncio
import logging
from pathlib import Path

from playwright.async_api import async_playwright

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.progress import Progress, BarColumn, TextColumn

from ..config import (
    SESSION_DIR, BROWSER_TYPE, BROWSER_ARGS
)
from ..database import get_failed_urls_from_db
from ..utils import _interactive_file_selector
from ..session_logic import check_login_status
from .page_processor import process_single_photo_page
from .ui.panels import create_summary_panel


console = Console()
logger = logging.getLogger(__name__)


async def run_single_file_download():
    """
    Uruchamia proces pobierania dla jednego, konkretnego adresu URL podanego przez użytkownika.
    """
    console.clear()
    console.print(Panel("[bold cyan]📥 Pobieranie Pojedynczego Pliku 📥[/bold cyan]", border_style="cyan"))
    url = Prompt.ask("\n[bold]Wklej adres URL zdjęcia lub filmu, który chcesz pobrać[/bold]")
    if not url.strip().startswith("http"):
        logger.error("Podany ciąg znaków nie jest prawidłowym adresem URL.")
        console.print("[bold red]To nie jest prawidłowy adres URL.[/]")
        return

    async with async_playwright() as p:
        browser, page = None, None
        try:
            browser = await getattr(p, BROWSER_TYPE).launch_persistent_context(Path(SESSION_DIR).expanduser(), headless=False, args=BROWSER_ARGS.get(BROWSER_TYPE))
            page = await browser.new_page()
            await check_login_status(page)

            # === POCZĄTEK ZMIAN ===
            with console.status(f"[cyan]Przetwarzanie URL: [dim]{url}[/dim]...") as status:
                # Tworzymy prosty callback, który aktualizuje tekst statusu
                status_callback = lambda msg: status.update(f"[cyan]Postęp: [dim]{msg}[/dim][/cyan]")
                
                await page.goto(url)
                
                # Przekazujemy nowo utworzony callback do funkcji
                success, status_result, details = await process_single_photo_page(
                    page, page.url, 'forced', status_callback
                )
            # === KONIEC ZMIAN ===

            console.clear()
            if success:
                summary_panel = create_summary_panel(url, status_result, details)
                console.print(summary_panel)
            else:
                error_panel = Panel(
                    f"Wystąpił błąd podczas przetwarzania:\n[dim]{details.get('error_message', 'Nieznany błąd')}[/]",
                    title="[bold red]❌ Pobieranie Nieudane[/]", border_style="red"
                )
                console.print(error_panel)

        except Exception as e:
            logger.critical("Wystąpił krytyczny błąd podczas pobierania pojedynczego pliku.", exc_info=True)
            console.print(f"\n[bold red]Wystąpił krytyczny błąd: {e}[/]")
        finally:
            if browser: await browser.close()


async def interactive_retry_failed_files():
    """
    Uruchamia interaktywne narzędzie do ponawiania pobierania dla plików,
    które wcześniej zakończyły się błędem.
    """
    console.clear()
    console.print(Panel("[bold yellow]🛠️ Interaktywne Narzędzie do Ponawiania Błędów 🛠️[/]", border_style="yellow"))
    
    failed_urls = await get_failed_urls_from_db()
    if not failed_urls:
        console.print("\n[bold green]✅ Gratulacje! Nie znaleziono żadnych plików z błędami do naprawy.[/bold green]")
        return

    selected_urls = await _interactive_file_selector(failed_urls, "Wybierz URL-e do ponowienia")
    if not selected_urls:
        logger.warning("Nie wybrano żadnych URL-i do ponowienia.")
        return

    async with async_playwright() as p:
        browser, page = None, None
        try:
            browser = await getattr(p, BROWSER_TYPE).launch_persistent_context(Path(SESSION_DIR).expanduser(), headless=False, args=BROWSER_ARGS.get(BROWSER_TYPE))
            page = await browser.new_page()
            await check_login_status(page)

            with Progress(TextColumn("[cyan]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", transient=True) as progress:
                task = progress.add_task("Ponawiam...", total=len(selected_urls))
                for url in selected_urls:
                    progress.console.print(f"Przetwarzam: [dim]{url}[/dim]")
                    await page.goto(url)

                    # === POCZĄTEK ZMIAN ===
                    # Tworzymy prosty callback, który drukuje status pod głównym paskiem postępu
                    status_callback = lambda msg: progress.console.print(f"[dim cyan]  -> {msg}[/dim cyan]")
                    
                    # Przekazujemy nowo utworzony callback do funkcji
                    success, status_result, details = await process_single_photo_page(
                        page, page.url, 'forced', status_callback
                    )
                    # === KONIEC ZMIAN ===

                    if success:
                        progress.console.print(f"[green]✅ Sukces:[/green] {details.get('FileName', 'N/A')}")
                    else:
                        progress.console.print(f"[red]❌ Błąd:[/red] {details.get('error_message', 'Nieznany błąd')}")
                    progress.update(task, advance=1)

        except Exception as e:
            logger.critical("Wystąpił krytyczny błąd podczas interaktywnego ponawiania.", exc_info=True)
            console.print(f"\n[bold red]Wystąpił krytyczny błąd: {e}[/]")
        finally:
            if browser: await browser.close()
