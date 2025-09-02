# -*- coding: utf-8 -*-

# plik: core/session_logic.py
# Wersja 4.0 - W pełni asynchroniczny i udokumentowany
#
# ##############################################################################
# ===                     MODUŁ ZARZĄDZANIA SESJĄ LOGOWANIA                  ===
# ##############################################################################
#
# Ten plik zawiera logikę odpowiedzialną za proces ręcznego logowania
# użytkownika do konta Google. Jego zadaniem jest usunięcie starej sesji,
# uruchomienie nowej, czystej przeglądarki i poczekanie, aż użytkownik
# zakończy proces logowania, a następnie zapisanie nowej sesji na dysku.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import shutil
import logging
from pathlib import Path
import asyncio

# --- Playwright ---
from playwright.async_api import async_playwright

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import (
    SESSION_DIR,
    BROWSER_TYPE,
    BROWSER_ARGS,
    WAIT_FOR_PAGE_LOAD
)

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


async def refresh_session():
    """
    Prowadzi użytkownika przez proces czyszczenia starej sesji i tworzenia nowej.

    Proces:
    1.  Asynchronicznie czyści stary folder sesji, aby zapewnić czysty start.
    2.  Wyświetla szczegółowe instrukcje dla użytkownika.
    3.  Prosi o potwierdzenie przed uruchomieniem przeglądarki.
    4.  Uruchamia nową instancję przeglądarki w trybie widocznym (`headless=False`),
        w której użytkownik może się swobodnie zalogować.
    5.  Skrypt czeka w tle, aż użytkownik ręcznie zamknie okno przeglądarki,
        co sygnalizuje zakończenie procesu logowania i zapisanie nowej sesji.
    6.  Wyświetla komunikat o sukcesie operacji.
    """
    console.clear()
    logger.info("Rozpoczynam procedurę odświeżania sesji logowania...")
    console.print(Panel("🔑 Odświeżanie Sesji Logowania 🔑", expand=False, style="bold yellow"))

    # Krok 1: Asynchroniczne przygotowanie czystego miejsca na nową sesję
    session_path = Path(SESSION_DIR).expanduser()
    try:
        if await asyncio.to_thread(session_path.exists):
            logger.warning(f"Znaleziono istniejący folder sesji w '{session_path}'. Zostanie on usunięty.")
            await asyncio.to_thread(shutil.rmtree, session_path)
            logger.info("Stary folder sesji został pomyślnie usunięty.")
        
        await asyncio.to_thread(session_path.mkdir, parents=True, exist_ok=True)
        logger.debug(f"Utworzono nowy folder sesji w '{session_path}'.")
    except OSError as e:
        logger.critical(f"Nie udało się przygotować folderu sesji w '{session_path}': {e}", exc_info=True)
        console.print(f"[bold red]Błąd krytyczny: Nie można zarządzać folderem '{session_path}'. Sprawdź uprawnienia.[/]")
        return

    # Krok 2: Wyświetlenie instrukcji dla użytkownika
    instructions = (
        "[bold]Za chwilę otworzy się nowe okno przeglądarki.[/bold]\n\n"
        "Twoim zadaniem jest:\n"
        "1. Zalogować się na swoje konto Google (login, hasło, 2FA itp.).\n"
        "2. Po pomyślnym zalogowaniu, gdy zobaczysz stronę Google Photos, po prostu\n"
        "   [bold red]ZAMKNIJ OKNO PRZEGLĄDARKI RĘCZNIE[/] (klikając 'X').\n\n"
        "Skrypt automatycznie wykryje zamknięcie okna i zapisze nową sesję."
    )
    console.print(Panel(instructions, title="[yellow]Instrukcje Odświeżania Sesji[/]", border_style="yellow"))

    if not Confirm.ask("\n[bold cyan]Czy jesteś gotowy, aby kontynuować?[/]", default=True):
        logger.warning("Procedura odświeżania sesji anulowana przez użytkownika.")
        return

    # Krok 3: Uruchomienie przeglądarki i oczekiwanie na działanie użytkownika
    try:
        async with async_playwright() as p:
            browser_launcher = getattr(p, BROWSER_TYPE, p.chromium)
            logger.info(f"Uruchamiam przeglądarkę '{BROWSER_TYPE}' w trybie odświeżania sesji...")

            with console.status("[cyan]Otwieranie przeglądarki...[/]"):
                context = await browser_launcher.launch_persistent_context(
                    user_data_dir=session_path, headless=False,
                    accept_downloads=True, args=BROWSER_ARGS.get(BROWSER_TYPE)
                )
                page = await context.new_page()
                await page.goto("https://photos.google.com/", wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)

            logger.info("Przeglądarka otwarta. Oczekuję na ręczne zamknięcie okna przez użytkownika...")
            console.print("\n[bold green]✅ Przeglądarka otwarta. Czekam, aż ją zamkniesz po zalogowaniu...[/]")
            
            await context.wait_for_event('close', timeout=0)

        logger.info("Okno przeglądarki zostało zamknięte. Sesja zapisana pomyślnie!")
        console.print(Panel(
            f"Nowa sesja logowania została zapisana w folderze:\n[cyan]{session_path.resolve()}[/cyan]",
            title="[bold green]✅ Sukces![/bold green]", border_style="green"
        ))

    except Exception as e:
        logger.critical("Wystąpił krytyczny błąd podczas uruchamiania przeglądarki.", exc_info=True)
        console.print(Panel(
            f"[bold red]Błąd krytyczny podczas uruchamiania Playwright![/]\n\n"
            f"Szczegóły: [dim]{e}[/dim]\n\n"
            "Upewnij się, że Playwright jest poprawnie zainstalowany.\n"
            "Uruchom w terminalu polecenie: [cyan]playwright install --with-deps[/cyan]",
            title="[red]Błąd Playwright[/red]", border_style="red"
        ))
