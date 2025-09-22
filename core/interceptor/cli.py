# plik: core/interceptor/cli.py
# Wersja 1.1 - Naprawiono błąd AttributeError w bloku `finally`.
# Opis: Moduł zarządzający przepływem pracy dla narzędzia deweloperskiego
#       do analizy komunikacji sieciowej.
# -*- coding: utf-8 -*-

import asyncio
import logging
from pathlib import Path

from playwright.async_api import async_playwright

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

# Importy z modułów projektu `core`
from ..config import SESSION_DIR, BROWSER_TYPE, BROWSER_ARGS, WAIT_FOR_PAGE_LOAD

# Importy z wewnętrznych modułów pakietu
from .tasks import Interceptor
from .ui import InterceptorLiveDisplay, display_interceptor_summary

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)


async def run_interceptor():
    """
    Uruchamia interfejs dla eksperymentalnego "Silnika Przechwytującego".
    """
    console.clear()
    logger.info("Uruchamiam Podsłuch Sieciowy (Interceptor)...")
    
    console.print(Panel(
        "Ten moduł to zaawansowane narzędzie deweloperskie, które 'podsłuchuje' "
        "ukrytą komunikację (żądania sieciowe) między przeglądarką a serwerami Google.",
        title="📡 [bold]Podsłuch Sieciowy (Interceptor)[/]",
        border_style="red"
    ))

    url = Prompt.ask("\n[cyan]Wklej adres URL do zdjęcia/filmu, który chcesz przeanalizować[/]")
    if not url.strip().startswith("http"):
        logger.error(f"Wprowadzono nieprawidłowy URL: {url}")
        console.print("[bold red]To nie jest prawidłowy adres URL. Anuluję.[/bold red]")
        return

    logger.info(f"Rozpoczynam nasłuch dla URL: {url}")

    async with async_playwright() as p:
        browser_context = None # Zmieniamy nazwę dla jasności
        try:
            with InterceptorLiveDisplay(console=console) as display:
                browser_context = await getattr(p, BROWSER_TYPE).launch_persistent_context(
                    Path(SESSION_DIR).expanduser(), headless=False, args=BROWSER_ARGS.get(BROWSER_TYPE)
                )
                page = await browser_context.new_page()
                
                interceptor = Interceptor(page, display.add_capture)
                await interceptor.start_listening()

                await page.goto(url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
                
                logger.info("Oczekuję na ręczne zamknięcie okna przeglądarki...")
                await browser_context.wait_for_event('close', timeout=0)
            
            captured_data = interceptor.captured_data
            display_interceptor_summary(captured_data)

        except Exception as e:
            # Błąd 'Target page, context or browser closed' jest oczekiwany, gdy użytkownik zamyka okno
            if "closed" not in str(e).lower():
                logger.critical("Wystąpił krytyczny błąd w Podsłuchu Sieciowym.", exc_info=True)
                console.print(f"[bold red]Wystąpił błąd: {e}[/bold red]")
        finally:
            # === POCZĄTEK POPRAWKI ===
            # `browser_context` jest kontekstem, nie przeglądarką.
            # Po prostu próbujemy go zamknąć, ignorując błędy, jeśli już jest zamknięty.
            if browser_context:
                try:
                    await browser_context.close()
                except Exception:
                    pass
            # === KONIEC POPRAWKI ===
