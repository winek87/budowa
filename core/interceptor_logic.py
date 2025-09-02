# -*- coding: utf-8 -*-

# plik: core/interceptor_logic.py
# Wersja 4.0 - W pełni udokumentowany i zintegrowany z nowymi standardami
#
# ##############################################################################
# ===                 MODUŁ PODSŁUCHU SIECIOWEGO (INTERCEPTOR)               ===
# ##############################################################################
#
# "Interceptor" to zaawansowane narzędzie deweloperskie, które "podsłuchuje"
# ukrytą komunikację (żądania sieciowe) między przeglądarką a serwerami Google.
#
# Jego celem jest przechwycenie odpowiedzi z serwera, które zawierają
# metadane zdjęcia w czystej, ustrukturyzowanej formie (JSON).
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import json
import logging
from pathlib import Path
from collections import deque
import asyncio

# --- Playwright ---
from playwright.async_api import async_playwright, Response

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.prompt import Prompt
from rich.json import JSON
from rich.text import Text
from rich.live import Live
from rich.layout import Layout

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import SESSION_DIR, BROWSER_TYPE, BROWSER_ARGS, WAIT_FOR_PAGE_LOAD

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


async def run_interceptor():
    """
    Uruchamia interfejs dla eksperymentalnego "Silnika Przechwytującego".

    Proces:
    1.  Prosi użytkownika o URL do analizy.
    2.  Uruchamia przeglądarkę w trybie widocznym (`headless=False`).
    3.  Podpina "nasłuch" (`handle_response`) do wszystkich odpowiedzi sieciowych.
    4.  Nawiguje do podanej strony i wyświetla interfejs na żywo, który
        pokazuje przechwycone zapytania w czasie rzeczywistym.
    5.  Czeka, aż użytkownik ręcznie zamknie okno przeglądarki.
    6.  Po zamknięciu, wyświetla pełne, sformatowane dane JSON ze wszystkich
        przechwyconych odpowiedzi.
    """
    console.clear()
    logger.info("Uruchamiam Eksperymentalny Podsłuch Sieciowy (Interceptor)...")
    console.print(Panel(
        "📡 Eksperymentalny Podsłuch Sieciowy (Interceptor) 📡",
        subtitle="[dim]Narzędzie deweloperskie do analizy komunikacji z API[/dim]",
        expand=False, style="bold red"
    ))

    url = Prompt.ask("\n[cyan]Wklej adres URL do zdjęcia/filmu, który chcesz przeanalizować[/]")
    if not url.strip().startswith("http"):
        logger.error(f"Wprowadzono nieprawidłowy URL: {url}")
        console.print("[bold red]To nie jest prawidłowy adres URL. Anuluję.[/bold red]")
        return

    logger.info(f"Rozpoczynam nasłuch dla URL: {url}")

    captured_data = []
    live_captured_urls = deque(maxlen=15)

    async def handle_response(response: Response):
        """
        Funkcja wywoływana asynchronicznie przez Playwright dla każdej
        odpowiedzi otrzymanej z serwera.
        """
        # Sprawdzamy, czy odpowiedź ma typ zawartości JSON
        if "application/json" in response.headers.get("content-type", "").lower():
            try:
                data = await response.json()
                captured_data.append({"url": response.url, "json_data": data})
                
                # Przygotuj tekst do wyświetlenia w interfejsie Live
                short_url = response.url.split('?')[0]
                size_kb = len(json.dumps(data)) / 1024
                display_text = Text.from_markup(f"✅ [green]{Path(short_url).name}[/] [dim]({size_kb:.1f} KB)[/dim]")
                live_captured_urls.appendleft(display_text)
                logger.debug(f"Przechwycono odpowiedź JSON z {short_url} ({size_kb:.1f} KB)")
                
            except json.JSONDecodeError:
                logger.debug(f"Nie udało się sparsować odpowiedzi JSON (błąd dekodowania) z: {response.url}")
            except Exception as e:
                logger.warning(f"Nieoczekiwany błąd podczas obsługi odpowiedzi z {response.url}: {e}")

    async with async_playwright() as p:
        browser = None
        try:
            with console.status("[cyan]Uruchamianie przeglądarki...[/]"):
                browser = await getattr(p, BROWSER_TYPE).launch_persistent_context(
                    Path(SESSION_DIR).expanduser(), headless=False, args=BROWSER_ARGS.get(BROWSER_TYPE)
                )
                page = await browser.new_page()
                page.on("response", handle_response)

            with console.status(f"[cyan]Nawigacja do [dim]{url[:60]}...[/dim][/]"):
                await page.goto(url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)

            info_panel = Panel(
                "[bold cyan]Strona załadowana.[/bold cyan]\n\n"
                "Wykonaj jakieś akcje (np. otwórz panel info, przewiń), aby wygenerować ruch sieciowy.\n"
                "Obserwuj panel 'Przechwycone Zapytania' poniżej.\n\n"
                "[bold red]Gdy skończysz, ZAMKNIJ OKNO PRZEGLĄDARKI, aby zobaczyć podsumowanie.[/bold red]",
                title="Instrukcje", border_style="yellow"
            )
            
            def generate_live_layout() -> Layout:
                layout = Layout()
                layout.split_column(
                    Layout(info_panel, size=8),
                    Layout(Panel(Group(*live_captured_urls), title="Przechwycone Odpowiedzi JSON (na żywo)"), name="live_view")
                )
                return layout

            with Live(generate_live_layout(), screen=True, auto_refresh=True, vertical_overflow="visible") as live:
                logger.info("Oczekuję na ręczne zamknięcie okna przeglądarki przez użytkownika...")
                await browser.wait_for_event('close', timeout=0)

        except Exception as e:
            logger.critical("Wystąpił krytyczny błąd w Podsłuchu Sieciowym.", exc_info=True)
            console.print(f"[bold red]Wystąpił błąd: {e}[/bold red]")
        finally:
            if browser and browser.contexts:
                # To jest dodatkowe zabezpieczenie, ale zamknięcie kontekstu przez
                # użytkownika zazwyczaj zamyka też przeglądarkę.
                await browser.close()
    
    console.clear()
    if captured_data:
        logger.info(f"Analiza zakończona. Przechwycono {len(captured_data)} odpowiedzi JSON.")
        console.print(Panel(f"Przechwycono łącznie [bold cyan]{len(captured_data)}[/bold cyan] odpowiedzi w formacie JSON.", title="[green]Analiza Zakończona[/]"))
        for item in captured_data:
            json_renderable = JSON.from_data(item["json_data"])
            console.print(Panel(
                json_renderable,
                title=f"URL: {item['url']}",
                border_style="cyan",
                subtitle=f"Rozmiar: {len(json.dumps(item['json_data']))/1024:.2f} KB"
            ))
    else:
        logger.warning("Nie udało się przechwycić żadnych danych w formacie JSON.")
        console.print(Panel("[yellow]Nie udało się przechwycić żadnych odpowiedzi w formacie JSON.[/yellow]", title="Brak Wyników"))
