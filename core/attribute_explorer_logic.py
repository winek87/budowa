# -*- coding: utf-8 -*-

# plik: core/attribute_explorer_logic.py
# Wersja 3.0 - W pełni udokumentowany i zintegrowany
#
# ##############################################################################
# ===                    MODUŁ ODKRYWCY ATRYBUTÓW                            ===
# ##############################################################################
#
# "Odkrywca Atrybutów" to narzędzie deweloperskie, które służy do "prześwietlania"
# strony Google Photos. Jego zadaniem jest:
#
#  1. Otworzyć podany przez użytkownika URL do zdjęcia.
#  2. Otworzyć panel boczny z informacjami.
#  3. Przeskanować całą stronę w poszukiwaniu wszystkich elementów HTML.
#  4. Dla każdego elementu, wyciągnąć wszystkie jego atrybuty (np. `aria-label`,
#     `jslog`, `data-test-id`).
#  5. Wyświetlić wyniki w czytelnej tabeli.
#
# Jest to kluczowe narzędzie do znajdowania nowych, stabilnych selektorów CSS,
# gdy Google zaktualizuje wygląd swojej strony.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import re
import logging
from pathlib import Path

# --- Playwright ---
from playwright.async_api import async_playwright

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import (
    SESSION_DIR, BROWSER_TYPE, BROWSER_ARGS,
    INFO_PANEL_BUTTON_SELECTOR, WAIT_FOR_PAGE_LOAD, WAIT_FOR_SELECTOR
)

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


async def run_attribute_explorer():
    """
    Uruchamia interaktywne narzędzie do odkrywania atrybutów HTML na stronie.
    """
    console.clear()
    logger.info("Uruchamiam Odkrywcę Atrybutów...")

    description = (
        "[bold green]Witaj w Zaawansowanym Odkrywcy Atrybutów![/]\n\n"
        "To narzędzie przeskanuje podaną stronę i pokaże wszystkie atrybuty "
        "dla każdego znalezionego elementu HTML. Jest to przydatne do "
        "znajdowania nowych selektorów, gdy strona Google Photos się zmieni."
    )
    console.print(Panel(description, title="Odkrywca Atrybutów", border_style="green"))

    url = Prompt.ask("\n[cyan]Wklej przykładowy URL do zdjęcia[/]")
    if not url.strip().startswith("http"):
        logger.error(f"Wprowadzono nieprawidłowy URL: {url}"); return

    logger.info(f"Rozpoczynam analizę dla URL: {url}")

    async with async_playwright() as p:
        browser = None
        try:
            with console.status("[yellow]Uruchamianie przeglądarki i analiza strony...[/]", spinner="dots"):
                browser = await getattr(p, BROWSER_TYPE).launch_persistent_context(
                    Path(SESSION_DIR).expanduser(), headless=False, args=BROWSER_ARGS.get(BROWSER_TYPE)
                )
                page = await browser.new_page()

                await page.goto(url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)

                if not (photo_id_match := re.search(r'AF1Qip[\w-]+', url)):
                    raise ValueError("Nie udało się znaleźć ID zdjęcia w podanym linku.")
                photo_id = photo_id_match.group(0)

                info_panel_selector = f"c-wiz[jslog*='{photo_id}']"
                try:
                    if not await page.is_visible(info_panel_selector):
                        await page.click(INFO_PANEL_BUTTON_SELECTOR, timeout=WAIT_FOR_SELECTOR * 1000)
                        await page.wait_for_selector(info_panel_selector, timeout=WAIT_FOR_SELECTOR * 1000, state="visible")
                    logger.info("Pomyślnie otwarto panel informacji.")
                except Exception:
                    logger.warning("Nie udało się otworzyć panelu informacji. Przeszukam tylko główny widok.")

                search_areas = [page.locator(info_panel_selector), page.locator(f"c-wiz[data-photo-id='{photo_id}']")]
                all_elements_data, processed_elements_html = [], set()

                for area in search_areas:
                    if not await area.is_visible(): continue
                    elements = await area.locator('*').all()
                    for element in elements:
                        try:
                            if (outer_html := await element.evaluate('node => node.outerHTML')) in processed_elements_html: continue
                            processed_elements_html.add(outer_html)

                            get_attrs_script = "node => Array.from(node.attributes).map(attr => ({ name: attr.name, value: attr.value }))"
                            attributes = await element.evaluate(get_attrs_script)

                            if attributes:
                                tag_name = await element.evaluate('node => node.tagName.toLowerCase()')
                                all_elements_data.append({'tag': tag_name, 'attrs': attributes})
                        except Exception: continue

            if not all_elements_data:
                logger.warning("Nie znaleziono żadnych elementów z atrybutami na stronie.")
                console.print("[yellow]Nie znaleziono żadnych elementów z atrybutami na stronie.[/yellow]")
                return

            table = Table(title="Znalezione Elementy i ich Atrybuty", show_header=True, header_style="bold magenta", expand=True, show_lines=True)
            table.add_column("Typ Elementu (tag)", style="cyan", width=15); table.add_column("Wszystkie Atrybuty (nazwa='wartość')", style="green")

            for data in all_elements_data:
                attrs_str_list = [f"[yellow]{attr['name']}[/yellow]='{attr['value'].replace('', ' ').strip()}'" for attr in data['attrs']]
                table.add_row(f"`{data['tag']}`", "\n".join(attrs_str_list))

            console.print("\n"); console.print(table)
            logger.info(f"Analiza zakończona. Znaleziono i wyświetlono {len(all_elements_data)} elementów z atrybutami.")

        except Exception as e:
            logger.critical("Wystąpił nieoczekiwany błąd w Odkrywcy Atrybutów.", exc_info=True)
            console.print(f"[bold red]Wystąpił błąd: {e}[/bold red]")
        finally:
            if browser: await browser.close()
            logger.info("Przeglądarka została zamknięta.")
            Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić do menu głównego...[/]")
