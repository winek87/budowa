# plik: core/attribute_explorer/tasks.py
# Wersja 1.2 - Zintegrowano z nowym dashboardem LiveDisplay.
# Opis: Moduł zawiera logikę analizy strony.
# -*- coding: utf-8 -*-

import logging
import re
from typing import List, Dict, Any, Tuple

from playwright.async_api import Page, Locator
from rich.console import Console
from rich.prompt import Prompt

from ..config import INFO_PANEL_BUTTON_SELECTOR, WAIT_FOR_SELECTOR
from .ui.live_display import AttributeExplorerLiveDisplay

logger = logging.getLogger(__name__)
console = Console()

async def find_all_attributes_on_page(page: Page, url: str, display: AttributeExplorerLiveDisplay) -> List[Dict[str, Any]]:
    """Skanuje stronę, raportując postęp do obiektu LiveDisplay."""
    if not (photo_id_match := re.search(r'AF1Qip[\w-]+', url)):
        raise ValueError("Nie udało się znaleźć ID zdjęcia w podanym linku.")
    photo_id = photo_id_match.group(0)

    display.update_status("Próba otwarcia panelu informacji...")
    info_panel_selector = f"c-wiz[jslog*='{photo_id}']"
    panel_opened = False
    try:
        if await page.is_visible(info_panel_selector):
            panel_opened = True
            logger.info("Panel informacji był już otwarty.")
            display.add_log("Panel informacji był już otwarty.", "dim")
        else:
            await page.click(INFO_PANEL_BUTTON_SELECTOR, timeout=WAIT_FOR_SELECTOR * 1000)
            await page.wait_for_selector(info_panel_selector, timeout=WAIT_FOR_SELECTOR * 1000, state="visible")
            panel_opened = True
            logger.info("Pomyślnie otwarto panel informacji.")
            display.add_log("✅ Panel informacji otwarty.", "green")
    except Exception as e:
        logger.warning(f"Automatyczne otwarcie panelu nie powiodło się: {e}")
        display.add_log("🟡 Automatyczne otwarcie panelu nie powiodło się.", "yellow")

    if not panel_opened:
        display.stop() # Zatrzymujemy Live, aby wyświetlić Prompt
        console.print("\n[bold yellow]Proszę, ręcznie kliknij ikonę 'i', aby otworzyć panel boczny.[/bold yellow]")
        Prompt.ask("[bold]Gdy panel będzie widoczny, naciśnij Enter, aby kontynuować...[/]")
        display.start() # Wznawiamy Live

    search_areas: List[Locator] = []
    if main_area := page.locator(f"c-wiz[data-photo-id='{photo_id}']"):
        if await main_area.count() > 0: search_areas.append(main_area)
    if info_panel_area := page.locator(info_panel_selector):
        if await info_panel_area.count() > 0: search_areas.append(info_panel_area)

    all_elements_data = []
    total_elements = sum([await area.locator('*').count() for area in search_areas])
    display.progress_bar.update(display._task_id, total=total_elements)
    
    for area in search_areas:
        elements = await area.locator('*').all()
        for element in elements:
            display.update_status(f"Analiza {display.found_elements}/{total_elements} elementów...")
            try:
                get_attrs_script = "node => Array.from(node.attributes).map(attr => ({ name: attr.name, value: attr.value }))"
                attributes = await element.evaluate(get_attrs_script, timeout=500)
                if attributes:
                    tag_name = await element.evaluate('node => node.tagName.toLowerCase()', timeout=500)
                    all_elements_data.append({'tag': tag_name, 'attrs': attributes})
                    display.found_elements += 1
            except Exception: pass
            finally: display.progress_bar.update(display._task_id, advance=1)
                
    return all_elements_data
