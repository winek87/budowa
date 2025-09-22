# plik: core/attribute_explorer/cli.py
# Wersja 1.4 - Poprawiono nazwy klawiszy nawigacyjnych w przeglądarce wyników.
# Opis: Moduł zarządzający przepływem pracy dla Odkrywcy Atrybutów.
# -*- coding: utf-8 -*-

import asyncio
import logging
import re
from pathlib import Path
from datetime import datetime

from playwright.async_api import async_playwright
from rich.console import Console, Group
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from rich.align import Align
from rich.text import Text

from ..config import SESSION_DIR, BROWSER_TYPE, BROWSER_ARGS, WAIT_FOR_PAGE_LOAD
from .utils import get_key
from .tasks import find_all_attributes_on_page
from .ui.live_display import AttributeExplorerLiveDisplay

console = Console()
logger = logging.getLogger(__name__)

def save_results_to_file(url: str, data: list) -> Path:
    output_dir = Path("app_data/atrybuty"); output_dir.mkdir(parents=True, exist_ok=True)
    photo_id = (re.search(r'AF1Qip[\w-]+', url).group(0)[-12:] if re.search(r'AF1Qip[\w-]+', url) else "unknown")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"{timestamp}_{photo_id}.log"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"# Wyniki analizy dla URL:\n# {url}\n\n")
        for d in data:
            f.write(f"--- <{d['tag']}> ---\n")
            for attr in d['attrs']: f.write(f"  {attr['name']} = \"{attr['value']}\"\n")
            f.write("\n")
    return output_path

async def display_scrollable_results(all_data: list, file_path: Path):
    """Wyświetla wyniki w interaktywnym, przewijalnym dashboardzie."""
    scroll_pos = 0
    
    def build_layout() -> Layout:
        visible_rows = console.height - 10
        # Dodajemy zabezpieczenie, aby scroll_pos nie wyszedł poza zakres
        start_index = max(0, min(scroll_pos, len(all_data) - visible_rows if len(all_data) > visible_rows else 0))
        visible_data = all_data[start_index : start_index + visible_rows]

        table = Table(title="[bold]Znalezione Elementy i ich Atrybuty[/]", show_header=True, header_style="bold magenta", expand=True, show_lines=True, border_style="dim")
        table.add_column("Typ Elementu (tag)", style="cyan", width=15); table.add_column("Atrybuty", style="green")
        for data in visible_data:
            attrs = [f"[yellow]{a['name']}[/yellow]='[dim]{a['value']}[/dim]'" for a in data['attrs']]
            table.add_row(f"[bold]{data['tag']}[/bold]", "\n".join(attrs))
        
        info = Panel(Align.center(f"Wyświetlanie {start_index+1}-{min(start_index+visible_rows, len(all_data))} z {len(all_data)}. Pełne wyniki: [cyan]{file_path}[/]"), border_style="green")
        footer = Text.from_markup("Nawigacja: [on bright_black] ▲/▼ [/] [on bright_black] PgUp/PgDn [/] [on bright_black] Home/End [/]  |  Wyjście: [on bright_black] Q [/]", justify="center")
        layout = Layout(); layout.split_column(Layout(table), Layout(info, size=3), Layout(footer, size=1))
        return layout

    with Live(build_layout(), screen=True, auto_refresh=False) as live:
        while True:
            live.update(build_layout(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue
            if key.upper() == 'Q': break
            
            # === POCZĄTEK POPRAWKI: Używamy poprawnych, wielkich liter ===
            visible_rows = console.height - 10
            if key == "UP": scroll_pos = max(0, scroll_pos - 1)
            elif key == "DOWN": scroll_pos = min(len(all_data) - 1, scroll_pos + 1)
            elif key == "PAGE_UP": scroll_pos = max(0, scroll_pos - visible_rows)
            elif key == "PAGE_DOWN": scroll_pos = min(len(all_data) - visible_rows, scroll_pos + visible_rows)
            elif key == "HOME": scroll_pos = 0
            elif key == "END": scroll_pos = max(0, len(all_data) - visible_rows)
            # === KONIEC POPRAWKI ===

async def run_attribute_explorer():
    """Uruchamia interaktywne narzędzie do odkrywania atrybutów HTML."""
    console.clear()
    description = ("[bold green]Witaj w Odkrywcy Atrybutów![/]\n\nTo narzędzie deweloperskie przeskanuje podaną stronę i wyświetli atrybuty dla każdego elementu. Wyniki zostaną zapisane do pliku w [cyan]app_data/atrybuty[/].")
    console.print(Panel(description, title="🔎 Odkrywca Atrybutów 🔎", border_style="green", expand=False))
    url = Prompt.ask("\n[cyan]Wklej przykładowy URL do zdjęcia[/]")
    if not url.strip().startswith("http"): console.print("[bold red]To nie jest prawidłowy adres URL.[/]"); return

    async with async_playwright() as p:
        browser = None
        try:
            with AttributeExplorerLiveDisplay(console=console) as display:
                display.update_status("Uruchamianie przeglądarki...")
                browser = await getattr(p, BROWSER_TYPE).launch_persistent_context(Path(SESSION_DIR).expanduser(), headless=False, args=BROWSER_ARGS.get(BROWSER_TYPE))
                page = await browser.new_page()
                display.update_status("Nawigacja do strony...")
                await page.goto(url, wait_until='load', timeout=WAIT_FOR_PAGE_LOAD * 1000)
                all_elements_data = await find_all_attributes_on_page(page, url, display)

            if not all_elements_data:
                console.print(Panel("[bold yellow]Nie znaleziono żadnych elementów z atrybutami.[/]", title="Brak Wyników")); return

            saved_file_path = save_results_to_file(url, all_elements_data)
            await display_scrollable_results(all_elements_data, saved_file_path)

        except Exception as e:
            console.print(Panel(f"[bold red]Wystąpił błąd krytyczny:[/]\n\n{e}", title="Błąd", border_style="red"))
        finally:
            if browser: await browser.close()
