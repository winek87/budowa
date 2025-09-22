# plik: core/ai_tagger/workflows/manager.py (WERSJA FINALNA Z NAWIGACJĄ PO LIŚCIE)
# -*- coding: utf-8 -*-

import asyncio
import math
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.text import Text
from rich.live import Live
from rich.console import Console, ConsoleOptions, RenderResult, Group

from ...database import (
    get_all_unique_tags, rename_tag_globally, 
    merge_tags_globally, delete_tag_globally
)
from ...utils import get_key

console = Console()
TAGS_PER_PAGE = 20

class TagManagerUI:
    """Zarządza stanem i renderowaniem interaktywnego menedżera tagów."""
    def __init__(self, all_tags):
        self.all_tags = all_tags
        self.current_page = 1
        self.total_pages = math.ceil(len(all_tags) / TAGS_PER_PAGE)
        self.selected_index_on_page = 0
        self.action = None

    def get_current_tags_on_page(self) -> list:
        """Zwraca listę tagów dla bieżącej strony."""
        start = (self.current_page - 1) * TAGS_PER_PAGE
        end = start + TAGS_PER_PAGE
        return self.all_tags[start:end]

    def get_selected_tag(self) -> str | None:
        """Zwraca nazwę aktualnie podświetlonego tagu."""
        tags_on_page = self.get_current_tags_on_page()
        if 0 <= self.selected_index_on_page < len(tags_on_page):
            return tags_on_page[self.selected_index_on_page]['tag']
        return None

    def __rich__(self) -> Panel:
        """Renderuje cały widok menedżera."""
        tags_on_page = self.get_current_tags_on_page()
        
        table = Table(
            title=f"Znaleziono {len(self.all_tags)} unikalnych tagów",
            caption=Text.from_markup(f"Strona {self.current_page}/{self.total_pages}", justify="center"),
            header_style="b magenta",
            row_styles=["", "dim"] # Naprzemienne kolory wierszy
        )
        table.add_column("Tag", style="cyan"); table.add_column("Liczba Wystąpień", justify="right", style="green")
        
        for i, item in enumerate(tags_on_page):
            style = "bold black on white" if i == self.selected_index_on_page else ""
            table.add_row(item['tag'], str(item['count']), style=style)

        footer_text = Text.from_markup(
            "[bold]Nawigacja:[/bold] [cyan]↑↓[/] (wybierz) | [cyan]← →[/] (strony) | [cyan]Enter[/] (akcje) | [cyan]S[/cyan] (scal) | [cyan]W[/cyan] (wyjdź)",
            justify="center"
        )
        return Panel(Group(table, footer_text), title="🗂️ Menedżer Tagów AI 🗂️")


async def run_tag_manager():
    """Główny interfejs do zarządzania tagami AI w bazie danych."""
    with console.status("[c]Agregowanie tagów...[/]"):
        all_tags = await get_all_unique_tags()

    if not all_tags:
        console.clear(); console.print(Panel("🗂️ Menedżer Tagów AI 🗂️", style="b purple"))
        console.print("\n[y]Brak tagów do zarządzania.[/y]"); Prompt.ask("\n[b]Enter...[/]"); return

    ui = TagManagerUI(all_tags)

    with Live(ui, screen=True, auto_refresh=False, transient=True) as live:
        should_exit = False
        while not should_exit:
            live.update(ui, refresh=True)
            key = await asyncio.to_thread(get_key)

            tags_on_page = ui.get_current_tags_on_page()
            
            # Nawigacja
            if key == "UP": ui.selected_index_on_page = max(0, ui.selected_index_on_page - 1)
            elif key == "DOWN": ui.selected_index_on_page = min(len(tags_on_page) - 1, ui.selected_index_on_page + 1)
            elif key == "LEFT":
                ui.current_page = max(1, ui.current_page - 1)
                ui.selected_index_on_page = 0
            elif key == "RIGHT":
                ui.current_page = min(ui.total_pages, ui.current_page + 1)
                ui.selected_index_on_page = 0
            elif key.lower() == 'w':
                should_exit = True
            elif key.lower() == 's':
                # Akcja globalna: Scalanie
                live.stop()
                sources_str = Prompt.ask("[b]Tagi do scalenia (oddzielone przecinkiem)[/b]").strip().lower()
                sources = {s.strip() for s in sources_str.split(',') if s.strip()}
                target = Prompt.ask("[b]Nazwa nowego, wspólnego tagu[/b]").strip().lower()
                if sources and target and Confirm.ask(f"\n[b y]Scalić {', '.join(sources)} w jeden tag '{target}'?[/b y]"):
                    with console.status("[c]Aktualizuję bazę danych...[/]"):
                        await merge_tags_globally(list(sources), target)
                    all_tags = await get_all_unique_tags()
                    ui.all_tags = all_tags
                    ui.total_pages = math.ceil(len(all_tags) / TAGS_PER_PAGE)
                live.start()
            elif key == "ENTER":
                selected_tag = ui.get_selected_tag()
                if selected_tag:
                    live.stop()
                    
                    action = Prompt.ask(
                        f"Wybierz akcję dla tagu [bold cyan]'{selected_tag}'[/bold cyan]",
                        choices=["z", "u", "a"],
                        default="a"
                    )
                    
                    if action == 'z': # Zmień nazwę
                        new_name = Prompt.ask(f"[b]Nowa nazwa dla '{selected_tag}'[/b]").strip().lower()
                        if new_name and Confirm.ask(f"\n[b y]Zmienić '{selected_tag}' na '{new_name}' we wszystkich zdjęciach?[/b y]"):
                             with console.status("[c]Aktualizuję bazę danych...[/]"):
                                await rename_tag_globally(selected_tag, new_name)

                    elif action == 'u': # Usuń
                        if Confirm.ask(f"\n[b r]Jesteś pewien? Tag '{selected_tag}' zostanie trwale usunięty ze wszystkich zdjęć.[/b r]", default=False):
                            with console.status("[c]Aktualizuję bazę danych...[/]"):
                                await delete_tag_globally(selected_tag)
                    
                    # Odśwież dane po każdej akcji
                    with console.status("[c]Odświeżanie listy tagów...[/]"):
                        all_tags = await get_all_unique_tags()
                    ui.all_tags = all_tags
                    ui.total_pages = math.ceil(len(all_tags) / TAGS_PER_PAGE)
                    ui.current_page = min(ui.current_page, ui.total_pages) if ui.total_pages > 0 else 1
                    ui.selected_index_on_page = 0

                    live.start()
