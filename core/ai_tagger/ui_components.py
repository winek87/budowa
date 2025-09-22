# plik: core/ai_tagger/ui_components.py (WERSJA FINALNA)
# -*- coding: utf-8 -*-

import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Union, Optional, Deque
import time

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Table
from rich.text import Text
from rich.prompt import Prompt, IntPrompt

from ..config import MODEL_NAME, AI_TAGGER_CONFIDENCE_THRESHOLD, AI_PROCESSING_BATCH_SIZE, THUMBNAIL_MAX_SIZE
from ..utils import get_key, ThumbnailRenderer

logger = logging.getLogger("app")
console = Console()

# ==============================================================================
# SEKCJA 1: INTERFEJS INTERAKTYWNY (ARCHITEKTURA HYBRYDOWA LIVE + PROMPT)
# ==============================================================================

class InteractiveDashboard:
    """Klasa odpowiedzialna wyłącznie za RENDEROWANIE interfejsu na podstawie stanu."""
    def __init__(self, image_path: Path, proposed_tags: List[Dict]):
        self.image_path = image_path
        self.tags = list(proposed_tags)
        self.mode = "main" # Dostępne tryby: 'main', 'edit'
        self.message = ""
        self._cached_thumbnail = None

    def __rich__(self) -> Layout:
        """Renderuje cały interfejs jako jeden, w pełni responsywny Layout."""
        root_layout = Layout(name="root")
        root_layout.split(
            Layout(name="main", ratio=1),
            Layout(name="footer", size=4)
        )
        
        main_area = root_layout["main"]
        main_area.split_row(Layout(name="preview", ratio=2), Layout(name="tags", ratio=1))
        
        if self._cached_thumbnail is None:
            self._cached_thumbnail = ThumbnailRenderer(self.image_path, max_size=THUMBNAIL_MAX_SIZE)
        main_area["preview"].update(Panel(Align.center(self._cached_thumbnail, vertical="middle"), title="Podgląd", border_style="yellow"))
        
        tags_content = Group(
            Text(self.image_path.name, style="bold cyan", overflow="fold"),
            Text("─" * 40, style="dim")
        )
        tags_table = Table(box=None, show_header=False, padding=(0, 1))
        tags_table.add_column(width=4, justify="right"); tags_table.add_column()
        if self.tags:
            for i, tag in enumerate(self.tags):
                tags_table.add_row(f"[cyan b]{i+1}[/]", f"{tag['label']} [dim]({tag.get('score', 1.0):.0%})[/dim]")
        else:
            tags_table.add_row("", "[yellow]Brak tagów.[/yellow]")
        main_area["tags"].update(Panel(Group(tags_content, tags_table), title="Proponowane Tagi"))

        footer_content = []
        if self.message: footer_content.append(Text(self.message, justify="center"))
        
        if self.mode == "main":
            footer_content.append(Text.from_markup("[b](Z)[/b]atwierdź | [b](E)[/b]dytuj | [b](O)[/b]drzuć | [b](W)[/b]yjście", justify="center"))
        elif self.mode == 'edit':
            footer_content.append(Text.from_markup("[b](D)[/b]odaj | [b](U)[/b]suń | [b](A)[/b]nuluj edycję", justify="center"))
        
        root_layout["footer"].update(Panel(Group(*footer_content), title="Akcje"))
        
        return root_layout

async def interactive_verification_ui_live(image_path: Path, tags_to_verify: List[Dict]) -> Optional[List[Dict]]:
    """Główny 'kontroler', który zarządza pętlą, stanem i interakcją z użytkownikiem."""
    dashboard = InteractiveDashboard(image_path, tags_to_verify)
    final_result: any = ... # Używamy obiektu Ellipsis jako sygnału "brak decyzji"

    with Live(dashboard, screen=True, auto_refresh=False, transient=True) as live:
        live.update(dashboard, refresh=True)
        
        while final_result is Ellipsis:
            # --- GŁÓWNA PĘTLA ZDARZEŃ ---
            key = await asyncio.to_thread(get_key)
            if not key: continue
            if key == "ctrl+c":
                final_result = None; break

            dashboard.message = "" # Zresetuj komunikat
            
            # --- Logika dla trybu GŁÓWNEGO ---
            if dashboard.mode == "main":
                if key.lower() == 'e': dashboard.mode = 'edit'
                elif key.lower() == 'z': final_result = dashboard.tags
                elif key.lower() == 'o': final_result = []
                elif key.lower() == 'w': final_result = None
            
            # --- Logika dla trybu EDYCJI ---
            elif dashboard.mode == "edit":
                if key.lower() == 'a': dashboard.mode = 'main'
                elif key.lower() in ('d', 'u'):
                    # ZATRZYMAJ LIVE, aby użyć standardowego i niezawodnego Prompt
                    live.stop()
                    
                    if key.lower() == 'd':
                        new_tag = Prompt.ask("[b cyan]Nowy tag[/b cyan] (pusty, by anulować)").strip().lower()
                        if new_tag and not any(t['label'] == new_tag for t in dashboard.tags):
                            dashboard.tags.append({'label': new_tag, 'score': 1.0, 'source': 'manual'})
                    
                    elif key.lower() == 'u' and dashboard.tags:
                        try:
                            console.print("Aktualne tagi:")
                            for i, tag in enumerate(dashboard.tags): console.print(f" [cyan b]{i+1}[/]: {tag['label']}")
                            num_to_delete_str = Prompt.ask("[red]Numer tagu do usunięcia[/red]", choices=[str(i+1) for i in range(len(dashboard.tags))])
                            del dashboard.tags[int(num_to_delete_str) - 1]
                        except (ValueError, IndexError):
                             pass # Błąd jest obsługiwany przez Prompt
                    
                    # WZNÓW LIVE, aby odświeżyć widok
                    live.start()
            
            live.update(dashboard, refresh=True)

    return final_result

# ==============================================================================
# SEKCJA 2: KOMPONENTY DLA TRYBU AUTOMATYCZNEGO (bez zmian)
# ==============================================================================
def create_dashboard_layout() -> Layout:
    layout = Layout(name="root"); layout.split_column(Layout(name="header", size=1), Layout(name="body", ratio=1), Layout(name="footer", size=1)); layout["body"].split_column(Layout(name="top_body", size=4), Layout(name="bottom_body", ratio=1)); layout["top_body"].split_row(Layout(name="stats"), Layout(name="settings")); layout["bottom_body"].split_row(Layout(name="logs", ratio=2), Layout(name="preview")); return layout
def update_dashboard_layout(layout: Layout, progress: Progress, stats: Dict, action_logs: Deque, total_items: int, thumbnail: Union[ThumbnailRenderer, Text]):
    stats_content = Table.grid(expand=True, padding=(0, 1)); stats_content.add_row("Przetworzone:", f"[cyan]{stats['processed']}/{total_items}[/]"); stats_content.add_row("Otagowane:", f"[green]{stats['tagged']}[/]"); stats_content.add_row("Pominięte:", f"[yellow]{stats['skipped']}[/]")
    settings_content = Table.grid(expand=True, padding=(0, 1)); settings_content.add_row("Model:", f"[dim]{MODEL_NAME}[/]"); settings_content.add_row("Próg:", f"[dim]{AI_TAGGER_CONFIDENCE_THRESHOLD:.0%}[/]"); settings_content.add_row("Rozmiar partii:", f"[dim]{AI_PROCESSING_BATCH_SIZE}[/]")
    footer_text = Text("Naciśnij CTRL+C, aby bezpiecznie zatrzymać...", justify="center", style="dim yellow")
    layout["header"].update(progress)
    layout["stats"].update(Panel(stats_content, title="[b blue]Statystyki[/]", border_style="blue"))
    layout["settings"].update(Panel(settings_content, title="[b blue]Ustawienia[/]", border_style="blue"))
    layout["logs"].update(Panel(Group(*action_logs), title="[b blue]Ostatnie Akcje[/]", border_style="blue"))
    layout["preview"].update(Panel(Align.center(thumbnail, vertical="middle"), title="[b blue]Podgląd[/]", border_style="blue"))
    layout["footer"].update(footer_text)
