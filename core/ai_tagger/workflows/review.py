# plik: core/ai_tagger/workflows/review.py (WERSJA FINALNA Z NIEZAWODNYM ROZMIAREM)
# -*- coding: utf-8 -*-

import asyncio
import json
from pathlib import Path
import time

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt
from rich.text import Text

from ...utils import get_key, ThumbnailRenderer
from ...database import get_tagged_images, update_ai_tags_for_entry

console = Console()

class Reviewer:
    """Klasa zarządzająca stanem i renderowaniem przeglądarki wewnątrz Live."""
    def __init__(self, tagged_images: list):
        self.images = tagged_images
        self.index = 0
        self.should_exit = False
        self._cached_thumb = None
        self._cached_thumb_path = None

    def get_current_record(self):
        return self.images[self.index]

    def _render_thumbnail(self, path: Path) -> ThumbnailRenderer:
        """Renderuje miniaturkę, używając cache'u, aby uniknąć migotania."""
        if path != self._cached_thumb_path:
            # === KLUCZOWA ZMIANA: Używamy stałego, bezpiecznego rozmiaru ===
            # Zamiast skomplikowanych obliczeń, używamy stałego, rozsądnego
            # rozmiaru, który na pewno zmieści się w layoucie.
            self._cached_thumb = ThumbnailRenderer(path, max_size=(60, 25))
            self._cached_thumb_path = path
        return self._cached_thumb

    def __rich__(self) -> Layout:
        """Renderuje kompletny, responsywny layout."""
        record = self.get_current_record()
        image_path = Path(record['final_path'])
        
        root = Layout(name="root")
        root.split(
            Layout(name="main", ratio=1),
            Layout(name="footer", size=3)
        )
        root["main"].split_row(Layout(name="preview", ratio=2), Layout(name="info"))

        # Panel z informacjami i tagami
        info_text = Text(f"Plik: {image_path.name}\nŚcieżka: {str(image_path.parent)}\n\n")
        tags_text = Text("Tagi: ", style="default")
        try:
            tags_data = json.loads(record['ai_tags'] or '[]')
            if tags_data and isinstance(tags_data[0], dict):
                tags_str = ", ".join(f"{t['label']} [dim]({t.get('score', 1.0):.0%})[/dim]" for t in tags_data)
            else: tags_str = ", ".join(map(str, tags_data))
            tags_text.append(Text.from_markup(tags_str if tags_str else "[dim]Brak tagów[/dim]"))
        except (json.JSONDecodeError, TypeError, IndexError):
            tags_text.append("Błąd odczytu tagów", style="red")
        info_text.append(tags_text)
        
        # Wypełnienie layoutu
        root["preview"].update(Panel(Align.center(self._render_thumbnail(image_path), vertical="middle"), title="Podgląd"))
        root["info"].update(Panel(info_text, title=f"Informacje ({self.index + 1}/{len(self.images)})"))
        root["footer"].update(Panel("[bold]Nawigacja:[/bold] [cyan]Strzałki L/P[/cyan] (następny/poprzedni) | [cyan]E[/cyan] (edytuj) | [cyan]W[/cyan] (wróć)"))

        return root
        
async def review_tagged_images():
    """Uruchamia interaktywną przeglądarkę w stylu Live."""
    console.clear()
    console.print(Panel("🖼️  Przeglądarka i Edytor Otagowanych Zdjęć 🖼️", expand=False, style="b green"))
    
    with console.status("[c]Pobieranie danych...[/]"):
        tagged_images = await get_tagged_images()
        
    if not tagged_images:
        console.print("\n[yellow]Nie znaleziono otagowanych obrazów.[/yellow]"); Prompt.ask("\n[b]Naciśnij Enter...[/]"); return

    reviewer = Reviewer(tagged_images)

    with Live(reviewer, screen=True, auto_refresh=False, transient=True) as live:
        while not reviewer.should_exit:
            live.update(reviewer, refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue

            if key == "RIGHT": reviewer.index = min(reviewer.index + 1, len(reviewer.images) - 1)
            elif key == "LEFT": reviewer.index = max(reviewer.index - 1, 0)
            elif key.lower() == 'w': reviewer.should_exit = True
            elif key.lower() == 'e':
                live.stop()
                
                record = reviewer.get_current_record()
                try:
                    tags_copy = json.loads(record['ai_tags'] or '[]')
                    if tags_copy and not isinstance(tags_copy[0], dict):
                        tags_copy = [{'label': str(t), 'score': 1.0} for t in tags_copy]
                except json.JSONDecodeError: tags_copy = []
                
                while True:
                    console.clear()
                    console.print(Panel(f"Edycja tagów dla: [cyan]{Path(record['final_path']).name}[/cyan]"))
                    if tags_copy:
                        for i, tag in enumerate(tags_copy): console.print(f" [cyan b]{i+1}[/]: {tag['label']}")
                    else:
                        console.print("[yellow]Brak tagów.[/yellow]")

                    console.rule()
                    edit_choice = Prompt.ask("Akcja: [b](D)[/b]odaj, [b](U)[/b]suń, [b](Z)[/b]apisz i wróć", choices=["d", "u", "z"], default="z").lower()
                    
                    if edit_choice == 'd':
                        new_tag = Prompt.ask("[b]Nowy tag[/b]").strip().lower()
                        if new_tag and not any(t['label'] == new_tag for t in tags_copy):
                            tags_copy.append({'label': new_tag, 'score': 1.0, 'source': 'manual'})
                    elif edit_choice == 'u' and tags_copy:
                        num = IntPrompt.ask("[b]Numer do usunięcia[/b]", choices=[str(i+1) for i in range(len(tags_copy))])
                        del tags_copy[num-1]
                    elif edit_choice == 'z':
                        if await update_ai_tags_for_entry(record['id'], tags_copy):
                            record['ai_tags'] = json.dumps(tags_copy)
                            console.print("\n[b green]✅ Zapisano.[/b green]")
                        else:
                            console.print("\n[b red]❌ Błąd zapisu.[/b red]")
                        time.sleep(1); break
                
                live.start()
