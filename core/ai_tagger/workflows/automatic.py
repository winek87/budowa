# plik: core/ai_tagger/workflows/automatic.py (WERSJA Z POPRAWIONĄ DEFINICJĄ FUNKCJI)
# -*- coding: utf-8 -*-

import asyncio
import logging
import json
from pathlib import Path
from collections import deque
import time

from rich.console import Console, Group
from rich.panel import Panel
from rich.prompt import Confirm
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.live import Live
from rich.text import Text
from rich.layout import Layout
from rich.table import Table

from ..model_handler import AITagger
from ..ui_components import create_dashboard_layout, update_dashboard_layout
from ...config import AI_PROCESSING_BATCH_SIZE, DB_BATCH_UPDATE_SIZE, LOG_DEQUE_SIZE, THUMBNAIL_MAX_SIZE
from ...utils import ThumbnailRenderer
from ...database import setup_database, get_images_to_tag, update_ai_tags_batch, clear_all_ai_tags

console = Console()
logger = logging.getLogger("app")

# === ZMIANA: Dodajemy 'model_path' do definicji funkcji ===
async def run_automatic_tagging(rescan: bool = False, model_path: str = None):
    """Orkiestruje w pełni automatyczny proces tagowania AI z adaptacyjnym dashboardem."""
    if not model_path:
        logger.error("Nie podano ścieżki modelu do funkcji tagującej.")
        return

    if rescan:
        cleared_count = await clear_all_ai_tags()
        console.print(f"\n[green]Usunięto stare tagi dla {cleared_count} obrazów.[/green]")

    console.clear()
    console.print(Panel("🤖 Tagowanie Obrazów (Automatyczne) 🤖", style="b blue"))

    # Przekazujemy ścieżkę modelu do AITagger
    tagger = AITagger(model_path=model_path)
    await setup_database()
    with console.status("[c]Pobieranie listy obrazów...[/]"):
        images = await get_images_to_tag()
    if not images:
        console.print("\n[b green]✅ Wszystkie obrazy są już otagowane.[/b green]"); return
    if not Confirm.ask(f"\nZnaleziono [b cyan]{len(images)}[/] obrazów. Rozpocząć proces?", default=True): return

    try:
        await tagger.load_model()
    except RuntimeError as e:
        console.print(f"[b red]Błąd ładowania modelu: {e}[/]"); return

    stats = {"processed": 0, "tagged": 0, "skipped": 0}
    logs, updates = deque(maxlen=LOG_DEQUE_SIZE), []
    path_map = {Path(r['final_path']): r['id'] for r in images}
    
    prog = Progress(TextColumn("[bold green]Postęp:[/bold green]"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", TimeRemainingColumn())
    task = prog.add_task("Tagowanie...", total=len(images))
    dashboard_layout = create_dashboard_layout()
    current_thumbnail = Text("Oczekiwanie...", justify="center")

    with Live(dashboard_layout, screen=True, transient=True, auto_refresh=False) as live:
        try:
            batch = []
            for i, record in enumerate(images):
                path = Path(record['final_path'])
                if not await asyncio.to_thread(path.exists):
                    stats['skipped'] += 1; stats['processed'] += 1
                    logs.append(Text.from_markup(f"⚠️ Pominięto [yellow]{path.name}[/yellow] (plik nie istnieje)"))
                    prog.update(task, advance=1)
                else: batch.append(path)
                
                is_last_item = (i == len(images) - 1)
                if len(batch) >= AI_PROCESSING_BATCH_SIZE or (is_last_item and batch):
                    if batch:
                        try: current_thumbnail = ThumbnailRenderer(batch[0], max_size=THUMBNAIL_MAX_SIZE)
                        except Exception as e: current_thumbnail = Text(f"Błąd podglądu\n{e}", style="red", justify="center")
                        results = await tagger.tag_images_batch(batch)
                        for p in batch:
                            stats['processed'] += 1
                            rec_id = path_map.get(p)
                            if p in results:
                                stats['tagged'] += 1; tags = results[p]
                                updates.append((json.dumps(tags), rec_id))
                                top_tags_str = ", ".join(t['label'] for t in tags[:3])
                                logs.append(Group(Text.from_markup(f"✅ Otagowano [b cyan]{p.name}[/b cyan]"), Table(box=None, show_header=False, padding=0, show_edge=False).add_row("└─", Text.from_markup(f"[dim]Tagi: {top_tags_str}...[/dim]"))))
                            else:
                                stats['skipped'] += 1
                                logs.append(Text.from_markup(f"⚠️ Pominięto [yellow]{p.name}[/yellow] (brak tagów)"))
                        prog.update(task, advance=len(batch)); batch.clear()
                
                update_dashboard_layout(dashboard_layout, prog, stats, logs, len(images), current_thumbnail); live.refresh()
                if len(updates) >= DB_BATCH_UPDATE_SIZE:
                    await update_ai_tags_batch(updates); updates.clear()
        except KeyboardInterrupt:
            console.print("\n[bold yellow]Przerwanie przez użytkownika...[/bold yellow]"); time.sleep(1)

    if updates: await update_ai_tags_batch(updates)
    console.print(f"Przetworzone: {stats['processed']}, Otagowane: {stats['tagged']}, Pominięte: {stats['skipped']}")
    console.print("\n[b green]✅ Proces tagowania AI zakończony![/b green]")
