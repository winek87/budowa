# plik: core/ai_tagger/workflows/interactive.py (WERSJA Z POPRAWIONĄ DEFINICJĄ FUNKCJI)
# -*- coding: utf-8 -*-

import asyncio
import logging
import json
from pathlib import Path
import time

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.live import Live

from ..model_handler import AITagger
from ..ui_components import interactive_verification_ui_live
from ...config import DB_BATCH_UPDATE_SIZE
from ...database import setup_database, get_images_to_tag, update_ai_tags_batch, clear_all_ai_tags

console = Console()
logger = logging.getLogger("app")

# === ZMIANA: Dodajemy 'model_path' do definicji funkcji ===
async def run_interactive_tagging(rescan: bool = False, model_path: str = None):
    """Orkiestruje proces tagowania AI w trybie interaktywnym, obraz po obrazie."""
    if not model_path:
        logger.error("Nie podano ścieżki modelu do funkcji tagującej.")
        return

    if rescan:
        cleared_count = await clear_all_ai_tags()
        console.print(f"\n[green]Usunięto stare tagi dla {cleared_count} obrazów.[/green]")

    console.clear()
    console.print(Panel("🤖 Tagowanie Obrazów (Interaktywne) 🤖", style="b blue"))

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
    updates = []
    path_map = {Path(r['final_path']): r['id'] for r in images}
    user_aborted = False

    prog = Progress(TextColumn("[bold green]Postęp:[/bold green]"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", TextColumn("{task.completed}/{task.total}"), TimeRemainingColumn())
    task = prog.add_task("Weryfikacja...", total=len(images))
    progress_panel = Panel(prog, title="Postęp ogólny", border_style="green", padding=(1, 2))

    with Live(progress_panel, console=console, screen=False, transient=True, auto_refresh=False) as live:
        for record in images:
            if user_aborted: break
            
            path = Path(record['final_path'])
            if not await asyncio.to_thread(path.exists):
                stats['skipped'] += 1; stats['processed'] += 1
                prog.update(task, advance=1); live.refresh()
                continue
            
            live.stop(); console.clear()
            with console.status(f"Przetwarzanie [cyan]{path.name}[/cyan]..."):
                results = await tagger.tag_images_batch([path])
            
            stats['processed'] += 1
            tags_to_verify = results.get(path, [])
            final_tags = await interactive_verification_ui_live(path, tags_to_verify)

            if final_tags is None:
                user_aborted = True; break
            
            rec_id = path_map.get(path)
            if final_tags:
                stats['tagged'] += 1
                updates.append((json.dumps(final_tags), rec_id))
            else:
                stats['skipped'] += 1
            
            prog.update(task, advance=1); live.start(); live.refresh()
            if len(updates) >= DB_BATCH_UPDATE_SIZE:
                await update_ai_tags_batch(updates); updates.clear()

    if updates: await update_ai_tags_batch(updates)
    console.print(f"Przetworzone: {stats['processed']}, Otagowane: {stats['tagged']}, Pominięte: {stats['skipped']}")
    if user_aborted:
        console.print("\n[bold yellow]⏹️ Proces tagowania został przerwany przez użytkownika.[/bold yellow]")
    else:
        console.print("\n[b green]✅ Proces tagowania AI zakończony![/b green]")
