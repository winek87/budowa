# plik: core/ai_tagger/workflows/writer.py
# -*- coding: utf-8 -*-

import json
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm
from rich.progress import Progress

from ...utils import run_exiftool_command
from ...database import get_records_for_ai_exif_writing

console = Console()

async def write_tags_to_exif():
    """Zapisuje tagi AI z bazy danych do metadanych EXIF/XMP plików graficznych."""
    console.clear()
    console.print(Panel("✍️ Zapisywanie Tagów AI do Plików (EXIF/XMP) ✍️", expand=False, style="b yellow"))
    
    with console.status("[c]Wyszukiwanie obrazów do aktualizacji...[/]"):
        records = await get_records_for_ai_exif_writing()
        
    if not records:
        console.print("\n[green]✅ Wszystkie tagi AI są już zapisane w plikach.[/green]")
        return
        
    console.print(f"Znaleziono [cyan]{len(records)}[/cyan] obrazów z tagami do zapisania w metadanych.")
    if not Confirm.ask("Czy chcesz kontynuować?", default=True):
        return
        
    success_count, fail_count = 0, 0
    with Progress(transient=True) as progress:
        task = progress.add_task("[c]Zapisywanie...", total=len(records))
        for r in records:
            path = Path(r['final_path'])
            try:
                tags_data = json.loads(r['ai_tags'])
                if not path.exists() or not tags_data:
                    fail_count += 1
                    continue
                
                tags = [i['label'] for i in tags_data if isinstance(i, dict)] if isinstance(tags_data[0], dict) else tags_data
                params = ["-overwrite_original"] + [f"-Subject+={t}" for t in tags] + [str(path)]
                ok, out = await run_exiftool_command(params)
                
                if ok:
                    console.print(f"✅ [g]Zapisano dla:[/g] {path.name}")
                    success_count += 1
                else:
                    console.print(f"❌ [r]Błąd dla:[/r] {path.name} -> {out}")
                    fail_count += 1
            except (json.JSONDecodeError, TypeError, IndexError):
                fail_count += 1
            finally:
                progress.update(task, advance=1)
                
    console.print(f"\n[b]Podsumowanie:[/b]\n[g]Zapisano pomyślnie: {success_count}[/g]\n[r]Błędy lub pominięte: {fail_count}[/r]")
