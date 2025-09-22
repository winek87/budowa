# plik: core/validator/tasks/duplicates.py
# Wersja 1.0 - Zrefaktoryzowana logika menedżera duplikatów
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import List, Dict

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm
from rich.progress import Progress
from rich.text import Text
from rich.align import Align
from rich.layout import Layout
from rich.live import Live

from ...database import (get_duplicate_hashes, get_entries_by_hash,
                           delete_entries_by_ids)
from ...utils import get_key, create_side_by_side_comparison_panel, _parse_metadata_for_display

logger = logging.getLogger(__name__)
console = Console()

async def _resolve_single_duplicate_set(duplicate_set: List[Dict]) -> Dict:
    """
    Wyświetla interfejs do rozwiązania pojedynczego zestawu duplikatów.
    """
    selected_to_keep_index = 0
    # Prosta heurystyka: domyślnie zaznacz większy plik do zachowania
    try:
        size_a_str = duplicate_set[0].get('size', '0 MB').split(' ')[0].replace(',', '.')
        size_b_str = duplicate_set[1].get('size', '0 MB').split(' ')[0].replace(',', '.')
        if float(size_b_str) > float(size_a_str):
            selected_to_keep_index = 1
    except (ValueError, IndexError):
        pass

    def generate_layout() -> Layout:
        item_a = duplicate_set[0]
        item_b = duplicate_set[1]

        # Budujemy słowniki z detalami dla naszego komponentu UI
        item_a_details = {"ID": item_a.get('id'), "Ścieżka": item_a.get('relative_path'), "separator_1": "", "Data": item_a.get('date'), "Rozmiar": item_a.get('size'), "Wymiary": item_a.get('dimensions'), "separator_2": "", "Aparat": item_a.get('camera')}
        item_b_details = {"ID": item_b.get('id'), "Ścieżka": item_b.get('relative_path'), "separator_1": "", "Data": item_b.get('date'), "Rozmiar": item_b.get('size'), "Wymiary": item_b.get('dimensions'), "separator_2": "", "Aparat": item_b.get('camera')}

        comparison_panel = create_side_by_side_comparison_panel(
            item_a_details, item_b_details, is_a_selected=(selected_to_keep_index == 0)
        )
        title = Text(f"Wybierz plik do ZACHOWANIA\n[dim]Hash: {item_a.get('hash', 'Brak')}[/dim]", justify="center")
        footer = Align.center(Text.from_markup("[bold]← →[/](wybierz) | [bold]Enter[/](zatwierdź) | [bold]S[/](pomiń) | [bold]Q[/](zakończ)"))
        
        layout = Layout()
        layout.split_column(Layout(title, size=3), comparison_panel, Layout(footer, size=1))
        return layout

    with Live(generate_layout(), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_layout(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue

            if key.upper() in ["Q"]: return {"action": "quit"}
            if key.upper() == "S": return {"action": "skip"}
            if key in ["LEFT", "RIGHT"]: selected_to_keep_index = 1 - selected_to_keep_index
            if key == "ENTER":
                to_keep = duplicate_set[selected_to_keep_index]
                to_delete = duplicate_set[1 - selected_to_keep_index]
                return {"action": "resolve", "keep": to_keep, "delete": to_delete}

async def find_duplicates_by_hash():
    """Znajduje i pozwala zarządzać duplikatami plików na podstawie hasha MD5."""
    console.clear()
    console.print(Panel("🧩 Menedżer Duplikatów Plików (wg zawartości) 🧩", expand=False, style="bold yellow"))

    with console.status("[cyan]Wyszukiwanie duplikatów w bazie danych...[/]"):
        duplicate_hashes = await get_duplicate_hashes()

    if not duplicate_hashes:
        console.print("\n[bold green]✅ Nie znaleziono żadnych duplikatów.[/bold green]")
        return

    logger.warning(f"Znaleziono {len(duplicate_hashes)} zestawy duplikatów.")
    all_files_to_delete = []

    for i, hash_val in enumerate(duplicate_hashes):
        console.clear()
        console.print(Panel(f"[bold yellow]Zestaw duplikatów {i + 1}/{len(duplicate_hashes)}[/]", expand=False))

        files_in_set_raw = await get_entries_by_hash(hash_val)
        files_in_set = []
        for file_row in files_in_set_raw:
            metadata = json.loads(file_row['metadata_json'] or '{}')
            file_path = Path(file_row['final_path'])
            display_info = _parse_metadata_for_display(metadata, file_path)
            # Dodajemy ścieżkę względną dla lepszego wyświetlania
            try:
                relative_path = file_path.relative_to(Path.cwd())
            except ValueError:
                relative_path = file_path
            files_in_set.append({"id": file_row['id'], "path": file_path, "hash": hash_val, "relative_path": str(relative_path), **display_info})

        if len(files_in_set) > 1:
            resolution = await _resolve_single_duplicate_set(files_in_set)
            if resolution.get("action") == "quit": break
            if resolution.get("action") == "resolve":
                all_files_to_delete.append(resolution['delete'])

    if all_files_to_delete:
        console.clear()
        console.print(Panel("[bold red]Podsumowanie Akcji Usunięcia[/]", expand=False))
        console.print(f"Wybrano [cyan]{len(all_files_to_delete)}[/cyan] plików do usunięcia.")

        if Confirm.ask("\n[bold red]Czy na pewno chcesz TRWALE usunąć te pliki z dysku i bazy danych?[/]", default=False):
            ids_to_delete = [f['id'] for f in all_files_to_delete]

            with Progress(console=console, transient=True) as progress:
                task = progress.add_task("[red]Usuwam pliki...", total=len(all_files_to_delete))
                for file_info in all_files_to_delete:
                    try:
                        if await asyncio.to_thread(file_info['path'].exists):
                            await asyncio.to_thread(os.remove, file_info['path'])
                    except OSError as e:
                        logger.error(f"Nie udało się usunąć pliku {file_info['path']}: {e}")
                    finally:
                        progress.update(task, advance=1)
            
            await delete_entries_by_ids(ids_to_delete)
            console.print(f"\n[bold green]✅ Usunięto {len(ids_to_delete)} duplikatów.[/bold green]")
