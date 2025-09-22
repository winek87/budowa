# plik: core/downloader/ui/panels.py
# Wersja 1.0 - Komponenty UI dla modułu Downloader.
# Opis: Ten moduł zawiera funkcje generujące specyficzne
#       komponenty `rich`, takie jak panele podsumowania.
# -*- coding: utf-8 -*-

from pathlib import Path
from typing import Dict

from rich.console import Group
from rich.panel import Panel
from rich.table import Table

# Importujemy z głównego modułu utils, bo to uniwersalna funkcja
from ...utils import format_size_for_display
from ...config import DOWNLOADS_DIR_BASE

def create_summary_panel(url: str, status: str, metadata: Dict) -> Panel:
    """Tworzy estetyczny panel Rich podsumowujący wynik operacji dla jednego pliku."""
    status_map = {
        "downloaded": ("[bold green]✅ Pobrany Plik[/]", "green"),
        "skipped": ("[bold yellow]🟡 Pominięty (już w bazie)[/]", "yellow"),
        "failed": ("[bold red]❌ Błąd Przetwarzania[/]", "red")
    }
    title, border_style = status_map.get(status, (f"[bold]ℹ️ Status: {status}[/]", "default"))
    filename = Path(metadata.get('final_path') or metadata.get('FileName') or "Brak nazwy").name
    
    file_info_table = Table(box=None, show_header=False, padding=0)
    file_info_table.add_column(style="dim", width=15); file_info_table.add_column(style="cyan")
    file_info_table.add_row("Plik:", filename)
    if final_path := metadata.get('final_path'):
        try:
            display_path = Path(final_path).relative_to(DOWNLOADS_DIR_BASE)
        except ValueError:
            display_path = final_path
        file_info_table.add_row("Zapisano w:", f"{DOWNLOADS_DIR_BASE}/{display_path}")
    if dt := metadata.get('DateTime'):
        file_info_table.add_row("Data:", dt)
    if size := metadata.get('size'):
        file_info_table.add_row("Rozmiar:", format_size_for_display(size))
    if dims := metadata.get('Dimensions'):
        file_info_table.add_row("Wymiary:", dims)
    if camera := metadata.get('Camera'):
        file_info_table.add_row("Aparat:", camera)
    
    content_group_items = [file_info_table]
    has_online_meta = any(k in metadata for k in ['Description', 'TaggedPeople', 'Albums', 'Location'])
    if has_online_meta:
        content_group_items.append("\n[dim]-- Metadane ze strony --[/dim]")
        online_metadata_table = Table(box=None, show_header=False, padding=0)
        online_metadata_table.add_column(style="dim", width=15); online_metadata_table.add_column(style="cyan")
        if d := metadata.get('Description'):
            display_desc = (d[:40] + '...') if len(d) > 43 else d
            online_metadata_table.add_row("Opis:", display_desc.replace('\n', ' '))
        if p := metadata.get('TaggedPeople'):
            online_metadata_table.add_row("Osoby:", ", ".join(p))
        if a := metadata.get('Albums'):
            online_metadata_table.add_row("Albumy:", ", ".join(a))
        if l := metadata.get('Location'):
            online_metadata_table.add_row("Lokalizacja:", l)
        content_group_items.append(online_metadata_table)
        
    short_url = url[:45] + "..." + url[-15:] if len(url) > 60 else url
    return Panel(
        Group(*content_group_items),
        title=title,
        subtitle=f"[dim link={url}]{short_url}[/dim link]",
        border_style=border_style,
        subtitle_align="right"
    )
