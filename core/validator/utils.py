# plik: core/validator/utils.py
# Wersja 1.0 - Dedykowane narzędzia UI dla modułu Walidatora
# -*- coding: utf-8 -*-

import asyncio
import math
import sys
from datetime import datetime
from pathlib import Path

# === NOWE IMPORTY DLA NIEZALEŻNOŚCI ===
try:
    import termios
    import tty
    import select
    IS_POSIX = True
except ImportError:
    IS_POSIX = False
    import msvcrt # Dla Windows

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()

def get_key_blocking() -> str | None:
    """
    BLOKUJĄCA funkcja do odczytu klawisza, potrzebna dla interaktywnych menu.
    Skopiowana z głównego utils, aby ten moduł był bardziej samowystarczalny.
    """
    if not IS_POSIX:
        try:
            char = msvcrt.getch()
            if char == b'\xe0': # Strzałki
                char = msvcrt.getch()
                if char == b'H': return "UP"
                if char == b'P': return "DOWN"
            elif char in (b'\r', b'\n'): return "ENTER"
            return char.decode().upper()
        except Exception: return None

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        char = sys.stdin.read(1)
        if char == '\x1b':
            seq = sys.stdin.read(2)
            if seq == '[A': return "UP"
            if seq == '[B': return "DOWN"
        elif char in ('\r', '\n'): return "ENTER"
        return char.upper()
    except Exception: return None
    finally: termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

def format_size_for_display(size_bytes: int) -> str:
    """Formatuje rozmiar pliku w bajtach do czytelnej formy (KB, MB, GB)."""
    if size_bytes is None or not isinstance(size_bytes, (int, float)): return "B/D"
    if size_bytes == 0: return "0.0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024))) if size_bytes > 0 else 0
    p = math.pow(1024, i)
    s = round(size_bytes / p, 1)
    return f"{s:.1f} {size_name[i]}"

async def interactive_file_selector(items: list, title: str) -> list:
    """
    Ulepszony, interaktywny selektor plików z dodatkowymi informacjami.
    """
    if not items: return []
    selected_indices = set(); current_index = 0
    file_details_cache = {}
    
    def get_file_details(path: Path, index: int) -> str:
        if index in file_details_cache: return file_details_cache[index]
        try:
            stat = path.stat()
            size = format_size_for_display(stat.st_size)
            mtime = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M')
            details = f"[dim]({size}, {mtime})[/dim]"
            file_details_cache[index] = details
            return details
        except Exception: return ""

    def generate_layout() -> Layout:
        table = Table(box=None, show_header=False, padding=(0, 1), expand=True)
        table.add_column(width=4); table.add_column("Filename", ratio=1); table.add_column("Details", width=30, justify="right")
        terminal_height = console.height
        visible_items_count = max(5, terminal_height - 10)
        start_index = max(0, min(current_index - visible_items_count // 2, len(items) - visible_items_count))
        end_index = min(len(items), start_index + visible_items_count)
        for i in range(start_index, end_index):
            item = items[i]; display_name = item.name if isinstance(item, Path) else str(item)
            cursor = "»" if i == current_index else " "; checkbox = "[bold green]✓[/]" if i in selected_indices else "[dim]o[/]"
            style = "bold white on blue" if i == current_index else ("green" if i in selected_indices else "")
            details_str = get_file_details(item, i) if isinstance(item, Path) else ""
            table.add_row(f"{cursor} {checkbox}", Text(display_name, style=style), details_str)
        footer_text = Text.from_markup("Nawigacja: [on bright_black] ▲ ▼ [/] | Zaznacz: [on bright_black] Spacja [/] | Zaznacz/Odznacz: [on bright_black] A/N [/] | Zatwierdź: [on bright_black] Enter [/] | Anuluj: [on bright_black] Q [/]", justify="center")
        layout = Layout(); layout.split_column(Layout(Panel(table, title=f"{title} (Zaznaczono: {len(selected_indices)} z {len(items)})", border_style="cyan"), name="main"), Layout(footer_text, name="footer", size=1)); return layout

    with Live(generate_layout(), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_layout(), refresh=True)
            key = await asyncio.to_thread(get_key_blocking)
            if not key: continue
            if key == "UP": current_index = (current_index - 1 + len(items)) % len(items)
            elif key == "DOWN": current_index = (current_index + 1) % len(items)
            elif key == ' ':
                if current_index in selected_indices: selected_indices.remove(current_index)
                else: selected_indices.add(current_index)
            elif key == 'A': selected_indices = set(range(len(items)))
            elif key == 'N': selected_indices.clear()
            elif key in ['Q', '\x1b']: return []
            elif key == "ENTER": return [items[i] for i in sorted(list(selected_indices))]

async def create_validator_menu(
    menu_items: list,
    title: str,
    subtitle: str = "",
    border_style: str = "yellow"
) -> any:
    """
    Ulepszona wersja menu, dedykowana dla modułu Walidatora,
    która renderuje profesjonalny, wyśrodkowany panel.
    """
    try:
        selected_index = next(i for i, item in enumerate(menu_items) if item[1] is not None)
    except StopIteration:
        selected_index = 0

    def generate_panel(sel_idx: int) -> Panel:
        menu_text = Text(justify="center")
        for i, (text, action) in enumerate(menu_items):
            if action is None:
                menu_text.append(f"\n[dim bold]{text}[/dim bold]\n\n")
                continue
            
            style = "bold white on yellow" if i == sel_idx else ""
            prefix = "» " if i == sel_idx else "  "
            
            menu_text.append(Text.from_markup(f"{prefix}{text}\n", style=style))

        final_content = Group(
            Align.center(menu_text, vertical="middle"),
            (Align.center(Text.from_markup(f"\n{subtitle}", style="dim")) if subtitle else "")
        )
        
        return Panel(
            final_content,
            title=f"[bold]{title}[/bold]",
            border_style=border_style,
            padding=(1, 2)
        )

    with Live(generate_panel(selected_index), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_panel(selected_index), refresh=True)
            # Używamy lokalnej, blokującej funkcji get_key
            key = await asyncio.to_thread(get_key_blocking)
            if not key: continue

            if key == "UP":
                original_index = selected_index
                while True:
                    selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                    if menu_items[selected_index][1] is not None: break
                    if selected_index == original_index: break
            elif key == "DOWN":
                original_index = selected_index
                while True:
                    selected_index = (selected_index + 1) % len(menu_items)
                    if menu_items[selected_index][1] is not None: break
                    if selected_index == original_index: break
            elif key.upper() in ["Q", "ESC"]:
                return "cancel" # Zwracamy spójną wartość
            elif key == "ENTER":
                _, selected_value = menu_items[selected_index]
                return selected_value
