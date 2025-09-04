# plik: core/path_fix_tool.py (Wersja z poprawioną logiką rozróżniania źródeł)
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
import asyncio

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm
from rich.progress import Progress

from .config import DOWNLOADS_DIR_BASE
# NOWE IMPORTY Z MODUŁU BAZY DANYCH
from .database import get_downloaded_entries_for_path_fixing, update_paths_for_entry_by_id

console = Console()
logger = logging.getLogger(__name__)

async def run_path_fixer():
    """
    Uruchamia narzędzie do masowej naprawy ścieżek w bazie danych dla plików,
    które zostały pobrane (a nie zaimportowane lokalnie).
    """
    console.clear()
    logger.info("Uruchamiam narzędzie do naprawy ścieżek w bazie danych.")
    console.print(Panel("🛠️ Narzędzie do Naprawy Ścieżek Plików 🛠️", expand=False, style="bold blue"))

    try:
        # Krok 1: Pobierz do analizy TYLKO pliki pobrane, ignorując lokalne.
        all_entries = await get_downloaded_entries_for_path_fixing()
        if not all_entries:
            console.print("\n[green]Nie znaleziono żadnych ścieżek (dla pobranych plików) do analizy w bazie danych.[/green]")
            return

        current_prefix = Path(DOWNLOADS_DIR_BASE).resolve()
        
        # Krok 2: Znajdź stary, nieaktualny prefiks w pobranych plikach
        old_prefix_str = None
        for entry in all_entries:
            path = Path(entry['final_path'])
            # Sprawdzamy, czy ścieżka w bazie jest zgodna z aktualną konfiguracją
            if not path.is_relative_to(current_prefix):
                # Znaleźliśmy ścieżkę, która nie pasuje. Obliczamy jej stary prefiks.
                num_parts_to_keep = len(path.parts) - len(current_prefix.parts)
                if num_parts_to_keep > 0:
                    old_prefix = Path(*path.parts[:num_parts_to_keep])
                    old_prefix_str = str(old_prefix)
                    break

        if not old_prefix_str:
            console.print(f"\n[bold green]✅ Wszystkie ścieżki w bazie ({len(all_entries)}) są aktualne i zgodne z prefiksem:[/]\n[cyan]{current_prefix}[/]")
            return

        # Krok 3: Poproś o potwierdzenie
        console.print(Panel(
            f"Narzędzie zamierza zastąpić nieaktualny prefiks ścieżki:\n\n"
            f"[red]'{old_prefix_str}'[/]\n\n"
            f"na aktualny prefiks z Twojej konfiguracji:\n\n"
            f"[green]'{current_prefix}'[/]\n\n"
            f"Operacja zostanie wykonana dla kolumn `final_path` oraz `expected_path`.",
            title="[bold yellow]Potwierdzenie Operacji[/]",
            border_style="yellow"
        ))

        if not Confirm.ask("\n[bold]Czy na pewno chcesz kontynuować?[/]", default=True):
            logger.warning("Naprawa ścieżek anulowana przez użytkownika.")
            return

        # Krok 4: Wykonaj naprawę
        updated_count = 0
        with Progress() as progress:
            task = progress.add_task("[green]Aktualizuję ścieżki...", total=len(all_entries))
            for entry in all_entries:
                old_final_path_str = entry['final_path']
                if old_final_path_str.startswith(old_prefix_str):
                    new_final_path = old_final_path_str.replace(old_prefix_str, str(current_prefix), 1)
                    
                    old_expected_path_str = entry['expected_path']
                    new_expected_path = old_expected_path_str.replace(old_prefix_str, str(current_prefix), 1)

                    await update_paths_for_entry_by_id(entry['id'], new_final_path, new_expected_path)
                    updated_count += 1
                progress.update(task, advance=1)

        console.print(f"\n[bold green]✅ Zakończono! Zaktualizowano {updated_count} wpisów w bazie danych.[/bold green]")

    except Exception as e:
        logger.critical(f"Wystąpił krytyczny błąd podczas naprawy ścieżek: {e}", exc_info=True)
        console.print(f"\n[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/bold red]")
