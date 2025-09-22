# plik: core/path_fixer/cli.py
# Wersja 1.1 - Zintegrowano z dedykowanym LiveDisplay.
# Opis: Moduł zarządzający interfejsem użytkownika i przepływem pracy
#       dla Narzędzia do Naprawy Ścieżek.
# -*- coding: utf-8 -*-

import logging
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm

# Importy z modułów projektu `core`
from ..config import DOWNLOADS_DIR_BASE
from ..database import get_downloaded_entries_for_path_fixing

# Importy z wewnętrznych modułów pakietu
from .tasks import find_mismatched_path_prefix, perform_path_update
from .ui.live_display import PathFixerLiveDisplay

# Inicjalizacja
console = Console()
logger = logging.getLogger(__name__)


async def run_path_fixer():
    """
    Uruchamia narzędzie do masowej naprawy ścieżek w bazie danych.

    Przepływ pracy:
    1.  Wyświetla ekran powitalny.
    2.  Pobiera z bazy danych wszystkie wpisy dla plików pobranych.
    3.  Analizuje ścieżki w poszukiwaniu niezgodności z aktualną konfiguracją.
    4.  Jeśli znajdzie problem, prezentuje go użytkownikowi i prosi o potwierdzenie.
    5.  Uruchamia proces aktualizacji z interaktywnym dashboardem postępu.
    6.  Wyświetla finalne podsumowanie.
    """
    console.clear()
    logger.info("Uruchamiam narzędzie do naprawy ścieżek w bazie danych.")
    console.print(Panel("🛠️ [bold]Narzędzie do Naprawy Ścieżek Plików[/] 🛠️", expand=False, style="blue"))
    console.print(
        "\nTo narzędzie analizuje ścieżki [bold]pobranych plików[/] w bazie danych. "
        "Jeśli przeniosłeś folder projektu w inne miejsce, pomoże Ci to zaktualizować "
        "wszystkie wpisy za jednym razem."
    )

    try:
        with console.status("[cyan]Analizowanie bazy danych...[/]"):
            all_entries = await get_downloaded_entries_for_path_fixing()
        
        if not all_entries:
            console.print("\n[green]Nie znaleziono żadnych pobranych plików do analizy w bazie danych.[/green]")
            return

        current_prefix = Path(DOWNLOADS_DIR_BASE).resolve()
        
        with console.status("[cyan]Wyszukiwanie nieaktualnych prefiksów...[/]"):
            old_prefix_str = await find_mismatched_path_prefix(all_entries, current_prefix)

        if not old_prefix_str:
            console.print(
                f"\n[bold green]✅ Wszystkie ścieżki ({len(all_entries)}) są aktualne i zgodne z prefiksem:[/]\n"
                f"[dim cyan]{current_prefix}[/]"
            )
            return

        # Prezentacja problemu i prośba o potwierdzenie
        console.print(Panel(
            f"Wykryto niezgodność ścieżek! Narzędzie proponuje następującą zmianę:\n\n"
            f"Stary, wykryty prefiks:\n[red]'{old_prefix_str}'[/]\n\n"
            f"Nowy, aktualny prefiks z konfiguracji:\n[green]'{current_prefix}'[/]\n\n"
            f"[dim]Operacja zostanie wykonana dla kolumn `final_path` i `expected_path` "
            f"we wszystkich pasujących wpisach.[/dim]",
            title="[bold yellow]Potwierdzenie Operacji[/]",
            border_style="yellow"
        ))

        if not Confirm.ask("\n[bold]Czy na pewno chcesz kontynuować?[/]", default=True):
            logger.warning("Naprawa ścieżek anulowana przez użytkownika.")
            return

        # Uruchomienie procesu aktualizacji z dedykowanym dashboardem
        updated_count = 0
        with PathFixerLiveDisplay(total_items=len(all_entries), console=console) as display:
            updated_count = await perform_path_update(all_entries, old_prefix_str, str(current_prefix), display)

        console.print(f"\n[bold green]✅ Zakończono! Zaktualizowano {updated_count} wpisów w bazie danych.[/bold green]")

    except Exception as e:
        logger.critical(f"Wystąpił krytyczny błąd podczas naprawy ścieżek: {e}", exc_info=True)
        console.print(f"\n[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/bold red]")
