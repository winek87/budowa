# plik: core/validator/tasks/existence.py
# Wersja 2.1 - Przystosowano do nowego silnika z dashboardem
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import List, Dict, Any

from rich.console import Console
from rich.table import Table

from ...database import get_downloaded_files_for_validation
from ..base_task import ValidatorTaskRunner

logger = logging.getLogger(__name__)
console = Console()

async def _check_single_file_existence(record: Dict[str, Any]) -> str | None:
    """
    Sprawdza istnienie pliku.
    Zwraca ścieżkę jako string, jeśli plik nie istnieje (aby go wyświetlić na liście "znalezionych").
    """
    file_path = Path(record['final_path'])
    if not file_path.exists():
        logger.warning(f"Brak pliku na dysku dla ID={record['id']}: {file_path}")
        return record['final_path']
    return None

def _display_existence_summary(missing_paths: List[str], total_checked: int):
    """Wyświetla podsumowanie weryfikacji istnienia plików."""
    if not missing_paths:
        logger.info("Weryfikacja istnienia plików zakończona pomyślnie.")
        console.print(f"\n[bold green]✅ Weryfikacja zakończona. Wszystkie {total_checked} pliki z bazy istnieją na dysku.[/bold green]")
    else:
        logger.error(f"Znaleziono {len(missing_paths)} brakujących plików ('duchów')!")
        console.print(f"\n[bold red]⚠️ Znaleziono {len(missing_paths)} brakujących plików:[/bold red]")

        table = Table(title="Lista Brakujących Plików ('Duchy')")
        table.add_column("Oczekiwana Ścieżka", style="red")
        for path in missing_paths:
            table.add_row(path)

        console.print(table)
        console.print("\n[yellow]Powyższe pliki zostały prawdopodobnie usunięte lub przeniesione ręcznie.[/yellow]")
        console.print("[dim]Użyj 'Edytora Bazy Danych', aby usunąć te martwe wpisy.[/dim]")

async def verify_file_existence():
    """Konfiguruje i uruchamia zadanie weryfikacji istnienia plików z dashboardem."""
    
    task = ValidatorTaskRunner(
        task_name="Weryfikator Istnienia Plików ('Duchy' w bazie)",
        data_fetch_func=get_downloaded_files_for_validation,
        record_process_func=_check_single_file_existence,
        summary_func=_display_existence_summary,
        style="yellow"
    )
    
    await task.run()
