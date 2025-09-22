# plik: core/validator/tasks/metadata.py
# Wersja 1.0 - Zrefaktoryzowana logika analizy spójności metadanych
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from ...database import get_metadata_for_consistency_check
from ..base_task import ValidatorTaskRunner

logger = logging.getLogger(__name__)
console = Console()

async def _check_single_metadata_consistency(record: Dict[str, Any]) -> Dict | None:
    """
    Sprawdza spójność dla jednego rekordu.
    Zwraca słownik z informacjami o niespójności, jeśli zostanie znaleziona.
    """
    try:
        path = Path(record['final_path'])
        if len(path.parts) < 3: return None # Ścieżka zbyt krótka do analizy

        db_datetime = datetime.fromisoformat(record['dt_from_json'].replace('Z', '+00:00'))
        path_month = int(path.parent.name)
        path_year = int(path.parent.parent.name)

        if db_datetime.year != path_year or db_datetime.month != path_month:
            mismatch_info = {
                'path': str(path),
                'db_date': f"{db_datetime.year}-{db_datetime.month:02d}",
                'path_date': f"{path_year}-{path_month:02d}"
            }
            logger.warning(f"Znaleziono niespójność metadanych: {mismatch_info}")
            return mismatch_info
            
    except (ValueError, IndexError, TypeError):
        # Ignoruj błędy parsowania, które mogą wystąpić dla nietypowych plików
        return None
    return None

def _display_metadata_summary(mismatches: List[Dict], total_checked: int):
    """Wyświetla podsumowanie analizy spójności metadanych."""
    if not mismatches:
        logger.info("Analiza spójności metadanych zakończona pomyślnie.")
        console.print("\n[bold green]✅ Spójność metadanych idealna! Wszystkie daty w ścieżkach plików zgadzają się z metadanymi w bazie.[/bold green]")
    else:
        logger.error(f"Znaleziono {len(mismatches)} niespójności między ścieżką a datą w metadanych.")
        console.print(f"\n[bold red]⚠️ Znaleziono {len(mismatches)} plików z niespójną datą:[/bold red]")

        table = Table(title="Pliki z Niezgodną Datą w Ścieżce vs. Metadanych")
        table.add_column("Ścieżka Pliku", style="cyan")
        table.add_column("Data ze Ścieżki", style="yellow", justify="center")
        table.add_column("Data z Metadanych", style="red", justify="center")
        for item in mismatches:
            table.add_row(item['path'], item['path_date'], item['db_date'])

        console.print(table)
        console.print("\n[yellow]Powyższe niespójności mogą wskazywać na błędnie przypisaną datę w metadanych lub ręczne przeniesienie pliku.[/yellow]")
        console.print("[dim]Rozważ użycie 'Skaner -> Krok 3: Napraw LOKALIZACJE', aby automatycznie przenieść te pliki.[/dim]")

async def analyze_metadata_consistency():
    """Konfiguruje i uruchamia zadanie analizy spójności metadanych."""
    
    task = ValidatorTaskRunner(
        task_name="Analiza Spójności Metadanych (Ścieżka vs. Data)",
        data_fetch_func=get_metadata_for_consistency_check,
        record_process_func=_check_single_metadata_consistency,
        summary_func=_display_metadata_summary,
        style="magenta"
    )
    
    await task.run()
