# plik: core/validator/tasks/hashing.py
# Wersja 1.0 - Zrefaktoryzowana logika obliczania hashy z dashboardem
# -*- coding: utf-8 -*-

import asyncio
import hashlib
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from typing import List

from rich.console import Console
from rich.panel import Panel

from ...database import get_records_to_hash, update_hashes_batch
from ..ui import ValidatorLiveDisplay

logger = logging.getLogger(__name__)
console = Console()

BATCH_SIZE = 100 # Liczba hashy zapisywanych do bazy w jednej transakcji

def _calculate_hash_sync(file_path: Path) -> str | None:
    """
    Synchroniczna funkcja do obliczania hasha MD5.
    Zaprojektowana do uruchamiania w osobnym procesie.
    """
    # Ta funkcja nie używa loggera, aby uniknąć problemów z serializacją
    hasher = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        return hasher.hexdigest()
    except (IOError, OSError):
        # Błędy będą logowane w głównym procesie
        return None

async def verify_and_write_hashes():
    """
    Oblicza i zapisuje sumy kontrolne MD5, wykorzystując wielordzeniowe
    przetwarzanie i wyświetlając postęp na interaktywnym dashboardzie.
    """
    console.clear()
    
    with console.status("[cyan]Pobieranie listy plików do hashowania...[/]"):
        records_to_hash = await get_records_to_hash()

    if not records_to_hash:
        console.print(Panel("🧮 Obliczanie i Zapis Sum Kontrolnych (MD5) 🧮", expand=False, style="bold yellow"))
        console.print("\n[bold green]✅ Wszystkie pobrane pliki w bazie mają już obliczone sumy kontrolne.[/bold green]")
        return

    logger.info(f"Znaleziono {len(records_to_hash)} plików do obliczenia hasha.")
    updates_batch: list[tuple[str, int]] = []
    
    loop = asyncio.get_running_loop()
    
    with ValidatorLiveDisplay(total_items=len(records_to_hash), title="Obliczanie Sum Kontrolnych (MD5)") as display:
        with ProcessPoolExecutor() as executor:
            # Tworzymy mapowanie ścieżki do ID rekordu w bazie
            path_to_id_map = {Path(rec['final_path']): rec['id'] for rec in records_to_hash}
            
            # Tworzymy listę zadań do wykonania w puli procesów
            futures = [
                loop.run_in_executor(executor, _calculate_hash_sync, path)
                for path in path_to_id_map.keys()
            ]

            # Przetwarzamy wyniki w miarę ich pojawiania się
            for path, future in zip(path_to_id_map.keys(), asyncio.as_completed(futures)):
                try:
                    file_hash = await future
                    
                    if file_hash:
                        entry_id = path_to_id_map[path]
                        updates_batch.append((file_hash, entry_id))
                        # W tym zadaniu nie ma "znalezionych", więc przekazujemy None
                        display.update_progress(str(path), None) 
                    else:
                        error_msg = f"Błąd odczytu pliku: {path.name}"
                        logger.error(error_msg)
                        display.update_progress(str(path), error_msg) # Pokaż błąd na liście znalezionych

                    # Zapisujemy do bazy w partiach
                    if len(updates_batch) >= BATCH_SIZE:
                        await update_hashes_batch(updates_batch)
                        logger.info(f"Zapisano partię {len(updates_batch)} hashy do bazy.")
                        updates_batch.clear()
                        
                except Exception as e:
                    error_msg = f"Błąd przetwarzania: {path.name}"
                    logger.error(f"{error_msg}: {e}", exc_info=True)
                    display.update_progress(str(path), error_msg)

    # Zapisz ostatnią partię
    if updates_batch:
        await update_hashes_batch(updates_batch)
        logger.info(f"Zapisano ostatnią partię {len(updates_batch)} hashy.")

    console.print(Panel(f"✅ Zakończono! Zaktualizowano {len(records_to_hash)} sum kontrolnych.", style="bold green", expand=False))
