# plik: core/scanner/offline/base_task.py
# Wersja 1.0 - Uniwersalny runner dla zadań offline (refaktoryzacja)
# Opis: Ten moduł zawiera klasę OfflineTaskRunner, która hermetyzuje
#       wspólną logikę dla wszystkich zadań offline (pasek postępu,
#       pętla, obsługa błędów, podsumowanie).
# -*- coding: utf-8 -*-

import logging
from typing import Callable, Coroutine, List, Dict, Any

# --- Zależności zewnętrzne ---
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress

# --- Inicjalizacja ---
logger = logging.getLogger(__name__)
console = Console()

class OfflineTaskRunner:
    """
    Uniwersalna klasa do uruchamiania zadań offline, które przetwarzają
    rekordy z bazy danych.
    """
    def __init__(self,
                 task_name: str,
                 fetch_data_func: Callable[[], Coroutine[Any, Any, List[Dict]]],
                 process_record_func: Callable[[Dict], Coroutine],
                 border_style: str = "green"):
        """
        Args:
            task_name: Nazwa zadania do wyświetlenia w panelu.
            fetch_data_func: Asynchroniczna funkcja, która pobiera dane z bazy.
            process_record_func: Asynchroniczna funkcja, która przetwarza jeden rekord.
            border_style: Kolor ramki panelu `rich`.
        """
        self.task_name = task_name
        self.fetch_data = fetch_data_func
        self.process_record = process_record_func
        self.border_style = border_style
        
        # Liczniki do podsumowania
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0

    async def run(self):
        """
        Główna metoda, która uruchamia cały proces:
        1. Wyświetla nagłówek.
        2. Pobiera dane.
        3. Przetwarza dane w pętli z paskiem postępu.
        4. Wyświetla podsumowanie.
        """
        console.clear()
        console.print(Panel(f"🛰️  {self.task_name} (Offline) 🛰️", expand=False, style=self.border_style))

        records_to_process = await self.fetch_data()

        if not records_to_process:
            logger.warning(f"Brak rekordów do przetworzenia dla zadania: '{self.task_name}'.")
            console.print("\n[bold green]✅ Wygląda na to, że nie ma nic do zrobienia.[/bold green]")
            return

        logger.info(f"Znaleziono {len(records_to_process)} rekordów do przetworzenia dla zadania '{self.task_name}'.")

        with Progress(console=console, transient=True) as progress:
            task_id = progress.add_task(f"[green]Przetwarzanie...[/]", total=len(records_to_process))
            
            for record in records_to_process:
                try:
                    result = await self.process_record(record)
                    # Funkcja przetwarzająca może zwrócić status 'skipped'
                    if result == 'skipped':
                        self.skipped_count += 1
                    else:
                        self.success_count += 1
                except Exception as e:
                    self.error_count += 1
                    # Logujemy błąd, ale nie przerywamy pętli
                    record_id = record.get('id', record.get('final_path', 'N/A'))
                    logger.error(
                        f"Błąd podczas przetwarzania rekordu '{record_id}' w zadaniu '{self.task_name}': {e}",
                        exc_info=True
                    )
                finally:
                    progress.update(task_id, advance=1)

        self._print_summary()

    def _print_summary(self):
        """Wyświetla końcowe podsumowanie operacji."""
        logger.info(f"Zakończono zadanie '{self.task_name}'. Sukces: {self.success_count}, Błędy: {self.error_count}, Pominięte: {self.skipped_count}")
        console.print(f"\n[bold {self.border_style}]Zakończono: {self.task_name}.[/]")
        console.print(f"  - [green]Pomyślnie przetworzono: {self.success_count}[/]")
        console.print(f"  - [yellow]Pominięto: {self.skipped_count}[/]")
        console.print(f"  - [red]Błędy: {self.error_count}[/]")
        if self.error_count > 0:
            console.print("[dim]Sprawdź pliki logów, aby uzyskać więcej informacji o błędach.[/dim]")
