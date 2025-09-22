# plik: core/validator/base_task.py
# Wersja 2.1 (Ostateczna) - Poprawiono błąd NameError w metodzie run
# -*- coding: utf-8 -*-

import logging
from typing import Callable, Coroutine, List, Any

from rich.console import Console
from rich.panel import Panel

# Importujemy nasz komponent UI
from .ui import ValidatorLiveDisplay

logger = logging.getLogger(__name__)
console = Console()

class ValidatorTaskRunner:
    """Uniwersalny silnik do uruchamiania zadań Walidatora z dashboardem."""
    
    def __init__(self,
                 task_name: str,
                 data_fetch_func: Callable[[], Coroutine],
                 record_process_func: Callable[[Any], Coroutine[Any, Any, str | None]],
                 summary_func: Callable[[List[Any], int], None],
                 style: str = "yellow"):
        self.task_name = task_name
        self.fetch_data = data_fetch_func
        self.process_record = record_process_func # Przechowujemy funkcję jako atrybut
        self.display_summary = summary_func
        self.style = style

    async def run(self):
        console.clear()
        
        with console.status("[cyan]Pobieranie danych z bazy...[/]"):
            records = await self.fetch_data()
        
        if not records:
            console.print(Panel(f"🔎 {self.task_name} 🔎", expand=False, style=f"bold {self.style}"))
            console.print("\n[green]Nie znaleziono żadnych danych do przetworzenia.[/green]")
            return

        found_items = []
        
        with ValidatorLiveDisplay(total_items=len(records), title=self.task_name) as display:
            for record in records:
                try:
                    current_item_text = record.get('final_path', str(record.get('id', 'N/A')))
                    
                    # === OSTATECZNA POPRAWKA JEST TUTAJ ===
                    # Używamy atrybutu `self.process_record`, który został
                    # ustawiony w konstruktorze __init__.
                    found_item = await self.process_record(record)
                    # === KONIEC POPRAWKI ===
                    
                    if found_item is not None:
                        found_items.append(found_item)
                    
                    display.update_progress(current_item_text, found_item)
                except Exception as e:
                    logger.error(f"Błąd podczas przetwarzania w '{self.task_name}': {e}", exc_info=True)
        
        # Podsumowanie jest wyświetlane PO zamknięciu dashboardu
        self.display_summary(found_items, len(records))
