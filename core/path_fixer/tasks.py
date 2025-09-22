# plik: core/path_fixer/tasks.py
# Wersja 1.1 - Zintegrowano z dedykowanym LiveDisplay.
# Opis: Ten moduł zawiera czystą logikę biznesową dla Narzędzia do Naprawy Ścieżek.
#       Odpowiada za analizę ścieżek w bazie danych i wykonywanie na nich
#       operacji naprawczych.
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import List, Dict, Any

# Importujemy dedykowany dashboard z pakietu UI
from .ui.live_display import PathFixerLiveDisplay

# Importujemy funkcje do interakcji z bazą danych
from ..database import get_downloaded_entries_for_path_fixing, update_paths_for_entry_by_id

# Inicjalizacja
logger = logging.getLogger(__name__)


async def find_mismatched_path_prefix(
    all_entries: List[Dict[str, Any]],
    current_prefix: Path
) -> str | None:
    """
    Analizuje listę wpisów z bazy danych i zwraca pierwszy znaleziony,
    nieaktualny prefiks ścieżki.

    Porównuje ścieżkę każdego pliku z aktualną ścieżką bazową z konfiguracji.
    Jeśli znajdzie niezgodność, próbuje "odgadnąć", jaki był stary prefiks.

    Args:
        all_entries (List[Dict[str, Any]]): Lista wpisów z bazy (słowniki).
        current_prefix (Path): Aktualna, poprawna ścieżka bazowa z konfiguracji.

    Returns:
        str | None: Nieaktualny prefiks jako string, lub None, jeśli wszystkie
                    ścieżki są poprawne.
    """
    logger.info("Rozpoczynam analizę w poszukiwaniu nieaktualnych prefiksów ścieżek...")
    for entry in all_entries:
        # Sprawdzamy, czy klucz 'final_path' istnieje i nie jest pusty
        if not (final_path_str := entry.get('final_path')):
            continue
        
        path = Path(final_path_str)
        
        # `is_relative_to` to bezpieczny sposób sprawdzenia, czy `path` zaczyna się od `current_prefix`
        if not path.is_relative_to(current_prefix):
            try:
                # Jeśli ścieżki się nie zgadzają, próbujemy odgadnąć stary prefiks.
                # Zakładamy, że struktura ROK/MIESIĄC jest stała (2 poziomy).
                # Bierzemy wszystkie części ścieżki oprócz ostatnich dwóch.
                old_prefix = Path(*path.parts[:-2])
                logger.info(f"Znaleziono potencjalnie nieaktualny prefiks: {old_prefix}")
                return str(old_prefix)
            except IndexError:
                # Ignorujemy ścieżki, które są zbyt krótkie, aby pasowały do wzorca.
                logger.warning(f"Ignoruję nietypową ścieżkę: {path}")
                continue
                
    logger.info("Nie znaleziono żadnych nieaktualnych prefiksów.")
    return None


async def perform_path_update(
    all_entries: List[Dict[str, Any]],
    old_prefix: str,
    new_prefix: str,
    display: PathFixerLiveDisplay
) -> int:
    """
    Iteruje przez wszystkie wpisy i aktualizuje te, które mają niepoprawny
    prefiks, raportując postęp do obiektu LiveDisplay.

    Args:
        all_entries (List[Dict[str, Any]]): Lista wszystkich wpisów do sprawdzenia.
        old_prefix (str): Stary prefiks ścieżki, który ma być zastąpiony.
        new_prefix (str): Nowy, poprawny prefiks ścieżki.
        display (PathFixerLiveDisplay): Obiekt dashboardu do raportowania postępu.

    Returns:
        int: Liczba wpisów, które zostały faktycznie zaktualizowane.
    """
    updated_count = 0
    
    for entry in all_entries:
        old_final_path = entry.get('final_path')
        old_expected_path = entry.get('expected_path')

        # Sprawdzamy, czy dany wpis faktycznie używa starego prefiksu
        if old_final_path and old_final_path.startswith(old_prefix):
            # Zastępujemy tylko pierwsze wystąpienie, aby uniknąć problemów,
            # gdyby fragment prefiksu powtórzył się w dalszej części ścieżki.
            new_final_path = old_final_path.replace(old_prefix, new_prefix, 1)
            new_expected_path = old_expected_path.replace(old_prefix, new_prefix, 1) if old_expected_path else None
            
            await update_paths_for_entry_by_id(entry['id'], new_final_path, new_expected_path)
            updated_count += 1
            
        # Niezależnie od tego, czy wpis został zaktualizowany,
        # przesuwamy pasek postępu, aby pokazać, że został sprawdzony.
        display.update_progress()
        
    logger.info(f"Zakończono aktualizację. Zmieniono {updated_count} wpisów.")
    return updated_count
