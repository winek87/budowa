# -*- coding: utf-8 -*-

# plik: core/analytics/audits.py
# Wersja 2.0 - Pełna integracja z asynchronicznym modułem bazy danych
#
# ##############################################################################
# ===            MODUŁ AUDYTÓW I NARZĘDZI ZARZĄDCZYCH DANYMI                 ===
# ##############################################################################
#
# Ten plik zawiera zaawansowane narzędzia, które nie tylko analizują dane,
# ale również pozwalają na ich modyfikację. Funkcje te służą do "leczenia"
# kolekcji, wyszukiwania problemów i zarządzania duplikatami.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import logging
import json
from pathlib import Path

# --- Importy asynchroniczne ---
import aiosqlite

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from ..config import DATABASE_FILE
from ..database import reset_media_for_reprocessing
from ..utils import create_interactive_menu
from .data_loader import get_all_media_entries

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


async def show_missing_data_audit():
    """
    Uruchamia interaktywny audyt w poszukiwaniu plików z brakującymi
    kluczowymi polami metadanych.
    """
    console.clear()
    logger.info("Uruchamiam Audyt Brakujących Danych...")
    console.print(Panel("🩺 Audyt Brakujących Danych w Kolekcji 🩺", expand=False, style="bold green"))

    KEYS_TO_AUDIT = {
        '1': ("Rozmiar Pliku", "size"),
        '2': ("Wymiary", "dimensions"),
        '3': ("Aparat/Telefon", "camera"),
        '4': ("Lokalizacja", "location"),
    }
    audit_prompt_text = "\n".join(f"  [cyan]{k}[/cyan]. {v[0]}" for k, v in KEYS_TO_AUDIT.items())
    choice = Prompt.ask(f"Wybierz, którego klucza metadanych chcesz szukać:\n{audit_prompt_text}\n\n[dim]Wpisz numer (lub 'q'):[/dim]")
    if choice.lower() == 'q' or choice not in KEYS_TO_AUDIT:
        logger.warning("Anulowano audyt brakujących danych.")
        return
        
    friendly_name, key_to_find = KEYS_TO_AUDIT[choice]
    logger.info(f"Rozpoczynam audyt w poszukiwaniu plików bez klucza '{key_to_find}'.")

    files_with_missing_key = []
    with console.status(f"[cyan]Skanowanie bazy w poszukiwaniu brakującego pola '{friendly_name}'...[/]"):
        # NOWA LOGIKA: Użyj data_loadera, który poprawnie parsuje dane
        all_media_data = await get_all_media_entries()
        
        for entry in all_media_data:
            # Sprawdzamy klucz w już sparsowanych, ustandaryzowanych danych
            if not entry.get(key_to_find):
                files_with_missing_key.append({
                    'id': entry['id'],
                    'filename': entry.get('filename', 'Brak nazwy'),
                    'date': entry.get('dt').strftime('%Y-%m-%d %H:%M:%S') if entry.get('dt') else 'Brak daty'
                })

    if not files_with_missing_key:
        console.print(Panel(f"[bold green]Gratulacje![/bold green]\nWszystkie analizowane pliki posiadają klucz '[bold]{friendly_name}[/bold]'.", border_style="green"))
        logger.info(f"Audyt zakończony. Nie znaleziono plików bez klucza '{key_to_find}'.")
        return

    table = Table(title=f"Znaleziono {len(files_with_missing_key)} plików bez klucza '[bold]{friendly_name}[/bold]'")
    table.add_column("Nazwa Pliku", style="cyan"); table.add_column("Data (jeśli dostępna)", style="green")
    for entry in files_with_missing_key[:100]:
        table.add_row(entry['filename'], entry['date'])
        
    if len(files_with_missing_key) > 100:
        console.print("[yellow]Uwaga: Wyświetlono tylko pierwsze 100 znalezionych plików.[/yellow]")
    console.print(table)
    
    if Confirm.ask(f"\n[bold yellow]Czy chcesz oznaczyć te {len(files_with_missing_key)} pliki do ponownego przetworzenia przez skaner?[/]", default=False):
        ids_to_update = [entry['id'] for entry in files_with_missing_key]
        with console.status("[cyan]Resetowanie statusu w bazie danych...[/]"):
            updated_count = await reset_media_for_reprocessing(ids_to_update)
        if updated_count > 0:
            console.print(Panel(f"[bold green]Sukces![/bold green]\nZresetowano status dla [bold]{updated_count}[/bold] plików.\nUruchom 'Skaner i Menedżer Metadanych', aby pobrać brakujące dane.", border_style="green"))
            logger.info(f"Pomyślnie zresetowano status dla {updated_count} plików.")
        else:
            logger.error("Nie udało się zaktualizować wpisów w bazie danych podczas audytu.")
            console.print("[bold red]Wystąpił błąd podczas aktualizacji bazy danych.[/bold red]")


async def manage_duplicates():
    """
    Wyświetla menu dla narzędzi do zarządzania duplikatami.

    Pełni rolę "launchera" dla różnych metod wyszukiwania i usuwania
    zduplikowanych plików w kolekcji. Pozwala to na łatwe rozbudowanie
    aplikacji o nowe algorytmy w przyszłości.
    """
    logger.info("Uruchamiam Menedżera Duplikatów.")
    
    # Importujemy funkcję wewnątrz, aby uniknąć cyklicznych zależności
    from ..visual_duplicate_finder import run_visual_duplicate_finder
    
    menu_items = [
        ("Znajdź duplikaty wizualne (pHash)", run_visual_duplicate_finder),
        # W przyszłości można tu dodać inne metody, np.:
        # ("Znajdź duplikaty po sumie kontrolnej (MD5)", run_md5_duplicate_finder),
        ("Wróć do menu audytów", "exit")
    ]
    
    while True:
        console.clear()
        selected_action = await create_interactive_menu(
            menu_items,
            "Menedżer Duplikatów",
            border_style="yellow"
        )

        if selected_action == "exit" or selected_action is None:
            logger.info("Powrót z Menedżera Duplikatów.")
            break
        
        # Uruchom wybrane narzędzie
        await selected_action()
        
        # Po zakończeniu narzędzia, poproś o interakcję
        Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić do menu duplikatów...[/]")
