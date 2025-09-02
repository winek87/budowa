# -*- coding: utf-8 -*-

# plik: core/analytics/__init__.py
# Wersja 2.1 - Przywrócenie wyświetlania raportów w terminalu i ulepszone menu
#
# ##############################################################################
# ===                   GŁÓWNY MODUŁ URUCHOMIENIOWY ANALITYKA                  ===
# ##############################################################################
#
# Ten plik jest głównym punktem wejścia dla całego pakietu analitycznego.
# Jego zadaniem jest zaimportowanie wszystkich narzędzi analitycznych
# z poszczególnych podmodułów i złożenie ich w jedno, spójne,
# wielopoziomowe menu nawigacyjne.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import logging
import asyncio
from rich.console import Console
from rich.prompt import Prompt

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from ..utils import create_interactive_menu

# Importujemy wszystkie funkcje-narzędzia z podmodułów
from .reports import (
    show_technical_stats,
    show_seasonal_stats,
    show_people_and_album_rankings,
    show_largest_files_ranking,
    show_metadata_health,
    export_report_menu
)
from .visualizations import (
    show_activity_heatmap,
    show_world_map,
    show_description_word_cloud
)
from .explorers import (
    interactive_timeline_navigator,
    explore_metadata,
    show_camera_stats,
    show_location_stats
)
from .audits import (
    show_missing_data_audit,
    manage_duplicates
)

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


# ##############################################################################
# ===                    GŁÓWNA FUNKCJA URUCHOMIENIOWA                       ===
# ##############################################################################

async def run_analytics():
    """
    Wyświetla i zarządza wielopoziomowym, interaktywnym menu analityka.
    """
    logger.info("Uruchamiam główny moduł Analityka...")
    
    async def run_submenu(title: str, items: list, border_style: str = "cyan"):
        """Pomocnicza funkcja do obsługi podmenu."""
        while True:
            console.clear()
            choice = await create_interactive_menu(items + [("Wróć", "exit")], title, border_style=border_style)
            if choice in ["exit", None]:
                break
            await choice()
            Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić do menu...[/]")

    main_menu_items = [
        ("📊 Pokaż Statystyki i Raporty w Terminalu", "reports"),
        ("🔎 Uruchom Interaktywne Eksploratory", "explorers"),
        ("✨ Uruchom Generatory Wizualne", "visualizations"),
        ("🛠️ Uruchom Narzędzia Zarządcze i Audyty", "audits"),
        ("Wróć do menu głównego", "exit")
    ]
    
    report_menu_items = [
        ("Analiza Techniczna", show_technical_stats),
        ("Analiza Sezonowa i Miesięczna", show_seasonal_stats),
        ("Ranking Osób i Albumów", show_people_and_album_rankings),
        ("Ranking Największych Plików", show_largest_files_ranking),
        ("Raport o Zdrowiu Metadanych", show_metadata_health),
        ("[bold cyan]Wyeksportuj Pełny Raport Statystyczny do HTML[/bold cyan]", export_report_menu),
    ]
    
    explorer_menu_items = [
        ("Interaktywna Oś Czasu Kolekcji", interactive_timeline_navigator),
        ("Interaktywny Ranking Aparatów", show_camera_stats),
        ("Interaktywny Ranking Lokalizacji", show_location_stats),
        ("Eksplorator Metadanych (Filtrowanie)", explore_metadata),
    ]
    
    viz_menu_items = [
        ("Mapa Cieplna Aktywności", show_activity_heatmap),
        ("Interaktywna Mapa Świata", show_world_map),
        ("Wygeneruj Chmurę Słów z Opisów", show_description_word_cloud),
    ]
    
    audit_menu_items = [
        ("Audyt Brakujących Danych", show_missing_data_audit),
        ("Znajdź i Zarządzaj Duplikatami", manage_duplicates),
    ]
    
    submenu_map = {
        "reports": ("Statystyki i Raporty", report_menu_items),
        "explorers": ("Eksploracja i Przeglądanie", explorer_menu_items),
        "visualizations": ("Generatory Wizualne", viz_menu_items),
        "audits": ("Narzędzia Zarządcze i Audyty", audit_menu_items),
    }

    while True:
        console.clear()
        main_choice = await create_interactive_menu(main_menu_items, "Analityk Bazy Danych", "green")
        if main_choice in ["exit", None]:
            break
        if main_choice in submenu_map:
            title, items = submenu_map[main_choice]
            await run_submenu(title, items)
