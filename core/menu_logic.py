# -*- coding: utf-8 -*-

# plik: core/menu_logic.py
# Wersja 17.2 - Zintegrowano Importer Plików z archiwum Takeout
#
# ##############################################################################
# ===                        GŁÓWNA LOGIKA MENU APLIKACJI                    ===
# ##############################################################################
#
# Ten plik zawiera logikę głównego, interaktywnego menu aplikacji. Jego zadaniem
# jest dynamiczne renderowanie interfejsu, obsługa nawigacji, wczytywanie
# statystyk i uruchamianie odpowiednich modułów/narzędzi w odpowiedzi na
# akcje użytkownika.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import logging
from functools import partial

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text
from rich.align import Align
from rich.layout import Layout
from rich.live import Live

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
# Konfiguracja i narzędzia podstawowe
from .config import DEFAULT_HEADLESS_MODE, DATABASE_FILE
from .utils import create_interactive_menu
from .config_editor_logic import get_key

# Nowy, asynchroniczny moduł bazy danych
from .database import get_db_stats, get_failed_urls_from_db, get_state

# --- GŁÓWNE SILNIKI I NARZĘDZIA ---
# Centrum Pobierania
from .master_logic import run_with_restarts as run_master_restarts
from .master_logic import interactive_retry_failed_files as master_interactive_retry
from .master_logic import run_single_file_download

# Skanery i Importery
from .advanced_scanner_logic import run_advanced_scanner
from .local_scanner_logic import run_local_scanner_menu
from .takeout_importer_logic import run_takeout_importer
from .takeout_url_processor_logic import run_takeout_url_processor
# --- POCZĄTEK ZMIAN: Import nowego modułu ---
from .takeout_file_importer_logic import run_takeout_file_importer
# --- KONIEC ZMIAN ---
from .recovery_logic import run_recovery_downloader
from .advanced_recovery_logic import run_advanced_recovery


# Narzędzia Analityczne i Diagnostyczne
from .analytics import run_analytics
from .smart_archiver_logic import run_smart_archiver
from .visual_duplicate_finder import run_visual_duplicate_finder
from .integrity_validator_logic import run_integrity_validator
from .doctor_logic import run_doctor
from .guardian_logic import run_guardian_menu

# Narzędzia Utrzymaniowe i Naprawcze
from .path_fix_tool import run_path_fixer
from .backup_logic import run_backup_manager
from .exif_writer_logic import run_exif_writer
from .image_fixer_logic import run_image_fixer
from .session_logic import refresh_session

# Narzędzia AI
from .ai_tagger_logic import run_ai_tagger_menu
from .face_recognition_logic import run_face_recognition_menu

# Narzędzia Zaawansowane / Deweloperskie
from .config_editor_logic import run_config_editor
from .db_editor_logic import run_db_editor
from .attribute_explorer_logic import run_attribute_explorer
from .profiler_logic import run_profiler
from .test_suite_logic import run_test_suite
from .code_analyzer_logic import run_code_analyzer
from .interceptor_logic import run_interceptor

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)


# ##############################################################################
# ===                     SEKCJA 1: DEFINICJE PODMENU                        ===
# ##############################################################################

async def _run_generic_tools_submenu(title: str, items: list, border_style: str):
    """
    Uniwersalna funkcja pomocnicza do obsługi prostych podmenu z narzędziami.
    """
    menu_items = items + [("Wróć", "back")]
    while True:
        console.clear()
        selected_action = await create_interactive_menu(menu_items, title, border_style=border_style)
        if selected_action == "back" or selected_action is None:
            logger.info(f"Powrót z podmenu '{title}'.")
            break
        
        original_func_name = getattr(getattr(selected_action, 'func', selected_action), '__name__', 'unknown_function')
        logger.info(f"Uruchamianie narzędzia z podmenu '{title}': [bold cyan]{original_func_name}[/bold cyan]", extra={"markup": True})
        
        await selected_action()
        Prompt.ask(f"\n[bold]Naciśnij Enter, aby wrócić do menu '{title}'...[/]")

async def run_master_scan_submenu():
    """
    Wyświetla i zarządza podmenu dla głównego silnika pobierającego ("Master").
    """
    while True:
        console.clear()
        num_failed_urls = await get_failed_urls_from_db()
        num_failed = len(num_failed_urls)
        last_scan_url = await get_state('last_scan_url')

        scan_label = "Rozpocznij nowy skan"
        if last_scan_url: scan_label = f"Wznów skan (od ...{last_scan_url[-40:]})"
        
        repair_label = f"Napraw błędy ({num_failed}), następnie wznów skan"
        if num_failed == 0: repair_label = "[dim]Napraw błędy (brak błędów do naprawy)[/dim]"

        menu_items = [
            (scan_label, partial(run_master_restarts, scan_mode='main', retry_failed=False, headless_mode=DEFAULT_HEADLESS_MODE)),
            (repair_label, partial(run_master_restarts, scan_mode='main', retry_failed=True, headless_mode=DEFAULT_HEADLESS_MODE)),
            ("Interaktywne ponawianie błędów", master_interactive_retry),
            ("Wymuś pełne odświeżenie (od początku)", partial(run_master_restarts, scan_mode='forced', retry_failed=False, headless_mode=DEFAULT_HEADLESS_MODE)),
            ("Wróć do menu głównego", "back")
        ]
        if num_failed == 0: menu_items[1] = (repair_label, None)

        selected_action = await create_interactive_menu(menu_items, "Centrum Pobierania (Silnik Master)", border_style="blue")
        if selected_action in ["back", None]: break
        if selected_action is None: continue
        
        await selected_action()
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter, aby wrócić do menu Master...[/]")

async def run_analysis_tools_submenu():
    """
    Wyświetla podmenu dla narzędzi analitycznych i diagnostycznych.
    """
    items = [
        ("Analiza i Statystyki Kolekcji", run_analytics),
        ("Asystent Porządkowania Zdjęć", run_smart_archiver),
        ("Znajdź Duplikaty Wizualne", run_visual_duplicate_finder),
        ("Walidator Integralności Danych", run_integrity_validator),
        ("Diagnostyka Systemu (Doktor)", run_doctor),
        ("Strażnik Systemu (Powiadomienia)", run_guardian_menu),
    ]
    await _run_generic_tools_submenu("Narzędzia Analityczne i Diagnostyczne", items, "green")

async def run_maintenance_tools_submenu():
    """
    Wyświetla podmenu dla narzędzi utrzymaniowych i naprawczych.
    """
    items = [
        ("Importuj metadane z Google Takeout", run_takeout_importer),
        # --- POCZĄTEK ZMIAN: Dodanie nowej opcji ---
        ("Importuj PLIKI z Google Takeout (uzupełnij braki)", run_takeout_file_importer),
        ("Napraw uszkodzone pliki obrazów (JPEG, PNG...)", run_image_fixer),
        # --- KONIEC ZMIAN ---
        ("Uzupełnij/Napraw z URL-a Takeout", run_takeout_url_processor),
        ("Napraw ścieżki plików w bazie", run_path_fixer),
        ("Menedżer Kopii Zapasowych", run_backup_manager),
        ("Zapisz metadane do plików (EXIF)", run_exif_writer),
        ("Odśwież Sesję Logowania", refresh_session),
        ("Silnik Ratunkowy (Prosty)", run_recovery_downloader),
        ("Zaawansowana Naprawa (Shake)", run_advanced_recovery),
    ]
    await _run_generic_tools_submenu("Narzędzia Utrzymaniowe i Naprawcze", items, "yellow")

async def run_advanced_tools_submenu():
    """
    Wyświetla podmenu dla narzędzi zaawansowanych i deweloperskich.
    """
    items = [
        ("Edytor Konfiguracji", run_config_editor),
        ("Edytor Bazy Danych", partial(run_db_editor, DATABASE_FILE)),
        ("Odkrywca Atrybutów Strony", run_attribute_explorer),
        ("Profiler Wydajności Silnika", run_profiler),
        ("Pakiet Testowy", run_test_suite),
        ("Audytor Kodu (Flake8 + Unittest)", run_code_analyzer),
        ("Podsłuch Sieciowy (Interceptor)", run_interceptor),
    ]
    await _run_generic_tools_submenu("Narzędzia Zaawansowane / Deweloperskie", items, "magenta")

# ##############################################################################
# ===                    SEKCJA 2: GŁÓWNE MENU APLIKACJI                     ===
# ##############################################################################

def generate_main_layout(selected_index: int, menu_items: list, stats: dict) -> Layout:
    """
    Tworzy pełny, dynamiczny układ interfejsu menu głównego.
    """
    header = Align.center(Text(">>> Google Photos Toolkit v17.2 <<<", style="bold white on blue"), vertical="middle")
    
    menu_text = Text(justify="center")
    for i, (text, action) in enumerate(menu_items):
        if action is None:
            menu_text.append(f"─ {text} ─\n", style="dim")
            continue
        style = "bold black on white" if i == selected_index else ""
        prefix = "» " if i == selected_index else "  "
        menu_text.append(Text.from_markup(f"{prefix}{text}\n", style=style))

    stats_table = Table.grid(padding=(0, 1), expand=True)
    stats_table.add_column(); stats_table.add_column(style="bold", justify="right")
    stats_table.add_row("Pobrane pliki:", f"[green]{stats.get('downloaded', 0)}[/green]")
    stats_table.add_row("Pominięte:", f"[yellow]{stats.get('skipped', 0)}[/yellow]")
    stats_table.add_row("Zeskanowane metadane:", f"[blue]{stats.get('scanned', 0)}[/blue]")
    stats_table.add_row("Błędy pobierania:", f"[red]{stats.get('failed', 0)}[/red]")
    stats_table.add_row("─" * 25, "─" * 10)
    stats_table.add_row("[bold]Łącznie wpisów:[/]", f"[bold cyan]{stats.get('total', 0)}[/bold cyan]")
    
    layout = Layout()
    layout.split_column(
        Layout(Panel(header, border_style="blue"), name="header", size=3),
        Layout(Align.center(menu_text, vertical="middle"), name="main", ratio=1),
        Layout(Panel(stats_table, title="Statystyki Kolekcji", border_style="dim"), name="footer", size=8)
    )
    return layout

async def run_main_menu():
    """
    Główna, nieskończona pętla, która zarządza całym menu głównym aplikacji.
    """
    selected_index = 0
    
    actions_with_own_loop = [
        run_master_scan_submenu, run_advanced_scanner, run_analysis_tools_submenu,
        run_maintenance_tools_submenu, run_advanced_tools_submenu
    ]

    menu_items = [
        ("GŁÓWNE MODUŁY", None),
        ("Centrum Pobierania (Silnik Master)", run_master_scan_submenu),
        ("Pobierz pojedynczy plik z URL", run_single_file_download),
        ("Skaner i Menedżer Metadanych", run_advanced_scanner),
        ("Lokalny Importer i Organizator Plików", run_local_scanner_menu),
        ("NARZĘDZIA AI", None),
        ("[bold magenta]🤖 Uruchom Inteligentne Tagowanie Obrazów (AI)[/bold magenta]", run_ai_tagger_menu),
        ("[bold magenta]👨‍👩‍👧‍👦 Uruchom Rozpoznawanie Twarzy (AI)[/bold magenta]", run_face_recognition_menu),
        ("NARZĘDZIA DODATKOWE", None),
        ("Narzędzia Analityczne i Diagnostyczne", run_analysis_tools_submenu),
        ("Narzędzia Utrzymaniowe i Naprawcze", run_maintenance_tools_submenu),
        ("Narzędzia Zaawansowane / Deweloperskie", run_advanced_tools_submenu),
        ("ZAKOŃCZ PRACĘ", None),
        ("Wyjście z Aplikacji", "exit"),
    ]
    
    selected_index = next((i for i, item in enumerate(menu_items) if item[1] is not None), 0)

    while True:
        stats = await get_db_stats()
        
        with Live(generate_main_layout(selected_index, menu_items, stats), screen=True, auto_refresh=False, transient=True) as live:
            while True:
                live.update(generate_main_layout(selected_index, menu_items, stats), refresh=True)
                key = await asyncio.to_thread(get_key)
                if not key: continue

                if key == "UP":
                    selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                    while menu_items[selected_index][1] is None:
                        selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                elif key == "DOWN":
                    selected_index = (selected_index + 1) % len(menu_items)
                    while menu_items[selected_index][1] is None:
                        selected_index = (selected_index + 1) % len(menu_items)
                elif key.upper() == "Q":
                    logger.info("Użytkownik wybrał wyjście z aplikacji (Q)."); return
                elif key == "ENTER":
                    _, selected_action = menu_items[selected_index]
                    if selected_action == "exit":
                        logger.info("Wybrano opcję 'Wyjście'. Zamykanie aplikacji."); return
                    
                    live.stop(); console.clear()
                    
                    func_to_log = getattr(selected_action, 'func', selected_action)
                    logger.info(f"Wybrano opcję z menu głównego: [bold cyan]{func_to_log.__name__}[/bold cyan]", extra={"markup": True})
                    await selected_action()

                    if func_to_log not in actions_with_own_loop:
                        Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić do menu głównego...[/]")
                    
                    console.clear(); break
