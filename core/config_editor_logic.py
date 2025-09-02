# -*- coding: utf-8 -*-

# plik: core/config_editor_logic.py
# Wersja 21.0 - W pełni asynchroniczny Edytor Konfiguracji
#
# ##############################################################################
# ===                    MODUŁ EDYTORA KONFIGURACJI                          ===
# ##############################################################################
#
# Ten plik zawiera całą logikę dla dwóch kluczowych funkcji:
#
#  1. get_key(): (Przeniesiona do utils.py)
#
#  2. run_config_editor(): W pełni interaktywny edytor pliku `config.py`
#     zbudowany w oparciu o `rich.live`. Pozwala na nawigację, edycję,
#     przełączanie opcji i zapisywanie zmian.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import sys
import os
import re
import json
import time
import logging
from pathlib import Path

# --- Importy specyficzne dla systemu (dla get_key) ---
try:
    import termios
    import tty
    IS_POSIX = True
except ImportError:
    IS_POSIX = False

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.prompt import Prompt, Confirm
from rich.align import Align
from rich.live import Live
from rich.layout import Layout
from rich.table import Table

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .utils import create_interactive_menu, get_key

# --- Konfiguracja i Inicjalizacja ---
console = Console(record=True)
logger = logging.getLogger(__name__)
CONFIG_FILE_PATH = Path(__file__).parent / "config.py"


# ##############################################################################
# ===           SEKCJA 1: FUNKCJE POMOCNICZE EDYTORA KONFIGURACJI            ===
# ##############################################################################

def read_config() -> dict:
    """Odczytuje plik konfiguracyjny `core/config.py` i zwraca jego wartości."""
    logger.info(f"Odczytuję plik konfiguracyjny z: {CONFIG_FILE_PATH}")
    if not CONFIG_FILE_PATH.exists():
        logger.critical(f"BŁĄD KRYTYCZNY: Plik konfiguracyjny '{CONFIG_FILE_PATH}' nie istnieje!")
        console.print(f"[bold red]BŁĄD: Plik konfiguracyjny '{CONFIG_FILE_PATH}' nie istnieje![/bold red]")
        sys.exit(1)
        
    try:
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            config_code = f.read()
        execution_scope = {'__file__': str(CONFIG_FILE_PATH)}
        exec(config_code, execution_scope)
        config_values = {k: v for k, v in execution_scope.items() if k.isupper() and not k.startswith('__')}
        logger.info(f"Pomyślnie wczytano {len(config_values)} zmiennych konfiguracyjnych.")
        return config_values
    except Exception as e:
        logger.critical(f"BŁĄD: Nie można odczytać lub sparsować pliku konfiguracyjnego: {e}", exc_info=True)
        console.print(f"[bold red]BŁĄD: Nie można odczytać pliku konfiguracyjnego: {e}[/bold red]")
        sys.exit(1)


def write_config(new_values: dict) -> bool:
    """
    Zapisuje zaktualizowane wartości do pliku `config.py`, starając się
    zachować oryginalne formatowanie, komentarze i strukturę.
    """
    logger.info("Rozpoczynam zapisywanie zmian do pliku konfiguracyjnego...")
    try:
        # Funkcja pomocnicza do rekurencyjnej konwersji Path na string
        def convert_paths_to_strings(data):
            if isinstance(data, dict):
                return {k: convert_paths_to_strings(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [convert_paths_to_strings(i) for i in data]
            elif isinstance(data, Path):
                return str(data)
            else:
                return data

        # Stwórz głęboką kopię i przekonwertuj wszystkie obiekty Path na stringi
        safe_new_values = convert_paths_to_strings(new_values)

        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        new_lines = []
        variable_regex = re.compile(r'^(?P<indent>\s*)(?P<var_name>[A-Z_][A-Z0-9_]*)\s*=\s*.*')
        lines_iterator = iter(lines)
        
        for line in lines_iterator:
            match = variable_regex.match(line)
            if match and (var_name := match.group('var_name')) in safe_new_values:
                original_indent = match.group('indent')
                new_value = safe_new_values[var_name] # Używamy bezpiecznej kopii
                comment_part = ""
                if '#' in line: comment_part = "  #" + line.split('#', 1)[1].strip()
                
                if isinstance(new_value, str):
                    formatted_value = json.dumps(new_value, ensure_ascii=False)
                elif isinstance(new_value, list):
                    list_str = json.dumps(new_value, indent=4, ensure_ascii=False)
                    json_lines = list_str.splitlines()
                    formatted_value = (json_lines[0] + "\n" + "\n".join([f"{original_indent}{l}" for l in json_lines[1:]]))
                elif isinstance(new_value, dict):
                    dict_str = json.dumps(new_value, indent=4, ensure_ascii=False)
                    json_lines = dict_str.splitlines()
                    formatted_value = (json_lines[0] + "\n" + "\n".join([f"{original_indent}    {l}" for l in json_lines[1:]]))
                else: formatted_value = str(new_value)
                    
                new_lines.append(f"{original_indent}{var_name} = {formatted_value}{comment_part}\n")
                
                start_char, end_char = ('[', ']') if isinstance(new_value, list) else ('{', '}')
                brace_count = line.count(start_char) - line.count(end_char)
                if brace_count > 0:
                    for next_line in lines_iterator:
                        brace_count += next_line.count(start_char) - next_line.count(end_char)
                        if brace_count <= 0: break
            else:
                new_lines.append(line)
                
        with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
            
        logger.info("[bold green]Plik konfiguracyjny został pomyślnie zaktualizowany![/bold green]", extra={"markup": True})
        return True
        
    except Exception as e:
        logger.critical(f"Nie udało się zapisać pliku konfiguracyjnego: {e}", exc_info=True)
        console.print(f"[bold red]Błąd zapisu do pliku konfiguracyjnego: {e}[/bold red]")
        return False


def validate_path(path_str: str) -> tuple[bool, str]:
    """Sprawdza, czy podana ścieżka jest prawidłowa."""
    if not path_str.strip(): return False, "Ścieżka nie może być pusta."
    try:
        p = Path(path_str).expanduser(); p.parent.mkdir(parents=True, exist_ok=True); return True, ""
    except Exception as e: return False, f"Nieprawidłowa ścieżka lub brak uprawnień. Błąd: {e}"

def validate_url(url_str: str) -> tuple[bool, str]:
    """Sprawdza, czy string jest prawdopodobnym adresem URL."""
    is_valid = url_str.strip().startswith("http"); return is_valid, "" if is_valid else "URL musi zaczynać się od 'http'."

def validate_positive_int(num_str: str) -> tuple[bool, str]:
    """Sprawdza, czy string jest liczbą całkowitą nieujemną."""
    try:
        if int(num_str) >= 0: return True, ""
        return False, "Wartość musi być liczbą całkowitą, 0 lub większą."
    except ValueError: return False, "To nie jest prawidłowa liczba całkowita."

def validate_path_or_empty(path_str: str) -> tuple[bool, str]:
    """Sprawdza, czy ścieżka jest prawidłowa LUB czy string jest pusty."""
    if not path_str.strip(): return True, ""
    return validate_path(path_str)

def validate_not_empty(val_str: str) -> tuple[bool, str]:
    """Sprawdza, czy podany string nie jest pusty."""
    return bool(val_str.strip()), "Wartość nie może być pusta."

async def manage_browser_args(config: dict, config_key: str) -> dict:
    """Wyświetla podmenu do edycji argumentów startowych przeglądarki."""
    browser_type = config.get("BROWSER_TYPE", "chromium")
    local_browser_args = config.get(config_key, {}).get(browser_type, []).copy()
    prompt_title = f"Zarządzanie argumentami przeglądarki ([bold]{browser_type}[/bold])"
    while True:
        console.clear(); console.print(Panel(prompt_title, style="yellow", border_style="yellow"))
        if not local_browser_args: console.print("\n[dim]Lista argumentów jest pusta.[/dim]")
        else:
            table = Table(title="Aktualne argumenty"); table.add_column("Argument", style="cyan")
            for arg in local_browser_args: table.add_row(arg)
            console.print(table)
        menu_items = [("Dodaj nowy argument", "add"), ("Usuń argument", "remove"), ("Zapisz zmiany i wróć", "save"), ("Anuluj zmiany i wróć", "cancel")]
        if not local_browser_args: menu_items[1] = ("[dim]Usuń argument (brak)[/dim]", None)
        choice = await create_interactive_menu(menu_items, "Opcje")
        if choice in ["cancel", None]: return config
        if choice == "save":
            if config_key not in config: config[config_key] = {}
            config[config_key][browser_type] = local_browser_args
            console.print("[green]Zmiany zapisane.[/green]"); await asyncio.sleep(1.5); return config
        elif choice == 'add':
            new_arg = Prompt.ask("Podaj nowy argument (np. --start-fullscreen)").strip()
            if new_arg and new_arg.startswith('--'): local_browser_args.append(new_arg)
            else: console.print("[red]Nieprawidłowy format argumentu.[/red]"); await asyncio.sleep(2)
        elif choice == 'remove':
            arg_to_remove = await create_interactive_menu([(arg, arg) for arg in local_browser_args], "Wybierz argument do usunięcia")
            if arg_to_remove: local_browser_args.remove(arg_to_remove)

async def manage_list_config(config: dict, config_key: str) -> dict:
    """Wyświetla podmenu do edycji listy stringów w konfiguracji."""
    local_list = config.get(config_key, []).copy()
    prompt_title = f"Zarządzanie listą '{config_key}'"
    while True:
        console.clear(); console.print(Panel(prompt_title, style="yellow", border_style="yellow"))
        if not local_list: console.print("\n[dim]Lista jest pusta.[/dim]")
        else:
            table = Table(title="Aktualne wpisy"); table.add_column("Wpis", style="cyan")
            for item in local_list: table.add_row(item)
            console.print(table)
        menu_items = [("Dodaj nowy wpis", "add"), ("Usuń wpis", "remove"), ("Zapisz zmiany i wróć", "save"), ("Anuluj zmiany i wróć", "cancel")]
        if not local_list: menu_items[1] = ("[dim]Usuń wpis (brak)[/dim]", None)
        choice = await create_interactive_menu(menu_items, "Opcje")
        if choice in ["cancel", None]: return config
        if choice == "save":
            config[config_key] = sorted(local_list); console.print("[green]Zmiany zapisane.[/green]"); await asyncio.sleep(1.5); return config
        elif choice == 'add':
            new_item = Prompt.ask("Podaj nowy wpis").strip()
            if new_item: local_list.append(new_item)
            else: logger.warning("Pusty wpis nie został dodany."); await asyncio.sleep(1.5)
        elif choice == 'remove':
            item_to_remove = await create_interactive_menu([(item, item) for item in local_list], "Wybierz wpis do usunięcia")
            if item_to_remove: local_list.remove(item_to_remove)

async def _show_selection_dialog(title: str, options: list) -> str | None:
    """Wyświetla uniwersalne, asynchroniczne okno dialogowe do wyboru opcji."""
    logger.debug(f"Wyświetlam okno dialogowe wyboru: '{title}'")
    selected_option_index = 0
    def generate_dialog_panel(selected_idx: int) -> Panel:
        text = Text(justify="center")
        for i, option in enumerate(options):
            style = "bold black on white" if i == selected_idx else "default"
            text.append(f" {option} \n", style=style)
        return Panel(Align.center(text, vertical="middle"), title=title, border_style="yellow")
    with Live(generate_dialog_panel(selected_option_index), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_dialog_panel(selected_option_index), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue
            if key == "UP": selected_option_index = (selected_option_index - 1 + len(options)) % len(options)
            elif key == "DOWN": selected_option_index = (selected_option_index + 1) % len(options)
            elif key == "ENTER": return options[selected_option_index]
            elif key.upper() in ["Q", "ESC"]: return None

def generate_layout(menu_items: list, selected_index: int, config: dict, scroll_offset: int, visible_lines: int) -> Layout:
    """Tworzy i zwraca pełen, dynamiczny układ interfejsu edytora."""
    header = Panel("Interaktywny Edytor Konfiguracji v21.0", style="bold blue", subtitle="[dim]Użyj strzałek GÓRA/DÓŁ do nawigacji.[/dim]")
    content_text = Text(); current_section = None
    start_index, end_index = scroll_offset, scroll_offset + visible_lines
    for i, item in enumerate(menu_items[start_index:end_index]):
        global_index = start_index + i
        is_selected = (global_index == selected_index)
        if "section" in item and item["section"] != current_section:
            current_section = item["section"]; content_text.append(f"\n--- {current_section} ---\n", style="bold yellow")
        line = Text(style="on bright_black" if is_selected else "")
        prefix = "» " if is_selected else "  "
        toggle_indicator = " ⇄" if "toggle_type" in item else ""
        line.append(Text(f"{prefix}{global_index+1:>2}. ", style="bold yellow" if is_selected else "default"))
        line.append(Text(item['prompt'], style="bold white" if is_selected else "default"))
        line.append(f"{toggle_indicator}: ")
        if "action" in item: line.append("(wejdź, aby zarządzać...)", style="dim")
        else:
            value = config.get(item['key'], 'BRAK DANYCH'); value_str = str(value)
            value_color = "white" if is_selected else "cyan"
            if isinstance(value, bool): value_color = "bold green" if value else "bold red"
            elif isinstance(value, list): value_str = f"[{len(value)} elementów]"
            elif item.get("toggle_type") in ["direction", "metadata"]: value_color = "bold magenta"
            elif isinstance(value, int) or (isinstance(value, str) and value.isdigit()): value_color = "bright_blue"
            if len(value_str) > 50: value_str = value_str[:47] + "..."
            line.append(value_str, style=value_color)
        content_text.append(line); content_text.append("\n")
    scroll_indicator = ""
    if len(menu_items) > visible_lines:
        percentage = (scroll_offset / (len(menu_items) - visible_lines)) * 100 if len(menu_items) > visible_lines else 0
        scroll_indicator = f"| Przewijanie: {percentage:.0f}%"
    footer = Align.center(Text(f"Nawigacja: Strzałki | Wybór/Przełącz: Enter | Zapisz: S | Wyjdź: Q {scroll_indicator}", style="bold dim"))
    layout = Layout(); layout.split_column(Layout(header, size=3), Layout(Panel(content_text, border_style="green", title="Opcje Konfiguracyjne")), Layout(footer, size=1))
    return layout

# ##############################################################################
# ===                    SEKCJA 2: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_config_editor():
    """Uruchamia główną, asynchroniczną pętlę interaktywnego edytora konfiguracji."""
    logger.info("Uruchamiam Interaktywny Edytor Konfiguracji...")
    config = read_config()
    original_config = config.copy()

    menu_items = [
        {"key": "SESSION_DIR", "prompt": "Ścieżka do folderu sesji", "validator": validate_path, "section": "USTAWIENIA PODSTAWOWE"},
        {"key": "DATABASE_FILE", "prompt": "Ścieżka do pliku bazy danych", "validator": validate_path, "section": "USTAWIENIA PODSTAWOWE"},
        {"key": "DOWNLOADS_DIR_BASE", "prompt": "Główny folder na pobrane pliki", "validator": validate_path, "section": "USTAWIENIA PODSTAWOWE"},
        {"key": "FORCED_DUPLICATES_DIR", "prompt": "Folder na duplikaty (skan wymuszony)", "validator": validate_path, "section": "USTAWIENIA PODSTAWOWE"},
        {"key": "START_URL", "prompt": "Startowy URL (link do najstarszego zdjęcia)", "validator": validate_url, "section": "USTAWIENIA PODSTAWOWE"},
        {"key": "LOG_FILENAME", "prompt": "Ścieżka do pliku logu aplikacji", "validator": validate_path, "section": "USTAWIENIA PODSTAWOWE"},
        {"key": "URL_INPUT_FILE", "prompt": "Ścieżka do pliku z URL-ami (dla skanera)", "validator": validate_path, "section": "USTAWIENIA PODSTAWOWE"},
        {"key": "LOG_ENABLED","prompt": "Włączyć logi?", "toggle_type": "bool", "section": "USTAWIENIA LOGÓW"},
        {"key": "LOG_TO_FILE", "prompt": "Zapisywać logi do pliku?", "toggle_type": "bool", "section": "USTAWIENIA LOGÓW"},
        {"key": "LOG_LEVEL", "prompt": "Poziom logowania", "predefined_choices": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], "section": "USTAWIENIA LOGÓW"},
        {"key": "DIRECTION_KEY", "prompt": "Kierunek nawigacji", "toggle_type": "direction", "section": "ZACHOWANIE SKANU"},
        {"key": "METADATA_STRATEGY", "prompt": "Strategia pozyskiwania metadanych", "predefined_choices": ['HYBRID', 'EXIF_ONLY', 'ONLINE_ONLY'], "section": "ZACHOWANIE SKANU"},
        {"key": "ENABLE_SHAKE_THE_SCAN", "prompt": "Włączyć 'Potrząśnięcie Skanu' w trybie naprawy?", "toggle_type": "bool", "section": "ZACHOWANIE SKANU"},
        {"key": "ENABLE_PAUSE_AFTER_REPAIR", "prompt": "Pauza po automatycznej naprawie błędów?", "toggle_type": "bool", "section": "ZACHOWANIE SKANU"},
        {"key": "ENABLE_RESOURCE_BLOCKING", "prompt": "Włączyć tryb 'lekki' (blokowanie zasobów)?", "toggle_type": "bool", "section": "WYDAJNOŚĆ I CZASY OCZEKIWANIA"},
        {"key": "ENABLE_ACTION_DELAY", "prompt": "Włączyć losowe opóźnienia między akcjami?", "toggle_type": "bool", "section": "WYDAJNOŚĆ I CZASY OCZEKIWANIA"},
        {"key": "ACTION_DELAY_RANGE", "prompt": "Zakres opóźnienia (min, max) w sekundach", "validator": validate_not_empty, "section": "WYDAJNOŚĆ I CZASY OCZEKIWANIA"},
        {"key": "WAIT_FOR_PAGE_LOAD", "prompt": "Max czas na załadowanie strony (s)", "validator": validate_positive_int, "section": "WYDAJNOŚĆ I CZASY OCZEKIWANIA"},
        {"key": "WAIT_FOR_SELECTOR", "prompt": "Max czas na znalezienie przycisku (s)", "validator": validate_positive_int, "section": "WYDAJNOŚĆ I CZASY OCZEKIWANIA"},
        {"key": "WAIT_FOR_DOWNLOAD_START", "prompt": "Max czas na rozpoczęcie pobierania (s)", "validator": validate_positive_int, "section": "WYDAJNOŚĆ I CZASY OCZEKIWANIA"},
        {"key": "NAV_REFRESH_ATTEMPTS", "prompt": "Ilość prób odświeżenia strony (F5)", "validator": validate_positive_int, "section": "ODPORNOŚĆ I RESTARTY"},
        {"key": "NAV_BLIND_CLICK_ENABLED", "prompt": "Nawigacja 'na ślepo' klawiaturą?", "toggle_type": "bool", "section": "ODPORNOŚĆ I RESTARTY"},
        {"key": "MAX_RETRIES", "prompt": "Ile razy próbować pobrać plik po błędzie", "validator": validate_positive_int, "section": "ODPORNOŚĆ I RESTARTY"},
        {"key": "MAX_RESTARTS_ON_FAILURE", "prompt": "Max liczba restartów silnika po awarii", "validator": validate_positive_int, "section": "ODPORNOŚĆ I RESTARTY"},
        {"key": "RESTART_DELAY_SECONDS", "prompt": "Opóźnienie między restartami (s)", "validator": validate_positive_int, "section": "ODPORNOŚĆ I RESTARTY"},
        {"key": "BROWSER_TYPE", "prompt": "Typ przeglądarki (np. chromium, firefox)", "predefined_choices": ["chromium", "firefox", "webkit"], "section": "USTAWIENIA TECHNICZNE"},
        {"key": "DEFAULT_HEADLESS_MODE", "prompt": "Domyślny tryb cichy (bez okna)?", "toggle_type": "bool", "section": "USTAWIENIA TECHNICZNE"},
        {"key": "ENABLE_HEADLESS_CURSOR", "prompt": "Kursor w trybie cichym?", "toggle_type": "bool", "section": "USTAWIENIA TECHNICZNE"},
        {"key": "BROWSER_ARGS", "prompt": "Zarządzaj argumentami startowymi przeglądarki", "action": manage_browser_args, "section": "USTAWIENIA TECHNICZNE"},
        {"key": "LOCAL_SCANNER_DIRECTORIES", "prompt": "Zapamiętane foldery lokalnego skanera", "action": manage_list_config, "section": "USTAWIENIA MODUŁÓW DODATKOWYCH"},
        {"key": "FACE_DB_VECTOR_PATH", "prompt": "Folder na bazy wektorów twarzy", "validator": validate_path, "section": "USTAWIENIA MODUŁÓW DODATKOWYCH"},
        {"key": "AI_TAGGER_CONFIDENCE_THRESHOLD", "prompt": "Próg pewności AI Taggera (0.0-1.0)", "validator": validate_not_empty, "section": "USTAWIENIA MODUŁÓW DODATKOWYCH"},
        {"key": "TELEGRAM_BOT_TOKEN", "prompt": "Token bota Telegram", "validator": validate_not_empty, "section": "POWIADOMIENIA"},
        {"key": "TELEGRAM_CHAT_ID", "prompt": "ID czatu Telegram", "validator": validate_not_empty, "section": "POWIADOMIENIA"},
        {"key": "AI_MODELS_CACHE_DIR", "prompt": "Folder na pobrane modele AI (cache)", "validator": validate_path_or_empty, "section": "USTAWIENIA AI"},
        {"key": "INFO_PANEL_BUTTON_SELECTOR", "prompt": "Selektor CSS przycisku 'Informacje'", "validator": validate_not_empty, "section": "SELEKTORY CSS (DLA EKSPERTÓW)"},
        {"key": "DOWNLOAD_OPTION_SELECTOR", "prompt": "Selektor CSS opcji 'Pobierz' w menu", "validator": validate_not_empty, "section": "SELEKTORY CSS (DLA EKSPERTÓW)"},
        {"key": "THREE_DOTS_MENU_SELECTOR", "prompt": "Selektor CSS menu 'Więcej opcji'", "validator": validate_not_empty, "section": "SELEKTORY CSS (DLA EKSPERTÓW)"},
        {"key": "NAV_ARROW_LEFT_SELECTOR", "prompt": "Selektor CSS strzałki w lewo", "validator": validate_not_empty, "section": "SELEKTORY CSS (DLA EKSPERTÓW)"},
        {"key": "NAV_ARROW_RIGHT_SELECTOR", "prompt": "Selektor CSS strzałki w prawo", "validator": validate_not_empty, "section": "SELEKTORY CSS (DLA EKSPERTÓW)"},
    ]

    selected_index, scroll_offset = 0, 0
    VISIBLE_LINES = 15

    with Live(generate_layout(menu_items, selected_index, config, scroll_offset, VISIBLE_LINES), screen=True, auto_refresh=False) as live:
        while True:
            live.update(generate_layout(menu_items, selected_index, config, scroll_offset, VISIBLE_LINES), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue

            if key == "UP":
                selected_index = max(0, selected_index - 1)
                if selected_index < scroll_offset: scroll_offset = selected_index
            elif key == "DOWN":
                selected_index = min(len(menu_items) - 1, selected_index + 1)
                if selected_index >= scroll_offset + VISIBLE_LINES: scroll_offset = selected_index - VISIBLE_LINES + 1
            elif key.upper() == "Q":
                if config != original_config:
                    live.stop()
                    if Confirm.ask("[yellow]Wykryto niezapisane zmiany. Wyjść bez zapisywania?[/]", default=False, console=console):
                        logger.warning("Zmiany w konfiguracji anulowane."); break
                    live.start(refresh=True)
                else: break
            elif key.upper() == "S":
                live.stop()
                if write_config(config): break
                else:
                    Prompt.ask("[red]Naciśnij Enter, aby kontynuować edycję...[/]", console=console)
                    live.start(refresh=True)
            elif key == "ENTER":
                option = menu_items[selected_index]
                var_name = option.get("key")
                
                if "toggle_type" in option:
                    if option["toggle_type"] == "bool": config[var_name] = not config.get(var_name, False)
                    elif option["toggle_type"] == "direction": config[var_name] = "ArrowRight" if config.get(var_name, "ArrowLeft") == "ArrowLeft" else "ArrowLeft"
                    continue

                live.stop()
                
                if "action" in option:
                    config = await option["action"](config, var_name)
                elif "predefined_choices" in option:
                    choices = option["predefined_choices"]
                    title = f"Wybierz nową wartość dla [bold]{option['prompt']}[/bold]"
                    new_value = await _show_selection_dialog(title, choices)
                    if new_value is not None: config[var_name] = new_value
                else:
                    prompt_text = option["prompt"]
                    validator = option.get("validator", lambda v: (True, ""))
                    current_value = config.get(var_name, "")
                    new_value_str = Prompt.ask(f"Podaj nową wartość dla [bold]{prompt_text}[/]", default=str(current_value), console=console)
                    is_valid, error_message = validator(new_value_str)
                    if is_valid:
                        original_type = type(config.get(var_name))
                        try:
                           if original_type == int: config[var_name] = int(new_value_str)
                           elif original_type == float: config[var_name] = float(new_value_str.replace(',', '.'))
                           else: config[var_name] = new_value_str
                        except (ValueError, TypeError): config[var_name] = new_value_str
                    else:
                        console.print(f"[bold red]BŁĄD WALIDACJI: {error_message}[/bold red]"); await asyncio.sleep(2)
                
                live.start(refresh=True)
