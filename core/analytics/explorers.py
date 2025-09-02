# -*- coding: utf-8 -*-

# plik: core/analytics/explorers.py
# Wersja 2.0 - Pełna integracja z asynchronicznym modułem ładowania danych
#
# ##############################################################################
# ===            MODUŁ INTERAKTYWNYCH EKSPLORATORÓW DANYCH                   ===
# ##############################################################################
#
# Ten plik zawiera zaawansowane, interaktywne narzędzia do "przewiercania się"
# (drill-down) i eksploracji zgromadzonej kolekcji. W przeciwieństwie do
# statycznych raportów, funkcje te pozwalają użytkownikowi na dynamiczne
# nawigowanie, filtrowanie i odkrywanie danych w czasie rzeczywistym.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from collections import Counter

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text
from rich.live import Live

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .data_loader import get_all_media_entries
from ..config_editor_logic import get_key
from ..utils import format_size_for_display

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)

# Stałe używane w tym module
FILES_PER_PAGE = 15
MONTH_NAMES = {
    1: "Styczeń", 2: "Luty", 3: "Marzec", 4: "Kwiecień", 5: "Maj", 6: "Czerwiec",
    7: "Lipiec", 8: "Sierpień", 9: "Wrzesień", 10: "Październik", 11: "Listopad", 12: "Grudzień"
}
DAY_NAMES = {
    0: "Poniedziałek", 1: "Wtorek", 2: "Środa", 3: "Czwartek",
    4: "Piątek", 5: "Sobota", 6: "Niedziela"
}

# plik: core/analytics/explorers.py

async def interactive_timeline_navigator():
    """
    Uruchamia interaktywny, wielopoziomowy eksplorator osi czasu kolekcji.

    Funkcja ta pozwala użytkownikowi na nawigację po kolekcji zdjęć od
    poziomu lat, przez miesiące i dni, aż do szczegółowej, paginowanej
    i sortowalnej listy plików dla wybranego dnia. Jest to główne
    narzędzie do przeglądania kolekcji w porządku chronologicznym.
    """
    console.clear()
    logger.info("Uruchamiam Interaktywną Oś Czasu Kolekcji...")
    console.print(Panel("📅 Interaktywna Oś Czasu Kolekcji 📅", expand=False, style="bold green"))
    
    # Krok 1: Asynchroniczne pobranie i wstępne przetworzenie danych
    with console.status("[cyan]Pobieranie i agregowanie danych...[/]"):
        all_media_data = await get_all_media_entries()
        if not all_media_data:
            logger.warning("Brak danych do analizy osi czasu. Przerwałem uruchomienie narzędzia.")
            console.print("\n[yellow]Nie znaleziono żadnych danych do analizy. Uruchom skaner metadanych.[/]")
            return

        year_counts = Counter(e['dt'].year for e in all_media_data)
        month_counts = Counter((e['dt'].year, e['dt'].month) for e in all_media_data)
        day_counts = Counter((e['dt'].year, e['dt'].month, e['dt'].day) for e in all_media_data)
        logger.debug("Agregacja danych dla osi czasu zakończona.")

    # Krok 2: Inicjalizacja stanu nawigatora
    state = {
        'level': 'years', 'year': None, 'month': None, 'day': None,
        'index': 0, 'page': 0, 'sort_mode_index': 0
    }
    SORT_MODES = [
        ('name_asc', "Nazwa (A-Z)"), ('name_desc', "Nazwa (Z-A)"),
        ('size_desc', "Rozmiar (Największe)"), ('size_asc', "Rozmiar (Najmniejsze)"),
        ('date_asc', "Data (najstarsze)"), ('date_desc', "Data (najnowsze)")
    ]

    def generate_view_panel() -> Panel:
        """Wewnętrzna funkcja renderująca aktualny widok na podstawie stanu."""
        title, headers, items_data = "", [], []
        
        if state['level'] == 'years':
            title = "Wybierz Rok"
            headers = ["Rok", "Liczba Plików"]
            sorted_years = sorted(year_counts.keys(), reverse=True)
            items_data = [(year, year_counts[year]) for year in sorted_years]
        elif state['level'] == 'months':
            title = f"Rok {state['year']} - Wybierz Miesiąc"
            headers = ["Miesiąc", "Liczba Plików"]
            months = sorted([m for y, m in month_counts if y == state['year']])
            items_data = [(m, month_counts[(state['year'], m)]) for m in months]
        elif state['level'] == 'days':
            title = f"Rok {state['year']} / {MONTH_NAMES.get(state['month'], '')} - Wybierz Dzień"
            headers = ["Dzień", "Liczba Plików"]
            days = sorted([d for y, m, d in day_counts if y == state['year'] and m == state['month']])
            items_data = [(d, day_counts[(state['year'], state['month'], d)]) for d in days]
        elif state['level'] == 'files':
            files_for_day = [e for e in all_media_data if e['dt'].year == state['year'] and e['dt'].month == state['month'] and e['dt'].day == state['day']]
            sort_key, sort_name = SORT_MODES[state['sort_mode_index']]
            reverse = 'desc' in sort_key
            sort_by = {'name': 'filename', 'size': 'size', 'date': 'dt'}.get(sort_key.split('_')[0], 'filename')
            files_for_day.sort(key=lambda x: x.get(sort_by, 0) or ('' if isinstance(x.get(sort_by), str) else 0), reverse=reverse)

            total_pages = max(1, (len(files_for_day) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
            state['page'] = max(0, min(state['page'], total_pages - 1))
            files_on_page = files_for_day[state['page'] * FILES_PER_PAGE : (state['page'] + 1) * FILES_PER_PAGE]
            
            headers = ["Nazwa Pliku", "Rozmiar", "Pełna Data"]
            items_data = [(f['filename'], format_size_for_display(f['size']), f['dt'].strftime('%Y-%m-%d %H:%M:%S')) for f in files_on_page]
            title = f"Pliki z {state['day']:02d}-{state['month']:02d}-{state['year']} | Strona {state['page'] + 1}/{total_pages} | Sort: {sort_name}"

        if state['index'] >= len(items_data): state['index'] = max(0, len(items_data) - 1)

        table = Table(title=title, title_justify="left")
        for header in headers: table.add_column(header, style="cyan", no_wrap=True)
        if len(headers) > 1: table.columns[-1].style="magenta"; table.columns[-1].justify="right"
        if len(headers) > 2: table.columns[-2].style="magenta"; table.columns[-2].justify="right"

        for i, item_tuple in enumerate(items_data):
            display_values = list(map(str, item_tuple))
            if state['level'] == 'months': display_values[0] = f"{int(display_values[0]):02d} - {MONTH_NAMES.get(int(display_values[0]), '')}"
            elif state['level'] == 'days':
                dt_obj = datetime(state['year'], state['month'], int(display_values[0]))
                display_values[0] = f"{int(display_values[0]):02d} - {DAY_NAMES.get(dt_obj.weekday(), '')}"
            table.add_row(*display_values, style="black on white" if i == state['index'] else "")

        nav_text = "[dim]Nawigacja: [bold]G/D[/] (wybór), [bold]ENTER[/] (wejdź), [bold]ESC/Q[/] (cofnij/wyjdź)"
        if state['level'] == 'files': nav_text += ", [bold]L/P[/] (strona), [bold]S[/] (sortuj)"
        
        return Panel(Group(table, Text.from_markup(nav_text, justify="center")), border_style="green")

    # Krok 3: Główna pętla interaktywna
    live_params = {"screen": True, "auto_refresh": False, "transient": True}
    with Live(generate_view_panel(), **live_params) as live:
        while True:
            live.update(generate_view_panel(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue

            if state['level'] == 'years': items_at_level = sorted(year_counts.keys(), reverse=True)
            elif state['level'] == 'months': items_at_level = sorted([m for y, m in month_counts if y == state['year']])
            elif state['level'] == 'days': items_at_level = sorted([d for y, m, d in day_counts if y == state['year'] and m == state['month']])
            elif state['level'] == 'files':
                files_for_day = [e for e in all_media_data if e['dt'].year == state['year'] and e['dt'].month == state['month'] and e['dt'].day == state['day']]
                items_at_level = files_for_day
            else: items_at_level = []
            
            total_items_on_page = len(items_at_level)
            if state['level'] == 'files':
                 total_pages = max(1, (total_items_on_page + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
                 total_items_on_page = len(items_at_level[state['page'] * FILES_PER_PAGE : (state['page'] + 1) * FILES_PER_PAGE])

            if key == "UP":
                if total_items_on_page > 0: state['index'] = (state['index'] - 1) % total_items_on_page
            elif key == "DOWN":
                if total_items_on_page > 0: state['index'] = (state['index'] + 1) % total_items_on_page
            elif key == "LEFT" and state['level'] == 'files':
                if state['page'] > 0: state['page'] -= 1; state['index'] = 0
            elif key == "RIGHT" and state['level'] == 'files':
                if state['page'] < total_pages - 1: state['page'] += 1; state['index'] = 0
            elif key.upper() == 'S' and state['level'] == 'files':
                state['sort_mode_index'] = (state['sort_mode_index'] + 1) % len(SORT_MODES); state['page'], state['index'] = 0, 0
            elif key == "ENTER" and items_at_level:
                if state['level'] == 'years':
                    state['year'] = items_at_level[state['index']]; state['level'] = 'months'; state['index'] = 0
                elif state['level'] == 'months':
                    state['month'] = items_at_level[state['index']]; state['level'] = 'days'; state['index'] = 0
                elif state['level'] == 'days':
                    state['day'] = items_at_level[state['index']]; state['level'] = 'files'; state['index'], state['page'] = 0, 0
            elif key.upper() in ["Q", "ESC"]:
                if state['level'] == 'files':
                    state['level'] = 'days'
                    days_in_month = sorted([d for y, m, d in day_counts if y == state['year'] and m == state['month']])
                    try: state['index'] = days_in_month.index(state['day'])
                    except ValueError: state['index'] = 0
                elif state['level'] == 'days':
                    state['level'] = 'months'
                    months_in_year = sorted([m for y, m in month_counts if y == state['year']])
                    try: state['index'] = months_in_year.index(state['month'])
                    except ValueError: state['index'] = 0
                elif state['level'] == 'months':
                    state['level'] = 'years'
                    sorted_years = sorted(year_counts.keys(), reverse=True)
                    try: state['index'] = sorted_years.index(state['year'])
                    except ValueError: state['index'] = 0
                elif state['level'] == 'years':
                    logger.info("Użytkownik wyszedł z osi czasu."); break


async def explore_metadata():
    """
    Uruchamia interaktywny eksplorator metadanych do filtrowania kolekcji.

    Ta zaawansowana funkcja pozwala użytkownikowi na dynamiczne budowanie
    złożonych zapytań poprzez dodawanie wielu filtrów. Umożliwia
    przeszukiwanie kolekcji według różnych kryteriów (tekstowych,
    numerycznych, list) i wyświetla pasujące wyniki w paginowanej tabeli.
    """
    console.clear()
    logger.info("Uruchamiam Eksplorator Metadanych...")
    console.print(Panel("🔎 Eksplorator Metadanych Kolekcji 🔎", expand=False, style="bold green"))

    # Krok 1: Asynchroniczne pobranie i walidacja danych
    with console.status("[cyan]Pobieranie danych do eksploracji...[/]"):
        all_media_data = await get_all_media_entries()
    if not all_media_data:
        logger.warning("Brak danych do eksploracji."); return
        
    # Krok 2: Inicjalizacja stanu eksploratora
    FILTERABLE_FIELDS = {
        '1': {'name': 'Rok', 'key': 'dt', 'type': 'numeric_year'},
        '2': {'name': 'Aparat/Telefon', 'key': 'Camera', 'type': 'text'},
        '3': {'name': 'Lokalizacja', 'key': 'Location', 'type': 'text'},
        '4': {'name': 'Rozmiar pliku (MB)', 'key': 'size', 'type': 'numeric_size'},
        '5': {'name': 'Otagowana osoba', 'key': 'TaggedPeople', 'type': 'list'},
        '6': {'name': 'Album', 'key': 'Albums', 'type': 'list'},
        '7': {'name': 'Opis', 'key': 'Description', 'type': 'text'},
    }
    OPERATORS = {'text': {'1': 'zawiera', '2': 'nie zawiera'}, 'numeric_year': {'1': '>', '2': '<', '3': '=='},
                 'numeric_size': {'1': '>', '2': '<'}, 'list': {'1': 'zawiera'}}
    
    active_filters = []
    search_results = None
    selected_menu_index = 0
    current_page = 0

    def add_filter_prompt():
        """Prowadzi użytkownika przez proces dodawania nowego filtra."""
        console.clear()
        field_prompt = "\n".join(f"  [cyan]{k}[/]. {v['name']}" for k, v in FILTERABLE_FIELDS.items())
        choice = Prompt.ask(f"Wybierz pole do filtrowania:\n{field_prompt}\n\n[dim]Wpisz numer (lub 'q' aby anulować):[/]")
        if choice.lower() == 'q' or choice not in FILTERABLE_FIELDS: return
        field = FILTERABLE_FIELDS[choice]
        ops = OPERATORS[field['type']]
        op_prompt = "\n".join(f"  [cyan]{k}[/]. {v}" for k, v in ops.items())
        op_choice = Prompt.ask(f"Wybierz operator:\n{op_prompt}")
        if op_choice not in ops: return
        op = ops[op_choice]
        value = Prompt.ask(f"Wpisz wartość dla warunku: [bold]{field['name']} {op}[/]")
        active_filters.append({'field': field, 'op': op, 'value': value})
        logger.info(f"Dodano nowy filtr: {field['name']} {op} '{value}'")

    def apply_filters():
        """Filtruje dane na podstawie aktywnych filtrów i aktualizuje stan."""
        nonlocal search_results
        logger.info(f"Stosowanie {len(active_filters)} filtrów na {len(all_media_data)} wpisach...")
        filtered_data = all_media_data[:]
        for f in active_filters:
            field, op, value = f['field'], f['op'], f['value']
            key, f_type = field['key'], field['type']
            def check_entry(entry):
                entry_val = entry.get(key)
                if entry_val is None: return False
                try:
                    if f_type == 'text': return (value.lower() in str(entry_val).lower()) if op == 'zawiera' else (value.lower() not in str(entry_val).lower())
                    elif f_type in ['numeric_year', 'numeric_size']:
                        val_num = float(value) * (1024**2 if f_type == 'numeric_size' else 1)
                        entry_num = entry_val.year if f_type == 'numeric_year' else entry_val
                        if entry_num is None: return False
                        return (op == '>' and entry_num > val_num) or (op == '<' and entry_num < val_num) or (op == '==' and entry_num == val_num)
                    elif f_type == 'list': return value in entry_val
                except (ValueError, TypeError): return False
                return False
            filtered_data = [entry for entry in filtered_data if check_entry(entry)]
        search_results = filtered_data
        logger.info(f"Filtrowanie zakończone. Znaleziono {len(search_results)} pasujących wyników.")

    menu_items = ["Dodaj filtr", "Uruchom wyszukiwanie", "Wyczyść filtry", "Wróć do menu"]

    def generate_full_view():
        """Pomocnicza funkcja do budowania całego widoku UI."""
        filter_texts = [Text.from_markup(f"{i+1}. {f['field']['name']} {f['op']} '{f['value']}'", style="yellow") for i, f in enumerate(active_filters)] if active_filters else [Text.from_markup("[dim]Brak aktywnych filtrów.[/dim]")]
        menu_text = Text(justify="center")
        for i, item in enumerate(menu_items):
            menu_text.append(f"  {item}  \n", style="bold black on white" if i == selected_menu_index else "")
        
        display_group = [Panel(Group(*filter_texts), title="Aktywne filtry", border_style="blue"), Panel(menu_text, title="Opcje", border_style="green")]
        
        if search_results is not None:
            total_pages = max(1, (len(search_results) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
            nonlocal current_page
            current_page = min(current_page, total_pages - 1)
            
            table = Table(title=f"Wyniki wyszukiwania ({len(search_results)} pasujących plików)")
            table.add_column("Plik", style="cyan"); table.add_column("Data", style="green"); table.add_column("Rozmiar", style="magenta", justify="right"); table.add_column("Aparat", style="yellow", no_wrap=True)
            start_idx, end_idx = current_page * FILES_PER_PAGE, (current_page + 1) * FILES_PER_PAGE
            for entry in search_results[start_idx:end_idx]:
                table.add_row(entry['filename'], entry['dt'].strftime('%Y-%m-%d'), format_size_for_display(entry['size']), str(entry.get('Camera', '')))
            
            page_info = Text(f"Strona {current_page + 1}/{total_pages}", justify="center")
            display_group.append(Panel(Group(table, page_info), title="Wyniki", border_style="yellow"))

        return Group(*display_group, Text.from_markup("\n[dim]Nawigacja: G/D, ENTER. Wyniki: L/P (strona). Q/ESC (wyjdź).[/dim]", justify="center"))

    # Krok 3: Główna pętla interaktywna
    live_params = {"screen": True, "auto_refresh": False, "transient": True}
    with Live(generate_full_view(), **live_params) as live:
        while True:
            live.update(generate_full_view(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue
        
            if key.upper() in ['Q', 'ESC']: break
            elif key == 'UP': selected_menu_index = (selected_menu_index - 1) % len(menu_items)
            elif key == 'DOWN': selected_menu_index = (selected_menu_index + 1) % len(menu_items)
            elif key == 'LEFT' and search_results is not None: current_page = max(0, current_page - 1)
            elif key == 'RIGHT' and search_results is not None:
                total_pages = max(1, (len(search_results) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
                current_page = min(total_pages - 1, current_page + 1)
            elif key == 'ENTER':
                choice = menu_items[selected_menu_index]
                if choice == "Dodaj filtr": live.stop(); add_filter_prompt(); search_results = None; live.start()
                elif choice == "Uruchom wyszukiwanie":
                    with console.status("[cyan]Filtrowanie danych...[/]"): apply_filters()
                    current_page = 0
                elif choice == "Wyczyść filtry":
                    active_filters.clear(); search_results = None; logger.info("Wyczyszczono wszystkie aktywne filtry.")
                elif choice == "Wróć do menu": break


async def _interactive_list_navigator(
    panel_title: str, table_title: str, column_header: str,
    data_key: str, all_media_data: list, no_data_message: str
):
    """
    Uruchamia uniwersalny, dwupoziomowy nawigator po liście z paginacją.

    Jest to reużywalny komponent UI, który może wyświetlić ranking dowolnych
    danych (np. aparatów, lokalizacji), a po wybraniu pozycji z rankingu,
    pokazać paginowaną i sortowalną listę plików z nią powiązanych.

    Args:
        panel_title (str): Tytuł całego narzędzia.
        table_title (str): Tytuł tabeli na pierwszym poziomie (rankingu).
        column_header (str): Nagłówek pierwszej kolumny w rankingu.
        data_key (str): Klucz w słowniku `entry`, po którym dane mają być grupowane.
        all_media_data (list): Wcześniej wczytana lista danych o mediach.
        no_data_message (str): Wiadomość do wyświetlenia, jeśli brakuje danych.
    """
    console.clear()
    logger.info(f"Uruchamiam interaktywny nawigator dla: {panel_title}")
    console.print(Panel(f"[bold green]{panel_title}[/]", expand=False))

    data_counter = Counter(entry[data_key] for entry in all_media_data if entry.get(data_key))
    if not data_counter:
        logger.warning(no_data_message)
        console.print(f"\n[yellow]{no_data_message}[/yellow]")
        return

    sorted_items_level1 = data_counter.most_common()
    
    # Inicjalizacja stanu
    state = {'level': 1, 'item_level1': None, 'index': 0, 'page': 0, 'sort_idx': 0}
    SORT_MODES = [
        ('name_asc', "Nazwa (A-Z)"), ('name_desc', "Nazwa (Z-A)"),
        ('size_desc', "Rozmiar (Największe)"), ('size_asc', "Rozmiar (Najmniejsze)"),
        ('date_desc', "Data (najnowsze)"), ('date_asc', "Data (najstarsze)")
    ]

    def generate_list_view() -> Panel:
        """Wewnętrzna funkcja renderująca aktualny widok na podstawie stanu."""
        # Poziom 1: Ranking (np. lista aparatów)
        if state['level'] == 1:
            total_pages = max(1, (len(sorted_items_level1) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
            state['page'] = max(0, min(state['page'], total_pages - 1))
            items_on_page = sorted_items_level1[state['page'] * FILES_PER_PAGE : (state['page'] + 1) * FILES_PER_PAGE]
            if state['index'] >= len(items_on_page): state['index'] = max(0, len(items_on_page) - 1)
            
            title = f"{table_title}\n[dim]Strona {state['page'] + 1}/{total_pages}[/dim]"
            table = Table(title=title, title_justify="left")
            table.add_column(column_header, style="cyan"); table.add_column("Liczba Zdjęć", style="magenta", justify="right")
            for i, (item, count) in enumerate(items_on_page):
                table.add_row(str(item), str(count), style="black on white" if i == state['index'] else "")
        
        # Poziom 2: Lista plików dla wybranej pozycji
        else:
            files_for_item = [e for e in all_media_data if e.get(data_key) == state['item_level1']]
            sort_key, sort_name = SORT_MODES[state['sort_idx']]
            reverse = 'desc' in sort_key
            sort_by = {'name': 'filename', 'size': 'size', 'date': 'dt'}.get(sort_key.split('_')[0], 'filename')
            files_for_item.sort(key=lambda x: x.get(sort_by, 0) or ('' if isinstance(x.get(sort_by), str) else 0), reverse=reverse)
            
            total_pages = max(1, (len(files_for_item) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
            state['page'] = max(0, min(state['page'], total_pages - 1))
            items_on_page = files_for_item[state['page'] * FILES_PER_PAGE : (state['page'] + 1) * FILES_PER_PAGE]
            if state['index'] >= len(items_on_page): state['index'] = max(0, len(items_on_page) - 1)
            
            title = f"Pliki dla: [bold cyan]{state['item_level1']}[/bold cyan]\n[dim]Strona {state['page'] + 1}/{total_pages} | Sort: {sort_name}[/dim]"
            table = Table(title=title, title_justify="left")
            table.add_column("Plik", style="cyan"); table.add_column("Data", style="green"); table.add_column("Rozmiar", style="magenta", justify="right")
            for i, entry in enumerate(items_on_page):
                table.add_row(entry['filename'], entry['dt'].strftime('%Y-%m-%d'), format_size_for_display(entry['size']), style="black on white" if i == state['index'] else "")

        nav_parts = ["[bold]G/D[/], [bold]L/P[/] (strona),"]
        if state['level'] == 1: nav_parts.append("[bold]ENTER[/] (pokaż pliki),")
        else: nav_parts.append("[bold]S[/] (sortuj), [bold]ESC[/] (cofnij),")
        nav_parts.append("[bold]Q[/] (wyjdź)")
        info_markup = f"[dim]Nawigacja: {' '.join(nav_parts)}[/dim]"
        return Panel(Group(table, Text.from_markup(info_markup, justify="center")), border_style="green")

    # Główna pętla interaktywna
    live_params = {"screen": True, "auto_refresh": False, "transient": True}
    with Live(generate_list_view(), **live_params) as live:
        while True:
            live.update(generate_list_view(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue

            # Logika nawigacji
            if state['level'] == 1:
                items_on_level = sorted_items_level1
            else:
                items_on_level = [e for e in all_media_data if e.get(data_key) == state['item_level1']]

            total_pages = max(1, (len(items_on_level) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
            items_on_page = items_on_level[state['page'] * FILES_PER_PAGE : (state['page'] + 1) * FILES_PER_PAGE]

            if key.upper() == 'Q': break
            if key == 'ESC' and state['level'] == 2:
                state.update({'level': 1, 'page': 0, 'index': 0})
                continue

            if key == "UP":
                if len(items_on_page) > 0: state['index'] = (state['index'] - 1) % len(items_on_page)
            elif key == "DOWN":
                if len(items_on_page) > 0: state['index'] = (state['index'] + 1) % len(items_on_page)
            elif key == "LEFT":
                if state['page'] > 0: state['page'] -= 1; state['index'] = 0
            elif key == "RIGHT":
                if state['page'] < total_pages - 1: state['page'] += 1; state['index'] = 0
            elif key.upper() == 'S' and state['level'] == 2:
                state['sort_idx'] = (state['sort_idx'] + 1) % len(SORT_MODES)
                state['page'], state['index'] = 0, 0
            elif key == 'ENTER' and state['level'] == 1 and items_on_page:
                state['item_level1'] = items_on_page[state['index']][0]
                state.update({'level': 2, 'page': 0, 'index': 0})


async def show_camera_stats():
    """
    Uruchamia interaktywny eksplorator modeli aparatów/telefonów.

    Funkcja ta najpierw asynchronicznie wczytuje wszystkie dane o mediach,
    a następnie wywołuje uniwersalny nawigator `_interactive_list_navigator`
    z parametrami skonfigurowanymi do wyświetlania rankingu aparatów.
    """
    # Krok 1: Asynchronicznie pobierz dane
    with console.status("[cyan]Pobieranie danych do rankingu aparatów...[/]"):
        all_media_data = await get_all_media_entries()

    # Krok 2: Uruchom uniwersalny nawigator z odpowiednimi parametrami
    await _interactive_list_navigator(
        panel_title="Interaktywny Ranking Sprzętów",
        table_title="Najczęściej Używane Aparaty i Telefony",
        column_header="Model Sprzętu",
        data_key='Camera',
        all_media_data=all_media_data,
        no_data_message="Brak danych o aparatach do analizy."
    )


async def show_location_stats():
    """
    Uruchamia interaktywny eksplorator lokalizacji.

    Funkcja ta najpierw asynchronicznie wczytuje wszystkie dane o mediach,
    a następnie wywołuje uniwersalny nawigator `_interactive_list_navigator`
    z parametrami skonfigurowanymi do wyświetlania rankingu lokalizacji.
    """
    # Krok 1: Asynchronicznie pobierz dane
    with console.status("[cyan]Pobieranie danych do rankingu lokalizacji...[/]"):
        all_media_data = await get_all_media_entries()

    # Krok 2: Uruchom uniwersalny nawigator z odpowiednimi parametrami
    await _interactive_list_navigator(
        panel_title="Interaktywny Eksplorator Lokalizacji",
        table_title="Najczęstsze Lokalizacje",
        column_header="Lokalizacja",
        data_key='Location',
        all_media_data=all_media_data,
        no_data_message="Brak danych o lokalizacjach do analizy."
    )
