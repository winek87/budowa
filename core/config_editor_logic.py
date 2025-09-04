# -*- coding: utf-8 -*-
"""
Ten plik zawiera całą logikę dla w pełni interaktywnego edytora pliku `config.py`
zbudowanego w oparciu o `rich.live`. Pozwala na nawigację, edycję,
przełączanie opcji i zapisywanie zmian w konfiguracji.
"""

# ##############################################################################
# ===                            GŁÓWNE IMPORTY                              ===
# ##############################################################################

# --- Importy z Bibliotek Standardowych Pythona ---
import asyncio  # Umożliwia programowanie asynchroniczne, kluczowe dla operacji I/O
import sys  # Dostęp do funkcji systemowych, np. `sys.exit()` do zamykania aplikacji
import os  # Zapewnia funkcje do interakcji z systemem operacyjnym (nieużywane bezpośrednio, ale dobre dla kontekstu)
import re  # Umożliwia operacje na wyrażeniach regularnych, używane do parsowania pliku konfiguracyjnego
import json  # Służy do formatowania złożonych typów danych (listy, słowniki) podczas zapisu
import time  # Używane do tworzenia opóźnień (nieużywane bezpośrednio, ale zachowane dla spójności)
import logging  # Centralny system do zapisywania logów i informacji o działaniu programu
from pathlib import Path  # Nowoczesny sposób na pracę ze ścieżkami plików i folderów

# --- Importy specyficzne dla systemu (dla get_key) ---
# Te moduły są potrzebne do odczytywania pojedynczych naciśnięć klawiszy w terminalu
# bez konieczności czekania na wciśnięcie Enter. Działają tylko w systemach POSIX.
try:
    import termios
    import tty
    IS_POSIX = True
except ImportError:
    IS_POSIX = False

# --- IMPORTY Z BIBLIOTEKI `rich` ---
# `rich` to biblioteka, która odpowiada za cały piękny, interaktywny interfejs w terminalu.
from rich.console import Console  # Główny obiekt do wyświetlania treści w terminalu
from rich.panel import Panel  # Umożliwia tworzenie ramek i paneli z tytułami
from rich.text import Text  # Zaawansowany obiekt do stylizowania tekstu (kolory, pogrubienie itp.)
from rich.prompt import Prompt, Confirm  # Narzędzia do zadawania pytań użytkownikowi
from rich.align import Align  # Służy do wyrównywania elementów (np. centrowania)
from rich.live import Live  # Kluczowy komponent do tworzenia dynamicznie odświeżanych interfejsów
from rich.layout import Layout  # Umożliwia dzielenie ekranu na sekcje i budowanie złożonych widoków
from rich.table import Table  # Służy do tworzenia estetycznych tabel

# --- IMPORTY Z WŁASNYCH MODUŁÓW APLIKACJI ---
# Importujemy funkcje pomocnicze z innych części naszego projektu.
from .utils import create_interactive_menu, get_key

# Założenie: opisy konfiguracji zostaną przeniesione do osobnego pliku dla porządku
# Jeśli go nie masz, możesz na razie usunąć tę linię i linijkę z `CONFIG_DESCRIPTIONS` w `_edit_value`
from .config_descriptions import CONFIG_DESCRIPTIONS

# --- Konfiguracja i Inicjalizacja ---
console = Console(record=True)
logger = logging.getLogger(__name__)
CONFIG_FILE_PATH = Path(__file__).parent / "config.py"

# ##############################################################################
# ##############################################################################

def read_config() -> dict:
    """
    Odczytuje plik konfiguracyjny `core/config.py` i zwraca jego wartości jako słownik.

    Funkcja dynamicznie wykonuje kod z pliku konfiguracyjnego w bezpiecznym,
    izolowanym środowisku, a następnie filtruje i zwraca tylko te zmienne, które są
    zapisane wielkimi literami (zgodnie z konwencją dla stałych w Pythonie).
    Jest to kluczowa funkcja, która pozwala aplikacji na wczytanie wszystkich
    ustawień zdefiniowanych przez użytkownika.

    Returns:
        dict: Słownik zawierający pary {NAZWA_ZMIENNEJ: wartość} wczytane z pliku konfiguracyjnego.

    Raises:
        SystemExit: Przerywa działanie całej aplikacji, jeśli plik konfiguracyjny
                    nie zostanie znaleziony lub wystąpi błąd podczas jego odczytu,
                    ponieważ bez konfiguracji program nie może poprawnie funkcjonować.
    """
    # Logujemy informację o rozpoczęciu operacji, podając pełną ścieżkę do pliku.
    logger.info(f"Odczytuję plik konfiguracyjny z: {CONFIG_FILE_PATH}")

    # Pierwszym krokiem jest sprawdzenie, czy plik konfiguracyjny w ogóle istnieje.
    if not CONFIG_FILE_PATH.exists():
        # Jeśli plik nie istnieje, jest to błąd krytyczny.
        logger.critical(f"BŁĄD KRYTYCZNY: Plik konfiguracyjny '{CONFIG_FILE_PATH}' nie istnieje!")
        console.print(f"[bold red]BŁĄD: Plik konfiguracyjny '{CONFIG_FILE_PATH}' nie istnieje![/bold red]")
        # Zamykamy aplikację, ponieważ dalsze działanie jest niemożliwe.
        sys.exit(1)

    try:
        # Odczytujemy całą zawartość pliku konfiguracyjnego jako tekst.
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            config_code = f.read()

        # Tworzymy puste, "bezpieczne" środowisko (słownik), w którym zostanie wykonany kod z pliku.
        # Pozwala to uniknąć zanieczyszczenia globalnej przestrzeni nazw.
        execution_scope = {'__file__': str(CONFIG_FILE_PATH)}

        # Wykonujemy kod z pliku konfiguracyjnego w naszym bezpiecznym środowisku.
        # Po tej operacji, słownik `execution_scope` będzie zawierał wszystkie zmienne zdefiniowane w config.py.
        exec(config_code, execution_scope)

        # Filtrujemy słownik, aby wyodrębnić tylko te zmienne, które nas interesują
        # (zgodnie z konwencją, są to zmienne pisane wielkimi literami).
        config_values = {k: v for k, v in execution_scope.items() if k.isupper() and not k.startswith('__')}
        logger.info(f"Pomyślnie wczytano {len(config_values)} zmiennych konfiguracyjnych.")

        # Zwracamy gotowy słownik z wczytaną konfiguracją.
        return config_values

    except Exception as e:
        # Jeśli wystąpi jakikolwiek błąd podczas odczytu lub wykonania pliku,
        # jest to również błąd krytyczny.
        logger.critical(f"BŁĄD: Nie można odczytać lub sparsować pliku konfiguracyjnego: {e}", exc_info=True)
        console.print(f"[bold red]BŁĄD: Nie można odczytać pliku konfiguracyjnego: {e}[/bold red]")
        sys.exit(1)

# ##############################################################################
# ##############################################################################

def write_config(new_values: dict) -> bool:
    """
    Zapisuje zaktualizowane wartości do pliku `config.py`, starając się
    zachować oryginalne formatowanie, komentarze i strukturę.

    Jest to zaawansowana funkcja, która parsuje plik linia po linii,
    identyfikuje definicje zmiennych i zastępuje ich wartości nowymi,
    jednocześnie próbując zachować strukturę dla złożonych typów danych
    jak listy i słowniki.

    Args:
        new_values (dict): Słownik zawierający tylko te zmienne, które mają zostać
                           zaktualizowane, w formacie {NAZWA_ZMIENNEJ: nowa_wartość}.

    Returns:
        bool: True, jeśli zapis zakończył się sukcesem, w przeciwnym razie False.
    """
    logger.info("Rozpoczynam zapisywanie zmian do pliku konfiguracyjnego...")
    try:
        # Funkcja pomocnicza do rekurencyjnej konwersji obiektów Path na stringi,
        # co jest konieczne przed serializacją do formatu JSON.
        def convert_paths_to_strings(data):
            if isinstance(data, dict):
                return {k: convert_paths_to_strings(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [convert_paths_to_strings(i) for i in data]
            elif isinstance(data, Path):
                return str(data)
            else:
                return data

        # Tworzymy głęboką kopię i przekonwertowujemy wszystkie obiekty Path na stringi,
        # aby uniknąć błędów podczas zapisu.
        safe_new_values = convert_paths_to_strings(new_values)

        # Wczytujemy cały plik konfiguracyjny do pamięci jako listę linii.
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        new_lines = []
        # Wyrażenie regularne do identyfikacji linii, która rozpoczyna definicję zmiennej.
        variable_regex = re.compile(r'^(?P<indent>\s*)(?P<var_name>[A-Z_][A-Z0-9_]*)\s*=\s*.*')
        lines_iterator = iter(lines)

        for line in lines_iterator:
            match = variable_regex.match(line)
            # Sprawdzamy, czy linia pasuje do wzorca i czy nazwa zmiennej jest na naszej liście do aktualizacji.
            if match and (var_name := match.group('var_name')) in safe_new_values:
                original_indent = match.group('indent')
                new_value = safe_new_values[var_name]
                
                # Zachowujemy oryginalny komentarz, jeśli istniał na końcu linii.
                comment_part = ""
                if '#' in line:
                    comment_part = "  #" + line.split('#', 1)[1].strip()

                # Poprawnie formatujemy nową wartość w zależności od jej typu.
                if isinstance(new_value, str):
                    formatted_value = json.dumps(new_value, ensure_ascii=False)
                elif isinstance(new_value, list):
                    list_str = json.dumps(new_value, indent=4, ensure_ascii=False)
                    json_lines = list_str.splitlines()
                    # Poprawiamy wcięcia dla wieloliniowych list.
                    formatted_value = (json_lines[0] + "\n" + "\n".join([f"{original_indent}{l}" for l in json_lines[1:]]))
                elif isinstance(new_value, dict):
                    dict_str = json.dumps(new_value, indent=4, ensure_ascii=False)
                    json_lines = dict_str.splitlines()
                    # Poprawiamy wcięcia dla wieloliniowych słowników.
                    formatted_value = (json_lines[0] + "\n" + "\n".join([f"{original_indent}    {l}" for l in json_lines[1:]]))
                else: # Dla int, float, bool
                    formatted_value = str(new_value)

                new_lines.append(f"{original_indent}{var_name} = {formatted_value}{comment_part}\n")

                # Obsługa wieloliniowych definicji: jeśli zastępujemy listę lub słownik,
                # musimy pominąć stare linie z oryginalnego pliku.
                start_char, end_char = ('[', ']') if isinstance(new_value, list) else ('{', '}')
                brace_count = line.count(start_char) - line.count(end_char)
                if brace_count > 0:
                    for next_line in lines_iterator:
                        brace_count += next_line.count(start_char) - next_line.count(end_char)
                        if brace_count <= 0:
                            break
            else:
                # Jeśli linia nie jest definicją zmiennej do zmiany, przepisujemy ją bez modyfikacji.
                new_lines.append(line)

        # Zapisujemy nowo utworzoną zawartość z powrotem do pliku konfiguracyjnego.
        with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)

        logger.info("[bold green]Plik konfiguracyjny został pomyślnie zaktualizowany![/bold green]", extra={"markup": True})
        return True

    except Exception as e:
        logger.critical(f"Nie udało się zapisać pliku konfiguracyjnego: {e}", exc_info=True)
        console.print(f"[bold red]Błąd zapisu do pliku konfiguracyjnego: {e}[/bold red]")
        return False

# ##############################################################################
# ##############################################################################

def validate_path(path_str: str) -> tuple[bool, str]:
    """
    Sprawdza, czy podany ciąg znaków jest prawidłową ścieżką do pliku lub folderu.

    Ta funkcja walidacyjna jest używana w edytorze konfiguracji, aby upewnić się,
    że ścieżki wprowadzane przez użytkownika są składniowo poprawne i że program
    ma uprawnienia do utworzenia nadrzędnych folderów dla tej ścieżki.

    Args:
        path_str (str): Ciąg znaków do walidacji, reprezentujący ścieżkę.

    Returns:
        tuple[bool, str]: Krotka składająca się z dwóch elementów:
                          - bool: True, jeśli ścieżka jest prawidłowa, w przeciwnym razie False.
                          - str: Pusty ciąg znaków w przypadku sukcesu lub komunikat
                                 o błędzie w przypadku niepowodzenia.
    """
    # Krok 1: Sprawdź, czy podany string nie jest pusty lub nie składa się tylko z białych znaków.
    if not path_str.strip():
        return False, "Ścieżka nie może być pusta."

    try:
        # Krok 2: Użyj `pathlib.Path`, aby przekształcić string w obiekt ścieżki.
        # `expanduser()` automatycznie zamienia tyldę (~) na ścieżkę do folderu domowego użytkownika.
        p = Path(path_str).expanduser()

        # Krok 3: To jest sprytna część. Próbujemy utworzyć folder nadrzędny dla podanej ścieżki.
        # Jeśli ta operacja się powiedzie, oznacza to, że ścieżka jest składniowo poprawna
        # i mamy odpowiednie uprawnienia do zapisu w tej lokalizacji.
        # `parents=True` tworzy wszystkie brakujące foldery w hierarchii.
        # `exist_ok=True` zapobiega błędowi, jeśli folder już istnieje.
        p.parent.mkdir(parents=True, exist_ok=True)
        
        # Jeśli nie wystąpił żaden błąd, walidacja jest pomyślna.
        return True, ""

    except Exception as e:
        # Krok 4: Jeśli wystąpi jakikolwiek błąd (np. nieprawidłowe znaki w ścieżce,
        # brak uprawnień do zapisu), złap wyjątek i zwróć błąd.
        return False, f"Nieprawidłowa ścieżka lub brak uprawnień. Błąd: {e}"

# ##############################################################################
# ##############################################################################

def validate_url(url_str: str) -> tuple[bool, str]:
    """
    Sprawdza, czy podany ciąg znaków jest prawdopodobnym adresem URL.

    Ta funkcja walidacyjna jest używana w edytorze konfiguracji, aby upewnić się,
    że wartość wprowadzana przez użytkownika (np. dla `START_URL`) ma poprawny
    format. Walidacja jest celowo uproszczona i sprawdza tylko, czy string
    zaczyna się od "http", co jest wystarczające dla potrzeb tej aplikacji.

    Args:
        url_str (str): Ciąg znaków do walidacji.

    Returns:
        tuple[bool, str]: Krotka składająca się z dwóch elementów:
                          - bool: True, jeśli URL jest prawidłowy, w przeciwnym razie False.
                          - str: Pusty ciąg znaków w przypadku sukcesu lub komunikat
                                 o błędzie w przypadku niepowodzenia.
    """
    # Krok 1: Sprawdź, czy podany string, po usunięciu białych znaków z początku i końca,
    # zaczyna się od "http". Metoda `startswith()` jest tutaj idealna.
    is_valid = url_str.strip().startswith("http")

    # Krok 2: Zwróć wynik. Jeśli `is_valid` jest True, drugi element krotki (komunikat błędu)
    # będzie pusty. Jeśli jest False, zostanie zwrócony pomocniczy komunikat.
    return is_valid, "" if is_valid else "URL musi zaczynać się od 'http'."

# ##############################################################################
# ##############################################################################

def validate_positive_int(num_str: str) -> tuple[bool, str]:
    """
    Sprawdza, czy podany ciąg znaków jest liczbą całkowitą nieujemną (zero lub większą).

    Ta funkcja walidacyjna jest używana w edytorze konfiguracji do weryfikacji
    pól, które muszą zawierać wartości liczbowe, takie jak limity prób
    (`MAX_RETRIES`) czy czasy oczekiwania. Zabezpiecza to program przed błędami,
    które mogłyby wystąpić, gdyby w takim polu znalazł się tekst lub liczba ujemna.

    Args:
        num_str (str): Ciąg znaków do walidacji, wprowadzony przez użytkownika.

    Returns:
        tuple[bool, str]: Krotka składająca się z dwóch elementów:
                          - bool: True, jeśli string reprezentuje poprawną liczbę,
                                  w przeciwnym razie False.
                          - str: Pusty ciąg znaków w przypadku sukcesu lub komunikat
                                 o błędzie w przypadku niepowodzenia.
    """
    try:
        # Krok 1: Spróbuj przekonwertować podany ciąg znaków na liczbę całkowitą (int).
        # Jeśli ta operacja się nie uda (np. dla tekstu "abc"), Python rzuci wyjątek ValueError.
        value = int(num_str)

        # Krok 2: Jeśli konwersja się powiodła, sprawdź, czy liczba jest nieujemna (większa lub równa 0).
        if value >= 0:
            # Jeśli warunek jest spełniony, walidacja jest pomyślna.
            return True, ""
        else:
            # Jeśli liczba jest ujemna, zwróć błąd.
            return False, "Wartość musi być liczbą całkowitą, 0 lub większą."

    except ValueError:
        # Krok 3: Jeśli wystąpił błąd ValueError podczas konwersji, oznacza to,
        # że podany string nie jest prawidłową liczbą całkowitą. Zwróć błąd.
        return False, "To nie jest prawidłowa liczba całkowita."

# ##############################################################################
# ##############################################################################

def validate_path_or_empty(path_str: str) -> tuple[bool, str]:
    """
    Sprawdza, czy podana ścieżka jest prawidłowa LUB czy ciąg znaków jest pusty.

    Ta funkcja walidacyjna jest wariantem `validate_path`. Została stworzona
    dla tych opcji konfiguracyjnych, w których pusta wartość jest również
    akceptowalna (np. `AI_MODELS_CACHE_DIR`, gdzie pusty string oznacza
    użycie domyślnej lokalizacji).

    Args:
        path_str (str): Ciąg znaków do walidacji.

    Returns:
        tuple[bool, str]: Krotka składająca się z dwóch elementów:
                          - bool: True, jeśli string jest pusty lub reprezentuje
                                  prawidłową ścieżkę, w przeciwnym razie False.
                          - str: Pusty ciąg znaków w przypadku sukcesu lub komunikat
                                 o błędzie w przypadku niepowodzenia.
    """
    # Krok 1: Sprawdź, czy podany string po usunięciu białych znaków jest pusty.
    if not path_str.strip():
        # Jeśli tak, walidacja jest pomyślna.
        return True, ""

    # Krok 2: Jeśli string nie jest pusty, przekaż go do bardziej rygorystycznej
    # funkcji `validate_path`, aby sprawdziła jego poprawność jako ścieżki.
    # Dzięki temu unikamy powtarzania kodu.
    return validate_path(path_str)

# ##############################################################################
# ##############################################################################

def validate_not_empty(val_str: str) -> tuple[bool, str]:
    """
    Sprawdza, czy podany ciąg znaków nie jest pusty.

    Jest to jedna z najprostszych, ale i najważniejszych funkcji walidacyjnych.
    Używana jest w edytorze konfiguracji dla tych pól, które bezwzględnie
    muszą zawierać jakąś wartość (np. selektory CSS). Zabezpiecza to
    program przed błędami, które mogłyby wystąpić, gdyby kluczowe
    ustawienia były puste.

    Args:
        val_str (str): Ciąg znaków do walidacji, wprowadzony przez użytkownika.

    Returns:
        tuple[bool, str]: Krotka składająca się z dwóch elementów:
                          - bool: True, jeśli string zawiera znaki inne niż białe,
                                  w przeciwnym razie False.
                          - str: Pusty ciąg znaków w przypadku sukcesu lub komunikat
                                 o błędzie w przypadku niepowodzenia.
    """
    # Krok 1: Usuń białe znaki (spacje, tabulatory, nowe linie) z początku i końca stringa.
    # Krok 2: Przekonwertuj wynikowy string na wartość logiczną (bool).
    # W Pythonie, pusty string `""` jest konwertowany na `False`,
    # a jakikolwiek niepusty string (np. "abc") jest konwertowany na `True`.
    is_valid = bool(val_str.strip())

    # Krok 3: Zwróć wynik. Jeśli `is_valid` jest True, drugi element krotki (komunikat błędu)
    # będzie pusty. Jeśli jest False, zostanie zwrócony pomocniczy komunikat.
    return is_valid, "Wartość nie może być pusta."

# ##############################################################################
# ##############################################################################

async def _edit_value(config: dict, option: dict, live: Live) -> dict:
    """
    Uruchamia interaktywny proces edycji pojedynczej zmiennej konfiguracyjnej
    z wbudowaną walidacją danych wejściowych.
    """
    key = option.get("key")
    current_value = config.get(key)

    # Zatrzymujemy Live, aby prompt był czytelny i nie migotał
    live.stop()

    # Wyświetlamy pomocnicze informacje o edytowanej zmiennej
    console.print(f"\nEdytujesz: [bold cyan]{key}[/]")
    console.print(f"Obecna wartość: [yellow]{current_value}[/]")
    # Opisy zostaną dodane w późniejszym kroku
    console.print(f"[dim]{CONFIG_DESCRIPTIONS.get(key, 'Brak opisu.')}[/dim]")

    try:
        if "action" in option:
            # Dla specjalnych akcji jak edytor list
            config = await option["action"](config, key)
        elif "toggle_type" in option:
            # Dla przełączników (np. True/False)
            if option["toggle_type"] == "bool":
                config[key] = not config.get(key, False)
            elif option["toggle_type"] == "direction":
                config[key] = "ArrowRight" if config.get(key, "ArrowLeft") == "ArrowLeft" else "ArrowLeft"
        elif "predefined_choices" in option:
            # Dla opcji z predefiniowanej listy
            title = f"Wybierz nową wartość dla [bold]{option['prompt']}[/bold]"
            new_value = await _show_selection_dialog(title, option["predefined_choices"])
            if new_value is not None:
                config[key] = new_value
        else:
            # Dla wszystkich pozostałych (tekst, liczby, ścieżki) z walidacją
            validator = option.get("validator", lambda v: (True, ""))
            while True:
                new_value_str = Prompt.ask(f"Podaj nową wartość dla [bold]{option['prompt']}[/]", default=str(current_value), console=console)
                is_valid, error_message = validator(new_value_str)
                if is_valid:
                    # Próbujemy przekonwertować z powrotem na oryginalny typ (np. int)
                    original_type = type(config.get(key))
                    try:
                        if original_type == int:
                            config[key] = int(new_value_str)
                        elif original_type == float:
                            config[key] = float(new_value_str.replace(',', '.'))
                        else:
                            config[key] = new_value_str
                    except (ValueError, TypeError):
                        config[key] = new_value_str
                    break
                else:
                    console.print(f"[bold red]BŁĄD WALIDACJI: {error_message}[/bold red]")

        console.print(f"[bold green]✅ Wartość dla '{key}' została zaktualizowana (na razie w pamięci).[/bold green]")
        await asyncio.sleep(1.5)

    except (ValueError, TypeError) as e:
        console.print(f"[bold red]Anulowano lub wprowadzono nieprawidłową wartość: {e}[/]")
        await asyncio.sleep(1.5)

    # Wznawiamy Live
    live.start(refresh=True)
    return config
# ##############################################################################
# ##############################################################################

def generate_layout(menu_items: list, selected_index: int, config: dict, scroll_offset: int, visible_lines: int) -> Layout:
    """
    Tworzy i zwraca pełny, dynamiczny układ interfejsu edytora konfiguracji.

    Ta funkcja jest wywoływana w pętli `rich.live`, aby odświeżać widok
    po każdej akcji użytkownika (np. naciśnięciu strzałki). Dynamicznie
    renderuje listę opcji, podświetla aktualnie wybraną pozycję i implementuje
    mechanizm przewijania, jeśli lista opcji jest dłuższa niż ekran.

    Args:
        menu_items (list): Pełna lista słowników, gdzie każdy słownik reprezentuje
                           jedną opcję w menu edytora.
        selected_index (int): Indeks aktualnie wybranej opcji na liście.
        config (dict): Aktualny słownik z wczytanymi wartościami konfiguracji,
                       używany do wyświetlania bieżących ustawień.
        scroll_offset (int): Indeks, od którego rozpoczyna się "okno" widocznych
                             linii na ekranie. Używane do przewijania.
        visible_lines (int): Liczba opcji, która ma być jednocześnie widoczna na ekranie.

    Returns:
        Layout: Kompletny, gotowy do wyświetlenia obiekt `rich.layout.Layout`,
                zawierający wszystkie elementy interfejsu.
    """
    # --- Nagłówek ---
    header = Panel(
        "Interaktywny Edytor Konfiguracji v21.0",
        style="bold blue",
        subtitle="[dim]Użyj strzałek GÓRA/DÓŁ do nawigacji.[/dim]"
    )

    # --- Główna treść (lista opcji) ---
    content_text = Text()
    current_section = None

    # Obliczamy, który fragment pełnej listy opcji ma być w tej chwili widoczny.
    start_index = scroll_offset
    end_index = scroll_offset + visible_lines

    # Iterujemy tylko po widocznych elementach, co jest wydajne.
    for i, item in enumerate(menu_items[start_index:end_index]):
        global_index = start_index + i
        is_selected = (global_index == selected_index)

        # Wyświetlamy nagłówki sekcji, aby pogrupować opcje.
        if "section" in item and item["section"] != current_section:
            current_section = item["section"]
            content_text.append(f"\n--- {current_section} ---\n", style="bold yellow")

        # Tworzymy pojedynczą linię menu. Jeśli jest zaznaczona, dostaje tło.
        line = Text(style="on bright_black" if is_selected else "")

        # Dodajemy wskaźnik » dla zaznaczonej opcji.
        prefix = "» " if is_selected else "  "
        toggle_indicator = " ⇄" if "toggle_type" in item else ""
        
        line.append(Text(f"{prefix}{global_index+1:>2}. ", style="bold yellow" if is_selected else "default"))
        line.append(Text(item['prompt'], style="bold white" if is_selected else "default"))
        line.append(f"{toggle_indicator}: ")

        # Wyświetlamy aktualną wartość opcji, kolorując ją w zależności od typu.
        if "action" in item:
            line.append("(wejdź, aby zarządzać...)", style="dim")
        else:
            value = config.get(item['key'], 'BRAK DANYCH')
            value_str = str(value)
            value_color = "white" if is_selected else "cyan"

            if isinstance(value, bool):
                value_color = "bold green" if value else "bold red"
            elif isinstance(value, list):
                value_str = f"[{len(value)} elementów]"
            elif item.get("toggle_type") in ["direction", "metadata"]:
                value_color = "bold magenta"
            elif isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
                value_color = "bright_blue"

            # Skracamy zbyt długie wartości, aby nie psuły layoutu.
            if len(value_str) > 50:
                value_str = value_str[:47] + "..."
            
            line.append(value_str, style=value_color)
        
        content_text.append(line)
        content_text.append("\n")

    # --- Stopka ---
    scroll_indicator = ""
    # Obliczamy i wyświetlamy wskaźnik przewijania, jeśli lista jest długa.
    if len(menu_items) > visible_lines:
        percentage = (scroll_offset / (len(menu_items) - visible_lines)) * 100 if len(menu_items) > visible_lines else 0
        scroll_indicator = f"| Przewijanie: {percentage:.0f}%"
    
    footer = Align.center(
        Text(f"Nawigacja: Strzałki | Wybór/Przełącz: Enter | Zapisz: S | Wyjdź: Q {scroll_indicator}", style="bold dim")
    )

    # --- Składamy wszystko w jeden layout ---
    layout = Layout()
    layout.split_column(
        Layout(header, size=3),
        Layout(Panel(content_text, border_style="green", title="Opcje Konfiguracyjne")),
        Layout(footer, size=1)
    )
    return layout

# ##############################################################################
# ##############################################################################

async def manage_browser_args(config: dict, config_key: str) -> dict:
    """
    Wyświetla i zarządza dedykowanym podmenu do edycji argumentów startowych przeglądarki.

    Ta funkcja jest wywoływana z głównego edytora, gdy użytkownik chce zmodyfikować
    listę argumentów w `BROWSER_ARGS`. Pozwala na dodawanie nowych argumentów,
    usuwanie istniejących i zapisywanie zmian, a wszystko to w przyjaznym,
    interaktywnym interfejsie.

    Args:
        config (dict): Główny słownik z całą wczytaną konfiguracją aplikacji.
        config_key (str): Klucz w słowniku konfiguracyjnym, który ma być edytowany
                          (w tym przypadku zawsze będzie to "BROWSER_ARGS").

    Returns:
        dict: Zwraca zaktualizowany (lub niezmieniony, jeśli użytkownik anulował)
              słownik konfiguracyjny.
    """
    # Pobieramy aktualnie wybrany typ przeglądarki (np. "chromium"), aby edytować
    # argumenty dla właściwej przeglądarki.
    browser_type = config.get("BROWSER_TYPE", "chromium")

    # Tworzymy lokalną, bezpieczną kopię listy argumentów do edycji.
    # Dzięki temu, jeśli użytkownik anuluje zmiany, oryginał pozostanie nietknięty.
    local_browser_args = config.get(config_key, {}).get(browser_type, []).copy()
    
    prompt_title = f"Zarządzanie argumentami przeglądarki ([bold]{browser_type}[/bold])"

    # Uruchamiamy nieskończoną pętlę, która będzie wyświetlać menu, dopóki
    # użytkownik nie zdecyduje się zapisać lub anulować zmian.
    while True:
        console.clear()
        console.print(Panel(prompt_title, style="yellow", border_style="yellow"))

        # Wyświetlamy aktualną listę argumentów w tabeli.
        if not local_browser_args:
            console.print("\n[dim]Lista argumentów jest pusta.[/dim]")
        else:
            table = Table(title="Aktualne argumenty")
            table.add_column("Argument", style="cyan")
            for arg in local_browser_args:
                table.add_row(arg)
            console.print(table)

        # Definiujemy opcje dostępne w menu. Opcja usuwania jest wyłączana, jeśli lista jest pusta.
        menu_items = [
            ("Dodaj nowy argument", "add"),
            ("Usuń argument", "remove"),
            ("Zapisz zmiany i wróć", "save"),
            ("Anuluj zmiany i wróć", "cancel")
        ]
        if not local_browser_args:
            menu_items[1] = ("[dim]Usuń argument (brak)[/dim]", None)
        
        choice = await create_interactive_menu(menu_items, "Opcje")

        if choice in ["cancel", None]:
            # Jeśli użytkownik anuluje, zwracamy oryginalny, niezmieniony config.
            return config
        
        if choice == "save":
            # Jeśli użytkownik zapisuje, aktualizujemy główny słownik konfiguracyjny
            # i zwracamy go, kończąc pętlę.
            if config_key not in config:
                config[config_key] = {}
            config[config_key][browser_type] = local_browser_args
            console.print("[green]Zmiany zapisane.[/green]")
            await asyncio.sleep(1.5)
            return config
        
        elif choice == 'add':
            # Prosimy użytkownika o podanie nowego argumentu.
            new_arg = Prompt.ask("Podaj nowy argument (np. --start-fullscreen)").strip()
            # Prosta walidacja, czy argument ma poprawny format.
            if new_arg and new_arg.startswith('--'):
                local_browser_args.append(new_arg)
            else:
                console.print("[red]Nieprawidłowy format argumentu.[/red]")
                await asyncio.sleep(2)
                
        elif choice == 'remove':
            # Wyświetlamy kolejne menu, aby użytkownik mógł wybrać, który argument usunąć.
            arg_to_remove = await create_interactive_menu(
                [(arg, arg) for arg in local_browser_args], "Wybierz argument do usunięcia"
            )
            if arg_to_remove:
                local_browser_args.remove(arg_to_remove)

# ##############################################################################
# ##############################################################################

async def manage_list_config(config: dict, config_key: str) -> dict:
    """
    Wyświetla i zarządza dedykowanym podmenu do edycji listy stringów w konfiguracji.

    Ta generyczna funkcja jest wywoływana z głównego edytora dla tych opcji,
    które są listami (np. lista folderów do skanowania). Pozwala na dodawanie
    nowych wpisów, usuwanie istniejących i zapisywanie zmian w interaktywny sposób.

    Args:
        config (dict): Główny słownik z całą wczytaną konfiguracją aplikacji.
        config_key (str): Klucz w słowniku konfiguracyjnym, który przechowuje listę
                          do edycji (np. "LOCAL_SCANNER_DIRECTORIES").

    Returns:
        dict: Zwraca zaktualizowany (lub niezmieniony, jeśli użytkownik anulował)
              słownik konfiguracyjny.
    """
    # Tworzymy lokalną, bezpieczną kopię listy do edycji.
    # Dzięki temu, jeśli użytkownik anuluje zmiany, oryginał pozostanie nietknięty.
    local_list = config.get(config_key, []).copy()
    
    prompt_title = f"Zarządzanie listą '{config_key}'"

    # Uruchamiamy nieskończoną pętlę, która będzie wyświetlać menu, dopóki
    # użytkownik nie zdecyduje się zapisać lub anulować zmian.
    while True:
        console.clear()
        console.print(Panel(prompt_title, style="yellow", border_style="yellow"))

        # Wyświetlamy aktualną zawartość listy w tabeli.
        if not local_list:
            console.print("\n[dim]Lista jest pusta.[/dim]")
        else:
            table = Table(title="Aktualne wpisy")
            table.add_column("Wpis", style="cyan")
            for item in local_list:
                table.add_row(item)
            console.print(table)

        # Definiujemy opcje dostępne w menu. Opcja usuwania jest wyłączana, jeśli lista jest pusta.
        menu_items = [
            ("Dodaj nowy wpis", "add"),
            ("Usuń wpis", "remove"),
            ("Zapisz zmiany i wróć", "save"),
            ("Anuluj zmiany i wróć", "cancel")
        ]
        if not local_list:
            menu_items[1] = ("[dim]Usuń wpis (brak)[/dim]", None)
        
        choice = await create_interactive_menu(menu_items, "Opcje")

        if choice in ["cancel", None]:
            # Jeśli użytkownik anuluje, zwracamy oryginalny, niezmieniony config.
            return config
        
        if choice == "save":
            # Jeśli użytkownik zapisuje, aktualizujemy główny słownik konfiguracyjny,
            # sortując listę dla porządku, i zwracamy go, kończąc pętlę.
            config[config_key] = sorted(local_list)
            console.print("[green]Zmiany zapisane.[/green]")
            await asyncio.sleep(1.5)
            return config
            
        elif choice == 'add':
            # Prosimy użytkownika o podanie nowego wpisu do listy.
            new_item = Prompt.ask("Podaj nowy wpis").strip()
            if new_item:
                local_list.append(new_item)
            else:
                logger.warning("Pusty wpis nie został dodany.")
                await asyncio.sleep(1.5)
                
        elif choice == 'remove':
            # Wyświetlamy kolejne menu, aby użytkownik mógł wybrać, który wpis usunąć.
            item_to_remove = await create_interactive_menu(
                [(item, item) for item in local_list], "Wybierz wpis do usunięcia"
            )
            if item_to_remove:
                local_list.remove(item_to_remove)

# ##############################################################################
# ##############################################################################

async def _show_selection_dialog(title: str, options: list) -> str | None:
    """
    Wyświetla uniwersalne, asynchroniczne okno dialogowe do wyboru opcji z listy.

    Ta funkcja tworzy dynamiczny, interaktywny interfejs za pomocą `rich.Live`,
    który pozwala użytkownikowi na nawigację po liście opcji za pomocą strzałek
    i dokonanie wyboru klawiszem Enter.

    Args:
        title (str): Tytuł, który zostanie wyświetlony na górze panelu dialogowego.
        options (list): Lista stringów reprezentujących opcje do wyboru.

    Returns:
        str | None: Zwraca wybrany przez użytkownika string z listy opcji,
                    lub `None`, jeśli użytkownik anulował operację (np. klawiszem Q).
    """
    logger.debug(f"Wyświetlam okno dialogowe wyboru: '{title}'")
    # Inicjalizujemy stan: domyślnie zaznaczona jest pierwsza opcja.
    selected_option_index = 0

    def generate_dialog_panel(selected_idx: int) -> Panel:
        """
        Wewnętrzna funkcja pomocnicza, która renderuje wygląd panelu
        dla aktualnie zaznaczonej opcji.
        """
        text = Text(justify="center")
        # Iterujemy po wszystkich dostępnych opcjach, aby je wyświetlić.
        for i, option in enumerate(options):
            # Jeśli indeks opcji zgadza się z aktualnie wybranym indeksem,
            # podświetlamy ją, stosując styl "odwróconych kolorów".
            style = "bold black on white" if i == selected_idx else "default"
            text.append(f" {option} \n", style=style)
        
        # Zwracamy gotowy do wyświetlenia panel `rich`.
        return Panel(Align.center(text, vertical="middle"), title=title, border_style="yellow")

    # Używamy `rich.Live` jako menedżera kontekstu, co pozwala na ciągłe
    # odświeżanie fragmentu ekranu bez "mrugania" całego terminala.
    with Live(generate_dialog_panel(selected_option_index), screen=True, auto_refresh=False, transient=True) as live:
        # Uruchamiamy nieskończoną pętlę, która czeka na akcję użytkownika.
        while True:
            # Odświeżamy widok na ekranie.
            live.update(generate_dialog_panel(selected_option_index), refresh=True)
            
            # Czekamy asynchronicznie na naciśnięcie klawisza.
            key = await asyncio.to_thread(get_key)
            if not key:
                continue

            # Obsługujemy nawigację strzałkami w górę i w dół.
            if key == "UP":
                selected_option_index = (selected_option_index - 1 + len(options)) % len(options)
            elif key == "DOWN":
                selected_option_index = (selected_option_index + 1) % len(options)
            # Obsługujemy zatwierdzenie wyboru.
            elif key == "ENTER":
                return options[selected_option_index]
            # Obsługujemy anulowanie operacji.
            elif key.upper() in ["Q", "ESC"]:
                return None

# ##############################################################################
# ##############################################################################

async def run_config_editor_bak():
    """
    Uruchamia główną, asynchroniczną pętlę interaktywnego edytora konfiguracji.

    Ta funkcja jest centralnym punktem modułu. Jej zadania to:
    1.  Wczytanie aktualnej konfiguracji z pliku `config.py`.
    2.  Przygotowanie struktury menu na podstawie wczytanych zmiennych.
    3.  Uruchomienie pętli `rich.Live`, która dynamicznie renderuje interfejs.
    4.  W pętli, nasłuchiwanie na akcje użytkownika (nawigacja, edycja, zapis, wyjście).
    5.  Delegowanie logiki edycji do wyspecjalizowanych funkcji pomocniczych.
    6.  Zarządzanie zapisem zmian i wyjściem z edytora.
    """
    logger.info("Uruchamiam Interaktywny Edytor Konfiguracji...")
    # Wczytujemy aktualną konfigurację i tworzymy jej kopię do porównania przy wyjściu.
    config = read_config()
    original_config = config.copy()

    # Definiujemy pełną strukturę menu. Każdy słownik to jedna opcja w edytorze.
    # Ta struktura jest sercem edytora i decyduje, jakie opcje są dostępne i jak działają.
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

    # Uzupełnijmy pełną listę kluczy z Twojego pliku, aby mieć pewność, że jest kompletna
#    from .config_editor_logic import menu_items as all_menu_items_from_your_file
#    menu_items = all_menu_items_from_your_file

    # Inicjalizujemy stan interfejsu.
    selected_index, scroll_offset = 0, 0
    VISIBLE_LINES = 15

    # Uruchamiamy `rich.Live`, który będzie dynamicznie odświeżał nasz interfejs.
    with Live(generate_layout(menu_items, selected_index, config, scroll_offset, VISIBLE_LINES), screen=True, auto_refresh=False) as live:
        # Główna pętla, która czeka na akcje użytkownika.
        while True:
            # Odświeżamy widok na ekranie.
            live.update(generate_layout(menu_items, selected_index, config, scroll_offset, VISIBLE_LINES), refresh=True)
            key = await asyncio.to_thread(get_key) # Czekamy na naciśnięcie klawisza.
            if not key: continue

            # Obsługa nawigacji góra/dół.
            if key == "UP":
                selected_index = max(0, selected_index - 1)
                if selected_index < scroll_offset: scroll_offset = selected_index
            elif key == "DOWN":
                selected_index = min(len(menu_items) - 1, selected_index + 1)
                if selected_index >= scroll_offset + VISIBLE_LINES: scroll_offset = selected_index - VISIBLE_LINES + 1
            
            # Obsługa wyjścia z edytora.
            elif key.upper() == "Q":
                # Jeśli dokonano zmian, pytamy o potwierdzenie.
                if config != original_config:
                    live.stop()
                    if Confirm.ask("[yellow]Wykryto niezapisane zmiany. Wyjść bez zapisywania?[/]", default=False, console=console):
                        logger.warning("Zmiany w konfiguracji anulowane."); break
                    live.start(refresh=True)
                else: break
            
            # Obsługa zapisu.
            elif key.upper() == "S":
                live.stop()
                if await write_config(config): # Zmieniono na asynchroniczne
                    console.print("[bold green]Wszystkie zmiany zostały pomyślnie zapisane do pliku![/bold green]")
                    await asyncio.sleep(2)
                    break
                else:
                    Prompt.ask("[red]Naciśnij Enter, aby kontynuować edycję...[/]", console=console)
                    live.start(refresh=True)
            
            # Obsługa akcji (Enter).
            elif key == "ENTER":
                selected_option = menu_items[selected_index]
                selected_key = selected_option.get("key")
                
                # Zamiast skomplikowanej logiki, wywołujemy jedną, czystą funkcję
                config = await _edit_value(config, selected_key, selected_option, live)

# ##############################################################################
# ##############################################################################

async def _edit_value_bak(all_vars: dict, key: str, option: dict, live: Live) -> dict:
    """Uruchamia interaktywny proces edycji pojedynczej zmiennej z walidacją."""
    current_value = all_vars.get(key)
    live.stop()

    console.print(f"\nEdytujesz: [bold cyan]{key}[/]")
    console.print(f"Obecna wartość: [yellow]{current_value}[/]")
    console.print(f"[dim]{CONFIG_DESCRIPTIONS.get(key, 'Brak opisu.')}[/dim]")

    try:
        if "action" in option:
            all_vars = await option["action"](all_vars, key)
        elif isinstance(current_value, bool):
            all_vars[key] = Confirm.ask("Wybierz nową wartość", default=current_value)
        elif isinstance(current_value, int):
            all_vars[key] = IntPrompt.ask(f"Podaj nową wartość", default=current_value)
        elif isinstance(current_value, float):
            all_vars[key] = FloatPrompt.ask(f"Podaj nową wartość", default=current_value)
        elif "predefined_choices" in option:
            new_val = await _show_selection_dialog(f"Wybierz dla {key}", option["predefined_choices"])
            if new_val is not None: all_vars[key] = new_val
        else: # Domyślnie edytuj jako string z walidatorem
            validator = option.get("validator", lambda v: (True, ""))
            while True:
                new_value_str = Prompt.ask(f"Podaj nową wartość", default=str(current_value))
                is_valid, error_message = validator(new_value_str)
                if is_valid:
                    all_vars[key] = new_value_str
                    break
                else: console.print(f"[bold red]BŁĄD: {error_message}[/bold red]")

        console.print(f"[bold green]✅ Wartość dla '{key}' została zmieniona (na razie w pamięci).[/bold green]")
        await asyncio.sleep(1.5)

    except (ValueError, TypeError) as e:
        console.print(f"[bold red]Anulowano lub wprowadzono nieprawidłową wartość: {e}[/]")
        await asyncio.sleep(1.5)
    
    live.start(refresh=True)
    return all_vars

# ##############################################################################
# ##############################################################################

async def run_config_editor():
    """
    Uruchamia główną, asynchroniczną pętlę interaktywnego edytora konfiguracji.
    """
    logger.info("Uruchamiam Interaktywny Edytor Konfiguracji...")
    config = read_config()
    original_config = config.copy()

    # Struktura menu pozostaje bez zmian, definiuje opcje i ich walidatory/akcje.
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
                if write_config(config):
                    console.print("[bold green]Wszystkie zmiany zostały pomyślnie zapisane do pliku![/bold green]")
                    await asyncio.sleep(2)
                    break
                else:
                    Prompt.ask("[red]Naciśnij Enter, aby kontynuować edycję...[/]", console=console)
                    live.start(refresh=True)
            elif key == "ENTER":
                # Zamiast wielkiego bloku if/elif, mamy teraz jedno, czyste wywołanie!
                selected_option = menu_items[selected_index]
                config = await _edit_value(config, selected_option, live)
