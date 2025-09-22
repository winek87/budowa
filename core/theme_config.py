#
# Plik: theme_config.py (Wersja 2.1)
# Opis: Dodano style i ikony dla modułu Skanera.
#

from rich.console import Console
from rich.panel import Panel
from rich.style import Style
from rich.text import Text
from rich.theme import Theme

# Krok 1: Rozbudowana, spójna paleta kolorów i stylów.
custom_theme = Theme({
    # Style ogólne
    "info": "dim cyan",
    "warning": "yellow",
    "danger": "bold red",
    "error": "bold red on white",
    "success": "bold green",
    "subtle": "dim",
    "highlight": "bold yellow",
    "footer": "white on bright_black",

    # Style dla komponentów UI (Główne Menu)
    "header": "bold underline gold3",
    "title.main": "bold white on navy_blue",
    "panel.main": "blue",

    # Style dla Skanera (+++ DODANE +++)
    "title.scanner": "bold cyan",
    "panel.scanner": "cyan",
    "highlight.selected.scanner": "bold white on dark_cyan",

    # Style dla Paneli Informacyjnych
    "title.info": "bold magenta",
    "panel.info": "magenta",
    "panel.success": "green",
    "panel.warning": "yellow",
    "panel.danger": "red",

    # Domyślne podświetlenie menu
    "highlight.selected": "bold white on dark_blue",
})

# Krok 2: Centralizacja ikon.
class AppIcons:
    """Klasa przechowująca ikony aplikacji."""
    # Menu Główne (bez zmian)
    DOWNLOAD_CENTER = "🚀"
    SCANNER = "🔎"
    LOCAL_IMPORTER = "📦"
    SINGLE_URL = "🔗"
    AI_TAGGER = "🤖"
    FACE_RECOGNITION = "👨‍👩‍👧‍👦"
    ANALYTICS = "🔬"
    MAINTENANCE = "🛠️"
    ADVANCED = "⚙️"
    EXIT = "🚪"
    
    # Statusy
    INFO = "ℹ️"
    SUCCESS = "✅"
    WARNING = "⚠️"
    ERROR = "❌"
    LOADING = "⏳"
    QUESTION = "❓"
    DONE = "✔️"
    
    # Skaner (+++ DODANE +++)
    SCAN_ONLINE = "🌐"
    SCAN_COMPLETE = "🧩"
    FIX_LOCATION = "📁"
    FIX_NAMES = "✏️"
    RETRY_ERRORS = "🔁"
    REFRESH_ALL = "🔄"
    SCAN_FILE = "📄"
    WRITE_METADATA = "✍️"
    EXPORT = "📤"
    DIAGNOSTICS = "🔬"


# Krok 3: Spójne, reużywalne komponenty (bez zmian).
def create_styled_message(message_type: str, text: str) -> Text:
    icons = {"info": AppIcons.INFO, "success": AppIcons.SUCCESS, "warning": AppIcons.WARNING, "error": AppIcons.ERROR, "danger": AppIcons.ERROR}
    style = message_type
    icon = icons.get(message_type, AppIcons.INFO)
    return Text.from_markup(f"[{style}]{icon} {text}[/]", style=style)

def create_standard_panel(content, title: str, style: str = "info") -> Panel:
    border_style = f"panel.{style}"
    title_style = f"title.{style}" if f"title.{style}" in custom_theme.styles else "default"
    return Panel(content, title=f"[{title_style}]{title}[/]", border_style=border_style, padding=(1, 2), expand=True)

# Krok 4: Główna instancja konsoli.
console = Console(theme=custom_theme, record=True)
