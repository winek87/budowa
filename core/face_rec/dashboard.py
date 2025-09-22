# Plik: core/face_rec/dashboard.py
# Moduł odpowiedzialny wyłącznie za renderowanie interfejsu
# graficznego (TUI) dla procesu indeksowania twarzy.

# --- GŁÓWNE IMPORTY ---
from typing import Dict, Any, Optional
from pathlib import Path
import logging
import shutil
import subprocess
from collections import deque

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Group
from rich.layout import Layout
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Table
from rich.text import Text

# --- INICJALIZACJA ---
logger = logging.getLogger(__name__)

# Sprawdzamy dostępność polecenia `timg` tylko raz, przy imporcie modułu.
TIMG_AVAILABLE = shutil.which("timg") is not None


# ##############################################################################
# ===                   SEKCJA 1: GENERATOR DASHBOARDU                       ===
# ##############################################################################

class ThumbnailRenderer:
    """
    Klasa pomocnicza do renderowania miniatur poprzez wywołanie
    zewnętrznego polecenia `timg` za pomocą `subprocess`.
    """
    def __init__(self, path: Optional[Path], max_size: tuple[int, int]):
        self.path = path
        self.max_size = max_size
        self.rendered_art = None
        self.last_rendered_path = None # Cache, aby nie renderować tego samego pliku w kółko

    def render(self):
        # Jeśli już raz wyrenderowaliśmy ten konkretny obraz, zwróć z cache'a
        if self.rendered_art is not None and self.path == self.last_rendered_path:
            return self.rendered_art

        self.last_rendered_path = self.path # Zaktualizuj ścieżkę w cache'u

        if not TIMG_AVAILABLE:
            self.rendered_art = Text("[polecenie 'timg' niedostępne]", style="dim")
            return self.rendered_art
        
        if self.path is None or not self.path.exists():
            self.rendered_art = Text("[Brak podglądu]", style="dim")
            return self.rendered_art

        try:
            # Przygotuj argumenty polecenia
            width = str(self.max_size[0])
            command = ["timg", str(self.path), "-ma24h", "-s", width]
            
            # Uruchom polecenie i przechwyć jego wynik
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True,
                encoding='utf-8'
            )
            
            self.rendered_art = Text.from_ansi(result.stdout)
            return self.rendered_art
            
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.warning(f"Błąd podczas renderowania miniatury dla '{self.path.name}': {e}")
            self.rendered_art = Text(f"[Błąd renderowania:\n{self.path.name}]", style="dim red")
            return self.rendered_art
        except Exception as e:
             logger.error(f"Nieoczekiwany błąd w ThumbnailRenderer: {e}", exc_info=True)
             self.rendered_art = Text("[Krytyczny błąd\nrenderowania]", style="bold red")
             return self.rendered_art

    def __rich_console__(self, console, options):
        yield self.render()

def generate_indexing_dashboard(progress: Progress, state: Dict[str, Any], settings: Dict[str, Any]) -> Panel:
    """
    Tworzy i zwraca kompletny, renderowalny panel dashboardu, włączając w to
    listę ostatnich plików ze znalezionymi twarzami.
    """
    stats = state['stats']
    action_logs = state['action_logs']
    info_logs = state['info_logs']
    error_logs = state['error_logs']
    current_activity = state['current_activity']
    current_path = state.get('current_path')
    # Pobieramy nową listę z ostatnimi znaleziskami
    recent_finds = state.get('recent_finds', deque(maxlen=5))

    # --- Panel 1: Podsumowanie i Ustawienia ---
    summary_and_settings_grid = Table.grid(expand=True)
    summary_and_settings_grid.add_column(ratio=1)
    summary_and_settings_grid.add_column(ratio=1)
    
    summary_table = Table.grid(expand=True)
    summary_table.add_column()
    summary_table.add_column(style="bold cyan", justify="right")
    summary_table.add_row("Przetworzono:", f"{stats['processed']} / {stats['total']}")
    summary_table.add_row("Znaleziono twarzy:", f"[green]{stats['faces_found']}[/green]")
    summary_table.add_row("Błędy plików:", f"[yellow]{stats['file_errors']}[/yellow]")
    summary_table.add_row("Błędy workera:", f"[red]{stats['worker_errors']}[/red]")
    
    settings_table = Table.grid(expand=True)
    settings_table.add_column(style="dim", width=12) # Zwiększono szerokość dla "Pewność (Hailo)"
    settings_table.add_column(style="bold")
    ram_limit = settings.get("FACE_REC_MEMORY_LIMIT_GB")
    ram_str = f"{ram_limit} GB" if ram_limit else "Brak"
    detector = settings.get('DETECTOR_BACKEND')
    if detector == 'hailo':
        conf = settings.get("HAILO_SETTINGS", {}).get("CONFIDENCE_THRESHOLD", 0.5)
        conf_label = "Pewność (Hailo):"
    else:
        conf = settings.get('DETECTION_CONFIDENCE_THRESHOLD', 0.95)
        conf_label = "Pewność:"

    settings_table.add_row("Detektor:", f"[cyan]{detector}[/]")
    settings_table.add_row(conf_label, f"[cyan]{conf:.0%}[/]")
    settings_table.add_row("Worker(y):", f"[cyan]{settings.get('NUM_AI_WORKERS', 1)}[/]")
    settings_table.add_row("Limit RAM:", f"[cyan]{ram_str}[/]")
    
    summary_and_settings_grid.add_row(
        Panel(summary_table, title="[bold blue]Postęp[/]"), 
        Panel(settings_table, title="[bold blue]Ustawienia Sesji[/]")
    )

    # --- NOWY PANEL: Ostatnie pliki, w których znaleziono twarze ---
    finds_table = Table.grid(expand=True, padding=(0, 2))
    finds_table.add_column(style="dim cyan")
    
    if recent_finds:
        for path in recent_finds:
            finds_table.add_row(f"✅ {str(path)}")
#            finds_table.add_row(f"✅ {Path(path).name}")
    else:
        finds_table.add_row("[dim]Oczekiwanie na pierwsze znalezisko...[/]")
        
    finds_panel = Panel(
        finds_table, 
        title="[bold magenta]Ostatnie Znaleziska[/]", 
        border_style="magenta",
        # Wysokość panelu dostosowuje się do liczby wpisów
        height=len(recent_finds) + 2 if recent_finds else 3 
    )
    # --- KONIEC NOWEGO PANELU ---

    # --- Panel 3: Aktywność i Podgląd ---
    activity_and_thumb_grid = Table.grid(expand=True)
    activity_and_thumb_grid.add_column(ratio=2)
    activity_and_thumb_grid.add_column(ratio=1)
    
    activity_panel = Panel(current_activity, title="[bold blue]Aktywność Workera AI[/]", border_style="blue", height=3)
    thumbnail = ThumbnailRenderer(current_path, max_size=(40, 20))
    
    activity_and_thumb_grid.add_row(
        activity_panel, 
        Panel(thumbnail, title="[bold blue]Podgląd[/]", height=10)
    )
    
    # --- Panel 4: Logi ---
    logs_grid = Table.grid(expand=True, padding=(0, 1))
    logs_grid.add_column(ratio=1)
    logs_grid.add_column(ratio=1)
    logs_grid.add_column(ratio=1)
    logs_grid.add_row(
        Panel(Group(*action_logs), title="[bold green]Logi Akcji[/]"), 
        Panel(Group(*info_logs), title="[bold blue]Logi Systemowe (Worker)[/]"), 
        Panel(Group(*error_logs), title="[bold red]Logi Błędów[/]")
    )
    
    # --- Główny Layout z nowym panelem ---
    main_layout = Layout()
    main_layout.split_column(
        Layout(progress, size=3),
        summary_and_settings_grid,
        finds_panel, # <-- DODANO NOWY PANEL
        activity_and_thumb_grid,
        logs_grid
    )
    
    return Panel(main_layout, title="[bold yellow]🔬 Dashboard Indeksowania Twarzy 🔬[/bold yellow]")
