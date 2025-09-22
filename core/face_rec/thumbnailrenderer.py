# W pliku core/utils.py (dodaj ten kod na końcu pliku)

# --- NOWY KOMPONENT DO RENDEROWANIA OBRAZÓW W TERMINALU ---
from rich.console import Console, ConsoleOptions, RenderResult
from rich.text import Text
from pathlib import Path
from typing import Optional, Tuple, Any
from io import BytesIO
import sys
import asyncio

# --- Importy specyficzne dla systemu (dla get_key) ---
try:
    import termios
    import tty
    IS_POSIX = True
except ImportError:
    IS_POSIX = False

# --- Playwright (dla type hintów) ---

try:
    import timg
    TIMG_AVAILABLE = True
except ImportError:
    TIMG_AVAILABLE = False

from rich.panel import Panel
from rich.live import Live
from rich.align import Align
	
class ThumbnailRenderer:
    """
    Klasa renderująca miniaturę obrazu, kompatybilna z `rich.Live`.
    Używa `timg` do wyświetlania grafiki, z fallbackiem do tekstu, jeśli
    biblioteka jest niedostępna lub terminal nie wspiera grafiki.
    """
    def __init__(self, image_path: Optional[Path], max_size: Tuple[int, int] = (60, 30)):
        self.image_path = image_path
        self.max_size = max_size

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        if not TIMG_AVAILABLE or not self.image_path or not self.image_path.exists():
            yield Text("[Podgląd niedostępny]", justify="center", style="dim")
            return

        # Timg renderuje do stdout, więc musimy to przechwycić
        original_stdout = sys.stdout
        captured_output = BytesIO()
        sys.stdout = captured_output  # Przekieruj stdout

        try:
            # Stwórz obiekt renderujący
            obj = timg.Renderer()
            # Ustaw maksymalny rozmiar, aby nie zalać terminala
            obj.set_size(*self.max_size)
            # Renderuj obraz do naszego bufora
            obj.render_from_file(str(self.image_path))
        except Exception as e:
            # W razie błędu (np. nieobsługiwany terminal), przywróć stdout i zwróć tekst
            sys.stdout = original_stdout
            logger.warning(f"Błąd renderowania miniatury: {e}")
            yield Text("[Podgląd niedostępny]", justify="center", style="dim")
            return
        finally:
            # Zawsze przywracaj oryginalny stdout!
            sys.stdout = original_stdout

        # Pobierz wyrenderowany obraz jako tekst z escape codami
        output_str = captured_output.getvalue().decode('utf-8', errors='ignore')
        yield Text(output_str, justify="center")

async def create_interactive_menu(
    menu_items: list,
    title: str,
    subtitle: str = "",
    border_style: str = "blue"
) -> Any:
    """
    Tworzy i zarządza uniwersalnym, interaktywnym menu w terminalu.
    """
    try:
        selected_index = next(i for i, item in enumerate(menu_items) if item[1] is not None)
    except StopIteration:
        selected_index = 0

    def generate_panel(sel_idx: int) -> Panel:
        menu_text = Text(justify="center")
        for i, (text, action) in enumerate(menu_items):
            if action is None:
                menu_text.append(f"\n[dim bold]{text}[/dim bold]\n\n")
                continue
            style = "bold black on white" if i == sel_idx else ""
            prefix = "» " if i == sel_idx else "  "
            menu_text.append(Text.from_markup(f"{prefix}{text}\n", style=style))

        if subtitle:
            final_content = Group(
                Align.center(menu_text, vertical="middle"),
                Align.center(Text.from_markup(f"\n{subtitle}", style="dim"))
            )
        else:
            final_content = Align.center(menu_text, vertical="middle")
        return Panel(final_content, title=f"[bold]{title}[/bold]", border_style=border_style)

    with Live(generate_panel(selected_index), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_panel(selected_index), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue

            if key == "UP":
                original_index = selected_index
                # --- POCZĄTEK POPRAWKI ---
                while True:
                    selected_index = (selected_index - 1 + len(menu_items)) % len(menu_items)
                    if menu_items[selected_index][1] is not None:
                        break
                    if selected_index == original_index:
                        break
                # --- KONIEC POPRAWKI ---
            elif key == "DOWN":
                original_index = selected_index
                # --- POCZĄTEK POPRAWKI ---
                while True:
                    selected_index = (selected_index + 1) % len(menu_items)
                    if menu_items[selected_index][1] is not None:
                        break
                    if selected_index == original_index:
                        break
                # --- KONIEC POPRAWKI ---
            elif key.upper() in ["Q", "ESC"]:
                return None
            elif key == "ENTER":
                _, selected_value = menu_items[selected_index]
                return selected_value

def get_key() -> str | None:
    """
    Odczytuje pojedyncze naciśnięcie klawisza w terminalu.
    """
    if not IS_POSIX:
        try:
            line = input()
            return line[0].upper() if line else "ENTER"
        except Exception: return None
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        char = sys.stdin.read(1)
        if char == '\x1b':
            seq = sys.stdin.read(2)
            if seq == '[A': return "UP"
            if seq == '[B': return "DOWN"
            if seq == '[C': return "RIGHT"
            if seq == '[D': return "LEFT"
            return None
        elif char in ('\r', '\n'): return "ENTER"
        elif char == ' ': return ' '
        elif char.isalpha(): return char.upper()
        return char
    except Exception: return None
    finally: termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
