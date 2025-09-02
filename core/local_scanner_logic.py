# -*- coding: utf-8 -*-

# plik: core/local_scanner_logic.py
# Wersja 5.0 - Hybrydowy, w pełni asynchroniczny Importer i Indekser Plików
#
# ##############################################################################
# ===            MODUŁ LOKALNEGO IMPORTERA I INDEKSERA PLIKÓW                ===
# ##############################################################################
#
# Ten moduł pozwala na importowanie plików multimedialnych z dowolnego
# folderu na dysku do bazy danych aplikacji. Działa w dwóch trybach:
#
# 1. IMPORT I ORGANIZACJA: Pliki są KOPIOWANE do centralnej biblioteki
#    i sortowane do folderów ROK/MIESIĄC.
#
# 2. SKANOWANIE I INDEKSOWANIE: Pliki NIE SĄ RUSZANE. Do bazy dodawany jest
#    jedynie wpis ("indeks") wskazujący na ich oryginalną lokalizację.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import logging
import shutil
from pathlib import Path
from functools import partial
from datetime import datetime

# --- Zależności zewnętrzne (opcjonalne) ---
try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.progress import Progress

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import LOCAL_SCANNER_DIRECTORIES, DOWNLOADS_DIR_BASE
from .database import add_local_file_entry
from .utils import create_interactive_menu, get_date_from_metadata, create_unique_filepath

# --- Inicjalizacja ---
console = Console(record=True)
logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {
    '.jpg', '.jpeg', '.png', '.heic', '.gif', '.webp', '.bmp',
    '.mp4', '.mov', '.avi', '.m4v', '.3gp', '.mkv'
}
# plik: core/local_scanner_logic.py

def _check_dependencies() -> bool:
    """
    Sprawdza, czy wszystkie zewnętrzne zależności tego modułu są dostępne.

    Główną zależnością jest program `exiftool` oraz biblioteka `pyexiftool`.
    W przypadku ich braku, wyświetla stosowny komunikat.

    Returns:
        bool: True, jeśli wszystkie zależności są spełnione, w przeciwnym razie False.
    """
    logger.debug("Sprawdzam zależności dla Lokalnego Importera...")
    if not EXIFTOOL_AVAILABLE:
        logger.critical("Brak kluczowej biblioteki 'pyexiftool'. Operacja została przerwana.")
        console.print(Panel(
            "[bold red]Błąd: Brak wymaganej biblioteki 'pyexiftool'![/bold red]\n\n"
            "To narzędzie wymaga tej biblioteki do odczytu metadanych z plików.\n"
            "Uruchom w terminalu: [cyan]pip install pyexiftool[/cyan]",
            title="Brak Zależności", border_style="red"
        ))
        return False

    logger.debug("Wszystkie zależności dla Lokalnego Importera są spełnione.")
    return True
# plik: core/local_scanner_logic.py

async def _perform_local_scan(folder_path: Path, import_mode: str):
    """
    Główna, asynchroniczna funkcja robocza: skanuje pliki, a następnie
    indeksuje je lub importuje do biblioteki, w zależności od wybranego trybu.

    Args:
        folder_path (Path): Ścieżka do folderu, który ma być przetworzony.
        import_mode (str): Tryb pracy ('copy' lub 'index').
    """
    logger.info(f"Rozpoczynam skanowanie folderu: {folder_path} (Tryb: {import_mode})")

    # Krok 1: Asynchroniczne wyszukiwanie plików
    with console.status(f"[cyan]Wyszukiwanie plików w '{folder_path}'...[/]"):
        def find_files():
            return [p for p in folder_path.rglob('*') if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS]
        files_to_process = await asyncio.to_thread(find_files)

    if not files_to_process:
        logger.warning(f"Nie znaleziono obsługiwanych plików w '{folder_path}'.")
        console.print(f"\n[yellow]Nie znaleziono obsługiwanych plików w '{folder_path}'.[/yellow]")
        return

    # Krok 2: Potwierdzenie od użytkownika
    console.print(f"\nZnaleziono [bold green]{len(files_to_process)}[/] pasujących plików.")
    if import_mode == 'copy':
        op_text = "skopiowane i zorganizowane w Twojej głównej bibliotece"
    else: # index
        op_text = "zaindeksowane (pliki pozostaną w oryginalnej lokalizacji)"
    console.print(f"[dim]Pliki zostaną {op_text}.[/dim]")
    if not Confirm.ask("[cyan]Czy chcesz kontynuować?[/]"):
        logger.warning("Operacja anulowana przez użytkownika."); return

    # Krok 3: Przetwarzanie plików
    imported_count, skipped_count, errors_count = 0, 0, 0
    library_base_path = Path(DOWNLOADS_DIR_BASE)
    loop = asyncio.get_running_loop()

    with Progress(console=console, transient=True) as progress:
        task_desc = "Importowanie..." if import_mode == 'copy' else "Indeksowanie..."
        task = progress.add_task(f"[green]{task_desc}[/green]", total=len(files_to_process))
        try:
            with exiftool.ExifToolHelper() as et:
                for source_path in files_to_process:
                    progress.update(task, description=f"Przetwarzanie: [dim]{source_path.name}[/dim]")
                    try:
                        metadata_list = await loop.run_in_executor(None, et.get_metadata, str(source_path))
                        if not metadata_list: errors_count += 1; continue
                        metadata = metadata_list[0]
                        
                        final_path = source_path # Domyślnie dla trybu 'index'
                        
                        # Logika dla trybu importu i organizacji
                        if import_mode == 'copy':
                            creation_date = await get_date_from_metadata(metadata) or datetime.now()
                            if not await get_date_from_metadata(metadata):
                                logger.warning(f"Brak daty dla pliku {source_path.name}. Używam dzisiejszej daty.")
                            
                            dest_dir = library_base_path / str(creation_date.year) / f"{creation_date.month:02d}"
                            await asyncio.to_thread(dest_dir.mkdir, parents=True, exist_ok=True)
                            final_path = create_unique_filepath(dest_dir, source_path.name)
                            await asyncio.to_thread(shutil.copy2, source_path, final_path)
                        
                        # Dodaj wpis do bazy
                        was_added = await add_local_file_entry(final_path, metadata)
                        if was_added: imported_count += 1
                        else:
                            skipped_count += 1
                            if import_mode == 'copy':
                                await asyncio.to_thread(final_path.unlink)
                    except Exception:
                        errors_count += 1; logger.error(f"Błąd importu pliku {source_path.name}", exc_info=True)
                    progress.update(task, advance=1)
        except FileNotFoundError:
             logger.critical("Nie znaleziono programu 'exiftool'. Import przerwany."); return

    # Krok 4: Podsumowanie
    op_text_done = "Zaimportowano" if import_mode == 'copy' else "Zaindeksowano"
    console.print(f"\n[bold green]✅ Proces zakończony![/]")
    console.print(f"  - {op_text_done}: [bold cyan]{imported_count}[/] nowych plików.")
    if skipped_count > 0: console.print(f"  - Pominięto: [bold yellow]{skipped_count}[/] plików (już istniały w bazie).")
    if errors_count > 0: console.print(f"  - Błędy: [bold red]{errors_count}[/]. Sprawdź logi.")
# plik: core/local_scanner_logic.py

async def scan_new_local_folder(import_mode: str):
    """
    Prowadzi użytkownika przez proces importu/indeksowania plików z nowego,
    jednorazowego folderu.

    Funkcja ta:
    1.  Prosi użytkownika o podanie pełnej ścieżki do folderu.
    2.  Weryfikuje, czy podana ścieżka jest prawidłowym, istniejącym folderem.
    3.  Uruchamia główną funkcję roboczą `_perform_local_scan` dla
        podanej ścieżki i w wybranym trybie.

    Args:
        import_mode (str): Tryb pracy ('copy' lub 'index').
    """
    logger.info(f"Uruchomiono opcję importu z nowego folderu (tryb: {import_mode}).")
    
    folder_path_str = Prompt.ask("\n[cyan]Podaj pełną ścieżkę do folderu, który chcesz przetworzyć[/]")
    
    try:
        if not folder_path_str.strip():
            logger.warning("Nie podano ścieżki. Anulowano.")
            return

        folder_path = Path(folder_path_str.strip()).expanduser().resolve()
        
        if not await asyncio.to_thread(folder_path.is_dir):
            logger.error(f"Podana ścieżka nie jest prawidłowym folderem: {folder_path}")
            console.print(f"\n[bold red]Błąd: Ścieżka '{folder_path}' nie istnieje lub nie jest folderem.[/bold red]")
            return
            
        logger.info(f"Użytkownik wybrał do przetworzenia folder: {folder_path}")
        await _perform_local_scan(folder_path, import_mode)
        
    except Exception as e:
        logger.error(f"Wystąpił nieoczekiwany błąd podczas próby importu z nowego folderu: {e}", exc_info=True)
        console.print(f"\n[bold red]Wystąpił błąd: {e}[/bold red]")
# plik: core/local_scanner_logic.py

async def show_paths_info():
    """
    Wyświetla informację o tym, gdzie zarządzać zapisanymi ścieżkami.

    Zgodnie z nową, bezpieczniejszą architekturą, lista zapisanych ścieżek
    jest zarządzana bezpośrednio w pliku `core/config.py`. Ta funkcja
    informuje o tym użytkownika.
    """
    logger.info("Wyświetlono informację o zarządzaniu zapisanymi ścieżkami.")
    console.clear()
    console.print(Panel(
        "Zarządzanie Zapisanymi Ścieżkami",
        style="bold blue",
        subtitle="[dim]Informacja[/dim]"
    ))
    
    info_text = (
        "Lista folderów do szybkiego importu jest teraz zarządzana "
        "bezpośrednio w pliku konfiguracyjnym.\n\n"
        "Aby dodać, usunąć lub zmodyfikować zapisane ścieżki, edytuj listę "
        "[bold cyan]LOCAL_SCANNER_DIRECTORIES[/bold cyan] w pliku:\n\n"
        f"[green]core/config.py[/green]"
    )
    
    console.print(Panel(info_text, padding=(1, 2)))
# plik: core/local_scanner_logic.py

async def run_local_scanner_menu():
    """
    Wyświetla i zarządza głównym, dynamicznym menu dla modułu skanera lokalnego.
    """
    logger.info("Uruchamiam menu Lokalnego Importera i Indeksera Plików.")
    
    while True:
        console.clear()
        
        menu_items = [("--- IMPORTUJ Z ZAPISANEJ LOKALIZACJI ---", None)]
        if not LOCAL_SCANNER_DIRECTORIES:
            menu_items.append(("[dim]Brak zapisanych lokalizacji w config.py[/dim]", None))
        
        for path_str in LOCAL_SCANNER_DIRECTORIES:
            menu_items.append((f"Przetwarzaj folder: {path_str}", Path(path_str)))

        menu_items.extend([
            ("--- INNE OPCJE ---", None),
            ("Przetwarzaj inny, jednorazowy folder...", "scan_new"),
            ("Jak zarządzać zapisanymi ścieżkami?", "show_info"),
            ("Wróć do menu głównego", "exit")
        ])

        selected_option = await create_interactive_menu(
            menu_items, "📂 Lokalny Importer i Indekser Plików 📂", border_style="blue"
        )
        
        if selected_option == "exit" or selected_option is None:
            logger.info("Zamykanie Lokalnego Importera."); break
        
        folder_to_process = None
        if isinstance(selected_option, Path):
            folder_to_process = selected_option
        elif selected_option == "scan_new":
            path_str = Prompt.ask("\n[cyan]Podaj pełną ścieżkę do nowego folderu[/]")
            if path_str: folder_to_process = Path(path_str.strip()).expanduser()
        elif selected_option == "show_info":
            await show_paths_info(); Prompt.ask("\n[bold]Naciśnij Enter...[/]"); continue
            
        if folder_to_process and await asyncio.to_thread(folder_to_process.is_dir):
            console.clear()
            console.print(Panel(f"Wybrano folder: [cyan]{folder_to_process}[/cyan]", style="blue"))
            
            mode_choice = await create_interactive_menu([
                ("Importuj i Organizuj (Kopiuj pliki)", "copy"),
                ("Skanuj i Indeksuj (Nie ruszaj plików)", "index"),
                ("Anuluj", "cancel")
            ], "Wybierz tryb pracy")
            
            if mode_choice and mode_choice != "cancel":
                if not _check_dependencies():
                    await asyncio.sleep(3); continue
                
                await _perform_local_scan(folder_to_process, mode_choice)
                Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter...[/]")
        elif folder_to_process:
            console.print(f"[red]Błąd: Ścieżka '{folder_to_process}' nie jest prawidłowym folderem.[/red]")
            await asyncio.sleep(3)
