# -*- coding: utf-8 -*-

# plik: core/integrity_validator_logic.py
# Wersja 9.0 - Pełna integracja z asynchronicznym modułem bazy danych
#
# ##############################################################################
# ===                        MODUŁ WALIDATORA INTEGRALNOŚCI                  ===
# ##############################################################################
#
# "Walidator Integralności" to zaawansowany zestaw narzędzi do "sprzątania"
# i weryfikacji pobranej kolekcji. Pozwala na:
#
#  - Obliczanie i zapisywanie sum kontrolnych (hash MD5) dla plików.
#  - Wyszukiwanie "duchów" (wpisów w bazie bez pliku na dysku) i "sierot"
#    (plików na dysku bez wpisu w bazie).
#  - Analizę spójności między datą w metadanych a lokalizacją pliku.
#  - Interaktywne zarządzanie duplikatami plików na podstawie ich zawartości.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import hashlib
import os
import json
import logging
import uuid
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from datetime import datetime

# --- Zależności zewnętrzne (opcjonalne) ---
try:
    import exiftool
    EXIFTOOL_AVAILABLE = True
except ImportError:
    EXIFTOOL_AVAILABLE = False

# --- Importy asynchroniczne ---
import aiosqlite

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text
from rich.live import Live
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.progress import Progress
from rich.align import Align
from rich.layout import Layout

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import DATABASE_FILE, DOWNLOADS_DIR_BASE, LOCAL_SCANNER_DIRECTORIES
from .utils import create_interactive_menu, _interactive_file_selector, get_key, _parse_metadata_for_display
# Importujemy nową, dedykowaną funkcję do zapisu plików lokalnych
from .database import add_local_file_entry

# --- Inicjalizacja i Konfiguracja Modułu ---
#console = Console(record=True)
console = Console()
logger = logging.getLogger(__name__)


# ##############################################################################
# ===           SEKCJA 1: FUNKCJE POMOCNICZE (ASYNC & EFFICIENT)             ===
# ##############################################################################

async def _calculate_hash_for_file(file_path: Path) -> Optional[str]:
    """
    Asynchronicznie i wydajnie oblicza hash MD5 dla podanego pliku.

    Funkcja ta czyta plik w małych kawałkach (8192 bajty), aby nie obciążać
    nadmiernie pamięci RAM. Po odczytaniu każdego kawałka, używa `await
    asyncio.sleep(0)`, aby oddać kontrolę do pętli zdarzeń, co zapobiega
    blokowaniu interfejsu użytkownika podczas hashowania dużych plików.

    Args:
        file_path (Path): Ścieżka do pliku, dla którego ma być obliczony hash.

    Returns:
        Optional[str]: Hash MD5 jako string heksadecymalny, lub None w przypadku
                       błędu odczytu pliku.
    """
    logger.debug(f"Rozpoczynam obliczanie hasha MD5 dla pliku '{file_path.name}'...")
    hasher = hashlib.md5()
    try:
        # Używamy `asyncio.to_thread`, aby cała operacja plikowa
        # (otwarcie, czytanie w pętli, zamknięcie) odbyła się w osobnym wątku.
        def read_and_hash():
            with open(file_path, "rb") as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)
            return hasher.hexdigest()

        file_hash = await asyncio.to_thread(read_and_hash)
        
        logger.debug(f"Obliczono hash dla '{file_path.name}': {file_hash}")
        return file_hash
        
    except (IOError, OSError) as e:
        logger.error(f"Błąd odczytu pliku '{file_path}' podczas hashowania: {e}", exc_info=True)
        return None


async def _get_all_disk_paths(root_dir: Path) -> Set[Path]:
    """
    Asynchronicznie i wydajnie skanuje podany folder i wszystkie jego
    podfoldery w poszukiwaniu plików.

    Używa `loop.run_in_executor`, aby cała, potencjalnie długa operacja
    skanowania dysku odbyła się w osobnym wątku, nie blokując głównej
    pętli `asyncio` i interfejsu użytkownika.

    Args:
        root_dir (Path): Główny folder do przeskanowania.

    Returns:
        Set[Path]: Zbiór ze wszystkimi znalezionymi, absolutnymi ścieżkami
                   do plików.
    """
    disk_paths: Set[Path] = set()
    logger.info(f"Rozpoczynam asynchroniczne skanowanie dysku w folderze: {root_dir}")
    
    # Pobierz aktualnie działającą pętlę zdarzeń
    loop = asyncio.get_running_loop()
    
    def scan_directory():
        """
        Funkcja pomocnicza, która wykonuje blokującą operację skanowania.
        """
        logger.debug("Uruchamiam blokującą operację rglob w osobnym wątku...")
        for path in root_dir.rglob('*'):
            if path.is_file():
                disk_paths.add(path.resolve())
    
    # Uruchom funkcję skanującą w puli wątków i poczekaj na jej zakończenie
    await loop.run_in_executor(None, scan_directory)
    
    logger.info(f"Zakończono skanowanie dysku. Znaleziono {len(disk_paths)} plików.")
    return disk_paths


async def _resolve_duplicates_interactively(duplicate_set: List[Dict]) -> Dict:
    """
    Wyświetla interfejs "obok siebie" do rozwiązania pojedynczego zestawu duplikatów.

    Funkcja ta:
    1.  Prezentuje dwa pliki z zestawu duplikatów w dwóch panelach,
        wyświetlając ich szczegółowe metadane.
    2.  Automatycznie sugeruje zachowanie pliku o większym rozmiarze na dysku.
    3.  Pozwala użytkownikowi na nawigację (strzałki lewo/prawo) w celu
        zmiany pliku, który ma zostać zachowany.
    4.  Obsługuje akcje: zatwierdzenie (Enter), pominięcie (P) i wyjście (Q).

    Args:
        duplicate_set (List[Dict]): Lista dwóch słowników, gdzie każdy
            słownik zawiera szczegółowe informacje o jednym z duplikatów.

    Returns:
        Dict: Słownik opisujący akcję podjętą przez użytkownika,
              np. {"action": "resolve", "keep": {...}, "delete": [{...}]}.
    """
    selected_to_keep = 0
    # Inteligentne, wstępne zaznaczenie pliku o większym rozmiarze
    try:
        size_a_str = duplicate_set[0].get('size', '0 MB').split(' ')[0].replace(',', '.')
        size_b_str = duplicate_set[1].get('size', '0 MB').split(' ')[0].replace(',', '.')
        if float(size_b_str) > float(size_a_str):
            selected_to_keep = 1
    except (ValueError, IndexError) as e:
        logger.debug(f"Nie udało się automatycznie porównać rozmiarów plików: {e}")
        pass

    def generate_layout() -> Layout:
        """Wewnętrzna funkcja renderująca interfejs porównawczy."""
        layout = Layout()
        layout.split_row(Layout(name="left"), Layout(name="right"))
        
        for i, file_info in enumerate(duplicate_set):
            is_kept = (i == selected_to_keep)
            status_text = Text("⭐ ZACHOWAJ", style="bold green") if is_kept else Text("🗑️ Usuń", style="dim")
            
            table = Table.grid(expand=True, padding=(0, 1))
            table.add_column(style="cyan", justify="right", width=15); table.add_column()
            
            try:
                relative_path = str(file_info['path'].relative_to(DOWNLOADS_DIR_BASE))
            except ValueError:
                relative_path = str(file_info['path'])
                
            table.add_row("ID w Bazie:", str(file_info.get('id', 'Brak')))
            table.add_row("Ścieżka:", relative_path)
            table.add_row("─" * 15, "─" * 30)
            table.add_row("Data:", file_info.get('date', 'Brak'))
            table.add_row("Rozmiar:", file_info.get('size', 'Brak'))
            table.add_row("Wymiary:", file_info.get('dimensions', 'Brak'))
            table.add_row("Typ Pliku:", file_info.get('type', 'Brak'))
            table.add_row("─" * 15, "─" * 30)
            table.add_row("Aparat:", file_info.get('camera', 'Brak'))
            table.add_row("Ekspozycja:", file_info.get('exposure', 'Brak'))
            table.add_row("GPS:", file_info.get('gps', 'Brak'))
            
            panel_content = Group(Align.center(status_text), table)
            layout["left" if i == 0 else "right"].update(
                Panel(panel_content, title=f"Plik {'A' if i == 0 else 'B'}", border_style="green" if is_kept else "default")
            )
            
        footer = Align.center(Text("[bold]L/P[/](wybierz)•[bold]ENTER[/](zatwierdź)•[bold]P[/](pomiń)•[bold]Q[/](zakończ)"))
        title_text = f"Wybierz plik do ZACHOWANIA\n[dim]Hash: {duplicate_set[0].get('hash', 'Brak')}[/dim]"
        
        main_layout = Layout()
        main_layout.split_column(Layout(Align.center(Text(title_text)), size=3), layout, Layout(footer, size=1))
        return main_layout
        
    # Główna pętla interaktywna
    with Live(generate_layout(), screen=True, auto_refresh=False, transient=True) as live:
        while True:
            live.update(generate_layout(), refresh=True)
            key = await asyncio.to_thread(get_key)
            if not key: continue
            
            if key.upper() == "Q" or key == "ESC": return {"action": "quit"}
            if key.upper() == "P": return {"action": "skip"}
            if key in ["LEFT", "RIGHT"]: selected_to_keep = 1 - selected_to_keep
            if key == "ENTER":
                to_keep = duplicate_set[selected_to_keep]
                to_delete = [duplicate_set[1 - selected_to_keep]]
                return {"action": "resolve", "keep": to_keep, "delete": to_delete}


# ##############################################################################
# ===                    SEKCJA 2: GŁÓWNE FUNKCJE WALIDATORA                 ===
# ##############################################################################

async def verify_file_existence():
    """
    Weryfikuje, czy pliki zarejestrowane w bazie danych jako 'downloaded'
    faktycznie istnieją na dysku pod zapisaną ścieżką `final_path`.

    Proces:
    1.  Asynchronicznie pobiera z bazy danych listę wszystkich plików,
        które powinny istnieć na dysku.
    2.  Iteruje przez tę listę, sprawdzając istnienie każdego pliku (operacja
        wykonywana w osobnym wątku, aby nie blokować UI).
    3.  Jeśli znajdzie "pliki-duchy" (wpisy w bazie bez pliku na dysku),
        generuje szczegółowy raport z listą brakujących plików i sugeruje
        dalsze kroki.
    """
    console.clear()
    logger.info("Uruchamiam Weryfikator Istnienia Plików (Baza vs. Dysk)...")
    console.print(Panel("👻 Weryfikator Istnienia Plików ('Duchy' w bazie) 👻", expand=False, style="bold yellow"))

    try:
        # Krok 1: Pobierz listę plików z bazy
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = "SELECT id, final_path, filename FROM downloaded_media WHERE status = 'downloaded' AND final_path IS NOT NULL AND final_path != ''"
            cursor = await conn.execute(query)
            records_to_check = await cursor.fetchall()

        if not records_to_check:
            logger.warning("Nie znaleziono w bazie żadnych plików o statusie 'downloaded' do weryfikacji.")
            console.print("\n[green]Nie znaleziono w bazie żadnych plików ze statusem 'downloaded' do weryfikacji.[/green]")
            return

        # Krok 2: Sprawdź istnienie plików na dysku
        missing_files = []
        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[green]Sprawdzanie plików na dysku...", total=len(records_to_check))
            for record in records_to_check:
                file_path = Path(record['final_path'])
                if not await asyncio.to_thread(file_path.exists):
                    missing_files.append({
                        "id": record['id'],
                        "path": str(file_path),
                        "filename": record['filename']
                    })
                    logger.warning(f"Brak pliku na dysku dla ID={record['id']}: {file_path}")
                progress.update(task, advance=1)

        # Krok 3: Wyświetl wyniki
        if not missing_files:
            logger.info("Weryfikacja zakończona pomyślnie. Wszystkie pliki z bazy istnieją na dysku.")
            console.print(f"\n[bold green]✅ Weryfikacja zakończona. Wszystkie {len(records_to_check)} pliki z bazy danych istnieją na dysku.[/bold green]")
        else:
            logger.error(f"Znaleziono {len(missing_files)} brakujących plików ('duchów')!")
            console.print(f"\n[bold red]⚠️ Znaleziono {len(missing_files)} brakujących plików (wpisy w bazie bez pliku na dysku):[/bold red]")
            
            table = Table(title="Lista Brakujących Plików ('Duchy')")
            table.add_column("ID Wpisu", style="cyan", justify="right")
            table.add_column("Oczekiwana Ścieżka", style="red")
            for missing in missing_files:
                table.add_row(str(missing['id']), missing['path'])
            
            console.print(table)
            console.print("\n[yellow]Powyższe pliki zostały prawdopodobnie usunięte lub przeniesione ręcznie.[/yellow]")
            console.print("[dim]Użyj 'Narzędzia Zaawansowane -> Edytor Bazy Danych', aby usunąć te martwe wpisy.[/dim]")

    except aiosqlite.Error as e:
        logger.critical("Błąd bazy danych podczas weryfikacji istnienia plików.", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd bazy danych: {e}[/bold red]")
    except Exception as e:
        logger.critical("Wystąpił nieoczekiwany błąd podczas weryfikacji istnienia plików.", exc_info=True)
        console.print(f"[bold red]Wystąpił nieoczekiwany błąd. Sprawdź logi.[/bold red]")


async def verify_and_write_hashes():
    """
    Skanuje pliki w bazie danych, oblicza dla nich sumy kontrolne MD5
    i zapisuje je w kolumnie `file_hash`.

    Proces:
    1.  Pobiera z bazy listę wszystkich plików o statusie 'downloaded',
        które nie mają jeszcze obliczonego hasha.
    2.  Dla każdego pliku:
        a) Sprawdza, czy plik istnieje na dysku.
        b) Wywołuje `_calculate_hash_for_file` do obliczenia sumy kontrolnej.
    3.  Zapisuje wyniki do bazy danych w partiach (batches), aby
        zminimalizować liczbę transakcji i zwiększyć wydajność.
    """
    console.clear()
    logger.info("Uruchamiam weryfikację i zapis sum kontrolnych (MD5)...")
    console.print(Panel("🧮 Obliczanie i Zapis Sum Kontrolnych (MD5) 🧮", expand=False, style="bold yellow"))
    
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            
            # Krok 1: Pobierz listę plików do przetworzenia
            query = "SELECT id, final_path FROM downloaded_media WHERE status = 'downloaded' AND (file_hash IS NULL OR file_hash = '')"
            cursor = await conn.execute(query)
            records_to_hash = await cursor.fetchall()

            if not records_to_hash:
                logger.info("Wszystkie pobrane pliki w bazie mają już obliczone sumy kontrolne.")
                console.print("\n[bold green]✅ Wszystkie pobrane pliki w bazie mają już obliczone sumy kontrolne.[/bold green]")
                return

            logger.info(f"Znaleziono {len(records_to_hash)} plików do obliczenia hasha.")
            
            # Krok 2: Przetwarzaj pliki i zbieraj wyniki w partii
            updates_batch: List[Tuple[str, int]] = []
            BATCH_SIZE = 100
            
            with Progress(console=console, transient=True) as progress:
                task = progress.add_task("[cyan]Obliczanie hashy MD5...", total=len(records_to_hash))
                for record in records_to_hash:
                    file_path = Path(record['final_path'])
                    if await asyncio.to_thread(file_path.exists):
                        file_hash = await _calculate_hash_for_file(file_path)
                        if file_hash:
                            updates_batch.append((file_hash, record['id']))
                    else:
                        logger.warning(f"Pominięto obliczanie hasha dla nieistniejącego pliku: {file_path}")
                    
                    # Krok 3: Zapisz partię do bazy, jeśli osiągnęła limit
                    if len(updates_batch) >= BATCH_SIZE:
                        await conn.executemany("UPDATE downloaded_media SET file_hash = ? WHERE id = ?", updates_batch)
                        await conn.commit()
                        logger.info(f"Zapisano partię {len(updates_batch)} hashy do bazy danych.")
                        updates_batch.clear()
                        
                    progress.update(task, advance=1)

            # Krok 4: Zapisz ostatnią, niepełną partię
            if updates_batch:
                await conn.executemany("UPDATE downloaded_media SET file_hash = ? WHERE id = ?", updates_batch)
                await conn.commit()
                logger.info(f"Zapisano ostatnią partię {len(updates_batch)} hashy do bazy danych.")
                
            console.print(f"\n[bold green]✅ Zakończono. Zaktualizowano sumy kontrolne dla {len(records_to_hash)} plików.[/bold green]")
            
    except aiosqlite.Error as e:
        logger.critical("Krytyczny błąd bazy danych podczas zapisu hashy.", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd bazy danych: {e}[/bold red]")
    except Exception as e:
        logger.critical("Wystąpił nieoczekiwany błąd podczas obliczania hashy.", exc_info=True)
        console.print(f"[bold red]Wystąpił nieoczekiwany błąd. Sprawdź logi.[/bold red]")


async def find_and_fix_inconsistencies():
    """
    Znajduje i pozwala naprawić niespójności między wpisami w bazie danych
    a fizycznymi plikami na dysku.

    Operacja ta identyfikuje dwa typy problemów:
    1.  **Duchy (Ghosts):** Wpisy w bazie, dla których nie istnieje plik na dysku.
    2.  **Sieroty (Orphans):** Pliki na dysku, dla których nie ma wpisu w bazie.

    Skanuje zarówno główną bibliotekę (`DOWNLOADS_DIR_BASE`), jak i wszystkie
    foldery zindeksowane z `LOCAL_SCANNER_DIRECTORIES`.
    """
    console.clear()
    logger.info("Uruchamiam wyszukiwanie niespójności (Baza Danych vs. Dysk)...")
    console.print(Panel("👻 Wyszukiwanie Niespójności (Duchy i Sieroty)", expand=False, style="bold yellow"))
    
    try:
        # Krok 1: Asynchronicznie wczytaj dane z bazy i z dysku
        with console.status("[cyan]Wczytywanie rekordów z bazy danych...[/]"):
            async with aiosqlite.connect(DATABASE_FILE) as conn:
                conn.row_factory = aiosqlite.Row
                query = "SELECT id, final_path FROM downloaded_media WHERE final_path IS NOT NULL AND final_path != ''"
                cursor = await conn.execute(query)
                db_records = [{'id': row['id'], 'path': Path(row['final_path']).resolve()} for row in await cursor.fetchall()]
        db_paths = {rec['path'] for rec in db_records}
        
        with console.status("[cyan]Skanowanie plików na dysku (może potrwać)...[/]"):
            # NOWA LOGIKA: Skanuj wszystkie zdefiniowane ścieżki
            disk_paths = set()
            paths_to_scan = [Path(DOWNLOADS_DIR_BASE)] + [Path(p) for p in LOCAL_SCANNER_DIRECTORIES]
            
            for root_dir in paths_to_scan:
                if await asyncio.to_thread(root_dir.is_dir):
                    logger.info(f"Skanuję folder: {root_dir}")
                    scanned_paths = await _get_all_disk_paths(root_dir)
                    disk_paths.update(scanned_paths)
                else:
                    logger.warning(f"Ścieżka '{root_dir}' z konfiguracji nie jest prawidłowym folderem i zostanie pominięta.")

        logger.info(f"Porównuję {len(db_paths)} wpisów z bazy z {len(disk_paths)} plikami na dysku...")
        
        # Krok 2: Zidentyfikuj "duchy" i "sieroty"
        db_ghosts = [rec for rec in db_records if rec['path'] not in disk_paths]
        disk_orphans = sorted([path for path in disk_paths if path not in db_paths])

        # Krok 3: Obsługa "duchów" (wpisów w bazie bez plików)
        if db_ghosts:
            console.print(f"\n[bold yellow]Znaleziono {len(db_ghosts)} 'duchów' w bazie danych (wpisy bez plików).[/]")
            if Confirm.ask("[cyan]Czy chcesz usunąć te martwe wpisy z bazy danych?[/]", default=True):
                ghost_ids = [ghost['id'] for ghost in db_ghosts]
                async with aiosqlite.connect(DATABASE_FILE) as conn:
                    placeholders = ','.join(['?'] * len(ghost_ids))
                    await conn.execute(f"DELETE FROM downloaded_media WHERE id IN ({placeholders})", ghost_ids)
                    await conn.commit()
                logger.info(f"Usunięto {len(db_ghosts)} wpisów-duchów z bazy danych.")
        
        # Krok 4: Obsługa "sierot" (plików na dysku bez wpisów w bazie)
        if disk_orphans:
            console.print(f"\n[bold yellow]Znaleziono {len(disk_orphans)} 'sierot' na dysku (pliki bez wpisów w bazie).[/]")
            selected_files = await _interactive_file_selector(disk_orphans, "Wybierz 'osierocone' pliki do zarządzania")

            if selected_files:
                action = Prompt.ask(f"\nWybrano [cyan]{len(selected_files)}[/cyan] plików. Co z nimi zrobić?", choices=["importuj", "usuń", "anuluj"], default="anuluj")
                
                if action == "importuj":
                    if not EXIFTOOL_AVAILABLE:
                        console.print(Panel("[bold red]Błąd: Brak 'pyexiftool'![/bold red]\nOperacja importu wymaga tej biblioteki.", title="Brak Zależności"))
                    else:
                        with Progress(console=console, transient=True) as progress_bar:
                            task = progress_bar.add_task("[green]Importuję do bazy...", total=len(selected_files))
                            with exiftool.ExifToolHelper() as et:
                                for file_path in selected_files:
                                    try:
                                        metadata_list = await asyncio.to_thread(et.get_metadata, str(file_path))
                                        if metadata_list:
                                            # Używamy nowej, poprawnej funkcji z `database.py`
                                            await add_local_file_entry(file_path, metadata_list[0])
                                    except Exception:
                                        logger.error(f"Błąd importu {file_path.name}", exc_info=True)
                                    progress_bar.update(task, advance=1)
                elif action == "usuń":
                    if Confirm.ask(f"\n[bold red]Czy na pewno chcesz TRWALE usunąć {len(selected_files)} zaznaczonych plików z dysku?[/]"):
                        with Progress(console=console, transient=True) as progress_bar:
                            task = progress_bar.add_task("[red]Usuwam z dysku...", total=len(selected_files))
                            for file_path in selected_files:
                                try:
                                    await asyncio.to_thread(os.remove, file_path)
                                except OSError:
                                    logger.error(f"Błąd usuwania {file_path.name}", exc_info=True)
                                progress_bar.update(task, advance=1)
        
        if not db_ghosts and not disk_orphans:
            console.print("\n[bold green]✅ Weryfikacja zakończona. Nie znaleziono żadnych niespójności.[/bold green]")

    except aiosqlite.Error as e:
        logger.critical("Krytyczny błąd bazy danych podczas wyszukiwania niespójności.", exc_info=True)
    except Exception as e:
        logger.critical("Wystąpił nieoczekiwany błąd podczas analizy niespójności.", exc_info=True)


async def analyze_metadata_consistency():
    """
    Sprawdza spójność danych między fizyczną lokalizacją pliku a datą
    zapisaną w jego metadanych w bazie danych.

    Proces:
    1.  Pobiera z bazy wszystkie wpisy, które mają zarówno ścieżkę (`final_path`),
        jak i datę (`DateTime`) w metadanych JSON.
    2.  Dla każdego wpisu, porównuje rok i miesiąc z daty w bazie z rokiem
        i miesiącem wynikającym ze struktury folderów (`.../ROK/MIESIĄC/...`).
    3.  Jeśli znajdzie niespójności, generuje szczegółowy raport w formie tabeli,
        wskazując, które pliki mogą znajdować się w niewłaściwych folderach.
    """
    console.clear()
    logger.info("Uruchamiam analizę spójności metadanych (Ścieżka vs. Baza)...")
    console.print(Panel("🔗 Analiza Spójności Metadanych (Ścieżka vs. Data w Bazie) 🔗", expand=False, style="bold yellow"))
    
    mismatches = []
    
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            query = """
                SELECT id, final_path, json_extract(metadata_json, '$.DateTime') as dt_from_json
                FROM downloaded_media
                WHERE status = 'downloaded' AND dt_from_json IS NOT NULL AND final_path IS NOT NULL AND final_path != ''
            """
            
            with Progress(console=console, transient=True) as progress:
                cursor = await conn.execute(query)
                all_records = await cursor.fetchall()
                
                if not all_records:
                    logger.warning("Nie znaleziono plików z kompletnymi danymi do analizy spójności.")
                    console.print("\n[green]Nie znaleziono plików z wymaganymi danymi do analizy.[/green]")
                    return

                task = progress.add_task("[cyan]Analizowanie spójności...", total=len(all_records))
                for row in all_records:
                    try:
                        path = Path(row['final_path'])
                        # Wymagane jest, aby ścieżka miała co najmniej 2 poziomy folderów (ROK/MIESIĄC)
                        if len(path.parts) < 3:
                            logger.debug(f"Pominięto rekord ze zbyt krótką ścieżką: ID {row['id']}")
                            continue
                            
                        db_datetime = datetime.fromisoformat(row['dt_from_json'].replace('Z', '+00:00'))
                        db_year, db_month = db_datetime.year, db_datetime.month

                        # Pobierz rok i miesiąc ze ścieżki (przedostatni i ostatni folder)
                        path_month = int(path.parent.name)
                        path_year = int(path.parent.parent.name)

                        if db_year != path_year or db_month != path_month:
                            mismatches.append({
                                'path': str(path),
                                'db_date': f"{db_year}-{db_month:02d}",
                                'path_date': f"{path_year}-{path_month:02d}"
                            })
                    except (ValueError, IndexError, TypeError, AttributeError) as e:
                        logger.debug(f"Pominięto rekord z powodu błędu parsowania (ścieżka/data): ID {row['id']}. Błąd: {e}")
                        continue
                    finally:
                         progress.update(task, advance=1)

        if not mismatches:
            logger.info("Analiza zakończona. Spójność metadanych idealna.")
            console.print("\n[bold green]✅ Spójność metadanych idealna! Wszystkie daty w ścieżkach zgadzają się z bazą danych.[/bold green]")
        else:
            logger.error(f"Znaleziono {len(mismatches)} niespójności między ścieżką a metadanymi.")
            console.print(f"\n[bold red]⚠️ Znaleziono {len(mismatches)} niespójności:[/bold red]")
            
            table = Table(title="Niespójności Metadanych (Ścieżka vs. Baza)")
            table.add_column("Ścieżka Pliku", style="cyan"); table.add_column("Data wg Bazy", style="yellow"); table.add_column("Data wg Ścieżki", style="red")
            for item in mismatches[:30]: table.add_row(item['path'], item['db_date'], item['path_date'])
            console.print(table)
            
            if len(mismatches) > 30: console.print(f"  ... i {len(mismatches) - 30} więcej.")
                
            console.print("\n[yellow]Powyższe niespójności mogą wskazywać na błąd w sortowaniu lub ręczne przeniesienie plików.[/yellow]")
            console.print("[dim]Użyj 'Skaner i Menedżer -> Sprawdź i napraw LOKALIZACJE plików', aby to naprawić.[/dim]")

    except aiosqlite.Error as e:
        logger.critical("Błąd bazy danych podczas analizy spójności.", exc_info=True)
    except Exception as e:
        logger.critical("Wystąpił nieoczekiwany błąd podczas analizy spójności.", exc_info=True)


async def find_duplicates_by_hash():
    """
    Uruchamia interaktywny menedżer do wyszukiwania i rozwiązywania
    problemu duplikatów plików na podstawie ich sum kontrolnych (hash MD5).

    Proces:
    1.  Wyszukuje w bazie danych wszystkie hashe MD5, które występują więcej
        niż raz.
    2.  Dla każdej grupy duplikatów, pobiera szczegółowe metadane.
    3.  Prezentuje użytkownikowi interfejs `_resolve_duplicates_interactively`
        do podjęcia decyzji, który plik zachować.
    4.  Po zakończeniu przeglądu, prosi o ostateczne potwierdzenie i bezpiecznie
        usuwa wybrane pliki z dysku oraz odpowiadające im wpisy z bazy danych.
    """
    console.clear()
    logger.info("Uruchamiam Interaktywny Menedżer Duplikatów (wg hash MD5)...")
    console.print(Panel("🧩 Menedżer Duplikatów Plików (wg zawartości) 🧩", expand=False, style="bold yellow"))
    
    try:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            
            # Krok 1: Znajdź hashe, które mają duplikaty
            query_hashes = "SELECT file_hash FROM downloaded_media WHERE file_hash IS NOT NULL AND file_hash != '' GROUP BY file_hash HAVING COUNT(id) > 1"
            cursor = await conn.execute(query_hashes)
            duplicate_hashes = await cursor.fetchall()

            if not duplicate_hashes:
                logger.info("Nie znaleziono żadnych duplikatów na podstawie sum kontrolnych.")
                console.print("\n[bold green]✅ Nie znaleziono żadnych duplikatów plików na podstawie ich zawartości.[/bold green]")
                return

            logger.warning(f"Znaleziono {len(duplicate_hashes)} zestawy potencjalnych duplikatów. Rozpoczynam przegląd...")
            
            all_files_to_delete = []

            # Krok 2: Iteruj po każdym zestawie duplikatów
            for i, row in enumerate(duplicate_hashes):
                hash_val = row['file_hash']
                console.clear()
                console.print(Panel(f"[bold yellow]Zestaw duplikatów {i + 1}/{len(duplicate_hashes)}[/]", expand=False))

                query_details = "SELECT id, final_path, metadata_json FROM downloaded_media WHERE file_hash = ?"
                cursor = await conn.execute(query_details, (hash_val,))
                files_in_set_raw = await cursor.fetchall()

                files_in_set = []
                for file_row in files_in_set_raw:
                    metadata = json.loads(file_row['metadata_json'] or '{}')
                    file_path = Path(file_row['final_path'])
                    # Wywołujemy scentralizowaną funkcję z utils.py
                    display_info = _parse_metadata_for_display(metadata, file_path)
                    files_in_set.append({"id": file_row['id'], "path": file_path, "hash": hash_val, **display_info})

                if len(files_in_set) > 1:
                    resolution = await _resolve_duplicates_interactively(files_in_set)
                    if resolution.get("action") == "quit": break
                    if resolution.get("action") == "skip": continue
                    if resolution.get("action") == "resolve": all_files_to_delete.extend(resolution['delete'])

            # Krok 3: Podsumowanie i ostateczne usunięcie
            if not all_files_to_delete:
                logger.info("Przegląd zakończony. Nie wybrano żadnych plików do usunięcia.")
                return

            console.clear()
            console.print(Panel("[bold red]Podsumowanie Akcji Usunięcia Duplikatów[/]", expand=False))
            console.print(f"Wybrano [cyan]{len(all_files_to_delete)}[/cyan] plików do trwałego usunięcia.")

            if Confirm.ask("\n[bold red]Czy na pewno chcesz TRWALE usunąć te pliki z dysku i z bazy danych?[/]", default=False):
                ids_to_delete = [f['id'] for f in all_files_to_delete]
                
                with Progress(console=console, transient=True) as progress:
                    task = progress.add_task("[red]Usuwam pliki z dysku...", total=len(all_files_to_delete))
                    for file_info in all_files_to_delete:
                        try:
                            if await asyncio.to_thread(file_info['path'].exists):
                                await asyncio.to_thread(os.remove, file_info['path'])
                        except OSError as e:
                            logger.error(f"Błąd usuwania pliku {file_info['path']}", exc_info=True)
                        progress.update(task, advance=1)

                with console.status("[bold red]Usuwam wpisy z bazy danych...[/]"):
                    placeholders = ','.join(['?'] * len(ids_to_delete))
                    await conn.execute(f"DELETE FROM downloaded_media WHERE id IN ({placeholders})", ids_to_delete)
                    await conn.commit()
                
                console.print(f"\n[bold green]✅ Usunięto {len(ids_to_delete)} duplikatów.[/bold green]")
                logger.info(f"Usunięto {len(ids_to_delete)} zduplikowanych plików i wpisów w bazie.")
            else:
                logger.warning("Anulowano operację usuwania duplikatów.")

    except aiosqlite.Error as e:
        logger.critical("Błąd bazy danych podczas wyszukiwania duplikatów.", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd bazy danych: {e}[/bold red]")
    except Exception as e:
        logger.critical("Wystąpił nieoczekiwany błąd podczas zarządzania duplikatami.", exc_info=True)
        console.print(f"[bold red]Wystąpił nieoczekiwany błąd. Sprawdź logi.[/bold red]")


# ##############################################################################
# ===                    SEKCJA 3: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_integrity_validator():
    """
    Wyświetla i zarządza interaktywnym menu dla "Walidatora Integralności".

    Ta funkcja jest "launcherem" dla całego modułu. Jej zadaniem jest:
    1.  Zdefiniowanie opcji dostępnych w menu.
    2.  Wywołanie uniwersalnej funkcji `create_interactive_menu` do wyświetlenia
        interfejsu i obsłużenia wyboru użytkownika.
    3.  Uruchomienie odpowiedniej akcji w zależności od decyzji użytkownika.
    """
    logger.info("Uruchamiam menu Walidatora Integralności Danych.")
    
    menu_items = [
        ("Sprawdź istnienie plików (Baza vs Dysk)", verify_file_existence),
        ("Oblicz i zapisz sumy kontrolne plików (hash MD5)", verify_and_write_hashes),
        ("Znajdź niespójności (pliki-sieroty i duchy)", find_and_fix_inconsistencies),
        ("Analizuj spójność metadanych (ścieżka vs. data)", analyze_metadata_consistency),
        ("Znajdź i zarządzaj duplikatami (wg zawartości)", find_duplicates_by_hash),
        ("Wróć do menu głównego", "exit")
    ]

    while True:
        console.clear()
        selected_action = await create_interactive_menu(
            menu_items,
            "Walidator Integralności Danych",
            border_style="blue"
        )

        if selected_action == "exit" or selected_action is None:
            logger.info("Zamykanie Walidatora Integralności.")
            break

        # Uruchom wybrane narzędzie
        await selected_action()
        
        # Poproś o interakcję przed ponownym wyświetleniem menu
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter, aby wrócić do menu walidatora...[/]")
