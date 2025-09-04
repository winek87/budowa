# plik: core/analytics/reports.py (FINALNA WERSJA PO REFRAKTORYZACJI)
# -*- coding: utf-8 -*-

import logging
import math
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict

from rich.console import Console, Group
from rich.panel import Panel
from rich.prompt import Confirm
from rich.table import Table

from .data_loader import get_all_media_entries
from ..utils import create_interactive_menu, format_size_for_display

console = Console(record=True)
logger = logging.getLogger(__name__)

MONTH_NAMES = {1: "Styczeń", 2: "Luty", 3: "Marzec", 4: "Kwiecień", 5: "Maj", 6: "Czerwiec",
               7: "Lipiec", 8: "Sierpień", 9: "Wrzesień", 10: "Październik", 11: "Listopad", 12: "Grudzień"}

# ##############################################################################
# ===           SEKCJA 1: FUNKCJE PRZYGOTOWANIA DANYCH (LOGIKA)              ===
# ##############################################################################

def _prepare_technical_stats_data(all_media_data: list) -> dict:
    """Przetwarza surowe dane i zwraca słownik ze statystykami technicznymi."""
    MEDIA_TYPES = {
        'Zdjęcia': ['JPG', 'JPEG', 'HEIC', 'PNG', 'GIF', 'WEBP', 'BMP'],
        'Wideo': ['MP4', 'MOV', 'AVI', 'M4V', '3GP', 'MKV'],
        'Pliki RAW': ['CR2', 'CR3', 'NEF', 'ARW', 'DNG', 'RW2', 'RAF']
    }
    category_stats = defaultdict(lambda: {"count": 0, "size": 0})
    aspect_ratio_counts = Counter()
    file_extension_counts = Counter()
    for entry in all_media_data:
        filename = entry.get('filename')
        if not filename or not isinstance(filename, str):
            category_stats["Inne"]["count"] += 1
            if size := entry.get('size'): category_stats["Inne"]["size"] += size
            continue
        extension = Path(filename).suffix.replace('.', '').upper()
        if not extension:
            category_stats["Inne"]["count"] += 1
            if size := entry.get('size'): category_stats["Inne"]["size"] += size
            continue
        file_extension_counts[extension] += 1
        found_category = next((cat for cat, exts in MEDIA_TYPES.items() if extension in exts), "Inne")
        category_stats[found_category]["count"] += 1
        if size := entry.get('size'): category_stats[found_category]["size"] += size
        if isinstance(dims_str := entry.get('Dimensions'), str) and '×' in dims_str:
            try:
                width, height = map(int, [p.strip() for p in dims_str.split('×')])
                if width > 0 and height > 0:
                    common_divisor = math.gcd(width, height)
                    orient = "Pozioma" if width > height else "Pionowa" if height > width else "Kwadrat"
                    label = f"{width // common_divisor}:{height // common_divisor} ({orient})"
                    aspect_ratio_counts[label] += 1
            except (ValueError, TypeError, IndexError): pass
    return {
        "category_stats": sorted(category_stats.items(), key=lambda item: item[1]['size'], reverse=True),
        "extension_counts": file_extension_counts.most_common(15),
        "ratio_counts": aspect_ratio_counts.most_common(10),
        "total_files": len(all_media_data)
    }

def _prepare_seasonal_stats_data(all_media_data: list) -> dict:
    """Przetwarza surowe dane i zwraca słownik ze statystykami sezonowymi."""
    monthly_counts = Counter(entry['dt'].month for entry in all_media_data)
    seasons = {"Wiosna (Mar-Maj)": 0, "Lato (Cze-Sier)": 0, "Jesień (Wrz-Lis)": 0, "Zima (Gru-Lut)": 0}
    for month, count in monthly_counts.items():
        if month in [3, 4, 5]: seasons["Wiosna (Mar-Maj)"] += count
        elif month in [6, 7, 8]: seasons["Lato (Cze-Sier)"] += count
        elif month in [9, 10, 11]: seasons["Jesień (Wrz-Lis)"] += count
        else: seasons["Zima (Gru-Lut)"] += count
    return {
        "monthly_counts": sorted(monthly_counts.items()),
        "seasonal_counts": sorted(seasons.items(), key=lambda item: item[1], reverse=True),
        "total_files": len(all_media_data)
    }

def _prepare_rankings_data(all_media_data: list) -> dict:
    """Przetwarza surowe dane i zwraca słownik z rankingami osób i albumów."""
    people_counter = Counter(p for e in all_media_data if e.get('TaggedPeople') for p in e['TaggedPeople'])
    album_counter = Counter(a for e in all_media_data if e.get('Albums') for a in e['Albums'])
    return {
        "people_ranking": people_counter.most_common(15),
        "album_ranking": album_counter.most_common(15),
        "total_files": len(all_media_data)
    }

def _prepare_largest_files_data(all_media_data: list) -> dict:
    """Przetwarza surowe dane i zwraca słownik z rankingiem największych plików."""
    media_with_size = [entry for entry in all_media_data if entry.get('size') is not None]
    if not media_with_size:
        return {"largest_files": [], "total_files": len(all_media_data)}
    sorted_by_size = sorted(media_with_size, key=lambda x: x['size'], reverse=True)
    return {
        "largest_files": sorted_by_size[:30],
        "total_files": len(all_media_data)
    }

def _prepare_metadata_health_data(all_media_data: list) -> dict:
    """Przetwarza surowe dane i zwraca słownik ze statystykami o kompletności metadanych."""
    total_entries = len(all_media_data)
    KEYS_TO_CHECK = {'dt': "Data", 'Location': "Lokalizacja", 'Camera': "Aparat", 'Dimensions': "Wymiary", 'size': "Rozmiar", 'TaggedPeople': "Osoby", 'Albums': "Albumy"}
    missing_counts = Counter(key for entry in all_media_data for key in KEYS_TO_CHECK if not entry.get(key))
    health_stats = []
    for key, name in KEYS_TO_CHECK.items():
        missing = missing_counts[key]
        completeness = ((total_entries - missing) / total_entries) * 100 if total_entries > 0 else 0
        color = "green" if completeness > 95 else "yellow" if completeness > 75 else "red"
        health_stats.append({
            "name": name,
            "missing_count": str(missing),
            "completeness_str": f"[{color}]{completeness:.1f}%[/{color}]"
        })
    return {
        "health_stats": health_stats,
        "total_files": total_entries
    }

# ##############################################################################
# ===           SEKCJA 2: FUNKCJE WYŚWIETLANIA RAPORTÓW (UI)                 ===
# ##############################################################################

def _generate_technical_stats_group(all_media_data: list) -> Group:
    """Wewnętrzna funkcja generująca grupę obiektów Rich dla raportu technicznego."""
    stats_data = _prepare_technical_stats_data(all_media_data)
    media_type_table = Table(title="Podsumowanie wg typów mediów")
    media_type_table.add_column("Typ", style="cyan"); media_type_table.add_column("Ilość", justify="right"); media_type_table.add_column("Rozmiar", justify="right")
    for cat, stats in stats_data["category_stats"]:
        if stats['count'] > 0: media_type_table.add_row(cat, str(stats['count']), format_size_for_display(stats['size']))
    extension_table = Table(title="Ranking rozszerzeń (TOP 15)")
    extension_table.add_column("Rozszerzenie", style="cyan"); extension_table.add_column("Ilość", justify="right")
    for ext, count in stats_data["extension_counts"]: extension_table.add_row(ext, str(count))
    ratio_table = Table(title="Ranking proporcji (TOP 10)")
    ratio_table.add_column("Proporcje", style="cyan"); ratio_table.add_column("Ilość", justify="right")
    for ratio, count in stats_data["ratio_counts"]: ratio_table.add_row(ratio, str(count))
    layout_grid = Table.grid(expand=True, padding=(0, 2)); layout_grid.add_column(ratio=1); layout_grid.add_column(ratio=1); layout_grid.add_column(ratio=1)
    layout_grid.add_row(media_type_table, extension_table, ratio_table)
    return Group(Panel(layout_grid, title=f"Analiza Techniczna ({stats_data['total_files']} plików)", border_style="green", padding=(1, 1)))

def _generate_seasonal_stats_group(all_media_data: list) -> Group:
    """Wewnętrzna funkcja generująca grupę obiektów Rich dla raportu sezonowego."""
    stats_data = _prepare_seasonal_stats_data(all_media_data)
    monthly_table = Table(title="Średnia aktywność w poszczególnych miesiącach")
    monthly_table.add_column("Miesiąc", style="cyan"); monthly_table.add_column("Liczba Zdjęć", justify="right")
    for month_num, count in stats_data["monthly_counts"]: monthly_table.add_row(f"{month_num:02d} - {MONTH_NAMES.get(month_num, '')}", str(count))
    seasonal_table = Table(title="Aktywność w podziale na pory roku")
    seasonal_table.add_column("Pora Roku", style="cyan"); seasonal_table.add_column("Liczba Zdjęć", justify="right")
    for season, count in stats_data["seasonal_counts"]: seasonal_table.add_row(season, str(count))
    return Group(Panel(Group(monthly_table, "\n", seasonal_table), title=f"Analiza Sezonowa ({stats_data['total_files']} plików)", border_style="green", padding=(1, 2)))

def _generate_rankings_group(all_media_data: list) -> Group:
    """Wewnętrzna funkcja generująca grupę obiektów Rich dla raportu rankingowego."""
    stats_data = _prepare_rankings_data(all_media_data)
    people_table = Table(title="Najczęściej fotografowane osoby (TOP 15)")
    people_table.add_column("Osoba", style="cyan"); people_table.add_column("Liczba zdjęć", justify="right")
    if not stats_data["people_ranking"]: people_table.add_row("[dim]Brak danych.[/dim]", "")
    else:
        for item, count in stats_data["people_ranking"]: people_table.add_row(str(item), str(count))
    album_table = Table(title="Najpopularniejsze albumy (TOP 15)")
    album_table.add_column("Album", style="cyan"); album_table.add_column("Liczba zdjęć", justify="right")
    if not stats_data["album_ranking"]: album_table.add_row("[dim]Brak danych.[/dim]", "")
    else:
        for item, count in stats_data["album_ranking"]: album_table.add_row(str(item), str(count))
    return Group(Panel(Group(people_table, "\n", album_table), title=f"Rankingi Społecznościowe ({stats_data['total_files']} plików)", border_style="green", padding=(1, 2)))

def _generate_largest_files_group(all_media_data: list) -> Group:
    """Wewnętrzna funkcja generująca grupę obiektów Rich dla raportu o największych plikach."""
    stats_data = _prepare_largest_files_data(all_media_data)
    if not stats_data["largest_files"]: return Group(Panel("[yellow]Brak danych o rozmiarach plików do analizy.[/yellow]"))
    table = Table(title="30 plików zajmujących najwięcej miejsca w kolekcji")
    table.add_column("Nazwa Pliku", style="cyan"); table.add_column("Rozmiar", justify="right"); table.add_column("Data", justify="right")
    for entry in stats_data["largest_files"]: table.add_row(entry['filename'], format_size_for_display(entry['size']), entry['dt'].strftime('%Y-%m-%d'))
    return Group(Panel(table, border_style="green", title=f"Ranking Największych Plików ({stats_data['total_files']} plików)"))

def _generate_metadata_health_group(all_media_data: list) -> Group:
    """Wewnętrzna funkcja generująca grupę obiektów Rich dla raportu o zdrowiu metadanych."""
    stats_data = _prepare_metadata_health_data(all_media_data)
    table = Table(title=f"Kompletność metadanych (na podstawie {stats_data['total_files']} plików)")
    table.add_column("Typ Metadanych", style="cyan"); table.add_column("Liczba Braków", justify="right"); table.add_column("Kompletność", justify="right")
    for item in stats_data["health_stats"]: table.add_row(item["name"], item["missing_count"], item["completeness_str"])
    return Group(Panel(table, border_style="green", padding=(1, 2), title=f"Raport o Zdrowiu Metadanych ({stats_data['total_files']} plików)"))

# ##############################################################################
# ===           SEKCJA 3: FUNKCJE URUCHAMIAJĄCE RAPORTY (ORKIESTRATOR)       ===
# ##############################################################################

async def _generate_and_show_report(report_key: str, generator_func, title: str):
    """Uniwersalna funkcja do wczytywania danych, generowania i wyświetlania raportu."""
    console.clear()
    logger.info(f"Uruchamiam generator raportu: {title}")
    console.print(Panel(f"[bold green]{title}[/]", expand=False))

    with console.status("[cyan]Przygotowywanie danych do raportu...[/]"):
        all_media_data = await get_all_media_entries()

    if not all_media_data:
        console.print("[red]Brak danych do wygenerowania raportu.[/red]"); return

    report_content = generator_func(all_media_data)
    console.print(report_content)

    if Confirm.ask("\n[cyan]Czy chcesz wyeksportować ten raport do pliku HTML?[/cyan]"):
        reports_dir = Path("app_data/reports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        filename = reports_dir / f"raport_{report_key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        try:
            # Tworzymy nową konsolę do zapisu, aby uniknąć problemów z Live
            export_console = Console(record=True, force_terminal=True)
            export_console.print(report_content)
            export_console.save_html(str(filename), clear=False)
            console.print(Panel(f"[green]Sukces![/green] Raport zapisano do [cyan]{filename.resolve()}[/cyan]"))
        except Exception as e:
            logger.error(f"Błąd eksportu do HTML: {e}", exc_info=True)

async def show_technical_stats(): await _generate_and_show_report("technical", _generate_technical_stats_group, "Analiza Techniczna")
async def show_seasonal_stats(): await _generate_and_show_report("seasonal", _generate_seasonal_stats_group, "Analiza Sezonowa")
async def show_people_and_album_rankings(): await _generate_and_show_report("rankings", _generate_rankings_group, "Ranking Osób i Albumów")
async def show_largest_files_ranking(): await _generate_and_show_report("largest_files", _generate_largest_files_group, "Ranking Największych Plików")
async def show_metadata_health(): await _generate_and_show_report("metadata_health", _generate_metadata_health_group, "Raport o Zdrowiu Metadanych")

async def export_report_menu():
    """Wyświetla menu wyboru raportu do wyeksportowania i zarządza procesem zapisu."""
    console.clear()
    logger.info("Uruchomiono menedżera eksportu raportów.")

    report_generators = {
        "full": ("Pełny Raport (wszystkie sekcje)", [_generate_technical_stats_group, _generate_seasonal_stats_group, _generate_rankings_group, _generate_largest_files_group, _generate_metadata_health_group]),
        "technical": ("Tylko Raport Techniczny", [_generate_technical_stats_group]),
        "seasonal": ("Tylko Raport Sezonowy", [_generate_seasonal_stats_group]),
    }
    menu_items = [(name, key) for key, (name, _) in report_generators.items()]
    menu_items.append(("Anuluj", "exit"))

    selected_report_key = await create_interactive_menu(menu_items, "Wybierz raport do wyeksportowania", "yellow")

    if selected_report_key in ["exit", None]:
        logger.info("Anulowano eksport raportu."); return

    with console.status("[cyan]Przygotowywanie danych do raportu...[/]"):
        all_media_data = await get_all_media_entries()

    if not all_media_data:
        console.print("[red]Brak danych do wygenerowania raportu.[/red]"); return

    _, generator_funcs = report_generators[selected_report_key]
    
    # Stwórz nową konsolę do zapisu, aby zebrać całą treść raportu
    export_console = Console(record=True, force_terminal=True)
    for func in generator_funcs:
        content_group = func(all_media_data)
        export_console.print(content_group)

    reports_dir = Path("app_data/reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    filename = reports_dir / f"raport_{selected_report_key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

    try:
        with console.status(f"[cyan]Zapisywanie do pliku [bold]{filename.name}[/bold]...[/]"):
            export_console.save_html(str(filename), clear=False)
        console.print(Panel(f"[green]Sukces![/green] Raport zapisano do [cyan]{filename.resolve()}[/cyan]"))
    except Exception as e:
        logger.critical("Błąd podczas zapisu pliku HTML.", exc_info=True)
