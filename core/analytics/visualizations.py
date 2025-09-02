# -*- coding: utf-8 -*-

# plik: core/analytics/visualizations.py
# Wersja 2.2 - W pełni asynchroniczny, z interaktywną mapą HTML (Folium)
#
# ##############################################################################
# ===                MODUŁ WIZUALIZACJI DANYCH ANALITYCZNYCH                 ===
# ##############################################################################

import logging
import math
import json
import os
import asyncio
import webbrowser
from pathlib import Path
from datetime import datetime
from collections import Counter

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.box import HEAVY
from rich.align import Align
from rich.prompt import Confirm

from .data_loader import get_all_media_entries
from ..config_editor_logic import get_key

console = Console(record=True)
logger = logging.getLogger(__name__)

DAY_NAMES = {
    0: "Poniedziałek", 1: "Wtorek", 2: "Środa", 3: "Czwartek",
    4: "Piątek", 5: "Sobota", 6: "Niedziela"
}


async def show_activity_heatmap():
    """
    Analizuje aktywność fotograficzną i wyświetla ją jako graficzną mapę cieplną.
    """
    console.clear()
    logger.info("Uruchamiam generator Mapy Cieplnej Aktywności...")
    console.print(Panel("🔥 Mapa Cieplna Aktywności Fotograficznej 🔥", expand=False, style="bold green"))
    
    with console.status("[cyan]Pobieranie danych do analizy...[/]"):
        all_media_data = await get_all_media_entries()
    
    if not all_media_data:
        logger.warning("Brak danych do analizy mapy cieplnej."); console.print("\n[yellow]Nie znaleziono danych do analizy.[/yellow]"); return

    logger.debug("Agregowanie danych o aktywności...")
    activity_counter = Counter((entry['dt'].weekday(), entry['dt'].hour) for entry in all_media_data)
    if not activity_counter:
        logger.warning("Nie znaleziono danych czasowych w bazie."); console.print("\n[yellow]Brak informacji o datach i godzinach.[/yellow]"); return

    max_activity = max(activity_counter.values())
    (peak_day_idx, peak_hour), peak_count = activity_counter.most_common(1)[0]
    logger.info(f"Znaleziono szczyt aktywności: {peak_count} plików w {DAY_NAMES.get(peak_day_idx)} o {peak_hour}:00.")

    HEATMAP_LEVELS = [
        (0.0, " ", "rgb(30,30,30)", "Brak aktywności"), (0.01, "▂", "#0e4429", "Bardzo niska"),
        (0.1, "▄", "#006d32", "Niska"), (0.3, "▆", "#26a641", "Średnia"), (0.6, "█", "#39d353", "Wysoka")
    ]

    def get_symbol_and_style(count: int, max_val: int) -> tuple[str, str]:
        if count == 0: return HEATMAP_LEVELS[0][1], HEATMAP_LEVELS[0][2]
        percentage = count / max_val
        for threshold, symbol, color, _ in reversed(HEATMAP_LEVELS):
            if percentage >= threshold: return symbol, color
        return HEATMAP_LEVELS[1][1], HEATMAP_LEVELS[1][2]

    table = Table(box=HEAVY, expand=True, padding=0, show_header=True, header_style="bold magenta")
    table.add_column("Dzień", style="cyan", justify="right", no_wrap=True)
    for hour in range(24): table.add_column(f"{hour:02d}", justify="center", width=3)
    for day_index in range(7):
        row_cells = [DAY_NAMES.get(day_index, '')]
        for hour_index in range(24):
            count = activity_counter.get((day_index, hour_index), 0)
            symbol, color = get_symbol_and_style(count, max_activity)
            row_cells.append(Text(symbol * 2, style=color))
        table.add_row(*row_cells)

    summary_text = Text.from_markup(f"Największa aktywność: [magenta]{peak_count}[/] plików w [cyan]{DAY_NAMES.get(peak_day_idx, '')}[/] około [magenta]{peak_hour}:00[/].", justify="center")
    legend_grid = Table.grid(padding=(0, 2), expand=False)
    legend_grid.add_column(); legend_grid.add_column()
    legend_grid.add_row(Text("Legenda:", style="bold")); legend_grid.add_row()
    for _, symbol, color, description in HEATMAP_LEVELS:
        legend_grid.add_row(Text(symbol * 2, style=color), Text(f" - {description}", style="dim"))

    console.print(Panel(Group(summary_text, "\n", table, "\n", Align.center(legend_grid)),
                        title=f"Aktywność fotograficzna ({len(all_media_data)} plików)", border_style="green", padding=(1, 1)))


# plik: core/analytics/visualizations.py

async def show_world_map1():
    """
    Generuje i wyświetla ulepszoną mapę świata ASCII z nazwami lokalizacji.
    """
    console.clear()
    logger.info("Uruchamiam generator Mapy Świata...")
    console.print(Panel("🗺️ Mapa Świata Twoich Zdjęć 🗺️", expand=False, style="bold green"))

    try:
        from geopy.geocoders import Nominatim
        from geopy.extra.rate_limiter import RateLimiter
    except ImportError:
        logger.error("Brak 'geopy'."); console.print(Panel("[bold red]Błąd: 'geopy'![/bold red]\nUruchom: [cyan]pip install geopy[/cyan]", title="Instrukcja Instalacji")); return

    CACHE_FILE = Path("app_data/location_cache.json")
    location_cache = {}
    if await asyncio.to_thread(CACHE_FILE.exists):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f: location_cache = json.load(f)
        except (json.JSONDecodeError, IOError):
            logger.warning("Plik cache'a lokalizacji jest uszkodzony.")

    all_media_data = await get_all_media_entries()
    location_counts = Counter(e['Location'] for e in all_media_data if e.get('Location'))
    if not location_counts:
        logger.warning("Brak danych o lokalizacjach."); console.print("\n[yellow]Nie znaleziono danych o lokalizacjach.[/yellow]"); return

    geolocator = Nominatim(user_agent=f"gp_toolkit_analytics/{datetime.now().timestamp()}")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.1); reverse_geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1.1)

    locations_to_geocode = [loc for loc in location_counts if loc not in location_cache]
    if locations_to_geocode:
        logger.info(f"Rozpoczynam geokodowanie {len(locations_to_geocode)} nowych lokalizacji...")
        with console.status("[cyan]Geokodowanie nowych lokalizacji...[/]") as status:
            for i, loc_str in enumerate(locations_to_geocode):
                status.update(f"[cyan]Geokodowanie: {loc_str} ({i+1}/{len(locations_to_geocode)})[/]")
                cached_entry = None
                try:
                    lat_str, lon_str = loc_str.replace(" ", "").split(',')
                    lat, lon = float(lat_str), float(lon_str)
                    location = await asyncio.to_thread(reverse_geocode, (lat, lon), language='pl')
                    if location and location.raw.get('address'):
                        address = location.raw['address']
                        city = address.get('city', address.get('town', address.get('village', '')))
                        country = address.get('country', '')
                        display_name = f"{city}, {country}".strip(", ") if city and country else location.address
                        cached_entry = (lat, lon, display_name if display_name else loc_str)
                except (ValueError, AttributeError):
                    try:
                        location = await asyncio.to_thread(geocode, loc_str, language='pl')
                        if location and location.raw.get('address'):
                            address = location.raw['address']
                            city = address.get('city', address.get('town', address.get('village', '')))
                            country = address.get('country', '')
                            display_name = f"{city}, {country}".strip(", ") if city and country else location.address
                            cached_entry = (location.latitude, location.longitude, display_name if display_name else loc_str)
                    except Exception: pass
                location_cache[loc_str] = cached_entry

    # Zapisz zaktualizowany cache
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(CACHE_FILE, 'w', encoding='utf-8') as f: json.dump(location_cache, f, ensure_ascii=False, indent=2)
    except IOError: logger.error("Nie udało się zapisać pliku cache.", exc_info=True)

    geocoded_locations = {loc: {'coords': coords, 'count': location_counts[loc]} for loc, coords in location_cache.items() if coords}
    
    # NOWA, LEPSZA MAPA ASCII
    MAP_STR = """
+--------------------------------------------------------------------------------------------------+
|                                                                                                  |
|    /----\\              ,--.                /-----\\                                                 |
|   /      \\            /    \\              /       \\                                                |
|  /        `----------'      `------------'         \\        /`--.\\                                 |
| /                                                   `------'     \\                                |
|/                                                                  `----.                          |
|                                                                        `---.                       |
|                                                                             \\                      |
|                                                                              |                     |
|                                                                              |                     |
|                                                                              |                     |
+--------------------------------------------------------------------------------------------------+
"""
    map_lines = MAP_STR.strip().split('\n')
    map_height = len(map_lines); map_width = len(map_lines[0]) if map_height > 0 else 0
    map_grid = [list(line) for line in map_lines]
    max_count = max(data['count'] for data in geocoded_locations.values()) if geocoded_locations else 1

    def get_marker_for_count(count: int, max_val: int) -> str:
        p = count / max_val
        if p > 0.7: return "[bold #FFD700]*[/]";
        if p > 0.3: return "[bold #FF4500]O[/]";
        if p > 0.1: return "[#DC143C]o[/]";
        return "[dim].[/]"

    for data in geocoded_locations.values():
        if not data['coords']: continue
        lat, lon, _ = data['coords']
        x = int((lon + 180) / 360 * (map_width - 2)) + 1
        y = int((-lat + 90) / 180 * (map_height - 2)) + 1
        if 1 <= y < map_height - 1 and 1 <= x < map_width - 1:
            map_grid[y][x] = get_marker_for_count(data['count'], max_count)

    final_map = "\n".join("".join(row) for row in map_grid)

    legend = Table.grid(expand=True); legend.add_column(); legend.add_column()
    legend.add_row(Text.from_markup(get_marker_for_count(1, 1000)), " Mało zdjęć")
    legend.add_row(Text.from_markup(get_marker_for_count(150, 1000)), " Średnio zdjęć")
    legend.add_row(Text.from_markup(get_marker_for_count(400, 1000)), " Dużo zdjęć")
    legend.add_row(Text.from_markup(get_marker_for_count(800, 1000)), " Najwięcej zdjęć")
    
    display_name_counts = Counter()
    for loc_str, count in location_counts.items():
        cached_data = location_cache.get(loc_str)
        display_name = cached_data[2] if cached_data and len(cached_data) > 2 else loc_str
        display_name_counts[display_name] += count

    top_locations_table = Table(title="TOP 10 Lokalizacji", title_justify="left", box=None, show_header=False)
    top_locations_table.add_column("Lokalizacja", style="cyan", no_wrap=True, max_width=50)
    top_locations_table.add_column("Zdjęć", justify="right")
    for display_name, count in display_name_counts.most_common(10):
        top_locations_table.add_row(display_name, str(count))

    console.print(Panel(
        Group(Text(final_map, justify="center"), "\n", Align.center(legend), "\n", Align.center(top_locations_table)),
        border_style="green", padding=(1, 2), title=f"Mapa Aktywności Fotograficznej ({len(display_name_counts)} unikalnych lokalizacji)"
    ))

async def show_world_map():
    """Generuje interaktywną mapę świata w pliku HTML z zaznaczonymi lokalizacjami."""
    console.clear()
    logger.info("Uruchamiam generator Mapy Świata...")
    console.print(Panel("🗺️ Generowanie Interaktywnej Mapy Świata 🗺️", expand=False, style="bold green"))

    try:
        from geopy.geocoders import Nominatim
        from geopy.extra.rate_limiter import RateLimiter
        import folium
        from folium.plugins import MarkerCluster
    except ImportError:
        logger.error("Brak wymaganych bibliotek. Zainstaluj 'geopy' i 'folium'.")
        console.print(Panel("[bold red]Błąd![/bold red]\nUruchom: [cyan]pip install geopy folium[/cyan]", title="Instrukcja Instalacji")); return

    CACHE_FILE = Path("app_data/location_cache.json")
    location_cache = {}
    if await asyncio.to_thread(CACHE_FILE.exists):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f: location_cache = json.load(f)
            logger.info(f"Wczytano {len(location_cache)} pozycji z pliku cache lokalizacji.")
        except (json.JSONDecodeError, IOError): logger.warning("Plik cache'a lokalizacji jest uszkodzony.")

    all_media_data = await get_all_media_entries()
    location_counts = Counter(e['Location'] for e in all_media_data if e.get('Location'))
    if not location_counts:
        logger.warning("Brak danych o lokalizacjach."); console.print("\n[yellow]Nie znaleziono danych o lokalizacjach.[/yellow]"); return

    geolocator = Nominatim(user_agent=f"gp_toolkit_analytics/{datetime.now().timestamp()}"); geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.1); reverse_geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1.1)
    
    locations_to_geocode = [loc for loc in location_counts if loc not in location_cache]
    if locations_to_geocode:
        logger.info(f"Rozpoczynam geokodowanie {len(locations_to_geocode)} nowych lokalizacji...")
        with console.status("[cyan]Geokodowanie nowych lokalizacji...[/]") as status:
            for i, loc_str in enumerate(locations_to_geocode):
                status.update(f"[cyan]Geokodowanie: {loc_str} ({i+1}/{len(locations_to_geocode)})[/]")
                cached_entry = None
                try:
                    lat_str, lon_str = loc_str.replace(" ", " ").split(',')
                    lat, lon = float(lat_str), float(lon_str)
                    location = await asyncio.to_thread(reverse_geocode, (lat, lon), language='pl')
                    cached_entry = (lat, lon, location.address if location else loc_str)
                except (ValueError, AttributeError):
                    try:
                        location = await asyncio.to_thread(geocode, loc_str, language='pl')
                        if location: cached_entry = (location.latitude, location.longitude, location.address)
                    except Exception: pass
                location_cache[loc_str] = cached_entry
        try:
            with open(CACHE_FILE, 'w', encoding='utf-8') as f: json.dump(location_cache, f, ensure_ascii=False, indent=2)
            logger.info("Zaktualizowano plik cache lokalizacji.")
        except IOError: logger.error("Nie udało się zapisać pliku cache.", exc_info=True)

    geocoded_locations = {loc: {'coords': coords, 'count': location_counts[loc]} for loc, coords in location_cache.items() if coords}
    if not geocoded_locations:
        logger.warning("Nie udało się zgeokodować żadnych lokalizacji."); console.print("[yellow]Nie udało się uzyskać współrzędnych.[/yellow]"); return
        
    with console.status("[cyan]Tworzenie interaktywnej mapy HTML...[/]"):
        avg_lat = sum(c['coords'][0] for c in geocoded_locations.values()) / len(geocoded_locations)
        avg_lon = sum(c['coords'][1] for c in geocoded_locations.values()) / len(geocoded_locations)
        world_map = folium.Map(location=[avg_lat, avg_lon], zoom_start=4)
        marker_cluster = MarkerCluster().add_to(world_map)
        for loc_str, data in geocoded_locations.items():
            lat, lon, display_name = data['coords']
            popup_text = f"<b>{display_name}</b><br>Zdjęć: {data['count']}"
            folium.Marker(location=[lat, lon], popup=popup_text, tooltip=display_name).add_to(marker_cluster)
        reports_dir = Path("app_data/reports"); reports_dir.mkdir(parents=True, exist_ok=True)
        map_filename = reports_dir / f"mapa_swiata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        await asyncio.to_thread(world_map.save, str(map_filename))

    console.print(Panel(f"[green]Sukces![/green] Interaktywna mapa została zapisana w pliku:\n[cyan]{map_filename.resolve()}[/cyan]"))
    display_name_counts = Counter()
    for loc_str, count in location_counts.items():
        cached_data = location_cache.get(loc_str)
        display_name = cached_data[2] if cached_data and len(cached_data) > 2 else loc_str
        display_name_counts[display_name] += count
    top_locations_table = Table(title="TOP 10 Lokalizacji", title_justify="left", box=None)
    top_locations_table.add_column("Lokalizacja", style="cyan", no_wrap=True, max_width=60); top_locations_table.add_column("Zdjęć", justify="right") # 60 usunac
    for display_name, count in display_name_counts.most_common(10): top_locations_table.add_row(display_name, str(count))
    console.print(top_locations_table)
    if Confirm.ask("\n[cyan]Czy chcesz otworzyć mapę w przeglądarce teraz?[/cyan]"):
        # Konwertuj ścieżkę na absolutną PRZED utworzeniem URI
        absolute_path = map_filename.resolve()
        webbrowser.open(absolute_path.as_uri())

async def show_description_word_cloud():
    """Generuje i zapisuje do pliku obraz chmury słów z opisów zdjęć."""
    console.clear()
    logger.info("Uruchamiam generator Chmury Słów z opisów.")
    console.print(Panel("☁️ Chmura Słów z Opisów Zdjęć ☁️", expand=False, style="bold green"))

    try:
        from wordcloud import WordCloud
    except ImportError:
        logger.error("Brak 'wordcloud'."); console.print(Panel("[bold red]Błąd: 'wordcloud'![/bold red]\nUruchom: [cyan]pip install wordcloud matplotlib[/cyan]", title="Instrukcja Instalacji")); return

    with console.status("[cyan]Agregowanie tekstów z opisów...[/]"):
        all_media_data = await get_all_media_entries()
        all_descriptions_text = " ".join(e['Description'] for e in all_media_data if e.get('Description') and isinstance(e['Description'], str))

    if not all_descriptions_text.strip():
        logger.warning("Nie znaleziono opisów do wygenerowania chmury słów."); console.print("\n[yellow]Nie znaleziono żadnych opisów w bazie danych.[/yellow]"); return

    with console.status("[cyan]Generowanie obrazu chmury słów (może potrwać)...[/]", spinner="dots"):
        stop_words = {'i', 'w', 'na', 'z', 'o', 'do', 'jest', 'to', 'się', 'oraz', 'ale', 'czy', 'był', 'była', 'było', 'były', 'są', 'nie', 'tak', 'jak', 'za', 'już', 'dnia'}
        def generate():
            wordcloud = WordCloud(width=1920, height=1080, background_color="white", stopwords=stop_words,
                                  collocations=False, min_font_size=12, max_words=300, contour_width=3, contour_color='steelblue').generate(all_descriptions_text)
            output_dir = Path("app_data/reports")
            output_dir.mkdir(parents=True, exist_ok=True)
            filename = output_dir / f"chmura_slow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            wordcloud.to_file(filename); return filename
        try:
            loop = asyncio.get_running_loop(); output_path = await loop.run_in_executor(None, generate); success = True
        except Exception as e:
            logger.critical(f"Błąd podczas generowania pliku chmury słów: {e}", exc_info=True); success = False

    if success:
        logger.info(f"Pomyślnie zapisano chmurę słów do: {output_path}")
        console.print(Panel(f"[bold green]Sukces![/bold green]\n\nChmura słów została zapisana w:\n[cyan]{output_path.resolve()}[/cyan]",
                            title="Generowanie Zakończone", border_style="green"))
    else:
        console.print(Panel("[bold red]Błąd![/bold red]\nNie udało się wygenerować chmury słów. Sprawdź logi.", title="Błąd", border_style="red"))
