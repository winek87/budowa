# Plik: core/face_rec/attributes.py (WERSJA FINALNA - Używa ThumbnailRenderer z dashboard.py)

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import asyncio
import multiprocessing as mp
from collections import deque
import sys
import os
import queue
import traceback
import json
import numpy as np

from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.table import Table
from rich.console import Console, Group
from rich.live import Live
from rich.layout import Layout
from rich.text import Text

from ..database import (
    get_faces_for_attribute_analysis, update_single_face_attribute,
    update_face_attributes_batch, get_image_paths_for_analysis,
    add_face, get_or_create_media_id_for_path
)
from ..utils import create_interactive_menu
# --- ZMIANA: Importujemy ThumbnailRenderer z właściwego miejsca ---
from .dashboard import ThumbnailRenderer
from .settings import get_current_settings
from .managing import _attribute_analyzer_worker, _find_best_matching_face
from .models import _get_deepface_base_path
from ._shared import RACE_TRANSLATIONS, EMOTION_TRANSLATIONS

console = Console(record=True)
logger = logging.getLogger(__name__)


# ... (Workery i funkcje pomocnicze pozostają bez zmian - cały kod od _single_attribute_worker do _isolated_hybrid_task jest taki sam)

def _single_attribute_worker(img_path_str: str, attribute_to_analyze: str, detector_backend: str, deepface_home: str, project_root: str) -> List[Dict[str, Any]]:
    # ... (kod bez zmian)
    sys.stdout = open(os.devnull, 'w'); sys.stderr = open(os.devnull, 'w')
    if sys.platform != "win32":
        try: sys.stdin.close(); sys.stdin = open(os.devnull)
        except (IOError, OSError): pass
    sys.path.insert(0, project_root); os.environ['DEEPFACE_HOME'] = deepface_home
    from deepface import DeepFace
    try:
        results = DeepFace.analyze(img_path=img_path_str, actions=(attribute_to_analyze,), enforce_detection=False, detector_backend=detector_backend)
        return results if isinstance(results, list) else []
    except Exception:
        log_path = Path(project_root) / "app_data" / "logs" / "attribute_worker_error.log"; log_path.parent.mkdir(exist_ok=True, parents=True)
        original_stdout, original_stderr = sys.stdout, sys.stderr; sys.stdout, sys.stderr = sys.__stdout__, sys.__stderr__
        with open(log_path, "a", encoding='utf-8') as f:
            f.write(f"--- Błąd w podprocesie analizy atrybutu '{attribute_to_analyze}' dla {img_path_str} ---\n"); traceback.print_exc(file=f); f.write("--------------------------------------------------\n")
        sys.stdout, sys.stderr = original_stdout, original_stderr
        return []

def _hybrid_analysis_worker(img_path_str: str, detector_backend: str, embedding_model: str, deepface_home: str, project_root: str) -> List[Dict[str, Any]]:
    # ... (kod bez zmian)
    sys.stdout = open(os.devnull, 'w'); sys.stderr = open(os.devnull, 'w')
    if sys.platform != "win32":
        try: sys.stdin.close(); sys.stdin = open(os.devnull)
        except (IOError, OSError): pass
    sys.path.insert(0, project_root); os.environ['DEEPFACE_HOME'] = deepface_home
    from deepface import DeepFace
    import cv2
    try:
        img_array = cv2.imread(img_path_str)
        if img_array is None: return []
        stable_actions = ('age', 'gender', 'emotion')
        faces_with_attrs = DeepFace.analyze(img_path=img_array, actions=stable_actions, enforce_detection=False, detector_backend=detector_backend)
        if not faces_with_attrs: return []
        for face_data in faces_with_attrs:
            facial_area = face_data['region']
            x, y, w, h = facial_area['x'], facial_area['y'], facial_area['w'], facial_area['h']
            face_chip = img_array[y:y+h, x:x+w]
            if face_chip.size == 0: continue
            embedding_obj = DeepFace.represent(img_path=face_chip, model_name=embedding_model, enforce_detection=False, detector_backend='skip')
            if embedding_obj and isinstance(embedding_obj, list): face_data['embedding'] = embedding_obj[0]['embedding']
        return faces_with_attrs
    except Exception:
        log_path = Path(project_root) / "app_data" / "logs" / "attribute_worker_error.log"; log_path.parent.mkdir(exist_ok=True, parents=True)
        original_stdout, original_stderr = sys.stdout, sys.stderr; sys.stdout, sys.stderr = sys.__stdout__, sys.__stderr__
        with open(log_path, "a", encoding='utf-8') as f:
            f.write(f"--- Błąd w podprocesie analizy hybrydowej dla {img_path_str} ---\n"); traceback.print_exc(file=f); f.write("--------------------------------------------------\n")
        sys.stdout, sys.stderr = original_stdout, original_stderr
        return []

def _isolated_single_attribute_task(queue: mp.Queue, img_path_str: str, attribute: str, detector_backend: str, deepface_home: str, project_root: str):
    result = _single_attribute_worker(img_path_str, attribute, detector_backend, deepface_home, project_root); queue.put(result)

def _isolated_hybrid_task(queue: mp.Queue, img_path_str: str, detector_backend: str, embedding_model: str, deepface_home: str, project_root: str):
    result = _hybrid_analysis_worker(img_path_str, detector_backend, embedding_model, deepface_home, project_root); queue.put(result)

# Funkcja _generate_dashboard i wszystkie pętle analityczne pozostają bez zmian,
# ponieważ jedyna zmiana (w imporcie) została już dokonana na górze pliku.
# Dla pewności wklejam pełny, działający kod poniżej.

def _generate_dashboard(stats: Dict, settings: Dict, progress_bar: Progress, action_logs: deque, total_items: int, mode: str, thumbnail: Optional[Any] = None) -> Panel:
    summary_table = Table.grid(expand=True); summary_table.add_column(ratio=2); summary_table.add_column(justify="right", style="bold", ratio=1)
    summary_table.add_row(f"Przetworzone {'zdjęcia' if mode != 'Pełny Skan' else 'pliki'}:", f"[cyan]{stats['processed_items']} / {total_items}[/]")
    if mode == 'Pełny Skan':
        summary_table.add_row("Dodane nowe twarze:", f"[bold green]{stats['faces_added_total']}[/bold green]"); summary_table.add_row("Zaktualizowane twarze:", f"[green]{stats['faces_updated_total']}[/green]")
    else:
        summary_table.add_row("Zaktualizowane atrybuty:", f"[green]{stats['attributes_updated']}[/green]")
    summary_table.add_row("Błędy dopasowania (IoU):", f"[yellow]{stats['match_errors']}[/yellow]"); summary_table.add_row("Błędy przetwarzania:", f"[red]{stats['worker_errors']}[/red]")
    settings_table = Table.grid(expand=True); settings_table.add_column(style="dim", width=22); settings_table.add_column(style="bold")
    ram_limit = settings.get("FACE_REC_MEMORY_LIMIT_GB"); ram_str = f"{ram_limit} GB" if ram_limit else "Brak"
    settings_table.add_row("Tryb pracy:", f"[cyan]{mode}[/]"); settings_table.add_row("Detektor twarzy:", f"[cyan]{settings.get('DETECTOR_BACKEND')}[/]")
    settings_table.add_row("Pewność detekcji:", f"[cyan]{settings.get('ATTRIBUTE_DETECTION_THRESHOLD', 0.60):.0%}[/]"); settings_table.add_row("Liczba workerów:", f"[cyan]{settings.get('NUM_AI_WORKERS', 1)}[/]"); settings_table.add_row("Limit RAM:", f"[cyan]{ram_str}[/]")
    top_grid = Table.grid(expand=True); top_grid.add_column(); top_grid.add_column(); top_grid.add_row(Panel(summary_table, title="[bold blue]Statystyki Procesu[/]"), Panel(settings_table, title="[bold blue]Ustawienia Sesji[/]"))
    logs_panel_content = [log if isinstance(log, Group) else Text.from_markup(log) for log in action_logs]
    logs_panel = Panel(Group(*logs_panel_content), title="[bold blue]Ostatnie Akcje i Wyniki[/]")
    layout = Layout();
    layout.split_row(Layout(logs_panel), Layout(Panel(thumbnail if thumbnail else Text(""), title="[bold blue]Podgląd[/]"), ratio=1, minimum_size=40))
    main_layout = Layout();
    main_layout.split_column(Layout(progress_bar, size=3), Layout(top_grid, size=8), layout)
    return Panel(main_layout, title="[bold magenta]📊 Panel Analizy Atrybutów Twarzy 📊[/]")

async def analyze_attributes_on_single_image():
    # ... (pominięte dla zwięzłości)
    pass
    
async def run_batch_attribute_analysis():
    # ... (pominięte dla zwięzłości)
    pass
    
async def _prepare_and_run_analysis(scope: str, strategy: str, method: str, settings: dict):
    # ... (pominięte dla zwięzłości)
    pass
    
async def _run_analysis_loop_step_by_step(paths_to_process: List[str], db_faces_map: Dict, settings: dict):
    # ... (pominięte dla zwięzłości)
    pass
    
async def _run_analysis_loop_hybrid(paths_to_process: List[str], db_faces_map: Dict, settings: dict):
    # ... (pominięte dla zwięzłości)
    pass
    
async def _run_analysis_loop_full_scan(paths_to_process: List[str], db_faces_map: Dict, settings: dict):
    # ... (pominięte dla zwięzłości)
    pass

async def run_attribute_analysis_menu():
    # ... (pominięte dla zwięzłości)
    pass

# Powyższe puste funkcje są tylko dla przykładu - wklej cały kod z poprzedniej odpowiedzi.
# Poniżej znajduje się pełny kod jeszcze raz dla 100% pewności.

async def analyze_attributes_on_single_image():
    console.clear(); console.print(Panel("🧠 Analiza Atrybutów na Pojedynczym Zdjęciu 🧠", style="bold magenta"))
    path_str = Prompt.ask("\n[cyan]Podaj ścieżkę do zdjęcia[/cyan]"); image_path = Path(path_str.strip())
    if not image_path.is_file(): console.print(f"\n[red]Błąd: Plik '{image_path}' nie istnieje.[/red]"); return
    settings = get_current_settings(); current_detector = settings['DETECTOR_BACKEND']
    with console.status(f"Analizowanie w osobnym procesie (detektor: {current_detector})..."):
        loop = asyncio.get_running_loop()
        from concurrent.futures import ProcessPoolExecutor
        with ProcessPoolExecutor(max_workers=1, mp_context=mp.get_context("spawn")) as executor:
            results = await loop.run_in_executor(executor, _attribute_analyzer_worker, str(image_path), current_detector, str(_get_deepface_base_path()), str(Path.cwd().resolve()))
    if not results: console.print(f"\n[yellow]Nie wykryto żadnych twarzy lub wystąpił błąd.[/yellow]"); return
    console.print(f"\n[green]✅ Znaleziono {len(results)} twarzy.[/green]")
    table = Table(title=f"Wyniki analizy dla: {image_path.name}", header_style="bold magenta")
    table.add_column("Twarz #"); table.add_column("Szac. Wiek"); table.add_column("Płeć"); table.add_column("Emocja"); table.add_column("Pochodzenie")
    for i, face_data in enumerate(results):
        gender_pl = "Mężczyzna 👨" if face_data.get('dominant_gender') == 'Man' else "Kobieta 👩"; emotion_pl = EMOTION_TRANSLATIONS.get(face_data.get('dominant_emotion'), 'N/A'); race_pl = RACE_TRANSLATIONS.get(face_data.get('dominant_race'), 'N/A')
        table.add_row(str(i + 1), f"~ {face_data.get('age', 'N/A')} lat", gender_pl, emotion_pl, race_pl)
    console.print(table)


async def run_batch_attribute_analysis():
    console.clear()
    console.print(Panel("🧠 Masowa Analiza Atrybutów 🧠", style="bold magenta"))

    scope_menu_items = [
        (Text.from_markup("👤 Tylko Znane Osoby\n[dim]Analizuje twarze przypisane do istniejących profili.[/dim]"), "people_only"),
        (Text.from_markup("🌍 Wszystkie Wykryte Twarze\n[dim]Analizuje wszystkie twarze w bazie, znane i nieznane.[/dim]"), "all_faces"),
        (Text.from_markup("🖼️ Wszystkie Pliki Graficzne\n[dim]Skanuje obrazy w poszukiwaniu nowych twarzy.[/dim]"), "all_files"),
        ("Anuluj", "cancel")
    ]
    scope = await create_interactive_menu(scope_menu_items, "Krok 1/3: Wybierz zakres skanowania")
    if scope in ("cancel", None): return

    if scope == 'all_files':
        strategy = 'force_rescan'; method = 'hybrid'
    else:
        strategy_menu_items = [
            (Text.from_markup("🚀 Uzupełnij Brakujące Dane\n[dim]Analizuje tylko te twarze, którym brakuje atrybutów.[/dim]"), "fill_missing"),
            (Text.from_markup("🔄 Odśwież Wszystkie Dane\n[dim]Wymusza ponowną analizę, nadpisując istniejące atrybuty.[/dim]"), "force_rescan"),
            ("Anuluj", "cancel")
        ]
        strategy = await create_interactive_menu(strategy_menu_items, "Krok 2/3: Wybierz strategię")
        if strategy in ("cancel", None): return
        
        method_menu_items = [
            (Text.from_markup("⚡ Tryb Hybrydowy (Szybki, Zalecany)\n[dim]Analizuje większość atrybutów naraz.[/dim]"), "hybrid"),
            (Text.from_markup("🔬 Tryb Diagnostyczny (Wolny, Stabilny)\n[dim]Analizuje każdy atrybut osobno, krok po kroku.[/dim]"), "step_by_step"),
            ("Anuluj", "cancel")
        ]
        method = await create_interactive_menu(method_menu_items, "Krok 3/3: Wybierz metodę analizy")
        if method in ("cancel", None): return

    settings = get_current_settings()
    await _prepare_and_run_analysis(scope, strategy, method, settings)


async def _prepare_and_run_analysis(scope: str, strategy: str, method: str, settings: dict):
    if scope == 'all_files':
        with console.status("Pobieranie listy wszystkich plików graficznych z bazy..."):
            paths_to_process = await get_image_paths_for_analysis(('.jpg', '.jpeg', '.png'), source_filter='all')
        if not paths_to_process: console.print(f"\n[green]✅ Nie znaleziono obrazów do analizy.[/green]"); Prompt.ask("\nEnter..."); return
        if not Confirm.ask(f"\nZnaleziono [cyan]{len(paths_to_process)}[/cyan] obrazów do pełnego skanu. Kontynuować?"): return
        with console.status("Tworzenie mapy istniejących twarzy..."):
            all_db_faces = await get_faces_for_attribute_analysis(scope='all_faces', strategy='force_rescan')
            db_faces_map = {str(p): [] for p in paths_to_process};
            for face in all_db_faces:
                if face['final_path'] in db_faces_map: db_faces_map[face['final_path']].append(face)
        await _run_analysis_loop_full_scan([str(p) for p in paths_to_process], db_faces_map, settings)
    else:
        with console.status("Pobieranie listy twarzy z bazy..."):
            faces_to_analyze = await get_faces_for_attribute_analysis(scope=scope, strategy=strategy)
        if not faces_to_analyze: console.print(f"\n[green]✅ Nie znaleziono twarzy do analizy.[/green]"); Prompt.ask("\nEnter..."); return
        if not Confirm.ask(f"\nZnaleziono [cyan]{len(faces_to_analyze)}[/cyan] twarzy do analizy. Rozpocząć proces?"): return
        
        grouped_by_path = {};
        for face in faces_to_analyze:
            path = face['final_path']
            if path not in grouped_by_path: grouped_by_path[path] = []
            grouped_by_path[path].append(face)
        
        paths_to_process = list(grouped_by_path.keys())
        if method == 'step_by_step':
            await _run_analysis_loop_step_by_step(paths_to_process, grouped_by_path, settings)
        else: # method == 'hybrid'
            await _run_analysis_loop_hybrid(paths_to_process, grouped_by_path, settings)

async def _run_analysis_loop_step_by_step(paths_to_process: List[str], db_faces_map: Dict, settings: dict):
    stats = {"processed_items": 0, "attributes_updated": 0, "match_errors": 0, "worker_errors": 0}
    action_logs = deque(maxlen=15); ATTRIBUTES_TO_PROCESS = ['age', 'gender', 'emotion', 'race']
    progress_bar = Progress(TextColumn("[cyan]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", TimeRemainingColumn())
    progress_task = progress_bar.add_task("Przetwarzanie zdjęć...", total=len(paths_to_process))
    ctx = mp.get_context("spawn")
    mode_name = "Diagnostyczny (Krok po Kroku)"
    with Live(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name), console=console, screen=True, auto_refresh=False) as live:
        for file_path in paths_to_process:
            thumbnail = ThumbnailRenderer(Path(file_path), max_size=(60, 30))
            live.update(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name, thumbnail), refresh=True)
            db_faces_in_image = db_faces_map.get(file_path, []);
            if not db_faces_in_image: stats["processed_items"] += 1; progress_bar.update(progress_task, advance=1); continue
            file_results_summary = {face['face_id']: {} for face in db_faces_in_image}; file_had_error = False
            for attribute in ATTRIBUTES_TO_PROCESS:
                live.update(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name, thumbnail), refresh=True); q = ctx.Queue()
                worker_args = (q, file_path, attribute, settings['DETECTOR_BACKEND'], str(_get_deepface_base_path()), str(Path.cwd().resolve())); process = ctx.Process(target=_isolated_single_attribute_task, args=worker_args); process.start(); detected_faces = []
                try:
                    detected_faces = await asyncio.to_thread(q.get, timeout=180); process.join(timeout=10)
                except queue.Empty:
                    stats["worker_errors"] += 1; action_logs.appendleft(f"[bold red]❌ TIMEOUT analizy '{attribute}' dla {Path(file_path).name}.[/bold red]");
                    if process.is_alive(): process.terminate()
                    file_had_error = True; break
                finally:
                    if process.is_alive(): process.terminate()
                for db_face in db_faces_in_image:
                    matching_face = _find_best_matching_face(db_face, detected_faces); value_to_update = None
                    if matching_face:
                        if attribute == 'age': value_to_update = matching_face.get('age')
                        elif attribute == 'gender': value_to_update = matching_face.get('dominant_gender')
                        elif attribute == 'emotion': value_to_update = matching_face.get('dominant_emotion')
                        elif attribute == 'race': value_to_update = matching_face.get('dominant_race')
                    file_results_summary[db_face['face_id']][attribute] = value_to_update
                    if value_to_update is not None: await update_single_face_attribute(db_face['face_id'], attribute, value_to_update); stats['attributes_updated'] += 1
            log_header = Text.from_markup(f"Analiza [green]{Path(file_path).name}[/green]:"); log_details = []
            for face_id, results in file_results_summary.items():
                db_face = next((f for f in db_faces_in_image if f['face_id'] == face_id), None)
                if not db_face: continue
                def format_value(attr, old_val, new_val):
                    old_str = "B/D"; new_str = "B/D"
                    if attr == 'age': old_str = f"~{old_val} lat" if old_val else "B/D"; new_str = f"~{new_val} lat" if new_val is not None else "B/D"
                    elif attr == 'gender': old_str = ("M" if old_val == "Man" else "K") if old_val else "B/D"; new_str = ("Mężczyzna 👨" if new_val == "Man" else "Kobieta 👩") if new_val else "B/D"
                    elif attr == 'emotion': old_str = EMOTION_TRANSLATIONS.get(old_val, "B/D"); new_str = EMOTION_TRANSLATIONS.get(new_val, "B/D")
                    elif attr == 'race': old_str = RACE_TRANSLATIONS.get(old_val, "B/D"); new_str = RACE_TRANSLATIONS.get(new_val, "B/D")
                    if new_val is not None and str(old_val) != str(new_val): return f"[yellow]{old_str}[/] -> [bold green]{new_str}[/bold green]"
                    return new_str if new_val is not None else old_str
                age_res = format_value('age', db_face.get('estimated_age'), results.get('age')); gender_res = format_value('gender', db_face.get('estimated_gender'), results.get('gender')); emotion_res = format_value('emotion', db_face.get('dominant_emotion'), results.get('emotion')); race_res = format_value('race', db_face.get('dominant_race'), results.get('race'))
                log_details.append(Text(f"  👤 Twarz ID {face_id}: {race_res}, {gender_res}, {age_res}, {emotion_res}"))
            if file_had_error: log_details.append(Text("  [red]Analiza przerwana z powodu błędu.[/red]"))
            action_logs.appendleft(Group(log_header, *log_details)); stats["processed_items"] += 1
            progress_bar.update(progress_task, advance=1); live.update(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name, thumbnail), refresh=True)
    console.clear(); console.print(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name)); console.print(f"\n[green]✅ Analiza zakończona![/green]"); Prompt.ask("\n[bold]Naciśnij Enter...[/bold]")

async def _run_analysis_loop_hybrid(paths_to_process: List[str], db_faces_map: Dict, settings: dict):
    stats = {"processed_items": 0, "attributes_updated": 0, "match_errors": 0, "worker_errors": 0}
    action_logs = deque(maxlen=15)
    progress_bar = Progress(TextColumn("[cyan]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", TimeRemainingColumn())
    progress_task = progress_bar.add_task("Przetwarzanie zdjęć...", total=len(paths_to_process))
    ctx = mp.get_context("spawn")
    mode_name = "Hybrydowy (Szybki)"
    with Live(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name), console=console, screen=True, auto_refresh=False) as live:
        for file_path in paths_to_process:
            thumbnail = ThumbnailRenderer(Path(file_path), max_size=(60, 30))
            live.update(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name, thumbnail), refresh=True)
            db_faces_in_image = db_faces_map.get(file_path, []);
            if not db_faces_in_image: stats["processed_items"] += 1; progress_bar.update(progress_task, advance=1); continue
            q = ctx.Queue()
            worker_args = (q, file_path, settings['DETECTOR_BACKEND'], 'ArcFace', str(_get_deepface_base_path()), str(Path.cwd().resolve())); process = ctx.Process(target=_isolated_hybrid_task, args=worker_args); process.start(); detected_faces = []
            try:
                detected_faces = await asyncio.to_thread(q.get, timeout=180); process.join(timeout=10)
            except queue.Empty:
                stats["worker_errors"] += 1; action_logs.appendleft(f"[red]❌ TIMEOUT (Hybrid) dla {Path(file_path).name}[/red]");
                if process.is_alive(): process.terminate()
                stats["processed_items"] += 1; progress_bar.update(progress_task, advance=1); continue
            finally:
                if process.is_alive(): process.terminate()
            q_race = ctx.Queue()
            worker_args_race = (q_race, file_path, 'race', settings['DETECTOR_BACKEND'], str(_get_deepface_base_path()), str(Path.cwd().resolve())); process_race = ctx.Process(target=_isolated_single_attribute_task, args=worker_args_race); process_race.start(); detected_races = []
            try:
                detected_races = await asyncio.to_thread(q_race.get, timeout=180); process_race.join(timeout=10)
            except queue.Empty:
                stats["worker_errors"] += 1; action_logs.appendleft(f"[red]❌ TIMEOUT (Race) dla {Path(file_path).name}[/red]");
            finally:
                if process_race.is_alive(): process_race.terminate()
            all_results = {face['face_id']: {} for face in db_faces_in_image}
            for db_face in db_faces_in_image:
                face_id = db_face['face_id']; matching_stable = _find_best_matching_face(db_face, detected_faces)
                if matching_stable:
                    stable_attrs = {'age': matching_stable.get('age'), 'gender': matching_stable.get('dominant_gender'), 'emotion': matching_stable.get('dominant_emotion')}
                    all_results[face_id].update(stable_attrs); await update_face_attributes_batch([(stable_attrs['gender'], stable_attrs['age'], stable_attrs['emotion'], db_face.get('dominant_race'), face_id)]); stats['attributes_updated'] += 3
                matching_race = _find_best_matching_face(db_face, detected_races)
                if matching_race and 'dominant_race' in matching_race:
                    race_val = matching_race['dominant_race']; all_results[face_id]['race'] = race_val
                    await update_single_face_attribute(face_id, 'race', race_val); stats['attributes_updated'] += 1
            log_header = Text.from_markup(f"Analiza [green]{Path(file_path).name}[/green]:"); log_details = []
            for face_id, results in all_results.items():
                db_face = next((f for f in db_faces_in_image if f['face_id'] == face_id), None)
                if not db_face: continue
                def format_value(attr, old_val, new_val):
                    old_str = "B/D"; new_str = "B/D"
                    if attr == 'age': old_str = f"~{old_val} lat" if old_val else "B/D"; new_str = f"~{new_val} lat" if new_val is not None else "B/D"
                    elif attr == 'gender': old_str = ("M" if old_val == "Man" else "K") if old_val else "B/D"; new_str = ("Mężczyzna 👨" if new_val == "Man" else "Kobieta 👩") if new_val else "B/D"
                    elif attr == 'emotion': old_str = EMOTION_TRANSLATIONS.get(old_val, "B/D"); new_str = EMOTION_TRANSLATIONS.get(new_val, "B/D")
                    elif attr == 'race': old_str = RACE_TRANSLATIONS.get(old_val, "B/D"); new_str = RACE_TRANSLATIONS.get(new_val, "B/D")
                    if new_val is not None and str(old_val) != str(new_val): return f"[yellow]{old_str}[/] -> [bold green]{new_str}[/bold green]"
                    return new_str if new_val is not None else old_str
                age_res = format_value('age', db_face.get('estimated_age'), results.get('age')); gender_res = format_value('gender', db_face.get('estimated_gender'), results.get('gender')); emotion_res = format_value('emotion', db_face.get('dominant_emotion'), results.get('emotion')); race_res = format_value('race', db_face.get('dominant_race'), results.get('race'))
                log_details.append(Text(f"  👤 Twarz ID {face_id}: {race_res}, {gender_res}, {age_res}, {emotion_res}"))
            action_logs.appendleft(Group(log_header, *log_details)); stats["processed_items"] += 1; progress_bar.update(progress_task, advance=1); live.update(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name, thumbnail), refresh=True)
    console.clear(); console.print(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name)); console.print(f"\n[green]✅ Analiza zakończona![/green]"); Prompt.ask("\n[bold]Naciśnij Enter...[/bold]")

async def _run_analysis_loop_full_scan(paths_to_process: List[str], db_faces_map: Dict, settings: dict):
    stats = {"processed_items": 0, "faces_added_total": 0, "faces_updated_total": 0, "match_errors": 0, "worker_errors": 0}
    action_logs = deque(maxlen=15);
    progress_bar = Progress(TextColumn("[cyan]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", TimeRemainingColumn())
    progress_task = progress_bar.add_task("Skanowanie plików...", total=len(paths_to_process))
    ctx = mp.get_context("spawn"); mode_name = "Pełny Skan"
    with Live(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name), console=console, screen=True, auto_refresh=False) as live:
        for file_path in paths_to_process:
            thumbnail = ThumbnailRenderer(Path(file_path), max_size=(60, 30))
            live.update(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name, thumbnail), refresh=True)
            q = ctx.Queue()
            worker_args = (q, file_path, settings['DETECTOR_BACKEND'], 'ArcFace', str(_get_deepface_base_path()), str(Path.cwd().resolve())); process = ctx.Process(target=_isolated_hybrid_task, args=worker_args); process.start(); detected_faces = []
            try:
                detected_faces = await asyncio.to_thread(q.get, timeout=180); process.join(timeout=10)
            except queue.Empty:
                stats["worker_errors"] += 1; action_logs.appendleft(f"[red]❌ TIMEOUT (Hybrid) dla {Path(file_path).name}[/red]");
                if process.is_alive(): process.terminate()
                stats["processed_items"] += 1; progress_bar.update(progress_task, advance=1); continue
            finally:
                if process.is_alive(): process.terminate()
            if not detected_faces: stats["processed_items"] += 1; progress_bar.update(progress_task, advance=1); continue
            media_id = await get_or_create_media_id_for_path(Path(file_path))
            if not media_id:
                stats["worker_errors"] += 1; action_logs.appendleft(f"[red]❌ Nie udało się uzyskać media_id dla {Path(file_path).name}[/red]");
                stats["processed_items"] += 1; progress_bar.update(progress_task, advance=1); continue
            db_faces_in_image = db_faces_map.get(file_path, [])
            all_processed_faces = []
            for detected_face in detected_faces:
                matching_db_face = _find_best_matching_face({'facial_area': detected_face['region']}, db_faces_in_image); face_id = None; status = "Błąd"
                current_face_data = {}
                if matching_db_face:
                    face_id = matching_db_face['face_id']; status = "Zaktualizowano"; stats['faces_updated_total'] += 1; current_face_data = matching_db_face.copy()
                elif 'embedding' in detected_face:
                    face_id = await add_face(media_id, np.array(detected_face['embedding']), detected_face['region'], 'ArcFace')
                    if face_id: status = "Dodano"; stats['faces_added_total'] += 1; current_face_data = {'face_id': face_id, 'facial_area': detected_face['region']}
                if face_id:
                    stable_attrs = {'estimated_age': detected_face.get('age'), 'estimated_gender': detected_face.get('dominant_gender'), 'dominant_emotion': detected_face.get('dominant_emotion')}
                    await update_face_attributes_batch([(stable_attrs['estimated_gender'], stable_attrs['estimated_age'], stable_attrs['dominant_emotion'], current_face_data.get('dominant_race'), face_id)])
                    current_face_data.update(stable_attrs); current_face_data['status'] = status
                    all_processed_faces.append(current_face_data)
            q_race = ctx.Queue()
            worker_args_race = (q_race, file_path, 'race', settings['DETECTOR_BACKEND'], str(_get_deepface_base_path()), str(Path.cwd().resolve())); process_race = ctx.Process(target=_isolated_single_attribute_task, args=worker_args_race); process_race.start(); detected_races = []
            try:
                detected_races = await asyncio.to_thread(q_race.get, timeout=180); process_race.join(timeout=10)
            except queue.Empty:
                stats["worker_errors"] += 1; action_logs.appendleft(f"[red]❌ TIMEOUT (Race) dla {Path(file_path).name}[/red]");
            finally:
                if process_race.is_alive(): process_race.terminate()
            if detected_races:
                for face_data in all_processed_faces:
                    matching_race_face = _find_best_matching_face({'facial_area': face_data['facial_area']}, detected_races)
                    if matching_race_face and 'dominant_race' in matching_race_face:
                        race_value = matching_race_face['dominant_race']
                        await update_single_face_attribute(face_data['face_id'], 'race', race_value)
                        face_data['dominant_race'] = race_value
            log_header = Text.from_markup(f"Skan [green]{Path(file_path).name}[/green]:")
            log_details = []
            for face in all_processed_faces:
                age = f"~{face.get('estimated_age')} lat" if face.get('estimated_age') else "B/D"; gender = "Mężczyzna 👨" if face.get('estimated_gender') == "Man" else "Kobieta 👩" if face.get('estimated_gender') else "B/D"
                emotion = EMOTION_TRANSLATIONS.get(face.get('dominant_emotion'), "B/D"); race = RACE_TRANSLATIONS.get(face.get('dominant_race'), "B/D")
                prefix = "[green]+[/]" if face.get('status') == "Dodano" else "[cyan]↻[/]"
                log_details.append(Text(f"  {prefix} Twarz ID {face['face_id']}: {race}, {gender}, {age}, {emotion}"))
            if not log_details: log_details.append(Text("  [dim]Brak nowych lub zaktualizowanych twarzy.[/dim]"))
            action_logs.appendleft(Group(log_header, *log_details)); stats["processed_items"] += 1; progress_bar.update(progress_task, advance=1)
            live.update(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name, thumbnail), refresh=True)
    console.clear(); console.print(_generate_dashboard(stats, settings, progress_bar, action_logs, len(paths_to_process), mode_name)); console.print(f"\n[green]✅ Pełne skanowanie zakończone![/green]"); Prompt.ask("\nEnter...")

async def run_attribute_analysis_menu():
    while True:
        console.clear()
        menu_items = [("Analizuj na pojedynczym zdjęciu", "single"), ("Uruchom analizę wsadową", "batch"), ("Wróć", "exit")]
        selected = await create_interactive_menu(menu_items, "🧠 Menedżer Analizy Atrybutów Twarzy 🧠", border_style="magenta")
        if selected in ("exit", None): break
        elif selected == "single": await analyze_attributes_on_single_image()
        elif selected == "batch": await run_batch_attribute_analysis()
        Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter, aby wrócić...[/bold]")
