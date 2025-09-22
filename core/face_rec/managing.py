# Plik: core/face_rec/managing.py (Wersja 2.9 - Naprawiony cykliczny import)

import asyncio
import logging
import multiprocessing as mp
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ProcessPoolExecutor
import sys
import os
import tempfile
import numpy as np
import queue
from datetime import datetime

try:
    from PIL import Image
except ImportError:
    Image = None

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.progress import Progress
from rich.table import Table
from rich.text import Text
from rich.live import Live
from rich.console import Group

from ..utils import create_interactive_menu, open_image_viewer, get_key
from ..database import (
    add_person, update_person_name, delete_person,
    get_all_people_with_status, update_person_status, reassign_faces,
    delete_faces_for_person, get_person_details, get_all_people,
    get_all_embeddings_for_person, update_master_embedding,
    get_all_people_with_attributes, get_media_id_by_path,
    get_face_entries_for_person, add_local_file_entry
)
from .models import _get_deepface_base_path
from .settings import get_current_settings
# --- ZMIANA: Importujemy ze _shared.py ---
from ._shared import RACE_TRANSLATIONS, EMOTION_TRANSLATIONS

console = Console(record=True)
logger = logging.getLogger(__name__)

# Reszta pliku pozostaje bez zmian, ale wklejam ją dla pewności

async def run_face_analysis_on_image(
    image_path: Path,
    model_name: str
) -> tuple[bool, List[Dict[str, Any]] | str]:
    """Uruchamia analizę twarzy na jednym obrazie, dynamicznie wybierając workera."""
    settings = get_current_settings()
    detector_backend = settings["DETECTOR_BACKEND"]
    
    spawn_ctx = mp.get_context("spawn")
    task_queue, result_queue = spawn_ctx.Queue(), spawn_ctx.Queue()
    log_queue = spawn_ctx.Queue()
    
    worker_process = None
    
    try:
        if detector_backend == 'hailo':
            from .hailo_worker import hailo_face_worker
            hailo_settings = settings.get("HAILO_SETTINGS", {})
            confidence_threshold = hailo_settings.get("CONFIDENCE_THRESHOLD", 0.5)
            worker_args = (task_queue, result_queue, log_queue, hailo_settings["HEF_PATH"], hailo_settings["PREPROC_LIB_PATH"], confidence_threshold, model_name, str(_get_deepface_base_path()), str(Path.cwd().resolve()))
            worker_process = spawn_ctx.Process(target=hailo_face_worker, args=worker_args)
        else:
            from .ai_worker import _ai_worker
            confidence_threshold = settings.get("DETECTION_CONFIDENCE_THRESHOLD", 0.95)
            worker_args = (task_queue, result_queue, log_queue, model_name, str(_get_deepface_base_path()), confidence_threshold)
            worker_process = spawn_ctx.Process(target=_ai_worker, args=worker_args)

        worker_process.start()
        task_queue.put((-1, str(image_path)))
        task_queue.put(None); task_queue.put("STOP")

        result_type, _, result_data = await asyncio.to_thread(result_queue.get, timeout=120)
        
        if result_type == "SUCCESS": return True, result_data
        elif result_type in ("FILE_ERROR", "WORKER_ERROR"): return False, result_data
        else: return False, "Otrzymano nieznany typ odpowiedzi od workera."

    except queue.Empty: return False, "Worker nie odpowiedział w wyznaczonym czasie."
    except Exception as e:
        logger.error(f"Krytyczny błąd podczas analizy twarzy: {e}", exc_info=True)
        return False, f"Błąd systemowy: {e}"
    finally:
        if worker_process and worker_process.is_alive():
            worker_process.terminate(); worker_process.join(timeout=5)

# ... (reszta funkcji z `managing.py` - enhance_profiles, itd.)
# Wklejam je poniżej dla kompletności.

def _find_best_matching_face(
    db_face_details: Dict[str, Any],
    detected_faces: List[Dict[str, Any]],
    iou_threshold: float = 0.5
) -> Optional[Dict[str, Any]]:
    # ... (bez zmian)
    def _calculate_iou(boxA: Dict[str, int], boxB: Dict[str, int]) -> float:
        xA = max(boxA["x"], boxB["x"]); yA = max(boxA["y"], boxB["y"])
        xB = min(boxA["x"] + boxA["w"], boxB["x"] + boxB["w"]); yB = min(boxA["y"] + boxA["h"], boxB["y"] + boxB["h"])
        interArea = max(0, xB - xA) * max(0, yB - yA)
        boxAArea = boxA["w"] * boxA["h"]; boxBArea = boxB["w"] * boxB["h"]
        union_area = float(boxAArea + boxBArea - interArea)
        return interArea / union_area if union_area > 0 else 0
    db_box = db_face_details.get('facial_area')
    if not db_box: return None
    best_match, max_iou = None, 0.0
    for face in detected_faces:
        area = face.get('facial_area') or face.get('region')
        if not area: continue
        iou = _calculate_iou(db_box, area)
        if iou > max_iou: max_iou, best_match = iou, face
    if max_iou > iou_threshold: return best_match
    return None

def _attribute_analyzer_worker(img_path_str: str, detector_backend: str, deepface_home: str, project_root: str) -> List[Dict[str, Any]]:
    """
    Funkcja-worker do analizy atrybutów, uruchamiana w osobnym procesie.
    """
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    if sys.platform != "win32":
        try:
            sys.stdin.close()
            sys.stdin = open(os.devnull)
        except (IOError, OSError):
            pass

    sys.path.insert(0, project_root)
    os.environ['DEEPFACE_HOME'] = deepface_home

    from deepface import DeepFace
    import cv2
    try:
        img_array = cv2.imread(img_path_str)
        if img_array is None: return []
        MAX_DIMENSION = 720
        height, width, _ = img_array.shape
        if height > MAX_DIMENSION or width > MAX_DIMENSION:
            scale = MAX_DIMENSION / max(height, width)
            new_width, new_height = int(width * scale), int(height * scale)
            img_array = cv2.resize(img_array, (new_width, new_height), interpolation=cv2.INTER_AREA)
        actions = ('age', 'gender', 'emotion', 'race')
        results = DeepFace.analyze(img_path=img_array, actions=actions, enforce_detection=False, detector_backend=detector_backend)
        return results if isinstance(results, list) else []
    except Exception:
        log_path = Path(project_root) / "app_data" / "logs" / "attribute_worker_error.log"
        log_path.parent.mkdir(exist_ok=True, parents=True)
        import traceback
        original_stdout, original_stderr = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = sys.__stdout__, sys.__stderr__
        with open(log_path, "a", encoding='utf-8') as f:
            f.write(f"--- Błąd w podprocesie analizy atrybutów dla obrazu {img_path_str} ---\n")
            traceback.print_exc(file=f)
            f.write("--------------------------------------------------\n")
        sys.stdout, sys.stderr = original_stdout, original_stderr
        return []

async def enhance_profiles(model_name: str):
    # ... (bez zmian)
    console.clear(); console.print(Panel("✨ Ulepszanie Profili Osób ✨", style="bold cyan"))
    with console.status("Pobieranie listy osób..."): active_people = await get_all_people(model_name)
    if not active_people:
        console.print("[yellow]Brak osób do ulepszenia.[/yellow]"); Prompt.ask("\nEnter..."); return
    if not Confirm.ask(f"\nZnaleziono [cyan]{len(active_people)}[/cyan] profili. Rozpocząć?"): return
    updated, skipped = 0, 0
    with Progress(TextColumn("[cyan]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%") as progress:
        task = progress.add_task("Ulepszanie...", total=len(active_people))
        for person in active_people:
            embeddings = await get_all_embeddings_for_person(person['person_id'])
            if len(embeddings) < 2:
                skipped += 1; progress.update(task, advance=1); continue
            await update_master_embedding(person['person_id'], np.mean(embeddings, axis=0))
            updated += 1; progress.update(task, advance=1)
    console.print(f"\n[green]✅ Proces zakończony![/green]\n  - Zaktualizowano: [cyan]{updated}[/]\n  - Pominięto: [dim]{skipped}[/]")
    Prompt.ask("\nEnter...")

async def interactive_attribute_explorer(model_name: str):
    # ... (bez zmian)
    people_with_attrs = await get_all_people_with_attributes(model_name)
    if not people_with_attrs:
        console.print("[yellow]Brak osób w bazie lub brak przeanalizowanych atrybutów.[/yellow]"); Prompt.ask("\nEnter..."); return
    ITEMS_PER_PAGE = 20; current_page = 0; total_pages = (len(people_with_attrs) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE; current_selection_on_page = 0
    def get_renderable():
        start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; page_items = people_with_attrs[start_index:end_index]
        table = Table(title=f"Lista Osób i Atrybuty (Model: {model_name}) - Strona {current_page + 1}/{total_pages}", show_header=True, header_style="bold cyan", min_width=140)
        table.add_column("ID", width=4); table.add_column("Imię i Nazwisko", width=25); table.add_column("Status", width=10); table.add_column("Liczba Twarzy", justify="right", width=10); table.add_column("Średni Wiek", width=12); table.add_column("Dominująca Płeć", width=18); table.add_column("Częsta Emocja", width=20); table.add_column("Dominujące Pochodzenie", width=25); table.add_column("Ostatnio Widziano", no_wrap=True, width=20)
        for i, p in enumerate(page_items):
            status_label = f"[yellow]Ignorowany[/yellow]" if p['status'] == 'ignored' else "[green]Aktywny[/green]"
            last_seen_obj = p.get('last_seen'); last_seen_str = last_seen_obj.strftime('%Y-%m-%d %H:%M:%S') if last_seen_obj and isinstance(last_seen_obj, datetime) else str(last_seen_obj).split('.')[0] if last_seen_obj else "[dim]b/d[/]"
            emotion_raw, gender_raw, race_raw = p.get('common_emotion'), p.get('dominant_gender'), p.get('dominant_race')
            emotion_pl = EMOTION_TRANSLATIONS.get(emotion_raw, emotion_raw) if emotion_raw else "[dim]b/d[/]"
            gender_pl = "Mężczyzna 👨" if gender_raw == 'Man' else "Kobieta 👩" if gender_raw == 'Woman' else "[dim]b/d[/]"
            race_pl = RACE_TRANSLATIONS.get(race_raw, race_raw) if race_raw else "[dim]b/d[/]"
            table.add_row(str(p['person_id']), p['name'], status_label, str(p.get('face_count', 0)), f"~{p['avg_age']}" if p.get('avg_age') is not None else "[dim]b/d[/]", gender_pl, emotion_pl, race_pl, last_seen_str, style="on grey15" if i == current_selection_on_page else "")
        return Group(table, Text("\n[bold]Sterowanie:[/bold] [cyan]G/D[/cyan] - nawigacja | [cyan]L/P[/cyan] - zmiana strony | [cyan]Enter[/cyan] - szczegóły | [cyan]Q[/cyan] - wyjdź"))
    with Live(get_renderable(), screen=True, auto_refresh=False) as live:
        key = ""
        while not (key and key.upper() == 'Q'):
            live.update(get_renderable(), refresh=True); key = await asyncio.to_thread(get_key)
            page_items_count = len(people_with_attrs[current_page * ITEMS_PER_PAGE : current_page * ITEMS_PER_PAGE + ITEMS_PER_PAGE])
            if page_items_count == 0: continue
            if key == "UP": current_selection_on_page = (current_selection_on_page - 1 + page_items_count) % page_items_count
            elif key == "DOWN": current_selection_on_page = (current_selection_on_page + 1) % page_items_count
            elif key == "LEFT": current_page = (current_page - 1 + total_pages) % total_pages; current_selection_on_page = 0
            elif key == "RIGHT": current_page = (current_page + 1) % total_pages; current_selection_on_page = 0
            elif key == "ENTER": break
    if key == "ENTER":
        start_index = current_page * ITEMS_PER_PAGE; page_items = people_with_attrs[start_index:start_index + ITEMS_PER_PAGE]
        selected_person = page_items[current_selection_on_page]
        await view_person_details(selected_person)
        await interactive_attribute_explorer(model_name)

async def view_person_details(person_data: dict):
    # ... (bez zmian)
    entries = await get_face_entries_for_person(person_data['person_id'])
    if not entries:
        console.print("[yellow]Brak szczegółowych danych o twarzach.[/yellow]"); Prompt.ask("\nEnter..."); return
    ITEMS_PER_PAGE = 20; current_page = 0; total_pages = (len(entries) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE; current_selection_on_page = 0
    def get_renderable():
        start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; page_items = entries[start_index:end_index]
        table = Table(title=f"Szczegółowa Historia Atrybutów dla: {person_data['name']} - Strona {current_page + 1}/{total_pages}", show_header=True, header_style="bold cyan")
        table.add_column("Data Zdjęcia"); table.add_column("Szac. Wiek"); table.add_column("Płeć"); table.add_column("Emocja"); table.add_column("Pochodzenie"); table.add_column("Plik", style="dim")
        for i, entry in enumerate(page_items):
            date_str = "[dim]b/d[/]";
            if p_date := entry.get('photo_date'): date_str = str(p_date).split('T')[0].split(' ')[0]
            g_raw, e_raw, r_raw = entry.get('estimated_gender'), entry.get('dominant_emotion'), entry.get('dominant_race')
            g_pl = "Mężczyzna 👨" if g_raw == 'Man' else "Kobieta 👩" if g_raw == 'Woman' else "[dim]b/d[/]"
            e_pl = EMOTION_TRANSLATIONS.get(e_raw, e_raw) if e_raw else "[dim]b/d[/]"
            r_pl = RACE_TRANSLATIONS.get(r_raw, r_raw) if r_raw else "[dim]b/d[/]"
            table.add_row(date_str, str(entry['estimated_age']) if entry['estimated_age'] is not None else "[dim]b/d[/]", g_pl, e_pl, r_pl, Path(entry['final_path']).name, style="on grey15" if i == current_selection_on_page else "")
        return Group(table, Text("\n[bold]Sterowanie:[/bold] [cyan]G/D[/cyan] - nawigacja | [cyan]L/P[/cyan] - zmiana strony | [cyan]Enter[/cyan] - otwórz zdjęcie | [cyan]Q[/cyan] - wróć"))
    with Live(get_renderable(), screen=True, auto_refresh=False) as live:
        while True:
            live.update(get_renderable(), refresh=True); key = await asyncio.to_thread(get_key)
            page_items_count = len(entries[current_page * ITEMS_PER_PAGE : current_page * ITEMS_PER_PAGE + ITEMS_PER_PAGE])
            if page_items_count == 0: continue
            if key == "UP": current_selection_on_page = (current_selection_on_page - 1 + page_items_count) % page_items_count
            elif key == "DOWN": current_selection_on_page = (current_selection_on_page + 1) % page_items_count
            elif key == "LEFT": current_page = (current_page - 1 + total_pages) % total_pages; current_selection_on_page = 0
            elif key == "RIGHT": current_page = (current_page + 1) % total_pages; current_selection_on_page = 0
            elif key and key.upper() == "Q": break
            elif key == "ENTER": 
                start_index = current_page * ITEMS_PER_PAGE; page_items = entries[start_index:start_index + ITEMS_PER_PAGE]
                selected_entry = page_items[current_selection_on_page]
                live.stop(); await asyncio.to_thread(open_image_viewer, Path(selected_entry['final_path'])); live.start()

async def manage_people(model_name: str):
    # ... (bez zmian)
    while True:
        console.clear(); console.print(Panel("👨‍👩‍👧‍👦 Zarządzanie Osobami 👨‍👩‍👧‍👦", style="bold cyan", subtitle=f"Model: [cyan]{model_name}[/cyan]"))
        menu_items = [("Dodaj nową osobę", "add"), ("Edytuj nazwę / Zmień status", "edit_status"), ("Połącz profile", "merge"), ("Eksplorator Osób i Atrybutów", "list_attributes"), ("Wróć", "exit")]
        selected_action = await create_interactive_menu(menu_items, "Wybierz operację")
        if selected_action in ("exit", None): break
        if selected_action == "add":
            path_str = Prompt.ask("[cyan]Podaj ścieżkę do wyraźnego zdjęcia osoby[/]")
            image_path = Path(path_str.strip()).resolve()
            if not image_path.is_file(): console.print("[red]Błąd: Podana ścieżka nie jest plikiem.[/red]")
            else:
                person_name = Prompt.ask("[cyan]Podaj imię i nazwisko tej osoby[/]").strip()
                if person_name:
                    obj_to_use = None
                    with console.status(f"Analizowanie zdjęcia... (detektor: {get_current_settings()['DETECTOR_BACKEND']})"):
                        success, result = await run_face_analysis_on_image(image_path, model_name)
                        objs = result if success and isinstance(result, list) else []
                    if not success: console.print(f"[red]Błąd analizy: {result}[/red]")
                    elif not objs: console.print("[yellow]Nie wykryto twarzy na zdjęciu.[/yellow]")
                    elif len(objs) == 1: obj_to_use = objs[0]
                    else:
                        console.print(f"[yellow]Wykryto {len(objs)} twarzy. Wybierz właściwą:[/yellow]")
                        choices = []
                        if Image:
                            try:
                                with tempfile.TemporaryDirectory() as tmp, Image.open(image_path) as img:
                                    for i, face_obj in enumerate(objs):
                                        area = face_obj['facial_area']
                                        box = (area['x'], area['y'], area['x'] + area['w'], area['y'] + area['h'])
                                        face_img = img.crop(box); tmp_path = Path(tmp) / f"face_{i}.jpg"
                                        face_img.save(tmp_path, "JPEG"); choices.append((f"Twarz #{i+1} (podgląd)", (face_obj, tmp_path)))
                                    choices.append(("Anuluj", "cancel"))
                                    while True:
                                        selected = await create_interactive_menu(choices, "Wybierz twarz")
                                        if selected in ("cancel", None): break
                                        face_obj, prev_path = selected
                                        await asyncio.to_thread(open_image_viewer, prev_path)
                                        if Confirm.ask("\nCzy to poprawna twarz?", default=True):
                                            obj_to_use = face_obj; break
                            except Exception as e: logger.error(f"Błąd podglądu: {e}")
                        else: console.print("[red]Błąd: Biblioteka Pillow jest wymagana do podglądu.[/red]")
                    if obj_to_use:
                        embedding = np.array(obj_to_use["embedding"])
                        media_id = await get_media_id_by_path(str(image_path))
                        if not media_id and await add_local_file_entry(image_path, {}):
                            media_id = await get_media_id_by_path(str(image_path))
                        new_id = await add_person(person_name, model_name, embedding, media_id)
                        if new_id: console.print(f"[green]✅ Dodano '{person_name}'.[/green]")
                        else: console.print(f"[yellow]Osoba '{person_name}' już istnieje.[/yellow]")
            Prompt.ask("\nEnter...")
        elif selected_action == "edit_status":
            with console.status("Pobieranie osób..."): all_people = await get_all_people_with_status(model_name)
            if not all_people: console.print("[yellow]Brak osób w bazie.[/yellow]")
            else:
                choices = [(f"{p['name']} {'[dim yellow](Ignorowany)[/]' if p['status'] == 'ignored' else ''}", p) for p in all_people]
                person = await create_interactive_menu(choices, "Wybierz osobę")
                if person:
                    details = await get_person_details(person['person_id'])
                    if details and details.get('avatar_path'): await asyncio.to_thread(open_image_viewer, Path(details['avatar_path']))
                    else: console.print(f"\n[dim]Brak zdjęcia profilowego.[/dim]")
                    action = await create_interactive_menu([("Zmień nazwę", "edit"), ("Zmień status", "status"), ("Anuluj", "cancel")], "Wybierz akcję")
                    if action == 'edit':
                        new_name = Prompt.ask(f"\nNowa nazwa dla '{person['name']}'", default=person['name']).strip()
                        if new_name: await update_person_name(person['person_id'], new_name); console.print("[green]✅ Nazwa zaktualizowana.[/green]")
                    elif action == 'status':
                        if person['status'] == 'active':
                            if Confirm.ask(f"\nIgnorować '{person['name']}'?"):
                                await update_person_status(person['person_id'], 'ignored'); console.print("[green]✅ Osoba ignorowana.[/green]")
                                if Confirm.ask(f"\nUsunąć wykryte twarze '{person['name']}'?"):
                                    with console.status("Usuwanie..."): await delete_faces_for_person(person['person_id'])
                                    console.print("[green]✅ Twarze usunięte.[/green]")
                        else:
                            if Confirm.ask(f"\nPrzywrócić '{person['name']}'?"):
                                await update_person_status(person['person_id'], 'active'); console.print("[green]✅ Osoba przywrócona.[/green]")
            Prompt.ask("\nEnter...")
        elif selected_action == "merge":
            with console.status("Pobieranie osób..."): all_people = await get_all_people_with_status(model_name)
            console.print(Panel(" Scalanie Profili ", style="bold green"))
            if len(all_people) < 2: console.print("[yellow]Potrzeba co najmniej dwóch profili.[/yellow]")
            else:
                choices = [(p['name'], p) for p in all_people]
                console.print("\n[bold]Krok 1/2:[/bold] Wybierz profil ŹRÓDŁOWY (zostanie usunięty).")
                source = await create_interactive_menu(choices, "Wybierz profil ŹRÓDŁOWY")
                if source:
                    target_choices = [p for p in choices if p[1]['person_id'] != source['person_id']]
                    console.print(f"\n[bold]Krok 2/2:[/bold] Wybierz profil DOCELOWY (pozostanie).")
                    target = await create_interactive_menu(target_choices, "Wybierz profil DOCELOWY")
                    if target:
                        s_id, s_name = source['person_id'], source['name']; t_id, t_name = target['person_id'], target['name']
                        if Confirm.ask(f"\nCzy scalić [yellow]'{s_name}'[/yellow] z [green]'{t_name}'[/green]?", default=False):
                            with console.status("Przenoszenie twarzy..."): await reassign_faces(s_id, t_id)
                            await delete_person(s_id, hard_delete=True); console.print(f"\n[green]✅ Profile scalone.[/green]")
                        else: console.print("\n[yellow]Anulowano.[/yellow]")
            Prompt.ask("\nEnter...")
        elif selected_action == "list_attributes":
            await interactive_attribute_explorer(model_name)
