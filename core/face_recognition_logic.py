# -*- coding: utf-8 -*-

# plik: core/face_recognition_logic.py
# Wersja 11.1 - Finalna wersja z ulepszonym detektorem i filtrowaniem pewności

# --- GŁÓWNE IMPORTY ---
import asyncio
import json
import logging
import os
import platform
import subprocess
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Optional

# Kluczowa biblioteka do obliczeń matematycznych na wektorach
import numpy as np

# Moduł do limitowania zasobów (pamięci RAM), specyficzny dla Unix
try:
    import resource
    RESOURCE_AVAILABLE = True
except ImportError:
    RESOURCE_AVAILABLE = False

# Główne biblioteki do przetwarzania obrazów i AI
try:
    from deepface import DeepFace
    from PIL import Image
except ImportError:
    DeepFace, Image = None, None

# Asynchroniczna obsługa bazy danych
import aiosqlite

# Biblioteka do tworzenia bogatego interfejsu w terminalu
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.progress import Progress, BarColumn, TextColumn, SpinnerColumn

# --- IMPORTY Z WŁASNYCH MODUŁÓW APLIKACJI ---
from .config import (
    DOWNLOADS_DIR_BASE,
    AI_MODELS_CACHE_DIR,
    DATABASE_FILE,
)
from .utils import create_interactive_menu, check_dependency, open_image_viewer, get_key

# Importujemy funkcje z naszego nowego, zaawansowanego modułu bazy danych
from .database import (
    setup_database,
    add_person,
    get_all_people,
    update_person_name,
    delete_person,
    add_face,
    get_unknown_faces,
    tag_face,
    get_media_ids_with_indexed_faces,
    get_all_tagged_people,
    get_media_for_person,
)

# --- INICJALIZACJA GLOBALNYCH OBIEKTÓW ---
console = Console()
logger = logging.getLogger(__name__)

# --- STAŁE I KONFIGURACJA MODUŁU ---
AVAILABLE_MODELS = [
    {"name": "VGG-Face", "description": "Dobry kompromis dokładności i wydajności."},
    {"name": "Facenet", "description": "Wysoka dokładność, ale powolny na CPU."},
    {"name": "Facenet512", "description": "Wersja Facenet, bardzo zasobożerna."},
    {"name": "ArcFace", "description": "Bardzo wysoka dokładność, bardzo powolny na CPU."},
    {"name": "Dlib", "description": "Najszybszy, najlżejszy. Dobry dla < 4GB RAM."},
    {"name": "OpenFace", "description": "Klasyczny, szybki model."},
    {"name": "DeepFace", "description": "Lekki model, alternatywa dla Dlib."},
    {"name": "GhostFaceNet", "description": "Nowoczesny, wydajny. [bold green]Zalecany dla RPi 5.[/bold green]"},
]
MODEL_FILE_MAP = {
    "VGG-Face": "vgg_face_weights.h5", "Facenet": "facenet_weights.h5", "ArcFace": "arcface_weights.h5",
    "Dlib": "dlib_face_recognition_resnet_model_v1.dat", "OpenFace": "openface_weights.h5",
    "DeepFace": "deepface_weights.h5", "GhostFaceNet": "ghostfacenet_v1.h5", "Facenet512": "facenet512_weights.h5",
}

SETTINGS_FILE_PATH = Path("app_data/face_rec_settings.json")

# Wybór `retinaface` jako domyślnego, dokładniejszego detektora twarzy
DETECTOR_BACKEND = "retinaface"

def findCosineDistance(source_representation, test_representation):
    """
    Oblicza odległość kosinusową między dwoma wektorami za pomocą NumPy.

    Args:
        source_representation (np.ndarray): Wektor pierwszej twarzy.
        test_representation (np.ndarray): Wektor drugiej twarzy.

    Returns:
        float: Odległość kosinusowa (od 0.0 do 1.0).
    """
    a = np.asarray(source_representation)
    b = np.asarray(test_representation)
    
    # Zabezpieczenie przed dzieleniem przez zero, jeśli któryś wektor jest pusty
    if np.linalg.norm(a) == 0 or np.linalg.norm(b) == 0:
        return 1.0  # Zwróć maksymalną odległość
        
    return 1 - (np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

def findThreshold(model_name: str, distance_metric: str) -> float:
    """
    Zwraca próg weryfikacji dla danego modelu i metryki.

    Args:
        model_name (str): Nazwa modelu (np. "VGG-Face").
        distance_metric (str): Nazwa metryki (np. "cosine").

    Returns:
        float: Wartość progowa do weryfikacji.
    """
    thresholds = {
        "VGG-Face": {"cosine": 0.40},
        "Facenet": {"cosine": 0.40},
        "ArcFace": {"cosine": 0.68},
        "Dlib": {"cosine": 0.07},
        "GhostFaceNet": {"cosine": 0.65},
    }
    # Zwraca próg dla danego modelu lub bezpieczną wartość domyślną
    return thresholds.get(model_name, {}).get(distance_metric, 0.40)

def _get_deepface_base_path() -> Path:
    """
    Zwraca ścieżkę bazową dla DeepFace (katalog nadrzędny dla folderu .deepface).
    
    Używa ścieżki z konfiguracji (AI_MODELS_CACHE_DIR), jeśli jest dostępna,
    w przeciwnym razie domyślnie wskazuje na katalog domowy użytkownika.
    """
    if AI_MODELS_CACHE_DIR:
        return Path(AI_MODELS_CACHE_DIR)
    return Path.home()

def _get_deepface_home_path() -> Path:
    """Zwraca pełną ścieżkę do folderu .deepface."""
    return _get_deepface_base_path() / ".deepface"

def run_deepface_represent_in_process(img_path_str: str, model_name: str, detector_backend: str):
    """
    Uruchamia `DeepFace.represent` w osobnym, odizolowanym procesie dla stabilności.

    Args:
        img_path_str (str): Ścieżka do pliku obrazu.
        model_name (str): Nazwa modelu AI do użycia.
        detector_backend (str): Nazwa silnika detekcji twarzy.

    Returns:
        list: Lista obiektów z wektorami twarzy lub pusta lista w razie błędu.
    """
    from deepface import DeepFace
    
    # Upewniamy się, że nowy proces wie, gdzie szukać modeli
    base_path = _get_deepface_base_path()
    os.environ['DEEPFACE_HOME'] = str(base_path)
    
    try:
        # Wywołujemy główną funkcję analizującą obraz z podanym detektorem
        return DeepFace.represent(
            img_path=img_path_str,
            model_name=model_name,
            enforce_detection=False,
            detector_backend=detector_backend
        )
    except Exception as e:
        # W razie błędu w podprocesie, logujemy go i zwracamy pustą listę
        logger.error(f"Błąd w podprocesie DeepFace dla obrazu {img_path_str}: {e}", exc_info=False)
        return []

def _initialize_deepface_env():
    """Konfiguruje środowisko, tworzy foldery i ustawia zmienne."""
    base_path = _get_deepface_base_path()
    deepface_home_path = base_path / ".deepface"
    weights_path = deepface_home_path / "weights"
    weights_path.mkdir(parents=True, exist_ok=True)
    os.environ['DEEPFACE_HOME'] = str(base_path)
    logger.info(f"Ustawiono bazę dla DeepFace: {base_path}")

def _check_dependencies() -> bool:
    """Sprawdza, czy wszystkie zależności dla tego modułu są dostępne."""
    logger.debug("Sprawdzam zależności dla modułu Rozpoznawania Twarzy...")
    dependencies_ok = all([
        check_dependency("deepface", "deepface", "DeepFace"),
        check_dependency("numpy", "numpy", "NumPy"),
        check_dependency("PIL", "Pillow", "Pillow"),
        check_dependency("pandas", "pandas", "Pandas"),
#        check_dependency("tensorflow", "tensorflow", "TensorFlow"),
        check_dependency("dlib", "dlib", "Dlib (dla modelu Dlib)")
    ])
    return dependencies_ok

async def _is_model_downloaded(model_name: str) -> bool:
    """
    Sprawdza asynchronicznie, czy plik z wagami modelu istnieje na dysku.

    Args:
        model_name (str): Nazwa modelu do sprawdzenia.

    Returns:
        bool: True, jeśli plik modelu istnieje, w przeciwnym razie False.
    """
    model_filename = MODEL_FILE_MAP.get(model_name)
    if not model_filename:
        return False
        
    model_path = _get_deepface_home_path() / "weights" / model_filename
    
    # Uruchamia synchroniczną operację sprawdzania pliku w osobnym wątku,
    # aby nie blokować głównej pętli programu.
    return await asyncio.to_thread(model_path.exists)

def set_memory_limit(gb_limit: float | None):
    """
    Ustawia limit wirtualnej pamięci dla bieżącego procesu (tylko Unix).

    Args:
        gb_limit (float | None): Limit pamięci w gigabajtach. 
                                 None lub 0 usuwa limit.
    """
    if not RESOURCE_AVAILABLE:
        if gb_limit is not None:
            console.print("[yellow]Ostrzeżenie: Ustawianie limitu pamięci nie jest wspierane w tym systemie.[/yellow]")
        return
        
    try:
        if gb_limit and gb_limit > 0:
            limit_bytes = int(gb_limit * 1024**3)
            resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
            console.print(f"[green]✅ Ustawiono limit pamięci na {gb_limit} GB.[/green]")
        else:
            # Ustawienie nieskończonego limitu
            resource.setrlimit(resource.RLIMIT_AS, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
            console.print("[yellow]✅ Usunięto limit pamięci.[/yellow]")
    except (ValueError, resource.error) as e:
        console.print(f"[bold red]Błąd: Nie udało się ustawić limitu pamięci: {e}[/bold red]")

def _read_face_rec_settings() -> dict:
    """
    Wczytuje ustawienia specyficzne dla rozpoznawania twarzy z pliku JSON.
    """
    try:
        if not SETTINGS_FILE_PATH.exists():
            return {}
        with open(SETTINGS_FILE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def _write_face_rec_settings(new_settings: dict):
    """
    Zapisuje ustawienia specyficzne dla rozpoznawania twarzy do pliku JSON.
    """
    try:
        all_settings = _read_face_rec_settings()
        all_settings.update(new_settings)
        SETTINGS_FILE_PATH.parent.mkdir(exist_ok=True, parents=True)
        with open(SETTINGS_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(all_settings, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Nie udało się zapisać ustawień: {e}")

async def index_faces(model_name: str):
    """
    Skanuje kolekcję w poszukiwaniu nowych zdjęć, wykrywa na nich twarze
    i zapisuje je w bazie danych jako "nieznane" do późniejszej identyfikacji.
    """
    console.clear()
    console.print(Panel(f"🔬 Indeksowanie Twarzy (Model: [cyan]{model_name}[/cyan]) 🔬", style="bold blue"))
    await setup_database()

    # Krok 1: Pobierz ID mediów, które już zostały zaindeksowane dla tego modelu
    with console.status("[cyan]Sprawdzanie stanu bazy danych...[/]"):
        indexed_media_ids = await get_media_ids_with_indexed_faces(model_name)
        
    # Krok 2: Pobierz wszystkie zdjęcia, pomijając te już zaindeksowane
    media_to_scan = []
    with console.status("[cyan]Pobieranie listy zdjęć do przetworzenia...[/]"):
        try:
            async with aiosqlite.connect(DATABASE_FILE) as conn:
                conn.row_factory = aiosqlite.Row
                
                # Wybieramy tylko pliki graficzne, których ID nie ma na liście już zaindeksowanych
                query = """
                    SELECT id, final_path FROM downloaded_media 
                    WHERE status = 'downloaded' AND final_path IS NOT NULL
                    AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')
                """
                
                if indexed_media_ids:
                    placeholders = ','.join('?' for _ in indexed_media_ids)
                    query += f" AND id NOT IN ({placeholders})"
                    cursor = await conn.execute(query, indexed_media_ids)
                else:
                    cursor = await conn.execute(query)

                media_to_scan = await cursor.fetchall()
        except aiosqlite.Error as e:
            logger.error(f"Błąd odczytu bazy danych: {e}", exc_info=True)
            return

    if not media_to_scan:
        console.print("[green]✅ Twoja baza twarzy jest już aktualna. Nie znaleziono nowych zdjęć do indeksowania.[/green]")
        return

    if not Confirm.ask(f"\nZnaleziono [cyan]{len(media_to_scan)}[/cyan] nowych zdjęć do analizy. Rozpocząć indeksowanie?"):
        return

    # Krok 3: Przetwarzanie i zapisywanie twarzy do bazy
    total_faces_found = 0
    loop = asyncio.get_running_loop()
    pool = ProcessPoolExecutor(max_workers=1)
    try:
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), transient=True) as progress:
            task = progress.add_task("[green]Indeksowanie...", total=len(media_to_scan))
            for media_record in media_to_scan:
                media_id, path_str = media_record['id'], media_record['final_path']
                path = Path(path_str)
                progress.update(task, advance=1, description=f"Skanuję: [dim]{path.name}[/dim]")

                if not await asyncio.to_thread(path.exists):
                    continue

                embedding_objs = await loop.run_in_executor(pool, run_deepface_represent_in_process, str(path), model_name, DETECTOR_BACKEND)
                
                for face_obj in embedding_objs:
                    # Filtr pewności, aby unikać "fałszywych" twarzy
                    confidence = face_obj.get("face_confidence", 0)
                    if confidence < 0.95:
                        logger.debug(f"Pominięto twarz w {path.name} z niską pewnością: {confidence:.2f}")
                        continue

                    if "embedding" in face_obj and "facial_area" in face_obj:
                        await add_face(
                            media_id=media_id,
                            embedding=face_obj["embedding"],
                            facial_area=face_obj["facial_area"],
                            model_name=model_name
                        )
                        total_faces_found += 1
    finally:
        pool.shutdown(wait=True)

    console.print(f"\n[bold green]✅ Indeksowanie zakończone! Dodano {total_faces_found} twarzy o wysokiej pewności.[/bold green]")

async def recognize_and_tag_faces(model_name: str):
    """
    Porównuje nieznane twarze z bazy danych ze znanymi osobami i taguje dopasowania.
    """
    console.clear()
    console.print(Panel(f"🕵️ Rozpoznawanie i Tagiwanie Twarzy (Model: [cyan]{model_name}[/cyan]) 🕵️", style="bold blue"))
    await setup_database()

    # Krok 1: Pobierz wszystkie znane osoby i nieznane twarze z bazy danych
    with console.status("[cyan]Pobieranie danych z bazy...[/]"):
        known_people = await get_all_people(model_name)
        unknown_faces = await get_unknown_faces(model_name)

    if not known_people:
        console.print(f"[yellow]Brak znanych osób dla modelu '{model_name}'. Dodaj kogoś w menu 'Zarządzaj osobami'.[/yellow]")
        return
    
    if not unknown_faces:
        console.print("[green]✅ Brak nowych, nieznanych twarzy do rozpoznania.[/green]")
        return
        
    console.print(f"Znaleziono [cyan]{len(known_people)}[/cyan] znanych osób i [cyan]{len(unknown_faces)}[/cyan] nieotagowanych twarzy.")

    # Krok 2: Porównaj każdą nieznaną twarz z każdą znaną osobą
    total_tagged = 0
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%"), transient=True) as progress:
        task = progress.add_task("[green]Rozpoznawanie...", total=len(unknown_faces))
        
        threshold = findThreshold(model_name, 'cosine')

        for face in unknown_faces:
            progress.update(task, advance=1)
            
            best_match_person_id = None
            min_distance = float('inf')

            for person in known_people:
                distance = findCosineDistance(face["embedding"], person["master_embedding"])
                
                if distance < min_distance and distance < threshold:
                    min_distance = distance
                    best_match_person_id = person["person_id"]
            
            # Krok 3: Jeśli znaleziono dopasowanie, otaguj twarz w bazie
            if best_match_person_id is not None:
                await tag_face(face["face_id"], best_match_person_id)
                total_tagged += 1
    
    console.print(f"\n[bold green]✅ Rozpoznawanie zakończone! Otagowano {total_tagged} nowych twarzy.[/bold green]")

# --- FUNKCJE OBSŁUGI MENU UŻYTKOWNIKA ---

async def manage_people(model_name: str):
    """Wyświetla interaktywne menu do zarządzania znanymi osobami (CRUD)."""
    while True:
        console.clear()
        console.print(Panel("👨‍👩‍👧‍👦 Zarządzanie Osobami 👨‍👩‍👧‍👦", style="bold blue", subtitle=f"Model: [cyan]{model_name}[/cyan]"))
        
        with console.status("Pobieranie listy osób..."):
            people = await get_all_people(model_name)

        menu_items = [
            ("Dodaj nową osobę", "add"),
            ("Edytuj nazwę osoby", "edit"),
            ("Usuń osobę", "delete"),
            ("Wyświetl listę znanych osób", "list"),
            ("Wróć", "exit")
        ]
        
        selected_action = await create_interactive_menu(menu_items, "Wybierz operację")

        if selected_action in ("exit", None):
            break

        if selected_action == "add":
            path_str = Prompt.ask("[cyan]Podaj ścieżkę do wyraźnego zdjęcia osoby[/cyan]")
            image_path = Path(path_str)
            if not image_path.is_file():
                console.print("[red]Błąd: Podana ścieżka nie jest plikiem.[/red]")
            else:
                person_name = Prompt.ask("[cyan]Podaj imię i nazwisko tej osoby[/cyan]").strip()
                if person_name:
                    with console.status(f"Przetwarzanie zdjęcia dla [bold]{person_name}[/bold]..."):
                        embedding_objs = run_deepface_represent_in_process(str(image_path), model_name, DETECTOR_BACKEND)
                        if embedding_objs:
                            person_id = await add_person(person_name, model_name, embedding_objs[0]["embedding"])
                            if person_id:
                                console.print(f"[green]✅ Pomyślnie dodano '{person_name}'.[/green]")
                        else:
                            console.print(f"[red]Nie wykryto twarzy na zdjęciu {image_path.name}.[/red]")
                else:
                    console.print("[red]Błąd: Imię i nazwisko nie może być puste.[/red]")
            Prompt.ask("\n[bold]Naciśnij Enter...[/]")

        elif selected_action in ("edit", "delete"):
            if not people:
                console.print("[yellow]Brak osób w bazie dla tego modelu do edycji/usunięcia.[/yellow]")
            else:
                person_choices = [(p['name'], p) for p in people]
                person_to_act_on = await create_interactive_menu(person_choices, f"Wybierz osobę do {'edycji' if selected_action == 'edit' else 'usunięcia'}")
                if person_to_act_on:
                    if selected_action == 'edit':
                        new_name = Prompt.ask(f"[cyan]Podaj nową nazwę dla '{person_to_act_on['name']}'[/cyan]").strip()
                        if new_name:
                            await update_person_name(person_to_act_on['person_id'], new_name)
                            console.print(f"[green]✅ Nazwa została zaktualizowana.[/green]")
                    elif selected_action == 'delete':
                        if Confirm.ask(f"[bold red]Czy na pewno chcesz usunąć '{person_to_act_on['name']}'? Jej otagowane zdjęcia staną się ponownie 'nieznane'.[/bold red]", default=False):
                            await delete_person(person_to_act_on['person_id'])
                            console.print(f"[green]✅ Usunięto '{person_to_act_on['name']}'.[/green]")
            Prompt.ask("\n[bold]Naciśnij Enter...[/]")

        elif selected_action == "list":
            console.print(f"\n--- [bold]Lista Znanych Osób (Model: {model_name})[/bold] ---")
            if not people:
                console.print("[dim]Brak osób w bazie danych dla tego modelu.[/dim]")
            else:
                for person in people:
                    console.print(f"- {person['name']} (ID: {person['person_id']})")
            Prompt.ask("\n[bold]Naciśnij Enter...[/]")

async def review_tagged_faces():
    """Uruchamia interaktywną przeglądarkę otagowanych zdjęć."""
    while True:
        console.clear()
        console.print(Panel("🖼️ Przeglądanie Otagowanych Zdjęć 🖼️", style="bold green"))
        
        with console.status("Pobieranie listy osób..."):
            people = await get_all_tagged_people()

        if not people:
            console.print("[yellow]Nie znaleziono jeszcze żadnych otagowanych osób.[/yellow]")
            Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić...[/]")
            return

        menu_items = [(f"{p['name']} ([cyan]{p['photo_count']} zdjęć[/cyan])", p) for p in people] + [("Wróć", "exit")]
        
        selected_person = await create_interactive_menu(menu_items, "Wybierz osobę, aby zobaczyć jej zdjęcia")

        if selected_person in ("exit", None):
            break
        
        media_files = await get_media_for_person(selected_person['person_id'])
        current_index = 0
        while True:
            if not media_files:
                console.print("[yellow]Brak zdjęć dla wybranej osoby.[/yellow]")
                break

            media = media_files[current_index]
            path = Path(media['final_path'])
            
            console.clear()
            console.print(Panel(f"Osoba: [bold cyan]{selected_person['name']}[/bold cyan] | Zdjęcie {current_index + 1}/{len(media_files)}", subtitle=f"[dim]{path.name}[/dim]"))

            await asyncio.to_thread(open_image_viewer, path)

            console.print("\nNawigacja: [bold](P)rawo[/bold] - następne, [bold](L)ewo[/bold] - poprzednie, [bold](W)yjście[/bold]")
            key = await asyncio.to_thread(get_key)

            if key in ("RIGHT", "P"):
                current_index = (current_index + 1) % len(media_files)
            elif key in ("LEFT", "L"):
                current_index = (current_index - 1 + len(media_files)) % len(media_files)
            elif key in ("W", "Q", "ESC"):
                break

async def manage_ai_models():
    """Wyświetla interfejs do zarządzania pobranymi modelami AI."""
    loop = asyncio.get_running_loop()
    while True:
        console.clear()
        with console.status("[cyan]Sprawdzanie statusu modeli...[/]"):
            statuses = await asyncio.gather(*[_is_model_downloaded(m['name']) for m in AVAILABLE_MODELS])
        menu_items = [(f"{m['name']} ({'[green]Pobrany[/]' if d else '[red]Niepobrany[/]'})", m['name']) for m, d in zip(AVAILABLE_MODELS, statuses)] + [("Wróć", "exit")]
        selected = await create_interactive_menu(menu_items, "🤖 Menedżer Modeli AI 🤖")
        if selected in ["exit", None]: break
        if await _is_model_downloaded(selected):
            console.print(f"\n[green]Model [cyan]{selected}[/cyan] jest już pobrany.[/green]")
        elif Confirm.ask(f"\nModel [cyan]{selected}[/cyan] nie jest pobrany. Pobrać?", default=True):
            def download(model_name):
                from deepface import DeepFace
                base_path = _get_deepface_base_path()
                os.environ['DEEPFACE_HOME'] = str(base_path)
                DeepFace.build_model(model_name)
            with Progress(SpinnerColumn(), TextColumn("[cyan]Pobieranie...[/]"), transient=True) as progress:
                progress.add_task(selected, total=None)
                try:
                    await loop.run_in_executor(None, download, selected)
                    console.print(f"\n[green]✅ Model {selected} pobrany pomyślnie.[/green]")
                except Exception as e:
                    console.print(f"\n[red]❌ Błąd pobierania modelu: {e}[/red]")
        Prompt.ask("\n[bold]Naciśnij Enter...[/]")

async def memory_limit_menu():
    """Wyświetla menu do wyboru lub ustawienia limitu pamięci RAM dla procesu."""
    if not RESOURCE_AVAILABLE:
        console.print("\n[yellow]Funkcja dostępna tylko w systemach Linux i macOS.[/yellow]")
        await Prompt.ask("\n[bold]Naciśnij Enter...[/]")
        return
    console.clear()
    settings = _read_face_rec_settings()
    current_limit = settings.get("FACE_REC_MEMORY_LIMIT_GB")
    current_limit_str = f"{current_limit} GB" if current_limit and current_limit > 0 else "Brak limitu"
    items = [("4 GB", "4"), ("6 GB", "6"), ("8 GB", "8"), ("Bez limitu", "0"), ("Wpisz wartość", "custom"), ("Wróć", "exit")]
    selected = await create_interactive_menu(items, title="⚙️ Ustaw Limit Pamięci RAM ⚙️", subtitle=f"Aktualnie: [cyan]{current_limit_str}[/]", border_style="yellow")
    if selected in ["exit", None]: return
    try:
        limit_str = Prompt.ask("[cyan]Podaj limit w GB[/]").replace(',', '.') if selected == "custom" else selected
        limit = float(limit_str)
        if limit < 0: raise ValueError
        _write_face_rec_settings({"FACE_REC_MEMORY_LIMIT_GB": limit if limit > 0 else None})
        console.print(f"\n[green]✅ Ustawienie zapisane.[/green]")
    except ValueError: console.print("[red]Błędna wartość.[/red]")
    Prompt.ask("\n[bold]Naciśnij Enter...[/]")

# --- GŁÓWNY KONTROLER APLIKACJI ---

async def run_face_recognition_menu():
    """Główna funkcja uruchamiająca całe menu rozpoznawania twarzy."""
    console.clear()
    if not _check_dependencies():
        Prompt.ask("\n[yellow]Brak kluczowych zależności. Naciśnij Enter...[/yellow]"); return
    _initialize_deepface_env()
    
    while True:
        console.clear()
        menu_items = [
            ("Zaindeksuj nowe zdjęcia", "index"),
            ("Rozpoznaj i oznacz twarze", "recognize"),
            ("Zarządzaj znanymi osobami", "manage_people"),
            ("Przeglądaj otagowane twarze", "review"),
            ("Zarządzaj modelami AI", "models"),
            ("Ustaw limit pamięci RAM dla AI", "limit"),
            ("Wróć do menu głównego", "exit")
        ]
        selected_action = await create_interactive_menu(menu_items, "👨‍👩‍👧‍👦 Menedżer Rozpoznawania Twarzy 👨‍👩‍👧‍👦", border_style="blue")
        if selected_action in ("exit", None): break
        
        async def _select_model(action_text: str) -> Optional[str]:
            items = [(f"{m['name']}\n[dim]{m['description']}[/dim]", m['name']) for m in AVAILABLE_MODELS] + [("Anuluj", "cancel")]
            model = await create_interactive_menu(items, f"Wybierz model AI do {action_text}")
            if model in ("cancel", None): return None
            if not await _is_model_downloaded(model):
                console.print(f"[bold red]Model '{model}' nie jest pobrany! Pobierz go w menedżerze.[/bold red]"); await Prompt.ask("\nEnter..."); return None
            return model

        if selected_action in ("index", "recognize", "manage_people"):
            action_map = {"index": "indeksowania", "recognize": "rozpoznawania", "manage_people": "zarządzania osobami"}
            model = await _select_model(action_map[selected_action])
            if model:
                if selected_action == "index": await index_faces(model)
                elif selected_action == "recognize": await recognize_and_tag_faces(model)
                elif selected_action == "manage_people": await manage_people(model)
                Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter...[/]")
        
        elif selected_action == "review": await review_tagged_faces()
        elif selected_action == "models": await manage_ai_models()
        elif selected_action == "limit": await memory_limit_menu()
