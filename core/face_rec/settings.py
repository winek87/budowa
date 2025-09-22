# Plik: core/face_rec/settings.py
# Wersja 2.2 - Ostateczna, z interaktywnym wyborem RAM i ulepszonym UI

import logging
import json
from pathlib import Path
import numpy as np
import asyncio

try:
    import resource
    RESOURCE_AVAILABLE = True
except ImportError:
    RESOURCE_AVAILABLE = False # Dostępne tylko na systemach POSIX (Linux, macOS)

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

from ..utils import create_interactive_menu

console = Console(record=True)
logger = logging.getLogger(__name__)

# ##############################################################################
# ===                   SEKCJA 1: STAŁE KONFIGURACYJNE                       ===
# ##############################################################################

AVAILABLE_MODELS = [
    {"name": "Dlib", "description": "Najszybszy CPU, najlżejszy. Zalecany dla RPi < 4GB RAM."},
    {"name": "GhostFaceNet", "description": "Nowoczesny, wydajny. Dobry balans CPU."},
    {"name": "SFace", "description": "Bardzo wydajny i dokładny. Doskonała alternatywa dla GhostFaceNet."},
    {"name": "ArcFace", "description": "Bardzo wysoka dokładność. [bold green]Zalecany do rozpoznawania (na CPU/GPU).[/bold green]"},
    {"name": "Facenet", "description": "Wysoka dokładność, ale powolny na CPU."},
    {"name": "Facenet512", "description": "Wersja Facenet, bardzo zasobożerna."},
    {"name": "OpenFace", "description": "Klasyczny, szybki model."},
    {"name": "VGG-Face", "description": "Dobry kompromis dokładności i wydajności."},
    {"name": "DeepFace", "description": "Lekki model, alternatywa dla Dlib."},
]

MODEL_FILE_MAP = {
    "VGG-Face": "vgg_face_weights.h5", "Facenet": "facenet_weights.h5", "ArcFace": "arcface_weights.h5",
    "Dlib": ["dlib_face_recognition_resnet_model_v1.dat", "shape_predictor_5_face_landmarks.dat"],
    "OpenFace": "openface_weights.h5", "DeepFace": "deepface_weights.h5", "GhostFaceNet": "ghostfacenet_v1.h5",
    "Facenet512": "facenet512_weights.h5", "SFace": "sface_weights.h5",
}

MODEL_THRESHOLDS = {
    "VGG-Face": {"cosine": 0.40}, "Facenet": {"cosine": 0.40}, "ArcFace": {"cosine": 0.68},
    "Dlib": {"cosine": 0.07}, "GhostFaceNet": {"cosine": 0.65}, "SFace": {"cosine": 0.593},
    "OpenFace": {"cosine": 0.10}, "DeepFace": {"cosine": 0.23}, "Facenet512": {"cosine": 0.30},
}

AVAILABLE_DETECTORS = [
    {'name': 'retinaface', 'desc': "Bardzo wysoka dokładność, wolniejszy. [bold green]Zalecany (CPU/GPU).[/bold green]"},
    {'name': 'mtcnn', 'desc': "Wysoka dokładność, nieco szybszy."},
    {'name': 'dlib', 'desc': "Dobra dokładność, dla twarzy frontalnych. Bardzo wolny."},
    {'name': 'ssd', 'desc': "Szybki, dobra dokładność ogólna."},
    {'name': 'opencv', 'desc': "Najszybszy, najniższa dokładność."},
    {'name': 'hailo', 'desc': "Używa akceleratora Hailo-8 do detekcji. [bold yellow]Wymaga konfiguracji.[/bold yellow]"}
]

SETTINGS_FILE_PATH = Path("app_data/face_rec_settings.json")

# ##############################################################################
# ===               SEKCJA 2: FUNKCJE OBSŁUGI USTAWIEŃ                       ===
# ##############################################################################

def _read_face_rec_settings() -> dict:
    try:
        if not SETTINGS_FILE_PATH.exists(): return {}
        with open(SETTINGS_FILE_PATH, 'r', encoding='utf-8') as f: return json.load(f)
    except Exception as e:
        logger.error(f"Błąd odczytu pliku ustawień '{SETTINGS_FILE_PATH}': {e}"); return {}

def _write_face_rec_settings(new_settings: dict):
    try:
        all_settings = _read_face_rec_settings(); all_settings.update(new_settings)
        SETTINGS_FILE_PATH.parent.mkdir(exist_ok=True, parents=True)
        with open(SETTINGS_FILE_PATH, 'w', encoding='utf-8') as f: json.dump(all_settings, f, indent=4, ensure_ascii=False)
    except Exception as e: logger.error(f"Błąd zapisu ustawień do pliku '{SETTINGS_FILE_PATH}': {e}")

def get_current_settings() -> dict:
    """Zwraca pełen słownik ustawień z wartościami domyślnymi."""
    defaults = {
        "DETECTOR_BACKEND": "retinaface",
        "HAILO_SETTINGS": {
            "HEF_PATH": "",
            "PREPROC_LIB_PATH": "core/face_rec/hailo_preproc_example.py",
            "CONFIDENCE_THRESHOLD": 0.5
        },
        "FACE_REC_MEMORY_LIMIT_GB": None,
        "DLIB_UPSAMPLING": 1,
        "SCAN_FILES_LIMIT": None,
        "COMPUTE_BACKEND": "CPU",
        "NUM_AI_WORKERS": 1,
        "DETECTION_CONFIDENCE_THRESHOLD": 0.95,
        "ATTRIBUTE_DETECTION_THRESHOLD": 0.60,
    }
    settings = _read_face_rec_settings()
    defaults.update(settings)
    if "HAILO_SETTINGS" in settings:
        defaults["HAILO_SETTINGS"].update(settings["HAILO_SETTINGS"])
    return defaults

def get_current_detector() -> str:
    settings = get_current_settings()
    return settings.get("DETECTOR_BACKEND", "retinaface")

def set_memory_limit(gb_limit: float | None):
    """Ustawia limit pamięci wirtualnej dla procesu (tylko systemy POSIX)."""
    if not RESOURCE_AVAILABLE:
        if gb_limit: logger.warning("Ustawianie limitu pamięci nie jest wspierane w tym systemie operacyjnym.")
        return
    
    try:
        if gb_limit and gb_limit > 0:
            limit_bytes = int(gb_limit * 1024 * 1024 * 1024)
            resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
            logger.info(f"Pomyślnie ustawiono limit pamięci na {gb_limit} GB.")
        else:
            resource.setrlimit(resource.RLIMIT_AS, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
            logger.info("Limit pamięci został usunięty.")
    except (ValueError, resource.error) as e:
        logger.error(f"Nie udało się ustawić limitu pamięci: {e}")

async def advanced_settings_menu():
    while True:
        settings = get_current_settings()
        hailo_settings = settings.get('HAILO_SETTINGS', {})
        
        current_limit_str = f"{settings.get('FACE_REC_MEMORY_LIMIT_GB')} GB" if settings.get('FACE_REC_MEMORY_LIMIT_GB') else "Brak"
        dlib_mode_str = "Dokładny" if settings.get('DLIB_UPSAMPLING', 1) > 0 else "Szybki"
        files_limit_str = str(settings.get("SCAN_FILES_LIMIT") or "Brak")
        hef_path_status = "[green]OK[/]" if hailo_settings.get('HEF_PATH') and Path(hailo_settings['HEF_PATH']).is_file() else "[red]Brak[/]"
        preproc_path_status = "[green]OK[/]" if hailo_settings.get('PREPROC_LIB_PATH') and Path(hailo_settings['PREPROC_LIB_PATH']).is_file() else "[red]Brak[/]"

        console.clear()
        subtitle = (
            f"[dim]Detektor:[/dim] [bold cyan]{settings.get('DETECTOR_BACKEND')}[/] | "
            f"[dim]RAM:[/dim] [bold cyan]{current_limit_str}[/] | "
            f"[dim]Workery:[/dim] [bold cyan]{settings.get('NUM_AI_WORKERS', 1)}[/]"
        )
        console.print(Panel("⚙️ Ustawienia Zaawansowane AI ⚙️", style="bold yellow", subtitle=subtitle))

        menu_items = [
            ("[bold]Detektor twarzy[/]", "detector"),
            (f"Ścieżka do modelu Hailo-8 .hef ({hef_path_status})", "hailo_hef_path"),
            (f"Ścieżka do biblioteki pre-processingu Hailo ({preproc_path_status})", "hailo_preproc_path"),
            (f"Próg pewności detekcji Hailo ([cyan]{hailo_settings.get('CONFIDENCE_THRESHOLD', 0.5)}[/cyan])", "hailo_confidence"),
            ("[bold]Urządzenie dla DeepFace (CPU/GPU)[/]", "compute"),
            ("[bold]Limit pamięci RAM[/]", "limit"),
            ("[bold]Tryb detekcji Dlib[/]", "dlib_mode"),
            (f"Limit plików na sesję ([cyan]{files_limit_str}[/cyan])", "scan_limit"),
            (f"Liczba procesów AI (workerów) ([cyan]{settings.get('NUM_AI_WORKERS', 1)}[/cyan])", "workers"),
            (f"Próg pewności detekcji twarzy ([cyan]{settings.get('DETECTION_CONFIDENCE_THRESHOLD', 0.95):.0%}[/cyan])", "confidence"),
            (f"Próg pewności ANALIZY ATRYBUTÓW ([cyan]{settings.get('ATTRIBUTE_DETECTION_THRESHOLD', 0.60):.0%}[/cyan])", "attr_confidence"),
            ("Wróć", "exit")
        ]
        
        selected = await create_interactive_menu(menu_items, "Wybierz opcję do skonfigurowania")

        if selected == "detector":
            choices = [(f"{d['name']}\n[dim]{d['desc']}[/dim]", d['name']) for d in AVAILABLE_DETECTORS]
            new_detector = await create_interactive_menu(choices + [("Anuluj", "cancel")], "Wybierz domyślny detektor twarzy")
            if new_detector and new_detector != "cancel":
                _write_face_rec_settings({"DETECTOR_BACKEND": new_detector})
        
        elif selected == "hailo_hef_path":
            console.print(f"\n[dim]Aktualna ścieżka: {hailo_settings.get('HEF_PATH') or 'Nie ustawiono'}[/dim]")
            new_path = Prompt.ask("[cyan]Podaj pełną ścieżkę do pliku modelu .hef[/cyan]").strip()
            if Path(new_path).is_file():
                hailo_settings['HEF_PATH'] = new_path
                _write_face_rec_settings({"HAILO_SETTINGS": hailo_settings})
                console.print("[green]✅ Ścieżka zapisana.[/green]")
            else:
                console.print("[red]Błąd: Podana ścieżka nie jest prawidłowym plikiem.[/red]")
            await asyncio.sleep(1.5)
        
        elif selected == "hailo_preproc_path":
            console.print(f"\n[dim]Aktualna ścieżka: {hailo_settings.get('PREPROC_LIB_PATH')}[/dim]")
            new_path = Prompt.ask("[cyan]Podaj ścieżkę do pliku .py z logiką pre/post-processingu[/cyan]").strip()
            if Path(new_path).is_file():
                hailo_settings['PREPROC_LIB_PATH'] = new_path
                _write_face_rec_settings({"HAILO_SETTINGS": hailo_settings})
                console.print("[green]✅ Ścieżka zapisana.[/green]")
            else:
                console.print("[red]Błąd: Podana ścieżka nie jest prawidłowym plikiem.[/red]")
            await asyncio.sleep(1.5)

        elif selected == "hailo_confidence":
            try:
                new_threshold = float(Prompt.ask("[cyan]Podaj nowy próg pewności (0.0 do 1.0)[/cyan]", default=str(hailo_settings.get('CONFIDENCE_THRESHOLD', 0.5))))
                if 0.0 < new_threshold < 1.0:
                    hailo_settings['CONFIDENCE_THRESHOLD'] = new_threshold
                    _write_face_rec_settings({"HAILO_SETTINGS": hailo_settings})
                    console.print("[green]✅ Próg zapisany.[/green]")
                else:
                    console.print("[red]Błąd: Wartość musi być z zakresu (0, 1).[/red]")
            except ValueError:
                console.print("[red]Błąd: Nieprawidłowa wartość liczbowa.[/red]")
            await asyncio.sleep(1.5)

        elif selected == "compute":
            new_backend = await create_interactive_menu([("CPU", "CPU"), ("GPU (Nvidia)", "GPU"), ("Anuluj", "cancel")], "Wybierz urządzenie")
            if new_backend and new_backend != "cancel":
                _write_face_rec_settings({"COMPUTE_BACKEND": new_backend})

        elif selected == "limit":
            limit_choices = [ ("Brak limitu", None), ("2 GB", 2.0), ("4 GB", 4.0), ("6 GB (Zalecane dla RPi 5 8GB)", 6.0), ("8 GB", 8.0), ("Anuluj", "cancel") ]
            new_limit = await create_interactive_menu(limit_choices, "Wybierz limit pamięci RAM dla procesów AI")
            if new_limit != "cancel":
                _write_face_rec_settings({"FACE_REC_MEMORY_LIMIT_GB": new_limit})
                console.print(f"[green]✅ Ustawiono limit RAM na: {new_limit or 'Brak'}[/green]")
                await asyncio.sleep(1.5)

        elif selected == "dlib_mode":
            new_mode = await create_interactive_menu([("Dokładny (wolniejszy)", "1"), ("Szybki (mniej dokładny)", "0"), ("Anuluj", "cancel")], "Wybierz tryb detekcji Dlib")
            if new_mode and new_mode != "cancel":
                _write_face_rec_settings({"DLIB_UPSAMPLING": int(new_mode)})

        elif selected == "scan_limit":
            try:
                new_limit_str = Prompt.ask(f"[cyan]Podaj limit plików na sesję (0 lub puste = bez limitu)[/cyan]", default=str(settings.get('SCAN_FILES_LIMIT') or '0'))
                new_limit = int(new_limit_str) if new_limit_str.strip() else 0
                _write_face_rec_settings({"SCAN_FILES_LIMIT": new_limit if new_limit > 0 else None})
            except ValueError:
                console.print("[red]Błąd: Nieprawidłowa wartość liczbowa.[/red]")
            await asyncio.sleep(1.5)

        elif selected == "workers":
            try:
                new_workers_str = Prompt.ask(f"[cyan]Podaj liczbę procesów roboczych AI[/cyan]", default=str(settings.get('NUM_AI_WORKERS', 1)))
                new_workers = int(new_workers_str)
                if new_workers > 0:
                    _write_face_rec_settings({"NUM_AI_WORKERS": new_workers})
                else:
                    console.print("[red]Liczba workerów musi być większa od zera.[/red]")
            except ValueError:
                console.print("[red]Błąd: Nieprawidłowa wartość liczbowa.[/red]")
            await asyncio.sleep(1.5)

        elif selected == "confidence":
            try:
                threshold_str = Prompt.ask(f"[cyan]Podaj nowy próg pewności detekcji (0.01 do 1.0)[/cyan]", default=str(settings.get('DETECTION_CONFIDENCE_THRESHOLD', 0.95)))
                new_threshold = float(threshold_str)
                if 0.0 < new_threshold <= 1.0:
                    _write_face_rec_settings({"DETECTION_CONFIDENCE_THRESHOLD": new_threshold})
                    console.print("[green]✅ Próg pewności zapisany.[/green]")
                else:
                    console.print("[red]Błąd: Wartość musi być z zakresu (0, 1].[/red]")
            except ValueError:
                console.print("[red]Błąd: Nieprawidłowa wartość liczbowa.[/red]")
            await asyncio.sleep(1.5)

        elif selected == "attr_confidence":
            try:
                threshold_str = Prompt.ask(f"[cyan]Podaj nowy próg pewności dla analizy atrybutów (0.01 do 1.0)[/cyan]", default=str(settings.get('ATTRIBUTE_DETECTION_THRESHOLD', 0.60)))
                new_threshold = float(threshold_str)
                if 0.0 < new_threshold <= 1.0:
                    _write_face_rec_settings({"ATTRIBUTE_DETECTION_THRESHOLD": new_threshold})
                    console.print("[green]✅ Próg pewności zapisany.[/green]")
                else:
                    console.print("[red]Błąd: Wartość musi być z zakresu (0, 1].[/red]")
            except ValueError:
                console.print("[red]Błąd: Nieprawidłowa wartość liczbowa.[/red]")
            await asyncio.sleep(1.5)

        elif selected in ("exit", None):
            break

# ##############################################################################
# ===                SEKCJA 3: FUNKCJE POMOCNICZE AI                         ===
# ##############################################################################

def findCosineDistance(source_representation, test_representation):
    a = np.asarray(source_representation); b = np.asarray(test_representation)
    if np.linalg.norm(a) == 0 or np.linalg.norm(b) == 0: return 1.0
    return 1 - (np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

def findThreshold(model_name: str, distance_metric: str) -> float:
    default_threshold = 0.40 if distance_metric == "cosine" else 1.0
    return MODEL_THRESHOLDS.get(model_name, {}).get(distance_metric, default_threshold)
