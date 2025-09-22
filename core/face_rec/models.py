# Plik: core/face_rec/models.py
# Wersja 2.0 - Zintegrowana obsługa wyboru CPU/GPU

import asyncio
import logging
import os
import subprocess
from pathlib import Path
import requests

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.progress import Progress, BarColumn, TextColumn, DownloadColumn, TransferSpeedColumn

from ..config import AI_MODELS_CACHE_DIR
from ..utils import create_interactive_menu
# ZMIANA: Importujemy teraz funkcje odczytu ustawień
from .settings import AVAILABLE_MODELS, MODEL_FILE_MAP, _read_face_rec_settings

console = Console(record=True)
logger = logging.getLogger(__name__)


# ##############################################################################
# ===                   SEKCJA 1: FUNKCJE POMOCNICZE                         ===
# ##############################################################################

def _get_deepface_base_path() -> Path:
    """Zwraca główną ścieżkę do cache'u modeli AI."""
    return Path(AI_MODELS_CACHE_DIR) if AI_MODELS_CACHE_DIR else Path.home()

def _get_deepface_home_path() -> Path:
    """Zwraca ścieżkę do folderu `.deepface`, gdzie przechowywane są wagi."""
    return _get_deepface_base_path() / ".deepface"

def initialize_deepface_env():
    """
    Przygotowuje środowisko dla DeepFace. Tworzy foldery, ustawia zmienną
    środowiskową DEEPFACE_HOME i konfiguruje TensorFlow do użycia CPU lub GPU.
    """
    base_path = _get_deepface_base_path()
    (base_path / ".deepface" / "weights").mkdir(parents=True, exist_ok=True)
    os.environ['DEEPFACE_HOME'] = str(base_path)
    logger.info(f"Środowisko DeepFace zainicjalizowane w: {base_path}")

    # --- POCZĄTEK ZMIAN: Logika wyboru CPU/GPU ---
    settings = _read_face_rec_settings()
    compute_backend = settings.get("COMPUTE_BACKEND", "CPU")
    
    if compute_backend == "GPU":
        logger.info("Próba konfiguracji TensorFlow do użycia GPU...")
        try:
            # Ustawiamy zmienną środowiskową, która jest jednym ze sposobów
            # kontrolowania widoczności urządzeń dla TensorFlow. Pusta wartość
            # oznacza "użyj wszystkich dostępnych GPU".
            os.environ['CUDA_VISIBLE_DEVICES'] = "0" # Użyj pierwszego GPU
            
            # Dodatkowo, próbujemy aktywnie skonfigurować TensorFlow
            import tensorflow as tf
            gpus = tf.config.list_physical_devices('GPU')
            if gpus:
                logger.info(f"Znaleziono {len(gpus)} urządzeń GPU. TensorFlow będzie ich używać.")
                console.print("[dim green]Tryb GPU aktywny.[/dim green]")
                # W nowszych wersjach TF nie trzeba już nic więcej robić.
            else:
                logger.warning("Nie znaleziono urządzeń GPU kompatybilnych z TensorFlow. Powrót do trybu CPU.")
                console.print("[dim yellow]Nie znaleziono GPU. Powrót do trybu CPU.[/dim yellow]")
                os.environ['CUDA_VISIBLE_DEVICES'] = "-1" # Mówimy TF, aby ignorował GPU
        except Exception as e:
            logger.error(f"Wystąpił błąd podczas konfiguracji GPU: {e}. Powrót do trybu CPU.")
            os.environ['CUDA_VISIBLE_DEVICES'] = "-1"
    else:
        logger.info("Konfiguracja TensorFlow do użycia tylko CPU.")
        os.environ['CUDA_VISIBLE_DEVICES'] = "-1"
    # --- KONIEC ZMIAN ---

async def is_model_downloaded(model_name: str) -> bool:
    """Sprawdza asynchronicznie, czy pliki wag dla danego modelu istnieją na dysku."""
    model_filenames = MODEL_FILE_MAP.get(model_name)
    if not model_filenames:
        logger.warning(f"Nie znaleziono mapowania pliku dla modelu '{model_name}'.")
        return False
        
    # ZMIANA: Ujednolicona logika dla Dlib i innych modeli
    # Upewniamy się, że model_filenames jest zawsze listą
    if not isinstance(model_filenames, list):
        model_filenames = [model_filenames]

    weights_path = _get_deepface_home_path() / "weights"
    for filename in model_filenames:
        model_path = weights_path / filename
        if not await asyncio.to_thread(model_path.exists):
            return False
            
    return True

# Funkcja _download_and_unzip_bzip2_with_subprocess pozostaje bez zmian
def _download_and_unzip_bzip2_with_subprocess(url: str, dest_path: Path):
    """Pobiera plik .bz2, pokazuje postęp i rozpakowuje go za pomocą systemowego `bzip2`."""
    compressed_path = dest_path.with_suffix(dest_path.suffix + ".bz2")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            with Progress(
                TextColumn("[progress.description]{task.description}"), BarColumn(),
                DownloadColumn(), TransferSpeedColumn(), transient=True
            ) as progress:
                task_id = progress.add_task(f"Pobieranie {dest_path.name}", total=total_size)
                with open(compressed_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk); progress.update(task_id, advance=len(chunk))
        logger.info(f"Rozpakowywanie {compressed_path} za pomocą 'bzip2'...")
        subprocess.run(['bzip2', '-d', str(compressed_path)], check=True, capture_output=True, text=True)
        logger.info(f"Pobrano i rozpakowano {dest_path.name}.")
    except FileNotFoundError:
        logger.critical("BŁĄD: Nie znaleziono polecenia 'bzip2'. Zainstaluj bzip2."); raise
    except subprocess.CalledProcessError as e:
        logger.critical(f"Błąd dekompresji: {e.stderr}"); compressed_path.unlink(missing_ok=True); raise
    except Exception as e:
        compressed_path.unlink(missing_ok=True); logger.critical(f"Błąd pobierania/dekompresji: {e}"); raise

# ##############################################################################
# ===                SEKCJA 2: GŁÓWNA LOGIKA ZARZĄDZANIA                     ===
# ##############################################################################

async def manage_ai_models():
    """Wyświetla interfejs do zarządzania (pobierania) dostępnymi modelami AI."""
    while True:
        console.clear()
        console.print(Panel("🤖 Menedżer Modeli AI 🤖", style="bold blue"))
        with console.status("[cyan]Sprawdzanie statusu modeli...[/]"):
            statuses = await asyncio.gather(*[is_model_downloaded(m['name']) for m in AVAILABLE_MODELS])
        menu_items = [(f"{m['name']} ({'[green]Pobrany[/]' if status else '[red]Niepobrany[/]'})", m['name']) for m, status in zip(AVAILABLE_MODELS, statuses)]
        menu_items.append(("Wróć", "exit"))
        selected_model_name = await create_interactive_menu(menu_items, "Wybierz model do zarządzania")
        if selected_model_name in ["exit", None]: break

        if await is_model_downloaded(selected_model_name):
            console.print(f"\n[green]Model [cyan]{selected_model_name}[/cyan] jest już pobrany.[/green]")
        elif Confirm.ask(f"\nPobrać model [cyan]{selected_model_name}[/cyan]?", default=True):
            async def download_model_logic(model_name: str):
                from deepface import DeepFace
                initialize_deepface_env() # Upewniamy się, że środowisko jest gotowe (z obsługą CPU/GPU)
                
                # ZMIANA: Obsługa specjalnych przypadków Dlib w jednym miejscu
                if model_name == 'Dlib':
                    console.print("Pobieranie komponentów dla [cyan]Dlib[/]...")
                    predictor_url = "http://dlib.net/files/shape_predictor_5_face_landmarks.dat.bz2"
                    predictor_path = _get_deepface_home_path() / "weights" / "shape_predictor_5_face_landmarks.dat"
                    if not predictor_path.exists():
                        await asyncio.to_thread(_download_and_unzip_bzip2_with_subprocess, predictor_url, predictor_path)
                    
                    # Pobieramy główny model Dlib (drugi plik)
                    await asyncio.to_thread(DeepFace.build_model, model_name)
                else:
                    console.print(f"Pobieranie komponentu dla [cyan]{model_name}[/]...")
                    await asyncio.to_thread(DeepFace.build_model, model_name)
            try:
                await download_model_logic(selected_model_name)
                console.print(f"\n[green]✅ Model {selected_model_name} pobrany pomyślnie.[/green]")
            except Exception as e:
                console.print(f"\n[bold red]❌ Błąd pobierania modelu {selected_model_name}:[/]\n[dim]{e}[/dim]")
                logger.error(f"Błąd pobierania modelu {selected_model_name}", exc_info=True)
        Prompt.ask("\n[bold]Naciśnij Enter...[/bold]")
