# -*- coding: utf-8 -*-

# plik: core/ai_tagger_logic.py
# Wersja 3.0 - Pełna integracja z ekosystemem TensorFlow/Keras

# --- GŁÓWNE IMPORTY ---
import logging
import asyncio
import json
from pathlib import Path
from typing import List

# --- Zależności AI (TensorFlow/Keras) ---
try:
    from PIL import Image, UnidentifiedImageError
    import numpy as np
    # Importujemy potrzebne komponenty bezpośrednio z TensorFlow i Keras
    import tensorflow as tf
    from tensorflow.keras.applications import EfficientNetV2B0
    from tensorflow.keras.applications.efficientnet_v2 import preprocess_input, decode_predictions
except ImportError:
    # Definiujemy puste obiekty, aby uniknąć błędów
    Image, UnidentifiedImageError, np, tf = None, None, None, None
    EfficientNetV2B0, preprocess_input, decode_predictions = None, None, None

# --- Importy asynchroniczne ---
import aiosqlite

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.progress import Progress, BarColumn, TextColumn, SpinnerColumn

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import DATABASE_FILE, AI_MODELS_CACHE_DIR, AI_TAGGER_CONFIDENCE_THRESHOLD
from .utils import create_interactive_menu, check_dependency
from .database import setup_database

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console()
logger = logging.getLogger(__name__)

# Definiujemy model jako stałą
MODEL_CLASS = EfficientNetV2B0


# --- GŁÓWNA KLASA SILNIKA AI ---

class AITagger:
    """
    Klasa enkapsulująca logikę ładowania modelu Keras i tagowania obrazów.
    """
    def __init__(self):
        """Inicjalizuje Tagger."""
        if AI_MODELS_CACHE_DIR:
            # Keras używa innego mechanizmu cache, ale ustawienie tego nie zaszkodzi
            Path(AI_MODELS_CACHE_DIR).mkdir(parents=True, exist_ok=True)
            
        self.model = None
        # Model Keras sam zarządza urządzeniem (CPU/GPU)
        logger.info("Inicjalizacja silnika AI Tagger (TensorFlow/Keras)...")

    async def load_model(self):
        """Asynchronicznie ładuje model Keras z wagami ImageNet."""
        if self.model:
            return

        with console.status(f"[cyan]Ładowanie modelu AI '{MODEL_CLASS.__name__}'...[/]"):
            try:
                def _load():
                    # Ładujemy model z wagami wytrenowanymi na zbiorze ImageNet
                    return MODEL_CLASS(weights="imagenet")

                self.model = await asyncio.to_thread(_load)
                logger.info("Model AI został pomyślnie załadowany.")
            except Exception as e:
                raise RuntimeError(f"Błąd podczas ładowania modelu: {e}")

    async def tag_image(self, image_path: Path) -> List[str]:
        """Analizuje obraz i zwraca listę najbardziej prawdopodobnych tagów."""
        if not self.model:
            raise RuntimeError("Model AI nie jest załadowany.")
            
        try:
            def _process_and_infer():
                # Model EfficientNetV2B0 oczekuje obrazów w rozmiarze 224x224
                img = Image.open(image_path).convert("RGB")
                img = img.resize((224, 224))
                
                # Konwersja obrazu do tablicy NumPy
                img_array = np.array(img)
                img_array = np.expand_dims(img_array, axis=0)
                
                # Specjalne przetwarzanie wstępne wymagane przez ten model
                processed_img = preprocess_input(img_array)
                
                # Predykcja
                predictions = self.model.predict(processed_img, verbose=0)
                
                # Dekodowanie wyników na zrozumiałe etykiety
                decoded = decode_predictions(predictions, top=5)[0]
                
                # Zwracamy tylko te etykiety, których prawdopodobieństwo jest wystarczająco wysokie
                tags = [label for _, label, score in decoded if score > AI_TAGGER_CONFIDENCE_THRESHOLD]
                return tags

            return await asyncio.to_thread(_process_and_infer)
            
        except (UnidentifiedImageError, ValueError):
            logger.warning(f"Pominięto plik, który nie jest poprawnym obrazem: {image_path.name}")
            return []
        except Exception:
            logger.error(f"Nie udało się otagować obrazu '{image_path.name}'.", exc_info=True)
            return []

# --- GŁÓWNA LOGIKA PRZETWARZANIA ---

async def run_ai_tagger_process():
    """Uruchamia główny proces tagowania obrazów w kolekcji."""
    console.clear()
    console.print(Panel("🤖 Tagowanie Obrazów (Silnik: Keras) 🤖", expand=False, style="bold blue"))

    tagger = AITagger()

    await setup_database()
    images_to_process = []
    with console.status("[cyan]Pobieranie listy obrazów z bazy...[/]"):
        try:
            async with aiosqlite.connect(DATABASE_FILE) as conn:
                conn.row_factory = aiosqlite.Row
                query = """
                    SELECT id, final_path FROM downloaded_media 
                    WHERE status = 'downloaded' AND (ai_tags IS NULL OR ai_tags = '' OR ai_tags = '[]')
                    AND (LOWER(final_path) LIKE '%.jpg' OR LOWER(final_path) LIKE '%.jpeg' OR LOWER(final_path) LIKE '%.png')
                """
                cursor = await conn.execute(query)
                images_to_process = await cursor.fetchall()
        except aiosqlite.Error as e:
            console.print(f"[bold red]Błąd bazy danych: {e}[/]"); return
        
    if not images_to_process:
        console.print("\n[bold green]✅ Wszystkie obrazy w bazie zostały już otagowane.[/bold green]"); return

    if not Confirm.ask(f"\nZnaleziono [bold cyan]{len(images_to_process)}[/bold cyan] obrazów. Rozpocząć tagowanie?", default=True):
        return

    try:
        await tagger.load_model()
    except RuntimeError as e:
        console.print(f"[bold red]Błąd ładowania modelu: {e}[/]"); return

    updates_batch, BATCH_SIZE = [], 50
    with Progress(TextColumn("[cyan]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", transient=True) as progress:
        task = progress.add_task("Tagowanie...", total=len(images_to_process))
        for record in images_to_process:
            image_path = Path(record['final_path'])
            if not await asyncio.to_thread(image_path.exists):
                progress.update(task, advance=1); continue

            tags = await tagger.tag_image(image_path)
            
            if tags:
                # Zapisujemy tagi jako string JSON
                updates_batch.append((json.dumps(tags), record['id']))
            
            progress.update(task, advance=1)
            
            if len(updates_batch) >= BATCH_SIZE:
                async with aiosqlite.connect(DATABASE_FILE) as conn:
                    await conn.executemany("UPDATE downloaded_media SET ai_tags = ? WHERE id = ?", updates_batch)
                    await conn.commit()
                updates_batch.clear()

    if updates_batch:
        async with aiosqlite.connect(DATABASE_FILE) as conn:
            await conn.executemany("UPDATE downloaded_media SET ai_tags = ? WHERE id = ?", updates_batch)
            await conn.commit()
        
    console.print("\n[bold green]✅ Proces tagowania AI został zakończony![/bold green]")

# --- GŁÓWNA FUNKCJA URUCHOMIENIOWA ---

async def run_ai_tagger_menu():
    """Wyświetla i zarządza uproszczonym menu dla modułu Tagowania AI."""
    console.clear()
    
    dependencies_ok = all([
        check_dependency("tensorflow", "tensorflow", "TensorFlow"),
        check_dependency("PIL", "Pillow", "Pillow"),
        check_dependency("numpy", "numpy", "NumPy")
    ])
    if not dependencies_ok:
        Prompt.ask("\n[yellow]Brak kluczowych zależności. Naciśnij Enter...[/yellow]")
        return

    while True:
        console.clear()
        console.print(Panel("🤖 Inteligentne Tagowanie Obrazów (AI) 🤖", expand=False, style="bold blue"))
        menu_items = [
            ("Uruchom tagowanie dla nowych obrazów", "run_tagging"),
            ("Wróć do menu głównego", "exit")
        ]
        selected_action = await create_interactive_menu(menu_items, "Wybierz operację")

        if selected_action in ["exit", None]:
            break
        
        if selected_action == "run_tagging":
            await run_ai_tagger_process()
            Prompt.ask("\n[bold]Operacja zakończona. Naciśnij Enter...[/]")
