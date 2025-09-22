# Plik: core/face_rec/recognition.py
# Wersja 2.0 - Zaimplementowano zoptymalizowane, zwektoryzowane rozpoznawanie

import logging
import numpy as np

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.progress import Progress, BarColumn, TextColumn, SpinnerColumn

from ..database import (
    setup_database, get_all_people, get_unknown_faces, tag_face
)
from .settings import findThreshold

console = Console(record=True)
logger = logging.getLogger(__name__)

# ##############################################################################
# ===           SEKCJA 1: LOGIKA AUTOMATYCZNEGO ROZPOZNAWANIA                ===
# ##############################################################################

# ZMIANA: Usunięto findCosineDistance, ponieważ numpy zrobi to za nas
# w zoptymalizowany sposób.

async def recognize_and_tag_faces(model_name: str):
    """
    Automatycznie rozpoznaje i taguje nieznane twarze w bazie danych,
    używając zoptymalizowanych operacji wektorowych numpy.
    """
    console.clear()
    console.print(Panel(f"🕵️ Rozpoznawanie Twarzy (Model: [cyan]{model_name}[/cyan]) 🕵️", style="bold blue"))
    await setup_database()

    with console.status("[cyan]Pobieranie danych z bazy...[/]"):
        known_people = await get_all_people(model_name)
        unknown_faces = await get_unknown_faces(model_name)

    if not known_people:
        console.print(f"[yellow]Brak zdefiniowanych osób dla modelu '{model_name}'.[/yellow]")
        Prompt.ask("\n[bold]Naciśnij Enter...[/]"); return
        
    if not unknown_faces:
        console.print("[green]✅ Brak nowych twarzy do rozpoznania.[/green]")
        Prompt.ask("\n[bold]Naciśnij Enter...[/]"); return

    console.print(f"Znaleziono [cyan]{len(known_people)}[/cyan] znanych osób i [cyan]{len(unknown_faces)}[/cyan] nieotagowanych twarzy do analizy.")
    
    threshold = findThreshold(model_name, 'cosine')
    
    # --- POCZĄTEK NOWEJ, ZOPTYMALIZOWANEJ LOGIKI ---
    
    # Krok 1: Przygotuj macierze numpy
    # Macierz embeddingów znanych osób
    known_embeddings = np.array([p['master_embedding'] for p in known_people])
    # Lista ID znanych osób, w tej samej kolejności co embeddingi
    known_person_ids = [p['person_id'] for p in known_people]
    
    # Macierz embeddingów nieznanych twarzy
    unknown_embeddings = np.array([f['embedding'] for f in unknown_faces])
    # Lista ID nieznanych twarzy, w tej samej kolejności
    unknown_face_ids = [f['face_id'] for f in unknown_faces]

    # Krok 2: Normalizuj wektory (kluczowe dla odległości kosinusowej)
    known_embeddings /= np.linalg.norm(known_embeddings, axis=1, keepdims=True)
    unknown_embeddings /= np.linalg.norm(unknown_embeddings, axis=1, keepdims=True)

    # Krok 3: Oblicz wszystkie odległości za jednym zamachem!
    # Mnożenie macierzy da nam iloczyny skalarne (podobieństwo kosinusowe).
    # Wynikiem jest macierz o wymiarach (liczba_nieznanych x liczba_znanych).
    with console.status("[cyan]Obliczanie podobieństwa (operacja wektorowa)...[/]"):
        similarity_matrix = np.dot(unknown_embeddings, known_embeddings.T)
        distance_matrix = 1 - similarity_matrix # Konwersja podobieństwa na odległość

    # Krok 4: Znajdź najlepsze dopasowania i otaguj
    updates_to_commit = []
    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%",
        transient=True
    ) as progress:
        task = progress.add_task("[green]Tagowanie...", total=len(unknown_faces))
        
        for i, face_distances in enumerate(distance_matrix):
            # Znajdź indeks i wartość najlepszego dopasowania dla i-tej twarzy
            best_match_index = np.argmin(face_distances)
            min_distance = face_distances[best_match_index]
            
            # Sprawdź, czy dopasowanie jest wystarczająco dobre (poniżej progu)
            if min_distance < threshold:
                face_id_to_tag = unknown_face_ids[i]
                person_id_to_assign = known_person_ids[best_match_index]
                # Zamiast od razu pisać do bazy, zbieramy aktualizacje
                updates_to_commit.append((person_id_to_assign, face_id_to_tag))
            
            progress.update(task, advance=1)

    # Krok 5: Zapisz wszystkie zmiany do bazy w jednej transakcji
    if updates_to_commit:
        with console.status(f"[cyan]Zapisywanie {len(updates_to_commit)} tagów w bazie danych...[/]"):
            # Potrzebujemy nowej funkcji w database.py do zapisu wsadowego
            from ..database import tag_faces_batch
            await tag_faces_batch(updates_to_commit)

    # --- KONIEC NOWEJ, ZOPTYMALIZOWANEJ LOGIKI ---

    console.print(f"\n[bold green]✅ Rozpoznawanie zakończone! Otagowano {len(updates_to_commit)} nowych twarzy.[/bold green]")
    Prompt.ask("\n[bold]Naciśnij Enter...[/]")
