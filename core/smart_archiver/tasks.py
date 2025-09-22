# plik: core/smart_archiver/tasks.py
# Wersja 1.1 - Usprawniono dynamiczne ładowanie zależności.
# Opis: Ten moduł zawiera czystą logikę biznesową dla Asystenta Porządkowania.
#       Funkcje te są zaprojektowane do uruchamiania w osobnych procesach
#       za pomocą `ProcessPoolExecutor`, aby nie blokować głównej pętli
#       aplikacji i w pełni wykorzystać dostępne rdzenie procesora.
# -*- coding: utf-8 -*-

import logging
import asyncio
from pathlib import Path
from typing import List, Dict, Any
#from typing import List, Set, Dict, Any, Optional, Tuple

from ..database import update_analysis_results

# Ciężkie zależności są celowo importowane jako None.
# Zostaną one załadowane dynamicznie tylko w procesach potomnych,
# co znacznie przyspiesza start głównej aplikacji.
Image, UnidentifiedImageError, cv2, np = None, None, None, None

logger = logging.getLogger(__name__)

def _initialize_dependencies():
    """
    Dynamicznie ładuje ciężkie biblioteki (Pillow, OpenCV, NumPy)
    w procesie potomnym, w którym jest wywoływana.
    """
    global Image, UnidentifiedImageError, cv2, np
    # Sprawdzamy, czy biblioteki nie zostały już załadowane w tym procesie
    if Image is None:
        try:
            from PIL import Image, UnidentifiedImageError
            import cv2
            import numpy as np
        except ImportError:
            # Ta sytuacja jest obsługiwana w `cli.py` przed uruchomieniem analizy.
            # Tutaj tylko logujemy błąd, jeśli wystąpi w procesie potomnym.
            logger.error("Brak kluczowych bibliotek (Pillow, OpenCV) w procesie potomnym.")

def is_blurry(image_path: Path, threshold: int = 100) -> bool:
    """
    Sprawdza, czy obraz jest nieostry, używając wariancji Laplasjanu.
    Niska wariancja sugeruje mało krawędzi, a więc potencjalne rozmycie.
    """
    try:
        image = cv2.imread(str(image_path))
        if image is None: return False # Nie udało się wczytać obrazu
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        laplacian_var = cv2.Laplacian(gray, cv2.CV_64F).var()
        return laplacian_var < threshold
    except Exception:
        # Ignorujemy błędy, np. dla uszkodzonych plików
        return False

def is_dark(image_path: Path, threshold: int = 60) -> bool:
    """
    Sprawdza, czy obraz jest zbyt ciemny, licząc średnią jasność
    wszystkich pikseli w skali szarości.
    """
    try:
        image = cv2.imread(str(image_path))
        if image is None: return False
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        return np.mean(gray) < threshold
    except Exception:
        return False

def is_too_small(image_path: Path, size_kb: int = 50) -> bool:
    """Sprawdza, czy rozmiar pliku na dysku jest poniżej zadanego progu."""
    try:
        return image_path.stat().st_size < (size_kb * 1024)
    except (OSError, FileNotFoundError):
        return False

def is_corrupted(image_path: Path) -> bool:
    """
    Sprawdza, czy plik obrazu jest uszkodzony, próbując go otworzyć
    i zweryfikować za pomocą biblioteki Pillow.
    """
    try:
        with Image.open(image_path) as img:
            img.verify() # Sprawdza integralność danych obrazu
        return False
    except (UnidentifiedImageError, IOError, SyntaxError):
        # Te wyjątki jednoznacznie wskazują na uszkodzony lub nieobsługiwany format
        return True
    except Exception:
        # Inne, nieoczekiwane błędy (np. błędy dekompresji) również traktujemy jako potencjalne uszkodzenie
        return True

def analyze_single_image(media_id: int, image_path_str: str) -> Dict:
    """
    Funkcja robocza, która wykonuje analizę i OD RAZU zapisuje wynik do bazy.
    Zwraca tylko podsumowanie dla dashboardu.
    """
    _initialize_dependencies()
    image_path = Path(image_path_str)
    
    if not all([Image, cv2, np]):
        return {'path': image_path, 'id': media_id}
        
    # === POCZĄTEK POPRAWKI: Rzutowanie wyników na standardowy typ bool ===
    analysis = {
        'is_blurry': bool(is_blurry(image_path)),
        'is_dark': bool(is_dark(image_path)),
        'is_small': bool(is_too_small(image_path)),
        'is_corrupted': bool(is_corrupted(image_path))
    }
    # === KONIEC POPRAWKI ===
    
    # Uruchamiamy asynchroniczną funkcję zapisu w nowej pętli zdarzeń,
    # ponieważ jesteśmy w osobnym procesie.
    try:
        asyncio.run(update_analysis_results(media_id, analysis))
    except Exception as e:
        # Logujemy błąd, ale nie przerywamy analizy innych plików
        logger.error(f"Błąd zapisu wyników analizy do bazy dla ID {media_id}: {e}")
    
    # Zwracamy pełne wyniki, aby dashboard mógł je wyświetlić
    return {'path': image_path, 'id': media_id, **analysis}
