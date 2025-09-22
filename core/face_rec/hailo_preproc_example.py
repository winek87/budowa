# Plik: core/face_rec/hailo_preproc_example.py (NOWY PLIK)
#
# ##############################################################################
# ===      PRZYKŁADOWA BIBLIOTEKA PRE- I POST-PROCESSIGNU DLA HAILO-8        ===
# ##############################################################################
#
# WAŻNE: Ten plik jest szablonem! Musisz dostosować funkcje `preprocess`
# i `postprocess` do specyfikacji modelu detekcji twarzy, którego używasz.
#
# Poniższy przykład jest napisany dla popularnego modelu `yolov5face_n_640.hef`.
#

import numpy as np
import cv2
from typing import List, Dict, Any, Tuple

def preprocess(image: np.ndarray) -> np.ndarray:
    """
    Przygotowuje obraz do formatu oczekiwanego przez sieć neuronową.
    
    Args:
        image (np.ndarray): Obraz wczytany przez OpenCV (format BGR).

    Returns:
        np.ndarray: Przetworzony obraz, gotowy do wysłania do akceleratora.
    """
    # Specyfikacja dla yolov5face_n_640
    input_shape = (640, 640)
    
    # 1. Zmiana rozmiaru
    img_resized = cv2.resize(image, input_shape)
    
    # 2. Konwersja BGR -> RGB
    img_rgb = cv2.cvtColor(img_resized, cv2.COLOR_BGR2RGB)
    
    # 3. Normalizacja wartości pikseli do zakresu [0, 1]
    img_normalized = img_rgb.astype(np.float32) / 255.0
    
    # 4. Dodanie wymiaru "batch" (zawsze 1)
    return np.expand_dims(img_normalized, axis=0)

def postprocess(
    raw_results: List[np.ndarray], 
    original_shape: Tuple[int, int], 
    confidence_threshold: float
) -> List[Dict[str, Any]]:
    """
    Przetwarza surowe dane wyjściowe z modelu Hailo, aby uzyskać współrzędne twarzy.

    Args:
        raw_results (List[np.ndarray]): Lista tensorów wyjściowych z Hailo.
        original_shape (Tuple[int, int]): Wysokość i szerokość oryginalnego obrazu.
        confidence_threshold (float): Próg pewności, powyżej którego detekcja jest akceptowana.

    Returns:
        List[Dict[str, Any]]: Lista słowników, gdzie każdy reprezentuje wykrytą twarz.
    """
    # Specyfikacja dla yolov5face_n_640
    predictions = raw_results[0]
    
    faces = []
    original_h, original_w = original_shape[:2]
    
    # Przetwarzamy predykcje z modelu YOLOv5Face
    for row in predictions[0]:
        confidence = row[4]
        if confidence > confidence_threshold:
            # Skalowanie współrzędnych z [0,1] do oryginalnego rozmiaru obrazu
            x_center, y_center, width, height = row[0:4]
            
            x1 = int((x_center - width / 2) * original_w)
            y1 = int((y_center - height / 2) * original_h)
            w_box = int(width * original_w)
            h_box = int(height * original_h)
            
            # Zabezpieczenie przed wyjściem poza obraz
            x1 = max(0, x1)
            y1 = max(0, y1)
            
            faces.append({
                'x': x1,
                'y': y1,
                'w': w_box,
                'h': h_box,
                'confidence': float(confidence)
            })
            
    return faces
