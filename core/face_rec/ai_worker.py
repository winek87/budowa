# Plik: core/face_rec/ai_worker.py (Wersja 2.7 - Zoptymalizowane filtrowanie)

import logging
import logging.handlers
import multiprocessing as mp
import os
import sys
from pathlib import Path
from typing import List, Dict, Any
import numpy as np

# --- Importy z własnych modułów ---
from .models import _get_deepface_home_path
from .settings import _read_face_rec_settings
from .. import config as core_config

logger = logging.getLogger(__name__)

class FaceRecognitionAI:
    """
    Klasa-fasada, która używa zróżnicowanych strategii przetwarzania.
    """
    # --- ZMIANA: Dodajemy confidence_threshold do konstruktora ---
    def __init__(self, model_name: str, deepface_home: str | None = None, confidence_threshold: float = 0.95):
        self.model_name = model_name
        settings = _read_face_rec_settings()
        self.detector_backend = settings.get("DETECTOR_BACKEND", "retinaface")
        self.dlib_upsampling = settings.get("DLIB_UPSAMPLING", 1)
        # --- ZMIANA: Zapisujemy próg pewności ---
        self.confidence_threshold = confidence_threshold
        self.is_dlib = model_name == "Dlib"
        if deepface_home:
            os.environ['DEEPFACE_HOME'] = deepface_home
        self.dlib_detector, self.shape_predictor, self.face_recognizer = None, None, None
        self._load_models()

    def _load_models(self):
        """Ładuje modele do pamięci."""
        logger.info(f"[{self.model_name} Engine] Ładowanie modeli (detektor: {self.detector_backend})...")
        if self.is_dlib:
            import dlib
            weights_path = _get_deepface_home_path() / "weights"
            self.dlib_detector = dlib.get_frontal_face_detector()
            self.shape_predictor = dlib.shape_predictor(str(weights_path / "shape_predictor_5_face_landmarks.dat"))
            self.face_recognizer = dlib.face_recognition_model_v1(str(weights_path / "dlib_face_recognition_resnet_model_v1.dat"))
        else:
            from deepface import DeepFace
            DeepFace.build_model(self.model_name)
        logger.info(f"[{self.model_name} Engine] Modele załadowane pomyślnie.")

    def represent(self, img_path_str: str, enforce_detection: bool = False) -> tuple[bool, list | str]:
        """
        Główna metoda, która decyduje, której strategii przetwarzania użyć.
        """
        try:
            if self.is_dlib:
                result = self._represent_dlib_safe(img_path_str, enforce_detection)
            else:
                result = self._represent_deepface_direct(img_path_str, enforce_detection)
            return True, result
        except Exception as e:
            error_msg = f"Błąd silnika AI ({type(e).__name__}) dla {Path(img_path_str).name}: {e}"
            logger.error(error_msg, exc_info=False)
            return False, error_msg

    def _represent_deepface_direct(self, img_path: str, enforce_detection: bool) -> List[Dict[str, Any]]:
        """Wywołuje DeepFace i filtruje oraz normalizuje wyniki ZANIM zwróci je do kolejki."""
        from deepface import DeepFace
        
        all_results = DeepFace.represent(
            img_path=img_path,
            model_name=self.model_name,
            enforce_detection=enforce_detection,
            detector_backend=self.detector_backend
        )
        
        filtered_and_normalized_results = []
        for res in all_results:
            if res.get("face_confidence", 0) > self.confidence_threshold:
                # --- NOWA LOGIKA: Normalizacja współrzędnych ---
                if 'facial_area' in res:
                    area = res['facial_area']
                    res['facial_area'] = {
                        'x': int(area.get('x', 0)),
                        'y': int(area.get('y', 0)),
                        'w': int(area.get('w', 0)),
                        'h': int(area.get('h', 0))
                    }
                
                res['model'] = self.model_name
                filtered_and_normalized_results.append(res)
        
        return filtered_and_normalized_results

    def _represent_dlib_safe(self, img_path: str, enforce_detection: bool) -> List[Dict[str, Any]]:
        """Wywołuje Dlib, ale najpierw "pierze" obraz za pomocą OpenCV."""
        import cv2
        import dlib

        if not all([self.dlib_detector, self.shape_predictor, self.face_recognizer]):
             raise RuntimeError("Modele Dlib nie są dostępne.")

        img_bgr = cv2.imread(img_path)
        if img_bgr is None:
            raise ValueError("OpenCV nie mogło wczytać pliku.")
        
        success, buffer = cv2.imencode('.jpg', img_bgr)
        if not success: raise ValueError("imencode failed")
        img_bgr_clean = cv2.imdecode(buffer, cv2.IMREAD_COLOR)
        if img_bgr_clean is None: raise ValueError("imdecode failed")
        
        img_array_rgb = cv2.cvtColor(img_bgr_clean, cv2.COLOR_BGR2RGB)
        faces = self.dlib_detector(img_array_rgb, self.dlib_upsampling)
        
        if enforce_detection and not faces:
            raise ValueError("Dlib: Nie wykryto żadnej twarzy na zdjęciu.")
            
        results = []
        for face in faces:
            shape = self.shape_predictor(img_array_rgb, face)
            embedding = np.array(self.face_recognizer.compute_face_descriptor(img_array_rgb, shape))
            results.append({
                'embedding': embedding.tolist(),
                'facial_area': {
                    'x': int(face.left()), 
                    'y': int(face.top()), 
                    'w': int(face.width()), 
                    'h': int(face.height())
                },
                'face_confidence': 1.0, 
                'model': 'Dlib'
            })
        return results

# --- ZMIANA: Dodajemy `confidence_threshold` do argumentów workera ---
def _ai_worker(task_queue: mp.Queue, result_queue: mp.Queue, log_queue: mp.Queue, model_name: str, deepface_home: str, confidence_threshold: float):
    """Główna funkcja procesu potomnego."""
    # Konfiguracja logowania (bez zmian)
    root_logger = logging.getLogger(); root_logger.handlers.clear(); root_logger.setLevel(logging.DEBUG)
    queue_handler = logging.handlers.QueueHandler(log_queue); root_logger.addHandler(queue_handler)
    
    if sys.platform != "win32":
        try: sys.stdin.close(); sys.stdin = open(os.devnull)
        except (IOError, OSError): pass
    
    try:
        # --- ZMIANA: Przekazujemy próg do silnika AI ---
        ai_engine = FaceRecognitionAI(model_name, deepface_home, confidence_threshold)
    except Exception as e:
        root_logger.critical("Krytyczny błąd podczas inicjalizacji silnika AI!", exc_info=True); result_queue.put(("WORKER_ERROR", f"Błąd inicjalizacji AI: {e}")); return
    
    while True:
        task = task_queue.get()
        if task == "STOP": break
        media_id, img_path_str = task
        success, result = ai_engine.represent(img_path_str=img_path_str, enforce_detection=False)
        if success:
            result_queue.put(("SUCCESS", media_id, result))
        else:
            result_queue.put(("FILE_ERROR", media_id, result))
    
    logger.info(f"[{model_name} Worker] Zakończono pracę.")

def _represent_worker(img_path_str: str, model_name: str, deepface_home: str, project_root: str) -> tuple[bool, List[Dict[str, Any]] | str]:
    """
    Uruchamia `DeepFace.represent` w izolowanym procesie, aby bezpiecznie
    wygenerować embeddingi dla wszystkich twarzy na danym zdjęciu.
    """
    # Przekierowujemy stdout/stderr, aby uciszyć logi DeepFace
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    if sys.platform != "win32":
        try:
            sys.stdin.close()
            sys.stdin = open(os.devnull)
        except (IOError, OSError):
            pass

    # Konfiguracja środowiska dla procesu potomnego
    sys.path.insert(0, project_root)
    os.environ['DEEPFACE_HOME'] = deepface_home

    from deepface import DeepFace
    
    try:
        # Używamy enforce_detection=False, aby znaleźć wszystkie twarze
        embedding_objs = DeepFace.represent(
            img_path=img_path_str,
            model_name=model_name,
            enforce_detection=False
        )
        return True, embedding_objs
    except Exception as e:
        # W razie błędu zwracamy informację o nim
        return False, f"Błąd w podprocesie represent: {e}"
