# Plik: core/face_rec/hailo_worker.py (NAPRAWIONA WERSJA)

import logging
import os
import sys
from pathlib import Path
import numpy as np
from typing import List, Dict, Any
import multiprocessing as mp
import importlib.util

try:
    import hailo_sdk_client as hailo
    from hailo_platform import VDevice, ConfigureParams, InputVStream, OutputVStream, HailoStreamInterface
    from hailo_sdk_common.targets.inference_targets import SdkEntity
    import cv2
    HAILO_READY = True
except ImportError:
    HAILO_READY = False

logger = logging.getLogger(__name__)

# ##############################################################################
# ===                     SEKCJA 1: KLASA-FASADA HAILO AI                    ===
# ##############################################################################

class HailoRecognitionAI:
    """
    Klasa-fasada do hybrydowej detekcji i rozpoznawania twarzy.
    Detekcja na Hailo-8, rozpoznawanie (embedding) na CPU.
    """
    # Dodajemy zmienną klasową do sprawdzania dostępności
    HAILO_READY = HAILO_READY
    
    def __init__(self, hef_path_str: str, preproc_lib_path_str: str, confidence_threshold: float, recognition_model: str, deepface_home: str):
        if not HAILO_READY:
            raise RuntimeError("Biblioteki HailoRT lub OpenCV nie są zainstalowane w środowisku tego procesu roboczego.")
        
        # --- NOWA LOGIKA: Dynamiczne ładowanie biblioteki pre-processingu ---
        self.preproc_lib_path = Path(preproc_lib_path_str)
        if not self.preproc_lib_path.is_file():
             raise FileNotFoundError(f"Plik biblioteki pre-processingu nie znaleziony: {self.preproc_lib_path}")
        
        spec = importlib.util.spec_from_file_location("hailo_preproc_lib", self.preproc_lib_path)
        preproc_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(preproc_module)
        
        self.preprocess = preproc_module.preprocess
        self.postprocess = preproc_module.postprocess
        self.confidence_threshold = confidence_threshold
        # --- KONIEC NOWEJ LOGIKI ---
        
        self.hef_path = Path(hef_path_str)
        if not self.hef_path.is_file():
            raise FileNotFoundError(f"Plik modelu Hailo (.hef) nie znaleziony: {self.hef_path}")
            
        os.environ['DEEPFACE_HOME'] = deepface_home
        
        self.target = VDevice()
        self.hef = SdkEntity.resolve(self.hef_path)
        self.configure_params = ConfigureParams.create_from_hef(self.hef, interface=HailoStreamInterface.PCIe)
        self.network_group = self.target.configure(self.hef, self.configure_params)[0]
        self.network_group_params = self.network_group.create_params()

        self.recognition_model_name = recognition_model
        # Importujemy i budujemy model DeepFace dopiero tutaj, wewnątrz procesu
        from deepface import DeepFace
        self.recognition_model_obj = DeepFace.build_model(self.recognition_model_name)
        
        logger.info(f"Silnik HailoAI pomyślnie zainicjalizowany.")

    def represent(self, img_path_str: str) -> List[Dict[str, Any]]:
        """Główna metoda klasy, wykonująca detekcję i rozpoznawanie."""
        original_image = cv2.imread(img_path_str)
        if original_image is None:
            raise IOError(f"Nie można wczytać obrazu: {img_path_str}")

        # Krok 1: Pre-processing obrazu
        preprocessed_image = self.preprocess(original_image)
        
        # Krok 2: Detekcja na Hailo
        with self.network_group.activate(self.network_group_params):
            with InputVStream(self.network_group, self.hef.get_input_vstream_infos()[0], hailo.HAILO_FORMAT_TYPE_FLOAT32) as input_vstream, \
                 OutputVStream(self.network_group, self.hef.get_output_vstream_infos()[0], hailo.HAILO_FORMAT_TYPE_FLOAT32) as output_vstream:
                input_vstream.write(preprocessed_image)
                raw_results = [output_vstream.read()]

        # Krok 3: Post-processing wyników
        detected_faces = self.postprocess(raw_results, original_image.shape, self.confidence_threshold)
        
        final_results = []
        for face_area in detected_faces:
            # Krok 4: Wycinamy twarz z oryginalnego obrazu
            x, y, w, h = face_area['x'], face_area['y'], face_area['w'], face_area['h']
            face_chip = original_image[y:y+h, x:x+w]
            
            # Krok 5: Generowanie embeddingu na CPU
            if face_chip.size > 0:
                from deepface.commons import functions as deepface_functions
                embedding_obj_list = deepface_functions.represent(
                    img_path=face_chip, 
                    model_name=self.recognition_model_name,
                    detector_backend='skip', # WAŻNE: Omijamy ponowną detekcję
                    model=self.recognition_model_obj
                )
                
                if embedding_obj_list:
                    final_results.append({
                        'embedding': embedding_obj_list[0]["embedding"],
                        'facial_area': face_area,
                        'face_confidence': face_area.get('confidence', 1.0),
                        'model': f"Hailo8+{self.recognition_model_name}"
                    })
        return final_results

# ##############################################################################
# ===                     SEKCJA 2: FUNKCJA WORKERA                          ===
# ##############################################################################

def hailo_face_worker(
    task_queue: mp.Queue,
    result_queue: mp.Queue,
    log_queue: logging.handlers.QueueHandler,
    hef_path: str,
    preproc_lib_path: str,
    confidence_threshold: float,
    recognition_model: str,
    deepface_home: str,
    project_root: str
):
    """Proces roboczy, który używa silnika HailoAI."""
    root_logger = logging.getLogger(); root_logger.handlers.clear(); root_logger.setLevel(logging.DEBUG)
    queue_handler = logging.handlers.QueueHandler(log_queue); root_logger.addHandler(queue_handler)

    if sys.platform != "win32":
        try: sys.stdin.close(); sys.stdin = open(os.devnull)
        except (IOError, OSError): pass
    
    try:
        # Importujemy DeepFace i inne ciężkie biblioteki dopiero wewnątrz workera
        from deepface import DeepFace
        ai_engine = HailoRecognitionAI(hef_path, preproc_lib_path, confidence_threshold, recognition_model, deepface_home)
    except Exception as e:
        logger.critical("Krytyczny błąd inicjalizacji HailoAI!", exc_info=True)
        result_queue.put(("WORKER_ERROR", f"Błąd inicjalizacji HailoAI: {e}"))
        return
        
    while True:
        task = task_queue.get()
        if task is None: # Sygnał do zakończenia
            break
        media_id, img_path_str = task
        try:
            results = ai_engine.represent(img_path_str=img_path_str)
            result_queue.put(("SUCCESS", media_id, results))
        except Exception as e:
            logger.error(f"Błąd przetwarzania pliku {Path(img_path_str).name} przez HailoAI", exc_info=True)
            result_queue.put(("FILE_ERROR", media_id, str(e)))
            
    logger.info("[Hailo Worker] Zakończono pracę.")
