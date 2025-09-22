# Plik: core/face_rec/hailo_attribute_worker.py (NOWY PLIK)
#
# Worker łączący super-szybką detekcję na Hailo-8L z analizą atrybutów
# i generowaniem embeddingów na CPU.

import logging
import logging.handlers
import multiprocessing as mp
import os
import sys
from pathlib import Path
from typing import List, Dict, Any
import numpy as np
import cv2

# Import bibliotek Hailo (TAPPAS)
try:
    from hailo_platform import (HEF, ConfigureParams, HailoStreamInterface, InferVStreams, InputVStream, OutputVStream)
    HAILO_AVAILABLE = True
except ImportError:
    HAILO_AVAILABLE = False

# Globalne zmienne, aby uniknąć wielokrotnej inicjalizacji
_deepface_models = {}
_tappas_initialized = False
_hailo_objects = {}

def _initialize_tappas(hef_path: str, preproc_lib_path: str):
    """Inicjalizuje środowisko TAPPAS i zwraca obiekty potrzebne do inferencji."""
    global _tappas_initialized, _hailo_objects
    if _tappas_initialized:
        return _hailo_objects

    if not HAILO_AVAILABLE:
        raise ImportError("Biblioteki Hailo TAPPAS nie są dostępne.")

    # Dynammiczny import biblioteki pre-processingu
    import importlib.util
    spec = importlib.util.spec_from_file_location("hailo_preproc", preproc_lib_path)
    preproc_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(preproc_module)

    hef = HEF(hef_path)
    configure_params = ConfigureParams.create_from_hef(hef, interface=HailoStreamInterface.PCIe)
    network_group = hef.get_network_group(hef.get_network_group_names()[0])
    network_group.configure(configure_params)
    
    input_vstreams_params = InputVStream.create_params(network_group, quantize=True, format_type='auto')
    output_vstreams_params = OutputVStream.create_params(network_group, quantize=True, format_type='auto')
    
    _hailo_objects = {
        'network_group': network_group,
        'input_vstreams_params': input_vstreams_params,
        'output_vstreams_params': output_vstreams_params,
        'preprocess_function': preproc_module.preprocess,
        'postprocess_function': preproc_module.postprocess
    }
    _tappas_initialized = True
    return _hailo_objects

def _initialize_deepface(embedding_model: str, deepface_home: str, project_root: str):
    """Inicjalizuje modele DeepFace dla atrybutów i embeddingów."""
    global _deepface_models
    if _deepface_models:
        return

    # Ustawienie środowiska
    sys.path.insert(0, project_root)
    os.environ['DEEPFACE_HOME'] = deepface_home
    os.environ['CUDA_VISIBLE_DEVICES'] = "-1" # Zawsze używamy CPU dla DeepFace w tym workerze

    from deepface import DeepFace
    
    # Pre-ładowanie modeli, aby uniknąć opóźnień przy pierwszym użyciu
    _deepface_models['age'] = DeepFace.build_model('Age')
    _deepface_models['gender'] = DeepFace.build_model('Gender')
    _deepface_models['emotion'] = DeepFace.build_model('Emotion')
    _deepface_models['race'] = DeepFace.build_model('Race')
    _deepface_models['embedding'] = DeepFace.build_model(embedding_model)
    _deepface_models['DeepFace'] = DeepFace # Przechowujemy referencję do modułu
    
    logging.info("Wszystkie modele DeepFace (atrybuty + embedding) załadowane do pamięci.")

def hailo_attribute_worker(
    task_queue: mp.Queue, 
    result_queue: mp.Queue, 
    log_queue: mp.Queue,
    hef_path: str,
    preproc_lib_path: str,
    confidence_threshold: float,
    embedding_model: str,
    deepface_home: str,
    project_root: str
):
    """Główna funkcja workera."""
    # Konfiguracja logowania
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG)
    queue_handler = logging.handlers.QueueHandler(log_queue)
    root_logger.addHandler(queue_handler)

    if sys.platform != "win32":
        try:
            sys.stdin.close()
            sys.stdin = open(os.devnull)
        except (IOError, OSError):
            pass

    try:
        # Inicjalizacja obu systemów AI
        hailo = _initialize_tappas(hef_path, preproc_lib_path)
        _initialize_deepface(embedding_model, deepface_home, project_root)
        DeepFace = _deepface_models['DeepFace']
    except Exception as e:
        logging.critical(f"Krytyczny błąd inicjalizacji workera Hailo: {e}", exc_info=True)
        result_queue.put(("WORKER_ERROR", -1, f"Błąd inicjalizacji AI: {e}"))
        return

    while True:
        task = task_queue.get()
        if task is None:
            break
        
        media_id, img_path_str = task
        
        try:
            image = cv2.imread(img_path_str)
            if image is None:
                raise ValueError("Nie można wczytać obrazu.")

            # Krok 1: Detekcja na Hailo
            input_tensor = hailo['preprocess_function'](image)
            with InferVStreams(hailo['network_group'], hailo['input_vstreams_params'], hailo['output_vstreams_params']) as infer_pipeline:
                with hailo['network_group'].activate():
                    infer_results = infer_pipeline.infer(input_tensor)
            
            detections = hailo['postprocess_function'](infer_results, image.shape, confidence_threshold)
            
            # Krok 2: Analiza atrybutów i embedding na CPU dla każdej wykrytej twarzy
            processed_faces = []
            for detection in detections:
                x, y, w, h = detection['x'], detection['y'], detection['w'], detection['h']
                
                # Wycinamy twarz (chip) z oryginalnego obrazu
                face_chip = image[y:y+h, x:x+w]
                if face_chip.size == 0:
                    continue

                # Analiza atrybutów na wycinku
                attributes = DeepFace.analyze(
                    img_path=face_chip, 
                    actions=('age', 'gender', 'emotion', 'race'), 
                    enforce_detection=False, 
                    detector_backend='skip'
                )
                
                # Generowanie embeddingu na wycinku
                embedding = DeepFace.represent(
                    img_path=face_chip, 
                    model_name=embedding_model, 
                    enforce_detection=False, 
                    detector_backend='skip'
                )

                if attributes and isinstance(attributes, list) and embedding and isinstance(embedding, list):
                    # Łączymy wyniki w jeden spójny słownik
                    final_face_data = {
                        "region": {'x': x, 'y': y, 'w': w, 'h': h},
                        "age": attributes[0].get('age'),
                        "dominant_gender": attributes[0].get('dominant_gender'),
                        "dominant_emotion": attributes[0].get('dominant_emotion'),
                        "dominant_race": attributes[0].get('dominant_race'),
                        "embedding": embedding[0].get('embedding')
                    }
                    processed_faces.append(final_face_data)

            result_queue.put(("SUCCESS", media_id, processed_faces))

        except Exception as e:
            logging.error(f"Błąd przetwarzania pliku {Path(img_path_str).name}: {e}", exc_info=True)
            result_queue.put(("FILE_ERROR", media_id, str(e)))

    logging.info("Worker atrybutów Hailo zakończył pracę.")
