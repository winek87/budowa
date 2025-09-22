# Plik: core/face_rec/attributes_single_worker.py (NOWY PLIK)

import sys
from pathlib import Path
import os
from typing import Dict, List, Any

def _single_attribute_worker(
    img_path_str: str,
    detector_backend: str,
    action: str, # 'age', 'gender', 'emotion', or 'race'
    deepface_home: str,
    project_root: str
) -> List[Dict[str, Any]]:
    """
    Worker, który analizuje tylko JEDEN atrybut na raz.
    Używany w trybie "krok po kroku" dla maksymalnej stabilności.
    """
    sys.stdout = open(os.devnull, 'w'); sys.stderr = open(os.devnull, 'w')
    if sys.platform != "win32":
        try: sys.stdin.close(); sys.stdin = open(os.devnull)
        except (IOError, OSError): pass
    
    sys.path.insert(0, project_root); os.environ['DEEPFACE_HOME'] = deepface_home
    from deepface import DeepFace; import cv2

    try:
        img_array = cv2.imread(img_path_str)
        if img_array is None: return []

        results = DeepFace.analyze(
            img_path=img_array,
            actions=(action,),
            enforce_detection=False,
            detector_backend=detector_backend
        )
        return results if isinstance(results, list) else []
    except Exception:
        log_path = Path(project_root) / "app_data/logs/single_attribute_worker_error.log"
        log_path.parent.mkdir(exist_ok=True, parents=True)
        import traceback
        o_stdout, o_stderr = sys.stdout, sys.stderr; sys.stdout, sys.stderr = sys.__stdout__, sys.__stderr__
        with open(log_path, "a", encoding='utf-8') as f:
            f.write(f"--- Błąd w podprocesie dla obrazu {img_path_str}, akcja: {action} ---\n"); traceback.print_exc(file=f); f.write("---\n")
        sys.stdout, sys.stderr = o_stdout, o_stderr
        return []
