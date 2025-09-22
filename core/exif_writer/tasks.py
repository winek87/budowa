# plik: core/exif_writer/tasks.py
# Wersja 1.5 - Naprawiono błąd 'Passing coroutines is forbidden' przez jawne tworzenie zadań.
# Opis: Ten moduł zawiera czystą logikę biznesową dla Zapisywarki EXIF.
#       Odpowiada za budowanie argumentów dla ExifTool, uruchamianie go
#       jako procesu systemowego oraz aktualizowanie statusu w bazie danych.
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Importujemy uniwersalną funkcję do aktualizacji bazy danych
from ..database import update_exif_write_status
# Importujemy lokalny event do obsługi przerwania
from .utils import stop_event

# --- Inicjalizacja ---
logger = logging.getLogger(__name__)

# --- Konfiguracja ---
EXIFTOOL_PATH = "exiftool"
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".3gp"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".gif", ".webp"}


# ##############################################################################
# ===                     SEKCJA 1: LOGOWANIE BŁĘDÓW                         ===
# ##############################################################################

def log_exif_error(file_path: Path, reason: str, exiftool_output: str = ""):
    """
    Zapisuje szczegółowe informacje o błędzie zapisu EXIF do pliku
    w folderze `app_data/exif_writer/`.

    Args:
        file_path (Path): Ścieżka do pliku, którego dotyczy błąd.
        reason (str): Krótki opis przyczyny błędu (np. błąd JSON, błąd ExifTool).
        exiftool_output (str, optional): Pełne wyjście z programu ExifTool.
    """
    try:
        log_dir = Path("app_data/exif_writer")
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "error.log"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"--- {timestamp} ---\n")
            f.write(f"Plik: {file_path}\n")
            f.write(f"Przyczyna: {reason}\n")
            if exiftool_output:
                f.write("--- Wyjście z ExifTool ---\n")
                f.write(exiftool_output.strip())
                f.write("\n---------------------------\n")
            f.write("\n")
    except Exception as e:
        logger.error(f"Nie udało się zapisać logu błędu EXIF: {e}")


# ##############################################################################
# ===                 SEKCJA 2: INTERAKCJA Z EXIFTOOL                        ===
# ##############################################################################

def build_exiftool_args(details: dict, file_path: Path) -> List[str]:
    """
    Na podstawie słownika z metadanymi buduje listę argumentów dla ExifTool.
    """
    args = []
    file_extension = file_path.suffix.lower()
    if dt_iso := details.get("DateTime"):
        try:
            dt_str = dt_iso.replace("-", ":", 2).replace("T", " ")
            if file_extension in IMAGE_EXTENSIONS: args.extend([f"-DateTimeOriginal={dt_str}", f"-CreateDate={dt_str}", f"-ModifyDate={dt_str}"])
            elif file_extension in VIDEO_EXTENSIONS: args.extend([f"-CreateDate={dt_str}", f"-ModifyDate={dt_str}"])
        except Exception: logger.warning(f"Nie udało się sparsować daty '{dt_iso}' dla {file_path.name}")
    if camera := details.get("Camera"):
        parts = camera.split(" ", 1)
        if len(parts) > 1: args.extend([f"-Make={parts[0]}", f"-Model={parts[1]}"])
        else: args.append(f"-Model={camera}")
    keywords = set()
    for key in ["TaggedPeople", "Albums"]:
        if value := details.get(key, []): keywords.update(str(item).strip() for item in value)
    for keyword in sorted(list(keywords)): args.extend([f"-Keywords+={keyword}", f"-Subject+={keyword}"])
    if gps_data := details.get("Experimental_Details", {}).get("GPS_Coords"):
        if 'latitude' in gps_data and 'longitude' in gps_data:
            lat, lon = gps_data['latitude'], gps_data['longitude']
            args.extend([f"-GPSLatitude={abs(lat)}", f"-GPSLongitude={abs(lon)}", f"-GPSLatitudeRef={'N' if lat >= 0 else 'S'}", f"-GPSLongitudeRef={'E' if lon >= 0 else 'W'}"])
    if description := details.get("Description"): args.extend([f"-ImageDescription={description}", f"-Description={description}", f"-Caption-Abstract={description}"])
    logger.debug(f"Zbudowano {len(args)} argumentów ExifTool dla {file_path.name}.")
    return args


async def run_exiftool(args: List[str], file_path: Path) -> Tuple[bool, str]:
    """
    Asynchronicznie uruchamia ExifTool, z możliwością natychmiastowego przerwania.

    Args:
        args (List[str]): Lista argumentów do przekazania do ExifTool.
        file_path (Path): Ścieżka do pliku do zmodyfikowania.

    Returns:
        Tuple[bool, str]: Krotka (sukces, wyjście_programu).
    
    Raises:
        asyncio.CancelledError: Jeśli operacja zostanie przerwana przez użytkownika.
    """
    command = [EXIFTOOL_PATH, "-m", "-overwrite_original", "-api", "quicktimeutc", "-charset", "utf8", *args, str(file_path)]
    
    proc = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # Opakowujemy korutyny w zadania, aby były kompatybilne z asyncio.wait
    proc_wait_task = asyncio.create_task(proc.wait())
    stop_event_task = asyncio.create_task(stop_event.wait())

    done, pending = await asyncio.wait(
        [proc_wait_task, stop_event_task],
        return_when=asyncio.FIRST_COMPLETED
    )
    
    # Zawsze anulujemy oczekujące zadanie, aby uniknąć wycieków zasobów
    for task in pending:
        task.cancel()

    if stop_event.is_set():
        try:
            proc.terminate()
            await proc.wait()
        except ProcessLookupError:
            pass # Proces mógł się już zakończyć
        raise asyncio.CancelledError("Operacja przerwana przez użytkownika (sygnał stop_event).")
    
    stdout, stderr = await proc.communicate()
    
    output = stdout.decode('utf-8', errors='ignore').lower()
    error_output = stderr.decode('utf-8', errors='ignore')
    
    success = proc.returncode == 0 and ("1 image files updated" in output or "1 video files updated" in output)
    
    return success, error_output or output


# ##############################################################################
# ===                     SEKCJA 3: GŁÓWNA LOGIKA PRZETWARZANIA                ===
# ##############################################################################

async def process_single_file(record: Dict) -> Dict:
    """
    Przetwarza jeden rekord z bazy: buduje argumenty, uruchamia ExifTool,
    aktualizuje status i zwraca wynik. Obsługuje przerwanie operacji.

    Args:
        record (Dict): Rekord z bazy danych.

    Returns:
        Dict: Słownik z wynikiem operacji.
    
    Raises:
        asyncio.CancelledError: Propaguje wyjątek przerwania w górę.
    """
    file_path_str = record.get("final_path")
    if not file_path_str:
        return {"status": "pominięty", "details": {"info": "Brak ścieżki w rekordzie."}}
        
    file_path = Path(file_path_str)
    
    try:
        if not await asyncio.to_thread(file_path.exists):
            return {"status": "pominięty", "details": {"info": "Plik nie istnieje na dysku."}}

        details = json.loads(record["metadata_json"])
        args = build_exiftool_args(details, file_path)

        if not args:
            await update_exif_write_status(file_path_str, "Skipped")
            return {"status": "pominięty", "details": {"info": "Brak tagów do zapisu."}}

        success, output = await run_exiftool(args, file_path)

        if success:
            await update_exif_write_status(file_path_str, "Success")
            return {"status": "sukces", "details": args}
        else:
            await update_exif_write_status(file_path_str, "Error")
            log_exif_error(file_path, "Polecenie ExifTool zakończyło się błędem.", output)
            return {"status": "błąd", "details": {"error": output.strip()[:200]}}

    except asyncio.CancelledError:
        raise
    except json.JSONDecodeError as e:
        await update_exif_write_status(file_path_str, "Error")
        log_exif_error(file_path, f"Błąd dekodowania JSON: {e}")
        return {"status": "błąd", "details": {"error": f"Błąd JSON: {e}"}}
    except Exception as e:
        logger.error(f"Krytyczny błąd podczas przetwarzania {file_path.name}", exc_info=True)
        await update_exif_write_status(file_path_str, "Error")
        log_exif_error(file_path, f"Wystąpił nieoczekiwany błąd w skrypcie: {e}")
        return {"status": "błąd", "details": {"error": f"Błąd krytyczny: {e}"}}
