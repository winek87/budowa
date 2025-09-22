# plik: core/image_fixer/tasks.py
# Wersja 1.6 - Dodano nowy, specjalistyczny silnik naprawczy ExifTool.
# Opis: Ten moduł zawiera czystą logikę biznesową dla Naprawiacza Obrazów.
#       Funkcje te są zaprojektowane do uruchamiania w osobnych procesach
#       w celu diagnozy i naprawy plików graficznych.
# -*- coding: utf-8 -*-

import asyncio
import io
import logging
import shutil
import subprocess
from contextlib import redirect_stderr
from pathlib import Path
from typing import Optional, Tuple, Dict, List

# Leniwe ładowanie ciężkich bibliotek
Image, UnidentifiedImageError, cv2, exiftool, np = None, None, None, None, None

logger = logging.getLogger(__name__)

def _initialize_heavy_deps():
    """Dynamicznie ładuje ciężkie biblioteki w procesie potomnym."""
    global Image, UnidentifiedImageError, cv2, exiftool, np
    if Image is None:
        from PIL import Image, UnidentifiedImageError
        Image.MAX_IMAGE_PIXELS = None
    if cv2 is None:
        import cv2
    if exiftool is None:
        import exiftool
    if np is None:
        import numpy as np

# ##############################################################################
# ===                     SEKCJA 1: FUNKCJE DIAGNOSTYCZNE                      ===
# ##############################################################################

def _test_with_exiftool(path: Path, et_helper: 'exiftool.ExifToolHelper') -> Optional[str]:
    """
    Sprawdza plik za pomocą Exiftool, ignorując ostrzeżenia i zwracając pierwszy błąd.
    """
    try:
        output = et_helper.execute("-validate", "-warning", "-error", "-a", "-m", str(path))
        if not output: return None
        lines = output.strip().splitlines()
        if any("Validate                      : OK" in line for line in lines):
            return None
        
        error_lines = [line for line in lines if "error" in line.lower()]
        if not error_lines:
            return None
            
        first_error = error_lines[0]
        return first_error.split(":", 1)[1].strip() if ":" in first_error else first_error.strip()
    except Exception as e:
        return f"Krytyczny błąd wykonania ({e})"

def _test_with_pillow_verify(path: Path) -> Optional[str]:
    """Wykonuje szybką weryfikację nagłówków i struktury za pomocą Pillow."""
    with io.StringIO() as buf, redirect_stderr(buf):
        try:
            with Image.open(path) as img:
                img.verify()
            err = buf.getvalue().strip()
            return f"Pillow (stderr): {err}" if err else None
        except Exception as e:
            return f"Pillow Verify: {e}"

def _test_with_opencv_load(path: Path) -> Optional[str]:
    """Próbuje w pełni załadować dane obrazu za pomocą OpenCV."""
    with io.StringIO() as buf, redirect_stderr(buf):
        try:
            if cv2.imread(str(path)) is None:
                return "OpenCV: Nie można załadować danych obrazu"
            err = buf.getvalue().strip()
            return f"OpenCV (stderr): {err}" if err else None
        except Exception as e:
            return f"OpenCV: {e}"

def _test_with_pillow_load(path: Path) -> Optional[str]:
    """Próbuje w pełni zdekodować i załadować dane obrazu za pomocą Pillow."""
    with io.StringIO() as buf, redirect_stderr(buf):
        try:
            with Image.open(path) as img:
                img.load()
            err = buf.getvalue().strip()
            return f"Pillow (stderr): {err}" if err else None
        except Exception as e:
            return f"Pillow Load: {e}"

TEST_MAP = {
    "Exiftool": _test_with_exiftool, "Pillow Verify": _test_with_pillow_verify,
    "OpenCV Load": _test_with_opencv_load, "Pillow Load": _test_with_pillow_load,
}

async def run_diagnostics(image_entries: List[Dict], selected_tests: List[str]) -> List[Dict]:
    """Skanuje obrazy, raportując postęp do obiektu display."""
    _initialize_heavy_deps()
    from .ui import ImageDiagnosticsLiveDisplay
    
    problematic_files_dict: Dict[Path, List[Tuple[str, str]]] = {}
    
    with ImageDiagnosticsLiveDisplay(total_items=len(image_entries), test_names=selected_tests, console=logging.getLogger().handlers[0].console) as display:
        with exiftool.ExifToolHelper() as et:
            for test_name in selected_tests:
                display.switch_test(test_name, len(image_entries))
                test_func = TEST_MAP[test_name]
                for entry in image_entries:
                    file_path = entry['path']
                    try:
                        error = await asyncio.to_thread(test_func, file_path, et) if test_name == "Exiftool" else await asyncio.to_thread(test_func, file_path)
                        display.update(test_name, file_path.name, error)
                        if error:
                            if file_path not in problematic_files_dict:
                                problematic_files_dict[file_path] = []
                            problematic_files_dict[file_path].append((test_name, error))
                    except Exception as e:
                        logger.error(f"Błąd w '{test_name}' dla {file_path}", exc_info=True)

    return [{"path": path, "reasons": reasons} for path, reasons in problematic_files_dict.items()]


# ##############################################################################
# ===                     SEKCJA 2: FUNKCJE NAPRAWCZE                        ===
# ##############################################################################

def _verify_fix(path: Path) -> bool:
    """Sprawdza, czy naprawiony plik jest poprawny, używając Pillow i OpenCV."""
    try:
        with redirect_stderr(io.StringIO()):
            with Image.open(path) as img:
                img.load()
            return cv2.imread(str(path)) is not None
    except Exception:
        return False

async def fix_with_exiftool(source_path: Path) -> Tuple[bool, str]:
    """
    Próbuje naprawić problemy z metadanymi, przepisując tagi za pomocą ExifTool.
    To jest najlepsze rozwiązanie dla błędów walidacji EXIF/XMP/IPTC.
    """
    _initialize_heavy_deps()
    try:
        # Komenda, która mówi exiftool, aby odczytał wszystkie tagi
        # i zapisał je ponownie, potencjalnie naprawiając błędy strukturalne.
        command = [
            "exiftool",
            "-all=",
            "-tagsfromfile", "@",
            "-all:all", "-unsafe",
            "-overwrite_original",
            str(source_path)
        ]
        
        proc = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
        
        if proc.returncode == 0:
            return True, "Metadane przepisane przez ExifTool."
        else:
            return False, f"Błąd ExifTool: {stderr.decode('utf-8', 'ignore').strip()}"
    except Exception as e:
        return False, f"Błąd wykonania ExifTool: {e}"

async def fix_with_pillow(source_path: Path) -> Tuple[bool, str]:
    """Próbuje naprawić obraz, przepisując go za pomocą biblioteki Pillow."""
    _initialize_heavy_deps()
    try:
        def pillow_process():
            temp_path = source_path.with_suffix(f"{source_path.suffix}.tmp")
            with Image.open(source_path) as img:
                params = {'exif': img.info.get('exif'), 'icc_profile': img.info.get('icc_profile')}
                img.load()
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                img.save(temp_path, format=img.format or 'JPEG', **params)
            shutil.move(str(temp_path), str(source_path))
        await asyncio.to_thread(pillow_process)
        return (True, "Plik przepisany przez Pillow.") if _verify_fix(source_path) else (False, "Weryfikacja po naprawie nie powiodła się.")
    except Exception as e:
        return False, f"Błąd Pillow: {e}"

async def fix_with_imagemagick(source_path: Path) -> Tuple[bool, str]:
    """Próbuje naprawić obraz za pomocą zewnętrznego narzędzia ImageMagick."""
    _initialize_heavy_deps()
    magick_path = shutil.which("magick") or shutil.which("convert")
    if not magick_path:
        return False, "Nie znaleziono programu 'magick' ani 'convert'."
    
    def magick_process():
        temp_path = source_path.with_suffix(f"{source_path.suffix}.tmp")
        cmd = [magick_path, str(source_path), "-auto-orient", str(temp_path)]
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            shutil.move(str(temp_path), str(source_path))
            return True, "Plik przepisany przez ImageMagick."
        else:
            if temp_path.exists():
                temp_path.unlink()
            return False, f"Błąd ImageMagick: {result.stderr.strip()}"
    
    success, message = await asyncio.to_thread(magick_process)
    return (success, message) if not success else ((True, message) if _verify_fix(source_path) else (False, "Weryfikacja po naprawie nie powiodła się."))
