# plik: core/video_fixer/tasks.py
# Wersja 1.1 - Usunięto zarządzanie `Live`, przekazując odpowiedzialność do cli.py.
# Opis: Ten moduł zawiera czystą logikę biznesową dla Naprawiacza Wideo.
#       Funkcje te są odpowiedzialne za uruchamianie `ffprobe` i `ffmpeg`
#       jako procesów systemowych w celu diagnozy i naprawy plików wideo.
# -*- coding: utf-8 -*-

import asyncio
import logging
import shutil
from pathlib import Path
from typing import List, Dict

from rich.console import Console
from rich.text import Text

# Importy z wewnętrznych modułów pakietu
from .ui import VideoFixerLiveDisplay
#VideoDiagnosticsLiveDisplay

# Inicjalizacja
logger = logging.getLogger(__name__)
console_tasks = Console() # Używamy osobnej konsoli, aby uniknąć konfliktów


async def _test_with_ffprobe(video_path: Path) -> str | None:
    """
    Używa ffprobe do szybkiego sprawdzenia, czy plik wideo jest uszkodzony.

    Uruchamia `ffprobe` z flagą `-v error`, która powoduje, że program
    zwróci kod błędu i wypisze komunikat na stderr tylko wtedy, gdy
    napotka poważny problem ze strukturą pliku.

    Args:
        video_path (Path): Ścieżka do pliku wideo do przetestowania.

    Returns:
        str | None: Komunikat błędu, jeśli plik jest uszkodzony, lub None,
                    jeśli plik wydaje się być poprawny.
    """
    try:
        command = ["ffprobe", "-v", "error", "-i", str(video_path)]
        proc = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            error = stderr.decode('utf-8', 'ignore').strip()
            # Zwracamy tylko pierwszą, najbardziej znaczącą linię błędu
            return error.splitlines()[0] if error else "Nieznany błąd ffprobe."
        return None
    except Exception as e:
        return f"Błąd wykonania ffprobe: {e}"


async def run_diagnostics(video_paths: List[Path], display: "VideoFixerLiveDisplay") -> List[Dict]:
    """Skanuje pliki, raportując postęp do obiektu display."""
    problematic_files = []
    display.progress.update(display._task_id, description="Diagnozuję...", total=len(video_paths))
    display.action_logs.appendleft(Text.from_markup("[green]Rozpoczynam diagnostykę FFprobe...[/]"))
    display.update()
    
    for path in video_paths:
        display.action_logs.appendleft(Text(f"Sprawdzam: {path.name}", style="cyan"))
        error = await _test_with_ffprobe(path)
        display.stats['diagnosed'] += 1
        if error:
            display.stats['problems'] += 1
            display.error_logs.appendleft(Text(f"{path.name}: {error}", style="red"))
            problematic_files.append({"path": path, "reason": error})
        display.progress.update(display._task_id, advance=1)
        display.update()
        await asyncio.sleep(0.01)
    return problematic_files

async def fix_single_video(source_path: Path, backup_dir: Path, display: "VideoFixerLiveDisplay"):
    """Wykonuje operację naprawy dla jednego pliku, raportując postęp."""
    backup_path = backup_dir / source_path.name
    await asyncio.to_thread(shutil.copy2, source_path, backup_path)
    display.action_logs.appendleft(Text(f"Tworzenie kopii: {backup_path.name}", style="dim"))
    
    temp_path = source_path.with_suffix(f"{source_path.suffix}.repaired")
    command = ["ffmpeg", "-i", str(source_path), "-c", "copy", "-map", "0", str(temp_path)]
    proc = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, stderr = await proc.communicate()
    
    if proc.returncode != 0:
        await asyncio.to_thread(shutil.move, str(backup_path), str(source_path))
        raise RuntimeError(stderr.decode('utf-8', 'ignore'))
        
    if await _test_with_ffprobe(temp_path) is None:
        await asyncio.to_thread(shutil.move, str(temp_path), str(source_path))
        display.stats['fixed'] += 1
        display.action_logs.appendleft(Text(f"Naprawiono: {source_path.name}", style="green"))
    else:
        await asyncio.to_thread(shutil.move, str(backup_path), str(source_path))
        raise RuntimeError("Plik wciąż jest uszkodzony po naprawie.")
