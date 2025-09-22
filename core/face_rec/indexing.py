# Plik: core/face_rec/indexing.py
# Wersja 3.3 - Ostateczna, Kompletna

import asyncio
import logging
import queue
import multiprocessing as mp
import sys
from pathlib import Path
from collections import deque
from datetime import datetime
import numpy as np


from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.live import Live
from rich.text import Text

from ..database import (
    setup_database, add_face,
    update_indexing_status_batch, get_images_for_face_indexing_optimized, delete_faces_by_media_ids
)
from .dashboard import generate_indexing_dashboard
from .models import _get_deepface_base_path
from .settings import get_current_settings

console = Console(record=True)
logger = logging.getLogger(__name__)


def _save_summary_to_file(model_name: str, state: dict, error_message: str | None, detector: str):
    """Zapisuje podsumowanie sesji indeksowania do pliku logu."""
    logs_dir = Path("app_data/logs"); logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = logs_dir / f"indexing_summary_{timestamp}.log"
    stats = state['stats']
    try:
        with open(log_file_path, 'w', encoding='utf-8') as f:
            f.write("="*50 + "\n PODSUMOWANIE SESJI INDEKSOWANIA TWARZY\n" + "="*50 + "\n\n")
            f.write(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Model rozpoznawania: {model_name}\n")
            f.write(f"Detektor twarzy: {detector}\n\nStatystyki:\n")
            f.write(f"  - Przetworzono plików: {stats['processed']} / {stats['total']}\n")
            f.write(f"  - Znaleziono twarzy: {stats['faces_found']}\n")
            f.write(f"  - Błędy plików: {stats['file_errors']}\n")
            f.write(f"  - Błędy workera: {stats['worker_errors']}\n\n")
            if error_message: f.write(f"--- BŁĄD KRYTYCZNY WORKERA ---\n{error_message}\n\n")
            if error_logs := state.get('error_logs', []):
                f.write("--- Logi Błędów Plików ---\n")
                for log_entry in reversed(error_logs): f.write(f"- {log_entry.plain}\n")
        logger.info(f"Zapisano podsumowanie do: {log_file_path}")
        console.print(f"\n[dim]Podsumowanie zapisano w:\n{log_file_path}[/dim]")
    except Exception as e:
        logger.error(f"Nie udało się zapisać pliku podsumowania: {e}", exc_info=True)

# ##############################################################################
# ===                     GŁÓWNA LOGIKA INDEKSOWANIA                         ===
# ##############################################################################

async def index_faces(model_name: str, heartbeat_event: asyncio.Event = None):
    """Uruchamia kompletny, równoległy proces indeksowania twarzy."""
    console.clear()
    console.print(Panel(f"🔬 Indeksowanie Twarzy (Model: [cyan]{model_name}[/cyan]) 🔬", style="bold blue"))
    await setup_database()
    
    force_rescan = Confirm.ask("\nWymusić ponowne skanowanie [bold red]WSZYSTKICH[/bold red] plików?", default=False)
    settings = get_current_settings()
    scan_limit = settings.get("SCAN_FILES_LIMIT")
    detector_backend = settings["DETECTOR_BACKEND"]
    num_workers = settings.get("NUM_AI_WORKERS", 1)
    
    if detector_backend == 'hailo':
        confidence_threshold = settings.get("HAILO_SETTINGS", {}).get("CONFIDENCE_THRESHOLD", 0.5)
    else:
        confidence_threshold = settings.get("DETECTION_CONFIDENCE_THRESHOLD", 0.95)

    console.print(f"Używany detektor twarzy: [bold cyan]{detector_backend}[/bold cyan] | Procesy robocze: [bold cyan]{num_workers}[/bold cyan] | Próg pewności: [bold cyan]{confidence_threshold:.0%}[/]")
    if scan_limit: console.print(f"Aktywny limit plików na sesję: [bold cyan]{scan_limit}[/bold cyan]")
    
    if detector_backend == 'hailo':
        hailo_settings = settings.get("HAILO_SETTINGS", {})
        hef_path = hailo_settings.get("HEF_PATH")
        preproc_path = hailo_settings.get("PREPROC_LIB_PATH")
        if not all([hef_path, preproc_path]) or not Path(hef_path).is_file() or not Path(preproc_path).is_file():
            console.print(Panel(
                f"[bold red]Błąd Konfiguracji Hailo-8[/]\n\n"
                f"Ścieżki do modelu .hef i/lub biblioteki pre-processingu są nieprawidłowe lub nieustawione.\n"
                f" - Ścieżka HEF: [cyan]{hef_path or 'Brak'}[/]\n"
                f" - Biblioteka: [cyan]{preproc_path or 'Brak'}[/]\n\n"
                f"Przejdź do [bold]Ustawień Zaawansowanych[/] i podaj poprawne ścieżki.",
                border_style="red"
            ))
            Prompt.ask("\n[bold]Naciśnij Enter...[/]"); return

    with console.status("[cyan]Wyszukiwanie plików do przetworzenia...[/]"):
        media_to_scan = await get_images_for_face_indexing_optimized(model_name, force_rescan, scan_limit)
        
    if not media_to_scan:
        console.print(f"[green]✅ Wszystkie pliki są już zaindeksowane.[/green]"); Prompt.ask("\nEnter..."); return
        
    if not Confirm.ask(f"\nZnaleziono [cyan]{len(media_to_scan)}[/cyan] zdjęć. Rozpocząć indeksowanie?"): return

    if force_rescan:
        with console.status("[bold red]Czyszczenie starych danych o twarzach...[/]"):
            media_ids_to_clear = [rec['id'] for rec in media_to_scan]
            await delete_faces_by_media_ids(media_ids_to_clear, model_name)

    spawn_ctx = mp.get_context("spawn")
    task_queue, result_queue, log_queue = spawn_ctx.Queue(), spawn_ctx.Queue(), spawn_ctx.Queue()
    
    if detector_backend == 'hailo':
        from .hailo_worker import hailo_face_worker
        hailo_settings = settings["HAILO_SETTINGS"]
        worker_target = hailo_face_worker
        worker_args = (
            task_queue, result_queue, log_queue,
            hailo_settings["HEF_PATH"],
            hailo_settings["PREPROC_LIB_PATH"],
            confidence_threshold,
            model_name,
            str(_get_deepface_base_path()),
            str(Path.cwd().resolve())
        )
        logger.info(f"Uruchamianie {num_workers} procesów roboczych HailoAI...")
    else:
        from .ai_worker import _ai_worker
        worker_target = _ai_worker
        worker_args = (task_queue, result_queue, log_queue, model_name, str(_get_deepface_base_path()), confidence_threshold)
        logger.info(f"Uruchamianie {num_workers} procesów roboczych AI (CPU/GPU)...")

    workers = [spawn_ctx.Process(target=worker_target, args=worker_args) for _ in range(num_workers)]
    for p in workers: p.start()

    dashboard_state = { 
        "stats": {"total": len(media_to_scan), "processed": 0, "faces_found": 0, "worker_errors": 0, "file_errors": 0},
        "action_logs": deque(maxlen=10),
        "info_logs": deque(maxlen=10),
        "error_logs": deque(maxlen=10),
        "current_activity": "[cyan]Inicjalizacja...[/cyan]",
        "current_path": None,
        "recent_finds": deque(maxlen=10)
    }
    progress_bar = Progress(TextColumn("[green]{task.description}"), BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%", TimeRemainingColumn())
    progress_task_id = progress_bar.add_task("Indeksuję...", total=len(media_to_scan))

    async def log_listener_task():
        while True:
            try:
                record = await asyncio.to_thread(log_queue.get_nowait)
                logger.handle(record)
                if record.levelno >= logging.INFO:
                    msg = record.getMessage()
                    dashboard_state['info_logs'].appendleft(Text.from_markup(f"[dim]{msg}[/dim]"))
            except queue.Empty:
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break

    listener = asyncio.create_task(log_listener_task())
    worker_error = None
    tasks_iterator = iter(media_to_scan)
    tasks_sent_count = 0
    status_updates_batch = []
    
    for _ in range(num_workers * 2):
        try:
            media_record = next(tasks_iterator)
            task_queue.put((media_record['id'], str(media_record['final_path'])))
            tasks_sent_count += 1
        except StopIteration:
            break

    with Live(generate_indexing_dashboard(progress_bar, dashboard_state, settings), console=console, screen=True, auto_refresh=False, vertical_overflow="crop") as live:
        while dashboard_state["stats"]["processed"] < dashboard_state["stats"]["total"]:
            if heartbeat_event: heartbeat_event.set()
            try:
                result_type, media_id, result_data = await asyncio.to_thread(result_queue.get, timeout=1.0)
                
                if tasks_sent_count < dashboard_state["stats"]["total"]:
                    try:
                        media_record = next(tasks_iterator)
                        task_queue.put((media_record['id'], str(media_record['final_path'])))
                        tasks_sent_count += 1
                    except StopIteration:
                        pass
                
                path_name = Path(next((m['final_path'] for m in media_to_scan if m['id'] == media_id), "")).name
                dashboard_state['current_path'] = Path(next((m['final_path'] for m in media_to_scan if m['id'] == media_id), ""))
                dashboard_state['current_activity'] = f"[dim]Przetworzono:[/dim] [yellow]{path_name}[/yellow]"
                
                if result_type == "SUCCESS":
                    faces_in_file = 0
                    for face_obj in result_data:
                        embedding_np = np.array(face_obj["embedding"], dtype=np.float32)
                        await add_face(media_id, embedding_np, face_obj["facial_area"], model_name)
                        dashboard_state["stats"]['faces_found'] += 1
                        faces_in_file += 1
                    
                    if faces_in_file > 0:
                        dashboard_state["recent_finds"].appendleft(dashboard_state['current_path'])
                    
                    action_msg = f"[green]✅ {path_name}:[/green] znaleziono {faces_in_file} twarzy."
                    dashboard_state["action_logs"].appendleft(Text.from_markup(action_msg))
                    status_updates_batch.append((media_id, model_name, 'processed' if faces_in_file > 0 else 'no_faces_found'))
                
                elif result_type == "FILE_ERROR":
                    full_error = f"[red]{path_name}:[/red] [dim]{result_data}[/dim]"
                    dashboard_state["error_logs"].appendleft(Text.from_markup(full_error))
                    dashboard_state["stats"]['file_errors'] += 1
                    status_updates_batch.append((media_id, model_name, 'error'))

                elif result_type == "WORKER_ERROR":
                    worker_error = result_data
                    dashboard_state["error_logs"].appendleft(Text.from_markup(f"[bold red]Krytyczny błąd workera![/]")); break
                
                dashboard_state["stats"]['processed'] += 1
                progress_bar.update(progress_task_id, advance=1)

            except queue.Empty:
                if not any(p.is_alive() for p in workers) and tasks_sent_count == dashboard_state["stats"]["processed"]:
                    worker_error = "Wszystkie procesy robocze zakończyły pracę przedwcześnie!"; break
                await asyncio.sleep(0.1)

            if len(status_updates_batch) >= 20:
                await update_indexing_status_batch(status_updates_batch); status_updates_batch.clear()
            live.update(generate_indexing_dashboard(progress_bar, dashboard_state, settings), refresh=True)
            
        dashboard_state['current_activity'] = "[bold green]Zakończono[/bold green]" if not worker_error else "[bold red]Przerwano[/bold red]"
        dashboard_state['current_path'] = None
        live.update(generate_indexing_dashboard(progress_bar, dashboard_state, settings), refresh=True)

    listener.cancel()
    for _ in range(num_workers): task_queue.put("STOP") # Dla ai_worker
    for _ in range(num_workers): task_queue.put(None) # Dla hailo_worker
    for p in workers: p.join(timeout=10)
    for p in workers:
        if p.is_alive(): p.terminate()
    if status_updates_batch: await update_indexing_status_batch(status_updates_batch)

    console.clear()
    console.print(generate_indexing_dashboard(progress_bar, dashboard_state, settings))
    _save_summary_to_file(model_name, dashboard_state, worker_error, detector_backend)
    
    if worker_error: console.print(f"\n[bold red]❌ Indeksowanie przerwane...[/]\n[dim]{worker_error}[/dim]")
    else: console.print(f"\n[bold green]✅ Indeksowanie zakończone![/bold green]")
    Prompt.ask("\n[bold]Naciśnij Enter...[/]")
