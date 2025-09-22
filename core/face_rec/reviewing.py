# Plik: core/face_rec/reviewing.py
# Wersja 2.1 - Ulepszony UX i dodanie opcji ustawienia zdjęcia profilowego

import asyncio
import logging
from pathlib import Path
from collections import deque
import tempfile
import os

try:
    from PIL import Image
except ImportError:
    Image = None

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.layout import Layout
from rich.live import Live
from rich.text import Text

from ..utils import create_interactive_menu, open_image_viewer, get_key
from ..database import (
    get_all_tagged_people, get_media_for_person, get_face_details_for_review,
    get_all_people, add_person, tag_face, delete_face_by_id,
    untag_face_by_media_and_person, update_person_avatar
)
from .dashboard import ThumbnailRenderer

console = Console(record=True)
logger = logging.getLogger(__name__)


# ##############################################################################
# ===            SEKCJA 1: PRZEGLĄDANIE TWARZY ZNANYCH (OTAGOWANYCH)         ===
# ##############################################################################

async def review_tagged_faces():
    """
    Uruchamia interaktywną przeglądarkę otagowanych zdjęć,
    z możliwością korygowania błędnych tagów.
    """
    while True:
        console.clear()
        console.print(Panel("🖼️ Przeglądanie i Korekta Otagowanych Zdjęć 🖼️", style="bold green"))
        with console.status("Pobieranie listy osób..."):
            people = await get_all_tagged_people()
        if not people:
            console.print("[yellow]Nie znaleziono jeszcze żadnych otagowanych osób.[/yellow]"); Prompt.ask("\nEnter..."); return

        menu_items = [(f"{p['name']} ([cyan]{p['photo_count']} zdjęć[/cyan])", p) for p in people]
        menu_items.append(("Wróć", "exit"))
        person = await create_interactive_menu(menu_items, "Wybierz osobę do przeglądania")
        if person in ("exit", None): break

        media_files = await get_media_for_person(person['person_id'])
        if not media_files:
            console.print(f"[yellow]Brak zdjęć dla osoby '{person['name']}'.[/yellow]"); Prompt.ask("\nEnter..."); continue
            
        media_queue = deque(media_files)
        current_index = 0
        
        while 0 <= current_index < len(media_queue):
            current_media = media_queue[current_index]
            path = Path(current_media['final_path'])
            console.clear()
            console.print(Panel(
                f"Osoba: [cyan]{person['name']}[/] | Zdjęcie {current_index + 1}/{len(media_queue)}",
                subtitle=f"[dim]{path.name}[/dim]"
            ))
            await asyncio.to_thread(open_image_viewer, path)
            
            console.print("\n[bold]Akcje:[/bold] [cyan]Strzałki L/P[/cyan] (nawigacja) | [cyan]U[/cyan] (usuń tag) | [cyan]Q[/cyan] (wróć)")
            key = await asyncio.to_thread(get_key)

            if key == "RIGHT": current_index = min(current_index + 1, len(media_queue) - 1)
            elif key == "LEFT": current_index = max(current_index - 1, 0)
            elif key and key.upper() == 'U':
                if Confirm.ask(f"\nUsunąć tag [cyan]'{person['name']}'[/cyan] ze zdjęcia?", default=True):
                    await untag_face_by_media_and_person(current_media['id'], person['person_id'])
                    console.print("[green]✅ Tag usunięty.[/green]")
                    media_queue.remove(current_media)
                    current_index = min(current_index, len(media_queue) - 1)
                    if not media_queue:
                        console.print("\n[bold]To było ostatnie zdjęcie. Powrót do listy osób...[/bold]"); await asyncio.sleep(2); break
                    await asyncio.sleep(1)
            elif key and key.upper() == 'Q':
                break

# ##############################################################################
# ===          SEKCJA 2: PRZEGLĄDANIE I TAGOWANIE TWARZY NIEZNANYCH          ===
# ##############################################################################

async def review_unknown_faces(model_name: str):
    """
    Uruchamia interaktywny proces przeglądania, tagowania lub usuwania
    niezidentyfikowanych twarzy z poprawnym, proporcjonalnym układem UI.
    """
    console.clear()
    console.print(Panel(f"🧐 Weryfikacja Nieznanych Twarzy (Model: [cyan]{model_name}[/cyan]) 🧐", style="bold yellow"))
    with console.status("Pobieranie listy nieznanych twarzy..."):
        all_face_details = await get_face_details_for_review(model_name)
    if not all_face_details:
        console.print("[green]✅ Brak nowych twarzy do weryfikacji.[/green]"); Prompt.ask("\nEnter..."); return

    faces_to_review = deque(all_face_details)
    total_faces_initial = len(faces_to_review)

    while faces_to_review:
        details = faces_to_review[0]
        face_id = details['face_id']
        
        selected_index = 0
        action_items = [
            ("Oznacz tę twarz...", "tag"), ("Pokaż CAŁE zdjęcie", "show_full"),
            ("Ignoruj (usuń z bazy)", "delete"), ("Pomiń (następna)", "skip"),
            ("Zakończ przeglądanie", "exit")
        ]

        thumbnail_renderer = None
        temp_file_path = None
        try:
            with Image.open(details['final_path']) as img:
                area = details['facial_area']
                margin_x, margin_y = int(area['w'] * 0.4), int(area['h'] * 0.4)
                box = (max(0, area['x'] - margin_x), max(0, area['y'] - margin_y),
                       min(img.width, area['x'] + area['w'] + margin_x), min(img.height, area['y'] + area['h'] + margin_y))
                face_img = img.crop(box)
                with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                    temp_file_path = tmp.name
                    face_img.save(tmp, format="JPEG")
                thumbnail_renderer = ThumbnailRenderer(Path(temp_file_path), max_size=(80, 40))
        except Exception as e:
            thumbnail_renderer = f"[red]Błąd podglądu: {e}[/red]"

        def generate_review_layout() -> Layout:
            """Renderuje kompletny widok: menu i podgląd z poprawnymi proporcjami."""
            layout = Layout()
            
            menu_text = Text(justify="center")
            for i, (text, _) in enumerate(action_items):
                style = "bold black on white" if i == selected_index else ""
                prefix = "» " if i == selected_index else "  "
                menu_text.append(f"{prefix}{text}\n", style=style)
            
            menu_panel = Panel(menu_text, title="Co chcesz zrobić z tą twarzą?")
            preview_panel = Panel(thumbnail_renderer or "", title="Podgląd")
            info_panel = Panel(
                f"Twarz {total_faces_initial - len(faces_to_review) + 1} z {total_faces_initial} (ID: {face_id})",
                subtitle=f"[dim]{details['final_path']}[/dim]"
            )

            layout.split_column(
                Layout(info_panel, size=3),
                Layout(menu_panel, size=len(action_items) + 2),
                Layout(preview_panel, ratio=1),
            )
            return layout

        action = None
        with Live(generate_review_layout(), screen=True, auto_refresh=False, transient=True) as live:
            while True:
                live.update(generate_review_layout(), refresh=True)
                key = await asyncio.to_thread(get_key)
                if not key: continue
                if key == "UP": selected_index = (selected_index - 1 + len(action_items)) % len(action_items)
                elif key == "DOWN": selected_index = (selected_index + 1) % len(action_items)
                elif key == "ENTER":
                    action = action_items[selected_index][1]
                    break
        
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)

        if action == "tag":
            people = await get_all_people(model_name)
            person_choices = [(p['name'], p['person_id']) for p in people]
            person_choices.extend([("---", None), ("[bold green](+) Dodaj nową osobę[/bold green]", "new_person"), ("Anuluj", "cancel")])
            selected_person_id = await create_interactive_menu(person_choices, "Wybierz osobę lub dodaj nową")
            if selected_person_id and selected_person_id != "cancel":
                if selected_person_id == "new_person":
                    new_name = Prompt.ask("[cyan]Podaj imię i nazwisko nowej osoby[/]").strip()
                    if new_name:
                        new_person_id = await add_person(new_name, model_name, details['embedding'], source_media_id=details['media_id'])
                        if new_person_id:
                            await tag_face(face_id, new_person_id)
                            console.print(f"[green]✅ Dodano '{new_name}' i oznaczono twarz.[/green]")
                            if Confirm.ask(f"Czy ustawić to zdjęcie jako profilowe dla {new_name}?", default=True):
                                await update_person_avatar(new_person_id, details['media_id'])
                                console.print("[green]✅ Ustawiono zdjęcie profilowe.[/green]")
                            faces_to_review.popleft()
                        else: console.print(f"[red]Błąd: Osoba '{new_name}' już istnieje.[/red]")
                else:
                    await tag_face(face_id, selected_person_id)
                    person_name = next(p['name'] for p in people if p['person_id'] == selected_person_id)
                    console.print(f"[green]✅ Oznaczono twarz jako '{person_name}'.[/green]")
                    faces_to_review.popleft()
                await asyncio.sleep(1.5)
        elif action == "show_full":
            await asyncio.to_thread(open_image_viewer, Path(details['final_path']))
            continue
        elif action == "delete":
            if Confirm.ask(f"[bold red]Czy na pewno trwale usunąć tę twarz (ID: {face_id}) z bazy?[/bold red]"):
                await delete_face_by_id(face_id)
                console.print("[green]✅ Twarz usunięta.[/green]"); faces_to_review.popleft(); await asyncio.sleep(1)
        elif action == "skip":
            faces_to_review.rotate(-1)
        elif action in ("exit", None):
            break
