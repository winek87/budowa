# Plik: core/face_recognition_logic.py
# Wersja 3.0 - Finalna, ze stabilną reinicjalizacją środowiska AI

import asyncio
import logging
from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.text import Text

from .utils import create_interactive_menu, check_dependency
from .watchdog import monitored_task, WatchdogError
from .face_rec.indexing import index_faces
from .face_rec.recognition import recognize_and_tag_faces
from .face_rec.managing import manage_people, enhance_profiles
from .face_rec.reviewing import review_unknown_faces, review_tagged_faces
from .face_rec.models import manage_ai_models, initialize_deepface_env, is_model_downloaded
from .face_rec.settings import (
    AVAILABLE_MODELS, set_memory_limit, get_current_settings,
    advanced_settings_menu
)
from .face_rec.attributes import run_attribute_analysis_menu

console = Console(record=True)
logger = logging.getLogger(__name__)

# ##############################################################################
# ===                   SEKCJA 1: PODMENU I NARZĘDZIA ROBOCZE                ===
# ##############################################################################

async def _run_model_specific_menu(model_name: str):
    """Wyświetla menu narzędzi dla konkretnego, wybranego modelu AI."""
    while True:
        console.clear()
        settings = get_current_settings()
        
        subtitle_text = (
            f"💾 Limit RAM: [bold cyan]{settings['FACE_REC_MEMORY_LIMIT_GB'] or 'Brak'}[/bold cyan] | "
            f"⚡ Detektor: [bold cyan]{settings['DETECTOR_BACKEND']}[/bold cyan]"
        )
        main_title = f"👨‍👩‍👧‍👦 Główne Narzędzia (Model: [bold green]{model_name}[/bold green]) 👨‍👩‍👧‍👦"
        
        menu_items = [
            ("Zaindeksuj nowe zdjęcia", "index"),
            ("Uruchom automatyczne rozpoznawanie", "recognize"),
            ("Przeglądaj i oznacz nieznane twarze", "review_unknown"),
            ("Przeglądaj otagowane twarze (wg osób)", "review_tagged"),
            ("Zarządzaj znanymi osobami", "manage_people"),
            ("Ulepsz profile osób (uśrednij embeddingi)", "enhance"),
            ("Analiza Atrybutów (Wiek/Płeć/Emocje)...", "attributes"),
            ("Wróć do wyboru modułu", "exit")
        ]
        selected = await create_interactive_menu(menu_items, main_title, subtitle=subtitle_text, border_style="cyan")
        if selected in ("exit", None): break

        try:
            if selected == "index": await monitored_task(index_faces, model_name, heartbeat_timeout=900)
            elif selected == "recognize": await recognize_and_tag_faces(model_name)
            elif selected == "review_unknown": await review_unknown_faces(model_name)
            elif selected == "review_tagged": await review_tagged_faces()
            elif selected == "manage_people": await manage_people(model_name)
            elif selected == "enhance": await enhance_profiles(model_name)
            elif selected == "attributes": await run_attribute_analysis_menu()
        except WatchdogError:
            console.print("\n[bold red]Wystąpił błąd krytyczny...[/]"); Prompt.ask("\n[bold]Naciśnij Enter...[/]")

async def _run_main_face_rec_tools():
    """Prowadzi użytkownika przez proces wyboru modelu AI i uruchamia narzędzia."""
    console.clear()
    console.print(Panel("🚀 Wybór Modelu Rozpoznawania 🚀", style="bold green", subtitle="Wybierz silnik do generowania cech twarzy (embeddingów)"))
    
    with console.status("[cyan]Sprawdzanie dostępności modeli...[/]"):
        model_statuses = await asyncio.gather(*[is_model_downloaded(m['name']) for m in AVAILABLE_MODELS])

    menu_items = []
    for model, is_ready in zip(AVAILABLE_MODELS, model_statuses):
        status_icon = Text("✔ ", style="green") if is_ready else Text("✘ ", style="red")
        status_text = Text("(Gotowy)", style="dim green") if is_ready else Text("(Wymaga pobrania)", style="dim red")
        label = Text(); label.append(status_icon); label.append(model['name'], style="bold"); label.append(" "); label.append(status_text)
        label.append("\n  "); label.append(Text.from_markup(model['description'], style="dim"))
        menu_items.append((label, model['name'] if is_ready else None))
        
    menu_items.append(("Wróć", "exit"))
    
    selected_model_name = await create_interactive_menu(menu_items, "Wybierz model AI")
    if selected_model_name in ("exit", None): return

    if selected_model_name == "Dlib" and not check_dependency("dlib", "dlib", "Dlib"):
        Prompt.ask("\n[bold]Naciśnij Enter...[/]"); return

    logger.info(f"Użytkownik wybrał model '{selected_model_name}' do pracy.")
    await _run_model_specific_menu(selected_model_name)

# ##############################################################################
# ===                    SEKCJA 2: GŁÓWNE MENU MODUŁU                        ===
# ##############################################################################

async def run_face_recognition_menu():
    """GŁÓWNE MENU MODUŁU. Wyświetla nadrzędne menu dla modułu rozpoznawania twarzy."""
    console.clear()
    if not check_dependency("deepface", "deepface", "DeepFace"):
        Prompt.ask("\n[bold]Naciśnij Enter...[/]"); return

    while True:
        console.clear()
        
        # Inicjalizujemy środowisko za każdym razem, gdy wracamy do tego menu
        initialize_deepface_env()
        settings = get_current_settings()
        if mem_limit := settings.get("FACE_REC_MEMORY_LIMIT_GB"):
            set_memory_limit(mem_limit)

        main_title = "👨‍👩‍👧‍👦 Menedżer Rozpoznawania Twarzy 👨‍👩‍👧‍👦"
        
        # --- ZMIANA: Bardziej informacyjny podtytuł ---
        ram_limit = settings.get("FACE_REC_MEMORY_LIMIT_GB")
        ram_str = f"{ram_limit} GB" if ram_limit else "Brak"
        subtitle = (
            f"Aktualny detektor: [bold cyan]{settings['DETECTOR_BACKEND']}[/] | "
            f"RAM: [bold cyan]{ram_str}[/]"
        )
        
        menu_items = [
            ("🚀 Uruchom Główne Narzędzia (Indeksowanie, Rozpoznawanie...)", "run_tools"),
            ("☁️ Zarządzaj Modelami AI (Pobieranie)", "manage_models"),
            ("⚙️ Ustawienia Zaawansowane (Detektor, CPU/GPU, Limity...)", "settings"),
            ("Wróć do menu głównego", "exit")
        ]
        
        selected = await create_interactive_menu(menu_items, main_title, subtitle=subtitle, border_style="blue")
        if selected in ("exit", None): break

        try:
            if selected == "run_tools":
                await _run_main_face_rec_tools()
            elif selected == "manage_models":
                await manage_ai_models()
            elif selected == "settings":
                await advanced_settings_menu()
        except WatchdogError:
            console.print("\n[bold red]Wystąpił błąd krytyczny...[/]"); Prompt.ask("\n[bold]Naciśnij Enter...[/]")

