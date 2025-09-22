# plik: core/ai_tagger_logic.py (WERSJA Z POPRAWIONYMI WYWOŁANIAMI)
# -*- coding: utf-8 -*-

import asyncio
import logging
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.logging import RichHandler

from .utils import create_interactive_menu, check_dependency
from .config import AI_TAGGER_MODELS, AI_TAGGER_DEFAULT_MODEL_PATH, AI_TAGGER_TRANSLATION_MODE
from .ai_tagger.workflows import (
    run_automatic_tagging,
    run_interactive_tagging,
    review_tagged_images,
    write_tags_to_exif,
    run_tag_manager
)

console = Console()

try:
    import googletrans
    GOOGLETRANS_AVAILABLE = True
except ImportError:
    GOOGLETRANS_AVAILABLE = False

async def run_ai_tagger_menu():
    """Główna funkcja zarządzająca menu modułu AI Tagger."""
    
    logging.basicConfig(
        level="INFO", format="%(message)s", datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=False)]
    )
    logging.getLogger("app")

    dependencies = [("transformers", "transformers", "Transformers"), ("torch", "torch", "PyTorch"), ("PIL", "Pillow", "Pillow")]
    if not all(check_dependency(lib, pkg, name) for lib, pkg, name in dependencies):
        Prompt.ask("\n[y]Brakuje podstawowych bibliotek AI. Naciśnij Enter, aby wrócić...[/y]"); return

    current_model_path = AI_TAGGER_DEFAULT_MODEL_PATH

    while True:
        console.clear()
        
        current_model_name = next((m['name'] for m in AI_TAGGER_MODELS if m['path'] == current_model_path), "Nieznany")
        panel_title = f"🤖 Inteligentne Tagowanie Obrazów (AI) 🤖\n[dim]Aktywny model: [cyan]{current_model_name}[/dim]"
        console.print(Panel(panel_title, expand=False, style="b blue"))

        if not GOOGLETRANS_AVAILABLE and AI_TAGGER_TRANSLATION_MODE == 'polish':
            console.print(Panel("⚠️ [b y]Ostrzeżenie: Brak `googletrans`.[/b y]\nTagi będą w języku angielskim.", title="Tłumaczenie wyłączone", border_style="y"))

        menu_items = [
            ("Uruchom tagowanie automatyczne", "auto"),
            ("Uruchom tagowanie interaktywne", "interactive"),
            ("Przeglądaj i edytuj otagowane obrazy", "review"),
            ("[purple]Zarządzaj tagami (Menedżer)[/purple]", "manager"),
            ("[bold cyan]Zmień model AI[/bold cyan]", "select_model"),
            ("[yellow]Zastosuj tagi AI (zapisz do plików)[/yellow]", "write"),
            ("[red]Wyczyść i otaguj wszystko od nowa[/red]", "rescan"),
            ("Wróć do menu głównego", "exit")
        ]
        action = await create_interactive_menu(menu_items, "Wybierz operację")

        if action in ["exit", None]: break

        should_wait = True

        # === ZMIANA: Przywrócenie poprawnej logiki wywołań ===
        if action == "auto":
            await run_automatic_tagging(rescan=False, model_path=current_model_path)
        elif action == "interactive":
            await run_interactive_tagging(rescan=False, model_path=current_model_path)
        # === KONIEC ZMIANY ===
        elif action == "review":
            await review_tagged_images(); should_wait = False
        elif action == "manager":
            await run_tag_manager(); should_wait = False
        elif action == "write":
            await write_tags_to_exif()
        elif action == "select_model":
            model_menu = [(m['name'], m['path']) for m in AI_TAGGER_MODELS]
            new_path = await create_interactive_menu(model_menu, "Wybierz model AI")
            if new_path:
                current_model_path = new_path
            should_wait = False
        elif action == "rescan":
            if Confirm.ask("\n[b y]Jesteś pewien? To usunie WSZYSTKIE tagi AI z bazy.[/b y]", default=False):
                is_interactive = Confirm.ask("[b]Czy ponowne skanowanie ma być w trybie interaktywnym?[/b]", default=False)
                if is_interactive:
                    await run_interactive_tagging(rescan=True, model_path=current_model_path)
                else:
                    await run_automatic_tagging(rescan=True, model_path=current_model_path)

        if should_wait:
            Prompt.ask("\n[b]Operacja zakończona. Naciśnij Enter...[/]")
