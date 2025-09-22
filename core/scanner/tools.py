# plik: core/scanner/tools.py
# Wersja 1.1 - Kompletna wersja z narzędziem diagnostycznym.
# Opis: Ten moduł zawiera samodzielne narzędzia pomocnicze i diagnostyczne
#       związane z modułem skanera, takie jak eksportowanie URL-i
#       czy testowanie pojedynczego adresu.
# -*- coding: utf-8 -*-

import logging
import json
from pathlib import Path

# --- Zależności zewnętrzne ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

# --- Importy z modułów projektu `core` ---
from ..config import URL_INPUT_FILE, DEFAULT_HEADLESS_MODE
from ..database import get_all_urls_from_db, get_urls_to_fix

# --- Importy z wewnętrznych modułów pakietu `scanner` ---
from .online.playwright_manager import PlaywrightManager
from .online.page_parser import get_advanced_photo_details_from_page

# --- Inicjalizacja ---
logger = logging.getLogger(__name__)
console = Console()

async def export_urls_from_db():
    """
    Eksportuje wszystkie adresy URL z bazy danych do pliku tekstowego.
    """
    console.clear()
    logger.info(f"Rozpoczynam eksport wszystkich adresów URL do pliku '{URL_INPUT_FILE}'...")
    console.print(Panel(f"📦 Eksport Wszystkich URL-i z Bazy do Pliku", expand=False, style="blue"))

    try:
        urls = await get_all_urls_from_db()
        
        if not urls:
            logger.warning("Baza danych jest pusta lub nie zawiera żadnych adresów URL.")
            console.print("\n[bold yellow]Nie znaleziono żadnych adresów URL w bazie danych.[/bold yellow]")
            return

        output_file = Path(URL_INPUT_FILE)
        with open(output_file, "w", encoding="utf-8") as f:
            for url in urls:
                f.write(f"{url}\n")
                
        logger.info(f"Sukces! Wyeksportowano {len(urls)} adresów URL.")
        console.print(f"\n[bold green]✅ Pomyślnie zapisano {len(urls)} URL-i w pliku:[/bold green]")
        console.print(f"[cyan]{output_file.resolve()}[/cyan]")
        
    except Exception as e:
        logger.critical(f"Krytyczny błąd podczas eksportu adresów URL: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd. Sprawdź logi.[/bold red]")


async def export_fix_urls():
    """
    Eksportuje URL-e wymagające ponownego skanowania do pliku `urls_to_fix.txt`.
    """
    FIX_URL_FILE = Path("urls_to_fix.txt")
    console.clear()
    logger.info(f"Rozpoczynam eksport URL-i wymagających naprawy do '{FIX_URL_FILE.name}'...")
    console.print(Panel(f"📦 Eksport URL-i do Naprawy do Pliku '{FIX_URL_FILE.name}'", expand=False, style="yellow"))

    try:
        urls_to_fix = await get_urls_to_fix()

        if not urls_to_fix:
            logger.info("Nie znaleziono żadnych wpisów wymagających naprawy metadanych.")
            console.print("\n[bold green]✅ Wszystkie metadany w bazie są kompletne.[/bold green]")
            return

        with open(FIX_URL_FILE, "w", encoding="utf-8") as f:
            for url in urls_to_fix:
                f.write(f"{url}\n")
        
        logger.info(f"Sukces! Wyeksportowano {len(urls_to_fix)} adresów URL.")
        console.print(f"\n[bold green]✅ Pomyślnie zapisano {len(urls_to_fix)} URL-i w pliku:[/bold green]")
        console.print(f"[cyan]{FIX_URL_FILE.resolve()}[/cyan]")

    except Exception as e:
        logger.critical(f"Krytyczny błąd podczas eksportu URL-i do naprawy: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd. Sprawdź logi.[/bold red]")


async def run_single_url_test():
    """
    Uruchamia pełny test diagnostyczny dla jednego, ręcznie podanego adresu URL.
    Wykorzystuje zrefaktoryzowane komponenty PlaywrightManager i page_parser.
    """
    console.clear()
    console.print(Panel("[bold yellow]🔬 Test Skanera Online dla Pojedynczego URL 🔬[/]", expand=False))
    
    url = Prompt.ask("\n[cyan]Wklej adres URL zdjęcia do przetestowania[/]")
    if not url.strip().startswith("http"):
        console.print("[bold red]To nie jest prawidłowy adres URL.[/bold red]")
        logger.error("Podano nieprawidłowy adres URL do testu.")
        return

    logger.info(f"Uruchamianie testu diagnostycznego dla URL: {url}")

    try:
        # Używamy naszego nowego, czystego menedżera przeglądarki
        async with PlaywrightManager(headless_mode=False) as pm:
            with console.status("[cyan]Uruchamianie przeglądarki...[/]"):
                page = await pm.new_page()

            with console.status(f"[cyan]Nawigacja do: [dim]{url}[/dim]...[/]"):
                await page.goto(url, wait_until='load', timeout=60 * 1000)
            
            logger.info("Strona załadowana. Uruchamiam parser...")
            with console.status("[cyan]Skanowanie metadanych ze strony...[/]"):
                metadata = await get_advanced_photo_details_from_page(page, url)
            
            console.clear()
            if metadata:
                console.print(Panel("[bold green]✅ SKANER ZAKOŃCZYŁ PRACĘ SUKCESEM[/]", title="Wynik Testu"))
                
                table = Table(title="Zebrane Metadane", show_header=False, box=None, padding=(0, 2))
                table.add_column(style="cyan", justify="right", width=25)
                table.add_column()
                
                for key, value in metadata.items():
                    val_str = ""
                    if isinstance(value, list):
                        val_str = "\n".join(f"- {item}" for item in value)
                    elif isinstance(value, dict):
                        val_str = json.dumps(value, indent=2, ensure_ascii=False)
                    else:
                        val_str = str(value)
                    table.add_row(f"[bold]{key}:[/bold]", val_str)
                
                console.print(Panel(table, border_style="green"))
            else:
                console.print(Panel("[bold red]❌ SKANER ZAKOŃCZYŁ PRACĘ BŁĘDEM[/]", title="Wynik Testu", border_style="red"))
                console.print("Nie udało się znaleźć żadnych metadanych. Sprawdź logi, aby uzyskać więcej informacji.")
                logger.error("Skaner diagnostyczny nie zwrócił żadnych metadanych.")

    except Exception as e:
        logger.critical(f"Wystąpił krytyczny błąd podczas testu diagnostycznego: {e}", exc_info=True)
        console.print(f"[bold red]Wystąpił błąd krytyczny. Sprawdź logi.[/bold red]")
