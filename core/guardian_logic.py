# -*- coding: utf-8 -*-

# plik: core/guardian_logic.py
# Wersja 5.1 - Stabilna i Niezawodna Logika Wywoływania Testów
#
# ##############################################################################
# ===                     MODUŁ STRAŻNIKA SYSTEMU                            ===
# ##############################################################################
#
# "Strażnik Systemu" to proaktywne narzędzie do monitorowania "zdrowia"
# aplikacji oraz zarządzania powiadomieniami. Jego główne funkcje to:
#
#  1. BILANS ZDROWIA: Uruchamia uproszczoną wersję "Doktora", aby szybko
#     sprawdzić kluczowe komponenty systemu.
#
#  2. POWIADOMIENIA: Pozwala na wysłanie wiadomości testowej lub pełnego
#     raportu o stanie kolekcji na Telegram.
#
################################################################################

# --- GŁÓWNE IMPORTY ---
import asyncio
import logging
from datetime import datetime
from collections import deque
from functools import partial

# --- Zależności zewnętrzne ---
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False

# --- IMPORTY Z BIBLIOTEKI `rich` ---
from rich.console import Console, Group
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
from rich.text import Text
from rich.live import Live
from rich.table import Table
from rich.align import Align
from rich.layout import Layout

# --- IMPORTY Z WŁASNYCH MODUŁÓW ---
from .config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from .utils import create_interactive_menu, LogCollectorHandler
from . import config as core_config
from .config_editor_logic import get_key

# Importujemy poszczególne, zmodernizowane testy z modułu "Doktora"
from .doctor_logic import (
    check_session_validity,
    check_database_schema_and_integrity,
    check_dependencies,
    check_exiftool_program,
    DoctorCheckError,
    DoctorInfo
)
# Importujemy asynchroniczną funkcję do pobierania statystyk
from .database import get_db_stats

# --- Inicjalizacja i Konfiguracja Modułu ---
console = Console(record=True)
logger = logging.getLogger(__name__)

async def run_health_check():
    """
    Przeprowadza szybki, automatyczny bilans zdrowia kluczowych
    komponentów systemu, prezentując wyniki w interfejsie na żywo
    z dedykowanym panelem na logi.
    """
    console.clear()
    logger.info("Uruchamiam Szybki Bilans Zdrowia Systemu...")
    console.print(Panel("🩺 Uruchamianie Szybkiego Bilansu Zdrowia...", expand=False, style="yellow"))

    health_checks = [
        ("Ważność sesji logowania", partial(check_session_validity, core_config), True),
        ("Struktura Bazy Danych", partial(check_database_schema_and_integrity, core_config), True),
        ("Zależności (Python)", check_dependencies, False),
        ("Zależności (ExifTool)", check_exiftool_program, False),
    ]

    live_logs = deque(maxlen=15)
    log_collector = LogCollectorHandler(live_logs)
    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]
    root_logger.handlers.clear()
    root_logger.addHandler(log_collector)

    step_statuses = {name: "[dim]Oczekuje...[/]" for name, _, _ in health_checks}
    overall_ok = True
    current_step_name = ""

    def generate_live_layout() -> Layout:
        """Dynamicznie tworzy layout interfejsu na żywo."""
        status_table = Table.grid(padding=(0, 2))
        status_table.add_column("Komponent Systemu", style="cyan", no_wrap=True, width=35)
        status_table.add_column("Status / Szczegóły", style="white")
        for name, _, _ in health_checks:
            status_text = step_statuses[name]
            if name == current_step_name:
                status_text = f"[bold yellow][ Sprawdzam... ][/] {console.render_str(':hourglass:')}"
            status_table.add_row(name, Text.from_markup(status_text))

        logs_panel = Panel(Group(*live_logs), title="Logi na Żywo", border_style="green")
        layout = Layout(name="root")
        layout.split(
            Layout(Panel(Align.center(status_table)), name="header", size=len(health_checks) + 2),
            Layout(logs_panel, name="body", ratio=1),
        )
        return layout

    # --- Uruchomienie testów z `rich.Live` ---
    with Live(generate_live_layout(), screen=True, auto_refresh=False, transient=True, vertical_overflow="visible") as live:
        for name, check_func, is_async in health_checks:
            current_step_name = name
            live.update(generate_live_layout(), refresh=True)
            await asyncio.sleep(0.5)

            try:
                if is_async:
                    details = await check_func()
                else:
                    details = await asyncio.to_thread(check_func)
                
                step_statuses[name] = f"[bold green]✅ OK[/]\n[dim]{details}[/dim]"
            except DoctorInfo as e:
                step_statuses[name] = f"[bold cyan]ℹ️ INFO[/]\n[dim]{e}[/dim]"
            except DoctorCheckError as e:
                step_statuses[name] = f"[bold red]❌ BŁĄD[/]\n[dim]{e}[/dim]"
                overall_ok = False
            except Exception as e:
                step_statuses[name] = f"[bold white on red]💥 KRYTYCZNY BŁĄD[/]\n[dim]{e}[/dim]"
                overall_ok = False
                logger.critical(f"Krytyczny błąd podczas testu zdrowia '{name}'", exc_info=True)

            live.update(generate_live_layout(), refresh=True)

        current_step_name = ""
        live.update(generate_live_layout(), refresh=True)
        logger.info("Bilans zdrowia zakończony. Oczekuję na interakcję użytkownika.")
        await asyncio.to_thread(get_key)

    # --- Sprzątanie i podsumowanie ---
    root_logger.removeHandler(log_collector)
    for h in original_handlers:
        root_logger.addHandler(h)
    
    console.clear()
    if overall_ok:
        logger.info("Bilans zdrowia zakończony pomyślnie.")
        console.print(Panel("✅ [bold green]Bilans zdrowia zakończony pomyślnie. Wszystkie kluczowe systemy działają poprawnie.[/]", border_style="green"))
    else:
        logger.warning("Bilans zdrowia wykrył problemy.")
        console.print(Panel("⚠️ [bold red]Bilans zdrowia wykrył problemy! Sprawdź szczegóły w logach i powyższej tabeli.[/]", border_style="red"))

async def send_telegram_notification(message: str) -> bool:
    """
    Wysyła wiadomość tekstową na Telegram za pośrednictwem bota.
    """
    logger.info("Próba wysłania powiadomienia na Telegram...")
    
    if not HTTPX_AVAILABLE:
        logger.error("Brak biblioteki 'httpx'. Nie można wysłać powiadomienia.")
        console.print(Panel("[bold red]Błąd: Brak biblioteki 'httpx'![/bold red]\nUruchom: [cyan]pip install httpx[/cyan]", title="Instrukcja Instalacji"))
        return False

    if not TELEGRAM_BOT_TOKEN or "WPISZ_SWOJ_TOKEN" in TELEGRAM_BOT_TOKEN or \
       not TELEGRAM_CHAT_ID or "WPISZ_SWOJE_ID" in TELEGRAM_CHAT_ID:
        logger.error("Dane do wysyłania powiadomień na Telegram nie zostały poprawnie skonfigurowane w `core/config.py`.")
        console.print(Panel(
            "[bold red]Konfiguracja Telegrama niekompletna![/]\n\n"
            "Przejdź do 'Narzędzia Zaawansowane -> Edytor Konfiguracji' "
            "i uzupełnij zmienne `TELEGRAM_BOT_TOKEN` oraz `TELEGRAM_CHAT_ID`.",
            title="Błąd Konfiguracji", border_style="red"
        ))
        return False

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(api_url, params=params, timeout=10)
            response.raise_for_status()
        logger.info("Powiadomienie na Telegram zostało pomyślnie wysłane.")
        return True
    except httpx.RequestError as e:
        logger.critical(f"Błąd sieciowy podczas wysyłania powiadomienia na Telegram: {e}", exc_info=True)
        console.print(f"[bold red]Błąd sieciowy:[/] Nie można połączyć się z serwerami Telegrama.")
        return False
    except httpx.HTTPStatusError as e:
        error_desc = e.response.json().get('description', 'Brak szczegółów.')
        logger.critical(f"Błąd API Telegrama: {e.response.status_code} - {error_desc}", exc_info=True)
        console.print(Panel(
            f"[bold red]Błąd API Telegrama ({e.response.status_code}):[/]\n{error_desc}\n\n"
            "[yellow]Sprawdź, czy token bota i ID czatu są poprawne.[/yellow]",
            title="Błąd Wysyłania", border_style="red"
        ))
        return False


async def generate_and_send_daily_report():
    """
    Generuje szczegółowy raport o stanie kolekcji i wysyła go na Telegram.
    """
    logger.info("Generowanie dziennego raportu o stanie kolekcji...")
    console.print(Panel("📊 Generowanie Dziennego Raportu 📊", expand=False, border_style="blue"))
    
    try:
        with console.status("[cyan]Pobieranie statystyk z bazy danych...[/]"):
            stats = await get_db_stats()
    except Exception as e:
        logger.critical("Nie udało się pobrać statystyk z bazy danych do raportu.", exc_info=True)
        await send_telegram_notification("🔴 *BŁĄD KRYTYCZNY*\n\nNie udało się wygenerować raportu.")
        return

    total_files = stats.get('total', 0)
    downloaded = stats.get('downloaded', 0)
    failed = stats.get('failed', 0)
    skipped = stats.get('skipped', 0)
    files_with_metadata = stats.get('scan_sukces', 0)
    to_scan_or_fix = total_files - downloaded
    
    report_title = "📊 *Raport Dzienny - Google Photos Toolkit*"
    timestamp = f"_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_"
    report_body = (
        f"📄 *Podsumowanie Bazy Danych*\n"
        f"  - Łącznie wpisów: *{total_files}*\n"
        f"  - Pobrane pliki: `{downloaded}`\n"
        f"  - Pominięte: `{skipped}`\n"
        f"  - Błędy: `{failed}`\n\n"
        f"🔍 *Status Metadanych*\n"
        f"  - Pliki z metadanymi: `{files_with_metadata}`\n"
        f"  - Pozostało do przeskanowania: `{to_scan_or_fix}`"
    )
    full_message = f"{report_title}\n{timestamp}\n\n{report_body}"
    
    with console.status("[cyan]Wysyłanie raportu na Telegram...[/]"):
        success = await send_telegram_notification(full_message)
        
    if success:
        console.print("\n[bold green]✅ Raport został pomyślnie wysłany![/]")
    else:
        console.print("\n[bold red]❌ Nie udało się wysłać raportu.[/]")


async def configure_notifications():
    """
    Wyświetla menu do zarządzania powiadomieniami na Telegram.
    """
    console.clear()
    logger.info("Uruchomiono konfigurator powiadomień.")
    console.print(Panel("🔔 Konfiguracja i Test Powiadomień 🔔", expand=False, style="yellow"))
    
    menu_items = [
        ("Wyślij wiadomość testową", "send_test"),
        ("Wygeneruj i wyślij pełny raport", "send_report"),
        ("Wróć", "back")
    ]
    selected_action = await create_interactive_menu(menu_items, "Opcje Powiadomień", "cyan")

    if selected_action in ["back", None]: return
        
    if selected_action == "send_test":
        test_message = "🚀 *Wiadomość testowa z Google Photos Toolkit!* 🚀\n\n" \
                       "Jeśli widzisz tę wiadomość, Twoja konfiguracja jest **poprawna**. ✅"
        with console.status("[cyan]Wysyłanie wiadomości testowej...[/]"):
            success = await send_telegram_notification(test_message)
        if success:
            console.print("\n[bold green]✅ Wiadomość testowa wysłana pomyślnie![/]")
    
    elif selected_action == "send_report":
        await generate_and_send_daily_report()


# ##############################################################################
# ===                    SEKCJA 2: GŁÓWNA FUNKCJA URUCHOMIENIOWA             ===
# ##############################################################################

async def run_guardian_menu():
    """
    Wyświetla i zarządza interaktywnym menu dla "Strażnika Systemu".

    Ta funkcja jest "launcherem" dla modułu. Jej zadaniem jest:
    1.  Zdefiniowanie opcji dostępnych w menu.
    2.  Wywołanie uniwersalnej funkcji `create_interactive_menu` do wyświetlenia
        interfejsu i obsłużenia wyboru użytkownika.
    3.  Uruchomienie odpowiedniej akcji w zależności od decyzji użytkownika.
    """
    logger.info("Uruchamiam menu Strażnika Systemu.")

    menu_items = [
        ("Uruchom Szybki Bilans Zdrowia", run_health_check),
        ("Powiadomienia i Raporty", configure_notifications),
        ("Wróć do menu głównego", "exit")
    ]

    while True:
        console.clear()

        selected_action = await create_interactive_menu(
            menu_items,
            "🛡️ Strażnik Systemu 🛡️",
            border_style="yellow"
        )

        if selected_action == "exit" or selected_action is None:
            logger.info("Zamykanie Strażnika Systemu.")
            break

        await selected_action()

        Prompt.ask("\n[bold]Naciśnij Enter, aby wrócić do menu Strażnika...[/]")

