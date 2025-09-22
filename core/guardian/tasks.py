# plik: core/guardian/tasks.py
# Wersja 1.0 - Czysta logika biznesowa dla Strażnika Systemu.
# Opis: Ten moduł zawiera funkcje do wysyłania powiadomień na Telegram
#       i generowania raportów o stanie kolekcji.
# -*- coding: utf-8 -*-

import logging
from datetime import datetime

# Zależności zewnętrzne
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False

from rich.console import Console
from rich.panel import Panel

# Importy z modułów projektu `core`
from ..config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from ..database import get_db_stats

# Inicjalizacja
logger = logging.getLogger(__name__)
console = Console()

async def send_telegram_notification(message: str) -> bool:
    """
    Wysyła wiadomość tekstową na Telegram za pośrednictwem bota.
    """
    logger.info("Próba wysłania powiadomienia na Telegram...")

    if not HTTPX_AVAILABLE:
        logger.error("Brak biblioteki 'httpx'.")
        console.print(Panel("[bold red]Błąd: Brak 'httpx'![/]\nUruchom: [cyan]pip install httpx[/]", title="Brak Zależności"))
        return False

    if not TELEGRAM_BOT_TOKEN or "WPISZ" in TELEGRAM_BOT_TOKEN or \
       not TELEGRAM_CHAT_ID or "WPISZ" in TELEGRAM_CHAT_ID:
        logger.error("Dane Telegrama nie są skonfigurowane.")
        console.print(Panel("[bold red]Konfiguracja Telegrama niekompletna![/]\n\nUzupełnij `TELEGRAM_BOT_TOKEN` i `TELEGRAM_CHAT_ID` w pliku konfiguracyjnym.", title="Błąd Konfiguracji"))
        return False

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(api_url, params=params, timeout=10)
            response.raise_for_status()
        logger.info("Powiadomienie na Telegram wysłane pomyślnie.")
        return True
    except httpx.RequestError as e:
        logger.critical(f"Błąd sieciowy podczas wysyłania na Telegram: {e}", exc_info=True)
        console.print(f"[bold red]Błąd sieciowy:[/] Nie można połączyć się z serwerami Telegrama.")
        return False
    except httpx.HTTPStatusError as e:
        error_desc = e.response.json().get('description', 'Brak szczegółów.')
        logger.critical(f"Błąd API Telegrama: {e.response.status_code} - {error_desc}", exc_info=True)
        console.print(Panel(f"[bold red]Błąd API Telegrama ({e.response.status_code}):[/]\n{error_desc}", title="Błąd Wysyłania"))
        return False

async def generate_and_send_report():
    """
    Generuje szczegółowy raport o stanie kolekcji i wysyła go na Telegram.
    """
    logger.info("Generowanie raportu o stanie kolekcji...")
    try:
        stats = await get_db_stats()
    except Exception as e:
        logger.critical("Nie udało się pobrać statystyk do raportu.", exc_info=True)
        await send_telegram_notification("🔴 *BŁĄD KRYTYCZNY*\n\nNie udało się wygenerować raportu z powodu błędu bazy danych.")
        return

    total = stats.get('total', 0)
    downloaded = stats.get('downloaded', 0)
    failed = stats.get('failed', 0)
    scanned = stats.get('scan_sukces', 0)
    to_scan = total - scanned

    report_title = "📊 *Raport o Stanie Kolekcji - GPhotos Toolkit*"
    timestamp = f"_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_"
    report_body = (
        f"📄 *Podsumowanie Bazy*\n"
        f"  - Łącznie wpisów: *{total}*\n"
        f"  - Pobrane pliki: `{downloaded}`\n"
        f"  - Błędy pobierania: `{failed}`\n\n"
        f"🔍 *Status Metadanych*\n"
        f"  - Pliki z metadanymi: `{scanned}`\n"
        f"  - Oczekujące na skan: `{to_scan}`"
    )
    full_message = f"{report_title}\n{timestamp}\n\n{report_body}"

    success = await send_telegram_notification(full_message)
    return success
