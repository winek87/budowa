# plik: core/local_scanner/config.py
# Wersja 1.0 - Dedykowana konfiguracja dla Lokalnego Skanera.
# Opis: Ten plik zawiera ustawienia specyficzne dla modułu local_scanner,
#       aby uczynić go bardziej autonomicznym.
# -*- coding: utf-8 -*-

import re
from pathlib import Path

# Lista folderów do szybkiego dostępu w menu Lokalnego Importera.
# Możesz tutaj dodać swoje domyślne ścieżki.
LOCAL_SCANNER_DIRECTORIES = [
    
    
    '/media/NEXTCLOUD/winek/files','/media/NEXTCLOUD/winek/files/Dev',# Przykłady:
    # '~/Obrazy/Do Przeniesienia',
    # '/mnt/dysk_z_archiwum/zdjecia',
]

def add_local_scanner_directory(new_path: str):
    """
    Dodaje nową ścieżkę do listy LOCAL_SCANNER_DIRECTORIES i zapisuje zmiany
    w tym pliku konfiguracyjnym, zachowując formatowanie.
    """
    config_path = Path(__file__)
    content = config_path.read_text(encoding="utf-8")

    # Upewnij się, że ścieżka nie jest już na liście
    if f"'{new_path}'" in content or f'"{new_path}"' in content:
        return False  # Ścieżka już istnieje

    # Znajdź linię z definicją LOCAL_SCANNER_DIRECTORIES
    match = re.search(r"(LOCAL_SCANNER_DIRECTORIES\s*=\s*\[\s*)", content)
    if not match:
        return False # Nie znaleziono definicji

    insert_pos = match.end(0)
    new_entry = f"\n    '{new_path}',"
    
    # Złóż nową zawartość
    new_content = content[:insert_pos] + new_entry + content[insert_pos:]
    
    # Zapisz zmiany do pliku
    config_path.write_text(new_content, encoding="utf-8")
    
    # Zaktualizuj listę w bieżącej sesji
    if new_path not in LOCAL_SCANNER_DIRECTORIES:
        LOCAL_SCANNER_DIRECTORIES.append(new_path)
    
    return True
