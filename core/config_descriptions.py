# plik: core/config_descriptions.py (NOWY PLIK)
# -*- coding: utf-8 -*-
"""
Ten plik zawiera centralny słownik z opisami dla wszystkich opcji
konfiguracyjnych. Jest on używany przez Edytor Konfiguracji, aby wyświetlać
pomocnicze informacje dla użytkownika podczas edycji pliku config.py.
"""

CONFIG_DESCRIPTIONS = {
    # SEKCJA 1: PODSTAWOWE ŚCIEŻKI I PLIKI
    "SESSION_DIR": "Folder przechowujący dane logowania przeglądarki, aby unikać logowania za każdym razem.",
    "DATABASE_FILE": "Główny plik bazy danych, w którym zapisywany jest cały postęp i zebrane metadane.",
    "DOWNLOADS_DIR_BASE": "Główny folder, do którego będą pobierane zdjęcia i filmy. Program utworzy w nim podfoldery ROK/MIESIĄC.",
    "FORCED_DUPLICATES_DIR": "Folder na duplikaty, gdy skan jest uruchomiony w trybie 'Wymuś pełne odświeżenie'.",
    "URL_INPUT_FILE": "Plik tekstowy z listą adresów URL do przetworzenia przez narzędzia takie jak 'Skaner z pliku'.",
    "LOG_FILENAME": "Główny plik logu, do którego zapisywane są wszystkie szczegółowe informacje o działaniu aplikacji.",

    # SEKCJA 2: GŁÓWNE ZACHOWANIE SKANU
    "START_URL": "Link do jednego z Twoich NAJSTARSZYCH zdjęć. To punkt startowy dla nowego skanu.",
    "DIRECTION_KEY": "Kierunek nawigacji po galerii. 'ArrowLeft' (zalecane) oznacza od najstarszych do najnowszych.",

    # SEKCJA 3: STRATEGIA SILNIKA MASTER
    "METADATA_STRATEGY": "Sposób pozyskiwania metadanych: 'HYBRID' (zalecane), 'EXIF_ONLY' (najszybszy), 'ONLINE_ONLY' (najdokładniejsze dane).",
    "ENABLE_SHAKE_THE_SCAN": "Czy w trybie naprawy błędów stosować zaawansowaną metodę 'potrząśnięcia' stroną (nawigacja przód/tył)?",
    "ENABLE_RESOURCE_BLOCKING": "Czy blokować zbędne zasoby (reklamy, czcionki, obrazki), aby przyspieszyć ładowanie stron?",
    "BLOCKED_RESOURCE_TYPES": "Lista typów zasobów do zablokowania, gdy ENABLE_RESOURCE_BLOCKING jest włączone.",
    "ENABLE_PAUSE_AFTER_REPAIR": "Czy zatrzymać program i czekać na potwierdzenie po zakończeniu automatycznej naprawy błędów?",

    # SEKCJA 4: CZASY OCZEKIWANIA I WYDAJNOŚĆ
    "WAIT_FOR_PAGE_LOAD": "Maksymalny czas w sekundach na pełne załadowanie strony.",
    "WAIT_FOR_SELECTOR": "Maksymalny czas w sekundach na pojawienie się na stronie elementu (np. przycisku).",
    "WAIT_FOR_DOWNLOAD_START": "Maksymalny czas w sekundach na rozpoczęcie pobierania pliku po kliknięciu 'Pobierz'.",
    "ENABLE_ACTION_DELAY": "Czy włączyć losowe opóźnienia między akcjami, aby symulować ludzkie zachowanie?",
    "ACTION_DELAY_RANGE": "Zakres losowego opóźnienia w sekundach (minimalny, maksymalny).",

    # SEKCJA 5: ODPORNOŚĆ I ODZYSKIWANIE PO BŁĘDACH
    "NAV_REFRESH_ATTEMPTS": "Ile razy program ma odświeżyć stronę (F5), gdy standardowa nawigacja zawiedzie.",
    "NAV_BLIND_CLICK_ENABLED": "Czy po nieudanych odświeżeniach próbować nawigacji 'na ślepo' za pomocą klawiatury?",
    "MAX_RETRIES": "Ile razy program ma próbować pobrać ten sam plik, zanim oznaczy go jako permanentny błąd.",
    "MAX_RESTARTS_ON_FAILURE": "Ile razy główny silnik ma próbować uruchomić się ponownie po krytycznej awarii.",
    "RESTART_DELAY_SECONDS": "Ile sekund program ma odczekać między kolejnymi restartami po awarii.",

    # SEKCJA 6: USTAWIENIA PRZEGLĄDARKI I TECHNICZNE
    "BROWSER_TYPE": "Typ przeglądarki używanej przez Playwright: 'chromium' (zalecane), 'firefox' lub 'webkit'.",
    "DEFAULT_HEADLESS_MODE": "Czy program ma domyślnie działać w tle (True) czy z widocznym oknem przeglądarki (False)?",
    "ENABLE_HEADLESS_CURSOR": "Czy symulator kursora ma być aktywny również w trybie cichym (headless)?",
    "BROWSER_ARGS": "Zaawansowane argumenty startowe dla przeglądarki. Edytuj w osobnym podmenu.",

    # SEKCJA 7: USTAWIENIA NARZĘDZI POMOCNICZYCH
    "TELEGRAM_BOT_TOKEN": "Token Twojego bota na Telegramie do wysyłania powiadomień.",
    "TELEGRAM_CHAT_ID": "ID Twojego czatu na Telegramie, na który mają być wysyłane powiadomienia.",
    
    # SEKCJA 8: USTAWIENIA LOGOWANIA
    "LOG_TO_FILE": "Czy logi mają być zapisywane do pliku zdefiniowanego w LOG_FILENAME?",
    "LOG_ENABLED": "Główny włącznik/wyłącznik dla całego systemu logowania.",
    "LOG_LEVEL": "Poziom szczegółowości logów: 'DEBUG', 'INFO' (zalecane), 'WARNING', 'ERROR', 'CRITICAL'.",
    "LOG_SAVE_TO_FILE": "Zastępuje LOG_TO_FILE. Czy zapisywać logi do pliku?",

    # SEKCJA 9: USTAWIENIA KOPII ZAPASOWYCH
    "BACKUP_DIR": "Folder, w którym Menedżer Kopii Zapasowych będzie przechowywał archiwa z danymi (baza, sesja).",
    "FILES_TO_BACKUP": "Lista kluczowych plików i folderów dołączanych do kopii zapasowej danych.",
    "AUTO_BACKUP_ON_START": "Czy przy każdym uruchomieniu tworzyć pełną kopię zapasową całego kodu projektu?",
    "PROJECT_BACKUP_CONFIG": "Zaawansowana konfiguracja dla kopii zapasowej projektu (folder docelowy, wykluczenia).",

    # SEKCJA 10: USTAWIENIA AI
    "AI_MODELS_CACHE_DIR": "Folder na pobrane modele AI. Pusty string oznacza użycie domyślnej lokalizacji systemowej.",
    "AI_TAGGER_CONFIDENCE_THRESHOLD": "Próg pewności (0.0-1.0) dla Inteligentnego Taggera. Tag zostanie przypisany, jeśli model jest go pewny powyżej tej wartości.",

    # SEKCJA 11: USTAWIENIA MODUŁÓW DODATKOWYCH
    "LOCAL_SCANNER_DIRECTORIES": "Lista folderów zapamiętanych przez 'Lokalny Skaner' do szybkiego dostępu.",
    "FACE_DB_VECTOR_PATH": "Folder do przechowywania baz wektorów dla modułu rozpoznawania twarzy.",

    # SEKCJA 12: USTAWIENIA NARZĘDZI INTERAKTYWNYCH
    "IMAGE_VIEWER_MODE": "Sposób wyświetlania obrazów w narzędziach interaktywnych: 'system' (zalecane), 'server', 'sixel'.",

    # SEKCJA 13: SELEKTORY CSS (DLA EKSPERTÓW)
    "INFO_PANEL_BUTTON_SELECTOR": "Selektor CSS dla przycisku otwierającego panel boczny z informacjami o zdjęciu.",
    "DOWNLOAD_OPTION_SELECTOR": "Selektor CSS dla opcji 'Pobierz' w menu 'Więcej opcji'.",
    "THREE_DOTS_MENU_SELECTOR": "Selektor CSS dla przycisku 'Więcej opcji' (trzy kropki).",
    "NAV_ARROW_LEFT_SELECTOR": "Selektor CSS dla strzałki nawigacyjnej w lewo (do nowszych zdjęć).",
    "NAV_ARROW_RIGHT_SELECTOR": "Selektor CSS dla strzałki nawigacyjnej w prawo (do starszych zdjęć)."
}
