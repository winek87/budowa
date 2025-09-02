# -*- coding: utf-8 -*-

# ==============================================================================
# ===         PLIK KONFIGURACYJNY APLIKACJI (core/config.py)               ===
# ==============================================================================
#
# Wersja 5.0 - Scentralizowana Konfiguracja i Ulepszone Ścieżki
#
# WITAJ W CENTRUM DOWODZENIA!
#
# To jest jedyny plik, który powinieneś edytować, aby dostosować działanie
# programu do swoich potrzeb. Każda opcja jest dokładnie opisana.
# Zmieniając wartości tutaj, wpływasz na zachowanie wszystkich narzędzi.
#

from pathlib import Path
import os

# --- Definicja Ścieżki Głównej Projektu ---
# Ta zmienna jest używana do budowania ścieżek względnych wewnątrz projektu.
# Powinna wskazywać na główny folder, w którym znajduje się plik uruchom.py.
# ZMIANA: Konwertujemy string na obiekt Path dla łatwiejszych operacji.
PROJECT_ROOT = "/media/MyDrive/budowa4"

# --- Funkcja pomocnicza do tworzenia folderów ---
def ensure_dir(path: Path):
    """Upewnia się, że dany folder istnieje."""
    path.mkdir(parents=True, exist_ok=True)

# ##############################################################################
# ===                   SEKCJA 1: PODSTAWOWE ŚCIEŻKI I PLIKI                 ===
# ##############################################################################
#
# Ta sekcja definiuje wszystkie kluczowe lokalizacje plików i folderów.
# Możesz tutaj podać dowolne ścieżki absolutne, aby przechowywać dane
# poza głównym folderem aplikacji (np. na innym dysku).
#
#
# --- AUTOMATYCZNA KONFIGURACJA I TWORZENIE FOLDERÓW ---
# Ten blok kodu nie wymaga edycji. Automatycznie przetwarza powyższe
# ścieżki, tworzy niezbędne foldery i przygotowuje zmienne dla reszty aplikacji.
#
# Ścieżka do folderu z sesją przeglądarki.
# To tutaj program przechowuje dane logowania, aby nie pytać o nie za każdym razem.
# Zastąp placeholder pełną ścieżką do folderu, np. "C:/Users/TwojaNazwa/google_photos_toolkit/session"
# lub "/home/twojanazwa/google_photos_toolkit/session".
# UŻYTKOWNIK DEFINIUJE SWOJĄ WŁASNĄ, ABSOLUTNĄ ŚCIEŻKĘ
SESSION_DIR = "app_data/session"

# Nazwa pliku bazy danych. To "pamiętnik" robota, w którym zapisuje postęp
# i wszystkie zebrane informacje. Zazwyczaj nie ma potrzeby tego zmieniać.
# Folder na bazę danych
DATABASE_FILE = "app_data/DB/database.db"

# Główny folder, w którym będą zapisywane pobrane zdjęcia i filmy.
# Program automatycznie utworzy w nim podfoldery ROK/MIESIĄC.
# Zastąp placeholder pełną ścieżką, np. "D:/Moje_Zdjecia_Google".
DOWNLOADS_DIR_BASE = "PF"

# Folder na duplikaty z trybu wymuszonego skanowania.
# Gdy uruchomisz skan w trybie "Wymuś pełne odświeżenie", a program napotka
# plik, który już istnieje w głównej bibliotece, zapisze go w tym osobnym
# folderze, zachowując strukturę ROK/MIESIĄC.
FORCED_DUPLICATES_DIR = "PF/_DUPLIKATY_WYMUSZONE"

# Nazwa pliku używanego przez narzędzia skanujące do wczytywania listy
# adresów URL do przetworzenia. Używane przez "Skanuj z pliku".
URL_INPUT_FILE = "ulr/urls_to_scan.txt"

# Plik logu dla RichHandler
LOG_FILENAME = "app_data/dziennik/app.log"

# ##############################################################################
# ===                      SEKCJA 2: GŁÓWNE ZACHOWANIE SKANU                 ===
# ##############################################################################

# Startowy URL. To punkt startowy dla zupełnie nowego skanu.
# WAŻNE: Wklej tutaj link do jednego z Twoich NAJSTARSZYCH zdjęć.
# Dzięki temu program będzie poruszał się od przeszłości do teraźniejszości.
START_URL = "https://photos.google.com/photo/AF1QipMnTUIRsS1Kc93fWxJHIegjVRplzs7RuUtXs5nQ"

# Kierunek nawigacji po galerii.
# 'ArrowLeft'  -> do NOWSZYCH zdjęć (od przeszłości do teraźniejszości). ZALECANE.
# 'ArrowRight' -> do STARSZYCH zdjęć (od teraźniejszości do przeszłości).
DIRECTION_KEY = "ArrowLeft"

# ##############################################################################
# ===                SEKCJA 3: STRATEGIA SILNIKA MASTER                      ===
# ##############################################################################
#
# Te przełączniki kontrolują zaawansowane funkcje zunifikowanego silnika
# "Master", pozwalając dostosować jego działanie do Twoich potrzeb.
#

# Wybierz, w jaki sposób silnik "Master" ma pozyskiwać metadane.
# Dostępne opcje:
#  - 'HYBRID': (ZALECANE) Najpierw próbuje skanera online dla bogatych danych
#              (opisy, albumy, tagi). Jeśli zawiedzie, używa `exiftool`
#              jako niezawodnego "fallbacku". Łączy najlepsze cechy obu metod.
#  - 'EXIF_ONLY': Używa wyłącznie `exiftool` na pobranym pliku. Najszybsza
#                 i najstabilniejsza metoda, ale dostarcza tylko podstawowych
#                 danych (data, model aparatu, GPS z pliku).
#  - 'ONLINE_ONLY': Używa wyłącznie skanera online. Dostarcza najbogatsze
#                   dane, ale jest najbardziej podatna na błędy i zmiany
#                   w wyglądzie strony Google.
METADATA_STRATEGY = "HYBRID"

# Czy w trybie "Napraw błędy..." ma być stosowana zaawansowana metoda
# "potrząśnięcia" stroną (nawigacja przód/tył)?
# True -> Tak, używaj zaawansowanej, potencjalnie skuteczniejszej metody.
# False -> Nie, używaj standardowej, szybszej metody (tylko przeładowanie).
ENABLE_SHAKE_THE_SCAN = True

# Czy program ma blokować zbędne zasoby (obrazki, style, czcionki)?
# True -> Tak, strony będą ładować się szybciej i zużywać mniej transferu.
# False -> Nie, ładuj pełne strony (zalecane dla maksymalnej kompatybilności).
ENABLE_RESOURCE_BLOCKING = True
BLOCKED_RESOURCE_TYPES = [
    "image",
    "media",
    "font",
    "other"
]

# Czy program ma się zatrzymać po zakończeniu automatycznej naprawy błędów
# i czekać na potwierdzenie użytkownika przed wznowieniem głównego skanu?
# True -> Tak, pełna kontrola nad procesem.
# False -> Nie, przechodź płynnie do wznawiania skanu.
ENABLE_PAUSE_AFTER_REPAIR = True

# ##############################################################################
# ===                   SEKCJA 4: CZASY OCZEKIWANIA I WYDAJNOŚĆ              ===
# ##############################################################################
#
# To ustawienia "cierpliwości" robota. Jeśli masz wolny internet lub komputer,
# możesz bezpiecznie zwiększyć te wartości. Wartości podane są w sekundach.
#

# Max czas na załadowanie całej strony po wejściu na nowy adres.
WAIT_FOR_PAGE_LOAD = 120

# Max czas na pojawienie się na stronie przycisku (np. strzałki, menu).
WAIT_FOR_SELECTOR = 60

# Max czas na ROZPOCZĘCIE pobierania pliku po kliknięciu "Pobierz".
WAIT_FOR_DOWNLOAD_START = 300

# Ustaw na True, aby włączyć losowe opóźnienia między pobieraniem plików.
# Pomaga to unikać blokad i symuluje ludzkie zachowanie.
ENABLE_ACTION_DELAY = True

# Zakres opóźnienia w sekundach (np. od 1.0 do 3.5 sekundy).
ACTION_DELAY_RANGE = (1.0, 3.5)

# ##############################################################################
# ===                 SEKCJA 5: ODPORNOŚĆ I ODZYSKIWANIE PO BŁĘDACH          ===
# ##############################################################################
#
# Ustawienia "upartości" robota, gdy strona lub program napotkają problemy.
#

# Ile razy robot ma odświeżyć stronę (F5), gdy nawigacja zawiedzie.
NAV_REFRESH_ATTEMPTS = 5

# Czy po nieudanych odświeżeniach ma próbować nawigacji "na ślepo" klawiaturą?
NAV_BLIND_CLICK_ENABLED = True

# Ile razy robot ma próbować pobrać ten sam plik, zanim oznaczy go jako błąd.
MAX_RETRIES = 5

# Ile razy program ma próbować uruchomić się ponownie po krytycznej awarii.
MAX_RESTARTS_ON_FAILURE = 5

# Ile sekund ma odczekać między kolejnymi restartami.
RESTART_DELAY_SECONDS = 30

# ##############################################################################
# ===                SEKCJA 6: USTAWIENIA PRZEGLĄDARKI I TECHNICZNE          ===
# ##############################################################################
#
# Ustawienia niskopoziomowe. Zmieniaj je tylko, jeśli wiesz, co robisz.
#

# Typ przeglądarki używanej przez Playwright. 'chromium' jest zalecany.
BROWSER_TYPE = "chromium"

# Czy program ma domyślnie działać w tle (bez widocznego okna przeglądarki)?
# True -> Działa w tle (szybciej, mniej zasobów).
# False -> Działa z widocznym oknem (dobre do obserwacji i diagnozy).
DEFAULT_HEADLESS_MODE = True

# Czy symulator kursora ma działać również w trybie cichym (headless)?
# Może pomóc uniknąć wykrycia jako bot na niektórych stronach.
ENABLE_HEADLESS_CURSOR = True

# Argumenty startowe dla przeglądarki. Domyślne wartości są zoptymalizowane
# pod kątem stabilności i unikania wykrycia jako automatyzacja.
BROWSER_ARGS = {
        "chromium": [
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--single-process",
            "--disable-infobars",
            "--disable-extensions",
            "--start-maximized",
            "--window-size=1280,720",
            "--disable-session-crashed-bubble"
        ]
    }

# Selektory CSS - "adresy" przycisków na stronie Google Photos.
# To najbardziej wrażliwa część konfiguracji. Jeśli Google zmieni wygląd
# strony, być może trzeba będzie je zaktualizować za pomocą "Odkrywcy Atrybutów".
INFO_PANEL_BUTTON_SELECTOR = "button[aria-label='Otwórz informacje']"
DOWNLOAD_OPTION_SELECTOR = "div[jscontroller='QTrN6d']"
THREE_DOTS_MENU_SELECTOR = "div[jscontroller='l2hVWe'] button[aria-label='Więcej opcji']"
NAV_ARROW_LEFT_SELECTOR = ".SxgK2b.OQEhnd.eLNT1d"
NAV_ARROW_RIGHT_SELECTOR = ".SxgK2b.Cwtbxf"

# ##############################################################################
# ===                     SEKCJA 7: USTAWIENIA NARZĘDZI POMOCNICZYCH         ===
# ##############################################################################

# --- USTAWIENIA POWIADOMIEŃ TELEGRAM ---
# Wypełnij te pola, aby otrzymywać powiadomienia (funkcja w budowie).

# Token Twojego bota na Telegramie. Uzyskasz go od @BotFather.
TELEGRAM_BOT_TOKEN = "6689110458:AAHBTUoayFunVGw4-xFF4RnascZO0zv2MOA"

# ID Twojego czatu na Telegramie. Możesz je uzyskać np. od bota @userinfobot.
TELEGRAM_CHAT_ID = "5387338482"

# ##############################################################################
# ===                     SEKCJA 8: USTAWIENIA LOGOWANIA                     ===
# ##############################################################################
#
# Ta sekcja kontroluje wbudowany, profesjonalny system logowania oparty
# na module 'logging' i upiększony przez bibliotekę 'rich'.

# Czy logi mają być dodatkowo zapisywane do pliku `log.txt`?
# Bardzo przydatne do diagnozowania problemów po długiej, nienadzorowanej sesji.
LOG_TO_FILE = True

# Główny włącznik/wyłącznik dla całego systemu logowania.
# True -> Logi są aktywne i będą wyświetlane oraz zapisywane zgodnie z poniższymi ustawieniami.
# False -> Cały system logowania jest wyłączony.
LOG_ENABLED = True

# Poziom szczegółowości logów. Zalecane: "INFO".
# Dostępne poziomy:
#  - "DEBUG":   Najbardziej szczegółowe informacje, przydatne do diagnozowania trudnych problemów.
#  - "INFO":    Standardowe informacje o postępie operacji (np. "Rozpoczynam sesję", "Zamykanie...").
#  - "WARNING": Ostrzeżenia o nietypowych, ale niekrytycznych sytuacjach.
#  - "ERROR":   Błędy, które zatrzymały pojedynczą operację, ale niekoniecznie cały program.
#  - "CRITICAL": Błędy krytyczne, które prawdopodobnie zakończą działanie całego programu.
LOG_LEVEL = "INFO"

# Czy logi mają być dodatkowo zapisywane do pliku tekstowego?
# Zastępuje poprzednią zmienną LOG_TO_FILE.
LOG_SAVE_TO_FILE = True

# ##############################################################################
# ===                  SEKCJA 9: USTAWIENIA KOPII ZAPASOWYCH                 ===
# ##############################################################################
#
# Ta sekcja centralizuje konfigurację dla obu typów kopii zapasowych:
#  1. KOPIA DANYCH: Zabezpiecza kluczowe dane użytkownika (baza, sesja).
#  2. KOPIA PROJEKTU: Zabezpiecza cały kod źródłowy aplikacji.
#

# --- USTAWIENIA KOPII ZAPASOWEJ DANYCH (dla Menedżera w menu) ---

# Folder na kopie zapasowe danych, umieszczony wewnątrz app_data.
BACKUP_DIR = "/media/MyDrive/budowa4/app_data/Kopia_Zapasowa_Danych"

# Lista kluczowych plików i folderów DANYCH dołączanych do tej kopii.
# ZMIANA: Używamy PROJECT_ROOT do tworzenia bezpiecznych, absolutnych ścieżek.
FILES_TO_BACKUP = [
    "/media/MyDrive/budowa4/core/config.py",
    "/media/MyDrive/budowa4/app_data/DB/database.db",
    "/media/MyDrive/google_photos_toolkit/ai-w/test/ai-v2/sesje/PF/session"
]


# --- USTAWIENIA KOPII ZAPASOWEJ PROJEKTU (dla Launchera) ---
# NOWOŚĆ: Przeniesione z pliku uruchom.py

# Czy launcher (`uruchom.py`) ma automatycznie tworzyć pełną kopię zapasową
# projektu przy każdym uruchomieniu aplikacji?
# Ta opcja jest zarządzana przez plik settings.json, ale tutaj definiujemy
# jej domyślną wartość, jeśli plik ustawień nie istnieje.
AUTO_BACKUP_ON_START = False

# Konfiguracja dla pełnego backupu projektu, wykonywanego przez launcher.
PROJECT_BACKUP_CONFIG = {
        "ARCHIVE_DIR": "/media/MyDrive/budowa4/app_data/Kopia_Zapasowa_Projektu",
        "BASE_NAME": "projekt_backup",
        "PATTERNS_TO_EXCLUDE": [
            "*.pyc",
            "__pycache__/",
            ".DS_Store",
            "app_data/"
        ]
    }

# ##############################################################################
# ===                     SEKCJA 10: USTAWIENIA AI                           ===
# ##############################################################################
#
# Ta sekcja kontroluje zachowanie modułów sztucznej inteligencji.

# Ścieżka do folderu, w którym będą przechowywane pobrane modele AI (cache).
# Jeśli zostawisz pusty string "", zostanie użyta domyślna lokalizacja
# w folderze domowym użytkownika (np. ~/.cache/huggingface).
# Przykład: "/media/MyDrive/google_photos_toolkit/ai_models_cache"
AI_MODELS_CACHE_DIR = "/media/MyDrive/budowa4/ai_models_cache"

# Próg pewności (0.0 do 1.0) dla Inteligentnego Taggera AI.
# Tag zostanie przypisany tylko, jeśli model AI jest go pewny na ponad 90%.
# Możesz obniżyć tę wartość (np. do 0.8), aby uzyskać więcej tagów,
# ale mogą być one mniej trafne.
AI_TAGGER_CONFIDENCE_THRESHOLD = 0.9

# ##############################################################################
# ===                SEKCJA 11: USTAWIENIA MODUŁÓW DODATKOWYCH              ===
# ##############################################################################
#
# Ta sekcja zawiera ustawienia dla narzędzi, które operują na dodatkowych
# źródłach danych, takich jak lokalne foldery ze zdjęciami.
#

# Lista folderów, które "Lokalny Skaner" będzie pamiętał.
# Możesz ręcznie dodać tutaj ścieżki w formacie:
# LOCAL_SCANNER_DIRECTORIES = [
#     "/home/uzytkownik/Zdjecia/Wakacje_2023",
#     "/mnt/dysk_zewnetrzny/Fotki_Rodzinne"
# ]
LOCAL_SCANNER_DIRECTORIES = [
    "/media/NEXTCLOUD/winek/files/Dev"
]

# Dedykowany folder do przechowywania baz wektorów dla rozpoznawania twarzy.
# Zaleca się, aby był to folder poza główną biblioteką.
# Przykład: "/home/uzytkownik/Aplikacje/FaceDB_Vectors"
FACE_DB_VECTOR_PATH = "/media/MyDrive/budowa4/FaceDB_Vectors"

    
# ##############################################################################
# ===             SEKCJA 12: USTAWIENIA NARZĘDZI INTERAKTYWNYCH              ===
# ##############################################################################
#
# Wybierz domyślną metodę wyświetlania obrazów w narzędziach interaktywnych.
# Dostępne opcje:
#  - 'system': (ZALECANE) Używa domyślnej przeglądarki systemowej.
#              Wymaga przekierowania X11 (ssh -Y) przy pracy zdalnej na Linuksie.
#  - 'eog-unsafe': (DLA ROOT/SSH) Uruchamia przeglądarkę 'eog' z flagą --no-sandbox.
#                  Wymaga 'sudo apt-get install eog' oraz przekierowania X11.
#  - 'server': Uruchamia tymczasowy serwer WWW i podaje link do wklejenia
#              w przeglądarce. Działa wszędzie, ale wymaga ręcznego kopiowania.
#  - 'sixel':  Próbuje wyświetlić obraz bezpośrednio w terminalu. Wymaga
#              kompatybilnego emulatora terminala (np. iTerm2, Kitty, WezTerm).
IMAGE_VIEWER_MODE = "eog-unsafe"
