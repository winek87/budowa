# plik: core/attribute_explorer/utils.py
# Wersja 1.0 - Dedykowane narzędzia dla Odkrywcy Atrybutów.
# Opis: Ten moduł zawiera funkcje pomocnicze, w tym zaawansowaną
#       obsługę klawiszy specjalnych dla interaktywnej przeglądarki.
# -*- coding: utf-8 -*-

import sys
import time

try:
    # Dla systemów Unix-podobnych (Linux, macOS)
    import tty
    import termios
    import fcntl
    import os
    IS_WINDOWS = False
except ImportError:
    # Dla systemu Windows
    import msvcrt
    IS_WINDOWS = True

def get_key() -> str:
    """
    Niezawodna funkcja do odczytu pojedynczego klawisza, działająca
    zarówno na systemach Windows, jak i Unix-podobnych, z obsługą
    rozszerzonych sekwencji escape.
    """
    if IS_WINDOWS:
        char = msvcrt.getch()
        if char in b'\xe0\x00':
            char = msvcrt.getch()
            key_map = {
                b'H': "UP", b'P': "DOWN", b'K': "LEFT", b'M': "RIGHT",
                b'I': "PAGE_UP", b'Q': "PAGE_DOWN",
                b'G': "HOME", b'O': "END",
            }
            return key_map.get(char, f"WIN_SPECIAL_{char}")
        try:
             decoded_char = char.decode('utf-8', errors='ignore')
             if decoded_char == '\r': return "ENTER"
             return decoded_char
        except UnicodeDecodeError:
             return ""
    else:
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            char = sys.stdin.read(1)
            if char == '\x1b':
                orig_fl = fcntl.fcntl(fd, fcntl.F_GETFL)
                fcntl.fcntl(fd, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)
                time.sleep(0.005) 
                seq = ""
                while True:
                    try:
                        next_char = sys.stdin.read(1)
                        if not next_char: break
                        seq += next_char
                    except IOError:
                        break
                fcntl.fcntl(fd, fcntl.F_SETFL, orig_fl)

                key_map = {
                    '[A': "UP", '[B': "DOWN", '[C': "RIGHT", '[D': "LEFT",
                    '[5~': "PAGE_UP", '[6~': "PAGE_DOWN",
                    '[H': "HOME", 'OH': "HOME",
                    '[F': "END", 'OF': "END",
                }
                return key_map.get(seq, f"UNIX_SPECIAL_{seq}")
                
            elif char == '\r':
                return "ENTER"
            return char
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
