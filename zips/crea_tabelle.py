# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys
from pathlib import Path

# Forza la CWD alla cartella dello script per coerenza dei path
os.chdir(Path(__file__).resolve().parent)

from util import ensure_console_utf8, pause_if_needed
from menu import main_menu

__CREA_TABELLE_VERSION__ = "v7"

def main() -> int:
    ensure_console_utf8()
    print(f"[crea_tabelle] versione: {__CREA_TABELLE_VERSION__} — python: {sys.version.split()[0]} — exe: {sys.executable}")
    main_menu()
    return 0

if __name__ == "__main__":
    sys.exit(main())
