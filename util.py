# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import sys

def resolve_here(p: str) -> Path:
    base = Path(__file__).resolve().parent
    q = Path(p)
    return q if q.is_absolute() else (base / q)

def ensure_console_utf8():
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

def pause_if_needed(should_pause: bool = False):
    if not should_pause:
        return
    try:
        input("\nPremi Invio per uscire...")
    except Exception:
        pass
