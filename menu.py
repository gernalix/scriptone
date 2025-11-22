# -*- coding: utf-8 -*-
from __future__ import annotations

from memento_import import memento_import_batch
from util import resolve_here
from table_editor import menu_tabelle_sqlite


def _ask(prompt: str, default: str = "") -> str:
    v = input(f"{prompt} [{default}]: ").strip()
    return v or default


def _menu_import_batch() -> None:
    print("\n============== MEMENTO – Import batch da YAML/INI ============== ")
    db_path = _ask("Percorso DB sqlite", "noutput.db")
    batch = _ask("Percorso batch (.ini o .yaml)", "memento_import.ini")
    db_path = str(resolve_here(db_path))
    batch = str(resolve_here(batch))
    n = memento_import_batch(db_path, batch)
    print(f"\n[import] Righe totali importate: {n}")


def _menu_memento_cloud() -> None:
    while True:
        print("\n============== MEMENTO CLOUD ============== ")
        print("1) Elenca librerie (SDK)")
        print("2) Deduci mappatura campi (SDK)")
        print("3) Mostra 1 entry grezza (SDK)")
        print("4) Importa libreria (auto)")
        print("5) Importa batch da YAML/INI")
        print("0) Indietro / Esci")
        sel = _ask("Scelta", "0").strip()
        if sel == "5":
            _menu_import_batch()
        elif sel == "0" or sel.lower() in ("q", "quit", "exit"):
            break
        elif sel in {"1", "2", "3", "4"}:
            print("Funzione non implementata in questa build: al momento è disponibile solo l'opzione 5.")
        else:
            print("Scelta non valida.")


def main_menu() -> None:
    while True:
        print("\n============== MENU PRINCIPALE ============== ")
        print("1) Gestione tabelle SQLite")
        print("2) Memento cloud")
        print("0) Esci")
        sel = _ask("Scelta", "0").strip()
        if sel == "1":
            menu_tabelle_sqlite()
        elif sel == "2":
            _menu_memento_cloud()
        elif sel == "0" or sel.lower() in ("q", "quit", "exit"):
            print("Uscita dal menu.")
            break
        else:
            print("Scelta non valida.")
