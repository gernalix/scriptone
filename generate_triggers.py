# -*- coding: utf-8 -*-
"""
generate_triggers.py — rigenera i trigger di audit per tutte le tabelle utente.

Usa la stessa logica di crea_tabelle.py: template triggers.sql, JSON con tutte le
colonne, nomi di trigger semplificati e tabelle di audit create automaticamente.
"""

from crea_tabelle import connect_db, list_tables, apply_triggers_to_table


def main() -> None:
    with connect_db() as conn:
        tables = list_tables(conn)
        for t in tables:
            if t in ("audit_dml", "audit_schema"):
                continue
            print(f"[INFO] Aggiorno trigger per tabella '{t}'...")
            apply_triggers_to_table(conn, t)
    print("[FINE] Rigenerazione trigger completata.")


if __name__ == "__main__":
    main()
