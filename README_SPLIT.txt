Scriptone (split v2) — Settings YAML + Menu 1–4
=================================================
Novità
- Tutti i default sono ora in **settings.yaml** (db path, API Memento, token, YAML batch path).
- Implementate le voci **1–4** con un client SDK minimale:
  1) Elenco librerie (GET /libraries)
  2) Deduci mappatura campi (prende 1 entry e deduce tipi grossolani)
  3) Mostra 1 entry grezza
  4) Import libreria (auto) in una tabella con nome = id libreria, con fallback ext_id/memento_id
- Opzione **5** continua a usare `memento_import.yaml` per batch controllati.

Setup
1) Installa dipendenze: `pip install pyyaml requests`
2) Compila `settings.yaml` (token Memento e, se vuoi, default_library_id)
3) Avvia `crea_tabelle.py` (doppio clic o `python crea_tabelle.py --nopause`)

Note
- Il codice dell'SDK è volutamente minimale: adatta gli endpoint al formato reale della tua API Memento (path, paginazione).
- L'**opzione B** (fallback/auto-migrazione `ext_id` ↔ `memento_id`) è attiva in tutto il flusso import.
