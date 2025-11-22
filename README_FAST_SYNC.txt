FAST SYNC FOR MEMENTO — HOW TO

1) Drop these files into your existing folder (overwrite the same-name files):
   - memento_sdk.py
   - memento_import.py
   - db_utils.py
   - check_memento_api.py

2) In each section of memento_import.ini / .yaml you can add:
     sync = incremental
   (default is incremental; set sync=full to force a full rescan).

3) New SQLite table:
   - memento_sync(library_id PRIMARY KEY, last_modified_remote TEXT, last_run_utc TEXT)
   The importer updates last_modified_remote with the max timestamp seen (based on tempo_col or common fields).

4) Speedups:
   - Uses updatedAfter/createdAfter if the API accepts them (auto-detected per library).
   - Falls back to paged scan with sorting and tokens.
   - Batch inserts via executemany in one transaction with WAL journal and proper indexes.

5) Quick probe:
   python check_memento_api.py memento_import.ini

6) Run import (as before via your menu or directly):
   python -c "from memento_import import memento_import_batch as run; print(run('noutput.db','memento_import.ini'))"
