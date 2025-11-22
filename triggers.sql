-- ======================================================
-- 🔍 TEMPLATE TRIGGER AUDIT COMPLETO
--   Include: INSERT, UPDATE, DELETE + set_logseq_random
--   Placeholders da sostituire via Python:
--   {table}, {trigger_name_insert}, {trigger_name_update},
--   {trigger_name_delete}, {trigger_name_logseq},
--   {json_new_pairs}, {json_old_pairs}, {logseq_col}
-- ======================================================

-- === AUDIT INSERT =====================================
CREATE TRIGGER {trigger_name_insert}
AFTER INSERT ON "{table}"
BEGIN
  INSERT INTO audit_dml(ts, action, table_name, rowid, old_values, new_values)
  VALUES (
    CURRENT_TIMESTAMP,
    'INSERT',
    "{table}",
    NEW.rowid,
    NULL,
    json_object({json_new_pairs})
  );
END;

-- === AUDIT UPDATE =====================================
CREATE TRIGGER {trigger_name_update}
AFTER UPDATE ON "{table}"
BEGIN
  INSERT INTO audit_dml(ts, action, table_name, rowid, old_values, new_values)
  VALUES (
    CURRENT_TIMESTAMP,
    'UPDATE',
    "{table}",
    NEW.rowid,
    json_object({json_old_pairs}),
    json_object({json_new_pairs})
  );
END;

-- === AUDIT DELETE =====================================
CREATE TRIGGER {trigger_name_delete}
AFTER DELETE ON "{table}"
BEGIN
  INSERT INTO audit_dml(ts, action, table_name, rowid, old_values, new_values)
  VALUES (
    CURRENT_TIMESTAMP,
    'DELETE',
    "{table}",
    OLD.rowid,
    json_object({json_old_pairs}),
    NULL
  );
END;

