-- 003_add_parent_id_defs.sql
-- Добавляем parent_id в skill_defs и content_element_defs.

BEGIN;

ALTER TABLE skill_defs
  ADD COLUMN IF NOT EXISTS parent_id INTEGER NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'skill_defs_parent_id_fk'
  ) THEN
    EXECUTE 'ALTER TABLE skill_defs ADD CONSTRAINT skill_defs_parent_id_fk FOREIGN KEY (parent_id) REFERENCES skill_defs(id) ON DELETE SET NULL';
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS skill_defs_parent_id_idx
  ON skill_defs(parent_id);

ALTER TABLE content_element_defs
  ADD COLUMN IF NOT EXISTS parent_id INTEGER NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'content_element_defs_parent_id_fk'
  ) THEN
    EXECUTE 'ALTER TABLE content_element_defs ADD CONSTRAINT content_element_defs_parent_id_fk FOREIGN KEY (parent_id) REFERENCES content_element_defs(id) ON DELETE SET NULL';
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS content_element_defs_parent_id_idx
  ON content_element_defs(parent_id);

COMMIT;

