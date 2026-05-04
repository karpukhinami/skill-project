-- 005_tag_staging.sql
-- Стейджинг для первичного извлечения тегов до нормализации/канонизации.
-- ВАЖНО: tag_aliases требует tag_id (FK), поэтому «сырые» термины складываем отдельно.

BEGIN;

CREATE TABLE IF NOT EXISTS tag_extraction_runs (
  id          SERIAL PRIMARY KEY,
  subject_id  INTEGER NOT NULL,
  program     TEXT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  status      TEXT NOT NULL DEFAULT 'running',
  note        TEXT NULL,
  CONSTRAINT tag_extraction_runs_subject_fk FOREIGN KEY (subject_id) REFERENCES subjects(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS tag_extraction_runs_subject_id_idx
  ON tag_extraction_runs(subject_id);


CREATE TABLE IF NOT EXISTS tag_raw_terms (
  id          SERIAL PRIMARY KEY,
  run_id      INTEGER NOT NULL,
  subject_id  INTEGER NOT NULL,
  term        TEXT NOT NULL,
  term_norm   TEXT GENERATED ALWAYS AS (normalize_term(term)) STORED,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT tag_raw_terms_run_fk FOREIGN KEY (run_id) REFERENCES tag_extraction_runs(id) ON DELETE CASCADE,
  CONSTRAINT tag_raw_terms_subject_fk FOREIGN KEY (subject_id) REFERENCES subjects(id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS tag_raw_terms_run_subject_term_uk
  ON tag_raw_terms(run_id, subject_id, term_norm);

CREATE INDEX IF NOT EXISTS tag_raw_terms_run_id_idx
  ON tag_raw_terms(run_id);


CREATE TABLE IF NOT EXISTS tag_raw_assignments (
  id            SERIAL PRIMARY KEY,
  run_id        INTEGER NOT NULL,
  subject_id    INTEGER NOT NULL,
  frp_topic_id  INTEGER NOT NULL,
  source_table  TEXT NOT NULL,
  source_id     INTEGER NOT NULL,
  term          TEXT NOT NULL,
  term_norm     TEXT GENERATED ALWAYS AS (normalize_term(term)) STORED,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT tag_raw_assignments_run_fk FOREIGN KEY (run_id) REFERENCES tag_extraction_runs(id) ON DELETE CASCADE,
  CONSTRAINT tag_raw_assignments_subject_fk FOREIGN KEY (subject_id) REFERENCES subjects(id) ON DELETE RESTRICT,
  CONSTRAINT tag_raw_assignments_frp_topic_fk FOREIGN KEY (frp_topic_id) REFERENCES frp_topics(id) ON DELETE RESTRICT
);

-- Запрещаем неожиданные значения источника (чтобы не размазалось).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'tag_raw_assignments_source_table_chk') THEN
    EXECUTE 'ALTER TABLE tag_raw_assignments
      ADD CONSTRAINT tag_raw_assignments_source_table_chk
      CHECK (source_table IN (''skill_defs'', ''content_element_defs''))';
  END IF;
END $$;

-- Дедуп: одна и та же связка "запись -> термин" в рамках run_id хранится один раз.
CREATE UNIQUE INDEX IF NOT EXISTS tag_raw_assignments_uk
  ON tag_raw_assignments(run_id, source_table, source_id, term_norm);

CREATE INDEX IF NOT EXISTS tag_raw_assignments_run_topic_idx
  ON tag_raw_assignments(run_id, frp_topic_id);

COMMIT;

