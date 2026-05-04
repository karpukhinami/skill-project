-- 005_tag_staging.sql
-- Стейджинг для первичного извлечения тегов до нормализации/канонизации.
-- Требование: хранить только "тема -> термин" (каждый термин отдельной строкой),
-- без привязки к skill_defs/content_element_defs, чтобы не раздувать объём.

BEGIN;

-- СТАРТ С ЧИСТОГО ЛИСТА: удаляем старые стейджинг-таблицы (если были).
DROP TABLE IF EXISTS tag_raw_assignments;
DROP TABLE IF EXISTS tag_raw_terms;
DROP TABLE IF EXISTS tag_topic_terms;
DROP TABLE IF EXISTS tag_extraction_runs;

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

-- Термины по теме: одна строка = один термин в рамках (run_id, frp_topic_id).
CREATE TABLE IF NOT EXISTS tag_topic_terms (
  id            SERIAL PRIMARY KEY,
  run_id        INTEGER NOT NULL,
  subject_id    INTEGER NOT NULL,
  frp_topic_id  INTEGER NOT NULL,
  term          TEXT NOT NULL,
  term_norm     TEXT GENERATED ALWAYS AS (normalize_term(term)) STORED,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT tag_topic_terms_run_fk FOREIGN KEY (run_id) REFERENCES tag_extraction_runs(id) ON DELETE CASCADE,
  CONSTRAINT tag_topic_terms_subject_fk FOREIGN KEY (subject_id) REFERENCES subjects(id) ON DELETE RESTRICT,
  CONSTRAINT tag_topic_terms_frp_topic_fk FOREIGN KEY (frp_topic_id) REFERENCES frp_topics(id) ON DELETE RESTRICT
);

-- Дедуп: один и тот же термин в рамках темы хранится один раз.
CREATE UNIQUE INDEX IF NOT EXISTS tag_topic_terms_uk
  ON tag_topic_terms(run_id, frp_topic_id, term_norm);

CREATE INDEX IF NOT EXISTS tag_topic_terms_run_topic_idx
  ON tag_topic_terms(run_id, frp_topic_id);

COMMIT;

