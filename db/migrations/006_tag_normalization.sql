-- 006_tag_normalization.sql
-- Поддержка нормализации: связь тегов с несколькими предметами + прогресс чанков.

BEGIN;

-- 1) tags могут встречаться в нескольких предметах: m2m.
CREATE TABLE IF NOT EXISTS tag_subjects (
  tag_id     INTEGER NOT NULL REFERENCES tags(id) ON DELETE RESTRICT,
  subject_id INTEGER NOT NULL REFERENCES subjects(id) ON DELETE RESTRICT,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tag_id, subject_id)
);

CREATE INDEX IF NOT EXISTS tag_subjects_subject_id_idx
  ON tag_subjects(subject_id);

-- 2) Отметки обработки в стейджинге (не удаляем, просто помечаем).
ALTER TABLE tag_topic_terms
  ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS processed_run_id INTEGER NULL;

CREATE INDEX IF NOT EXISTS tag_topic_terms_processed_idx
  ON tag_topic_terms(run_id, subject_id, processed_at);

-- 3) Прогон синонимизации (root_subject_id + extraction_run_id).
CREATE TABLE IF NOT EXISTS tag_syn_runs (
  id                SERIAL PRIMARY KEY,
  extraction_run_id INTEGER NOT NULL REFERENCES tag_extraction_runs(id) ON DELETE CASCADE,
  root_subject_id   INTEGER NOT NULL REFERENCES subjects(id) ON DELETE RESTRICT,
  chunk_size        INTEGER NOT NULL DEFAULT 500,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  status            TEXT NOT NULL DEFAULT 'running', -- running / paused / done / failed
  note              TEXT NULL
);

CREATE INDEX IF NOT EXISTS tag_syn_runs_extraction_root_idx
  ON tag_syn_runs(extraction_run_id, root_subject_id);

-- 4) Чанки синонимизации: храним вход и выход, чтобы можно было продолжить после сбоя.
CREATE TABLE IF NOT EXISTS tag_syn_chunks (
  id          SERIAL PRIMARY KEY,
  syn_run_id  INTEGER NOT NULL REFERENCES tag_syn_runs(id) ON DELETE CASCADE,
  phase       TEXT NOT NULL DEFAULT 'chunk', -- 'chunk' | 'merge'
  chunk_idx   INTEGER NOT NULL,
  input_terms JSONB NOT NULL,
  result_json JSONB NULL,
  status      TEXT NOT NULL DEFAULT 'pending', -- pending / done / failed
  error_text  TEXT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  applied_at  TIMESTAMPTZ NULL,
  UNIQUE (syn_run_id, phase, chunk_idx)
);

CREATE INDEX IF NOT EXISTS tag_syn_chunks_run_status_idx
  ON tag_syn_chunks(syn_run_id, status);

-- 5) Прогон иерархии (после синонимизации).
CREATE TABLE IF NOT EXISTS tag_hierarchy_runs (
  id                SERIAL PRIMARY KEY,
  root_subject_id   INTEGER NOT NULL REFERENCES subjects(id) ON DELETE RESTRICT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  status            TEXT NOT NULL DEFAULT 'running',
  result_json       JSONB NULL,
  error_text        TEXT NULL
);

COMMIT;

