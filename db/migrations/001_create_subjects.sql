-- 001_create_subjects.sql
-- Справочник предметов + иерархия через parent_id.

BEGIN;

-- Нормализация: нижний регистр + схлопывание пробелов.
CREATE OR REPLACE FUNCTION normalize_term(t TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT regexp_replace(lower(btrim(t)), '\s+', ' ', 'g')
$$;

CREATE TABLE IF NOT EXISTS subjects (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  name_norm   TEXT GENERATED ALWAYS AS (normalize_term(name)) STORED,
  parent_id   INTEGER NULL,
  is_archived BOOLEAN NOT NULL DEFAULT FALSE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT subjects_parent_fk FOREIGN KEY (parent_id) REFERENCES subjects(id) ON DELETE RESTRICT
);

-- Уникальность по нормализованному имени.
CREATE UNIQUE INDEX IF NOT EXISTS subjects_name_norm_uk
  ON subjects(name_norm);

-- Быстрые выборки детей.
CREATE INDEX IF NOT EXISTS subjects_parent_id_idx
  ON subjects(parent_id);

-- Сидинг фиксированного списка (в нижнем регистре).
INSERT INTO subjects (name, parent_id)
VALUES
  ('алгебра', NULL),
  ('алгебра и начала математического анализа', NULL),
  ('английский язык', NULL),
  ('биология', NULL),
  ('вероятность и статистика', NULL),
  ('география', NULL),
  ('геометрия', NULL),
  ('информатика', NULL),
  ('испанский язык', NULL),
  ('история', NULL),
  ('литература', NULL),
  ('математика', NULL),
  ('немецкий язык', NULL),
  ('обществознание', NULL),
  ('русский язык', NULL),
  ('физика', NULL),
  ('французский язык', NULL),
  ('химия', NULL)
ON CONFLICT (name_norm) DO NOTHING;

-- Иерархия: родитель "математика" для 4 веток.
WITH m AS (
  SELECT id FROM subjects WHERE name_norm = normalize_term('математика') LIMIT 1
)
UPDATE subjects s
SET parent_id = (SELECT id FROM m)
WHERE s.name_norm IN (
  normalize_term('алгебра'),
  normalize_term('алгебра и начала математического анализа'),
  normalize_term('геометрия'),
  normalize_term('вероятность и статистика')
)
AND s.parent_id IS DISTINCT FROM (SELECT id FROM m);

COMMIT;

