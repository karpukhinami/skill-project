-- 004_tags.sql
-- Таблицы тегов + алиасы + связи с навыками/ЭС.
-- Нормализация терминов делается в БД (триггерами) через normalize_term().

BEGIN;

-- 1) tags
CREATE TABLE IF NOT EXISTS tags (
  id          SERIAL PRIMARY KEY,
  subject_id  INTEGER NOT NULL,
  tag         TEXT NOT NULL,
  parent_id   INTEGER NULL,
  is_new      BOOLEAN NOT NULL DEFAULT TRUE,
  is_archived BOOLEAN NOT NULL DEFAULT FALSE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  reviewed_at TIMESTAMPTZ NULL,
  CONSTRAINT tags_subject_fk FOREIGN KEY (subject_id) REFERENCES subjects(id) ON DELETE RESTRICT
);

-- Композитная уникальность, чтобы можно было сослаться (subject_id, id).
CREATE UNIQUE INDEX IF NOT EXISTS tags_subject_id__id_uk
  ON tags(subject_id, id);

-- parent_id должен быть в рамках того же subject_id:
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'tags_parent_same_subject_fk'
  ) THEN
    EXECUTE 'ALTER TABLE tags ADD CONSTRAINT tags_parent_same_subject_fk FOREIGN KEY (subject_id, parent_id) REFERENCES tags(subject_id, id) ON DELETE RESTRICT';
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS tags_parent_id_idx
  ON tags(parent_id);

CREATE INDEX IF NOT EXISTS tags_subject_id_idx
  ON tags(subject_id);

-- Уникальность канонического тега в пределах предмета среди неархивных.
CREATE UNIQUE INDEX IF NOT EXISTS tags_subject_tag_uk_active
  ON tags(subject_id, tag)
  WHERE is_archived = FALSE;

-- 2) tag_aliases
CREATE TABLE IF NOT EXISTS tag_aliases (
  id          SERIAL PRIMARY KEY,
  subject_id  INTEGER NOT NULL,
  alias       TEXT NOT NULL,
  tag_id      INTEGER NOT NULL,
  is_archived BOOLEAN NOT NULL DEFAULT FALSE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- alias должен указывать на тег того же предмета:
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'tag_aliases_tag_same_subject_fk'
  ) THEN
    EXECUTE 'ALTER TABLE tag_aliases ADD CONSTRAINT tag_aliases_tag_same_subject_fk FOREIGN KEY (subject_id, tag_id) REFERENCES tags(subject_id, id) ON DELETE RESTRICT';
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS tag_aliases_tag_id_idx
  ON tag_aliases(tag_id);

CREATE UNIQUE INDEX IF NOT EXISTS tag_aliases_subject_alias_uk_active
  ON tag_aliases(subject_id, alias)
  WHERE is_archived = FALSE;

-- 3) связи skill_tags / content_tags
CREATE TABLE IF NOT EXISTS skill_tags (
  skill_id INTEGER NOT NULL,
  tag_id   INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (skill_id, tag_id),
  CONSTRAINT skill_tags_skill_fk FOREIGN KEY (skill_id) REFERENCES skill_defs(id) ON DELETE CASCADE,
  CONSTRAINT skill_tags_tag_fk   FOREIGN KEY (tag_id)   REFERENCES tags(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS skill_tags_tag_id_idx
  ON skill_tags(tag_id);

CREATE TABLE IF NOT EXISTS content_tags (
  content_id INTEGER NOT NULL,
  tag_id     INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (content_id, tag_id),
  CONSTRAINT content_tags_content_fk FOREIGN KEY (content_id) REFERENCES content_element_defs(id) ON DELETE CASCADE,
  CONSTRAINT content_tags_tag_fk     FOREIGN KEY (tag_id)     REFERENCES tags(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS content_tags_tag_id_idx
  ON content_tags(tag_id);

-- 4) Нормализация tag/alias в БД (before insert/update).
CREATE OR REPLACE FUNCTION tags_normalize_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.tag IS NOT NULL THEN
    NEW.tag := normalize_term(NEW.tag);
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION tag_aliases_normalize_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.alias IS NOT NULL THEN
    NEW.alias := normalize_term(NEW.alias);
  END IF;
  RETURN NEW;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'tags_normalize_biu') THEN
    EXECUTE 'CREATE TRIGGER tags_normalize_biu BEFORE INSERT OR UPDATE ON tags FOR EACH ROW EXECUTE FUNCTION tags_normalize_trigger()';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'tag_aliases_normalize_biu') THEN
    EXECUTE 'CREATE TRIGGER tag_aliases_normalize_biu BEFORE INSERT OR UPDATE ON tag_aliases FOR EACH ROW EXECUTE FUNCTION tag_aliases_normalize_trigger()';
  END IF;
END $$;

-- 5) Запрет DELETE (вместо этого is_archived=true).
CREATE OR REPLACE FUNCTION forbid_delete_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'DELETE запрещён (используйте is_archived=true). Таблица: %', TG_TABLE_NAME;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'tags_forbid_delete') THEN
    EXECUTE 'CREATE TRIGGER tags_forbid_delete BEFORE DELETE ON tags FOR EACH ROW EXECUTE FUNCTION forbid_delete_trigger()';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'tag_aliases_forbid_delete') THEN
    EXECUTE 'CREATE TRIGGER tag_aliases_forbid_delete BEFORE DELETE ON tag_aliases FOR EACH ROW EXECUTE FUNCTION forbid_delete_trigger()';
  END IF;
END $$;

-- 6) Запрет менять tag после принятия (is_new=false).
CREATE OR REPLACE FUNCTION forbid_rename_after_review_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF (OLD.is_new = FALSE) AND (NEW.tag IS DISTINCT FROM OLD.tag) THEN
    RAISE EXCEPTION 'Нельзя менять tag после принятия (is_new=false).';
  END IF;
  RETURN NEW;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'tags_forbid_rename_after_review') THEN
    EXECUTE 'CREATE TRIGGER tags_forbid_rename_after_review BEFORE UPDATE ON tags FOR EACH ROW EXECUTE FUNCTION forbid_rename_after_review_trigger()';
  END IF;
END $$;

COMMIT;

