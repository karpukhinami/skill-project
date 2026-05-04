-- 002_frp_topics_subject_id.sql
-- Перевод frp_topics.subject (TEXT) -> subject_id (FK на subjects).
-- Старое значение сохраняем в subject_name (для отладки/обратной совместимости).

BEGIN;

-- 1) Переименовать колонку subject -> subject_name (если ещё не делали).
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'frp_topics' AND column_name = 'subject'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'frp_topics' AND column_name = 'subject_name'
  ) THEN
    EXECUTE 'ALTER TABLE frp_topics RENAME COLUMN subject TO subject_name';
  END IF;
END $$;

-- 2) Добавить subject_id (если нет).
ALTER TABLE frp_topics
  ADD COLUMN IF NOT EXISTS subject_id INTEGER NULL;

-- 3) Заполнить subject_id по совпадению subject_name с subjects.name_norm.
--    Частный кейс: "вероятность" -> "вероятность и статистика".
UPDATE frp_topics f
SET subject_id = s.id
FROM subjects s
WHERE f.subject_id IS NULL
  AND s.name_norm = CASE
    WHEN normalize_term(f.subject_name) = normalize_term('вероятность')
      THEN normalize_term('вероятность и статистика')
    ELSE normalize_term(f.subject_name)
  END;

-- 4) Проверка, что не осталось строк без subject_id (иначе лучше остановить миграцию).
DO $$
DECLARE missing_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO missing_count
  FROM frp_topics
  WHERE subject_id IS NULL;

  IF missing_count > 0 THEN
    RAISE EXCEPTION 'frp_topics: не удалось сопоставить subject_name -> subjects.id для % строк. Проверьте значения frp_topics.subject_name.', missing_count;
  END IF;
END $$;

-- 5) FK на subjects.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'frp_topics_subject_id_fk'
  ) THEN
    EXECUTE 'ALTER TABLE frp_topics ADD CONSTRAINT frp_topics_subject_id_fk FOREIGN KEY (subject_id) REFERENCES subjects(id) ON DELETE RESTRICT';
  END IF;
END $$;

-- 6) subject_id делаем NOT NULL (после успешного бэкфилла).
ALTER TABLE frp_topics
  ALTER COLUMN subject_id SET NOT NULL;

-- 7) Новая уникальность (grade_class, subject_id, section, topic).
--    Старую уникальность на subject_name пытаемся убрать (динамически), чтобы не держать две.
DO $$
DECLARE con_to_drop TEXT;
BEGIN
  -- ищем UQ, где участвуют grade_class + subject_name + section + topic (порядок может отличаться)
  SELECT c.conname INTO con_to_drop
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  WHERE t.relname = 'frp_topics'
    AND c.contype = 'u'
    AND (
      pg_get_constraintdef(c.oid) ILIKE '%grade_class%'
      AND pg_get_constraintdef(c.oid) ILIKE '%subject_name%'
      AND pg_get_constraintdef(c.oid) ILIKE '%section%'
      AND pg_get_constraintdef(c.oid) ILIKE '%topic%'
    )
  LIMIT 1;

  IF con_to_drop IS NOT NULL THEN
    EXECUTE format('ALTER TABLE frp_topics DROP CONSTRAINT %I', con_to_drop);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS frp_topics_grade_subject_section_topic_uk
  ON frp_topics(grade_class, subject_id, section, topic);

COMMIT;

