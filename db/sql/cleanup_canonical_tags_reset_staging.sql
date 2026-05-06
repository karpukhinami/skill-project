-- Очистка канонического словаря после синонимизации + сброс меток в первичном стейджинге.
-- Первичное извлечение — tag_topic_terms: строки НЕ удаляются, только сбрасывается «обработано».
-- Канонический словарь — tags / tag_aliases / tag_subjects (плюс skill_tags / content_tags при наличии ссылок).
--
-- Замените литералы ниже (123, 456) на ваши id и выполните блок целиком в транзакции.
--
-- Узнать id:
--   SELECT id, name FROM subjects ORDER BY id;
--   SELECT id, status, created_at FROM tag_extraction_runs ORDER BY id DESC LIMIT 10;

BEGIN;

-- 1) Снять пометки обработки со всех терминов выбранного прогона извлечения
UPDATE tag_topic_terms
SET processed_at = NULL,
    processed_run_id = NULL
WHERE run_id = 123;  -- <-- tag_extraction_runs.id

-- Узкий сброс только по части предметов (раскомментируйте при необходимости):
-- AND subject_id = ANY (ARRAY[10, 11, 12]::int[]);

-- 2) Удалить прогоны синонимизации для этого extraction (чанки удалятся каскадом)
DELETE FROM tag_syn_runs
WHERE extraction_run_id = 123;  -- тот же run_id, что выше

-- 3) Удалить канонические теги только у выбранного канонического предмета (например, математика)
--    Не затрагивает tag_topic_terms и не трогает tags других subject_id.

DELETE FROM skill_tags
WHERE tag_id IN (SELECT id FROM tags WHERE subject_id = 456);  -- <-- subjects.id канона

DELETE FROM content_tags
WHERE tag_id IN (SELECT id FROM tags WHERE subject_id = 456);

DELETE FROM tag_aliases
WHERE tag_id IN (SELECT id FROM tags WHERE subject_id = 456);

DELETE FROM tag_subjects
WHERE tag_id IN (SELECT id FROM tags WHERE subject_id = 456);

UPDATE tags
SET parent_id = NULL
WHERE subject_id = 456;

DELETE FROM tags
WHERE subject_id = 456;

-- Вариант «мягче», если в этом subject уже есть «не синонимизационные» теги:
-- замените последний DELETE на:
-- DELETE FROM tags WHERE subject_id = 456 AND is_new = TRUE;
-- (и в DELETE выше по skill_tags/content_tags/tag_aliases/tag_subjects ограничьте подзапрос тем же AND is_new = TRUE)

COMMIT;
