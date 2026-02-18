# -*- coding: utf-8 -*-
"""Автономное веб-приложение: извлечение данных из ФРП и кодификатора"""

import pandas as pd
import json
import re
import io
import copy
import streamlit as st
import requests
from typing import Dict, List, Tuple, Optional

st.set_page_config(page_title="Извлечение ФРП", layout="wide")

# --- Вспомогательные функции для JSON ---

def get_json_info(data_dict):
    """Возвращает информацию о JSON: тип, предметы, классы."""
    if not data_dict:
        return {'type': '?', 'subjects': [], 'classes': [], 'count': 0}
    sample_key = next(iter(data_dict.keys()), '')
    item_type = 'навыки' if 'skill' in sample_key.lower() else 'содержание'
    subjects = set()
    classes = set()
    for item in data_dict.values():
        if isinstance(item, dict):
            s = item.get('subject', '')
            if s:
                subjects.add(str(s).strip())
            c = item.get('class', '')
            if c:
                classes.add(str(c).strip())
    return {
        'type': item_type,
        'subjects': sorted(subjects) if subjects else ['—'],
        'classes': sorted(classes, key=lambda x: int(x) if str(x).isdigit() else 0),
        'count': len(data_dict)
    }


# --- Логика извлечения ФРП (таблица) ---

def detect_table_structure(df):
    num_cols = len(df.columns)
    if num_cols < 5:
        return {'type': '4_columns', 'col_section': None, 'col_topic': 0,
                'col_hours': 1, 'col_content': 2, 'col_skills': 3}
    last_col = df.iloc[:, 4]
    non_empty = last_col.dropna()
    if len(non_empty) > 10:
        return {'type': '5_columns', 'col_section': 0, 'col_topic': 1,
                'col_hours': 2, 'col_content': 3, 'col_skills': 4}
    return {'type': '4_columns', 'col_section': None, 'col_topic': 0,
            'col_hours': 1, 'col_content': 2, 'col_skills': 3}


def extract_frp_from_df(df, subject, program):
    """Извлекает навыки и содержание из DataFrame (универсально для Excel и PDF)."""
    structure = detect_table_structure(df)
    
    header_row = None
    for idx in range(min(30, len(df))):
        row_text = ' '.join([str(x).lower() for x in df.iloc[idx] if pd.notna(x)])
        if 'содержан' in row_text or 'деятельност' in row_text:
            header_row = idx
            break
    if header_row is None:
        header_row = 0
    
    current_class = None
    for idx in range(header_row):
        col0_text = str(df.iloc[idx, 0])
        if col0_text.lower().startswith('федера'):
            continue
        match = re.search(r'(\d+)\s*класс', col0_text.lower())
        if match:
            current_class = match.group(1)
            break
    
    skills_list = []
    content_list = []
    current_section = ""
    last_content_item = None
    last_skills_item = None
    
    for idx in range(header_row + 1, len(df)):
        row = df.iloc[idx]
        col0 = row[0] if pd.notna(row[0]) else None
        col1 = row[1] if pd.notna(row[1]) else None
        col_content = row[structure['col_content']] if structure['col_content'] < len(row) and pd.notna(row[structure['col_content']]) else None
        col_skills = row[structure['col_skills']] if structure['col_skills'] < len(row) and pd.notna(row[structure['col_skills']]) else None
        
        if all(pd.isna(row)):
            continue
        if col0 and str(col0).lower().startswith('итого'):
            continue
        if col0 and str(col0).lower().startswith('федера'):
            continue
        if col0 and str(col0).isdigit() and not col1:
            continue
        
        if col0:
            col0_str = str(col0).strip()
            match = re.match(r'^(\d+)\s*класс$', col0_str, re.IGNORECASE)
            if match:
                new_class = match.group(1)
                if 5 <= int(new_class) <= 11:
                    current_class = new_class
                continue
        
        if structure['col_section'] is not None and col0 and 'раздел' in str(col0).lower() and '.' in str(col0):
            section_text = re.sub(r'(?i)раздел\s*\d+\.\s*', '', str(col0))
            current_section = section_text.strip()
            continue
        
        is_topic = False
        topic_code = ""
        topic_name = ""
        
        if structure['type'] == '4_columns':
            if col0 and col1:
                try:
                    int(str(col1).strip())
                    is_topic = True
                    topic_name = str(col0).strip()
                except ValueError:
                    pass
        else:
            if col0 and re.match(r'^\d+\.\d+', str(col0)):
                is_topic = True
                topic_code = str(col0).strip()
                topic_name = str(col1).strip() if col1 else ""
        
        if is_topic:
            if col_content:
                content_item = {
                    'code': topic_code, 'text': str(col_content).strip(),
                    'class': current_class, 'section': current_section,
                    'topic': topic_name, 'subject': subject,
                    'program': program, 'sources': ['фрп_планирование']
                }
                content_list.append(content_item)
                last_content_item = content_item
            if col_skills:
                skills_item = {
                    'code': topic_code, 'text': str(col_skills).strip(),
                    'class': current_class, 'section': current_section,
                    'topic': topic_name, 'subject': subject,
                    'program': program, 'sources': ['фрп_планирование']
                }
                skills_list.append(skills_item)
                last_skills_item = skills_item
            continue
        
        if not col0 and not col1:
            if col_content and last_content_item:
                last_content_item['text'] += " " + str(col_content).strip()
            if col_skills and last_skills_item:
                last_skills_item['text'] += " " + str(col_skills).strip()
    
    return skills_list, content_list


def extract_from_sheet(filename, sheet_name, program, file_content=None):
    """Извлекает навыки и содержание из листа Excel."""
    if file_content is not None:
        df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name, header=None)
    else:
        df = pd.read_excel(filename, sheet_name=sheet_name, header=None)
    return extract_frp_from_df(df, sheet_name, program)


def split_text(text):
    # Разбиваем только если после точки идет пробел и заглавная буква
    sentences = re.split(r'\.\s+(?=[А-ЯЁA-Z])', text)
    return [s.strip() for s in sentences if s.strip()]


# --- Логика извлечения из кодификатора ---

def clean_section_name(s):
    """Преобразует «По теме «...»» в «...» — убирает обёртку и кавычки."""
    if not s or not isinstance(s, str):
        return s
    m = re.search(r'По теме\s*[«"]([^»"]+)[»"]', s, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return s


def parse_codifier_sheet(df_raw, subject='', program=''):
    """Парсит вкладку кодификатора (навыки или содержание)."""
    results = []
    current_class = None
    current_section = None
    current_section_code = None
    last_valid_row = None

    for idx, row in df_raw.iterrows():
        col0 = row[0] if 0 in row else None
        col1 = row[1] if 1 in row else None

        if pd.isna(col1):
            if pd.notna(col0) and 'класс' in str(col0).lower():
                match = re.search(r'(\d+)', str(col0))
                if match:
                    current_class = match.group(1)
            continue

        text_value = str(col1).strip()
        if not text_value or text_value == 'nan':
            continue
        if 'таблица' in text_value.lower() or 'результат' in text_value.lower() or 'содержан' in text_value.lower():
            continue

        if pd.isna(col0):
            if last_valid_row is not None:
                last_valid_row['text'] += " " + text_value
            continue

        col0_str = str(col0).strip()
        if col0_str.isdigit() and len(col0_str) <= 2:
            current_section_code = col0_str
            current_section = clean_section_name(text_value)
            continue

        if '.' in col0_str:
            new_row = {
                'code': col0_str,
                'text': text_value,
                'class': current_class,
                'section': current_section,
                'section_code': current_section_code,
                'subject': subject,
                'program': program,
                'sources': ['кодификатор'],
                'topic': ''
            }
            last_valid_row = new_row
            results.append(new_row)

    return results


def split_sentences(text):
    # Разбиваем только если после точки идет пробел и заглавная буква, или точка в конце строки
    sentences = re.split(r'\.\s+(?=[А-ЯЁA-Z])|\.$', text)
    return [s.strip() for s in sentences if s.strip()]


# --- Функции для сравнения и слияния JSON ---

def normalize_text_for_comparison(text):
    """Нормализация текста для сравнения: только удаление пунктуации в конце, регистр не меняем"""
    if not text:
        return ""
    # Удаляем знаки препинания в конце
    text = text.rstrip('.,;:!?')
    return text.strip()


def comprehensive_similarity(text1, text2):
    """
    Комплексная оценка похожести текстов.
    Возвращает словарь с метриками похожести.
    """
    from difflib import SequenceMatcher
    
    norm1 = normalize_text_for_comparison(text1)
    norm2 = normalize_text_for_comparison(text2)
    
    # Если после нормализации идентичны
    if norm1 == norm2:
        return {
            'similarity': 1.0,
            'method': 'exact_match',
            'normalized_match': True,
            'difference_type': 'none'
        }
    
    # SequenceMatcher для общего сходства
    seq_ratio = SequenceMatcher(None, norm1, norm2).ratio()
    
    # Токен-сходство (сравнение по словам) - коэффициент Сёренсена-Дайса
    tokens1 = set(norm1.split())
    tokens2 = set(norm2.split())
    token_intersection = len(tokens1 & tokens2)
    token_sum = len(tokens1) + len(tokens2)
    token_sim = (2 * token_intersection) / token_sum if token_sum > 0 else 0.0
    
    # Разница в количестве токенов
    token_diff = abs(len(tokens1) - len(tokens2))
    
    # Биграммы для учета порядка - коэффициент Сёренсена-Дайса
    def get_bigrams(text):
        return set(text[i:i+2] for i in range(len(text)-1))
    bigrams1 = get_bigrams(norm1)
    bigrams2 = get_bigrams(norm2)
    bigram_intersection = len(bigrams1 & bigrams2)
    bigram_sum = len(bigrams1) + len(bigrams2)
    bigram_sim = (2 * bigram_intersection) / bigram_sum if bigram_sum > 0 else 0.0
    
    # Взвешенная комбинация
    combined = (seq_ratio * 0.4 + token_sim * 0.4 + bigram_sim * 0.2)
    
    # Ограничиваем сверху значением 1.0 на всякий случай
    combined = min(combined, 1.0)
    
    # Определение типа различий
    if combined >= 0.95:
        diff_type = 'chars_only'  # Только различия в символах/буквах
    elif combined >= 0.85:
        diff_type = 'few_words'  # Одно-два слова отличаются
    else:
        diff_type = 'significant'  # Значительные различия
    
    return {
        'similarity': combined,
        'sequence_ratio': seq_ratio,
        'token_similarity': token_sim,
        'bigram_similarity': bigram_sim,
        'common_tokens': token_intersection,
        'total_tokens': token_sum,
        'token_diff': token_diff,
        'difference_type': diff_type
    }


def add_prefix_to_keys(data_dict, prefix):
    """
    Добавляет префикс ко всем ключам в словаре.
    
    Args:
        data_dict: словарь с записями
        prefix: префикс для добавления (например, 'frp_table_')
    
    Returns:
        Новый словарь с префиксами в ключах
    """
    return {f"{prefix}{key}": value for key, value in data_dict.items()}


def find_similar_records(target_record, base_data_dict, subject, class_num, section_filter=None):
    """
    Находит похожие записи в базовом наборе.
    
    Args:
        target_record: запись для сравнения
        base_data_dict: словарь базовых записей (с ключами!)
        subject: предмет для фильтрации
        class_num: класс для фильтрации
        section_filter: название раздела для фильтрации (опционально)
    
    Returns:
        Список из 3 наиболее похожих записей с метриками и ключами
    """
    target_text = target_record.get('text', '')
    if not target_text:
        return []
    
    candidates = []
    subject_norm = subject.strip().lower()
    class_num_str = str(class_num).strip() if class_num else ''
    skip_class_filter = not class_num_str or class_num_str == '0'
    
    for base_key, base_record in base_data_dict.items():
        if base_record.get('subject', '').strip().lower() != subject_norm:
            continue
        if not skip_class_filter and str(base_record.get('class', '')).strip() != class_num_str:
            continue
        
        # Фильтрация по разделу, если указан
        if section_filter:
            base_section = base_record.get('section', '').strip()
            if base_section and base_section != section_filter.strip():
                continue
        
        base_text = base_record.get('text', '')
        if not base_text:
            continue
        
        # Вычисление похожести
        similarity_data = comprehensive_similarity(target_text, base_text)
        similarity_data['record'] = base_record
        similarity_data['key'] = base_key  # Сохраняем ключ!
        candidates.append(similarity_data)
    
    # Сортировка по похожести и возврат топ-3
    candidates.sort(key=lambda x: x['similarity'], reverse=True)
    return candidates[:3]


def validate_json_source(data_dict, expected_source):
    """
    Проверяет, что в JSON файле есть записи с ожидаемым источником.
    
    Args:
        data_dict: словарь с записями
        expected_source: ожидаемый источник ('фрп_текст', 'фрп_планирование', 'кодификатор')
    
    Returns:
        (is_valid, message)
    """
    if not data_dict:
        return False, "Файл пустой"
    
    found_source = False
    for item in data_dict.values():
        if isinstance(item, dict):
            sources = item.get('sources', [])
            if sources and len(sources) > 0:
                first_source = str(sources[0]).strip()
                if first_source == expected_source:
                    found_source = True
                    break
    
    if not found_source:
        return False, f"В файле не найдено записей с источником '{expected_source}'. Проверьте правильность файла."
    
    return True, "Файл валиден"


def get_unique_sections(base_records, subject, class_num):
    """Получает уникальные разделы из базовых записей для заданного предмета и класса.
    При class_num=='0' берёт разделы из всех записей с данным предметом."""
    sections = set()
    subj_norm = subject.strip().lower()
    cls_norm = str(class_num).strip()
    for record in base_records:
        rsubj = (record.get('subject') or '').strip().lower()
        rcls = str(record.get('class', '')).strip() or '0'
        if rsubj == subj_norm and (cls_norm == '0' or rcls == cls_norm):
            section = (record.get('section') or '').strip()
            if section:
                sections.add(section)
    return sorted(list(sections))


def _check_and_transition_next_iteration():
    """Если все три словаря пусты и есть следующая итерация — переходим."""
    if not (st.session_state.compare_for_choice or st.session_state.compare_for_section_topic or st.session_state.compare_for_section_only):
        if st.session_state.compare_next_data:
            st.session_state.compare_iteration = 2
            st.session_state.compare_compare_data = copy.deepcopy(st.session_state.compare_next_data)
            st.session_state.compare_next_data = None
            compare_data = st.session_state.compare_compare_data
            fails = extract_fails_and_clean(compare_data)
            st.session_state.compare_fails.update(fails)
            st.session_state.compare_etalon_data = copy.deepcopy(st.session_state.compare_base_data)
            base_data = copy.deepcopy(st.session_state.compare_base_data)
            merged_data, for_choice, for_section_topic, for_section_only = process_comparison_iteration(
                base_data, compare_data,
                st.session_state.compare_report,
                st.session_state.compare_stats,
                etalon_data=st.session_state.compare_etalon_data,
                simple_mode=st.session_state.get('compare_simple_mode', False)
            )
            st.session_state.compare_base_data = merged_data
            st.session_state.compare_for_choice = for_choice
            st.session_state.compare_for_section_topic = for_section_topic
            st.session_state.compare_for_section_only = for_section_only
            if not (for_choice or for_section_topic or for_section_only):
                st.session_state.compare_merged_result = merged_data
            return True
        else:
            st.session_state.compare_merged_result = st.session_state.compare_base_data
            return True
    return False


def extract_fails_and_clean(compare_data):
    """
    Извлекает записи без предмета/класса/текста в отдельный словарь fails,
    удаляя их из compare_data.
    
    Returns:
        fails: dict {key: record} — записи с недостающими полями
    """
    fails = {}
    keys_to_remove = []
    for key, record in list(compare_data.items()):
        subject = (record.get('subject') or '').strip()
        class_val = str(record.get('class', '')).strip()
        text = (record.get('text') or '').strip()
        if not subject or not class_val or not text:
            fails[key] = record.copy()
            keys_to_remove.append(key)
    for k in keys_to_remove:
        del compare_data[k]
    return fails


def process_comparison_iteration(base_data, compare_data, report, stats, etalon_data=None, simple_mode=False):
    """
    Обрабатывает одну итерацию сравнения.
    Обработанные записи удаляются из compare_data.
    
    Args:
        base_data: рабочая копия для добавления/объединения (модифицируется в результате)
        compare_data: словарь записей для сравнения (модифицируется — обработанные удаляются)
        report: список для журнала отчёта
        stats: словарь статистики
        etalon_data: эталонный словарь для сопоставления (не изменяется). Если None — используется base_data.
    
    Returns:
        merged_data: объединённый словарь (результат слияния в base_data)
        for_choice: {compare_key: decision} — сличение двух и выбор (высокий порог сходства)
        for_section_topic: {compare_key: decision} — подбор раздела и темы (совпадения есть, но не близкие)
        for_section_only: {compare_key: decision} — выбор раздела (совпадений нет, раздел неизвестен)
    """
    etalon = etalon_data if etalon_data is not None else base_data
    merged_data = base_data.copy()
    for_choice = {}
    for_section_topic = {}  # В простом режиме остаётся пустым
    for_section_only = {}
    
    # Глобальный набор для отслеживания всех обработанных записей по нормализованному тексту
    global_processed_texts = set()
    
    # Группировка по классам и предметам (записи без класса → группа "0")
    classes_by_subject = {}
    for key, record in compare_data.items():
        subject = record.get('subject', '').strip()
        class_num = str(record.get('class', '')).strip()
        if not subject:
            continue
        if not class_num:
            class_num = '0'
        if subject not in classes_by_subject:
            classes_by_subject[subject] = set()
        classes_by_subject[subject].add(class_num)
    
    # Обработка по каждому предмету и классу
    for subject in classes_by_subject:
        for class_num in sorted(classes_by_subject[subject], key=lambda x: int(x) if x.isdigit() else 0):
            # Получаем записи текущего класса из compare_data (class_num "0" = пустой у записи)
            def _match(rec, subj, cls):
                rsubj = rec.get('subject', '').strip().lower()
                rcls = str(rec.get('class', '')).strip() or '0'
                return rsubj == subj.strip().lower() and rcls == cls
            compare_records = [
                (key, record) for key, record in compare_data.items()
                if _match(record, subject, class_num)
            ]
            
            # Отслеживаем уже обработанные записи по ключам, чтобы избежать дубликатов
            processed_keys = set()
            
            for compare_key, compare_record in compare_records:
                compare_text = compare_record.get('text', '')
                if not compare_text:
                    continue
                
                # Пропускаем, если эта запись уже обрабатывалась (по ключу)
                if compare_key in processed_keys or compare_key in global_processed_texts:
                    continue
                processed_keys.add(compare_key)
                global_processed_texts.add(compare_key)
                
                # Определяем фильтр по разделу
                compare_section = compare_record.get('section', '').strip()
                section_filter = None
                if compare_section and class_num != '0':
                    # Получаем список эталонных записей для проверки разделов
                    etalon_records_list = [
                        rec for rec in etalon.values()
                        if (rec.get('subject', '').strip().lower() == subject.strip().lower() and
                            str(rec.get('class', '')).strip() == class_num)
                    ]
                    base_sections = get_unique_sections(etalon_records_list, subject, class_num)
                    if compare_section.strip().lower() in [s.strip().lower() for s in base_sections]:
                        section_filter = compare_section
                
                # Сопоставляем с эталоном (не с рабочей копией)
                similar = find_similar_records(
                    compare_record, etalon, subject, class_num, section_filter
                )
                
                if not similar:
                    # Нет похожих записей: берём разделы из эталона (по предмету/классу, при пустоте — из всего эталона)
                    etalon_records_for_sections = [
                        r for r in etalon.values()
                        if (r.get('subject', '').strip().lower() == subject.strip().lower() and
                            (class_num == '0' or str(r.get('class', '')).strip() == class_num))
                    ]
                    base_sections_list = get_unique_sections(etalon_records_for_sections, subject, class_num) if etalon_records_for_sections else []
                    if not base_sections_list:
                        sc = {}
                        for r in etalon.values():
                            s = (r.get('section') or '').strip()
                            if s:
                                sc[s] = sc.get(s, 0) + 1
                        base_sections_list = sorted(sc.keys(), key=lambda x: (-sc.get(x, 0), x))
                    compare_section_val = compare_record.get('section', '').strip()
                    # Регистронезависимое совпадение раздела
                    section_matches = compare_section_val and any(
                        s.strip().lower() == compare_section_val.lower() for s in base_sections_list
                    )
                    if section_matches:
                        # Раздел совпадает — сохраняем запись как есть (тему оставляем или пусто)
                        new_record = compare_record.copy()
                        new_record['section'] = compare_section_val
                        new_record['topic'] = compare_record.get('topic', '') or ''
                        compare_sources = new_record.get('sources', [])
                        source_prefix = 'frp_text_' if (compare_sources and 'фрп_текст' in compare_sources) else 'codifier_'
                        max_num = 0
                        for k in merged_data.keys():
                            if k.startswith(source_prefix) and ('skill_' in k or 'content_' in k):
                                try:
                                    parts = k.split('_')
                                    if len(parts) >= 3 and parts[-1].isdigit():
                                        max_num = max(max_num, int(parts[-1]))
                                except Exception:
                                    pass
                        new_key = f"{source_prefix}skill_{max_num + 1:04d}"
                        merged_data[new_key] = new_record
                        report.append({
                            'action': 'no_match_section_ok',
                            'compare_key': compare_key,
                            'text': compare_text,
                            'section': compare_section_val,
                            'topic': new_record.get('topic', ''),
                            'note': 'Сохранено: раздел совпал с эталоном'
                        })
                        stats['section_assigned_auto'] = stats.get('section_assigned_auto', 0) + 1
                        if compare_key in compare_data:
                            del compare_data[compare_key]
                    else:
                        # Раздел неизвестен или не совпадает — на выбор пользователю (только раздел, тему не трогаем)
                        decision = {
                            'type': 'new_record',
                            'record': compare_record,
                            'compare_key': compare_key,
                            'similar_records': [],
                            'subject': subject,
                            'class': class_num,
                            'base_sections': base_sections_list
                        }
                        for_section_only[compare_key] = decision
                        if compare_key in compare_data:
                            del compare_data[compare_key]
                    continue
                
                best_match = similar[0]
                similarity = best_match['similarity']
                diff_type = best_match.get('difference_type', 'significant')
                base_record = best_match['record']
                
                # Проверяем полное совпадение текстов (100% или почти 100%)
                base_text_norm = normalize_text_for_comparison(base_record.get('text', ''))
                compare_text_norm = normalize_text_for_comparison(compare_text)
                is_exact_match = (base_text_norm == compare_text_norm) or (similarity >= 0.99)
                
                if is_exact_match or (diff_type == 'chars_only' and similarity >= 0.95):
                    # Автоматическое объединение
                    base_record = best_match['record']
                    base_key = best_match.get('key')  # Ключ уже есть в результатах поиска
                    
                    if base_key and base_key in merged_data:
                        # Объединяем источники
                        merged_record = merged_data[base_key].copy()
                        compare_sources = compare_record.get('sources', [])
                        base_sources = merged_record.get('sources', [])
                        merged_sources = list(set(base_sources + compare_sources))
                        merged_record['sources'] = merged_sources
                        
                        # При точном совпадении всегда присваиваем раздел/тему из эталонной записи
                        if base_record.get('section'):
                            merged_record['section'] = base_record.get('section')
                        if base_record.get('topic'):
                            merged_record['topic'] = base_record.get('topic')
                        
                        merged_data[base_key] = merged_record
                        
                        # Запись в журнал (compare_key нужен для точного подсчёта при одинаковых текстах)
                        report.append({
                            'action': 'auto_merge',
                            'compare_key': compare_key,
                            'base_text': base_record.get('text', ''),
                            'compare_text': compare_text,
                            'similarity': similarity,
                            'merged_sources': merged_sources,
                            'section': merged_record.get('section', ''),
                            'topic': merged_record.get('topic', '')
                        })
                        stats['auto_merged'] += 1
                        if compare_key in compare_data:
                            del compare_data[compare_key]
                    else:
                        # Точное совпадение найдено, но base_key не найден - добавляем как новую с автоматическим присвоением раздела/темы
                        new_record = compare_record.copy()
                        if base_record.get('section'):
                            new_record['section'] = base_record.get('section')
                        if base_record.get('topic'):
                            new_record['topic'] = base_record.get('topic')
                        # Объединяем источники
                        compare_sources = compare_record.get('sources', [])
                        base_sources = base_record.get('sources', [])
                        new_record['sources'] = list(set(base_sources + compare_sources))
                        
                        # Генерируем новый ключ с префиксом на основе источника
                        source_prefix = 'frp_text_' if 'фрп_текст' in compare_sources else 'codifier_'
                        # Находим максимальный номер среди существующих ключей с таким префиксом
                        max_num = 0
                        for k in merged_data.keys():
                            if k.startswith(source_prefix) and ('skill_' in k or 'content_' in k):
                                try:
                                    parts = k.split('_')
                                    if len(parts) >= 3 and parts[-1].isdigit():
                                        max_num = max(max_num, int(parts[-1]))
                                except:
                                    pass
                        
                        new_key = f"{source_prefix}skill_{max_num + 1:04d}"
                        merged_data[new_key] = new_record
                        
                        report.append({
                            'action': 'auto_merge_new',
                            'compare_key': compare_key,
                            'base_text': base_record.get('text', ''),
                            'compare_text': compare_text,
                            'similarity': similarity,
                            'section': new_record.get('section', ''),
                            'topic': new_record.get('topic', '')
                        })
                        stats['auto_merged'] += 1
                        if compare_key in compare_data:
                            del compare_data[compare_key]
                    
                    # После автоматического объединения переходим к следующей записи
                    continue
                
                elif diff_type == 'few_words' and similarity >= 0.85:
                    # Требуется выбор пользователя (сличение двух и выбора)
                    decision = {
                        'type': 'choice',
                        'compare_record': compare_record,
                        'compare_key': compare_key,
                        'base_record': best_match['record'],
                        'base_key': best_match.get('key'),
                        'similarity': similarity,
                        'similar_records': similar[:3],
                        'subject': subject,
                        'class': class_num
                    }
                    for_choice[compare_key] = decision
                    if compare_key in compare_data:
                        del compare_data[compare_key]
                
                else:
                    # Низкая похожесть
                    if simple_mode:
                        # Простое сравнение: обрабатываем как "нет совпадений" — раздел есть -> сохранить, иначе выбор раздела
                        etalon_records_for_sections = [
                            r for r in etalon.values()
                            if (r.get('subject', '').strip().lower() == subject.strip().lower() and
                                (class_num == '0' or str(r.get('class', '')).strip() == class_num))
                        ]
                        base_sections_list = get_unique_sections(etalon_records_for_sections, subject, class_num) if etalon_records_for_sections else []
                        if not base_sections_list:
                            sc = {}
                            for r in etalon.values():
                                s = (r.get('section') or '').strip()
                                if s:
                                    sc[s] = sc.get(s, 0) + 1
                            base_sections_list = sorted(sc.keys(), key=lambda x: (-sc.get(x, 0), x))
                        compare_section_val = compare_record.get('section', '').strip()
                        section_matches = compare_section_val and any(
                            s.strip().lower() == compare_section_val.lower() for s in base_sections_list
                        )
                        if section_matches:
                            new_record = compare_record.copy()
                            new_record['section'] = compare_section_val
                            new_record['topic'] = compare_record.get('topic', '') or ''
                            compare_sources = new_record.get('sources', [])
                            source_prefix = 'frp_text_' if (compare_sources and 'фрп_текст' in compare_sources) else 'codifier_'
                            max_num = 0
                            for k in merged_data.keys():
                                if k.startswith(source_prefix) and ('skill_' in k or 'content_' in k):
                                    try:
                                        parts = k.split('_')
                                        if len(parts) >= 3 and parts[-1].isdigit():
                                            max_num = max(max_num, int(parts[-1]))
                                    except Exception:
                                        pass
                            new_key = f"{source_prefix}skill_{max_num + 1:04d}"
                            merged_data[new_key] = new_record
                            report.append({
                                'action': 'no_match_section_ok',
                                'compare_key': compare_key,
                                'text': compare_text,
                                'section': compare_section_val,
                                'topic': new_record.get('topic', ''),
                                'note': 'Сохранено (простое сравнение): раздел совпал'
                            })
                            stats['section_assigned_auto'] = stats.get('section_assigned_auto', 0) + 1
                            if compare_key in compare_data:
                                del compare_data[compare_key]
                        else:
                            decision = {
                                'type': 'new_record',
                                'record': compare_record,
                                'compare_key': compare_key,
                                'similar_records': [],
                                'subject': subject,
                                'class': class_num,
                                'base_sections': base_sections_list
                            }
                            for_section_only[compare_key] = decision
                            if compare_key in compare_data:
                                del compare_data[compare_key]
                    else:
                        # Обычный режим: подбор раздела/темы на основе похожих записей
                        decision = {
                            'type': 'section_topic_choice',
                            'record': compare_record,
                            'compare_key': compare_key,
                            'similar_records': similar[:3],
                            'subject': subject,
                            'class': class_num
                        }
                        for_section_topic[compare_key] = decision
                        if compare_key in compare_data:
                            del compare_data[compare_key]
    
    return merged_data, for_choice, for_section_topic, for_section_only


def create_separate_elements(items, id_prefix):
    """Разбивает элементы на отдельные предложения."""
    result = {}
    counter = 1
    for item in items:
        sentences = split_sentences(item['text'])
        for sentence in sentences:
            new_item = item.copy()
            new_item['text'] = sentence
            result[f"{id_prefix}_{counter:04d}"] = new_item
            counter += 1
    return result


# --- Извлечение из PDF ---

def _table_to_df(table):
    """Список списков → DataFrame, выравнивание по максимальной ширине."""
    if not table:
        return pd.DataFrame()
    max_cols = max(len(r) for r in table)
    padded = [list(r) + [None] * (max_cols - len(r)) for r in table]
    return pd.DataFrame(padded)


def _detect_table_type(table):
    """Определяет тип: frp (4-5 кол), codifier (2 кол с кодами X.Y)."""
    if not table:
        return None
    flat = " ".join(str(c).lower() for row in table[:5] for c in row if c)
    n_col = max(len(r) for r in table) if table else 0
    if "содержан" in flat or "деятельност" in flat:
        return "frp"
    if n_col == 2 and any(
        c and "." in str(c) and str(c).split(".")[0].isdigit()
        for row in table[:15] for c in row if c
    ):
        return "codifier"
    if n_col >= 4 and ("содержан" in flat or "деятельност" in flat or "раздел" in flat):
        return "frp"
    return None


def _get_class_from_frp_table(table):
    """Находит класс в таблице ФРП (5, 6, ...)."""
    for row in table[:10]:
        for c in row:
            if c and re.search(r"(\d+)\s*класс", str(c), re.I):
                m = re.search(r"(\d+)", str(c))
                if m and 5 <= int(m.group(1)) <= 11:
                    return m.group(1)
    return None


def _get_class_from_codifier_table(table):
    """Кодификатор может содержать класс в заголовках."""
    for row in table[:5]:
        for c in row:
            if c and re.search(r"(\d+)\s*класс", str(c), re.I):
                m = re.search(r"(\d+)", str(c))
                if m:
                    return m.group(1)
    return None


def extract_and_merge_pdf_tables(pdf_bytes):
    """
    Извлекает таблицы из PDF, определяет тип, объединяет по классу.
    Возвращает: (doc_type, merged_dfs, stats)
    - doc_type: 'frp' | 'codifier'
    - merged_dfs: список (subject_or_class, df) для передачи в extract
    - stats: dict с таблицами, классами
    """
    import pdfplumber
    tables_by_page = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_num, page in enumerate(pdf.pages):
            for t in page.extract_tables():
                if t and len(t) > 0:
                    tables_by_page.append((page_num + 1, t))
    
    if not tables_by_page:
        return None, [], {"tables": 0, "classes": []}
    
    # Определяем тип по первой подходящей таблице
    doc_type = None
    for _, t in tables_by_page:
        dt = _detect_table_type(t)
        if dt:
            doc_type = dt
            break
    if not doc_type:
        doc_type = "frp" if any(max(len(r) for r in t) >= 4 for _, t in tables_by_page) else "codifier"
    
    # Объединение по классу: таблицы одного класса склеиваются
    merged = {}  # class -> list of dfs
    current_class = "_"
    for page_num, table in tables_by_page:
        dt = _detect_table_type(table)
        if dt != doc_type:
            continue
        df = _table_to_df(table)
        if df.empty or len(df) < 2:
            continue
        cls = _get_class_from_frp_table(table) if doc_type == "frp" else _get_class_from_codifier_table(table)
        if cls:
            current_class = cls
        if current_class not in merged:
            merged[current_class] = []
        merged[current_class].append(df)
    
    # Склеиваем df одного класса
    result = []
    for cls in sorted(merged.keys(), key=lambda x: int(x) if str(x).isdigit() else 999):
        dfs = merged[cls]
        combined = pd.concat(dfs, ignore_index=True)
        result.append((cls, combined))
    
    stats = {"tables": len(tables_by_page), "classes": list(merged.keys())}
    return doc_type, result, stats


# --- Извлечение из ФРП (текст doc/txt) ---

def _read_doc_or_txt(file_content, filename):
    """Читает текст из .txt или .docx (.doc не поддерживается — используйте .docx или .txt)."""
    name = (filename or "").lower()
    if name.endswith('.txt'):
        return file_content.decode('utf-8', errors='replace')
    if name.endswith('.docx'):
        from docx import Document
        doc = Document(io.BytesIO(file_content))
        return "\n".join(p.text for p in doc.paragraphs)
    if name.endswith('.doc'):
        raise ValueError("Формат .doc не поддерживается. Сохраните файл как .docx или скопируйте в .txt")
    return file_content.decode('utf-8', errors='replace')


def extract_frp_from_text(text, subject, program):
    """
    Извлечение из текста ФРП (после заголовков «содержание обучения» и «предметные результаты»).
    Правила: N класс, Раздел (без слова), Тема (без слова). Разбивка по точкам или абзацам.
    """
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    lines = text.split('\n')
    
    content_start = None
    skills_start = None
    for i, line in enumerate(lines):
        low = line.strip().lower()
        if 'содержание обучения' in low:
            content_start = i + 1
        if 'предметные результаты' in low:
            skills_start = i + 1
            break
    
    def parse_block(start_idx, end_idx, is_content):
        items = []
        current_class = ""
        current_section = ""
        current_topic = ""
        buffer = []
        
        def flush_buffer():
            nonlocal buffer
            if not buffer:
                return
            combined = " ".join(buffer).strip()
            if not combined:
                buffer = []
                return
            if "." in combined:
                # Разбиваем только если после точки идет пробел и заглавная буква
                parts = re.split(r'\.\s+(?=[А-ЯЁA-Z])', combined)
                for p in parts:
                    p = p.strip()
                    if not p:
                        continue
                    if not p.endswith('.'):
                        p += "."
                    items.append({
                        'code': '', 'text': p,
                        'class': current_class, 'section': current_section,
                        'topic': current_topic, 'subject': subject,
                        'program': program, 'sources': ['фрп_текст']
                    })
            else:
                for para in buffer:
                    para = para.strip()
                    if para:
                        items.append({
                            'code': '', 'text': para,
                            'class': current_class, 'section': current_section,
                            'topic': current_topic, 'subject': subject,
                            'program': program, 'sources': ['фрп_текст']
                        })
            buffer = []
        
        for i in range(start_idx, end_idx if end_idx is not None else len(lines)):
            line = lines[i]
            stripped = line.strip()
            if not stripped:
                flush_buffer()
                continue
            
            low = stripped.lower()
            if low.startswith('федеральная программа') or low.startswith('федеральная рабочая программа'):
                continue
            if re.match(r'^\d+\s*$', stripped):
                continue
            
            class_match = re.match(r'^(\d+)\s*класс\s*\.?\s*$', low, re.I)
            if class_match:
                flush_buffer()
                current_class = class_match.group(1)
                continue
            
            if re.match(r'^раздел\b', low):
                flush_buffer()
                current_topic = ""
                rest = re.sub(r'^раздел\s*[.:;\-–—\s]*', '', stripped, flags=re.I).strip()
                rest = re.sub(r'^[.:;\-–—\s]+', '', rest).strip()
                if rest:
                    current_section = rest
                continue
            
            if re.match(r'^тема\b', low):
                flush_buffer()
                rest = re.sub(r'^тема\s*[.:;\-–—\s]*', '', stripped, flags=re.I).strip()
                rest = re.sub(r'^[.:;\-–—\s]+', '', rest).strip()
                if rest:
                    current_topic = rest
                continue
            
            buffer.append(stripped)
        
        flush_buffer()
        return items
    
    content_items = []
    skills_items = []
    
    if content_start is not None:
        end = skills_start if skills_start is not None else len(lines)
        content_items = parse_block(content_start, end, True)
    
    if skills_start is not None:
        skills_items = parse_block(skills_start, None, False)
    
    return content_items, skills_items


def json_to_excel_sorted(data_dict, columns_title='Содержание'):
    """Преобразует JSON в Excel с листами по предметам. Сортировка: класс → раздел → тема → порядок по ID."""
    # Собираем строки с предметом
    rows = []
    for key, item in data_dict.items():
        subject = item.get('subject', '') or ''
        rows.append({
            'subject': subject,
            'Класс': item.get('class', ''),
            'Раздел': item.get('section', ''),
            'Тема': item.get('topic', ''),
            columns_title: item.get('text', ''),
            '_sort_key': key
        })
    
    df = pd.DataFrame(rows)
    df['subject'] = df['subject'].fillna('').astype(str).str.strip()
    df.loc[df['subject'] == '', 'subject'] = 'Общее'  # пустые — в лист "Общее"
    
    subjects = sorted(df['subject'].unique())
    
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        for subject in subjects:
            sub_df = df[df['subject'] == subject].copy()
            sub_df = sub_df.drop(columns=['subject'])
            sub_df = sub_df.sort_values(['Класс', 'Раздел', 'Тема', '_sort_key'], na_position='last')
            sub_df = sub_df.drop(columns=['_sort_key'])
            sheet_name = str(subject)[:31].replace('/', '-').replace('\\', '-').replace('*', '').replace('?', '').replace('[', '').replace(']', '').replace(':', '-')
            sub_df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    buf.seek(0)
    return buf.getvalue()


# --- Функции для работы с LLM ---

def get_claude_api_key():
    """Получает API ключ Claude из session_state."""
    return st.session_state.get('claude_api_key', '')

def test_claude_api_key(api_key: str, verify_ssl: bool = True) -> Dict:
    """
    Тестирует API ключ Claude и возвращает информацию о доступных моделях и параметрах.
    Возвращает словарь с результатами проверки.
    """
    if not api_key:
        return {'valid': False, 'error': 'API ключ не введен'}
    
    # Список версий API для проверки
    api_versions = [
        "2024-10-22",
        "2024-06-01", 
        "2023-06-01"
    ]
    
    # Список моделей для проверки
    test_models = [
        "claude-3-5-sonnet-20241022",
        "claude-sonnet-4-20250514",
        "claude-3-opus-20240229",
        "claude-3-sonnet-20240229"
    ]
    
    # Тестовый запрос (минимальный)
    test_messages = [{
        "role": "user",
        "content": "Hi"
    }]
    
    url = "https://api.anthropic.com/v1/messages"
    
    last_error = None
    
    for version in api_versions:
        for model in test_models:
            test_data = {
                "model": model,
                "max_tokens": 10,
                "messages": test_messages
            }
            
            headers = {
                "x-api-key": api_key,
                "anthropic-version": version,
                "content-type": "application/json"
            }
            
            try:
                response = requests.post(
                    url, 
                    headers=headers, 
                    json=test_data, 
                    timeout=30, 
                    verify=verify_ssl
                )
                
                if response.status_code == 200:
                    result = response.json()
                    return {
                        'valid': True,
                        'api_version': version,
                        'model': model,
                        'response': result,
                        'headers': headers,
                        'data': test_data
                    }
                elif response.status_code == 401:
                    return {
                        'valid': False,
                        'error': 'Неверный API ключ (401 Unauthorized)',
                        'api_version': version,
                        'model': model
                    }
                elif response.status_code == 403:
                    # Сохраняем информацию об ошибке, но продолжаем пробовать
                    last_error = {
                        'status_code': response.status_code,
                        'error_text': response.text[:200] if response.text else 'No error text',
                        'api_version': version,
                        'model': model
                    }
                    continue
                else:
                    # Сохраняем информацию об ошибке, но продолжаем пробовать
                    last_error = {
                        'status_code': response.status_code,
                        'error_text': response.text[:200] if response.text else 'No error text',
                        'api_version': version,
                        'model': model
                    }
            except requests.exceptions.SSLError:
                if verify_ssl:
                    # Пробуем без SSL
                    try:
                        response = requests.post(
                            url, 
                            headers=headers, 
                            json=test_data, 
                            timeout=30, 
                            verify=False
                        )
                        if response.status_code == 200:
                            result = response.json()
                            return {
                                'valid': True,
                                'api_version': version,
                                'model': model,
                                'response': result,
                                'headers': headers,
                                'data': test_data,
                                'ssl_warning': True
                            }
                    except Exception as e:
                        continue
                continue
            except Exception as e:
                # Пробуем следующую комбинацию
                continue
    
    # Если ничего не сработало, возвращаем последнюю ошибку или общую
    return {
        'valid': False,
        'error': 'Не удалось подключиться ни с одной комбинацией версии API и модели',
        'last_error': last_error
    }

def call_claude_api(messages: List[Dict], api_key: str, model: str = None, api_version: str = None, verify_ssl: bool = True) -> Optional[str]:
    """Вызывает API Claude и возвращает ответ."""
    if not api_key:
        return None
    
    # Используем сохраненные параметры из проверки ключа, если они есть
    if model is None:
        model = st.session_state.get('claude_working_model', "claude-sonnet-4-20250514")
    if api_version is None:
        api_version = st.session_state.get('claude_working_api_version', "2023-06-01")
    
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": api_version,
        "content-type": "application/json"
    }
    
    data = {
        "model": model,
        "max_tokens": 4096,
        "messages": messages
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=120, verify=verify_ssl)
        response.raise_for_status()
        result = response.json()
        return result.get('content', [{}])[0].get('text', '')
    except requests.exceptions.SSLError as ssl_error:
        if verify_ssl:
            # Пробуем без проверки SSL, если была ошибка SSL
            try:
                st.warning("⚠️ Ошибка проверки SSL сертификата. Повторяю запрос без проверки SSL...")
                response = requests.post(url, headers=headers, json=data, timeout=120, verify=False)
                response.raise_for_status()
                result = response.json()
                return result.get('content', [{}])[0].get('text', '')
            except Exception as e2:
                st.error(f"Ошибка при вызове API Claude (без проверки SSL): {e2}")
                return None
        else:
            st.error(f"Ошибка SSL при вызове API Claude: {ssl_error}")
            return None
    except Exception as e:
        st.error(f"Ошибка при вызове API Claude: {e}")
        return None

def group_content_by_structure(data_dict: Dict) -> Dict:
    """Группирует записи содержания по предмету -> класс -> раздел -> тема."""
    grouped = {}
    
    for key, record in data_dict.items():
        if 'content' not in key.lower():
            continue
        
        subject = record.get('subject', '').strip() or 'без предмета'
        class_num = str(record.get('class', '')).strip() or '0'
        section = record.get('section', '').strip() or 'без раздела'
        topic = record.get('topic', '').strip() or 'без темы'
        text = record.get('text', '').strip()
        
        if not text:
            continue
        
        if subject not in grouped:
            grouped[subject] = {}
        if class_num not in grouped[subject]:
            grouped[subject][class_num] = {}
        if section not in grouped[subject][class_num]:
            grouped[subject][class_num][section] = {}
        if topic not in grouped[subject][class_num][section]:
            grouped[subject][class_num][section][topic] = []
        
        grouped[subject][class_num][section][topic].append(text)
    
    return grouped

def get_frp_sections_and_topics(data_dict: Dict) -> Dict:
    """Собирает разделы и темы из записей, где есть источник фрп_таблица."""
    frp_structure = {}
    
    for key, record in data_dict.items():
        sources = record.get('sources', [])
        if not sources or 'фрп_таблица' not in sources:
            continue
        
        subject = record.get('subject', '').strip()
        section = record.get('section', '').strip()
        topic = record.get('topic', '').strip()
        
        if not subject or not section:
            continue
        
        if subject not in frp_structure:
            frp_structure[subject] = {}
        if section not in frp_structure[subject]:
            frp_structure[subject][section] = set()
        if topic:
            frp_structure[subject][section].add(topic)
    
    # Преобразуем set в list для JSON сериализации
    result = {}
    for subject, sections in frp_structure.items():
        result[subject] = {}
        for section, topics in sections.items():
            result[subject][section] = sorted(list(topics))
    
    return result

def format_content_text(grouped: Dict, frp_structure: Dict) -> str:
    """Формирует текст для передачи модели."""
    lines = []
    
    # Добавляем информацию о разделах и темах ФРП
    if frp_structure:
        lines.append("По фрп имеются следующие разделы и темы:")
        for subject, sections in frp_structure.items():
            for section, topics in sections.items():
                lines.append(f"раздел: {section}")
                for topic in topics:
                    lines.append(f"  {topic}")
        lines.append("")
    
    # Группируем по предмету и классу
    for subject in sorted(grouped.keys()):
        for class_num in sorted(grouped[subject].keys(), key=lambda x: int(x) if str(x).isdigit() else 0):
            lines.append(f"предмет: {subject}")
            lines.append(f"класс: {class_num}")
            
            sections = grouped[subject][class_num]
            
            # Сначала записи с разделом и темой
            for section in sorted(sections.keys()):
                if section == 'без раздела':
                    continue
                topics = sections[section]
                for topic in sorted(topics.keys()):
                    if topic == 'без темы':
                        continue
                    lines.append(f"раздел: {section}")
                    lines.append(f"тема: {topic}")
                    
                    texts = topics[topic]
                    # Обрабатываем каждую запись отдельно: добавляем точку, если её нет
                    processed_texts = []
                    for text in texts:
                        text = text.strip()
                        if text:
                            # Если не заканчивается точкой/восклицательным/вопросительным, добавляем точку
                            if not text.rstrip().endswith(('.', '!', '?')):
                                text = text.rstrip() + '.'
                            processed_texts.append(text)
                    content_text = ' '.join(processed_texts)
                    lines.append(content_text)
                    lines.append("")
            
            # Записи с разделом, но без темы
            for section in sorted(sections.keys()):
                if section == 'без раздела':
                    continue
                if 'без темы' in sections[section]:
                    lines.append(f"раздел: {section}")
                    lines.append("тема: без темы")
                    texts = sections[section]['без темы']
                    # Обрабатываем каждую запись отдельно: добавляем точку, если её нет
                    processed_texts = []
                    for text in texts:
                        text = text.strip()
                        if text:
                            # Если не заканчивается точкой/восклицательным/вопросительным, добавляем точку
                            if not text.rstrip().endswith(('.', '!', '?')):
                                text = text.rstrip() + '.'
                            processed_texts.append(text)
                    content_text = ' '.join(processed_texts)
                    lines.append(content_text)
                    lines.append("")
            
            # Записи без раздела (в конец)
            if 'без раздела' in sections:
                lines.append("раздел: без раздела")
                all_no_section = []
                for topic_texts in sections['без раздела'].values():
                    all_no_section.extend(topic_texts)
                if all_no_section:
                    # Обрабатываем каждую запись отдельно: добавляем точку, если её нет
                    processed_texts = []
                    for text in all_no_section:
                        text = text.strip()
                        if text:
                            # Если не заканчивается точкой/восклицательным/вопросительным, добавляем точку
                            if not text.rstrip().endswith(('.', '!', '?')):
                                text = text.rstrip() + '.'
                            processed_texts.append(text)
                    content_text = ' '.join(processed_texts)
                    lines.append(content_text)
                    lines.append("")
    
    return '\n'.join(lines)

def parse_llm_response(response_text: str, subject: str, class_num: str) -> List[Dict]:
    """Парсит ответ от LLM и возвращает список записей."""
    records = []
    
    # Пытаемся найти JSON в ответе
    json_match = re.search(r'\{[\s\S]*\}', response_text)
    if json_match:
        try:
            data = json.loads(json_match.group())
            if isinstance(data, dict):
                # Если это словарь записей
                for key, value in data.items():
                    if isinstance(value, dict):
                        value['subject'] = subject
                        value['class'] = class_num
                        records.append(value)
            elif isinstance(data, list):
                # Если это список записей
                for item in data:
                    if isinstance(item, dict):
                        item['subject'] = subject
                        item['class'] = class_num
                        records.append(item)
        except json.JSONDecodeError:
            pass
    
    return records

# --- UI ---

st.title("📚 Извлечение и преобразование данных")
st.markdown("*ФРП, кодификатор, JSON → Excel*")

# Общее окошко для API ключа Claude в sidebar
with st.sidebar:
    st.header("⚙️ Настройки LLM")
    api_key = st.text_input(
        "API ключ Claude",
        value=st.session_state.get('claude_api_key', ''),
        type="password",
        help="Введите API ключ для работы с Claude Sonnet 4.5",
        key='claude_api_key_input'
    )
    if api_key:
        st.session_state.claude_api_key = api_key
        st.success("✅ API ключ сохранен")
    else:
        st.warning("⚠️ API ключ не введен")
    
    verify_ssl = st.checkbox(
        "Проверять SSL сертификат",
        value=st.session_state.get('claude_verify_ssl', True),
        help="Отключите, если возникают ошибки SSL сертификата",
        key='claude_verify_ssl_input'
    )
    st.session_state.claude_verify_ssl = verify_ssl
    if not verify_ssl:
        st.warning("⚠️ Проверка SSL отключена")
    
    # Кнопка проверки API ключа
    if api_key:
        if st.button("🔍 Проверить API ключ", key='test_api_key', use_container_width=True):
            with st.spinner("Проверка API ключа..."):
                test_result = test_claude_api_key(api_key, verify_ssl)
                
                if test_result.get('valid'):
                    st.success("✅ API ключ валиден!")
                    st.markdown("---")
                    st.markdown("**Параметры подключения:**")
                    
                    # Выводим все параметры для копирования
                    st.code(f"""
API Version: {test_result.get('api_version')}
Model: {test_result.get('model')}
SSL Verify: {verify_ssl}
                    """, language='text')
                    
                    st.markdown("**Заголовки запроса:**")
                    st.json(test_result.get('headers', {}))
                    
                    st.markdown("**Данные запроса:**")
                    st.json(test_result.get('data', {}))
                    
                    if test_result.get('ssl_warning'):
                        st.warning("⚠️ SSL проверка отключена при тестировании")
                    
                    # Сохраняем рабочую версию и модель
                    st.session_state.claude_working_api_version = test_result.get('api_version')
                    st.session_state.claude_working_model = test_result.get('model')
                else:
                    st.error(f"❌ {test_result.get('error', 'Неизвестная ошибка')}")
                    if test_result.get('last_error'):
                        st.json(test_result.get('last_error'))

# Инициализация session_state
for k, v in [
    ('mode', 'frp_table'),
    ('extracted', False),
    ('intermediate_skills', {}),
    ('intermediate_content', {}),
    ('sections_df', None),
    ('original_pairs', []),
    ('final_skills_json', None),
    ('final_content_json', None),
    ('excel_skills_bytes', None),
    ('excel_content_bytes', None),
    ('available_jsons', []),
    ('last_extraction_mode', None),
    ('pdf_extracted', False),
    ('pdf_doc_type', None),
    ('pdf_merged', []),
    ('merge_jsons', []),  # список {name, data, type} для объединения
    ('compare_frp_table', None),  # ФРП таблица для сравнения
    ('compare_frp_text', None),   # ФРП текст для сравнения
    ('compare_codifier', None),   # Кодификатор для сравнения
    ('compare_etalon_data', None),  # Эталон для сопоставления (не изменяется)
    ('compare_report', []),       # Журнал отчёта
    ('compare_stats', {}),        # Статистика
    ('compare_pending_decisions', {}),  # [устаревшее] оставлено для совместимости
    ('compare_current_class', None),    # [устаревшее] оставлено для совместимости
    ('compare_fails', {}),             # Записи без предмета/класса/текста
    ('compare_for_choice', {}),        # Сличение двух и выбора (высокий порог сходства)
    ('compare_for_section_topic', {}), # Подбор раздела и темы
    ('compare_for_section_only', {}),  # Выбор раздела (совпадений нет)
    ('compare_simple_mode', False),     # Режим «простое сравнение»
    ('compare_merged_result', None),    # Результат объединения
    ('claude_api_key', ''),             # API ключ Claude
    ('claude_verify_ssl', True),        # Проверка SSL сертификата для Claude API
    ('claude_working_api_version', '2023-06-01'),  # Рабочая версия API (из проверки ключа)
    ('claude_working_model', 'claude-sonnet-4-20250514'),  # Рабочая модель (из проверки ключа)
    ('llm_content_data', None),         # Загруженные данные содержания для LLM
    ('llm_grouped_data', None),         # Сгруппированные данные
    ('llm_frp_structure', None),        # Структура ФРП разделов и тем
    ('llm_formatted_text', None),       # Отформатированный текст для модели
    ('llm_results', {}),                # Результаты обработки по парам предмет+класс
    ('llm_final_json', None),          # Финальный объединенный JSON
]:
    if k not in st.session_state:
        st.session_state[k] = v

# Выбор режима
st.header("Выберите режим")

mode = st.radio(
    "Режим работы:",
    options=[
        'frp_table',      # Из ФРП (таблица)
        'codifier',       # Из кодификатора
        'pdf',            # Из PDF
        'frp_text',       # Из ФРП (текст)
        'json_to_excel',  # JSON → Excel-таблицы
        'json_merge',     # Объединение JSON
        'json_compare',   # Слияние с сравнением JSON
        'llm_structure',  # Структурирование с помощью LLM
    ],
    format_func=lambda x: {
        'frp_table': 'Извлечение: ФРП (таблица Excel)',
        'codifier': 'Извлечение: из кодификатора',
        'pdf': 'Извлечение: PDF (ФРП или кодификатор)',
        'frp_text': 'Извлечение: ФРП (текст)',
        'json_to_excel': 'Преобразование: JSON → Excel-таблицы',
        'json_merge': 'Объединение нескольких JSON в один',
        'json_compare': 'Слияние и сравнение JSON файлов',
        'llm_structure': '🤖 Структурирование с помощью LLM',
    }[x],
    horizontal=True,
    key='mode_selector'
)

st.markdown("---")

# ============ РЕЖИМ: ФРП (таблица) ============
if mode == 'frp_table':
    st.header("1️⃣ Извлечение из ФРП (таблица)")

    uploaded_file = st.file_uploader("Загрузите Excel файл ФРП", type=['xlsx', 'xls'], key='frp_upload')
    program_value = st.radio("Программа:", ['базовый', 'профильный'], horizontal=True, key='frp_program')

    if uploaded_file and not st.session_state.extracted:
        if st.button("Извлечь данные", type="primary"):
            with st.spinner("Извлечение данных..."):
                file_content = uploaded_file.read()
                xl_file = pd.ExcelFile(io.BytesIO(file_content))
                
                all_skills = []
                all_content = []
                for sheet_name in xl_file.sheet_names:
                    skills, content = extract_from_sheet(None, sheet_name, program_value, file_content)
                    all_skills.extend(skills)
                    all_content.extend(content)
                
                intermediate_skills = {f"skill_{i:04d}": s for i, s in enumerate(all_skills, 1)}
                intermediate_content = {f"content_{i:04d}": c for i, c in enumerate(all_content, 1)}
                
                sections_data = []
                seen = set()
                for item in list(intermediate_skills.values()) + list(intermediate_content.values()):
                    key = (item['section'] or '', item['topic'] or '')
                    if key not in seen:
                        sections_data.append({'Раздел': item['section'] or '', 'Тема': item['topic'] or ''})
                        seen.add(key)
                
                st.session_state.intermediate_skills = intermediate_skills
                st.session_state.intermediate_content = intermediate_content
                st.session_state.sections_df = pd.DataFrame(sections_data)
                st.session_state.original_pairs = [(r['Раздел'], r['Тема']) for _, r in st.session_state.sections_df.iterrows()]
                st.session_state.extracted = True
                st.session_state.last_extraction_mode = 'frp_table'
                st.rerun()

    if st.session_state.extracted:
        st.success(f"✅ Извлечено: {len(st.session_state.intermediate_skills)} навыков, {len(st.session_state.intermediate_content)} содержания")
        
        # Шаг 2: Редактирование
        st.header("2️⃣ Редактирование разделов и тем")
        st.caption("Отредактируйте таблицу и нажмите «Применить изменения»")
        
        edited_df = st.data_editor(
            st.session_state.sections_df,
            use_container_width=True,
            key="sections_editor",
            column_config={
                "Раздел": st.column_config.TextColumn("Раздел", width="large"),
                "Тема": st.column_config.TextColumn("Тема", width="large")
            },
            num_rows="fixed"
        )
        
        col1, col2, col3 = st.columns([1, 1, 3])
        with col1:
            if st.button("Применить изменения", type="primary"):
                mapping = {}
                for i in range(len(st.session_state.original_pairs)):
                    old_s, old_t = st.session_state.original_pairs[i]
                    if i < len(edited_df):
                        new_s = str(edited_df.iloc[i]['Раздел']).strip() if pd.notna(edited_df.iloc[i]['Раздел']) else ''
                        new_t = str(edited_df.iloc[i]['Тема']).strip() if pd.notna(edited_df.iloc[i]['Тема']) else ''
                        mapping[(old_s, old_t)] = (new_s, new_t)
                
                for skill in st.session_state.intermediate_skills.values():
                    key = (skill['section'], skill['topic'])
                    if key in mapping:
                        skill['section'], skill['topic'] = mapping[key]
                
                for content in st.session_state.intermediate_content.values():
                    key = (content['section'], content['topic'])
                    if key in mapping:
                        content['section'], content['topic'] = mapping[key]
                
                st.session_state.sections_df = edited_df.copy()
                st.session_state.original_pairs = [(edited_df.iloc[i]['Раздел'] or '', edited_df.iloc[i]['Тема'] or '') 
                    for i in range(len(edited_df))]
                st.success("✅ Изменения применены!")
                st.rerun()
        
        # Шаг 3: Сохранение в JSON
        st.header("3️⃣ Сохранение в JSON")
        
        if 'final_skills_json' not in st.session_state:
            st.session_state.final_skills_json = None
            st.session_state.final_content_json = None
        
        if st.button("Разбить на предложения"):
            with st.spinner("Обработка..."):
                final_skills = {}
                counter = 1
                for skill in st.session_state.intermediate_skills.values():
                    for sentence in split_text(skill['text']):
                        new_skill = skill.copy()
                        new_skill['text'] = sentence
                        final_skills[f"skill_{counter:04d}"] = new_skill
                        counter += 1
                
                final_content = {}
                counter = 1
                for content in st.session_state.intermediate_content.values():
                    for sentence in split_text(content['text']):
                        new_content = content.copy()
                        new_content['text'] = sentence
                        final_content[f"content_{counter:04d}"] = new_content
                        counter += 1
                
                st.session_state.final_skills_json = json.dumps(final_skills, ensure_ascii=False, indent=2)
                st.session_state.final_content_json = json.dumps(final_content, ensure_ascii=False, indent=2)
                st.session_state.final_counts = (len(final_skills), len(final_content))
                st.rerun()
        
        if st.session_state.final_skills_json:
            st.download_button("📥 Скачать frp_skills.json", st.session_state.final_skills_json.encode('utf-8'), 
                file_name="frp_skills.json", mime="application/json")
            st.download_button("📥 Скачать frp_content.json", st.session_state.final_content_json.encode('utf-8'), 
                file_name="frp_content.json", mime="application/json", key="dl_content")
            if 'final_counts' in st.session_state:
                st.info(f"Навыков: {st.session_state.final_counts[0]}, Содержания: {st.session_state.final_counts[1]}")
        
        if st.button("🔄 Начать заново", key='frp_reset'):
            st.session_state.extracted = False
            st.session_state.intermediate_skills = {}
            st.session_state.intermediate_content = {}
            st.session_state.sections_df = None
            st.session_state.original_pairs = []
            st.session_state.final_skills_json = None
            st.session_state.final_content_json = None
            st.session_state.excel_skills_bytes = None
            st.session_state.excel_content_bytes = None
            st.rerun()

# ============ РЕЖИМ: Кодификатор ============
elif mode == 'codifier':
    st.header("Извлечение из кодификатора")

    codifier_file = st.file_uploader("Загрузите Excel файл кодификатора", type=['xlsx', 'xls'], key='codifier_upload')
    subject_input = st.text_input("Предмет:", value="математика", key='codifier_subject')
    program_cod = st.radio("Программа:", ['базовый', 'профильный'], horizontal=True, key='codifier_program')

    if codifier_file:
        if st.button("🚀 Обработать кодификатор", type="primary", key='codifier_process'):
            with st.spinner("Обработка..."):
                file_content = codifier_file.read()
                xl_file = pd.ExcelFile(io.BytesIO(file_content))

                skills_sheet = next((n for n in xl_file.sheet_names if 'результат' in n.lower()), None)
                content_sheet = next((n for n in xl_file.sheet_names if 'содержан' in n.lower()), None)

                skills_dict = {}
                content_dict = {}

                if skills_sheet:
                    df_skills = pd.read_excel(io.BytesIO(file_content), sheet_name=skills_sheet, header=None)
                    skills_list = parse_codifier_sheet(df_skills, subject=subject_input.strip(), program=program_cod)
                    skills_dict = create_separate_elements(skills_list, 'skill')

                if content_sheet:
                    df_content = pd.read_excel(io.BytesIO(file_content), sheet_name=content_sheet, header=None)
                    content_list = parse_codifier_sheet(df_content, subject=subject_input.strip(), program=program_cod)
                    content_dict = create_separate_elements(content_list, 'content')

                st.session_state.final_skills_json = json.dumps(skills_dict, ensure_ascii=False, indent=2)
                st.session_state.final_content_json = json.dumps(content_dict, ensure_ascii=False, indent=2)
                st.session_state.final_counts = (len(skills_dict), len(content_dict))
                st.session_state.last_extraction_mode = 'codifier'
                st.rerun()

    if st.session_state.get('final_counts') and mode == 'codifier' and st.session_state.get('last_extraction_mode') == 'codifier':
        sk, ct = st.session_state.final_counts
        st.success(f"✅ Извлечено: {sk} навыков, {ct} содержания")
        col1, col2 = st.columns(2)
        with col1:
            if st.session_state.get('final_skills_json'):
                st.download_button("📥 Скачать навыки", st.session_state.final_skills_json.encode('utf-8'),
                    file_name="codifier_skills.json", mime="application/json", key="dl_cod_skills")
        with col2:
            if st.session_state.get('final_content_json'):
                st.download_button("📥 Скачать содержание", st.session_state.final_content_json.encode('utf-8'),
                    file_name="codifier_content.json", mime="application/json", key="dl_cod_content")
        st.caption("Данные также доступны в режиме «JSON → таблицы» для создания Excel.")

# ============ РЕЖИМ: PDF ============
elif mode == 'pdf':
    st.header("Извлечение из PDF")
    st.caption("Загрузка PDF → определение типа (ФРП/кодификатор) → извлечение таблиц с объединением по классу.")
    
    pdf_file = st.file_uploader("Загрузите PDF файл", type=['pdf'], key='pdf_upload')
    program_pdf = st.radio("Программа:", ['базовый', 'профильный'], horizontal=True, key='pdf_program')
    subject_pdf = st.text_input("Предмет (для кодификатора или если один предмет в ФРП):", value="математика", key='pdf_subject')
    
    if pdf_file:
        if st.button("Извлечь таблицы из PDF", type="primary", key='pdf_extract_btn'):
            try:
                import pdfplumber
            except ImportError:
                st.error("Установите pdfplumber: pip install pdfplumber")
            else:
                with st.spinner("Обработка PDF..."):
                    pdf_bytes = pdf_file.read()
                    doc_type, merged_dfs, stats = extract_and_merge_pdf_tables(pdf_bytes)
                    st.session_state.pdf_doc_type = doc_type
                    st.session_state.pdf_merged = merged_dfs
                    st.session_state.pdf_extracted = True
                    st.session_state.pdf_stats = stats
                    st.rerun()
    
    if st.session_state.get('pdf_extracted'):
        dt = st.session_state.pdf_doc_type
        stats = st.session_state.get('pdf_stats', {})
        merged = st.session_state.get('pdf_merged', [])
        
        st.success(f"Тип: **{dt.upper()}** | Таблиц извлечено: {stats.get('tables', 0)} | Классов/секций: {len(stats.get('classes', []))} — {', '.join(map(str, stats.get('classes', [])))}")
        
        col_save, col_process = st.columns(2)
        with col_save:
            if st.button("Сохранить в Excel", key='pdf_save_excel'):
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine='openpyxl') as wr:
                    for name, df in merged:
                        sn = str(name)[:31]
                        df.to_excel(wr, sheet_name=sn, index=False)
                buf.seek(0)
                st.download_button("📥 Скачать Excel", buf.getvalue(), file_name="pdf_extracted.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key='dl_pdf_excel')
        
        with col_process:
            if st.button("Обработать и извлечь в JSON", type="primary", key='pdf_process_btn'):
                all_skills, all_content = [], []
                subj = subject_pdf.strip() or 'документ'
                prog = program_pdf
                for cls_or_name, df in merged:
                    if dt == 'frp':
                        sk, ct = extract_frp_from_df(df, subj, prog)
                        all_skills.extend(sk)
                        all_content.extend(ct)
                    else:
                        items = parse_codifier_sheet(df, subject=subj, program=prog)
                        flat = str(df.iloc[:3].values).lower() if len(df) > 0 else ""
                        if "содержан" in flat and "результат" not in flat:
                            all_content.extend(items)
                        else:
                            all_skills.extend(items)
                if dt == 'codifier':
                    sk_dict = create_separate_elements(all_skills, 'skill') if all_skills else {}
                    ct_dict = create_separate_elements(all_content, 'content') if all_content else {}
                
                if dt == 'frp':
                    inter_sk = {f"skill_{i:04d}": s for i, s in enumerate(all_skills, 1)}
                    inter_ct = {f"content_{i:04d}": c for i, c in enumerate(all_content, 1)}
                    sec_data = []
                    seen = set()
                    for it in list(inter_sk.values()) + list(inter_ct.values()):
                        k = (it.get('section', '') or '', it.get('topic', '') or '')
                        if k not in seen:
                            sec_data.append({'Раздел': k[0], 'Тема': k[1]})
                            seen.add(k)
                    st.session_state.intermediate_skills = inter_sk
                    st.session_state.intermediate_content = inter_ct
                    st.session_state.sections_df = pd.DataFrame(sec_data)
                    st.session_state.original_pairs = [(r['Раздел'], r['Тема']) for _, r in st.session_state.sections_df.iterrows()]
                    st.session_state.extracted = True
                    st.session_state.last_extraction_mode = 'frp_table'
                    st.session_state.final_skills_json = None
                    st.session_state.final_content_json = None
                    st.info("Данные готовы. Переключитесь на режим «ФРП (таблица Excel)» для редактирования разделов/тем и сохранения в JSON.")
                else:
                    st.session_state.final_skills_json = json.dumps(sk_dict, ensure_ascii=False, indent=2)
                    st.session_state.final_content_json = json.dumps(ct_dict, ensure_ascii=False, indent=2)
                    st.session_state.final_counts = (len(sk_dict), len(ct_dict))
                    st.session_state.last_extraction_mode = 'codifier'
                    st.success(f"Извлечено: {len(sk_dict)} навыков, {len(ct_dict)} содержания. Скачайте JSON в режиме «Кодификатор» или «JSON → таблицы».")
                st.rerun()

# ============ РЕЖИМ: ФРП (текст) ============
elif mode == 'frp_text':
    st.header("Извлечение из ФРП (текст)")
    st.caption("Загрузка DOC/DOCX/TXT → поиск «содержание обучения» и «предметные результаты» → извлечение в JSON.")
    
    frp_text_file = st.file_uploader("Загрузите DOC, DOCX или TXT", type=['doc', 'docx', 'txt'], key='frp_text_upload')
    subject_text = st.text_input("Предмет (обязательно):", value="", placeholder="напр. русский язык, математика", key='frp_text_subject')
    program_text = st.radio("Программа:", ['базовый', 'профильный'], horizontal=True, key='frp_text_program')
    
    if st.button("Обработать", type="primary", key='frp_text_process'):
        if not frp_text_file:
            st.warning("Сначала загрузите файл.")
        elif not subject_text.strip():
            st.warning("Заполните поле «Предмет» перед обработкой.")
        else:
            try:
                raw = frp_text_file.read()
                text = _read_doc_or_txt(raw, frp_text_file.name)
                content_items, skills_items = extract_frp_from_text(text, subject_text.strip(), program_text)
                content_dict = {f"content_{i:04d}": c for i, c in enumerate(content_items, 1)}
                skills_dict = {f"skill_{i:04d}": s for i, s in enumerate(skills_items, 1)}
                st.session_state.final_skills_json = json.dumps(skills_dict, ensure_ascii=False, indent=2)
                st.session_state.final_content_json = json.dumps(content_dict, ensure_ascii=False, indent=2)
                st.session_state.final_counts = (len(skills_dict), len(content_dict))
                st.session_state.frp_text_prefix = True
                st.success(f"Извлечено: {len(skills_dict)} навыков, {len(content_dict)} содержания.")
                st.rerun()
            except Exception as e:
                st.error(f"Ошибка: {e}")
                import traceback
                st.code(traceback.format_exc())
    
    if st.session_state.get('frp_text_prefix') and st.session_state.get('final_skills_json'):
        st.download_button("📥 Скачать frp_text_skills.json", st.session_state.final_skills_json.encode('utf-8'),
            file_name="frp_text_skills.json", mime="application/json", key='dl_frp_text_skills')
        st.download_button("📥 Скачать frp_text_content.json", st.session_state.final_content_json.encode('utf-8'),
            file_name="frp_text_content.json", mime="application/json", key='dl_frp_text_content')
        if st.session_state.get('final_counts'):
            st.info(f"Навыков: {st.session_state.final_counts[0]}, Содержания: {st.session_state.final_counts[1]}")

# ============ РЕЖИМ: JSON → Excel ============
elif mode == 'json_to_excel':
    st.header("Преобразование JSON → Excel-таблицы")

    # Собираем доступные JSON из session
    available = []
    if st.session_state.get('final_skills_json'):
        data = json.loads(st.session_state.final_skills_json)
        available.append({'type': 'навыки', 'data': data, 'name': 'Из текущей сессии (навыки)', 'info': get_json_info(data)})
    if st.session_state.get('final_content_json'):
        data = json.loads(st.session_state.final_content_json)
        available.append({'type': 'содержание', 'data': data, 'name': 'Из текущей сессии (содержание)', 'info': get_json_info(data)})

    # Добавляем загруженные ранее (из available_jsons)
    for entry in st.session_state.get('available_jsons', []):
        available.append(entry)

    # Показываем доступные
    if available:
        st.subheader("Доступные JSON")
        for i, entry in enumerate(available):
            info = entry.get('info') or get_json_info(entry.get('data', {}))
            with st.expander(f"📄 {entry.get('name', f'JSON {i+1}')} — {info.get('type', '?')}, {info.get('count', 0)} записей"):
                st.write("**Предметы:**", ", ".join(info.get('subjects', ['—'])))
                st.write("**Классы:**", ", ".join(str(c) for c in info.get('classes', ['—'])))
        st.caption("Для добавления ещё — загрузите файл ниже.")
    else:
        st.caption("Нет доступных JSON. Загрузите файл или выполните извлечение в другом режиме.")

    # Загрузка дополнительных JSON
    extra_upload = st.file_uploader("Загрузить ещё JSON", type=['json'], key='json_extra_upload')
    extra_type = st.radio("Тип файла:", ['навыки', 'содержание'], horizontal=True, key='extra_json_type')
    if extra_upload and st.button("Добавить к списку", key='add_json_btn'):
        try:
            data = json.loads(extra_upload.read().decode('utf-8'))
            name = extra_upload.name or "Загруженный файл"
            existing_names = [a.get('name') for a in st.session_state.get('available_jsons', [])]
            if name in existing_names:
                st.warning("Файл с таким именем уже добавлен.")
            else:
                entry = {'type': extra_type, 'data': data, 'name': name, 'info': get_json_info(data)}
                st.session_state.available_jsons = st.session_state.get('available_jsons', []) + [entry]
                st.success("Файл добавлен в список.")
                st.rerun()
        except Exception as e:
            st.error(f"Ошибка: {e}")

    # Выбор и конвертация
    st.subheader("Создание Excel")
    skills_options = [a for a in available if a.get('type') == 'навыки']
    content_options = [a for a in available if a.get('type') == 'содержание']

    excel_col1, excel_col2 = st.columns(2)
    with excel_col1:
        st.markdown("**Навыки → Excel**")
        if skills_options:
            sel_skills = st.selectbox("Выберите JSON", range(len(skills_options)), format_func=lambda i: skills_options[i].get('name', f'Вариант {i+1}'), key='sel_skills')
            if st.button("Создать Excel — навыки", key='excel_skills_btn'):
                st.session_state.excel_skills_bytes = json_to_excel_sorted(skills_options[sel_skills]['data'], 'Навык')
                st.rerun()
            if st.session_state.excel_skills_bytes:
                st.download_button("📥 Скачать frp_skills.xlsx", st.session_state.excel_skills_bytes,
                    file_name="frp_skills.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key='dl_skills_xlsx')
        else:
            st.caption("Нет JSON с навыками.")

    with excel_col2:
        st.markdown("**Содержание → Excel**")
        if content_options:
            sel_content = st.selectbox("Выберите JSON", range(len(content_options)), format_func=lambda i: content_options[i].get('name', f'Вариант {i+1}'), key='sel_content')
            if st.button("Создать Excel — содержание", key='excel_content_btn'):
                st.session_state.excel_content_bytes = json_to_excel_sorted(content_options[sel_content]['data'], 'Содержание')
                st.rerun()
            if st.session_state.excel_content_bytes:
                st.download_button("📥 Скачать frp_content.xlsx", st.session_state.excel_content_bytes,
                    file_name="frp_content.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key='dl_content_xlsx')
        else:
            st.caption("Нет JSON с содержанием.")

# ============ РЕЖИМ: Объединение JSON ============
elif mode == 'json_merge':
    st.header("Объединение нескольких JSON в один")
    st.caption("Загружайте JSON одного типа (только навыки или только содержание). Сквозная нумерация при объединении.")
    
    if 'merge_jsons' not in st.session_state:
        st.session_state.merge_jsons = []
    
    merge_list = st.session_state.merge_jsons
    
    # Список загруженных
    if merge_list:
        types = set(e.get('type') for e in merge_list)
        if len(types) > 1:
            st.error("⚠️ Смешивать навыки и содержание нельзя! Загружайте файлы только одного типа. Очистите список и начните заново.")
        else:
            st.subheader("Загружено для объединения")
            for i, e in enumerate(merge_list):
                info = e.get('info') or get_json_info(e.get('data', {}))
                st.text(f"  {i+1}. {e.get('name', '?')} — {info.get('count', 0)} записей")
    
    # Загрузка (или «Загрузить ещё» если уже есть файлы)
    merge_upload = st.file_uploader("Загрузить ещё JSON" if merge_list else "Загрузить JSON", type=['json'], key='merge_upload')
    
    if merge_upload:
        if st.button("Добавить к списку", key='merge_add_btn'):
            try:
                data = json.loads(merge_upload.read().decode('utf-8'))
                detected = get_json_info(data)
                det_type = 'навыки' if detected.get('type') == 'навыки' else 'содержание'
                if merge_list and det_type != (merge_list[0].get('type') or ''):
                    st.warning("Этот файл содержит «содержание», а в списке — навыки (или наоборот). Смешивать нельзя!")
                else:
                    entry = {'type': det_type, 'data': data, 'name': merge_upload.name or "Файл", 'info': detected}
                    st.session_state.merge_jsons = merge_list + [entry]
                    st.success("Файл добавлен.")
                    st.rerun()
            except Exception as e:
                st.error(f"Ошибка: {e}")
    
    if merge_list:
        if st.button("Объединить", type="primary", key='merge_do_btn'):
            types = set(e.get('type') for e in merge_list)
            if len(types) > 1:
                st.error("В списке смешаны навыки и содержание. Удалите лишние файлы.")
            else:
                merged = {}
                counter = 1
                prefix = 'skill' if 'навыки' in (merge_list[0].get('type') or '') else 'content'
                for entry in merge_list:
                    for _key, item in sorted(entry['data'].items(), key=lambda x: x[0]):
                        merged[f"{prefix}_{counter:04d}"] = item
                        counter += 1
                st.session_state.merged_json_result = json.dumps(merged, ensure_ascii=False, indent=2)
                st.session_state.merged_json_type = prefix
                st.session_state.merged_json_count = len(merged)
                st.rerun()
        
        if st.button("Очистить список", key='merge_clear_btn'):
            st.session_state.merge_jsons = []
            st.session_state.merged_json_result = None
            st.rerun()
    
    if st.session_state.get('merged_json_result'):
        jr = st.session_state.merged_json_result
        cnt = st.session_state.get('merged_json_count', 0)
        st.success(f"Объединено: {cnt} записей")
        fname = "merged_skills.json" if st.session_state.get('merged_json_type') == 'skill' else "merged_content.json"
        st.download_button("📥 Скачать объединённый JSON", jr.encode('utf-8'), file_name=fname, mime="application/json", key='dl_merged')

# ============ РЕЖИМ: Слияние и сравнение JSON ============
elif mode == 'json_compare':
    st.header("🔗 Слияние и сравнение JSON файлов")
    st.caption("Загрузка ФРП (таблица), ФРП (текст) и кодификатора → автоматическое сравнение и объединение с интерактивным выбором")
    
    # Загрузка файлов
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("ФРП таблица")
        frp_table_file = st.file_uploader("Загрузить ФРП таблица", type=['json'], key='compare_frp_table_upload')
        if frp_table_file:
            try:
                data = json.loads(frp_table_file.read().decode('utf-8'))
                is_valid, message = validate_json_source(data, 'фрп_планирование')
                if is_valid:
                    # Добавляем префикс к ключам для различения источников
                    prefixed_data = add_prefix_to_keys(data, 'frp_table_')
                    st.session_state.compare_frp_table = {'name': frp_table_file.name, 'data': prefixed_data}
                    st.success(f"✅ {len(prefixed_data)} записей")
                else:
                    st.warning(f"⚠️ {message}")
            except Exception as e:
                st.error(f"Ошибка: {e}")
    
    with col2:
        st.subheader("ФРП текст")
        frp_text_file = st.file_uploader("Загрузить ФРП текст", type=['json'], key='compare_frp_text_upload')
        if frp_text_file:
            try:
                data = json.loads(frp_text_file.read().decode('utf-8'))
                is_valid, message = validate_json_source(data, 'фрп_текст')
                if is_valid:
                    # Добавляем префикс к ключам для различения источников
                    prefixed_data = add_prefix_to_keys(data, 'frp_text_')
                    st.session_state.compare_frp_text = {'name': frp_text_file.name, 'data': prefixed_data}
                    st.success(f"✅ {len(prefixed_data)} записей")
                else:
                    st.warning(f"⚠️ {message}")
            except Exception as e:
                st.error(f"Ошибка: {e}")
    
    with col3:
        st.subheader("Кодификатор")
        codifier_file = st.file_uploader("Загрузить кодификатор", type=['json'], key='compare_codifier_upload')
        if codifier_file:
            try:
                data = json.loads(codifier_file.read().decode('utf-8'))
                is_valid, message = validate_json_source(data, 'кодификатор')
                if is_valid:
                    # Добавляем префикс к ключам для различения источников
                    prefixed_data = add_prefix_to_keys(data, 'codifier_')
                    st.session_state.compare_codifier = {'name': codifier_file.name, 'data': prefixed_data}
                    st.success(f"✅ {len(prefixed_data)} записей")
                else:
                    st.warning(f"⚠️ {message}")
            except Exception as e:
                st.error(f"Ошибка: {e}")
    
    # Проверка наличия файлов
    frp_table_loaded = st.session_state.compare_frp_table is not None
    frp_text_loaded = st.session_state.compare_frp_text is not None
    codifier_loaded = st.session_state.compare_codifier is not None
    
    if not frp_table_loaded:
        st.info("ℹ️ Загрузите ФРП таблицу для начала сравнения. Без этого файла сравнение невозможно.")
    elif not frp_text_loaded and not codifier_loaded:
        st.info("ℹ️ Загрузите хотя бы один дополнительный файл (ФРП текст или кодификатор) для сравнения.")
    else:
        # Кнопки начала сравнения
        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            start_full = st.button("🚀 Начать сравнение и слияние", type="primary", key='compare_start_btn')
        with btn_col2:
            start_simple = st.button("⚡ Простое сравнение", key='compare_simple_btn', help="Без подбора раздела/темы по похожим — всё сразу в пилот или выбор раздела")
        
        if start_full or start_simple:
            st.session_state.compare_simple_mode = bool(start_simple)
            # Инициализация отчёта и статистики
            st.session_state.compare_report = []
            st.session_state.compare_stats = {
                'auto_merged': 0,
                'user_selected': 0,
                'both_saved': 0,
                'section_assigned_auto': 0,
                'section_assigned_user': 0
            }
            st.session_state.compare_pending_decisions = {}
            st.session_state.compare_current_class = None
            st.session_state.compare_fails = {}
            st.session_state.compare_for_choice = {}
            st.session_state.compare_for_section_topic = {}
            st.session_state.compare_for_section_only = {}
            st.session_state.compare_merged_result = None
            
            # Сохраняем исходное количество записей в исследуемых файлах
            st.session_state.compare_initial_counts = {}
            if frp_text_loaded:
                st.session_state.compare_initial_counts['frp_text'] = len(st.session_state.compare_frp_text['data'])
            if codifier_loaded:
                st.session_state.compare_initial_counts['codifier'] = len(st.session_state.compare_codifier['data'])
            
            # Эталон — исходный словарь, не изменяется. Рабочая копия — для добавления/объединения.
            etalon = copy.deepcopy(st.session_state.compare_frp_table['data'])
            st.session_state.compare_etalon_data = etalon
            st.session_state.compare_base_data = copy.deepcopy(etalon)
            # Определяем порядок сравнения
            if frp_table_loaded and frp_text_loaded and codifier_loaded:
                st.session_state.compare_iteration = 1
                st.session_state.compare_compare_data = st.session_state.compare_frp_text['data']
                st.session_state.compare_next_data = st.session_state.compare_codifier['data']
            elif frp_table_loaded and frp_text_loaded:
                st.session_state.compare_iteration = 1
                st.session_state.compare_compare_data = st.session_state.compare_frp_text['data']
                st.session_state.compare_next_data = None
            elif frp_table_loaded and codifier_loaded:
                st.session_state.compare_iteration = 1
                st.session_state.compare_compare_data = st.session_state.compare_codifier['data']
                st.session_state.compare_next_data = None
            
            st.rerun()
        
        if 'compare_iteration' in st.session_state:
            
            # CSS: текст навыков чёрный (не серый)
            st.markdown("""
                <style>
                div[data-testid="stVerticalBlock"] textarea[disabled] { color: #000 !important; -webkit-text-fill-color: #000 !important; }
                .stCaption { color: #000 !important; }
                </style>
                """, unsafe_allow_html=True)
            
            # Фейлы — в самое начало (все возможные)
            if st.session_state.compare_fails:
                with st.expander(f"⚠️ Фейлы — записи без предмета/класса/текста ({len(st.session_state.compare_fails)})", expanded=True):
                    for fk, frec in sorted(st.session_state.compare_fails.items()):
                        subj = (frec.get('subject') or '').strip() or '(пусто)'
                        cls = str(frec.get('class', '')) or '(пусто)'
                        txt = (frec.get('text') or '') or '(пусто)'
                        st.markdown(f"**{fk}** | предмет: {subj} | класс: {cls}")
                        st.write(f"Текст: {txt}")
            
            # Режим
            if st.session_state.get('compare_simple_mode'):
                st.info("⚡ **Простое сравнение**: совпадения и выбор из двух — как есть; остальное сразу в пилот при наличии раздела или выбор раздела.")
            # Проверка: compare_compare_data должен быть пуст после обработки
            remaining = st.session_state.get('compare_compare_data', {})
            if remaining:
                st.warning(f"⚠️ В compare_compare_data осталось {len(remaining)} записей — не должно остаться. Ключи: {list(remaining.keys())[:10]}{'…' if len(remaining) > 10 else ''}")
            else:
                st.success("✅ compare_compare_data пуст — все записи обработаны")
            
            # Записи без сходства: что с ними сделано
            if st.session_state.compare_report:
                no_match_ok_reports = [r for r in st.session_state.compare_report if r.get('action') == 'no_match_section_ok']
                pending_section = len(st.session_state.compare_for_section_only)
                if no_match_ok_reports or pending_section:
                    st.write("**Записи без сходства с эталоном:**")
                if no_match_ok_reports:
                    with st.expander(f"✅ Без совпадений, раздел совпал — автосохранено: {len(no_match_ok_reports)}", expanded=False):
                        for r in no_match_ok_reports:
                            txt = r.get('text', '')
                            st.write(f"**Текст:** {txt[:100] + ('…' if len(txt) > 100 else '')}")
                            st.write(f"Раздел: {r.get('section', '')} | {r.get('note', '')}")
                if pending_section > 0:
                    st.write(f"Ожидают выбора раздела: {pending_section} (см. блок «Записи без совпадений» ниже)")
                auto_merged_reports = [
                    r for r in st.session_state.compare_report 
                    if r.get('action') in ['auto_merge', 'auto_merge_new']
                ]
                if auto_merged_reports:
                    with st.expander(f"✅ Автоматически объединено записей: {len(auto_merged_reports)}", expanded=False):
                        for report_item in auto_merged_reports:
                            if report_item.get('action') == 'auto_merge':
                                st.write(f"**Объединено:**")
                                st.write(f"- **Эталонная:** {report_item.get('base_text', '')}")
                                st.write(f"- **Исследуемая:** {report_item.get('compare_text', '')}")
                                st.write(f"- Похожесть: {report_item.get('similarity', 0):.1%}")
                                if report_item.get('section'):
                                    st.write(f"- Раздел: {report_item.get('section')} | Тема: {report_item.get('topic')}")
            
            # Работа пользователя: фейлы, затем три словаря для выбора
            has_work = (st.session_state.compare_fails or st.session_state.compare_for_choice or
                        st.session_state.compare_for_section_topic or st.session_state.compare_for_section_only)
            if has_work:
                # Кнопка «Принять всё как есть» — добавляет все необработанные записи в базу как есть
                any_pending = (st.session_state.compare_for_choice or st.session_state.compare_for_section_topic or
                               st.session_state.compare_for_section_only)
                # Две кнопки: «Принять все изменения» (по текущему выбору) и «Принять всё как есть» (игнорируя выбор)
                if any_pending:
                    btn_col_a, btn_col_b = st.columns(2)
                    with btn_col_a:
                        accept_changes = st.button("✅ Принять все изменения", key="accept_all_changes",
                            help="Применить текущий выбор (радио, селекты) ко всем отображаемым записям")
                    with btn_col_b:
                        accept_as_is = st.button("✅ Принять всё как есть", key="accept_all_as_is",
                            help="Добавить все записи в базу без учёта выбора (как есть)")
                
                if accept_changes:
                    # Применяем текущий выбор из UI ко всем записям
                    base_data = st.session_state.compare_base_data
                    keys_to_del = []
                    for compare_key, decision in list(st.session_state.compare_for_choice.items()):
                        choice_key = f"choice_{compare_key}"
                        if choice_key in st.session_state:
                            selected = st.session_state[choice_key]
                            base_key = decision.get('base_key')
                            base_record = decision.get('base_record', {})
                            compare_record = decision.get('compare_record', {})
                            if selected == 'base' and base_key and base_key in base_data:
                                merged_rec = base_data[base_key].copy()
                                merged_rec['sources'] = list(set(merged_rec.get('sources', []) + compare_record.get('sources', [])))
                                base_data[base_key] = merged_rec
                            elif selected == 'compare' and base_key and base_key in base_data:
                                rec = compare_record.copy()
                                rec['section'] = base_record.get('section', '')
                                rec['topic'] = base_record.get('topic', '')
                                rec['sources'] = list(set(base_data[base_key].get('sources', []) + rec.get('sources', [])))
                                base_data[base_key] = rec
                            elif selected == 'both' and base_key:
                                rec = compare_record.copy()
                                rec['section'] = base_record.get('section', '')
                                rec['topic'] = base_record.get('topic', '')
                                # Объединяем источники
                                base_sources = base_data[base_key].get('sources', [])
                                compare_sources = compare_record.get('sources', [])
                                rec['sources'] = list(set(base_sources + compare_sources))
                                prefix = 'frp_text_' if ('фрп_текст' in (compare_record.get('sources') or [])) else 'codifier_'
                                max_num = 0
                                for k in base_data:
                                    m = re.search(r'(?:skill_|content_)(\d+)', k)
                                    if m: max_num = max(max_num, int(m.group(1)))
                                base_data[f"{prefix}skill_{max_num+1:04d}"] = rec
                            keys_to_del.append(('choice', compare_key))
                    # section_topic — восстанавливаем опции из similar_records, читаем индекс из radio
                    for compare_key, decision in list(st.session_state.compare_for_section_topic.items()):
                        rec = decision.get('record', {}).copy()
                        section_key = f"section_{compare_key}"
                        topic_key = f"topic_{compare_key}"
                        similar = decision.get('similar_records', [])
                        sections_count = {}
                        topics_count = {}
                        for sim in similar:
                            s = (sim.get('record', {}).get('section') or '').strip()
                            t = (sim.get('record', {}).get('topic') or '').strip()
                            if s: sections_count[s] = sections_count.get(s, 0) + 1
                            if t: topics_count[t] = topics_count.get(t, 0) + 1
                        unique_sections = [x[0] for x in sorted(sections_count.items(), key=lambda z: -z[1])]
                        unique_topics = [x[0] for x in sorted(topics_count.items(), key=lambda z: -z[1])]
                        section_options = [''] + unique_sections
                        topic_options = [''] + unique_topics
                        current_s = rec.get('section', '').strip()
                        current_t = rec.get('topic', '').strip()
                        if current_s and current_s not in unique_sections:
                            unique_sections = [current_s] + unique_sections
                            section_options = [''] + unique_sections
                        if current_t and current_t not in unique_topics:
                            unique_topics = [current_t] + unique_topics
                            topic_options = [''] + unique_topics
                        if section_key in st.session_state:
                            idx = st.session_state[section_key]
                            if isinstance(idx, int) and 0 <= idx < len(section_options):
                                rec['section'] = section_options[idx]
                        if topic_key in st.session_state:
                            idx = st.session_state[topic_key]
                            if isinstance(idx, int) and 0 <= idx < len(topic_options):
                                rec['topic'] = topic_options[idx]
                        prefix = 'frp_text_' if ('фрп_текст' in (rec.get('sources') or [])) else 'codifier_'
                        max_num = max((int(m.group(1)) for k in base_data for m in [re.search(r'(?:skill_|content_)(\d+)', k)] if m), default=0)
                        base_data[f"{prefix}skill_{max_num+1:04d}"] = rec
                        keys_to_del.append(('section_topic', compare_key))
                    # section_only — selectbox возвращает выбранную строку напрямую
                    for compare_key, decision in list(st.session_state.compare_for_section_only.items()):
                        rec = decision.get('record', {}).copy()
                        sel_key = f"new_section_{compare_key}"
                        if sel_key in st.session_state:
                            sel_val = st.session_state[sel_key]
                            if isinstance(sel_val, str):
                                rec['section'] = sel_val
                        prefix = 'frp_text_' if ('фрп_текст' in (rec.get('sources') or [])) else 'codifier_'
                        max_num = max((int(m.group(1)) for k in base_data for m in [re.search(r'(?:skill_|content_)(\d+)', k)] if m), default=0)
                        base_data[f"{prefix}skill_{max_num+1:04d}"] = rec
                        keys_to_del.append(('section_only', compare_key))
                    for kind, k in keys_to_del:
                        if kind == 'choice' and k in st.session_state.compare_for_choice:
                            del st.session_state.compare_for_choice[k]
                        elif kind == 'section_topic' and k in st.session_state.compare_for_section_topic:
                            del st.session_state.compare_for_section_topic[k]
                        elif kind == 'section_only' and k in st.session_state.compare_for_section_only:
                            del st.session_state.compare_for_section_only[k]
                    if keys_to_del:
                        st.session_state.compare_stats['section_assigned_user'] = st.session_state.compare_stats.get('section_assigned_user', 0) + len(keys_to_del)
                        if _check_and_transition_next_iteration():
                            st.success("✅ Все сравнения завершены!")
                        st.rerun()
                
                elif accept_as_is:
                    base_data = st.session_state.compare_base_data
                    prefix = 'merged_'
                    import re
                    max_num = max((int(m.group(1)) for k in base_data for m in [re.search(r'(?:skill_|content_)(\d+)', k)] if m), default=0)
                    to_add = []
                    for compare_key, decision in list(st.session_state.compare_for_choice.items()):
                        rec = decision.get('compare_record', decision.get('record'))
                        if rec:
                            to_add.append(('choice', compare_key, rec))
                    for compare_key, decision in list(st.session_state.compare_for_section_topic.items()):
                        rec = decision.get('record', {})
                        if rec:
                            to_add.append(('section_topic', compare_key, rec))
                    for compare_key, decision in list(st.session_state.compare_for_section_only.items()):
                        rec = decision.get('record', {})
                        if rec:
                            to_add.append(('section_only', compare_key, rec))
                    for kind, compare_key, rec in to_add:
                        max_num += 1
                        base_data[f"{prefix}skill_{max_num:04d}"] = rec.copy()
                        if kind == 'choice' and compare_key in st.session_state.compare_for_choice:
                            del st.session_state.compare_for_choice[compare_key]
                        elif kind == 'section_topic' and compare_key in st.session_state.compare_for_section_topic:
                            del st.session_state.compare_for_section_topic[compare_key]
                        elif kind == 'section_only' and compare_key in st.session_state.compare_for_section_only:
                            del st.session_state.compare_for_section_only[compare_key]
                    if to_add:
                        st.session_state.compare_stats['section_assigned_user'] = st.session_state.compare_stats.get('section_assigned_user', 0) + len(to_add)
                        if _check_and_transition_next_iteration():
                            st.success("✅ Все сравнения завершены!")
                        st.rerun()
                
                # Сличение двух и выбора (высокий порог сходства)
                if st.session_state.compare_for_choice:
                    st.subheader("Сличение двух и выбора (высокий порог сходства)")
                for compare_key, decision in list(st.session_state.compare_for_choice.items()):
                    if decision.get('type') == 'choice':
                            base_text = decision['base_record'].get('text', '')
                            compare_text = decision['compare_record'].get('text', '')
                            similarity = decision.get('similarity', 0)
                            base_section = decision['base_record'].get('section', 'не указан')
                            base_topic = decision['base_record'].get('topic', 'не указана')
                            
                            # Извлекаем номер записи из ключа
                            record_num = ''
                            if compare_key:
                                # Пытаемся извлечь номер из ключа вида "frp_text_skill_0001" или "codifier_skill_0001"
                                parts = compare_key.split('_')
                                if len(parts) >= 2 and parts[-1].isdigit():
                                    record_num = f" (№{parts[-1]})"
                                elif '_skill_' in compare_key or '_content_' in compare_key:
                                    # Извлекаем номер после skill_ или content_
                                    import re
                                    match = re.search(r'(?:skill_|content_)(\d+)', compare_key)
                                    if match:
                                        record_num = f" (№{match.group(1)})"
                            
                            st.write(f"**Исследуемая запись{record_num} совпала с эталонной (похожесть: {similarity:.1%})**")
                            st.write(f"**Эталонная запись:** Раздел: {base_section} | Тема: {base_topic}")
                            
                            base_prefix = "✅ Вариант 1 (эталонная): "
                            compare_prefix = "✅ Вариант 2 (исследуем): "
                            base_label = f"{base_prefix}{base_text}"
                            compare_label = f"{compare_prefix}{compare_text}"
                            
                            # Радиобаттоны для выбора с выровненными текстами
                            choice_key = f"choice_{compare_key}"
                            selected = st.radio(
                                "Выберите вариант:",
                                options=['base', 'compare', 'both'],
                                format_func=lambda x: {
                                    'base': base_label,
                                    'compare': compare_label,
                                    'both': "✅ Сохранить оба варианта"
                                }[x],
                                key=choice_key
                            )
                            
                            col_save1, col_save2 = st.columns(2)
                            with col_save1:
                                if st.button("💾 Сохранить выбранное", key=f"save_{choice_key}"):
                                    # Сохраняем выбранный вариант
                                    base_data = st.session_state.compare_base_data
                                    base_key = decision.get('base_key')
                                    
                                    if selected == 'base' or selected == 'both':
                                        # Обновляем базовую запись
                                        if base_key:
                                            merged_record = base_data[base_key].copy()
                                            compare_sources = decision['compare_record'].get('sources', [])
                                            base_sources = merged_record.get('sources', [])
                                            merged_record['sources'] = list(set(base_sources + compare_sources))
                                            base_data[base_key] = merged_record
                                    
                                    if selected == 'compare' or selected == 'both':
                                        # Добавляем сравниваемую запись
                                        # Раздел/тему всегда берём из эталонной записи
                                        base_section = decision['base_record'].get('section', '')
                                        base_topic = decision['base_record'].get('topic', '')
                                        
                                        if selected == 'compare':
                                            # Заменяем базовую
                                            if base_key:
                                                compare_record = decision['compare_record'].copy()
                                                compare_record['section'] = base_section
                                                compare_record['topic'] = base_topic
                                                base_sources = base_data[base_key].get('sources', [])
                                                compare_sources = compare_record.get('sources', [])
                                                compare_record['sources'] = list(set(base_sources + compare_sources))
                                                base_data[base_key] = compare_record
                                        else:
                                            # Добавляем как новую
                                            # Определяем префикс на основе источника
                                            compare_sources = decision['compare_record'].get('sources', [])
                                            if compare_sources and 'фрп_текст' in compare_sources:
                                                prefix = 'frp_text_'
                                            elif compare_sources and 'кодификатор' in compare_sources:
                                                prefix = 'codifier_'
                                            else:
                                                prefix = 'merged_'
                                            
                                            # Находим максимальный номер среди существующих ключей с таким префиксом
                                            max_num = 0
                                            for k in base_data.keys():
                                                if k.startswith(prefix) and ('skill_' in k or 'content_' in k):
                                                    try:
                                                        parts = k.split('_')
                                                        if len(parts) >= 3 and parts[-1].isdigit():
                                                            max_num = max(max_num, int(parts[-1]))
                                                    except:
                                                        pass
                                            
                                            new_key = f"{prefix}skill_{max_num + 1:04d}"
                                            compare_record = decision['compare_record'].copy()
                                            compare_record['section'] = base_section
                                            compare_record['topic'] = base_topic
                                            # Объединяем источники с базовой записью
                                            if base_key:
                                                base_sources = base_data[base_key].get('sources', [])
                                                compare_sources_list = compare_record.get('sources', [])
                                                compare_record['sources'] = list(set(base_sources + compare_sources_list))
                                            base_data[new_key] = compare_record
                                    
                                    # Обновляем статистику и журнал
                                    if selected == 'both':
                                        st.session_state.compare_stats['both_saved'] += 1
                                        st.session_state.compare_report.append({
                                            'action': 'both_saved',
                                            'base_text': base_text,
                                            'compare_text': compare_text
                                        })
                                    else:
                                        st.session_state.compare_stats['user_selected'] += 1
                                        st.session_state.compare_report.append({
                                            'action': 'user_selected',
                                            'selected': selected,
                                            'base_text': base_text,
                                            'compare_text': compare_text
                                        })
                                    
                                    # Удаляем из словаря — запись обработана
                                    if compare_key in st.session_state.compare_for_choice:
                                        del st.session_state.compare_for_choice[compare_key]
                                    if _check_and_transition_next_iteration():
                                        st.success("✅ Все сравнения завершены!" if st.session_state.compare_merged_result else "Переход к следующему источнику...")
                                    st.rerun()
                            
                            with col_save2:
                                pass
                
                # 3. Подбор раздела и темы (совпадения есть, но не очень близкие)
                if st.session_state.compare_for_section_topic:
                    st.subheader("Подбор раздела и темы")
                for compare_key, decision in list(st.session_state.compare_for_section_topic.items()):
                            record_text = decision['record'].get('text', '')
                            similar_records = decision.get('similar_records', [])
                            
                            record_num = ''
                            if compare_key:
                                # Пытаемся извлечь номер из ключа вида "frp_text_skill_0001" или "codifier_skill_0001"
                                parts = compare_key.split('_')
                                if len(parts) >= 2 and parts[-1].isdigit():
                                    record_num = f" (№{parts[-1]})"
                                elif '_skill_' in compare_key or '_content_' in compare_key:
                                    # Извлекаем номер после skill_ или content_
                                    import re
                                    match = re.search(r'(?:skill_|content_)(\d+)', compare_key)
                                    if match:
                                        record_num = f" (№{match.group(1)})"
                            
                            st.write(f"**Исследуемая запись{record_num}:**")
                            st.text_area("Текст записи:", value=record_text, height=60, key=f"text_section_{compare_key}", disabled=True)
                            
                            # Показываем 3 самые ближайшие эталонные записи
                            if similar_records:
                                st.write("**Три самые ближайшие эталонные записи:**")
                                for i, sim_rec in enumerate(similar_records, 1):
                                    rec = sim_rec.get('record', {})
                                    rec_text = rec.get('text', '')
                                    similarity = sim_rec.get('similarity', 0)
                                    section = rec.get('section', 'не указан')
                                    topic = rec.get('topic', 'не указана')
                                    
                                    st.write(f"{i}. **{rec_text}**")
                                    st.write(f"   Раздел: {section} | Тема: {topic} | Похожесть: {similarity:.1%}")
                            # Анализируем разделы и темы похожих записей с подсчётом частоты и хранением текстов записей
                            sections_count = {}
                            topics_count = {}
                            topics_records = {}  # Словарь: тема -> список текстов записей
                            for sim_rec in similar_records:
                                rec = sim_rec.get('record', {})
                                sec = rec.get('section', '').strip()
                                top = rec.get('topic', '').strip()
                                rec_text = rec.get('text', '').strip()
                                if sec:
                                    sections_count[sec] = sections_count.get(sec, 0) + 1
                                if top:
                                    topics_count[top] = topics_count.get(top, 0) + 1
                                    # Сохраняем текст записи для этой темы
                                    if top not in topics_records:
                                        topics_records[top] = []
                                    if rec_text:
                                        topics_records[top].append(rec_text)
                            
                            current_section = decision['record'].get('section', '').strip()
                            current_topic = decision['record'].get('topic', '').strip()
                            
                            st.write("**Текущая разметка записи:**")
                            st.write(f"- Раздел: {current_section if current_section else 'не размечено'}")
                            st.write(f"- Тема: {current_topic if current_topic else 'не размечено'}")
                            
                            if sections_count or topics_count:
                                st.write("**Разметка похожих записей:**")
                                # Сортируем по частоте (от большей к меньшей)
                                unique_sections = sorted(sections_count.items(), key=lambda x: x[1], reverse=True)
                                unique_sections = [s[0] for s in unique_sections]
                                unique_topics = sorted(topics_count.items(), key=lambda x: x[1], reverse=True)
                                unique_topics = [t[0] for t in unique_topics]
                                
                                # Показываем частоты для информации с полными текстами записей
                                sections_info = [f"{s} ({sections_count[s]})" for s in unique_sections]
                                topics_info = []
                                for t in unique_topics:
                                    count = topics_count[t]
                                    # Формируем список текстов записей для этой темы
                                    records_texts = topics_records.get(t, [])
                                    if records_texts:
                                        # Показываем все записи полностью
                                        texts_str = "; ".join([f'"{text}"' for text in records_texts])
                                        topics_info.append(f"{t} ({count}): {texts_str}")
                                    else:
                                        topics_info.append(f"{t} ({count})")
                                
                                st.write(f"- Разделы похожих: {', '.join(sections_info) if sections_info else 'нет'}")
                                st.write(f"- Темы похожих:")
                                for topic_info in topics_info:
                                    st.write(f"  • {topic_info}")
                                
                                # Проверяем, все ли одинаковые
                                if len(unique_sections) == 1 and len(unique_topics) == 1:
                                    # Все три эталонные записи имеют одинаковые раздел и тему - автоматически присваиваем
                                    assigned_section = unique_sections[0]
                                    assigned_topic = unique_topics[0]
                                    
                                    st.success(f"✅ Все три ближайшие эталонные записи имеют одинаковые раздел и тему. Присвоено: Раздел: {assigned_section} | Тема: {assigned_topic}")
                                    
                                    decision['record']['section'] = assigned_section
                                    decision['record']['topic'] = assigned_topic
                                    
                                    # Определяем префикс на основе источника
                                    compare_sources = decision['record'].get('sources', [])
                                    if compare_sources and 'фрп_текст' in compare_sources:
                                        prefix = 'frp_text_'
                                    elif compare_sources and 'кодификатор' in compare_sources:
                                        prefix = 'codifier_'
                                    else:
                                        prefix = 'merged_'
                                    
                                    # Находим максимальный номер среди существующих ключей с таким префиксом
                                    max_num = 0
                                    for k in st.session_state.compare_base_data.keys():
                                        if k.startswith(prefix) and ('skill_' in k or 'content_' in k):
                                            try:
                                                parts = k.split('_')
                                                if len(parts) >= 3 and parts[-1].isdigit():
                                                    max_num = max(max_num, int(parts[-1]))
                                            except:
                                                pass
                                    
                                    # Добавляем в merged_data
                                    new_key = f"{prefix}skill_{max_num + 1:04d}"
                                    st.session_state.compare_base_data[new_key] = decision['record']
                                    
                                    st.session_state.compare_stats['section_assigned_auto'] += 1
                                    st.session_state.compare_report.append({
                                        'action': 'section_assigned_auto',
                                        'text': record_text,
                                        'section': assigned_section,
                                        'topic': assigned_topic
                                    })
                                    if compare_key in st.session_state.compare_for_section_topic:
                                        del st.session_state.compare_for_section_topic[compare_key]
                                    if _check_and_transition_next_iteration():
                                        st.success("✅ Все сравнения завершены!" if st.session_state.compare_merged_result else "Переход к следующему источнику...")
                                    st.rerun()
                                else:
                                    # Разные - запрашиваем у пользователя
                                    # Добавляем исходный раздел/тему, если их нет в списке
                                    if current_section and current_section not in unique_sections:
                                        unique_sections = [current_section] + unique_sections
                                        sections_count[current_section] = 0  # для метки "(исходный)"
                                    if current_topic and current_topic not in unique_topics:
                                        unique_topics = [current_topic] + unique_topics
                                        topics_count[current_topic] = 0  # для метки "(исходный)"
                                    # Используем радиобаттоны для выбора раздела
                                    if unique_sections:
                                        st.write("**Выберите раздел:**")
                                        section_options = [''] + unique_sections
                                        section_labels = ['Не указывать раздел'] + [
                                            f"{s} ({sections_count.get(s, 0)}) [исходный]" if s == current_section
                                            else f"{s} ({sections_count.get(s, 0)})"
                                            for s in unique_sections
                                        ]
                                        
                                        section_key = f"section_{compare_key}"
                                        # Проверяем, есть ли уже значение в session_state
                                        if section_key in st.session_state:
                                            default_section_index = st.session_state[section_key]
                                            # Проверяем, что индекс в допустимом диапазоне
                                            if default_section_index >= len(section_options):
                                                default_section_index = 0
                                        else:
                                            # Используем index только если значения нет в session_state
                                            default_section_index = 0 if not current_section else (section_options.index(current_section) if current_section in section_options else 0)
                                        
                                        # Дополнительная проверка на всякий случай
                                        default_section_index = max(0, min(default_section_index, len(section_options) - 1))
                                        
                                        selected_section_idx = st.radio(
                                            "",
                                            options=range(len(section_options)),
                                            format_func=lambda i: section_labels[i],
                                            index=default_section_index,
                                            key=section_key
                                        )
                                        selected_section = section_options[selected_section_idx]
                                    else:
                                        selected_section = ''
                                    
                                    # Используем радиобаттоны для выбора темы (только названия, без текстов)
                                    if unique_topics:
                                        st.write("**Выберите тему:**")
                                        topic_options = [''] + unique_topics
                                        topic_labels = ['Не указывать тему'] + [
                                            f"{t} ({topics_count.get(t, 0)}) [исходный]" if t == current_topic
                                            else f"{t} ({topics_count.get(t, 0)})"
                                            for t in unique_topics
                                        ]
                                        
                                        topic_key = f"topic_{compare_key}"
                                        # Проверяем, есть ли уже значение в session_state
                                        if topic_key in st.session_state:
                                            default_topic_index = st.session_state[topic_key]
                                            # Проверяем, что индекс в допустимом диапазоне
                                            if default_topic_index >= len(topic_options):
                                                default_topic_index = 0
                                        else:
                                            # Используем index только если значения нет в session_state
                                            default_topic_index = 0 if not current_topic else (topic_options.index(current_topic) if current_topic in topic_options else 0)
                                        
                                        # Дополнительная проверка на всякий случай
                                        default_topic_index = max(0, min(default_topic_index, len(topic_options) - 1))
                                        
                                        selected_topic_idx = st.radio(
                                            "",
                                            options=range(len(topic_options)),
                                            format_func=lambda i: topic_labels[i],
                                            index=default_topic_index,
                                            key=topic_key
                                        )
                                        selected_topic = topic_options[selected_topic_idx]
                                    else:
                                        selected_topic = ''
                                    
                                    if st.button("💾 Сохранить разметку", key=f"save_section_{compare_key}"):
                                        decision['record']['section'] = selected_section
                                        decision['record']['topic'] = selected_topic
                                        
                                        # Определяем префикс на основе источника
                                        compare_sources = decision['record'].get('sources', [])
                                        if compare_sources and 'фрп_текст' in compare_sources:
                                            prefix = 'frp_text_'
                                        elif compare_sources and 'кодификатор' in compare_sources:
                                            prefix = 'codifier_'
                                        else:
                                            prefix = 'merged_'
                                        
                                        # Находим максимальный номер среди существующих ключей с таким префиксом
                                        max_num = 0
                                        for k in st.session_state.compare_base_data.keys():
                                            if k.startswith(prefix) and ('skill_' in k or 'content_' in k):
                                                try:
                                                    parts = k.split('_')
                                                    if len(parts) >= 3 and parts[-1].isdigit():
                                                        max_num = max(max_num, int(parts[-1]))
                                                except:
                                                    pass
                                        
                                        # Добавляем в merged_data
                                        new_key = f"{prefix}skill_{max_num + 1:04d}"
                                        st.session_state.compare_base_data[new_key] = decision['record']
                                        
                                        st.session_state.compare_stats['section_assigned_user'] += 1
                                        st.session_state.compare_report.append({
                                            'action': 'section_assigned_user',
                                            'text': record_text,
                                            'section': selected_section,
                                            'topic': selected_topic
                                        })
                                        
                                        if compare_key in st.session_state.compare_for_section_topic:
                                            del st.session_state.compare_for_section_topic[compare_key]
                                        if _check_and_transition_next_iteration():
                                            st.success("✅ Все сравнения завершены!" if st.session_state.compare_merged_result else "Переход к следующему источнику...")
                                        st.rerun()
                
                # 4. Выбор раздела (совпадений нет, раздел неизвестен)
                if st.session_state.compare_for_section_only:
                    st.subheader("Записи без совпадений, требуется выбрать раздел")
                for compare_key, decision in list(st.session_state.compare_for_section_only.items()):
                            record_text = decision['record'].get('text', '')
                            subject = decision.get('subject', '')
                            class_num = decision.get('class', '0')
                            record_num = ''
                            if compare_key:
                                parts = compare_key.split('_')
                                if len(parts) >= 2 and parts[-1].isdigit():
                                    record_num = f" (№{parts[-1]})"
                                elif '_skill_' in compare_key or '_content_' in compare_key:
                                    import re
                                    match = re.search(r'(?:skill_|content_)(\d+)', compare_key)
                                    if match:
                                        record_num = f" (№{match.group(1)})"
                            st.write(f"**Запись{record_num}:**")
                            st.text_area("Текст:", value=record_text, height=80, key=f"text_new_{compare_key}", disabled=True)
                            all_sections = decision.get('base_sections', [])
                            if not all_sections:
                                base_source = st.session_state.get('compare_etalon_data') or st.session_state.compare_base_data
                                base_records_list = [
                                    r for r in base_source.values()
                                    if (r.get('subject', '').strip().lower() == subject.strip().lower() and
                                        (class_num == '0' or (str(r.get('class', '')).strip() or '0') == class_num))
                                ]
                                sections_count = {}
                                for rec in base_records_list:
                                    s = (rec.get('section') or '').strip()
                                    if s:
                                        sections_count[s] = sections_count.get(s, 0) + 1
                                all_sections = sorted(sections_count.keys(), key=lambda x: (-sections_count.get(x, 0), x))
                                if not all_sections:
                                    for rec in base_source.values():
                                        s = (rec.get('section') or '').strip()
                                        if s:
                                            sections_count[s] = sections_count.get(s, 0) + 1
                                    all_sections = sorted(sections_count.keys(), key=lambda x: (-sections_count.get(x, 0), x))
                            current_section = decision['record'].get('section', '').strip()
                            current_topic = decision['record'].get('topic', '').strip()
                            st.write(f"Текущий раздел: {current_section or 'не указан'}" + (f" | Тема: {current_topic}" if current_topic else ""))
                            if all_sections:
                                section_options = [''] + all_sections
                                if current_section and current_section not in section_options:
                                    section_options = ['', current_section] + [s for s in all_sections]
                                default_idx = section_options.index(current_section) if current_section in section_options else 0
                                selected_section = st.selectbox(
                                    "Выберите раздел:",
                                    options=section_options,
                                    index=default_idx,
                                    key=f"new_section_{compare_key}",
                                    format_func=lambda x, cs=current_section: "Не указывать" if not x else (f"{x} (исходный)" if x == cs else x)
                                )
                            else:
                                selected_section = ''
                                st.info("Нет доступных разделов")
                            if st.button("💾 Сохранить", key=f"save_new_{compare_key}"):
                                decision['record']['section'] = selected_section
                                compare_sources = decision['record'].get('sources', [])
                                prefix = 'frp_text_' if (compare_sources and 'фрп_текст' in compare_sources) else 'codifier_' if (compare_sources and 'кодификатор' in compare_sources) else 'merged_'
                                max_num = 0
                                for k in st.session_state.compare_base_data.keys():
                                    if k.startswith(prefix) and ('skill_' in k or 'content_' in k):
                                        try:
                                            parts = k.split('_')
                                            if len(parts) >= 3 and parts[-1].isdigit():
                                                max_num = max(max_num, int(parts[-1]))
                                        except: pass
                                new_key = f"{prefix}skill_{max_num + 1:04d}"
                                st.session_state.compare_base_data[new_key] = decision['record']
                                st.session_state.compare_stats['section_assigned_user'] += 1
                                st.session_state.compare_report.append({
                                    'action': 'new_record_assigned',
                                    'compare_key': compare_key,
                                    'text': record_text,
                                    'section': selected_section,
                                    'topic': decision['record'].get('topic', '')
                                })
                                if compare_key in st.session_state.compare_for_section_only:
                                    del st.session_state.compare_for_section_only[compare_key]
                                if _check_and_transition_next_iteration():
                                    st.success("✅ Все сравнения завершены!" if st.session_state.compare_merged_result else "Переход к следующему источнику...")
                                st.rerun()
                
                # Когда все три словаря пусты (только фейлы могут остаться) — кнопка для перехода
                if not (st.session_state.compare_for_choice or st.session_state.compare_for_section_topic or st.session_state.compare_for_section_only):
                    if st.button("➡️ Перейти к следующей итерации или завершить", key="proceed_next_iter"):
                        if _check_and_transition_next_iteration():
                            st.success("✅ Сравнение завершено!" if st.session_state.compare_merged_result else "Переход к следующему источнику...")
                        st.rerun()
            
            else:
                # Если уже есть результат и нечего обрабатывать — не запускаем обработку заново (избегаем бесконечного цикла)
                already_done = (
                    st.session_state.get('compare_merged_result') is not None
                    and not st.session_state.get('compare_compare_data')
                    and not st.session_state.get('compare_next_data')
                )
                # Запускаем первую итерацию сравнения (только когда есть что обрабатывать)
                if not already_done and 'compare_base_data' in st.session_state:
                    base_data = st.session_state.compare_base_data
                    # Копируем, чтобы не портить исходные данные
                    compare_data = copy.deepcopy(st.session_state.compare_compare_data)
                    st.session_state.compare_compare_data = compare_data
                    
                    # Извлекаем записи без предмета/класса/текста
                    fails = extract_fails_and_clean(compare_data)
                    st.session_state.compare_fails.update(fails)
                    
                    etalon = st.session_state.get('compare_etalon_data') or base_data
                    merged_data, for_choice, for_section_topic, for_section_only = process_comparison_iteration(
                        base_data, compare_data,
                        st.session_state.compare_report,
                        st.session_state.compare_stats,
                        etalon_data=etalon,
                        simple_mode=st.session_state.get('compare_simple_mode', False)
                    )
                    
                    st.session_state.compare_base_data = merged_data
                    st.session_state.compare_for_choice = for_choice
                    st.session_state.compare_for_section_topic = for_section_topic
                    st.session_state.compare_for_section_only = for_section_only
                    
                    has_pending = for_choice or for_section_topic or for_section_only
                    if has_pending:
                        st.rerun()
                    else:
                        # Нет решений - переходим к следующей итерации или завершаем
                        if st.session_state.compare_next_data:
                            st.info("Первая итерация завершена без решений. Переходим к сравнению с кодификатором...")
                            st.session_state.compare_iteration = 2
                            st.session_state.compare_compare_data = copy.deepcopy(st.session_state.compare_next_data)
                            st.session_state.compare_next_data = None
                            
                            compare_data2 = st.session_state.compare_compare_data
                            fails2 = extract_fails_and_clean(compare_data2)
                            st.session_state.compare_fails.update(fails2)
                            
                            st.session_state.compare_etalon_data = copy.deepcopy(merged_data)
                            base_data2 = copy.deepcopy(merged_data)
                            merged_data2, for_choice2, for_section_topic2, for_section_only2 = process_comparison_iteration(
                                base_data2, compare_data2,
                                st.session_state.compare_report,
                                st.session_state.compare_stats,
                                etalon_data=st.session_state.compare_etalon_data,
                                simple_mode=st.session_state.get('compare_simple_mode', False)
                            )
                            
                            st.session_state.compare_base_data = merged_data2
                            st.session_state.compare_for_choice = for_choice2
                            st.session_state.compare_for_section_topic = for_section_topic2
                            st.session_state.compare_for_section_only = for_section_only2
                            
                            if for_choice2 or for_section_topic2 or for_section_only2:
                                st.rerun()
                            else:
                                st.session_state.compare_merged_result = merged_data2
                                st.success("✅ Все сравнения завершены!")
                            st.rerun()
                        else:
                            st.session_state.compare_merged_result = merged_data
                            st.success("✅ Сравнение завершено!")
                            st.rerun()
            
            if st.session_state.compare_stats:
                stats = st.session_state.compare_stats
                st.subheader("Статистика")
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Автообъединено", stats.get('auto_merged', 0))
                with col2:
                    st.metric("Выбрано пользователем", stats.get('user_selected', 0))
                with col3:
                    st.metric("Сохранено оба", stats.get('both_saved', 0))
                with col4:
                    st.metric("Раздел авто", stats.get('section_assigned_auto', 0))
                with col5:
                    st.metric("Раздел вручную", stats.get('section_assigned_user', 0))
            
            # Кнопка «Сохранить объединённое» — всегда доступна, при нажатии собирает всё в один JSON
            st.subheader("Результаты")
            if st.button("💾 Сохранить объединённое", key="save_merged_btn"):
                # 1. Берём всё обработанное (base_data)
                base_data = st.session_state.get('compare_base_data', {})
                all_records = list(base_data.values())
                # 2. Добавляем необработанное: for_choice, for_section_topic, for_section_only
                for compare_key, decision in st.session_state.get('compare_for_choice', {}).items():
                    rec = decision.get('compare_record', decision.get('record'))
                    if rec:
                        all_records.append(rec.copy())
                for compare_key, decision in st.session_state.get('compare_for_section_topic', {}).items():
                    rec = decision.get('record', {})
                    if rec:
                        all_records.append(rec.copy())
                for compare_key, decision in st.session_state.get('compare_for_section_only', {}).items():
                    rec = decision.get('record', {})
                    if rec:
                        all_records.append(rec.copy())
                # 3. Добавляем фейлы как есть
                for fk, frec in st.session_state.get('compare_fails', {}).items():
                    all_records.append(frec.copy())
                # Сквозная нумерация
                merged = {f"skill_{i+1:04d}": rec for i, rec in enumerate(all_records)}
                st.session_state.compare_merged_result = merged
                st.rerun()
            
            if st.session_state.compare_merged_result:
                merged_json = json.dumps(st.session_state.compare_merged_result, ensure_ascii=False, indent=2)
                st.download_button(
                    "📥 Скачать объединённый JSON",
                    merged_json.encode('utf-8'),
                    file_name="merged_compared.json",
                    mime="application/json",
                    key='dl_merged_compared'
                )
                report_text = "\n".join([
                    f"{i+1}. {json.dumps(item, ensure_ascii=False)}"
                    for i, item in enumerate(st.session_state.compare_report)
                ])
                st.download_button(
                    "📄 Скачать отчёт",
                    report_text.encode('utf-8'),
                    file_name="comparison_report.txt",
                    mime="text/plain",
                    key='dl_report'
                )

# ============ РЕЖИМ: Структурирование с помощью LLM ============
elif mode == 'llm_structure':
    st.header("🤖 Структурирование содержания с помощью LLM")
    st.caption("Загрузите JSON файл с элементами содержания. Модель предложит логичное разделение на разделы и темы.")
    
    # Инициализация session_state для LLM режима
    if 'llm_content_data' not in st.session_state:
        st.session_state.llm_content_data = None
    if 'llm_grouped_data' not in st.session_state:
        st.session_state.llm_grouped_data = None
    if 'llm_frp_structure' not in st.session_state:
        st.session_state.llm_frp_structure = None
    if 'llm_formatted_text' not in st.session_state:
        st.session_state.llm_formatted_text = None
    if 'llm_results' not in st.session_state:
        st.session_state.llm_results = {}  # {subject_class: [records]}
    if 'llm_prompt_template' not in st.session_state:
        st.session_state.llm_prompt_template = """Изучи представленные элементы содержания и предложи логичное разделение на разделы и темы внутри разделов.

Для каждого элемента содержания верни запись в формате JSON со следующими полями:
- "section": название раздела, который ты предлагаешь
- "frp_section": название раздела из ФРП, внутри которого должен быть расположен этот раздел (если возможно определить, иначе пустая строка)
- "topic": название темы внутри этого раздела
- "frp_topic": похожая/такая же/более охватывающая тема из ФРП (если есть, иначе пустая строка)

Верни результат в формате JSON массива объектов."""
    if 'llm_custom_prompt' not in st.session_state:
        st.session_state.llm_custom_prompt = None
    
    # Загрузка файла
    uploaded_file = st.file_uploader("Загрузите JSON файл с элементами содержания", type=['json'], key='llm_upload')
    
    if uploaded_file:
        try:
            data = json.loads(uploaded_file.read().decode('utf-8'))
            
            # Проверка, что это content
            sample_key = next(iter(data.keys()), '')
            if 'content' not in sample_key.lower():
                st.error("❌ Ошибка: Загруженный файл не содержит элементы содержания (content). Проверьте формат файла.")
            else:
                st.session_state.llm_content_data = data
                st.success(f"✅ Файл загружен: {len(data)} записей")
                
                # Группировка данных
                if st.button("📊 Подготовить данные для анализа", type="primary", key='llm_prepare'):
                    with st.spinner("Группировка данных..."):
                        grouped = group_content_by_structure(data)
                        frp_structure = get_frp_sections_and_topics(data)
                        formatted_text = format_content_text(grouped, frp_structure)
                        
                        st.session_state.llm_grouped_data = grouped
                        st.session_state.llm_frp_structure = frp_structure
                        st.session_state.llm_formatted_text = formatted_text
                        st.success("✅ Данные подготовлены!")
                        st.rerun()
        except json.JSONDecodeError as e:
            st.error(f"❌ Ошибка при чтении JSON файла: {e}")
        except Exception as e:
            st.error(f"❌ Ошибка: {e}")
            import traceback
            st.code(traceback.format_exc())
    
    # Показ подготовленных данных
    if st.session_state.llm_formatted_text:
        st.subheader("Подготовленный текст для анализа")
        with st.expander("📝 Просмотр текста", expanded=False):
            st.text_area("Текст", value=st.session_state.llm_formatted_text, height=300, disabled=True, key='llm_text_view')
        
        # Редактирование промпта
        if st.button("✏️ Редактировать промпт", key='llm_edit_prompt'):
            st.session_state.llm_show_prompt_editor = True
        
        if st.session_state.get('llm_show_prompt_editor', False):
            st.subheader("Редактирование промпта")
            edited_prompt = st.text_area(
                "Промпт для модели",
                value=st.session_state.llm_custom_prompt or st.session_state.llm_prompt_template,
                height=200,
                key='llm_prompt_editor'
            )
            col1, col2 = st.columns(2)
            with col1:
                if st.button("💾 Сохранить", key='llm_save_prompt'):
                    st.session_state.llm_custom_prompt = edited_prompt
                    st.session_state.llm_show_prompt_editor = False
                    st.success("Промпт сохранен!")
                    st.rerun()
            with col2:
                if st.button("↩️ Вернуться к исходному", key='llm_reset_prompt'):
                    st.session_state.llm_custom_prompt = None
                    st.session_state.llm_show_prompt_editor = False
                    st.rerun()
        
        # Проверка API ключа
        api_key = get_claude_api_key()
        if not api_key:
            st.warning("⚠️ Для работы с LLM необходимо ввести API ключ Claude в боковой панели.")
        else:
            # Обработка пачками (предмет + класс)
            if st.session_state.llm_grouped_data:
                st.subheader("Обработка данных")
                
                # Получаем список пар предмет+класс
                subject_class_pairs = []
                for subject in sorted(st.session_state.llm_grouped_data.keys()):
                    for class_num in sorted(st.session_state.llm_grouped_data[subject].keys(), 
                                          key=lambda x: int(x) if str(x).isdigit() else 0):
                        subject_class_pairs.append((subject, class_num))
                
                if subject_class_pairs:
                    st.write(f"Найдено {len(subject_class_pairs)} пар предмет+класс для обработки")
                    
                    # Формируем текст для каждой пары
                    for idx, (subject, class_num) in enumerate(subject_class_pairs):
                        pair_key = f"{subject}_{class_num}"
                        
                        st.markdown(f"---")
                        st.markdown(f"**{idx + 1}. Предмет: {subject}, Класс: {class_num}**")
                        
                        # Формируем текст для этой пары
                        pair_text_lines = []
                        pair_text_lines.append(f"предмет: {subject}")
                        pair_text_lines.append(f"класс: {class_num}")
                        
                        sections = st.session_state.llm_grouped_data[subject][class_num]
                        
                        # Записи с разделом и темой
                        for section in sorted(sections.keys()):
                            if section == 'без раздела':
                                continue
                            topics = sections[section]
                            for topic in sorted(topics.keys()):
                                if topic == 'без темы':
                                    continue
                                pair_text_lines.append(f"раздел: {section}")
                                pair_text_lines.append(f"тема: {topic}")
                                texts = topics[topic]
                                # Обрабатываем каждую запись отдельно: добавляем точку, если её нет
                                processed_texts = []
                                for text in texts:
                                    text = text.strip()
                                    if text:
                                        # Если не заканчивается точкой/восклицательным/вопросительным, добавляем точку
                                        if not text.rstrip().endswith(('.', '!', '?')):
                                            text = text.rstrip() + '.'
                                        processed_texts.append(text)
                                content_text = ' '.join(processed_texts)
                                pair_text_lines.append(content_text)
                                pair_text_lines.append("")
                        
                        # Записи с разделом, но без темы
                        for section in sorted(sections.keys()):
                            if section == 'без раздела':
                                continue
                            if 'без темы' in sections[section]:
                                pair_text_lines.append(f"раздел: {section}")
                                pair_text_lines.append("тема: без темы")
                                texts = sections[section]['без темы']
                                # Обрабатываем каждую запись отдельно: добавляем точку, если её нет
                                processed_texts = []
                                for text in texts:
                                    text = text.strip()
                                    if text:
                                        # Если не заканчивается точкой/восклицательным/вопросительным, добавляем точку
                                        if not text.rstrip().endswith(('.', '!', '?')):
                                            text = text.rstrip() + '.'
                                        processed_texts.append(text)
                                content_text = ' '.join(processed_texts)
                                pair_text_lines.append(content_text)
                                pair_text_lines.append("")
                        
                        # Записи без раздела
                        if 'без раздела' in sections:
                            pair_text_lines.append("раздел: без раздела")
                            all_no_section = []
                            for topic_texts in sections['без раздела'].values():
                                all_no_section.extend(topic_texts)
                            if all_no_section:
                                # Обрабатываем каждую запись отдельно: добавляем точку, если её нет
                                processed_texts = []
                                for text in all_no_section:
                                    text = text.strip()
                                    if text:
                                        # Если не заканчивается точкой/восклицательным/вопросительным, добавляем точку
                                        if not text.rstrip().endswith(('.', '!', '?')):
                                            text = text.rstrip() + '.'
                                        processed_texts.append(text)
                                content_text = ' '.join(processed_texts)
                                pair_text_lines.append(content_text)
                                pair_text_lines.append("")
                        
                        pair_text = '\n'.join(pair_text_lines)
                        
                        # Добавляем информацию о ФРП разделах и темах для этого предмета в начало
                        if st.session_state.llm_frp_structure and subject in st.session_state.llm_frp_structure:
                            frp_info_lines = ["По фрп имеются следующие разделы и темы:"]
                            for section, topics in st.session_state.llm_frp_structure[subject].items():
                                frp_info_lines.append(f"раздел: {section}")
                                for topic in topics:
                                    frp_info_lines.append(f"  {topic}")
                            frp_info_text = '\n'.join(frp_info_lines)
                            pair_text = frp_info_text + '\n\n' + pair_text
                        elif st.session_state.llm_frp_structure:
                            # Если есть ФРП структура, но не для этого предмета, добавляем общую информацию
                            frp_info_lines = ["По фрп имеются следующие разделы и темы:"]
                            for frp_subject, sections in st.session_state.llm_frp_structure.items():
                                for section, topics in sections.items():
                                    frp_info_lines.append(f"раздел: {section}")
                                    for topic in topics:
                                        frp_info_lines.append(f"  {topic}")
                            frp_info_text = '\n'.join(frp_info_lines)
                            pair_text = frp_info_text + '\n\n' + pair_text
                        
                        # Проверяем, обработана ли уже эта пара
                        if pair_key not in st.session_state.llm_results:
                            with st.expander(f"📄 Текст для обработки ({subject}, {class_num})", expanded=False):
                                st.text_area("", value=pair_text, height=200, disabled=True, key=f'llm_pair_text_{idx}')
                            
                            if st.button(f"🚀 Обработать с помощью LLM", key=f'llm_process_{idx}'):
                                with st.spinner(f"Обработка {subject}, {class_num}..."):
                                    prompt = st.session_state.llm_custom_prompt or st.session_state.llm_prompt_template
                                    full_prompt = prompt + "\n\n" + pair_text
                                    
                                    messages = [{
                                        "role": "user",
                                        "content": full_prompt
                                    }]
                                    
                                    verify_ssl = st.session_state.get('claude_verify_ssl', True)
                                    # Используем сохраненные параметры из проверки ключа
                                    model = st.session_state.get('claude_working_model', 'claude-sonnet-4-20250514')
                                    api_version = st.session_state.get('claude_working_api_version', '2023-06-01')
                                    response = call_claude_api(messages, api_key, model=model, api_version=api_version, verify_ssl=verify_ssl)
                                    if response:
                                        records = parse_llm_response(response, subject, class_num)
                                        if records:
                                            st.session_state.llm_results[pair_key] = records
                                            st.success(f"✅ Обработано: {len(records)} записей")
                                            st.rerun()
                                        else:
                                            st.error("Не удалось распарсить ответ модели. Проверьте формат ответа.")
                                            st.text_area("Ответ модели:", value=response, height=200, key=f'llm_response_{idx}')
                                    else:
                                        st.error("Ошибка при обращении к API")
                        else:
                            st.success(f"✅ Уже обработано: {len(st.session_state.llm_results[pair_key])} записей")
                            
                            # Показываем результаты в редактируемой таблице
                            st.subheader(f"Результаты для {subject}, {class_num}")
                            records = st.session_state.llm_results[pair_key]
                            
                            # Создаем DataFrame для редактирования
                            # Группируем по разделам, чтобы дублировать название раздела
                            df_data = []
                            current_section = None
                            for i, rec in enumerate(records):
                                section = rec.get('section', '')
                                # Если раздел изменился, запоминаем его
                                if section != current_section:
                                    current_section = section
                                
                                df_data.append({
                                    '№': i + 1,
                                    'Раздел': current_section or '',  # Всегда показываем раздел
                                    'Раздел ФРП': rec.get('frp_section', ''),
                                    'Тема': rec.get('topic', ''),
                                    'Тема ФРП': rec.get('frp_topic', '')
                                })
                            
                            df = pd.DataFrame(df_data)
                            
                            # Редактируемая таблица
                            edited_df = st.data_editor(
                                df,
                                use_container_width=True,
                                key=f'llm_editor_{idx}',
                                num_rows="dynamic",
                                column_config={
                                    'Раздел': st.column_config.TextColumn('Раздел', width='medium'),
                                    'Раздел ФРП': st.column_config.TextColumn('Раздел ФРП', width='medium'),
                                    'Тема': st.column_config.TextColumn('Тема', width='medium'),
                                    'Тема ФРП': st.column_config.TextColumn('Тема ФРП', width='medium'),
                                }
                            )
                            
                            if st.button(f"💾 Сохранить изменения", key=f'llm_save_{idx}'):
                                # Обновляем записи из отредактированной таблицы
                                updated_records = []
                                for i, row in edited_df.iterrows():
                                    if i < len(records):
                                        updated_rec = records[i].copy()
                                        updated_rec['section'] = str(row['Раздел']).strip()
                                        updated_rec['frp_section'] = str(row['Раздел ФРП']).strip()
                                        updated_rec['topic'] = str(row['Тема']).strip()
                                        updated_rec['frp_topic'] = str(row['Тема ФРП']).strip()
                                        updated_records.append(updated_rec)
                                    else:
                                        # Новая запись (если пользователь добавил строку)
                                        new_rec = {
                                            'subject': subject,
                                            'class': class_num,
                                            'section': str(row['Раздел']).strip(),
                                            'frp_section': str(row['Раздел ФРП']).strip(),
                                            'topic': str(row['Тема']).strip(),
                                            'frp_topic': str(row['Тема ФРП']).strip()
                                        }
                                        updated_records.append(new_rec)
                                
                                st.session_state.llm_results[pair_key] = updated_records
                                st.success("Изменения сохранены!")
                                st.rerun()
                
                # Объединение всех результатов и сохранение
                if len(st.session_state.llm_results) == len(subject_class_pairs):
                    st.markdown("---")
                    st.subheader("📥 Сохранение результатов")
                    
                    if st.button("💾 Объединить и сохранить JSON", type="primary", key='llm_save_all'):
                        # Объединяем все записи
                        all_records = []
                        for pair_key in sorted(st.session_state.llm_results.keys()):
                            records = st.session_state.llm_results[pair_key]
                            all_records.extend(records)
                        
                        # Создаем финальный JSON
                        final_json = {}
                        for i, rec in enumerate(all_records):
                            key = f"content_{i+1:04d}"
                            final_json[key] = {
                                'subject': rec.get('subject', ''),
                                'class': rec.get('class', ''),
                                'section': rec.get('section', ''),
                                'topic': rec.get('topic', ''),
                                'text': '',  # Текст будет добавлен на следующем этапе
                                'sources': ['llm_structure']
                            }
                        
                        st.session_state.llm_final_json = final_json
                        st.success(f"✅ Объединено {len(all_records)} записей")
                        st.rerun()
                    
                    if st.session_state.get('llm_final_json'):
                        final_json_str = json.dumps(st.session_state.llm_final_json, ensure_ascii=False, indent=2)
                        st.download_button(
                            "📥 Скачать JSON файл",
                            final_json_str.encode('utf-8'),
                            file_name="llm_structured_content.json",
                            mime="application/json",
                            key='llm_download_json'
                        )
