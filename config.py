# -*- coding: utf-8 -*-
"""Общие настройки ДЗ №3: хранилище hh-lake, вариант 10 «Аналитик», правило нормализации навыков.

Условие варианта и нормализация навыков в MapReduce берутся отсюда; в DuckDB они
написаны отдельно на SQL (duckdb_counts.py), а verify.py проверяет, что оба способа
дают одинаковый результат.
"""
from pathlib import Path

import duckdb

ROOT = Path(__file__).parent
BUCKET = "hh-lake"
RAW_KEY = "raw/it_vacancies_2024.csv"
PARQUET_KEY = "curated/it_vacancies_2024.parquet"
LOCAL_RAW_CSV = ROOT.parent / "hw-hdfs" / "it_vacancies_2024.csv"

S3_ENDPOINT = "127.0.0.1:9000"
S3_ACCESS_KEY = "rustfsadmin"  # тестовые ключи локального кластера из ДЗ №2
S3_SECRET_KEY = "rustfsadmin"

# Вариант 10 «Аналитик»: профессиональные роли hh, кроме «Руководитель отдела аналитики»
VARIANT_ROLES = frozenset({
    "Аналитик",
    "Системный аналитик",
    "Бизнес-аналитик",
    "BI-аналитик, аналитик данных",
    "Продуктовый аналитик",
})

SKILL_SEPARATOR = ";"
SKILL_STRIP_CHARS = " \t\r\n\xa0"
STACK_SIZES = (1, 2, 3)


def normalize_skills(raw: str) -> list[str]:
    """Навыки вакансии: нижний регистр, без пробелов по краям, без пустых и повторов, по алфавиту."""
    skills = {part.strip(SKILL_STRIP_CHARS).lower() for part in raw.split(SKILL_SEPARATOR)}
    skills.discard("")
    return sorted(skills)


def result_key(method: str, size: int) -> str:
    return f"results/{method}/skills_k{size}.csv"


def duckdb_connection(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """DuckDB с httpfs и секретом для чтения s3://hh-lake из локального RustFS."""
    connection = duckdb.connect()
    if threads:
        connection.execute(f"SET threads = {int(threads)}")
    connection.execute("LOAD httpfs")
    connection.execute(f"""
        CREATE SECRET rustfs (TYPE s3, KEY_ID '{S3_ACCESS_KEY}', SECRET '{S3_SECRET_KEY}',
            ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')
    """)
    return connection


if __name__ == "__main__":
    assert normalize_skills(" SQL; python;Python ;\xa0Excel ;; ") == ["excel", "python", "sql"]
    assert normalize_skills("") == []
    print("config self-check ok")
