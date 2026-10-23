# -*- coding: utf-8 -*-
"""Этап 3, способ 2: частоты навыков, пар и троек варианта 10 одним SQL в DuckDB.

DuckDB читает тот же объект s3://hh-lake/raw/it_vacancies_2024.csv через httpfs (или, для
бонусного сравнения, его Parquet-копию). Условие варианта и нормализация навыков написаны
здесь на SQL самостоятельно, без кода MapReduce. Итог пишется в
s3://hh-lake/results/duckdb/skills_k{1,2,3}.csv.
"""
import argparse
import time

from config import BUCKET, PARQUET_KEY, RAW_KEY, STACK_SIZES, duckdb_connection, result_key

SOURCES = {
    "csv": f"read_csv('s3://{BUCKET}/{RAW_KEY}', header = true, all_varchar = true)",
    "parquet": f"read_parquet('s3://{BUCKET}/{PARQUET_KEY}')",
}

VACANCY_SKILLS_SQL = """
CREATE TEMP TABLE vacancy_skills AS
WITH variant AS (
    SELECT vacancy_id, key_skills
    FROM {source}
    WHERE role IN ('Аналитик', 'Системный аналитик', 'Бизнес-аналитик',
                   'BI-аналитик, аналитик данных', 'Продуктовый аналитик')
),
exploded AS (
    SELECT vacancy_id,
           lower(trim(unnest(string_split(coalesce(key_skills, ''), ';')),
                      ' ' || chr(9) || chr(10) || chr(13) || chr(160))) AS skill
    FROM variant
)
SELECT DISTINCT vacancy_id, skill FROM exploded WHERE skill <> ''
"""

COMBINATION_SQL = {
    1: """SELECT a.skill AS skill_1, count(*) AS count
          FROM vacancy_skills a GROUP BY ALL""",
    2: """SELECT a.skill AS skill_1, b.skill AS skill_2, count(*) AS count
          FROM vacancy_skills a JOIN vacancy_skills b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
          GROUP BY ALL""",
    3: """SELECT a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3, count(*) AS count
          FROM vacancy_skills a
          JOIN vacancy_skills b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
          JOIN vacancy_skills c ON b.vacancy_id = c.vacancy_id AND b.skill < c.skill
          GROUP BY ALL""",
}


def run(source: str, threads: int | None, top: int) -> None:
    connection = duckdb_connection(threads)
    timings = {}
    started = time.perf_counter()
    connection.execute(VACANCY_SKILLS_SQL.format(source=SOURCES[source]))
    timings["read+filter+explode"] = time.perf_counter() - started
    vacancies = connection.execute("SELECT count(DISTINCT vacancy_id), count(*) FROM vacancy_skills").fetchone()
    print(f"source: {source}; vacancies with skills: {vacancies[0]:,}; (vacancy, skill) rows: {vacancies[1]:,}")
    for size in STACK_SIZES:
        step = time.perf_counter()
        order = ", ".join(f"skill_{i}" for i in range(1, size + 1))
        connection.execute(f"CREATE TEMP TABLE k{size} AS {COMBINATION_SQL[size]}")
        connection.execute(f"COPY (SELECT * FROM k{size} ORDER BY count DESC, {order}) "
                           f"TO 's3://{BUCKET}/{result_key('duckdb', size)}' (HEADER, DELIMITER ',')")
        timings[f"k={size} count+write"] = time.perf_counter() - step
        distinct, occurrences = connection.execute(f"SELECT count(*), sum(count) FROM k{size}").fetchone()
        print(f"\nk={size}: distinct={distinct:,} occurrences={occurrences:,}")
        for *key, count in connection.execute(
                f"SELECT * FROM k{size} ORDER BY count DESC, {order} LIMIT {top}").fetchall():
            print(f"  {count:6d}  {' + '.join(key)}")
    print("\ntimings: " + ", ".join(f"{name} {seconds:.2f} s" for name, seconds in timings.items()))


def main() -> None:
    parser = argparse.ArgumentParser(description="DuckDB: навыки, пары и тройки (вариант 10 «Аналитик»)")
    parser.add_argument("--source", choices=SOURCES, default="csv")
    parser.add_argument("--threads", type=int)
    parser.add_argument("--top", type=int, default=10)
    arguments = parser.parse_args()
    run(arguments.source, arguments.threads, arguments.top)


if __name__ == "__main__":
    main()
