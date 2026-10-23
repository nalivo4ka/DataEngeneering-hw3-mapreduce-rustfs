# -*- coding: utf-8 -*-
"""Этапы 5–6: влияние сочетаний навыков на медианную зарплату и сравнение трёх регионов.

Данные — тот же объект s3://hh-lake/raw/it_vacancies_2024.csv, вариант 10 «Аналитик».
Зарплата: только RUR; середина вилки (или единственная граница); «до вычета налогов» приводится
к «на руки» умножением на 0,87; выбросы вне [15 тыс., 1 млн] ₽ исключаются.

Для зарплатного анализа повторные публикации одного объявления (тот же работодатель, название,
навыки и вилка — обычно одна вакансия в десятках городов) считаются одним предложением, иначе
массовые объявления с длинными списками навыков забивают таблицы своими сочетаниями.
Прибавка пары = медиана предложений с парой − медиана предложений с БОЛЕЕ частым навыком пары (база).
Прибавка тройки = медиана тройки − медиана самой частой из трёх её пар (база).
Сочетание учитывается, если у него не меньше MIN_SUPPORT предложений с зарплатой
от не менее чем MIN_EMPLOYERS разных работодателей.
Таблицы печатаются и сохраняются в out/*.csv и s3://hh-lake/results/analysis/.
"""
from pathlib import Path

from config import BUCKET, RAW_KEY, duckdb_connection

OUT = Path(__file__).parent / "out"
MIN_SUPPORT = 30
MIN_EMPLOYERS = 10
TOP_N = 10
REGIONS = ("Москва", "Санкт-Петербург", "Екатеринбург")
NET_FROM_GROSS = 0.87
SALARY_MIN, SALARY_MAX = 15_000, 1_000_000

PREPARE_SQL = f"""
CREATE TEMP TABLE vac AS
WITH raw AS (
    SELECT vacancy_id, area, experience, schedule, key_skills, employer_id, name,
           salary_from, salary_to, currency, gross,
           CASE WHEN currency = 'RUR' THEN
               (CASE WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
                     THEN (salary_from::DOUBLE + salary_to::DOUBLE) / 2
                     ELSE coalesce(salary_from::DOUBLE, salary_to::DOUBLE) END)
               * CASE WHEN gross = 'true' THEN {NET_FROM_GROSS} ELSE 1 END
           END AS salary_raw
    FROM read_csv('s3://{BUCKET}/{RAW_KEY}', header = true, all_varchar = true)
    WHERE role IN ('Аналитик', 'Системный аналитик', 'Бизнес-аналитик',
                   'BI-аналитик, аналитик данных', 'Продуктовый аналитик')
)
SELECT *, CASE WHEN salary_raw BETWEEN {SALARY_MIN} AND {SALARY_MAX} THEN salary_raw END AS salary,
       (experience IN ('От 3 до 6 лет', 'Более 6 лет'))::INT AS senior
FROM raw;

CREATE TEMP TABLE vs AS
SELECT DISTINCT vacancy_id, skill FROM (
    SELECT vacancy_id, lower(trim(unnest(string_split(coalesce(key_skills, ''), ';')),
                                  ' ' || chr(9) || chr(10) || chr(13) || chr(160))) AS skill
    FROM vac)
WHERE skill <> '';

CREATE TEMP TABLE skill_freq AS SELECT skill, count(*) AS n FROM vs GROUP BY skill;
CREATE TEMP TABLE pair_freq AS
SELECT a.skill AS s1, b.skill AS s2, count(*) AS n
FROM vs a JOIN vs b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill GROUP BY ALL;

CREATE TEMP TABLE offers AS
SELECT * FROM vac WHERE salary IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY employer_id, name, key_skills, salary_from, salary_to, currency, gross
                           ORDER BY vacancy_id) = 1;

CREATE TEMP TABLE vss AS
SELECT vs.vacancy_id, vs.skill, offers.salary, offers.senior, offers.employer_id
FROM vs JOIN offers USING (vacancy_id);

CREATE TEMP TABLE skill_sal AS
SELECT skill, count(*) AS n_sal, count(DISTINCT employer_id) AS n_emp, median(salary) AS med, avg(senior) AS senior
FROM vss GROUP BY skill;
CREATE TEMP TABLE pair_sal AS
SELECT a.skill AS s1, b.skill AS s2, count(*) AS n_sal, count(DISTINCT a.employer_id) AS n_emp,
       median(a.salary) AS med, avg(a.senior) AS senior
FROM vss a JOIN vss b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill GROUP BY ALL;
CREATE TEMP TABLE triple_sal AS
SELECT a.skill AS s1, b.skill AS s2, c.skill AS s3, count(*) AS n_sal, count(DISTINCT a.employer_id) AS n_emp,
       median(a.salary) AS med,
       avg(a.senior) AS senior
FROM vss a
JOIN vss b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
JOIN vss c ON b.vacancy_id = c.vacancy_id AND b.skill < c.skill
GROUP BY ALL HAVING count(*) >= {MIN_SUPPORT} AND count(DISTINCT a.employer_id) >= {MIN_EMPLOYERS};
"""

PAIR_EFFECT_SQL = f"""
WITH p AS (
    SELECT ps.*, CASE WHEN f1.n >= f2.n THEN ps.s1 ELSE ps.s2 END AS base,
                 CASE WHEN f1.n >= f2.n THEN ps.s2 ELSE ps.s1 END AS added
    FROM pair_sal ps JOIN skill_freq f1 ON f1.skill = ps.s1 JOIN skill_freq f2 ON f2.skill = ps.s2
    WHERE ps.n_sal >= {MIN_SUPPORT} AND ps.n_emp >= {MIN_EMPLOYERS}
)
SELECT p.base, p.added, p.n_sal, p.n_emp, round(p.med) AS med, b.n_sal AS base_n, round(b.med) AS base_med,
       round(p.med - b.med) AS uplift, round(100 * (p.med - b.med) / b.med, 1) AS uplift_pct,
       round(100 * p.senior) AS senior_pct, round(100 * b.senior) AS base_senior_pct
FROM p JOIN skill_sal b ON b.skill = p.base
"""

TRIPLE_EFFECT_SQL = """
WITH t AS (
    SELECT ts.*, f12.n AS n12, f13.n AS n13, f23.n AS n23
    FROM triple_sal ts
    JOIN pair_freq f12 ON f12.s1 = ts.s1 AND f12.s2 = ts.s2
    JOIN pair_freq f13 ON f13.s1 = ts.s1 AND f13.s2 = ts.s3
    JOIN pair_freq f23 ON f23.s1 = ts.s2 AND f23.s2 = ts.s3
), b AS (
    SELECT *,
        CASE WHEN n12 >= n13 AND n12 >= n23 THEN [s1, s2] WHEN n13 >= n23 THEN [s1, s3] ELSE [s2, s3] END AS base,
        CASE WHEN n12 >= n13 AND n12 >= n23 THEN s3 WHEN n13 >= n23 THEN s2 ELSE s1 END AS added
    FROM t
)
SELECT b.base[1] || ' + ' || b.base[2] AS base, b.added, b.n_sal, b.n_emp, round(b.med) AS med,
       ps.n_sal AS base_n, round(ps.med) AS base_med,
       round(b.med - ps.med) AS uplift, round(100 * (b.med - ps.med) / ps.med, 1) AS uplift_pct,
       round(100 * b.senior) AS senior_pct, round(100 * ps.senior) AS base_senior_pct
FROM b JOIN pair_sal ps ON ps.s1 = b.base[1] AND ps.s2 = b.base[2]
"""

MASS_POSTINGS_SQL = """
SELECT employer_id, name, count(*) AS postings, count(DISTINCT area) AS cities,
       coalesce(len(string_split(any_value(key_skills), ';')), 0) AS skills, round(any_value(salary)) AS salary_net
FROM vac WHERE salary IS NOT NULL
GROUP BY employer_id, name, key_skills, salary_from, salary_to, currency, gross
HAVING count(*) >= 10 ORDER BY postings DESC LIMIT 8
"""

REGION_LIST = ", ".join(f"'{region}'" for region in REGIONS)

REGION_METRICS_SQL = f"""
WITH per_vacancy AS (
    SELECT vac.*, (SELECT count(*) FROM vs WHERE vs.vacancy_id = vac.vacancy_id) AS n_skills FROM vac
), scoped AS (
    SELECT area AS region, * EXCLUDE (area) FROM per_vacancy WHERE area IN ({REGION_LIST})
    UNION ALL SELECT 'Все регионы' AS region, * EXCLUDE (area) FROM per_vacancy
)
SELECT region, count(*) AS vacancies,
       round(100 * avg((n_skills > 0)::INT), 1) AS with_skills_pct,
       round(avg(n_skills) FILTER (WHERE n_skills > 0), 1) AS skills_per_vacancy,
       count(salary) AS with_salary,
       round(quantile_cont(salary, 0.25)) AS p25, round(median(salary)) AS median, round(quantile_cont(salary, 0.75)) AS p75,
       round(100 * avg((schedule = 'Удаленная работа')::INT), 1) AS remote_pct,
       round(100 * avg((experience = 'Нет опыта')::INT), 1) AS no_experience_pct,
       round(100 * avg(senior), 1) AS senior_pct
FROM scoped GROUP BY region ORDER BY vacancies DESC
"""

REGION_TOP_SQL = f"""
WITH rvs AS (SELECT vs.*, vac.area, vac.salary FROM vs JOIN vac USING (vacancy_id) WHERE vac.area IN ({REGION_LIST})),
base AS (SELECT area, count(DISTINCT vacancy_id) AS with_skills FROM rvs GROUP BY area),
combos AS (
    SELECT area, 1 AS size, skill AS combo, count(*) AS n, median(salary) AS med FROM rvs GROUP BY ALL
    UNION ALL
    SELECT a.area, 2, a.skill || ' + ' || b.skill, count(*), median(a.salary)
    FROM rvs a JOIN rvs b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill GROUP BY ALL
    UNION ALL
    SELECT a.area, 3, a.skill || ' + ' || b.skill || ' + ' || c.skill, count(*), median(a.salary)
    FROM rvs a JOIN rvs b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
               JOIN rvs c ON b.vacancy_id = c.vacancy_id AND b.skill < c.skill GROUP BY ALL
), ranked AS (
    SELECT c.*, row_number() OVER (PARTITION BY c.area, c.size ORDER BY c.n DESC, c.combo) AS rank,
           round(100.0 * c.n / base.with_skills, 1) AS share_pct
    FROM combos c JOIN base USING (area)
)
SELECT area AS region, size, rank, combo, n, share_pct, round(med) AS median_salary
FROM ranked WHERE rank <= 5 ORDER BY region, size, rank
"""

REGION_DISTINCT_SQL = f"""
WITH rvs AS (SELECT vs.*, vac.area FROM vs JOIN vac USING (vacancy_id) WHERE vac.area IN ({REGION_LIST})),
totals AS (SELECT area, count(DISTINCT vacancy_id) AS with_skills FROM rvs GROUP BY area),
counts AS (SELECT area, skill, count(*) AS n FROM rvs GROUP BY ALL),
shares AS (
    SELECT c.area, c.skill, c.n, c.n / t.with_skills AS share,
           (sum(c.n) OVER (PARTITION BY c.skill) - c.n)
             / (sum(t.with_skills) OVER (PARTITION BY c.skill) - t.with_skills) AS other_share
    FROM counts c JOIN totals t USING (area)
)
SELECT area AS region, skill, n, round(100 * share, 1) AS share_pct, round(100 * other_share, 1) AS other_regions_pct,
       round(share / nullif(other_share, 0), 2) AS ratio
FROM shares WHERE n >= {MIN_SUPPORT} AND share >= 0.01 AND other_share > 0
QUALIFY row_number() OVER (PARTITION BY area ORDER BY share / other_share DESC) <= 5
ORDER BY region, ratio DESC
"""

REGION_SKILL_SALARY_SQL = f"""
WITH top_skills AS (SELECT skill FROM skill_freq ORDER BY n DESC LIMIT 8)
SELECT vs.skill, vac.area AS region, count(*) AS n, round(median(vac.salary)) AS median_salary
FROM vs JOIN vac USING (vacancy_id)
WHERE vac.salary IS NOT NULL AND vac.area IN ({REGION_LIST}) AND vs.skill IN (SELECT skill FROM top_skills)
GROUP BY ALL ORDER BY vs.skill, region
"""


class SalaryAnalysis:
    """Считает таблицы этапов 5–6 в DuckDB и сохраняет их локально и в hh-lake."""

    def __init__(self) -> None:
        self._connection = duckdb_connection()
        self._connection.execute(PREPARE_SQL)

    def overview(self) -> None:
        total, with_skills, with_salary, both, median = self._connection.execute("""
            SELECT count(*), count(*) FILTER (WHERE vacancy_id IN (SELECT vacancy_id FROM vs)),
                   count(salary), count(salary) FILTER (WHERE vacancy_id IN (SELECT vacancy_id FROM vs)),
                   round(median(salary)) FROM vac""").fetchall()[0]
        print(f"variant vacancies: {total:,}; with skills: {with_skills:,}; with salary (RUR, 15k-1M): "
              f"{with_salary:,}; with skills and salary: {both:,}; median salary: {median:,.0f} RUB")
        offers, offers_skills = self._connection.execute(
            "SELECT count(*), count(*) FILTER (WHERE vacancy_id IN (SELECT vacancy_id FROM vs)) FROM offers").fetchall()[0]
        print(f"unique salaried offers after removing repeated postings: {offers:,} (with skills: {offers_skills:,})")
        for size, table in ((1, "skill_sal"), (2, "pair_sal"), (3, "triple_sal")):
            eligible = self._connection.execute(
                f"SELECT count(*) FROM {table} WHERE n_sal >= {MIN_SUPPORT} AND n_emp >= {MIN_EMPLOYERS}").fetchall()[0][0]
            print(f"k={size}: combinations with >= {MIN_SUPPORT} offers from >= {MIN_EMPLOYERS} employers: {eligible:,}")

    def save_and_print(self, name: str, sql: str) -> None:
        relation = self._connection.sql(sql)
        OUT.mkdir(exist_ok=True)
        relation.write_csv(str(OUT / f"{name}.csv"))
        self._connection.execute(f"COPY ({sql}) TO 's3://{BUCKET}/results/analysis/{name}.csv' (HEADER)")
        print(f"\n=== {name} ===")
        relation.show(max_width=250, max_rows=60)

    def effects(self, name: str, sql: str) -> None:
        self.save_and_print(f"{name}_up", f"{sql} ORDER BY uplift DESC, n_sal DESC LIMIT {TOP_N}")
        self.save_and_print(f"{name}_down", f"{sql} ORDER BY uplift ASC, n_sal DESC LIMIT {TOP_N}")


def main() -> None:
    analysis = SalaryAnalysis()
    analysis.overview()
    analysis.save_and_print("mass_postings", MASS_POSTINGS_SQL)
    analysis.effects("pairs", PAIR_EFFECT_SQL)
    analysis.effects("triples", TRIPLE_EFFECT_SQL)
    analysis.save_and_print("regions_metrics", REGION_METRICS_SQL)
    analysis.save_and_print("regions_top_combos", REGION_TOP_SQL)
    analysis.save_and_print("regions_distinctive_skills", REGION_DISTINCT_SQL)
    analysis.save_and_print("regions_skill_salary", REGION_SKILL_SALARY_SQL)


if __name__ == "__main__":
    main()
