# ДЗ №3: MapReduce и DuckDB поверх объектного хранилища RustFS

Вариант **10 — «Аналитик»**: вакансии с профессиональной ролью hh «Аналитик», «Системный аналитик»,
«Бизнес-аналитик», «BI-аналитик, аналитик данных», «Продуктовый аналитик» (без «Руководитель отдела аналитики»).

Полный отчёт с выводами команд, профилированием и выводами: [`report/report.pdf`](report/report.pdf).

## Что внутри

| Файл | Этап | Что делает |
|---|---|---|
| `s3client.py` | — | S3-клиент с подписью SigV4 (без boto3): PUT/GET/HEAD, чтение по диапазону байт |
| `config.py` | — | бакет `hh-lake`, условие варианта, нормализация навыков, подключение DuckDB к RustFS |
| `upload_raw.py` | 2 | загрузка датасета в `s3://hh-lake/raw/it_vacancies_2024.csv` |
| `mapreduce.py` | 3 | свой MapReduce: чтение объекта чанками (HTTP Range), мапперы в процессах, комбайнер, партиционирование по crc32, редьюсеры, повтор упавшего маппера |
| `duckdb_counts.py` | 3 | тот же расчёт одним SQL в DuckDB, читающим объект из S3 через httpfs |
| `verify.py` | 4 | полная сверка результатов обоих способов (EXCEPT в обе стороны + md5) |
| `analysis.py` | 5–6 | влияние пар и троек навыков на медианную зарплату, сравнение трёх регионов |
| `make_parquet.py` | бонус | Parquet-копия данных в `hh-lake` |
| `profile_runs.py` | профилирование | 5 конфигураций × 3 прогона: время и пик памяти дерева процессов |

Результаты в хранилище: `s3://hh-lake/results/{mapreduce,duckdb}/skills_k{1,2,3}.csv`,
`s3://hh-lake/results/analysis/*.csv`. Выводы всех запусков — в `logs/`, таблицы анализа — в `out/`.

## Как воспроизвести

1. Запустить RustFS (кластер из ДЗ №2, 8 папок-дисков) на `127.0.0.1:9000`.
2. `pip install duckdb psutil requests`
3. По порядку:

```bash
python upload_raw.py
python mapreduce.py
python duckdb_counts.py
python verify.py
python mapreduce.py --fail-map-task 5   # тест отказа маппера
python analysis.py
python make_parquet.py
python profile_runs.py
```
