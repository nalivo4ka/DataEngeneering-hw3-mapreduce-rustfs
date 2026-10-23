# -*- coding: utf-8 -*-
"""Этап 4: полная сверка результатов MapReduce и DuckDB, лежащих в hh-lake.

Для каждого размера сочетаний сравниваются все строки (ключ + частота) в обе стороны через
EXCEPT, плюс контрольные числа: количество ключей, сумма частот и md5 отсортированного содержимого.
"""
from config import BUCKET, STACK_SIZES, duckdb_connection, result_key


def table_sql(method: str) -> str:
    # формат задан явно: автоопределение по первым строкам может не увидеть кавычек
    return (f"read_csv('s3://{BUCKET}/{result_key(method, 0)}', header = true, all_varchar = true, "
            "delim = ',', quote = '\"', escape = '\"')")


def fingerprint(connection, source: str, size: int) -> tuple:
    columns = ", ".join(f"skill_{i}" for i in range(1, size + 1))
    return connection.execute(f"""
        SELECT count(*), sum(count::BIGINT),
               md5(string_agg(concat_ws(chr(31), {columns}, count), chr(10) ORDER BY {columns}))
        FROM {source}""").fetchone()


def main() -> None:
    connection = duckdb_connection()
    all_equal = True
    for size in STACK_SIZES:
        mapreduce = table_sql("mapreduce").replace("skills_k0", f"skills_k{size}")
        duck = table_sql("duckdb").replace("skills_k0", f"skills_k{size}")
        only_mapreduce = connection.execute(f"SELECT count(*) FROM (FROM {mapreduce} EXCEPT FROM {duck})").fetchone()[0]
        only_duckdb = connection.execute(f"SELECT count(*) FROM (FROM {duck} EXCEPT FROM {mapreduce})").fetchone()[0]
        mr_print, db_print = fingerprint(connection, mapreduce, size), fingerprint(connection, duck, size)
        equal = only_mapreduce == only_duckdb == 0 and mr_print == db_print
        all_equal &= equal
        print(f"k={size}: MapReduce keys={mr_print[0]:,} sum={mr_print[1]:,} md5={mr_print[2]}")
        print(f"     DuckDB    keys={db_print[0]:,} sum={db_print[1]:,} md5={db_print[2]}")
        print(f"     rows only in MapReduce: {only_mapreduce}, only in DuckDB: {only_duckdb} -> "
              f"{'MATCH' if equal else 'MISMATCH'}")
    print("\nRESULT:", "all three stack sizes are identical" if all_equal else "DIFFERENCES FOUND")
    raise SystemExit(0 if all_equal else 1)


if __name__ == "__main__":
    main()
