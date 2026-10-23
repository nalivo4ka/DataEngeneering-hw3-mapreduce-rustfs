# -*- coding: utf-8 -*-
"""Бонус: Parquet-копия сырого CSV в hh-lake (все строки и колонки, zstd) для сравнения «до/после» в DuckDB."""
import time

from config import BUCKET, PARQUET_KEY, RAW_KEY, duckdb_connection
from s3client import S3Client


def main() -> None:
    connection = duckdb_connection()
    started = time.perf_counter()
    connection.execute(f"""
        COPY (SELECT * FROM read_csv('s3://{BUCKET}/{RAW_KEY}', header = true, all_varchar = true))
        TO 's3://{BUCKET}/{PARQUET_KEY}' (FORMAT parquet, COMPRESSION zstd)""")
    elapsed = time.perf_counter() - started
    client = S3Client()
    csv_size = client.head(BUCKET, RAW_KEY).size_bytes
    parquet_size = client.head(BUCKET, PARQUET_KEY).size_bytes
    rows = connection.execute(f"SELECT count(*) FROM read_parquet('s3://{BUCKET}/{PARQUET_KEY}')").fetchall()[0][0]
    print(f"CSV     s3://{BUCKET}/{RAW_KEY}: {csv_size:,} bytes")
    print(f"Parquet s3://{BUCKET}/{PARQUET_KEY}: {parquet_size:,} bytes ({csv_size / parquet_size:.1f}x smaller), "
          f"{rows:,} rows, written in {elapsed:.1f} s")


if __name__ == "__main__":
    main()
