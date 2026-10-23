# -*- coding: utf-8 -*-
"""Этап 2: загрузка датасета ИТ-вакансий в бакет hh-lake и проверка, что объект совпадает с файлом."""
from config import BUCKET, LOCAL_RAW_CSV, RAW_KEY
from s3client import S3Client, sha256_of_file


def main() -> None:
    client = S3Client()
    client.create_bucket(BUCKET)
    etag = client.put_file(BUCKET, RAW_KEY, LOCAL_RAW_CSV)
    info = client.head(BUCKET, RAW_KEY)
    print(f"bucket      : {BUCKET}")
    print(f"object      : {info.key}")
    print(f"size bytes  : {info.size_bytes:,} (local {LOCAL_RAW_CSV.stat().st_size:,})")
    print(f"etag        : {etag}")
    print(f"sha256 local: {sha256_of_file(LOCAL_RAW_CSV)}")
    assert info.size_bytes == LOCAL_RAW_CSV.stat().st_size, "размер объекта не совпал с файлом"
    print("OK: object in hh-lake matches the local file")


if __name__ == "__main__":
    main()
