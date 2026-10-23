# -*- coding: utf-8 -*-
"""Минимальный S3-клиент с подписью AWS Signature V4.

Нужен домашнему заданию по RustFS, чтобы работать с хранилищем без установки
boto3/awscli: хватает `requests`. Поддержаны только операции, которые используются
в сценариях: создание бакета, PUT/GET/HEAD объекта, листинг и удаление.
Файлы передаются потоком, поэтому CSV на 850 МБ не загружается в память целиком.
"""
import datetime
import hashlib
import hmac
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote
from xml.etree import ElementTree

import requests

SERVICE_NAME = "s3"
SIGNING_ALGORITHM = "AWS4-HMAC-SHA256"
HASH_CHUNK_BYTES = 8 * 1024 * 1024
REQUEST_TIMEOUT_SECONDS = 600
S3_XML_NAMESPACE = "{http://s3.amazonaws.com/doc/2006-03-01/}"


class S3Error(RuntimeError):
    """Хранилище вернуло ошибку; в сообщении — статус и тело ответа."""


@dataclass(frozen=True)
class S3Config:
    endpoint: str = "http://127.0.0.1:9000"
    access_key: str = "rustfsadmin"
    secret_key: str = "rustfsadmin"
    region: str = "us-east-1"
    timeout_seconds: int = REQUEST_TIMEOUT_SECONDS


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size_bytes: int
    etag: str


def sha256_of_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


class SignatureV4:
    """Строит заголовки Authorization для одного запроса."""

    def __init__(self, config: S3Config) -> None:
        self._config = config

    def sign(self, method: str, host: str, path: str, query: str, payload_sha256: str) -> dict[str, str]:
        now = datetime.datetime.now(datetime.timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        headers = {"host": host, "x-amz-content-sha256": payload_sha256, "x-amz-date": amz_date}

        signed_names = ";".join(sorted(headers))
        canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in sorted(headers))
        canonical_request = "\n".join(
            [method, quote(path, safe="/-_.~"), query, canonical_headers, signed_names, payload_sha256]
        )

        scope = f"{date_stamp}/{self._config.region}/{SERVICE_NAME}/aws4_request"
        string_to_sign = "\n".join(
            [SIGNING_ALGORITHM, amz_date, scope, hashlib.sha256(canonical_request.encode()).hexdigest()]
        )
        signature = hmac.new(self._signing_key(date_stamp), string_to_sign.encode(), hashlib.sha256).hexdigest()
        headers["Authorization"] = (
            f"{SIGNING_ALGORITHM} Credential={self._config.access_key}/{scope}, "
            f"SignedHeaders={signed_names}, Signature={signature}"
        )
        return headers

    def _signing_key(self, date_stamp: str) -> bytes:
        key = ("AWS4" + self._config.secret_key).encode()
        for part in (date_stamp, self._config.region, SERVICE_NAME, "aws4_request"):
            key = hmac.new(key, part.encode(), hashlib.sha256).digest()
        return key


class S3Client:
    def __init__(self, config: S3Config | None = None) -> None:
        self._config = config or S3Config()
        self._signer = SignatureV4(self._config)
        self._host = self._config.endpoint.split("://", 1)[1]

    def create_bucket(self, bucket: str) -> None:
        response = self._send("PUT", f"/{bucket}", expected_statuses=(200, 409))
        response.close()

    def put_file(self, bucket: str, key: str, path: Path) -> str:
        payload_sha256 = sha256_of_file(path)
        with path.open("rb") as stream:
            response = self._send("PUT", f"/{bucket}/{key}", payload_sha256=payload_sha256, body=stream)
        return response.headers.get("ETag", "").strip('"')

    def get_to_file(self, bucket: str, key: str, target: Path) -> str:
        """Скачивает объект в файл и возвращает SHA-256 полученных байт."""
        digest = hashlib.sha256()
        response = self._send("GET", f"/{bucket}/{key}", stream=True)
        with target.open("wb") as output:
            for chunk in response.iter_content(HASH_CHUNK_BYTES):
                digest.update(chunk)
                output.write(chunk)
        return digest.hexdigest()

    def get_range(self, bucket: str, key: str, start: int, end: int) -> bytes:
        """Байты объекта с позиции start по end включительно (HTTP Range)."""
        headers = {"Range": f"bytes={start}-{end}"}
        return self._send("GET", f"/{bucket}/{key}", extra_headers=headers, expected_statuses=(206,)).content

    def put_bytes(self, bucket: str, key: str, data: bytes) -> None:
        payload_sha256 = hashlib.sha256(data).hexdigest()
        self._send("PUT", f"/{bucket}/{key}", payload_sha256=payload_sha256, body=data).close()

    def get_bytes(self, bucket: str, key: str) -> bytes:
        return self._send("GET", f"/{bucket}/{key}").content

    def head(self, bucket: str, key: str) -> ObjectInfo:
        response = self._send("HEAD", f"/{bucket}/{key}")
        return ObjectInfo(key, int(response.headers["Content-Length"]), response.headers.get("ETag", "").strip('"'))

    def list_objects(self, bucket: str, prefix: str = "") -> list[ObjectInfo]:
        objects: list[ObjectInfo] = []
        token = ""
        while True:
            params = {"list-type": "2", "prefix": prefix}
            if token:
                params["continuation-token"] = token
            # SigV4 требует канонический запрос с параметрами, отсортированными по имени
            query = "&".join(f"{name}={quote(params[name], safe='')}" for name in sorted(params))
            root = ElementTree.fromstring(self._send("GET", f"/{bucket}", query=query).content)
            for item in root.findall(f"{S3_XML_NAMESPACE}Contents"):
                objects.append(self._parse_contents(item))
            if root.findtext(f"{S3_XML_NAMESPACE}IsTruncated") != "true":
                return objects
            token = root.findtext(f"{S3_XML_NAMESPACE}NextContinuationToken", "")

    def delete_object(self, bucket: str, key: str) -> None:
        self._send("DELETE", f"/{bucket}/{key}", expected_statuses=(200, 204)).close()

    @staticmethod
    def _parse_contents(item: ElementTree.Element) -> ObjectInfo:
        key = item.findtext(f"{S3_XML_NAMESPACE}Key", "")
        size = int(item.findtext(f"{S3_XML_NAMESPACE}Size", "0"))
        etag = item.findtext(f"{S3_XML_NAMESPACE}ETag", "").strip('"')
        return ObjectInfo(key, size, etag)

    def _send(
        self,
        method: str,
        path: str,
        query: str = "",
        payload_sha256: str = hashlib.sha256(b"").hexdigest(),
        body=None,
        stream: bool = False,
        expected_statuses: tuple[int, ...] = (200, 204),
        extra_headers: dict[str, str] | None = None,
    ) -> requests.Response:
        headers = self._signer.sign(method, self._host, path, query, payload_sha256)
        headers.update(extra_headers or {})  # не подписываются: SigV4 допускает неподписанные заголовки
        url = self._config.endpoint + quote(path, safe="/-_.~") + (f"?{query}" if query else "")
        response = requests.request(
            method, url, headers=headers, data=body, stream=stream, timeout=self._config.timeout_seconds
        )
        if response.status_code not in expected_statuses:
            raise S3Error(f"{method} {path} -> HTTP {response.status_code}: {response.text[:300]}")
        return response
