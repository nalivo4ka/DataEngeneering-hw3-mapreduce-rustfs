# -*- coding: utf-8 -*-
"""Этап 3: собственный MapReduce по данным из hh-lake — частоты навыков, пар и троек (вариант 10).

Схема:
  * мастер читает объект s3://hh-lake/raw/... кусками по --chunk-mb МиБ (HTTP Range) и режет
    их по границам CSV-записей; запись не рвётся, остаток переносится в следующий кусок;
  * каждый кусок — один сплит, его обрабатывает маппер в отдельном процессе: фильтр варианта,
    нормализация навыков, сочетания размеров 1, 2, 3;
  * комбайнер (по умолчанию включён) суммирует сочетания внутри сплита, затем они
    раскладываются по R партициям по crc32 ключа;
  * R редьюсеров суммируют свои партиции, итог по каждому размеру кладётся в
    s3://hh-lake/results/mapreduce/skills_k{1,2,3}.csv.
Отказ маппера моделируется флагом --fail-map-task N: сплит N падает на первой попытке,
мастер запускает его повторно (до MAX_ATTEMPTS раз).
"""
import argparse
import csv
import io
import os
import time
import zlib
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass, field
from itertools import combinations
from math import comb

from config import BUCKET, RAW_KEY, STACK_SIZES, VARIANT_ROLES, normalize_skills, result_key
from s3client import S3Client

BYTES_PER_MIB = 1024 * 1024
MAX_ATTEMPTS = 3
KEY_SEPARATOR = "\x1f"
QUOTE, NEWLINE = b'"', b"\n"


@dataclass(frozen=True)
class MapTask:
    split_id: int
    blob: bytes
    role_index: int
    skills_index: int
    reducers: int
    use_combiner: bool
    fail_on_first_attempt: bool
    attempt: int = 1


@dataclass
class MapResult:
    split_id: int
    rows: int = 0
    variant_rows: int = 0
    skills_histogram: Counter = field(default_factory=Counter)  # число навыков в вакансии -> вакансий
    partitions: list = field(default_factory=list)  # [partition] -> {k: Counter или list[(key, 1)]}


def last_record_end(data: bytes) -> int:
    """Позиция сразу за последним переводом строки вне кавычек (0, если такого нет).

    Перевод строки завершает CSV-запись, только если число кавычек до него чётное:
    удвоенные кавычки внутри поля тоже дают чётный вклад.
    """
    position = data.rfind(NEWLINE)
    quotes_before = data.count(QUOTE, 0, position) if position != -1 else 0
    while position != -1 and quotes_before % 2:
        previous = data.rfind(NEWLINE, 0, position)
        quotes_before -= data.count(QUOTE, max(previous, 0), position)
        position = previous
    return position + 1


class ChunkedObjectReader:
    """Читает объект из S3 кусками и отдаёт сплиты, состоящие только из целых CSV-записей."""

    def __init__(self, client: S3Client, chunk_bytes: int) -> None:
        self._client = client
        self._chunk_bytes = chunk_bytes
        self.object_size = client.head(BUCKET, RAW_KEY).size_bytes
        self.bytes_read = 0
        self.range_requests = 0

    def splits(self):
        carry = b""
        for start in range(0, self.object_size, self._chunk_bytes):
            end = min(start + self._chunk_bytes, self.object_size) - 1
            chunk = self._client.get_range(BUCKET, RAW_KEY, start, end)
            self.bytes_read += len(chunk)
            self.range_requests += 1
            data = carry + chunk
            cut = len(data) if end == self.object_size - 1 else last_record_end(data)
            carry = data[cut:]
            if cut:
                yield data[:cut]


def split_header(first_split: bytes) -> tuple[list[str], bytes]:
    header_end = first_split.index(NEWLINE) + 1
    header = next(csv.reader([first_split[:header_end].decode("utf-8").strip("\r\n")]))
    return header, first_split[header_end:]


def run_map(task: MapTask) -> MapResult:
    """Маппер (+ комбайнер): одна пара сплит -> партиции с частотами сочетаний."""
    if task.fail_on_first_attempt and task.attempt == 1:
        raise RuntimeError(f"injected failure in map task {task.split_id}")
    result = MapResult(task.split_id)
    combined = {size: Counter() for size in STACK_SIZES}
    raw_pairs = {size: [] for size in STACK_SIZES}
    min_columns = max(task.role_index, task.skills_index) + 1
    for row in csv.reader(io.StringIO(task.blob.decode("utf-8"), newline="")):
        if len(row) < min_columns:  # пустая строка
            continue
        result.rows += 1
        if row[task.role_index] not in VARIANT_ROLES:
            continue
        result.variant_rows += 1
        skills = normalize_skills(row[task.skills_index])
        result.skills_histogram[len(skills)] += 1
        for size in STACK_SIZES:
            if task.use_combiner:
                combined[size].update(combinations(skills, size))
            else:
                raw_pairs[size].extend((combo, 1) for combo in combinations(skills, size))
    result.partitions = partition(combined if task.use_combiner else raw_pairs, task.reducers, task.use_combiner)
    return result


def partition_of(key: tuple[str, ...], reducers: int) -> int:
    # crc32, а не hash(): встроенный hash строк рандомизирован в каждом процессе Python
    return zlib.crc32(KEY_SEPARATOR.join(key).encode("utf-8")) % reducers


def partition(emitted: dict, reducers: int, combined: bool) -> list[dict]:
    parts = [{size: (Counter() if combined else []) for size in STACK_SIZES} for _ in range(reducers)]
    for size, items in emitted.items():
        pairs = items.items() if combined else items
        for key, count in pairs:
            target = parts[partition_of(key, reducers)][size]
            if combined:
                target[key] = count
            else:
                target.append((key, count))
    return parts


def run_reduce(partition_inputs: list[dict]) -> dict[int, Counter]:
    """Редьюсер: сумма частот по ключам одной партиции."""
    totals = {size: Counter() for size in STACK_SIZES}
    for part in partition_inputs:
        for size, items in part.items():
            pairs = items.items() if isinstance(items, Counter) else items
            for key, count in pairs:
                totals[size][key] += count
    return totals


class MapReduceJob:
    def __init__(self, arguments: argparse.Namespace) -> None:
        self._args = arguments
        self.client = S3Client()
        self.stats = Counter()
        self.skills_histogram = Counter()
        self.timings: dict[str, float] = {}

    def run(self) -> dict[int, Counter]:
        started = time.perf_counter()
        with ProcessPoolExecutor(max_workers=self._args.workers) as pool:
            shuffled = self._map_phase(pool)
            self.timings["map+shuffle"] = time.perf_counter() - started
            reduce_started = time.perf_counter()
            totals = self._reduce_phase(pool, shuffled)
            self.timings["reduce"] = time.perf_counter() - reduce_started
        return totals

    def _map_phase(self, pool: ProcessPoolExecutor) -> list[list[dict]]:
        reader = ChunkedObjectReader(self.client, self._args.chunk_mb * BYTES_PER_MIB)
        shuffled: list[list[dict]] = [[] for _ in range(self._args.reducers)]
        pending: dict[Future, MapTask] = {}
        header_indices = None
        for split_id, blob in enumerate(reader.splits()):
            if header_indices is None:
                header, blob = split_header(blob)
                header_indices = (header.index("role"), header.index("key_skills"))
            task = MapTask(split_id, blob, *header_indices, self._args.reducers, not self._args.no_combiner,
                           split_id == self._args.fail_map_task)
            pending[pool.submit(run_map, task)] = task
            while len(pending) >= 2 * self._args.workers:  # ограничиваем память: не больше 2W сплитов в работе
                self._collect(pool, pending, shuffled)
        while pending:
            self._collect(pool, pending, shuffled)
        self.stats.update(splits=reader.range_requests, bytes_read=reader.bytes_read)
        return shuffled

    def _collect(self, pool, pending: dict, shuffled: list) -> None:
        done, _ = wait(pending, return_when=FIRST_COMPLETED)
        for future in done:
            task = pending.pop(future)
            try:
                result = future.result()
            except RuntimeError as error:
                if task.attempt >= MAX_ATTEMPTS:
                    raise
                print(f"[master] map task {task.split_id} failed (attempt {task.attempt}): {error} -> retry")
                retry = MapTask(**{**task.__dict__, "attempt": task.attempt + 1})
                pending[pool.submit(run_map, retry)] = retry
                self.stats["map_retries"] += 1
                continue
            self.stats.update(map_tasks=1, rows=result.rows, variant_rows=result.variant_rows)
            self.skills_histogram.update(result.skills_histogram)
            for index, part in enumerate(result.partitions):
                shuffled[index].append(part)

    def _reduce_phase(self, pool: ProcessPoolExecutor, shuffled: list[list[dict]]) -> dict[int, Counter]:
        totals = {size: Counter() for size in STACK_SIZES}
        for reduced in pool.map(run_reduce, shuffled):
            for size in STACK_SIZES:
                totals[size].update(reduced[size])  # ключи партиций не пересекаются
        return totals


def to_csv_bytes(counts: Counter, size: int) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow([f"skill_{i}" for i in range(1, size + 1)] + ["count"])
    for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        writer.writerow([*key, count])
    return buffer.getvalue().encode("utf-8")


def print_report(job: MapReduceJob, totals: dict[int, Counter], top: int) -> None:
    stats = job.stats
    print(f"splits (range requests): {stats['splits']}, bytes read from S3: {stats['bytes_read']:,}")
    print(f"map tasks ok: {stats['map_tasks']}, retries: {stats['map_retries']}")
    print(f"rows parsed: {stats['rows']:,}, variant rows: {stats['variant_rows']:,}, "
          f"with skills: {stats['variant_rows'] - job.skills_histogram[0]:,}")
    for size in STACK_SIZES:
        expected = sum(comb(n, size) * vacancies for n, vacancies in job.skills_histogram.items())
        occurrences = sum(totals[size].values())
        print(f"\nk={size}: distinct={len(totals[size]):,} occurrences={occurrences:,} "
              f"(sum C(n_i,{size}) over vacancies = {expected:,})")
        for key, count in sorted(totals[size].items(), key=lambda item: (-item[1], item[0]))[:top]:
            print(f"  {count:6d}  {' + '.join(key)}")
    print("\ntimings: " + ", ".join(f"{name} {seconds:.2f} s" for name, seconds in job.timings.items()))


def main() -> None:
    parser = argparse.ArgumentParser(description="MapReduce: навыки, пары и тройки (вариант 10 «Аналитик»)")
    parser.add_argument("--chunk-mb", type=int, default=32)
    parser.add_argument("--workers", type=int, default=os.cpu_count())
    parser.add_argument("--reducers", type=int, default=4)
    parser.add_argument("--no-combiner", action="store_true")
    parser.add_argument("--fail-map-task", type=int, default=-1)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--no-upload", action="store_true")
    arguments = parser.parse_args()

    job = MapReduceJob(arguments)
    totals = job.run()
    if not arguments.no_upload:
        upload_started = time.perf_counter()
        for size in STACK_SIZES:
            job.client.put_bytes(BUCKET, result_key("mapreduce", size), to_csv_bytes(totals[size], size))
        job.timings["write results to S3"] = time.perf_counter() - upload_started
    print_report(job, totals, arguments.top)


if __name__ == "__main__":
    assert last_record_end(b'a,"x\ny"\nb,"z') == 8  # перевод строки внутри кавычек не граница
    assert last_record_end(b'a,"x""y"\nb') == 9      # удвоенная кавычка внутри поля
    assert last_record_end(b'a,"open') == 0
    main()
