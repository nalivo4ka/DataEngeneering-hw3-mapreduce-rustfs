# -*- coding: utf-8 -*-
"""Профилирование: каждая конфигурация запускается RUNS раз отдельным процессом.

Время — по часам стены от старта до завершения процесса. Пик памяти — максимум суммарного RSS
всего дерева процессов (у MapReduce это мастер + воркеры), снимается каждые SAMPLE_SECONDS.
Результаты печатаются и сохраняются в logs/profile.json; вывод каждого прогона — в logs/profile/.
"""
import json
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import psutil

ROOT = Path(__file__).parent
LOGS = ROOT / "logs"
RUNS = 3
SAMPLE_SECONDS = 0.05
BYTES_PER_MB = 1_000_000

CONFIGS = {
    "MapReduce naive (1 worker, no combiner)": ["mapreduce.py", "--workers", "1", "--no-combiner", "--top", "0"],
    "MapReduce + combiner (1 worker)": ["mapreduce.py", "--workers", "1", "--top", "0"],
    "MapReduce + combiner (12 workers)": ["mapreduce.py", "--workers", "12", "--top", "0"],
    "DuckDB, CSV from S3": ["duckdb_counts.py", "--source", "csv", "--top", "0"],
    "DuckDB, Parquet from S3": ["duckdb_counts.py", "--source", "parquet", "--top", "0"],
}


@dataclass
class RunResult:
    config: str
    run: int
    seconds: float
    peak_mb: float
    exit_code: int


def tree_rss(process: psutil.Process) -> int:
    total = 0
    for member in [process, *process.children(recursive=True)]:
        try:
            total += member.memory_info().rss
        except psutil.NoSuchProcess:  # воркер успел завершиться между перечислением и замером
            continue
    return total


def measure(config: str, args: list[str], run: int) -> RunResult:
    log_path = LOGS / "profile" / f"{config.split(' ')[0].lower()}_{list(CONFIGS).index(config)}_run{run}.txt"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        started = time.perf_counter()
        process = subprocess.Popen([sys.executable, *args], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                   env={**psutil.os.environ, "PYTHONIOENCODING": "utf-8"})
        watched, peak = psutil.Process(process.pid), 0
        while process.poll() is None:
            try:
                peak = max(peak, tree_rss(watched))
            except psutil.NoSuchProcess:
                break
            time.sleep(SAMPLE_SECONDS)
        process.wait()  # процесс мог завершиться между замерами: дочитываем код возврата
        seconds = time.perf_counter() - started
    return RunResult(config, run, round(seconds, 2), round(peak / BYTES_PER_MB, 1), process.returncode)


def main() -> None:
    results = []
    for config, args in CONFIGS.items():
        for run in range(1, RUNS + 1):
            result = measure(config, args, run)
            results.append(result)
            print(f"{config:42s} run {run}: {result.seconds:7.2f} s, peak {result.peak_mb:8.1f} MB, exit {result.exit_code}",
                  flush=True)
    (LOGS / "profile.json").write_text(json.dumps([asdict(r) for r in results], ensure_ascii=False, indent=1),
                                       encoding="utf-8")
    print(f"\n{'configuration':42s} {'time mean':>10s} {'min':>7s} {'max':>7s} {'peak MB mean':>13s} {'max':>8s}")
    for config in CONFIGS:
        runs = [r for r in results if r.config == config]
        seconds, peaks = [r.seconds for r in runs], [r.peak_mb for r in runs]
        print(f"{config:42s} {statistics.mean(seconds):10.2f} {min(seconds):7.2f} {max(seconds):7.2f} "
              f"{statistics.mean(peaks):13.1f} {max(peaks):8.1f}")


if __name__ == "__main__":
    main()
