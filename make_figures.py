# -*- coding: utf-8 -*-
"""Рисунки к отчёту: рост числа сочетаний, профилирование, зарплаты по регионам."""
import csv
import json
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).parent
FIGURES = ROOT / "report" / "figures"
ACCENT, WARM, GRAY = "#1B4F9C", "#E07B00", "#8D99AE"

# из вывода mapreduce.py / duckdb_counts.py (совпадают, см. verify.py)
DISTINCT = {1: 8_369, 2: 152_112, 3: 1_098_033}
OCCURRENCES = {1: 231_216, 2: 1_003_894, 3: 3_871_492}


def growth_figure() -> None:
    figure, axis = plt.subplots(figsize=(7.2, 3.2))
    sizes = list(DISTINCT)
    width = 0.38
    axis.bar([s - width / 2 for s in sizes], [DISTINCT[s] for s in sizes], width, color=ACCENT, label="различных сочетаний")
    axis.bar([s + width / 2 for s in sizes], [OCCURRENCES[s] for s in sizes], width, color=WARM, label="вхождений в вакансии")
    for size in sizes:
        axis.text(size - width / 2, DISTINCT[size] * 1.15, f"{DISTINCT[size]:,}".replace(",", " "), ha="center", fontsize=8)
        axis.text(size + width / 2, OCCURRENCES[size] * 1.15, f"{OCCURRENCES[size]:,}".replace(",", " "), ha="center", fontsize=8)
    axis.set_yscale("log")
    axis.set_xticks(sizes, ["навыки (k=1)", "пары (k=2)", "тройки (k=3)"])
    axis.set_ylim(1e3, 2e7)
    axis.legend(frameon=False, fontsize=9)
    axis.spines[["top", "right"]].set_visible(False)
    figure.tight_layout()
    figure.savefig(FIGURES / "growth.pdf")


def profile_figure() -> None:
    runs = json.loads((ROOT / "logs" / "profile.json").read_text(encoding="utf-8"))
    configs = list(dict.fromkeys(r["config"] for r in runs))
    labels = [c.replace(" (", "\n(").replace(", ", ",\n", 1) if c.startswith("DuckDB") else c.replace(" (", "\n(") for c in configs]
    colors = [WARM if c.startswith("MapReduce") else ACCENT for c in configs]
    figure, (left, right) = plt.subplots(1, 2, figsize=(9.6, 3.6))
    for axis, field, title in ((left, "seconds", "Время, с (среднее, разброс min–max)"),
                               (right, "peak_mb", "Пик памяти, МБ (среднее, min–max)")):
        values = [[r[field] for r in runs if r["config"] == c] for c in configs]
        means = [statistics.mean(v) for v in values]
        errors = [[m - min(v) for m, v in zip(means, values)], [max(v) - m for m, v in zip(means, values)]]
        axis.barh(range(len(configs)), means, xerr=errors, color=colors, capsize=3)
        for index, mean in enumerate(means):
            axis.text(mean * 1.02, index, f"{mean:,.1f}".replace(",", " "), va="center", fontsize=8)
        axis.set_yticks(range(len(configs)), labels if axis is left else [""] * len(configs), fontsize=8)
        axis.invert_yaxis()
        axis.set_title(title, fontsize=10)
        axis.spines[["top", "right"]].set_visible(False)
        axis.set_xlim(0, max(means) * 1.25)
    figure.tight_layout()
    figure.savefig(FIGURES / "profile.pdf")


def regions_figure() -> None:
    with (ROOT / "out" / "regions_metrics.csv").open(encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    figure, axis = plt.subplots(figsize=(7.2, 2.9))
    for index, row in enumerate(rows):
        p25, median, p75 = (float(row[k]) / 1000 for k in ("p25", "median", "p75"))
        color = GRAY if row["region"] == "Все регионы" else ACCENT
        axis.plot([p25, p75], [index, index], color=color, linewidth=8, alpha=0.35, solid_capstyle="round")
        axis.plot(median, index, "o", color=color, markersize=9)
        axis.text(p75 + 4, index, f"медиана {median:.0f} тыс. (P25–P75: {p25:.0f}–{p75:.0f})", va="center", fontsize=8)
    axis.set_yticks(range(len(rows)), [row["region"] for row in rows])
    axis.invert_yaxis()
    axis.set_xlabel("Зарплата на руки, тыс. ₽")
    axis.set_xlim(40, 300)
    axis.spines[["top", "right"]].set_visible(False)
    figure.tight_layout()
    figure.savefig(FIGURES / "regions.pdf")


def main() -> None:
    FIGURES.mkdir(parents=True, exist_ok=True)
    growth_figure()
    profile_figure()
    regions_figure()
    print(f"figures saved to {FIGURES}")


if __name__ == "__main__":
    main()
