# -*- coding: utf-8 -*-
"""Генерирует LaTeX-таблицы отчёта из out/*.csv (результаты analysis.py), чтобы числа не переносить руками."""
import csv
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).parent
OUT = ROOT / "out"
TABLES = ROOT / "report" / "tables"
REGIONS = ("Москва", "Санкт-Петербург", "Екатеринбург")
LATEX_SPECIAL = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#", "_": r"\_",
                 "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}"}


def tex(text: str) -> str:
    return "".join(LATEX_SPECIAL.get(char, char) for char in text)


def rub(value: str) -> str:
    return f"{float(value):,.0f}".replace(",", r"\,")


def signed(value: str) -> str:
    number = float(value)
    return ("+" if number > 0 else "−") + f"{abs(number):,.0f}".replace(",", r"\,")


def read(name: str) -> list[dict]:
    with (OUT / f"{name}.csv").open(encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def write(name: str, lines: list[str]) -> None:
    (TABLES / f"{name}.tex").write_text("\n".join(lines) + "\n", encoding="utf-8")


def effects_table(name: str) -> None:
    lines = []
    for row in read(name):
        pct = float(row["uplift_pct"])
        lines.append(
            f"{tex(row['base'])} & \\textbf{{+ {tex(row['added'])}}} & {row['n_sal']} / {row['n_emp']} & {rub(row['med'])} & "
            f"{row['base_n']} & {rub(row['base_med'])} & \\textbf{{{signed(row['uplift'])}}} ({pct:+.0f}\\%) & "
            f"{float(row['senior_pct']):.0f}\\% / {float(row['base_senior_pct']):.0f}\\% \\\\")
    write(name, lines)


def mass_postings_table() -> None:
    lines = [f"{row['employer_id']} & {tex(row['name'])} & {row['postings']} & {row['cities']} & {row['skills']} & "
             f"{rub(row['salary_net'])} \\\\" for row in read("mass_postings")]
    write("mass_postings", lines)


def regions_metrics_table() -> None:
    lines = []
    for row in read("regions_metrics"):
        name = f"\\textit{{{row['region']}}}" if row["region"] == "Все регионы" else row["region"]
        lines.append(
            f"{name} & {rub(row['vacancies'])} & {row['with_skills_pct']}\\% & {row['skills_per_vacancy']} & "
            f"{rub(row['with_salary'])} & {rub(row['p25'])} & \\textbf{{{rub(row['median'])}}} & {rub(row['p75'])} & "
            f"{row['remote_pct']}\\% & {row['no_experience_pct']}\\% & {row['senior_pct']}\\% \\\\")
    write("regions_metrics", lines)


def regions_top_tables() -> None:
    rows = read("regions_top_combos")
    for size in ("1", "2", "3"):
        lines = []
        for rank in range(1, 6):
            cells = []
            for region in REGIONS:
                match = next(r for r in rows if r["region"] == region and r["size"] == size and r["rank"] == str(rank))
                cells.append(f"{tex(match['combo'])} \\newline {{\\footnotesize\\color{{black!55}}{match['n']} вак., "
                             f"{match['share_pct']}\\%, медиана {rub(match['median_salary'])}}}")
            lines.append(f"{rank} & " + " & ".join(cells) + " \\\\")
        write(f"regions_top_k{size}", lines)


def regions_distinctive_table() -> None:
    lines = [f"{row['region']} & {tex(row['skill'])} & {row['n']} & {row['share_pct']}\\% & {row['other_regions_pct']}\\% & "
             f"\\textbf{{{float(row['ratio']):.2f}×}} \\\\" for row in read("regions_distinctive_skills")]
    write("regions_distinctive", lines)


def regions_skill_salary_table() -> None:
    rows = read("regions_skill_salary")
    lines = []
    for skill in dict.fromkeys(r["skill"] for r in rows):
        cells = []
        for region in REGIONS:
            match = next((r for r in rows if r["skill"] == skill and r["region"] == region), None)
            cells.append(f"{rub(match['median_salary'])} {{\\footnotesize\\color{{black!55}}({match['n']})}}" if match else "—")
        lines.append(f"{tex(skill)} & " + " & ".join(cells) + " \\\\")
    write("regions_skill_salary", lines)


def profile_table() -> None:
    runs = json.loads((ROOT / "logs" / "profile.json").read_text(encoding="utf-8"))
    lines = []
    for config in dict.fromkeys(r["config"] for r in runs):
        selected = [r for r in runs if r["config"] == config]
        seconds = [r["seconds"] for r in selected]
        peaks = [r["peak_mb"] for r in selected]
        per_run = " / ".join(f"{s:.2f}".replace(".", ",") for s in seconds)
        peaks_text = " / ".join(rub(p) for p in peaks)
        mean_seconds = f"{statistics.mean(seconds):.2f}".replace(".", ",")
        lines.append(f"{tex(config)} & {per_run} & \\textbf{{{mean_seconds}}} & {peaks_text} & "
                     f"\\textbf{{{rub(statistics.mean(peaks))}}} \\\\")
    write("profile", lines)


def main() -> None:
    TABLES.mkdir(parents=True, exist_ok=True)
    profile_table()
    for name in ("pairs_up", "pairs_down", "triples_up", "triples_down"):
        effects_table(name)
    mass_postings_table()
    regions_metrics_table()
    regions_top_tables()
    regions_distinctive_table()
    regions_skill_salary_table()
    print(f"tables written to {TABLES}")


if __name__ == "__main__":
    assert tex("a&b_c%") == r"a\&b\_c\%"
    assert signed("-1500") == "−1\\,500"
    main()
