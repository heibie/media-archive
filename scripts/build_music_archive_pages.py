#!/usr/bin/env python3
import pathlib, yaml, re

DATA_DIR = pathlib.Path("_data/lastfm")
OUT_DIR  = pathlib.Path("musik")

LAYOUT_YEAR  = "music_year"
LAYOUT_MONTH = "music_month"

def years_months():
    result = {}
    if not DATA_DIR.exists(): return result
    for ydir in sorted(DATA_DIR.iterdir()):
        if not ydir.is_dir(): continue
        y = ydir.name
        months = []
        for mfile in sorted((ydir).glob("*.yml")):
            mm = mfile.stem
            months.append(mm)
        if months:
            result[y] = months
    return result

def write_year_page(y):
    p = OUT_DIR / y / "index.html"
    p.parent.mkdir(parents=True, exist_ok=True)
    front = f"""---
layout: {LAYOUT_YEAR}
title: Musik {y}
permalink: /musik/{y}/
year: "{y}"
---
"""
    p.write_text(front, encoding="utf-8")

def write_month_page(y, m):
    p = OUT_DIR / y / m / "index.html"
    p.parent.mkdir(parents=True, exist_ok=True)
    front = f"""---
layout: {LAYOUT_MONTH}
title: Musik {y}/{m}
permalink: /musik/{y}/{m}/
year: "{y}"
month: "{m}"
---
"""
    p.write_text(front, encoding="utf-8")

def main():
    ym = years_months()
    total = 0
    for y, months in ym.items():
        write_year_page(y); total += 1
        for m in months:
            write_month_page(y, m); total += 1
    print(f"[OK] Archivseiten erzeugt/aktualisiert: {total}")

if __name__ == "__main__":
    main()
