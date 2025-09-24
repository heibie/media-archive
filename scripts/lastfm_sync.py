#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Last.fm → YAML (monatlich) mit Cover + MBIDs + jahresweisem Backfill

Schreibt nach: _data/lastfm/YYYY/MM.yml (neueste zuerst)
- Inkrementell (seit letztem Timestamp)
- Backfill gezielt pro Jahr: --backfill-years "2010-2012,2015"

ENV:
  LASTFM_API_KEY  – Last.fm API Key (read-only)

Beispiele:
  # 1) Erster Backfill: Jahre in Portionen
  LASTFM_API_KEY=... python scripts/lastfm_sync.py --user heikobielinski --backfill-years "2010-2012"
  LASTFM_API_KEY=... python scripts/lastfm_sync.py --user heikobielinski --backfill-years "2013-2016,2018"

  # 2) Danach inkrementell
  LASTFM_API_KEY=... python scripts/lastfm_sync.py --user heikobielinski
"""

import os
import sys
import time
import glob
import argparse
import pathlib
import datetime
from collections import defaultdict
from typing import Optional

import requests
import yaml

API_ROOT = "https://ws.audioscrobbler.com/2.0/"
DATA_DIR = pathlib.Path("_data/lastfm")

PAGE_LIMIT = 200                 # max laut Last.fm
REQUEST_SLEEP_SEC = 0.3          # sanftes Paging
MAX_RETRIES = 5                  # Retries für Netz-/HTTP-Fehler
RETRY_BACKOFF_BASE = 1.5         # Exponential Backoff

def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def iso_from_uts(uts: int) -> str:
    return datetime.datetime.utcfromtimestamp(uts).isoformat(timespec="seconds") + "Z"

def year_month_from_iso(iso_utc: str):
    return iso_utc[:4], iso_utc[5:7]  # YYYY, MM

def load_yaml(path: pathlib.Path):
    if not path.exists():
        return []
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return data or []
    except Exception as e:
        print(f"[WARN] YAML konnte nicht gelesen werden: {path} – {e}", file=sys.stderr)
        return []

def save_yaml(path: pathlib.Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows_sorted = sorted(rows, key=lambda x: x["played_at_utc"], reverse=True)
    txt = yaml.safe_dump(rows_sorted, allow_unicode=True, sort_keys=False)
    path.write_text(txt, encoding="utf-8")

def dedupe_merge(existing, new_items):
    # Strenger Key: Zeit + Artist + Track + Album
    def k(e):
        return (e.get("played_at_utc"), e.get("artist"), e.get("track"), e.get("album"))
    seen = {k(e) for e in existing}
    merged = existing[:]
    for e in new_items:
        if k(e) not in seen:
            merged.append(e)
            seen.add(k(e))
    return merged

def newest_uts_from_files() -> Optional[int]:
    newest = None
    for path_str in glob.glob(str(DATA_DIR / "*" / "*.yml")):
        rows = load_yaml(pathlib.Path(path_str))
        for r in rows or []:
            ts = r.get("played_at_utc")
            if not ts:
                continue
            try:
                dt = datetime.datetime.fromisoformat(ts.replace("Z", ""))
                uts = int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())
                if newest is None or uts > newest:
                    newest = uts
            except Exception:
                continue
    return newest

def largest_image_url(images: list) -> Optional[str]:
    if not images:
        return None
    order = ["mega", "extralarge", "large", "medium", "small"]
    by_size = {im.get("size"): (im.get("#text") or "").strip() for im in images}
    for sz in order:
        url = by_size.get(sz)
        if url:
            return url
    for im in images:
        u = (im.get("#text") or "").strip()
        if u:
            return u
    return None

def req_with_retries(params: dict, timeout=30):
    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.get(API_ROOT, params=params, timeout=timeout)
            if r.status_code == 429:
                raise requests.HTTPError("429 Too Many Requests", response=r)
            return r
        except Exception as e:
            if attempt >= MAX_RETRIES:
                raise
            sleep = (RETRY_BACKOFF_BASE ** (attempt - 1))
            print(f"[WARN] Request-Fehler (Versuch {attempt}/{MAX_RETRIES}): {e} → warte {sleep:.1f}s", file=sys.stderr)
            time.sleep(sleep)

def fetch_recent(user: str, api_key: str, from_uts: Optional[int], to_uts: Optional[int] = None) -> list[dict]:
    page = 1
    collected = []
    base = {
        "method": "user.getRecentTracks",
        "user": user,
        "api_key": api_key,
        "format": "json",
        "limit": str(PAGE_LIMIT),
    }
    if from_uts is not None:
        base["from"] = str(from_uts)
    if to_uts is not None:
        base["to"] = str(to_uts)

    while True:
        params = dict(base, page=str(page))
        r = req_with_retries(params)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(f"Last.fm API Fehler: {data.get('message', data.get('error'))}")

        items = (data.get("recenttracks") or {}).get("track", []) or []
        if not items:
            break

        batch = []
        for t in items:
            if t.get("@attr", {}).get("nowplaying") == "true":
                continue
            date = t.get("date") or {}
            uts_str = date.get("uts")
            if not uts_str:
                continue
            uts = int(uts_str)
            played_at = iso_from_uts(uts)

            artist_obj = t.get("artist") or {}
            album_obj  = t.get("album") or {}
            images     = t.get("image") or []
            cover_url  = largest_image_url(images)

            batch.append({
                "artist": artist_obj.get("#text"),
                "track": t.get("name"),
                "album": album_obj.get("#text") or None,
                "played_at_utc": played_at,
                "lastfm_url": t.get("url"),
                # MBIDs direkt aus der Last.fm-Antwort:
                "mbid_track": t.get("mbid") or None,
                "mbid_artist": artist_obj.get("mbid") or None,
                "mbid_album": album_obj.get("mbid") or None,
                # Medien:
                "cover_url": cover_url,
                # Platzhalter für evtl. spätere Felder:
                "loved": False,
                "duration_sec": None,
                "source": "lastfm",
            })
        collected.extend(batch)

        attr = (data.get("recenttracks") or {}).get("@attr", {}) or {}
        total_pages = int(attr.get("totalPages", "1"))
        if page >= total_pages:
            break
        page += 1
        time.sleep(REQUEST_SLEEP_SEC)

    collected.sort(key=lambda x: x["played_at_utc"], reverse=True)
    return collected

def bucket_by_month(items: list[dict]) -> dict[tuple[str, str], list[dict]]:
    buckets = defaultdict(list)
    for e in items:
        y, m = year_month_from_iso(e["played_at_utc"])
        buckets[(y, m)].append(e)
    return buckets

def write_month_buckets(buckets: dict[tuple[str, str], list[dict]]):
    total_written = 0
    for (y, m), items in buckets.items():
        path = DATA_DIR / y / f"{m}.yml"
        existing = load_yaml(path)
        merged = dedupe_merge(existing, items)
        save_yaml(path, merged)
        total_written += len(items)
        print(f"[OK] {y}/{m}: +{len(items)} (gesamt: {len(merged)}) → {path}")
    return total_written

def backfill_year(user: str, api_key: str, year: int):
    # UTC-Zeitrand für das Jahr
    start = datetime.datetime(year, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    end   = datetime.datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    from_uts = int(start.timestamp())
    to_uts   = int(end.timestamp()) - 1
    print(f"[INFO] Backfill Jahr {year} (uts {from_uts}–{to_uts}) …")

    items = fetch_recent(user, api_key, from_uts=from_uts, to_uts=to_uts)
    if not items:
        print(f"[INFO] Jahr {year}: keine Scrobbles.")
        return 0
    buckets = bucket_by_month(items)
    return write_month_buckets(buckets)

def parse_years_spec(spec: str) -> list[int]:
    years: set[int] = set()
    parts = [p.strip() for p in spec.split(",") if p.strip()]
    for p in parts:
        if "-" in p:
            a, b = [int(x) for x in p.split("-", 1)]
            lo, hi = min(a, b), max(a, b)
            years.update(range(lo, hi + 1))
        else:
            years.add(int(p))
    return sorted(years)

def incremental_since_latest(user: str, api_key: str):
    latest = newest_uts_from_files()
    if latest is None:
        print("[INFO] Keine vorhandenen Monatsdateien – bitte zuerst jahresweise backfillen (--backfill-years).")
        return
    from_uts = latest + 1
    print(f"[INFO] Inkrementeller Sync seit uts={from_uts} …")
    items = fetch_recent(user, api_key, from_uts=from_uts, to_uts=None)
    if not items:
        print("[INFO] Keine neuen Scrobbles.")
        return
    buckets = bucket_by_month(items)
    n = write_month_buckets(buckets)
    print(f"[DONE] Inkrementell abgeschlossen: {n} neue Einträge.")

def parse_args():
    p = argparse.ArgumentParser(description="Last.fm → YAML (monatlich) mit Cover + MBIDs + jahresweisem Backfill")
    p.add_argument("--user", required=True, help="Last.fm Benutzername (z. B. heikobielinski)")
    p.add_argument("--backfill-years", help="Jahresliste/-bereiche, z. B. '2010-2012,2015,2018-2019'")
    return p.parse_args()

def main():
    ensure_data_dir()
    args = parse_args()
    api_key = os.environ.get("LASTFM_API_KEY")
    if not api_key:
        print("ERROR: Environment-Variable LASTFM_API_KEY fehlt.", file=sys.stderr)
        sys.exit(1)

    if args.backfill_years:
        years = parse_years_spec(args.backfill_years)
        total = 0
        for y in years:
            try:
                total += backfill_year(args.user, api_key, y)
                time.sleep(1.0)  # kleine Pause zwischen Jahren
            except Exception as e:
                print(f"[ERROR] Backfill {y} abgebrochen: {e}", file=sys.stderr)
                time.sleep(2.0)
        print(f"[DONE] Jahres-Backfill abgeschlossen: {total} Einträge.")
    else:
        incremental_since_latest(args.user, api_key)

if __name__ == "__main__":
    main()
