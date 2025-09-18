#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trakt → YAML Sync (inkrementell, mit TMDB-Enrichment)
- Holt neue History ab letztem Cursor/YAML
- Schreibt nach _data/watched_movies.yml & _data/watched_episodes.yml
- TMDB-Enrichment (de-DE)
- Access-/Refresh-Token-Refresh; bei Erfolg werden die neuen Tokens in .trakt_tokens.json
  im Repo-Root gespeichert. Die Secret-Rotation übernimmt der Workflow.

ENV (required):
  TRAKT_CLIENT_ID
  TRAKT_CLIENT_SECRET
  TRAKT_REFRESH_TOKEN
  TMDB_API_KEY

ENV (optional):
  TRAKT_ACCESS_TOKEN
  TRAKT_USERNAME
  OUTPUT_DIR                (default: "_data")
  TRAKT_HISTORY_LIMIT       (default: "200")
  TRAKT_HISTORY_PAGES       (default: "5")
  TRAKT_START_AT_ISO        (default: wird automatisch aus Cursor/YAML bestimmt)
"""

import os
import sys
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

import requests
import yaml

# -----------------------------
# Pfade
# -----------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "_data"))
if not OUTPUT_DIR.is_absolute():
    OUTPUT_DIR = REPO_ROOT / OUTPUT_DIR
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MOVIES_YAML   = OUTPUT_DIR / "watched_movies.yml"
EPISODES_YAML = OUTPUT_DIR / "watched_episodes.yml"
CURSOR_FILE   = REPO_ROOT / ".trakt_cursor"       # ISO-String
TOKENS_OUT    = REPO_ROOT / ".trakt_tokens.json"  # neue Tokens für Workflow-Rotation

# -----------------------------
# Konfiguration
# -----------------------------
TRAKT_BASE = "https://api.trakt.tv"
TMDB_BASE  = "https://api.themoviedb.org/3"
USER_AGENT = "trakt-yaml-sync/1.3 (+github actions)"

TRAKT_CLIENT_ID     = os.environ.get("TRAKT_CLIENT_ID", "")
TRAKT_CLIENT_SECRET = os.environ.get("TRAKT_CLIENT_SECRET", "")
TRAKT_ACCESS_TOKEN  = os.environ.get("TRAKT_ACCESS_TOKEN", "")
TRAKT_REFRESH_TOKEN = os.environ.get("TRAKT_REFRESH_TOKEN", "")
TRAKT_USERNAME      = os.environ.get("TRAKT_USERNAME", "")

TMDB_API_KEY        = os.environ.get("TMDB_API_KEY", "")

if not (TRAKT_CLIENT_ID and TRAKT_CLIENT_SECRET and TRAKT_REFRESH_TOKEN and TMDB_API_KEY):
    print("[trakt-sync] ERROR: Missing required env (TRAKT_CLIENT_ID/SECRET/REFRESH_TOKEN + TMDB_API_KEY).", file=sys.stderr)
    sys.exit(1)

TRAKT_HEADERS = {
    "Content-Type": "application/json",
    "trakt-api-version": "2",
    "trakt-api-key": TRAKT_CLIENT_ID,
    "User-Agent": USER_AGENT,
}
if TRAKT_ACCESS_TOKEN:
    TRAKT_HEADERS["Authorization"] = f"Bearer {TRAKT_ACCESS_TOKEN}"

SESSION = requests.Session()
SESSION.headers.update(TRAKT_HEADERS)

# -----------------------------
# Utils
# -----------------------------
def log(msg: str):
    print(f"[trakt-sync] {msg}")

def iso_now_z() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def yaml_load(path: Path):
    if not path.exists():
        return []
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or []
    except Exception as e:
        log(f"Warn: YAML konnte nicht geladen werden ({path}): {e}")
        return []

def yaml_dump(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)

def parse_iso(s: str) -> Optional[datetime]:
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None

def read_cursor_env_or_file() -> Optional[str]:
    v = os.environ.get("TRAKT_START_AT_ISO", "").strip()
    if v:
        return v
    if CURSOR_FILE.exists():
        try:
            txt = CURSOR_FILE.read_text(encoding="utf-8").strip()
            return txt or None
        except Exception:
            return None
    return None

def write_cursor(iso_str: str):
    CURSOR_FILE.write_text(iso_str, encoding="utf-8")

def latest_watched_iso_from_yaml() -> Optional[str]:
    max_dt: Optional[datetime] = None
    for path in (MOVIES_YAML, EPISODES_YAML):
        arr = yaml_load(path)
        for row in arr:
            w = row.get("watched_on") or row.get("watched_at")
            if not w:
                continue
            d = parse_iso(w)
            if d and (max_dt is None or d > max_dt):
                max_dt = d
    if max_dt:
        return max_dt.isoformat().replace("+00:00", "Z")
    return None

def determine_start_at() -> Optional[str]:
    return read_cursor_env_or_file() or latest_watched_iso_from_yaml()

def save_tokens_file(access_token: str, refresh_token: str):
    try:
        TOKENS_OUT.write_text(
            json.dumps({"access_token": access_token, "refresh_token": refresh_token}, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
    except Exception:
        pass

# -----------------------------
# Trakt OAuth & API
# -----------------------------
def trakt_refresh_tokens() -> Tuple[bool, Optional[str], Optional[str]]:
    """Refresh Trakt tokens. Returns (ok, new_access, new_refresh)."""
    global TRAKT_ACCESS_TOKEN, TRAKT_REFRESH_TOKEN
    payload = {
        "refresh_token": TRAKT_REFRESH_TOKEN,
        "client_id": TRAKT_CLIENT_ID,
        "client_secret": TRAKT_CLIENT_SECRET,
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
        "grant_type": "refresh_token",
    }
    try:
        r = requests.post(f"{TRAKT_BASE}/oauth/token", json=payload,
                          headers={"Content-Type": "application/json", "User-Agent": USER_AGENT}, timeout=30)
    except requests.RequestException as e:
        log(f"Token-Refresh exception: {e}")
        return False, None, None

    if r.status_code != 200:
        # zeige begrenzt den Body (z.B. {"error":"invalid_grant",...})
        body = r.text
        if body and len(body) > 500:
            body = body[:500] + "…"
        log(f"Token-Refresh failed: HTTP {r.status_code} {r.reason}")
        if body:
            log(f"Body: {body}")
        return False, None, None

    tok = r.json()
    new_access  = tok.get("access_token")
    new_refresh = tok.get("refresh_token")
    if not (new_access and new_refresh):
        log("Token-Refresh: Antwort ohne Tokens.")
        return False, None, None

    # Session-Header aktualisieren
    SESSION.headers["Authorization"] = f"Bearer {new_access}"
    TRAKT_ACCESS_TOKEN  = new_access
    TRAKT_REFRESH_TOKEN = new_refresh

    log("Refreshed Trakt access token.")
    save_tokens_file(new_access, new_refresh)
    return True, new_access, new_refresh

def trakt_get(path: str, params: Optional[Dict[str, Any]] = None, retry_on_401: bool = True) -> requests.Response:
    url = f"{TRAKT_BASE}{path}"
    r = SESSION.get(url, params=params or {}, timeout=45)
    if r.status_code == 401 and retry_on_401:
        log("401 from Trakt. Attempting token refresh…")
        ok, _, _ = trakt_refresh_tokens()
        if not ok:
            raise RuntimeError("Token-Refresh fehlgeschlagen.")
        r = SESSION.get(url, params=params or {}, timeout=45)
    r.raise_for_status()
    return r

# -----------------------------
# TMDB Enrichment (de-DE)
# -----------------------------
def tmdb_get(path: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    params = dict(params)
    params["api_key"] = TMDB_API_KEY
    params.setdefault("language", "de-DE")
    try:
        resp = requests.get(f"{TMDB_BASE}{path}", params=params, timeout=45)
        if resp.status_code != 200:
            return None
        return resp.json()
    except requests.RequestException:
        return None

def enrich_movie_by_tmdb_ids(tmdb_id: Optional[int], imdb_id: Optional[str], title: str, year: Optional[int]) -> Dict[str, Any]:
    movie = {}
    if tmdb_id:
        data = tmdb_get(f"/movie/{tmdb_id}", {"append_to_response": "internal,external_ids"})
        if data:
            movie = data
    if not movie and imdb_id:
        data = tmdb_get(f"/find/{imdb_id}", {"external_source": "imdb_id"})
        if data and data.get("movie_results"):
            hit = data["movie_results"][0]
            det = tmdb_get(f"/movie/{hit['id']}", {"append_to_response": "external_ids"})
            if det:
                movie = det
    if not movie:
        params = {"query": title}
        if year: params["year"] = year
        sr = tmdb_get("/search/movie", params)
        if sr and sr.get("results"):
            hit = sr["results"][0]
            det = tmdb_get(f"/movie/{hit['id']}", {"append_to_response": "external_ids"})
            if det:
                movie = det
    return movie or {}

def enrich_episode_by_tmdb_ids(show_tmdb_id: Optional[int], ep_season: Optional[int], ep_number: Optional[int]) -> Dict[str, Any]:
    if not (show_tmdb_id and ep_season is not None and ep_number is not None):
        return {}
    ep = tmdb_get(f"/tv/{show_tmdb_id}/season/{ep_season}/episode/{ep_number}", {"append_to_response": "external_ids"})
    return ep or {}

def enrich_show_by_tmdb_id(show_tmdb_id: Optional[int], title: Optional[str], year: Optional[int]) -> Dict[str, Any]:
    show = {}
    if show_tmdb_id:
        det = tmdb_get(f"/tv/{show_tmdb_id}", {"append_to_response": "external_ids"})
        if det:
            show = det
    if not show and title:
        params = {"query": title}
        if year: params["first_air_date_year"] = year
        sr = tmdb_get("/search/tv", params)
        if sr and sr.get("results"):
            hit = sr["results"][0]
            det = tmdb_get(f"/tv/{hit['id']}", {"append_to_response": "external_ids"})
            if det:
                show = det
    return show or {}

# -----------------------------
# Normalisierung
# -----------------------------
def normalize_movie_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if item.get("type") != "movie" or "movie" not in item:
        return None
    m = item["movie"]
    w = item.get("watched_at")
    return {
        "type": "movie",
        "title": m.get("title"),
        "year": m.get("year"),
        "ids": m.get("ids", {}),
        "watched_on": w,
        "action": item.get("action"),
    }

def normalize_episode_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if item.get("type") != "episode" or "episode" not in item:
        return None
    e = item["episode"]
    s = item.get("show", {})
    w = item.get("watched_at")
    return {
        "type": "episode",
        "show": s.get("title"),
        "year": s.get("year"),
        "ids": {"show": s.get("ids", {}), "episode": e.get("ids", {})},
        "season": e.get("season"),
        "episode": e.get("number"),
        "title": e.get("title"),
        "watched_on": w,
        "action": item.get("action"),
    }

# -----------------------------
# Merge/Dedupe
# -----------------------------
def add_or_update(records: List[Dict[str, Any]], new_items: List[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    index = {key_fn(r): i for i, r in enumerate(records) if key_fn(r) is not None}
    for it in new_items:
        k = key_fn(it)
        if k is None:
            continue
        if k in index:
            records[index[k]] = it
        else:
            records.append(it)
    return records

def movie_key(r: Dict[str, Any]):
    ids = r.get("ids", {}) or {}
    return ids.get("trakt") or (r.get("title"), r.get("year"), r.get("watched_on"))

def episode_key(r: Dict[str, Any]):
    ids = (r.get("ids", {}) or {}).get("episode", {}) or {}
    k = ids.get("trakt")
    if k:
        return k
    return (r.get("show"), r.get("season"), r.get("episode"), r.get("watched_on"))

# -----------------------------
# Fetch History
# -----------------------------
def fetch_trakt_history(start_at: Optional[str], limit: int, pages: int) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    for page in range(1, pages + 1):
        params = {"limit": limit, "page": page}
        if start_at:
            params["start_at"] = start_at
        r = trakt_get("/sync/history", params=params)
        batch = r.json()
        if not batch:
            break
        collected.extend(batch)
    return collected

# -----------------------------
# MAIN
# -----------------------------
def main():
    # /users/me (triggert ggf. 401 → Refresh)
    try:
        _ = trakt_get("/users/me").json()
    except Exception as e:
        log(f"/users/me check: {e}")

    start_at = determine_start_at()
    if start_at:
        log(f"Starte ab: {start_at}")
    else:
        log("Kein Cursor gefunden – hole aktuelle History ohne start_at.")

    limit = int(os.environ.get("TRAKT_HISTORY_LIMIT", "200"))
    pages = int(os.environ.get("TRAKT_HISTORY_PAGES", "5"))

    history = fetch_trakt_history(start_at, limit, pages)
    log(f"Fetched {len(history)} history items von Trakt (start_at={start_at}).")
    if not history:
        log("Keine neuen History-Items.")
        write_cursor(iso_now_z())
        return

    movies_raw, episodes_raw = [], []
    for it in history:
        if it.get("type") == "movie":
            nm = normalize_movie_item(it)
            if nm: movies_raw.append(nm)
        elif it.get("type") == "episode":
            ne = normalize_episode_item(it)
            if ne: episodes_raw.append(ne)

    # Enrichment
    log(f"Enrichment: {len(movies_raw)} Movies, {len(episodes_raw)} Episodes …")
    def safe(fn, arg):
        try:
            return fn(arg)
        except Exception as e:
            # keine harte Unterbrechung bei TMDB-Fehlern
            log(f"Enrichment-Fehler: {e}")
            return arg

    # Movie-Enrichment
    enriched_movies = []
    for m in movies_raw:
        ids = m.get("ids", {}) or {}
        tmdb_id = ids.get("tmdb")
        imdb_id = ids.get("imdb")
        title   = m.get("title") or ""
        year    = m.get("year")
        info = {}
        try:
            info = enrich_movie_by_tmdb_ids(tmdb_id, imdb_id, title, year)
        except Exception as e:
            log(f"Movie-Enrichment Fehler ({title}): {e}")
        if info:
            m["tmdb"] = {
                "id": info.get("id"),
                "title": info.get("title") or info.get("original_title"),
                "original_title": info.get("original_title"),
                "overview": info.get("overview"),
                "poster_path": info.get("poster_path"),
                "backdrop_path": info.get("backdrop_path"),
                "release_date": info.get("release_date"),
                "genres": [g.get("name") for g in (info.get("genres") or []) if g.get("name")],
                "vote_average": info.get("vote_average"),
                "runtime": info.get("runtime"),
                "external_ids": info.get("external_ids", {}),
            }
        enriched_movies.append(m)

    # Episode-/Show-Enrichment
    enriched_eps = []
    for e in episodes_raw:
        show_ids = (e.get("ids") or {}).get("show") or {}
        show_tmdb_id = show_ids.get("tmdb")
        show_title   = e.get("show")
        show_year    = e.get("year")
        show_det = {}
        try:
            show_det = enrich_show_by_tmdb_id(show_tmdb_id, show_title, show_year)
        except Exception as ex:
            log(f"Show-Enrichment Fehler ({show_title}): {ex}")
        if show_det:
            e["tmdb_show"] = {
                "id": show_det.get("id"),
                "name": show_det.get("name") or show_det.get("original_name"),
                "overview": show_det.get("overview"),
                "poster_path": show_det.get("poster_path"),
                "backdrop_path": show_det.get("backdrop_path"),
                "first_air_date": show_det.get("first_air_date"),
                "genres": [g.get("name") for g in (show_det.get("genres") or []) if g.get("name")],
                "vote_average": show_det.get("vote_average"),
                "external_ids": show_det.get("external_ids", {}),
            }
        ep_det = {}
        try:
            ep_det = enrich_episode_by_tmdb_ids(show_det.get("id") if show_det else show_tmdb_id, e.get("season"), e.get("episode"))
        except Exception as ex:
            log(f"Episoden-Enrichment Fehler ({show_title} S{e.get('season')}E{e.get('episode')}): {ex}")
        if ep_det:
            e["tmdb_episode"] = {
                "id": ep_det.get("id"),
                "name": ep_det.get("name"),
                "overview": ep_det.get("overview"),
                "still_path": ep_det.get("still_path"),
                "air_date": ep_det.get("air_date"),
                "vote_average": ep_det.get("vote_average"),
                "external_ids": ep_det.get("external_ids", {}),
            }
        enriched_eps.append(e)

    # Merge + Sort
    movies_all = yaml_load(MOVIES_YAML)
    episodes_all = yaml_load(EPISODES_YAML)

    def movie_key(r: Dict[str, Any]):
        ids = r.get("ids", {}) or {}
        return ids.get("trakt") or (r.get("title"), r.get("year"), r.get("watched_on"))

    def episode_key(r: Dict[str, Any]):
        ids = (r.get("ids", {}) or {}).get("episode", {}) or {}
        k = ids.get("trakt")
        if k: return k
        return (r.get("show"), r.get("season"), r.get("episode"), r.get("watched_on"))

    def add_or_update(records: List[Dict[str, Any]], new_items: List[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
        index = {key_fn(r): i for i, r in enumerate(records) if key_fn(r) is not None}
        for it in new_items:
            k = key_fn(it)
            if k is None: continue
            if k in index:
                records[index[k]] = it
            else:
                records.append(it)
        return records

    movies_all   = add_or_update(movies_all, enriched_movies, movie_key)
    episodes_all = add_or_update(episodes_all, enriched_eps, episode_key)

    movies_all.sort(key=lambda r: (r.get("watched_on") or ""), reverse=True)
    episodes_all.sort(key=lambda r: (r.get("watched_on") or "", r.get("season") or 0, r.get("episode") or 0), reverse=True)

    yaml_dump(MOVIES_YAML, movies_all)
    yaml_dump(EPISODES_YAML, episodes_all)
    log(f"Aktualisiert: {MOVIES_YAML}, {EPISODES_YAML}")

    # Cursor fortschreiben (jetzt)
    write_cursor(iso_now_z())
    # Cursor fortschreiben: auf neuestes watched_at der frisch geholten Items
    newest = None
    for it in (movies_raw + episodes_raw):
        ts = it.get("watched_on")
        if ts and (newest is None or ts > newest):
            newest = ts
    if newest:
        # 1–2 Sekunden „zurückspringen“, um Boundary-Issues zu vermeiden (start_at ist inkl.)
        # und eventuelle Rundungs-/TZ-Edgecases abzufangen.
        # Wir rechnen ohne externe Libs, daher nur sicherheitshalber  'Z' belassen.
        write_cursor(newest)
        log(f"Cursor aktualisiert auf: {newest}")
    else:
        log("Keine neuen watched_at-Zeiten gefunden – Cursor unverändert.")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as http_err:
        sc = http_err.response.status_code if http_err.response is not None else "?"
        log(f"HTTP error: {http_err} (status {sc})")
        sys.exit(2)
    except RuntimeError as re:
        log(str(re))
        sys.exit(1)
    except Exception as e:
        log(f"Fatal error: {e}")
        sys.exit(2)
