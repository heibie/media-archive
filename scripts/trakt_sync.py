#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trakt → YAML Sync (inkrementell, mit TMDB-Enrichment)
- Holt neue History ab letztem Cursor/YAML
- Schreibt **Movies** nach _data/watched_movies.yml im alten Frontend-Format
- Schreibt **Episoden** nach _data/watched_episodes.yml im alten Frontend-Format
- TMDB-Enrichment (de-DE)
- Refresh-Flow; neue Tokens nach .trakt_tokens.json (Rotation im Workflow)

Debug-Logs:
- zeigt CWD, REPO_ROOT, OUTPUT_DIR, Pfade, GITHUB_WORKSPACE
- zeigt jeden rohen Trakt-History-Eintrag (EP/MOV)
- zeigt normalisierte Items
- zeigt beim Merge: ADD/UPDATE inkl. Key
- zeigt Datei-Status vor/nach Write + Tail der YAMLs
- Cursor = neuestes watched_on – 1s (Boundary-sicher)

ENV (required):
  TRAKT_CLIENT_ID
  TRAKT_CLIENT_SECRET
  TRAKT_REFRESH_TOKEN
  TMDB_API_KEY

ENV (optional):
  TRAKT_ACCESS_TOKEN
  OUTPUT_DIR                (default: "_data")
  TRAKT_HISTORY_LIMIT       (default: "200")
  TRAKT_HISTORY_PAGES       (default: "5")
  TRAKT_START_AT_ISO        (default: automatisch aus Cursor/YAML)
"""

import os
import sys
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

import requests
import yaml

# -----------------------------
# Pfade
# -----------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
WS = os.environ.get("GITHUB_WORKSPACE", "")
REPO_ROOT = Path(WS).resolve() if WS else SCRIPT_DIR.parent.resolve()

OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "_data"))
if not OUTPUT_DIR.is_absolute():
    OUTPUT_DIR = (REPO_ROOT / OUTPUT_DIR).resolve()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MOVIES_YAML   = OUTPUT_DIR / "watched_movies.yml"
EPISODES_YAML = OUTPUT_DIR / "watched_episodes.yml"
CURSOR_FILE   = REPO_ROOT / ".trakt_cursor"
TOKENS_OUT    = REPO_ROOT / ".trakt_tokens.json"

# -----------------------------
# Konfiguration
# -----------------------------
TRAKT_BASE = "https://api.trakt.tv"
TMDB_BASE  = "https://api.themoviedb.org/3"
IMG_BASE   = "https://image.tmdb.org/t/p"
USER_AGENT = "trakt-yaml-sync/2.0 (+github actions)"

TRAKT_CLIENT_ID     = os.environ.get("TRAKT_CLIENT_ID", "")
TRAKT_CLIENT_SECRET = os.environ.get("TRAKT_CLIENT_SECRET", "")
TRAKT_ACCESS_TOKEN  = os.environ.get("TRAKT_ACCESS_TOKEN", "")
TRAKT_REFRESH_TOKEN = os.environ.get("TRAKT_REFRESH_TOKEN", "")
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

def only_date(iso_ts: Optional[str]) -> Optional[str]:
    if not iso_ts:
        return None
    try:
        return iso_ts.split("T", 1)[0]
    except Exception:
        return iso_ts

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
            if w and len(w) == 10 and w.count("-") == 2:
                w_iso = f"{w}T00:00:00Z"
            else:
                w_iso = w
            d = parse_iso(w_iso) if w_iso else None
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

def stat_path(p: Path) -> str:
    try:
        if not p.exists():
            return f"{p} [exists=False]"
        s = p.stat()
        return f"{p} [exists=True size={s.st_size}B mtime={datetime.fromtimestamp(s.st_mtime).isoformat()}]"
    except Exception as e:
        return f"{p} [stat error: {e}]"

# -----------------------------
# Trakt OAuth & API
# -----------------------------
def trakt_refresh_tokens() -> Tuple[bool, Optional[str], Optional[str]]:
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
# TMDB (de-DE)
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

# -----------------------------
# Enrichment-Helfer
# -----------------------------
def enrich_show(show_tmdb_id: Optional[int], title: Optional[str], year: Optional[int]) -> Dict[str, Any]:
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

def enrich_episode(show_tmdb_id: Optional[int], season: Optional[int], number: Optional[int]) -> Dict[str, Any]:
    if not (show_tmdb_id and season is not None and number is not None):
        return {}
    ep = tmdb_get(f"/tv/{show_tmdb_id}/season/{season}/episode/{number}", {"append_to_response": "external_ids"})
    return ep or {}

def enrich_season_meta(show_tmdb_id: Optional[int], season: Optional[int]) -> Dict[str, Any]:
    if not (show_tmdb_id and season is not None):
        return {}
    det = tmdb_get(f"/tv/{show_tmdb_id}/season/{season}", {})
    return det or {}

def enrich_movie_by_tmdb_ids(tmdb_id: Optional[int], imdb_id: Optional[str], title: str, year: Optional[int]) -> Dict[str, Any]:
    movie = {}
    if tmdb_id:
        data = tmdb_get(f"/movie/{tmdb_id}", {"append_to_response": "external_ids"})
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

# -----------------------------
# Normalisierung (mit history_id)
# -----------------------------
def normalize_movie_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if item.get("type") != "movie" or "movie" not in item:
        return None
    m = item["movie"]
    w = item.get("watched_at")
    out = {
        "type": "movie",
        "history_id": item.get("id"),
        "title": m.get("title"),
        "year": m.get("year"),
        "ids": m.get("ids", {}),
        "watched_on": w,
        "action": item.get("action"),
    }
    return out

def normalize_episode_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if item.get("type") != "episode" or "episode" not in item:
        return None
    e = item["episode"]
    s = item.get("show", {})
    w = item.get("watched_at")
    out = {
        "type": "episode",
        "history_id": item.get("id"),
        "show": s.get("title"),
        "year": s.get("year"),
        "ids": {"show": s.get("ids", {}), "episode": e.get("ids", {})},
        "season": e.get("season"),
        "episode": e.get("number"),
        "title": e.get("title"),
        "watched_on": w,
        "action": item.get("action"),
    }
    return out

# -----------------------------
# Keys (pro Watch via history_id)
# -----------------------------
def movie_key(r: Dict[str, Any]):
    if r.get("history_id") is not None:
        return ("hist", r["history_id"])
    ids = r.get("ids", {}) or {}
    return ids.get("trakt") or ("movie-fallback", r.get("title"), r.get("year"), r.get("watched_on"))

def episode_key(r: Dict[str, Any]):
    if r.get("history_id") is not None:
        return ("hist", r["history_id"])
    ids = (r.get("ids", {}) or {}).get("episode", {}) or {}
    k = ids.get("trakt")
    if k:
        return ("ep", k, r.get("watched_on"))
    return ("ep-fallback", r.get("show"), r.get("season"), r.get("episode"), r.get("watched_on"))

# -----------------------------
# Merge mit detailliertem Logging
# -----------------------------
def add_or_update_verbose(records: List[Dict[str, Any]],
                          new_items: List[Dict[str, Any]],
                          key_fn,
                          kind: str) -> Tuple[List[Dict[str, Any]], int, int]:
    index = {key_fn(r): i for i, r in enumerate(records) if key_fn(r) is not None}
    new_count = 0
    upd_count = 0
    for it in new_items:
        k = key_fn(it)
        if it.get("type") == "episode":
            label = f"{it.get('show')} S{it.get('season')}E{it.get('episode')} @ {it.get('watched_on')} [hist={it.get('history_id')}]"
        else:
            label = f"{it.get('title')} ({it.get('year')}) @ {it.get('watched_on')} [hist={it.get('history_id')}]"
        if k in index:
            records[index[k]] = it
            upd_count += 1
            log(f"{kind}: UPDATE  -> {label}  key={k}")
        else:
            records.append(it)
            index[k] = len(records) - 1
            new_count += 1
            log(f"{kind}: ADD     -> {label}  key={k}")
    return records, new_count, upd_count

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
        for raw in batch:
            t = raw.get("type")
            if t == "episode":
                e = raw.get("episode") or {}
                s = raw.get("show") or {}
                log(f"  raw: EP  hist={raw.get('id')}  {s.get('title')} S{e.get('season')}E{e.get('number')}  watched_at={raw.get('watched_at')}  ids.ep.trakt={(e.get('ids') or {}).get('trakt')}")
            elif t == "movie":
                m = raw.get("movie") or {}
                log(f"  raw: MOV hist={raw.get('id')}  {m.get('title')}({m.get('year')})  watched_at={raw.get('watched_at')}  ids.trakt={(m.get('ids') or {}).get('trakt')}")
        collected.extend(batch)
    return collected

# -----------------------------
# Frontend-Format: Episoden & Filme
# -----------------------------
def img_or_none(path: Optional[str], variant: str) -> Optional[str]:
    if not path:
        return None
    return f"{IMG_BASE}/{variant}{path}"

def episode_to_frontend(e: Dict[str, Any]) -> Dict[str, Any]:
    show_ids = (e.get("ids") or {}).get("show") or {}
    tmdb_show = e.get("tmdb_show") or {}
    tmdb_ep   = e.get("tmdb_episode") or {}
    season_meta = e.get("tmdb_season") or {}
    data = {
        "show": e.get("show"),
        "year": e.get("year"),
        "season": e.get("season"),
        "episode": e.get("episode"),
        "plays": 1,
        "watched_on": only_date(e.get("watched_on")),
        "trakt_show": show_ids.get("trakt"),
        "tvdb": show_ids.get("tvdb"),
        "imdb": show_ids.get("imdb"),
        "tmdb": show_ids.get("tmdb") or tmdb_show.get("id"),
        "slug": show_ids.get("slug"),
        "source": "trakt",
        "show_title_de": tmdb_show.get("name") or e.get("show"),
        "show_poster": img_or_none(tmdb_show.get("poster_path"), "w500"),
        "show_backdrop": img_or_none(tmdb_show.get("backdrop_path"), "w780"),
        "show_total_episodes": tmdb_show.get("number_of_episodes"),
        "episode_title": e.get("title") or (tmdb_ep.get("name") or None),
        "episode_title_de": tmdb_ep.get("name") or None,
        "episode_runtime": tmdb_ep.get("runtime") or (tmdb_show.get("episode_run_time", [None])[0] if tmdb_show.get("episode_run_time") else None),
        "season_total_episodes": len(season_meta.get("episodes", [])) if season_meta.get("episodes") else None,
        "episode_still": img_or_none(tmdb_ep.get("still_path"), "w300"),
    }
    return {k: v for k, v in data.items() if v is not None}

def movie_to_frontend(m: Dict[str, Any]) -> Dict[str, Any]:
    ids = m.get("ids") or {}
    tmdb = m.get("tmdb") or {}
    data = {
        "title": m.get("title"),
        "year": m.get("year"),
        "imdb": ids.get("imdb"),
        "tmdb": ids.get("tmdb") or tmdb.get("id"),
        "trakt": ids.get("trakt"),
        "slug": ids.get("slug"),
        "plays": 1,
        "watched_on": only_date(m.get("watched_on")),
        "poster": img_or_none(tmdb.get("poster_path"), "w500"),
        "backdrop": img_or_none(tmdb.get("backdrop_path"), "w780"),
        "source": "trakt",
        "runtime": tmdb.get("runtime"),
        "title_de": tmdb.get("title") or tmdb.get("original_title") or m.get("title"),
        "overview_de": tmdb.get("overview"),
    }
    return {k: v for k, v in data.items() if v is not None}

# -----------------------------
# MAIN
# -----------------------------
def main():
    # Debug: Pfade & Umgebung
    try:
        log(f"CWD={os.getcwd()}")
    except Exception:
        pass
    log(f"REPO_ROOT={REPO_ROOT}")
    log(f"OUTPUT_DIR={OUTPUT_DIR}")
    log(f"MOVIES_YAML={MOVIES_YAML}")
    log(f"EPISODES_YAML={EPISODES_YAML}")
    log(f"CURSOR_FILE={CURSOR_FILE}")
    log(f"GITHUB_WORKSPACE={os.environ.get('GITHUB_WORKSPACE','<unset>')}")

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
        log("Keine neuen History-Items. Cursor bleibt unverändert.")
        return

    movies_raw, episodes_raw = [], []
    for it in history:
        if it.get("type") == "movie":
            nm = normalize_movie_item(it)
            if nm:
                movies_raw.append(nm)
                log(f"  norm: MOV hist={nm.get('history_id')}  {nm.get('title')}({nm.get('year')})  watched_on={nm.get('watched_on')}")
        elif it.get("type") == "episode":
            ne = normalize_episode_item(it)
            if ne:
                episodes_raw.append(ne)
                log(f"  norm: EP  hist={ne.get('history_id')}  {ne.get('show')} S{ne.get('season')}E{ne.get('episode')}  watched_on={ne.get('watched_on')}")

    log(f"Enrichment: {len(movies_raw)} Movies, {len(episodes_raw)} Episodes …")

    # Movies enrichment
    enriched_movies = []
    for m in movies_raw:
        ids = m.get("ids", {}) or {}
        info = {}
        try:
            info = enrich_movie_by_tmdb_ids(ids.get("tmdb"), ids.get("imdb"), m.get("title") or "", m.get("year"))
        except Exception as e:
            log(f"Movie-Enrichment Fehler ({m.get('title')}): {e}")
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

    # Episodes enrichment (Show + Episode + Season)
    enriched_eps = []
    for e in episodes_raw:
        show_ids = (e.get("ids") or {}).get("show") or {}
        show_tmdb_id = show_ids.get("tmdb")
        show_title   = e.get("show")
        show_year    = e.get("year")

        show_det = enrich_show(show_tmdb_id, show_title, show_year) or {}
        if show_det:
            e["tmdb_show"] = show_det

        ep_det = enrich_episode(show_det.get("id") if show_det else show_tmdb_id, e.get("season"), e.get("episode")) or {}
        if ep_det:
            e["tmdb_episode"] = ep_det

        season_meta = enrich_season_meta(show_det.get("id") if show_det else show_tmdb_id, e.get("season")) or {}
        if season_meta:
            e["tmdb_season"] = season_meta

        enriched_eps.append(e)

    # Initialer Dateistatus & Listing
    log("Initial file state:")
    log("  " + stat_path(MOVIES_YAML))
    log("  " + stat_path(EPISODES_YAML))
    try:
        listing = "\n".join(sorted(os.listdir(OUTPUT_DIR)))
        log(f"OUTPUT_DIR listing:\n{listing}")
    except Exception as e:
        log(f"OUTPUT_DIR check/listing failed: {e}")

    # Merge (auf Basis der normalisierten Items)
    movies_all_norm   = yaml_load(MOVIES_YAML)   # wir überschreiben gleich mit Legacy-Form, aber zum Start ggf. leer
    episodes_all_norm = yaml_load(EPISODES_YAML)

    before_movies = len(movies_all_norm)
    before_eps    = len(episodes_all_norm)

    merged_movies_norm, new_m, upd_m = add_or_update_verbose(movies_all_norm, enriched_movies, movie_key, "MOV")
    merged_eps_norm,  new_e, upd_e  = add_or_update_verbose(episodes_all_norm, enriched_eps,  episode_key, "EP ")

    # ➜ VOR dem Schreiben in Legacy-Form mappen
    legacy_movies = [movie_to_frontend(m) for m in merged_movies_norm]
    legacy_eps    = [episode_to_frontend(e) for e in merged_eps_norm]

    # Sortierung: neueste zuerst
    legacy_movies.sort(key=lambda r: (r.get("watched_on") or ""), reverse=True)
    legacy_eps.sort(key=lambda r: (r.get("watched_on") or "", r.get("season") or 0, r.get("episode") or 0), reverse=True)

    # Write + Tail (Movies)
    log("Vor Write (Movies): " + stat_path(MOVIES_YAML))
    yaml_dump(MOVIES_YAML, legacy_movies)
    log("Nach Write (Movies): " + stat_path(MOVIES_YAML))
    try:
        with MOVIES_YAML.open("r", encoding="utf-8") as f:
            lines = f.readlines()
        tail = "".join(lines[-10:]) if len(lines) > 10 else "".join(lines)
        log("Tail Movies YAML:\n" + tail)
    except Exception as e:
        log(f"Tail Movies YAML fehlgeschlagen: {e}")

    # Write + Tail (Episodes)
    log("Vor Write (Episodes): " + stat_path(EPISODES_YAML))
    yaml_dump(EPISODES_YAML, legacy_eps)
    log("Nach Write (Episodes): " + stat_path(EPISODES_YAML))
    try:
        with EPISODES_YAML.open("r", encoding="utf-8") as f:
            lines = f.readlines()
        tail = "".join(lines[-10:]) if len(lines) > 10 else "".join(lines)
        log("Tail Episodes YAML:\n" + tail)
    except Exception as e:
        log(f"Tail Episodes YAML fehlgeschlagen: {e}")

    after_movies = len(legacy_movies)
    after_eps    = len(legacy_eps)
    log(f"Aktualisiert: {MOVIES_YAML}, {EPISODES_YAML}")
    log(f"Movies: {before_movies} → {after_movies} (neu: {new_m}, aktualisiert: {upd_m})")
    log(f"Episodes: {before_eps} → {after_eps} (neu: {new_e}, aktualisiert: {upd_e})")

    # Cursor fortschreiben: neuestes watched_on – 1s
    newest_ts = None
    for it in (movies_raw + episodes_raw):
        ts = it.get("watched_on")
        if ts and (newest_ts is None or ts > newest_ts):
            newest_ts = ts
    if newest_ts:
        dt = parse_iso(newest_ts)
        cursor_iso = (dt - timedelta(seconds=1)).isoformat().replace("+00:00", "Z") if dt else newest_ts
        write_cursor(cursor_iso)
        log(f"Cursor aktualisiert auf: {cursor_iso}")
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
