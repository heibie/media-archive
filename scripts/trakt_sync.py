#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trakt → YAML Sync (strict append-only)
- Holt neue History ab (ab Cursor/YAML)
- Schreibt **nur neue** Einträge im Legacy-Format ans Ende der bestehenden YAMLs
- Existierende YAML-Einträge werden niemals angetastet/überschrieben
- Legt vor der ersten Änderung pro Run eine Backup-Datei an: *.bak-YYYYmmdd-HHMMSS
- Cursor = neuestes watched_on – 1s

ENV (required):
  TRAKT_CLIENT_ID, TRAKT_CLIENT_SECRET, TRAKT_REFRESH_TOKEN, TMDB_API_KEY
ENV (optional):
  TRAKT_ACCESS_TOKEN, OUTPUT_DIR, TRAKT_HISTORY_LIMIT, TRAKT_HISTORY_PAGES, TRAKT_START_AT_ISO
"""

import os, sys, json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests, yaml

# -----------------------------
# Pfade / Setup
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

TRAKT_BASE = "https://api.trakt.tv"
TMDB_BASE  = "https://api.themoviedb.org/3"
IMG_BASE   = "https://image.tmdb.org/t/p"
USER_AGENT = "trakt-yaml-sync/2.3-append-only (+github actions)"

TRAKT_CLIENT_ID     = os.environ.get("TRAKT_CLIENT_ID", "")
TRAKT_CLIENT_SECRET = os.environ.get("TRAKT_CLIENT_SECRET", "")
TRAKT_ACCESS_TOKEN  = os.environ.get("TRAKT_ACCESS_TOKEN", "")
TRAKT_REFRESH_TOKEN = os.environ.get("TRAKT_REFRESH_TOKEN", "")
TMDB_API_KEY        = os.environ.get("TMDB_API_KEY", "")

if not (TRAKT_CLIENT_ID and TRAKT_CLIENT_SECRET and TRAKT_REFRESH_TOKEN and TMDB_API_KEY):
    print("[trakt-sync] ERROR: Missing required env.", file=sys.stderr)
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
def log(msg: str): print(f"[trakt-sync] {msg}")

def yaml_load(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception as e:
        log(f"Warn: YAML load {path}: {e}")
        return []

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
    return iso_ts.split("T", 1)[0] if "T" in iso_ts else iso_ts

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

def latest_watched_iso_from_yaml() -> Optional[str]:
    max_dt = None
    for path in (MOVIES_YAML, EPISODES_YAML):
        for row in yaml_load(path):
            if not isinstance(row, dict):
                continue
            w = row.get("watched_on") or row.get("watched_at")
            w_iso = f"{w}T00:00:00Z" if w and len(w) == 10 and w.count("-") == 2 else w
            dt = parse_iso(w_iso) if w_iso else None
            if dt and (max_dt is None or dt > max_dt):
                max_dt = dt
    return max_dt.isoformat().replace("+00:00", "Z") if max_dt else None

def determine_start_at() -> Optional[str]:
    return read_cursor_env_or_file() or latest_watched_iso_from_yaml()

def write_cursor(iso_str: str):
    CURSOR_FILE.write_text(iso_str, encoding="utf-8")

def save_tokens_file(a: str, r: str):
    TOKENS_OUT.write_text(json.dumps({"access_token": a, "refresh_token": r}, indent=2), encoding="utf-8")

def stat_path(p: Path) -> str:
    try:
        if not p.exists():
            return f"{p} [exists=False]"
        s = p.stat()
        return f"{p} [exists=True size={s.st_size}B]"
    except Exception as e:
        return f"{p} [stat error: {e}]"

def as_dict(v): return v if isinstance(v, dict) else {}
def as_list(v): return v if isinstance(v, list) else []

def img_or_none(path: Optional[str], variant: str) -> Optional[str]:
    return f"{IMG_BASE}/{variant}{path}" if path else None

def append_yaml_items(path: Path, items: List[Dict[str, Any]]):
    """Hängt items als '- ' Einträge an bestehende Datei an. Schreibt NIE die bestehende Liste neu.
       Falls Datei nicht existiert: legt vollständige Liste an."""
    if not items:
        return

    # Falls Datei existiert: 1x Backup pro Run
    if path.exists():
        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        bak = Path(str(path) + f".bak-{ts}")
        try:
            bak.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
            log(f"Backup geschrieben: {bak.name}")
        except Exception as e:
            log(f"Backup fehlgeschlagen ({path}): {e}")

    if not path.exists():
        # Erstbefüllung: komplette Liste schreiben
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(items, f, allow_unicode=True, sort_keys=False)
        return

    # Anhängen: pro Item einen Listeneintrag dumpen
    # Damit PyYAML '- ' erzeugt, dumpen wir eine Ein-Eintrags-Liste
    with path.open("a", encoding="utf-8") as f:
        for it in items:
            txt = yaml.safe_dump([it], allow_unicode=True, sort_keys=False)
            # PyYAML hängt einen Zeilenumbruch an; wir schreiben direkt an
            f.write(txt)

# -----------------------------
# Trakt OAuth / API
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
                          headers={"Content-Type":"application/json","User-Agent":USER_AGENT}, timeout=30)
    except requests.RequestException as e:
        log(f"Token-Refresh exception: {e}")
        return False, None, None
    if r.status_code != 200:
        log(f"Token-Refresh failed: {r.status_code} {r.reason} {r.text[:300]}")
        return False, None, None
    tok = r.json()
    acc, ref = tok.get("access_token"), tok.get("refresh_token")
    if not (acc and ref):
        log("Token-Refresh: Antwort ohne Tokens.")
        return False, None, None
    SESSION.headers["Authorization"] = f"Bearer {acc}"
    TRAKT_ACCESS_TOKEN, TRAKT_REFRESH_TOKEN = acc, ref
    log("Refreshed Trakt access token.")
    save_tokens_file(acc, ref)
    return True, acc, ref

def trakt_get(path: str, params: Optional[Dict[str, Any]] = None, retry_on_401=True) -> requests.Response:
    url = f"{TRAKT_BASE}{path}"
    r = SESSION.get(url, params=params or {}, timeout=45)
    if r.status_code == 401 and retry_on_401:
        log("401 from Trakt → token refresh…")
        ok,_,_ = trakt_refresh_tokens()
        if not ok: raise RuntimeError("Token-Refresh fehlgeschlagen.")
        r = SESSION.get(url, params=params or {}, timeout=45)
    r.raise_for_status()
    return r

# -----------------------------
# TMDB (de-DE)
# -----------------------------
def tmdb_get(path: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    p = dict(params); p["api_key"]=TMDB_API_KEY; p.setdefault("language","de-DE")
    try:
        resp = requests.get(f"{TMDB_BASE}{path}", params=p, timeout=45)
        if resp.status_code != 200: return None
        return resp.json()
    except requests.RequestException:
        return None

def enrich_show(show_tmdb_id: Optional[int], title: Optional[str], year: Optional[int]) -> Dict[str, Any]:
    show={}
    if show_tmdb_id:
        det=tmdb_get(f"/tv/{show_tmdb_id}", {"append_to_response":"external_ids"})
        if det: show=det
    if not show and title:
        params={"query":title}
        if year: params["first_air_date_year"]=year
        sr=tmdb_get("/search/tv", params)
        if sr and as_list(sr.get("results")):
            hit=sr["results"][0]
            det=tmdb_get(f"/tv/{hit['id']}", {"append_to_response":"external_ids"})
            if det: show=det
    return show or {}

def enrich_episode(show_tmdb_id: Optional[int], season: Optional[int], number: Optional[int]) -> Dict[str, Any]:
    if not (show_tmdb_id and season is not None and number is not None): return {}
    ep=tmdb_get(f"/tv/{show_tmdb_id}/season/{season}/episode/{number}", {"append_to_response":"external_ids"})
    return ep or {}

def enrich_season_meta(show_tmdb_id: Optional[int], season: Optional[int]) -> Dict[str, Any]:
    if not (show_tmdb_id and season is not None): return {}
    det=tmdb_get(f"/tv/{show_tmdb_id}/season/{season}", {})
    return det or {}

def enrich_movie_by_tmdb_ids(tmdb_id: Optional[int], imdb_id: Optional[str], title: str, year: Optional[int]) -> Dict[str, Any]:
    movie={}
    if tmdb_id:
        data=tmdb_get(f"/movie/{tmdb_id}", {"append_to_response":"external_ids"})
        if data: movie=data
    if not movie and imdb_id:
        data=tmdb_get(f"/find/{imdb_id}", {"external_source":"imdb_id"})
        if data and as_list(data.get("movie_results")):
            hit=data["movie_results"][0]
            det=tmdb_get(f"/movie/{hit['id']}", {"append_to_response":"external_ids"})
            if det: movie=det
    if not movie:
        params={"query":title}
        if year: params["year"]=year
        sr=tmdb_get("/search/movie", params)
        if sr and as_list(sr.get("results")):
            hit=sr["results"][0]
            det=tmdb_get(f"/movie/{hit['id']}", {"append_to_response":"external_ids"})
            if det: movie=det
    return movie or {}

# -----------------------------
# Normalisierung
# -----------------------------
def normalize_movie_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if item.get("type")!="movie" or "movie" not in item: return None
    m=as_dict(item.get("movie")); w=item.get("watched_at")
    return {"type":"movie","history_id":item.get("id"),"title":m.get("title"),
            "year":m.get("year"),"ids":as_dict(m.get("ids")),"watched_on":w,
            "action":item.get("action")}

def normalize_episode_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if item.get("type")!="episode" or "episode" not in item: return None
    e=as_dict(item.get("episode")); s=as_dict(item.get("show")); w=item.get("watched_at")
    return {"type":"episode","history_id":item.get("id"),"show":s.get("title"),
            "year":s.get("year"),"ids":{"show":as_dict(s.get("ids")),"episode":as_dict(e.get("ids"))},
            "season":e.get("season"),"episode":e.get("number"),"title":e.get("title"),
            "watched_on":w,"action":item.get("action")}

# -----------------------------
# Legacy-Mapping (für neue Items)
# -----------------------------
def episode_to_frontend(e: Dict[str, Any]) -> Dict[str, Any]:
    ids_all=as_dict(e.get("ids")); show_ids=as_dict(ids_all.get("show"))
    tmdb_show=as_dict(e.get("tmdb_show")); tmdb_ep=as_dict(e.get("tmdb_episode"))
    season_meta=as_dict(e.get("tmdb_season"))
    # runtime
    ep_runtime=tmdb_ep.get("runtime")
    if ep_runtime is None:
        ert=tmdb_show.get("episode_run_time")
        ep_runtime = (ert[0] if isinstance(ert,list) and ert else (ert if isinstance(ert,int) else None))
    season_total = len(season_meta.get("episodes")) if isinstance(season_meta.get("episodes"), list) else None
    return {k:v for k,v in {
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
        "episode_title": e.get("title") or tmdb_ep.get("name"),
        "episode_title_de": tmdb_ep.get("name"),
        "episode_runtime": ep_runtime,
        "season_total_episodes": season_total,
        "episode_still": img_or_none(tmdb_ep.get("still_path"), "w300"),
    }.items() if v is not None}

def movie_to_frontend(m: Dict[str, Any]) -> Dict[str, Any]:
    ids=as_dict(m.get("ids")); tmdb=as_dict(m.get("tmdb"))
    return {k:v for k,v in {
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
    }.items() if v is not None}

# -----------------------------
# Legacy-Keys (Duplikat-Erkennung)
# -----------------------------
def legacy_ep_key(r: Dict[str, Any]):
    r = r if isinstance(r, dict) else {}
    return ("ep", r.get("show"), r.get("season"), r.get("episode"), r.get("watched_on"))

def legacy_mov_key(r: Dict[str, Any]):
    r = r if isinstance(r, dict) else {}
    preferred_id = r.get("trakt") or r.get("imdb") or r.get("tmdb") or r.get("title")
    return ("mov", preferred_id, r.get("watched_on"))

# -----------------------------
# Fetch History
# -----------------------------
def fetch_trakt_history(start_at: Optional[str], limit: int, pages: int) -> List[Dict[str, Any]]:
    out=[]
    for page in range(1, pages+1):
        params={"limit":limit,"page":page}
        if start_at: params["start_at"]=start_at
        r=trakt_get("/sync/history", params=params)
        batch=r.json()
        if not batch: break
        for raw in batch:
            t=raw.get("type")
            if t=="episode":
                e=as_dict(raw.get("episode")); s=as_dict(raw.get("show"))
                log(f"  raw: EP  hist={raw.get('id')}  {s.get('title')} S{e.get('season')}E{e.get('number')}  watched_at={raw.get('watched_at')}")
            elif t=="movie":
                m=as_dict(raw.get("movie"))
                log(f"  raw: MOV hist={raw.get('id')}  {m.get('title')}({m.get('year')})  watched_at={raw.get('watched_at')}")
        out.extend(batch)
    return out

# -----------------------------
# MAIN
# -----------------------------
def main():
    # Pfad-Debug
    log(f"REPO_ROOT={REPO_ROOT}")
    log(f"OUTPUT_DIR={OUTPUT_DIR}")
    log(f"MOVIES_YAML={MOVIES_YAML}")
    log(f"EPISODES_YAML={EPISODES_YAML}")
    log(f"CURSOR_FILE={CURSOR_FILE}")

    # /users/me (401→Refresh)
    try: _=trakt_get("/users/me").json()
    except Exception as e: log(f"/users/me check: {e}")

    start_at = determine_start_at()
    log(f"Starte ab: {start_at}" if start_at else "Kein Cursor – hole aktuelle History ohne start_at.")

    limit=int(os.environ.get("TRAKT_HISTORY_LIMIT","200"))
    pages=int(os.environ.get("TRAKT_HISTORY_PAGES","5"))
    history=fetch_trakt_history(start_at, limit, pages)
    log(f"Fetched {len(history)} history items von Trakt (start_at={start_at}).")
    if not history:
        log("Keine neuen History-Items. Cursor unverändert.")
        return

    movies_raw, episodes_raw = [], []
    for it in history:
        if it.get("type")=="movie":
            nm=normalize_movie_item(it)
            if nm: movies_raw.append(nm)
        elif it.get("type")=="episode":
            ne=normalize_episode_item(it)
            if ne: episodes_raw.append(ne)

    # Enrichment
    log(f"Enrichment: {len(movies_raw)} Movies, {len(episodes_raw)} Episodes …")
    # Movies
    new_movies_legacy=[]
    for m in movies_raw:
        ids=as_dict(m.get("ids"))
        info=enrich_movie_by_tmdb_ids(ids.get("tmdb"), ids.get("imdb"), m.get("title") or "", m.get("year")) or {}
        m["tmdb"]=info
        new_movies_legacy.append(movie_to_frontend(m))
    # Episodes
    new_eps_legacy=[]
    for e in episodes_raw:
        show_ids=as_dict(as_dict(e.get("ids")).get("show"))
        tmdb_show_id = show_ids.get("tmdb")
        show_det=enrich_show(tmdb_show_id, e.get("show"), e.get("year")) or {}
        ep_det=enrich_episode(show_det.get("id") if show_det else tmdb_show_id, e.get("season"), e.get("episode")) or {}
        season_meta=enrich_season_meta(show_det.get("id") if show_det else tmdb_show_id, e.get("season")) or {}
        e["tmdb_show"]=show_det; e["tmdb_episode"]=ep_det; e["tmdb_season"]=season_meta
        new_eps_legacy.append(episode_to_frontend(e))

    # Bestehende YAMLs (nur zum Duplikat-Check einlesen — Inhalte bleiben unberührt)
    existing_movies = [r for r in yaml_load(MOVIES_YAML) if isinstance(r, dict)]
    existing_eps    = [r for r in yaml_load(EPISODES_YAML) if isinstance(r, dict)]

    mov_keys = { legacy_mov_key(r) for r in existing_movies }
    ep_keys  = { legacy_ep_key(r)  for r in existing_eps }

    # Nur NEUE Einträge anhängen
    to_append_movies = []
    to_append_eps    = []

    for row in new_movies_legacy:
        k = legacy_mov_key(row)
        if k not in mov_keys:
            to_append_movies.append(row)
            mov_keys.add(k)
            log(f"MOV: QUEUE ADD -> {row.get('title')} ({row.get('year')}) @ {row.get('watched_on')} key={k}")
        else:
            log(f"MOV: SKIP (exists) -> {row.get('title')} ({row.get('year')}) @ {row.get('watched_on')} key={k}")

    for row in new_eps_legacy:
        k = legacy_ep_key(row)
        if k not in ep_keys:
            to_append_eps.append(row)
            ep_keys.add(k)
            log(f"EP : QUEUE ADD -> {row.get('show')} S{row.get('season')}E{row.get('episode')} @ {row.get('watched_on')} key={k}")
        else:
            log(f"EP : SKIP (exists) -> {row.get('show')} S{row.get('season')}E{row.get('episode')} @ {row.get('watched_on')} key={k}")

    # Anhängen (append-only) + Backup
    if to_append_movies:
        log("Vor Append (Movies): " + stat_path(MOVIES_YAML))
        append_yaml_items(MOVIES_YAML, to_append_movies)
        log("Nach Append (Movies): " + stat_path(MOVIES_YAML))
    else:
        log("Movies: nichts anzuhängen.")

    if to_append_eps:
        log("Vor Append (Episodes): " + stat_path(EPISODES_YAML))
        append_yaml_items(EPISODES_YAML, to_append_eps)
        log("Nach Append (Episodes): " + stat_path(EPISODES_YAML))
    else:
        log("Episodes: nichts anzuhängen.")

    log(f"Appended: Movies={len(to_append_movies)} | Episodes={len(to_append_eps)}")

    # Cursor fortschreiben: neuestes watched_on – 1s
    newest_ts = None
    for it in (movies_raw + episodes_raw):
        ts = it.get("watched_on")
        if ts and (newest_ts is None or ts > newest_ts):
            newest_ts = ts
    if newest_ts:
        dt = parse_iso(newest_ts)
        cursor_iso = (dt - timedelta(seconds=1)).isoformat().replace("+00:00","Z") if dt else newest_ts
        write_cursor(cursor_iso)
        log(f"Cursor aktualisiert auf: {cursor_iso}")
    else:
        log("Keine neuen watched_at-Zeiten – Cursor unverändert.")

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as http_err:
        sc = http_err.response.status_code if http_err.response is not None else "?"
        log(f"HTTP error: {http_err} (status {sc})"); sys.exit(2)
    except RuntimeError as re:
        log(str(re)); sys.exit(1)
    except Exception as e:
        log(f"Fatal error: {e}"); sys.exit(2)
