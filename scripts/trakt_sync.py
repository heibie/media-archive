#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trakt → YAML Sync (inkrementell, mit Enrichment)
- Holt NUR neue Trakt-History seit dem letzten Datum in watched_movies.yml / watched_episodes.yml
- Movies & Episodes separat (Pagination)
- Enrichment (de-DE) via TMDB inkl. external_ids (IMDB/TVDB)
- Schreibt/aktualisiert watched_movies.yml & watched_episodes.yml
- Optional: rotiert TRAKT_ACCESS_TOKEN/REFRESH_TOKEN in Repo-Secrets (wenn GH_PAT gesetzt ist)

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
  TRAKT_START_AT_ISO        (default: aus YAML bestimmt)
  GH_PAT                    (Personal Access Token classic mit 'repo')
  GITHUB_REPOSITORY         (owner/repo – wird in Actions automatisch gesetzt)
"""

import os
import sys
import json
import time
import base64
import requests
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

try:
    import yaml
except ImportError:
    print("[trakt-sync] pyyaml fehlt. Bitte pyyaml installieren.", file=sys.stderr)
    sys.exit(2)

# PyNaCl für GH Secret Rotation (optional)
try:
    from nacl import encoding, public
    HAVE_NACL = True
except Exception:
    HAVE_NACL = False

# -----------------------------
# Konfiguration & Pfade
# -----------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "_data")).resolve()
if not OUTPUT_DIR.is_absolute():
    OUTPUT_DIR = REPO_ROOT / OUTPUT_DIR
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MOVIES_YAML   = OUTPUT_DIR / "watched_movies.yml"
EPISODES_YAML = OUTPUT_DIR / "watched_episodes.yml"
CURSOR_FILE   = REPO_ROOT / ".trakt_cursor"       # ISO-String (zuletzt verarbeitet)
TOKENS_OUT    = REPO_ROOT / ".trakt_tokens.json"  # Debug/optional

# -----------------------------
# Trakt / TMDB Basics
# -----------------------------
TRAKT_BASE = "https://api.trakt.tv"
TMDB_BASE  = "https://api.themoviedb.org/3"
USER_AGENT = "trakt-yaml-sync/1.2 (+github actions)"

TRAKT_CLIENT_ID     = os.environ.get("TRAKT_CLIENT_ID", "")
TRAKT_CLIENT_SECRET = os.environ.get("TRAKT_CLIENT_SECRET", "")
TRAKT_ACCESS_TOKEN  = os.environ.get("TRAKT_ACCESS_TOKEN", "")
TRAKT_REFRESH_TOKEN = os.environ.get("TRAKT_REFRESH_TOKEN", "")
TRAKT_USERNAME      = os.environ.get("TRAKT_USERNAME", "")

TMDB_API_KEY        = os.environ.get("TMDB_API_KEY", "")

if not (TRAKT_CLIENT_ID and TRAKT_CLIENT_SECRET and TRAKT_REFRESH_TOKEN and TMDB_API_KEY):
    print("[trakt-sync] ERROR: Missing required env vars (need TRAKT_CLIENT_ID/SECRET/REFRESH_TOKEN + TMDB_API_KEY).", file=sys.stderr)
    sys.exit(1)

TRAKT_HEADERS = {
    "Content-Type": "application/json",
    "trakt-api-version": "2",
    "trakt-api-key": TRAKT_CLIENT_ID or "",
    "User-Agent": USER_AGENT,
}
if TRAKT_ACCESS_TOKEN:
    TRAKT_HEADERS["Authorization"] = f"Bearer {TRAKT_ACCESS_TOKEN}"

SESSION = requests.Session()
SESSION.headers.update(TRAKT_HEADERS)

# GitHub Secrets Rotation
GH_PAT            = os.environ.get("GH_PAT", "")
GITHUB_REPOSITORY = os.environ.get("GITHUB_REPOSITORY", "")

# -----------------------------
# Helpers
# -----------------------------
def log(msg: str):
    print(f"[trakt-sync] {msg}")

def iso_now_z() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def parse_iso(s: str) -> Optional[datetime]:
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None

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

def save_tokens_file(access_token: str, refresh_token: str):
    try:
        TOKENS_OUT.write_text(
            json.dumps({"access_token": access_token, "refresh_token": refresh_token}, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
    except Exception:
        pass

# -----------------------------
# Trakt: OAuth Refresh
# -----------------------------
def trakt_refresh_tokens() -> Tuple[bool, Optional[str], Optional[str]]:
    """Refresh Trakt tokens. Returns (ok, new_access, new_refresh)."""
    payload = {
        "refresh_token": TRAKT_REFRESH_TOKEN,
        "client_id": TRAKT_CLIENT_ID,
        "client_secret": TRAKT_CLIENT_SECRET,
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
        "grant_type": "refresh_token",
    }
    try:
        r = requests.post(
            f"{TRAKT_BASE}/oauth/token", json=payload,
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
            timeout=30
        )
    except requests.RequestException as e:
        log(f"Token-Refresh exception: {e}")
        return False, None, None

    if r.status_code != 200:
        log(f"Token-Refresh failed: HTTP {r.status_code} {r.reason}")
        try:
            log(f"Body: {r.text[:500]}")
        except Exception:
            pass
        return False, None, None

    tok = r.json()
    new_access  = tok.get("access_token")
    new_refresh = tok.get("refresh_token")
    if not (new_access and new_refresh):
        log("Token-Refresh: Antwort ohne Tokens.")
        return False, None, None

    # Update global header
    SESSION.headers["Authorization"] = f"Bearer {new_access}"
    # Auch global für evtl. weitere Funktionen
    global TRAKT_ACCESS_TOKEN, TRAKT_REFRESH_TOKEN
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
    """Sucht Film in TMDB, lädt Details + external_ids."""
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
        # Fallback: Suche per Titel/Jahr
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
    """Lädt Episoden-Details (inkl. external_ids) für eine bekannte TMDB Show-ID."""
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
# YAML / History Utility
# -----------------------------
def latest_watched_iso_from_yaml() -> Optional[str]:
    """Liest den jüngsten 'watched_on' (ISO) aus beiden YAMLs und gibt ihn zurück."""
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
        iso = max_dt.isoformat().replace("+00:00", "Z")
        return iso
    return None

def determine_start_at() -> Optional[str]:
    # 1) env override
    env = os.environ.get("TRAKT_START_AT_ISO", "").strip()
    if env:
        return env
    # 2) cursor file
    if CURSOR_FILE.exists():
        try:
            val = CURSOR_FILE.read_text(encoding="utf-8").strip()
            if val:
                return val
        except Exception:
            pass
    # 3) from YAMLs
    return latest_watched_iso_from_yaml()

def sorted_unique(items: List[Dict[str, Any]], key_field: str) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for it in items:
        k = it.get(key_field)
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out

# -----------------------------
# GH Secrets Rotation (optional)
# -----------------------------
def gh_get_public_key(repo: str, token: str) -> Tuple[str, str]:
    """Returns (key, key_id) for repo secrets."""
    url = f"https://api.github.com/repos/{repo}/actions/secrets/public-key"
    r = requests.get(url, headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"})
    r.raise_for_status()
    data = r.json()
    return data["key"], data["key_id"]

def gh_encrypt(key_b64: str, secret_value: str) -> str:
    """Encrypts secret_value using the repo's public key (libsodium)."""
    if not HAVE_NACL:
        raise RuntimeError("PyNaCl nicht installiert. Kann Secrets nicht verschlüsseln.")
    public_key = public.PublicKey(key_b64, encoding.Base64Encoder())
    sealed_box = public.SealedBox(public_key)
    encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
    return base64.b64encode(encrypted).decode("utf-8")

def gh_put_secret(repo: str, token: str, name: str, value: str):
    pub_key, key_id = gh_get_public_key(repo, token)
    enc = gh_encrypt(pub_key, value)
    url = f"https://api.github.com/repos/{repo}/actions/secrets/{name}"
    payload = {"encrypted_value": enc, "key_id": key_id}
    r = requests.put(url, headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}, json=payload)
    r.raise_for_status()

def maybe_rotate_trakt_secrets(new_access: Optional[str], new_refresh: Optional[str]):
    """Rotiert Tokens in Repo-Secrets (falls alles vorhanden)."""
    if not new_refresh or not new_access:
        log("Secret-Rotation übersprungen: neue Tokens fehlen.")
        return
    if not GH_PAT:
        log("Secret-Rotation übersprungen: GH_PAT fehlt.")
        return
    if not GITHUB_REPOSITORY:
        log("Secret-Rotation übersprungen: GITHUB_REPOSITORY fehlt.")
        return
    if not HAVE_NACL:
        log("Secret-Rotation übersprungen: PyNaCl nicht installiert.")
        return
    try:
        log("Rotiere TRAKT_REFRESH_TOKEN und TRAKT_ACCESS_TOKEN in Repo-Secrets …")
        gh_put_secret(GITHUB_REPOSITORY, GH_PAT, "TRAKT_REFRESH_TOKEN", new_refresh)
        gh_put_secret(GITHUB_REPOSITORY, GH_PAT, "TRAKT_ACCESS_TOKEN", new_access)
        log("Secrets aktualisiert.")
    except Exception as e:
        log(f"Secret-Rotation fehlgeschlagen: {e}")

# -----------------------------
# Hauptlogik: Fetch + Enrich
# -----------------------------
def fetch_trakt_history(start_at: Optional[str], limit: int, pages: int) -> List[Dict[str, Any]]:
    """Holt History (movie/episode) ab start_at, paginiert."""
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

def normalize_movie_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if item.get("type") != "movie" or "movie" not in item:
        return None
    m = item["movie"]
    w = item.get("watched_at")
    out = {
        "type": "movie",
        "title": m.get("title"),
        "year": m.get("year"),
        "ids": m.get("ids", {}),
        "watched_on": w,
        "action": item.get("action"),  # watch/scrobble
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
        "show": s.get("title"),
        "year": s.get("year"),
        "ids": {
            "show": s.get("ids", {}),
            "episode": e.get("ids", {}),
        },
        "season": e.get("season"),
        "episode": e.get("number"),
        "title": e.get("title"),
        "watched_on": w,
        "action": item.get("action"),
    }
    return out

def enrich_movie(record: Dict[str, Any]) -> Dict[str, Any]:
    ids = record.get("ids", {}) or {}
    tmdb_id = ids.get("tmdb")
    imdb_id = ids.get("imdb")
    title   = record.get("title") or ""
    year    = record.get("year")
    tmdb = enrich_movie_by_tmdb_ids(tmdb_id, imdb_id, title, year)

    if tmdb:
        record["tmdb"] = {
            "id": tmdb.get("id"),
            "title": tmdb.get("title") or tmdb.get("original_title"),
            "original_title": tmdb.get("original_title"),
            "overview": tmdb.get("overview"),
            "poster_path": tmdb.get("poster_path"),
            "backdrop_path": tmdb.get("backdrop_path"),
            "release_date": tmdb.get("release_date"),
            "genres": [g.get("name") for g in (tmdb.get("genres") or []) if g.get("name")],
            "vote_average": tmdb.get("vote_average"),
            "runtime": tmdb.get("runtime"),
            "external_ids": tmdb.get("external_ids", {}),
        }
    return record

def enrich_episode(record: Dict[str, Any]) -> Dict[str, Any]:
    ids = record.get("ids", {}) or {}
    show_ids = (ids.get("show") or {})
    show_tmdb_id = show_ids.get("tmdb")
    show_title   = record.get("show")
    show_year    = record.get("year")

    # Show-Details (für Poster etc.)
    show_det = enrich_show_by_tmdb_id(show_tmdb_id, show_title, show_year)
    if show_det:
        record.setdefault("tmdb_show", {})
        record["tmdb_show"] = {
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

    # Episode-Details
    ep_season = record.get("season")
    ep_number = record.get("episode")
    ep_det = enrich_episode_by_tmdb_ids(show_det.get("id") if show_det else show_tmdb_id, ep_season, ep_number)
    if ep_det:
        record.setdefault("tmdb_episode", {})
        record["tmdb_episode"] = {
            "id": ep_det.get("id"),
            "name": ep_det.get("name"),
            "overview": ep_det.get("overview"),
            "still_path": ep_det.get("still_path"),
            "air_date": ep_det.get("air_date"),
            "vote_average": ep_det.get("vote_average"),
            "external_ids": ep_det.get("external_ids", {}),
        }
    return record

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
    if k: return k
    # fallback
    return (r.get("show"), r.get("season"), r.get("episode"), r.get("watched_on"))

# -----------------------------
# MAIN
# -----------------------------
def main():
    # 0) Sanity /users/me → triggert ggf. Refresh
    try:
        _ = trakt_get("/users/me").json()
    except Exception as e:
        log(f"/users/me check: {e}")

    start_at = determine_start_at()
    if start_at:
        log(f"Starte inkrementelles Update ab: {start_at}")
    else:
        log("Kein Cursor gefunden – hole aktuelle History (ohne start_at).")

    limit = int(os.environ.get("TRAKT_HISTORY_LIMIT", "200"))
    pages = int(os.environ.get("TRAKT_HISTORY_PAGES", "5"))

    # 1) History holen
    history = fetch_trakt_history(start_at, limit, pages)
    if not history:
        log("Keine neuen History-Items von Trakt.")
        # cursor trotzdem vorsichtig nach vorn setzen (jetzt)
        write_cursor(iso_now_z())
        return

    # 2) Normalisieren & trennen
    movies_raw: List[Dict[str, Any]] = []
    episodes_raw: List[Dict[str, Any]] = []
    for it in history:
        t = it.get("type")
        if t == "movie":
            nm = normalize_movie_item(it)
            if nm:
                movies_raw.append(nm)
        elif t == "episode":
            ne = normalize_episode_item(it)
            if ne:
                episodes_raw.append(ne)

    if not movies_raw and not episodes_raw:
        log("History enthielt keine Movie/Episode Items.")
        write_cursor(iso_now_z())
        return

    # 3) Enrichment
    log(f"Enrichment: {len(movies_raw)} Movies, {len(episodes_raw)} Episodes …")
    enr_movies: List[Dict[str, Any]] = []
    for m in movies_raw:
        try:
            enr_movies.append(enrich_movie(m))
        except Exception as e:
            log(f"Movie-Enrichment Fehler ({m.get('title')}): {e}")
            enr_movies.append(m)

    enr_eps: List[Dict[str, Any]] = []
    for e in episodes_raw:
        try:
            enr_eps.append(enrich_episode(e))
        except Exception as ex:
            log(f"Episode-Enrichment Fehler ({e.get('show')} S{e.get('season')}E{e.get('episode')}): {ex}")
            enr_eps.append(e)

    # 4) Bestehende YAMLs laden, mergen, sortieren
    movies_all = yaml_load(MOVIES_YAML)
    episodes_all = yaml_load(EPISODES_YAML)

    movies_all = add_or_update(movies_all, enr_movies, movie_key)
    episodes_all = add_or_update(episodes_all, enr_eps, episode_key)

    movies_all.sort(key=lambda r: (r.get("watched_on") or ""), reverse=True)
    episodes_all.sort(key=lambda r: (r.get("watched_on") or "", r.get("season") or 0, r.get("episode") or 0), reverse=True)

    # 5) Schreiben
    yaml_dump(MOVIES_YAML, movies_all)
    yaml_dump(EPISODES_YAML, episodes_all)
    log(f"Aktualisiert: {MOVIES_YAML}, {EPISODES_YAML}")

    # 6) Cursor fortschreiben (jetzt Zeitpunkt)
    write_cursor(iso_now_z())

    # 7) Falls beim Lauf ein Refresh stattfand, Tokens in Secrets rotieren
    try:
        maybe_rotate_trakt_secrets(TRAKT_ACCESS_TOKEN, TRAKT_REFRESH_TOKEN)
    except Exception as e:
        log(f"Secret-Rotation übersprungen/fehlgeschlagen: {e}")

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as http_err:
        # Wenn Refresh nötig/fehlgeschlagen ist, hier klar abbrechen
        try:
            sc = http_err.response.status_code if http_err.response is not None else "?"
        except Exception:
            sc = "?"
        log(f"HTTP error: {http_err} (status {sc})")
        sys.exit(2)
    except RuntimeError as re:
        log(str(re))
        sys.exit(1)
    except Exception as e:
        log(f"Fatal error: {e}")
        sys.exit(2)
