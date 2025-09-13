#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trakt → YAML Sync (inkrementell)
- Holt NUR neue Trakt-History seit dem letzten Datum in watched_movies.yml / watched_episodes.yml
- Movies & Episodes separat (Pagination)
- Enrichment (de-DE) via TMDB inkl. external_ids (IMDB/TVDB)
- Schreibt/aktualisiert watched_movies.yml & watched_episodes.yml
- Optional: rotiert TRAKT_ACCESS_TOKEN/REFRESH_TOKEN in Repo-Secrets (wenn GH_PAT gesetzt ist)
"""
import os
import sys
import requests
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

try:
    from yaml import safe_load, safe_dump
except Exception:
    print("Bitte PyYAML installieren (pip install PyYAML).", file=sys.stderr)
    raise

# ==== ENV ====
TRAKT_CLIENT_ID     = os.environ.get("TRAKT_CLIENT_ID")
TRAKT_CLIENT_SECRET = os.environ.get("TRAKT_CLIENT_SECRET")
TRAKT_ACCESS_TOKEN  = os.environ.get("TRAKT_ACCESS_TOKEN")
TRAKT_REFRESH_TOKEN = os.environ.get("TRAKT_REFRESH_TOKEN")
TRAKT_USERNAME      = os.environ.get("TRAKT_USERNAME")
TMDB_API_KEY        = os.environ.get("TMDB_API_KEY")

OUTPUT_DIR          = os.environ.get("OUTPUT_DIR", ".").rstrip("/")
MOVIES_YAML         = os.path.join(OUTPUT_DIR, "watched_movies.yml")
EPISODES_YAML       = os.path.join(OUTPUT_DIR, "watched_episodes.yml")

GH_PAT              = os.environ.get("GH_PAT")
GITHUB_REPOSITORY   = os.environ.get("GITHUB_REPOSITORY")  # z.B. "heibie/media-archive"

# ==== API Basics ====
TRAKT_BASE = "https://api.trakt.tv"
TMDB_BASE  = "https://api.themoviedb.org/3"
TMDB_IMG   = {
    "poster_w500": "https://image.tmdb.org/t/p/w500",
    "backdrop_w780": "https://image.tmdb.org/t/p/w780",
    "still_w300": "https://image.tmdb.org/t/p/w300",
}

TRAKT_HEADERS = {
    "Content-Type": "application/json",
    "trakt-api-version": "2",
    "trakt-api-key": TRAKT_CLIENT_ID or "",
}

SESSION = requests.Session()
SESSION.headers.update(TRAKT_HEADERS)

# ==== Utils ====
def log(msg: str):
    print(f"[trakt-sync] {msg}")

def ensure_env():
    missing = [k for k,v in [
        ("TRAKT_CLIENT_ID",TRAKT_CLIENT_ID),
        ("TRAKT_CLIENT_SECRET",TRAKT_CLIENT_SECRET),
        ("TRAKT_REFRESH_TOKEN",TRAKT_REFRESH_TOKEN),
        ("TRAKT_USERNAME",TRAKT_USERNAME),
        ("TMDB_API_KEY",TMDB_API_KEY),
    ] if not v]
    if missing:
        raise RuntimeError(f"Fehlende Variablen: {', '.join(missing)}")

def yaml_load(path: str) -> List[Dict[str,Any]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = safe_load(f) or []
        if not isinstance(data, list):
            raise RuntimeError(f"{path} muss eine Liste sein.")
        return data

def yaml_dump(path: str, data: List[Dict[str,Any]]):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        safe_dump(
            data,
            f,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
            width=1000,
        )

def parse_date(d: str) -> datetime:
    # Eingabe 'YYYY-MM-DD'
    return datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)

def max_watched_date_str(*lists: List[Dict[str,Any]]) -> str:
    """Ermittelt jüngstes watched_on (YYYY-MM-DD) aus übergebenen YAML-Listen."""
    latest = None
    for data in lists:
        for item in data:
            d = item.get("watched_on")
            if not d:
                continue
            try:
                dt = parse_date(d)
            except Exception:
                continue
            if latest is None or dt > latest:
                latest = dt
    # Fallback weit in die Vergangenheit
    return (latest or datetime(1970,1,1,tzinfo=timezone.utc)).date().isoformat()

def to_iso_start_of_day_utc(date_str: str) -> str:
    # "YYYY-MM-DD" → "YYYY-MM-DDT00:00:00.000Z"
    return f"{date_str}T00:00:00.000Z"

def dt_to_date_str(watched_at_iso: str) -> str:
    # Trakt: "2025-09-06T21:35:00.000Z" → "YYYY-MM-DD"
    dt = datetime.fromisoformat(watched_at_iso.replace("Z", "+00:00"))
    return dt.date().isoformat()

def with_auth(headers: Dict[str,str]) -> Dict[str,str]:
    if not TRAKT_ACCESS_TOKEN:
        return headers
    h = dict(headers)
    h["Authorization"] = f"Bearer {TRAKT_ACCESS_TOKEN}"
    return h

# ==== Trakt OAuth Refresh ====
def trakt_refresh() -> Optional[Dict[str,str]]:
    url = f"{TRAKT_BASE}/oauth/token"
    payload = {
        "refresh_token": TRAKT_REFRESH_TOKEN,
        "client_id": TRAKT_CLIENT_ID,
        "client_secret": TRAKT_CLIENT_SECRET,
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
        "grant_type": "refresh_token",
    }
    r = SESSION.post(url, json=payload)
    if r.status_code != 200:
        log(f"Token-Refresh fehlgeschlagen: {r.status_code} {r.text}")
        return None
    tok = r.json()
    return {"access_token": tok["access_token"], "refresh_token": tok["refresh_token"]}

# ==== Trakt History (seit Startdatum, paginiert) ====
def trakt_history_since(kind: str, start_at_iso: str, per_page=50) -> List[Dict[str,Any]]:
    """
    kind: 'movies' oder 'episodes'
    nutzt /sync/history/{kind}?start_at=...
    """
    assert kind in ("movies", "episodes")
    items: List[Dict[str,Any]] = []
    page = 1
    while True:
        params = {
            "start_at": start_at_iso,
            "page": page,
            "limit": per_page,
            "extended": "full",
        }
        url = f"{TRAKT_BASE}/sync/history/{kind}"
        r = SESSION.get(url, params=params, headers=with_auth({}))
        if r.status_code == 401:
            log("401 von Trakt → refreshe Token …")
            fresh = trakt_refresh()
            if not fresh:
                raise RuntimeError("Token-Refresh fehlgeschlagen.")
            global TRAKT_ACCESS_TOKEN, TRAKT_REFRESH_TOKEN
            TRAKT_ACCESS_TOKEN = fresh["access_token"]
            TRAKT_REFRESH_TOKEN = fresh["refresh_token"]
            r = SESSION.get(url, params=params, headers=with_auth({}))
        r.raise_for_status()
        batch = r.json() or []
        if not batch:
            break
        items.extend(batch)
        # Wenn weniger als per_page kamen, ist Schluss
        if len(batch) < per_page:
            break
        page += 1
    return items

# ==== TMDB Helpers (de-DE + external_ids) ====
def tmdb_get(path: str, params: Dict[str,Any] = None) -> Dict[str,Any]:
    params = params or {}
    params["api_key"] = TMDB_API_KEY
    params["language"] = "de-DE"
    url = f"{TMDB_BASE}{path}"
    rr = SESSION.get(url, params=params)
    if rr.status_code == 404:
        return {}
    rr.raise_for_status()
    return rr.json()

def tmdb_get_no_lang(path: str) -> Dict[str,Any]:
    # für /external_ids ohne language-Param
    url = f"{TMDB_BASE}{path}"
    rr = SESSION.get(url, params={"api_key": TMDB_API_KEY})
    if rr.status_code == 404:
        return {}
    rr.raise_for_status()
    return rr.json()

def tmdb_movie_info(tmdb_id: int) -> Dict[str,Any]:
    if not tmdb_id:
        return {}
    data = tmdb_get(f"/movie/{tmdb_id}")
    out = {}
    if data:
        out["title_de"] = data.get("title") or data.get("original_title")
        out["overview_de"] = data.get("overview")
        if data.get("poster_path"):
            out["poster"] = f'{TMDB_IMG["poster_w500"]}{data["poster_path"]}'
        if data.get("backdrop_path"):
            out["backdrop"] = f'{TMDB_IMG["backdrop_w780"]}{data["backdrop_path"]}'
        out["runtime"] = data.get("runtime")
    # external_ids für imdb
    ext = tmdb_get_no_lang(f"/movie/{tmdb_id}/external_ids")
    if ext:
        if ext.get("imdb_id"):
            out["imdb"] = ext["imdb_id"]
        # Für Filme hat TMDB kein tvdb
    return out

def tmdb_show_info_with_external_ids(tmdb_id: int) -> Dict[str,Any]:
    if not tmdb_id:
        return {}
    data = tmdb_get(f"/tv/{tmdb_id}")
    out = {}
    if data:
        out["show_title_de"] = data.get("name") or data.get("original_name")
        if data.get("poster_path"):
            out["show_poster"] = f'{TMDB_IMG["poster_w500"]}{data["poster_path"]}'
        if data.get("backdrop_path"):
            out["show_backdrop"] = f'{TMDB_IMG["backdrop_w780"]}{data["backdrop_path"]}'
        out["show_total_episodes"] = data.get("number_of_episodes")
        out["show_episode_run_time"] = (data.get("episode_run_time") or [None])[0]
    # external_ids für imdb & tvdb (Show-Ebene)
    ext = tmdb_get_no_lang(f"/tv/{tmdb_id}/external_ids")
    if ext:
        if ext.get("imdb_id"):
            out["imdb"] = ext["imdb_id"]
        if ext.get("tvdb_id") is not None:
            out["tvdb"] = ext["tvdb_id"]
    return out

def tmdb_episode_info(tmdb_id: int, season: int, episode: int) -> Dict[str,Any]:
    if not tmdb_id or season is None or episode is None:
        return {}
    data = tmdb_get(f"/tv/{tmdb_id}/season/{season}/episode/{episode}")
    out = {}
    if data:
        out["episode_title_de"] = data.get("name")
        out["episode_runtime"] = data.get("runtime")
        if data.get("still_path"):
            out["episode_still"] = f'{TMDB_IMG["still_w300"]}{data["still_path"]}'
    # Season-Info für season_total_episodes
    sdata = tmdb_get(f"/tv/{tmdb_id}/season/{season}")
    if sdata and isinstance(sdata.get("episodes"), list):
        out["season_total_episodes"] = len(sdata["episodes"])
    return out

# ==== Optional: GitHub Secrets Rotation ====
def gh_get_repo_public_key(repo: str, token: str) -> Dict[str,str]:
    url = f"https://api.github.com/repos/{repo}/actions/secrets/public-key"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    r.raise_for_status()
    return r.json()  # {key_id, key}

def gh_put_secret(repo: str, token: str, name: str, value: str):
    try:
        from nacl import encoding, public
    except Exception:
        raise RuntimeError("Bitte PyNaCl installieren (pip install pynacl) für Secret-Rotation.")
    pk = gh_get_repo_public_key(repo, token)
    public_key = pk["key"]
    key_id = pk["key_id"]

    sealed_box = public.SealedBox(public.PublicKey(public_key.encode("utf-8"), encoding.Base64Encoder()))
    encrypted = sealed_box.encrypt(value.encode("utf-8"), encoder=encoding.Base64Encoder()).decode("utf-8")

    url = f"https://api.github.com/repos/{repo}/actions/secrets/{name}"
    payload = {"encrypted_value": encrypted, "key_id": key_id}
    rr = requests.put(url, headers={"Authorization": f"Bearer {token}"}, json=payload)
    rr.raise_for_status()

def maybe_rotate_trakt_secrets():
    if not (GH_PAT and GITHUB_REPOSITORY and TRAKT_ACCESS_TOKEN and TRAKT_REFRESH_TOKEN):
        return
    log("Rotiere TRAKT_ACCESS_TOKEN und TRAKT_REFRESH_TOKEN in Repo-Secrets …")
    gh_put_secret(GITHUB_REPOSITORY, GH_PAT, "TRAKT_ACCESS_TOKEN", TRAKT_ACCESS_TOKEN)
    gh_put_secret(GITHUB_REPOSITORY, GH_PAT, "TRAKT_REFRESH_TOKEN", TRAKT_REFRESH_TOKEN)

# ==== Merge/Dedup Keys ====
def movie_key(entry: Dict[str,Any]) -> str:
    tmdb = entry.get("tmdb") or ""
    trakt = entry.get("trakt") or ""
    return f"{tmdb or trakt}:{entry.get('watched_on')}"

def episode_key(entry: Dict[str,Any]) -> str:
    tmdb = entry.get("tmdb") or ""
    s = entry.get("season")
    e = entry.get("episode")
    return f"{tmdb}-S{s}E{e}:{entry.get('watched_on')}"

def upsert(items: List[Dict[str,Any]], new_item: Dict[str,Any], key_fn) -> bool:
    new_k = key_fn(new_item)
    for old in items:
        if key_fn(old) == new_k:
            return False
    items.append(new_item)
    return True

# ==== Record Builder ====
def build_movie_record(h: Dict[str,Any]) -> Dict[str,Any]:
    mv = h["movie"]
    ids = mv.get("ids", {})
    tmdb_id = ids.get("tmdb")
    rec = {
        "title": mv.get("title"),
        "year": mv.get("year"),
        "imdb": ids.get("imdb"),   # ggf. via TMDB ergänzen
        "tmdb": tmdb_id,
        "trakt": ids.get("trakt"),
        "slug": ids.get("slug"),
        "plays": 1,
        "watched_on": dt_to_date_str(h["watched_at"]),
        "source": "trakt",
    }
    de = tmdb_movie_info(tmdb_id) if tmdb_id else {}
    # bevorzugt TMDB imdb, falls in Trakt leer
    if not rec.get("imdb") and de.get("imdb"):
        rec["imdb"] = de["imdb"]
    for k in ["poster", "backdrop", "runtime", "title_de", "overview_de"]:
        if de.get(k) is not None:
            rec[k] = de[k]
    return rec

def build_episode_record(h: Dict[str,Any]) -> Dict[str,Any]:
    ep = h["episode"]
    show = h["show"]
    s = ep.get("season")
    e = ep.get("number")
    show_ids = show.get("ids", {})
    tmdb_id = show_ids.get("tmdb")

    rec = {
        "show": show.get("title"),
        "year": show.get("year"),
        "season": s,
        "episode": e,
        "plays": 1,
        "watched_on": dt_to_date_str(h["watched_at"]),
        "trakt_show": show_ids.get("trakt"),
        "tvdb": show_ids.get("tvdb"),   # ggf. via TMDB ergänzen
        "imdb": show_ids.get("imdb"),   # ggf. via TMDB ergänzen
        "tmdb": tmdb_id,
        "slug": show_ids.get("slug"),
        "source": "trakt",
    }
    show_de = tmdb_show_info_with_external_ids(tmdb_id) if tmdb_id else {}
    # falls tvdb/imdb auf Show-Ebene fehlen → via TMDB external_ids setzen
    if not rec.get("imdb") and show_de.get("imdb"):
        rec["imdb"] = show_de["imdb"]
    if rec.get("tvdb") is None and (show_de.get("tvdb") is not None):
        rec["tvdb"] = show_de["tvdb"]

    for k in ["show_title_de", "show_poster", "show_backdrop", "show_total_episodes", "show_episode_run_time"]:
        if show_de.get(k) is not None:
            rec[k] = show_de[k]

    # Episode-spezifisch
    if ep.get("title"):
        rec["episode_title"] = ep["title"]
    ep_de = tmdb_episode_info(tmdb_id, s, e) if tmdb_id else {}
    for k in ["episode_title_de", "episode_runtime", "season_total_episodes", "episode_still"]:
        if ep_de.get(k) is not None:
            rec[k] = ep_de[k]
    return rec

# ==== Main ====
def main():
    ensure_env()

    # Bestehende YAMLs laden
    movies = yaml_load(MOVIES_YAML)
    episodes = yaml_load(EPISODES_YAML)

    # jüngstes Datum über beide Dateien ermitteln
    since_date = max_watched_date_str(movies, episodes)
    since_iso = to_iso_start_of_day_utc(since_date)
    log(f"Suche neue History seit {since_iso} …")

    # Inkrementell laden
    new_movies   = trakt_history_since("movies", since_iso)
    new_episodes = trakt_history_since("episodes", since_iso)
    log(f"Neu (raw): movies={len(new_movies)}, episodes={len(new_episodes)}")

    changed = False

    for h in new_movies:
        if "movie" in h:
            rec = build_movie_record(h)
            if upsert(movies, rec, movie_key):
                changed = True

    for h in new_episodes:
        if "episode" in h and "show" in h:
            rec = build_episode_record(h)
            if upsert(episodes, rec, episode_key):
                changed = True

    if changed:
        # Sortierung: neueste zuerst
        movies.sort(key=lambda x: (x.get("watched_on") or "", x.get("title") or ""), reverse=True)
        episodes.sort(key=lambda x: (x.get("watched_on") or "", x.get("show") or "", x.get("season") or 0, x.get("episode") or 0), reverse=True)
        yaml_dump(MOVIES_YAML, movies)
        yaml_dump(EPISODES_YAML, episodes)
        log(f"Aktualisiert: {MOVIES_YAML}, {EPISODES_YAML}")
    else:
        log("Keine neuen Einträge gefunden.")

    # neue Tokens (falls refresht) in Secrets rotieren
    try:
        maybe_rotate_trakt_secrets()
    except Exception as e:
        log(f"Secret-Rotation übersprungen/fehlgeschlagen: {e}")

if __name__ == "__main__":
    main()
