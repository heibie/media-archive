#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

import requests
import yaml

# --- Pfade ---
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent
DATA_DIR   = REPO_ROOT / "_data"
DATA_DIR.mkdir(exist_ok=True)

MOVIES_YAML = DATA_DIR / "watched_movies.yml"
EPISODES_YAML = DATA_DIR / "watched_episodes.yml"

CURSOR_FILE = REPO_ROOT / ".trakt_cursor"         # ISO-String
TOKENS_OUT  = REPO_ROOT / ".trakt_tokens.json"    # schreibt neue Tokens für die Secret-Rotation

# --- Trakt ---
TRAKT_BASE = "https://api.trakt.tv"
USER_AGENT = "trakt-yaml-sync/1.1 (+github actions)"

CLIENT_ID     = os.environ.get("TRAKT_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("TRAKT_CLIENT_SECRET", "")
ACCESS_TOKEN  = os.environ.get("TRAKT_ACCESS_TOKEN", "")
REFRESH_TOKEN = os.environ.get("TRAKT_REFRESH_TOKEN", "")

if not CLIENT_ID or not CLIENT_SECRET or not REFRESH_TOKEN:
    print("ERROR: Missing required Trakt credentials (CLIENT_ID/CLIENT_SECRET/REFRESH_TOKEN).", file=sys.stderr)
    sys.exit(1)

TRAKT_HEADERS = {
    "Content-Type": "application/json",
    "trakt-api-version": "2",
    "trakt-api-key": CLIENT_ID,
    "User-Agent": USER_AGENT,
}
if ACCESS_TOKEN:
    TRAKT_HEADERS["Authorization"] = f"Bearer {ACCESS_TOKEN}"


def read_cursor() -> Optional[str]:
    v = os.environ.get("TRAKT_START_AT_ISO", "").strip()
    if v:
        return v
    if CURSOR_FILE.exists():
        return CURSOR_FILE.read_text(encoding="utf-8").strip() or None
    return None


def write_cursor(iso_timestamp: str) -> None:
    CURSOR_FILE.write_text(iso_timestamp, encoding="utf-8")


def save_tokens(access_token: str, refresh_token: str) -> None:
    TOKENS_OUT.write_text(
        json.dumps({"access_token": access_token, "refresh_token": refresh_token}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def refresh_tokens() -> bool:
    """Versucht Token-Refresh. Liefert True bei Erfolg, False bei Fehlschlag (mit Debug-Ausgabe)."""
    global ACCESS_TOKEN, REFRESH_TOKEN, TRAKT_HEADERS
    payload = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
        "grant_type": "refresh_token",
    }
    try:
        r = requests.post(f"{TRAKT_BASE}/oauth/token", json=payload,
                          headers={"Content-Type": "application/json", "User-Agent": USER_AGENT}, timeout=30)
        if r.status_code != 200:
            # Debug-Ausgabe ohne Secrets
            print(f"[trakt-sync] Token-Refresh failed: HTTP {r.status_code} {r.reason}", file=sys.stderr)
            try:
                print(f"[trakt-sync] Body: {r.text[:500]}", file=sys.stderr)
            except Exception:
                pass
            return False
        tok = r.json()
        ACCESS_TOKEN  = tok["access_token"]
        REFRESH_TOKEN = tok["refresh_token"]
        TRAKT_HEADERS["Authorization"] = f"Bearer {ACCESS_TOKEN}"
        print("[trakt-sync] Refreshed access token.")
        save_tokens(ACCESS_TOKEN, REFRESH_TOKEN)
        return True
    except requests.RequestException as e:
        print(f"[trakt-sync] Token-Refresh exception: {e}", file=sys.stderr)
        return False


def trakt_get(path: str, params: Optional[Dict[str, Any]] = None, retry_on_401: bool = True):
    url = f"{TRAKT_BASE}{path}"
    r = requests.get(url, headers=TRAKT_HEADERS, params=params or {}, timeout=45)
    if r.status_code == 401 and retry_on_401:
        print("[trakt-sync] 401 from Trakt. Attempting token refresh…")
        if refresh_tokens():
            r = requests.get(url, headers=TRAKT_HEADERS, params=params or {}, timeout=45)
        else:
            # Hier NICHT sofort crashen; Caller kann entscheiden
            raise RuntimeError("Token-Refresh fehlgeschlagen (HTTP 401).")
    r.raise_for_status()
    return r


def load_yaml(p: Path):
    return yaml.safe_load(p.read_text(encoding="utf-8")) if p.exists() else []


def dump_yaml(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def normalize_item(it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    t = it.get("type")
    watched_at = it.get("watched_at")
    if not t or not watched_at:
        return None
    if t == "movie" and "movie" in it:
        m = it["movie"]
        return {
            "title": m.get("title"),
            "year": m.get("year"),
            "ids": m.get("ids", {}),
            "watched_on": watched_at,
            "type": "movie",
        }
    if t == "episode" and "episode" in it:
        e = it["episode"]
        s = it.get("show", {})
        return {
            "show": s.get("title"),
            "year": s.get("year"),
            "ids": {"show": s.get("ids", {}), "episode": e.get("ids", {})},
            "season": e.get("season"),
            "episode": e.get("number"),
            "title": e.get("title"),
            "watched_on": watched_at,
            "type": "episode",
        }
    return None


def fetch_history_batch(start_at: Optional[str], page: int, limit: int) -> List[Dict[str, Any]]:
    params = {"limit": limit, "page": page}
    if start_at:
        params["start_at"] = start_at
    r = trakt_get("/sync/history", params=params)
    return r.json()


def main():
    # 1) /users/me (triggert ggf. 401 → Refresh)
    try:
        _ = trakt_get("/users/me", retry_on_401=True).json()
    except RuntimeError as e:
        print(f"[trakt-sync] {e}", file=sys.stderr)
        sys.exit(1)
    except requests.HTTPError as e:
        print(f"[trakt-sync] /users/me HTTP error: {e}", file=sys.stderr)
        sys.exit(1)

    start_at = read_cursor()
    limit = int(os.environ.get("TRAKT_HISTORY_LIMIT", "200"))
    pages = int(os.environ.get("TRAKT_HISTORY_PAGES", "5"))

    movies = load_yaml(MOVIES_YAML) or []
    episodes = load_yaml(EPISODES_YAML) or []

    added = 0
    for page in range(1, pages + 1):
        batch = fetch_history_batch(start_at, page, limit)
        if not batch:
            break
        for it in batch:
            norm = normalize_item(it)
            if not norm:
                continue
            if norm["type"] == "movie":
                movies.append(norm)
            elif norm["type"] == "episode":
                episodes.append(norm)
            added += 1

    # Cursor aktualisieren
    now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    write_cursor(now_iso)

    # Sortieren & Speichern
    movies.sort(key=lambda x: (x.get("watched_on") or ""), reverse=True)
    episodes.sort(key=lambda x: (x.get("watched_on") or "", x.get("season") or 0, x.get("episode") or 0), reverse=True)
    dump_yaml(MOVIES_YAML, movies)
    dump_yaml(EPISODES_YAML, episodes)

    print(f"[trakt-sync] Added {added} new items. Movies: {len(movies)}, Episodes: {len(episodes)}")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as http_err:
            print(f"[trakt-sync] HTTP error: {http_err}", file=sys.stderr)
            sys.exit(2)
    except Exception as e:
        print(f"[trakt-sync] Fatal error: {e}", file=sys.stderr)
        sys.exit(2)
