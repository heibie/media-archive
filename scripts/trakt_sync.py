#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Trakt → YAML Sync
- Holt die zuletzt gesehenen Einträge (Movies & Episodes) aus Trakt.
- Schreibt/aktualisiert _data/trakt_history.yml (YAML) für Jekyll.
- Verwendet OAuth2 Refresh-Flow und legt neue Tokens in .trakt_tokens.json im Repo-Root ab,
  damit der Workflow anschließend das Repo-Secret rotieren kann.

ENV EXPECTED:
  TRAKT_CLIENT_ID
  TRAKT_CLIENT_SECRET
  TRAKT_ACCESS_TOKEN   # optional
  TRAKT_REFRESH_TOKEN

OPTIONAL ENV:
  TRAKT_HISTORY_LIMIT (default: 200)
  TRAKT_HISTORY_PAGES (default: 5)
  TRAKT_START_AT_ISO  (default: read from .trakt_cursor if present)
"""

from __future__ import annotations
import os
import sys
import json
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml

# --- Pfade ---
SCRIPT_DIR = Path(__file__).resolve().parent          # .../scripts
REPO_ROOT  = SCRIPT_DIR.parent                        # Repo-Root
DATA_DIR   = REPO_ROOT / "_data"
DATA_DIR.mkdir(exist_ok=True)

HISTORY_FILE = DATA_DIR / "trakt_history.yml"
CURSOR_FILE  = REPO_ROOT / ".trakt_cursor"            # im Repo-Root
TOKENS_OUT   = REPO_ROOT / ".trakt_tokens.json"       # im Repo-Root

# --- Trakt ---
TRAKT_BASE = "https://api.trakt.tv"
USER_AGENT = "trakt-yaml-sync/1.0 (+github actions)"

CLIENT_ID     = os.environ.get("TRAKT_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("TRAKT_CLIENT_SECRET", "")
ACCESS_TOKEN  = os.environ.get("TRAKT_ACCESS_TOKEN", "")
REFRESH_TOKEN = os.environ.get("TRAKT_REFRESH_TOKEN", "")

if not CLIENT_ID or not CLIENT_SECRET or not REFRESH_TOKEN:
    print("ERROR: Missing required Trakt credentials (CLIENT_ID/CLIENT_SECRET/REFRESH_TOKEN).", file=sys.stderr)
    sys.exit(1)

TRAKT_HEADERS = {
    "Content-Type": "application/json",
    "trakt-api-key": CLIENT_ID,
    "trakt-api-version": "2",
    "User-Agent": USER_AGENT,
}
if ACCESS_TOKEN:
    TRAKT_HEADERS["Authorization"] = f"Bearer {ACCESS_TOKEN}"


def read_cursor() -> Optional[str]:
    if "TRAKT_START_AT_ISO" in os.environ and os.environ["TRAKT_START_AT_ISO"].strip():
        return os.environ["TRAKT_START_AT_ISO"].strip()
    if CURSOR_FILE.exists():
        return CURSOR_FILE.read_text(encoding="utf-8").strip()
    return None


def write_cursor(iso_timestamp: str) -> None:
    CURSOR_FILE.write_text(iso_timestamp, encoding="utf-8")


def save_tokens(access_token: str, refresh_token: str) -> None:
    TOKENS_OUT.write_text(
        json.dumps({"access_token": access_token, "refresh_token": refresh_token}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def refresh_tokens() -> None:
    global ACCESS_TOKEN, REFRESH_TOKEN, TRAKT_HEADERS
    payload = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
        "grant_type": "refresh_token",
    }
    r = requests.post(f"{TRAKT_BASE}/oauth/token", json=payload,
                      headers={"Content-Type": "application/json", "User-Agent": USER_AGENT}, timeout=30)
    r.raise_for_status()
    tok = r.json()
    ACCESS_TOKEN  = tok["access_token"]
    REFRESH_TOKEN = tok["refresh_token"]
    TRAKT_HEADERS["Authorization"] = f"Bearer {ACCESS_TOKEN}"
    print("Refreshed Trakt access token.")
    save_tokens(ACCESS_TOKEN, REFRESH_TOKEN)


def trakt_get(path: str, params: Optional[Dict[str, Any]] = None, retry_on_401: bool = True):
    url = f"{TRAKT_BASE}{path}"
    r = requests.get(url, headers=TRAKT_HEADERS, params=params or {}, timeout=45)
    if r.status_code == 401 and retry_on_401:
        print("401 from Trakt. Attempting token refresh…")
        refresh_tokens()
        r = requests.get(url, headers=TRAKT_HEADERS, params=params or {}, timeout=45)
    r.raise_for_status()
    return r


def normalize_history_item(item: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "type": item.get("type"),
        "watched_at": item.get("watched_at"),
        "id": item.get("id"),
    }
    if item.get("movie"):
        m = item["movie"]
        out.update({"title": m.get("title"), "year": m.get("year"), "ids": m.get("ids", {})})
    if item.get("show"):
        s = item["show"]
        out.update({"show": {"title": s.get("title"), "year": s.get("year"), "ids": s.get("ids", {})}})
    if item.get("episode"):
        e = item["episode"]
        out.update({"episode": {"season": e.get("season"), "number": e.get("number"),
                                "title": e.get("title"), "ids": e.get("ids", {})}})
    if "action" in item:
        out["action"] = item["action"]
    return out


def merge_history(existing: List[Dict[str, Any]], new: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_id = {e.get("id"): e for e in existing if "id" in e}
    for e in new:
        by_id[e.get("id")] = e
    merged = list(by_id.values())
    merged.sort(key=lambda x: x.get("watched_at") or "", reverse=True)
    return merged


def load_existing_yaml() -> List[Dict[str, Any]]:
    if not HISTORY_FILE.exists():
        return []
    with HISTORY_FILE.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or []
        return data if isinstance(data, list) else []


def save_yaml(data: List[Dict[str, Any]]) -> None:
    with HISTORY_FILE.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)


def fetch_history() -> List[Dict[str, Any]]:
    limit = int(os.environ.get("TRAKT_HISTORY_LIMIT", "200"))
    pages = int(os.environ.get("TRAKT_HISTORY_PAGES", "5"))
    start_at = read_cursor()

    collected: List[Dict[str, Any]] = []
    page = 1
    while page <= pages:
        params = {"limit": limit, "page": page}
        if start_at:
            params["start_at"] = start_at
        r = trakt_get("/sync/history", params=params)
        batch = r.json()
        if not batch:
            break
        collected.extend(normalize_history_item(x) for x in batch)
        page += 1
    return collected


def update_cursor_from(history: List[Dict[str, Any]]) -> None:
    if not history:
        return
    newest = max(history, key=lambda x: x.get("watched_at") or "")
    if newest.get("watched_at"):
        write_cursor(newest["watched_at"])


def main() -> None:
    try:
        _ = trakt_get("/users/me").json()
    except Exception as e:
        print(f"Warning: /users/me check failed: {e}", file=sys.stderr)

    history_new = fetch_history()
    if history_new:
        existing = load_existing_yaml()
        merged = merge_history(existing, history_new)
        save_yaml(merged)
        update_cursor_from(history_new)
        print(f"Synced {len(history_new)} new history items. Total: {len(merged)}")
    else:
        print("No new history items.")
    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as http_err:
        if http_err.response is not None and http_err.response.status_code == 401:
            print("HTTP 401 on main flow. Trying one last token refresh + retry…", file=sys.stderr)
            try:
                refresh_tokens()
                main()
            except Exception as e2:
                print(f"Final failure after refresh: {e2}", file=sys.stderr)
                sys.exit(2)
        else:
            print(f"HTTP error: {http_err}", file=sys.stderr)
            sys.exit(2)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(2)
