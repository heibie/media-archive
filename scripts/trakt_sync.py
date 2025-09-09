#!/usr/bin/env python3
import os, sys, time, json, requests, yaml
from pathlib import Path
from datetime import datetime, timezone

CLIENT_ID     = os.environ["TRAKT_CLIENT_ID"]
CLIENT_SECRET = os.environ["TRAKT_CLIENT_SECRET"]
ACCESS_TOKEN  = os.environ["TRAKT_ACCESS_TOKEN"]
REFRESH_TOKEN = os.environ["TRAKT_REFRESH_TOKEN"]

TRAKT_HEADERS = {
    "Content-Type": "application/json",
    "trakt-api-version": "2",
    "trakt-api-key": CLIENT_ID,
    "Authorization": f"Bearer {ACCESS_TOKEN}",
}

ROOT              = Path(os.environ.get("GITHUB_WORKSPACE", "."))  # GH Actions liefert das
DATA_DIR          = ROOT / "_data"
MOVIES_YAML       = DATA_DIR / "watched_movies.yml"
EPISODES_YAML     = DATA_DIR / "watched_episodes.yml"
CURSOR_FILE       = ROOT / ".trakt_cursor.json"  # speichert letzte verarbeitete Zeit

def load_yaml(p): 
    return yaml.safe_load(p.read_text(encoding="utf-8")) if p.exists() else []

def dump_yaml(p, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)

def load_cursor():
    if not CURSOR_FILE.exists(): return {"last_seen": None}
    return json.loads(CURSOR_FILE.read_text())

def save_cursor(obj):
    CURSOR_FILE.write_text(json.dumps(obj, ensure_ascii=False, indent=2))

def refresh_tokens():
    global ACCESS_TOKEN, REFRESH_TOKEN, TRAKT_HEADERS
    r = requests.post("https://api.trakt.tv/oauth/token", json={
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
        "grant_type": "refresh_token"
    }, headers={"Content-Type":"application/json"})
    r.raise_for_status()
    tok = r.json()
    ACCESS_TOKEN = tok["access_token"]
    REFRESH_TOKEN = tok["refresh_token"]
    TRAKT_HEADERS["Authorization"] = f"Bearer {ACCESS_TOKEN}"
    print("Refreshed Trakt token.")

def paged_get(url, params):
    """Generator über /sync/history Pagination (X-Pagination-* header)."""
    page = 1
    while True:
        rp = dict(params, page=page, limit=100)
        r = requests.get(url, headers=TRAKT_HEADERS, params=rp)
        if r.status_code == 401:
            refresh_tokens()
            r = requests.get(url, headers=TRAKT_HEADERS, params=rp)
        r.raise_for_status()
        items = r.json() or []
        yield from items

        # Pagination-Header auslesen, abbrechen wenn durch
        page_count = int(r.headers.get("X-Pagination-Page-Count", "1"))
        if page >= page_count: break
        page += 1
        time.sleep(0.2)

def as_date(dts):
    # Trakt liefert ISO8601 (UTC). Wir nehmen YYYY-MM-DD.
    try:
        dt = datetime.fromisoformat(dts.replace("Z","+00:00")).astimezone(timezone.utc)
        return dt.date().isoformat()
    except Exception:
        return dts.split("T")[0] if "T" in dts else dts

def upsert_movie(movies, it):
    m = it["movie"]
    watched_on = as_date(it["watched_at"])
    key = (m.get("ids",{}).get("tmdb"), m.get("ids",{}).get("imdb"), watched_on)
    # Duplikate vermeiden: gleicher TMDB/IMDb + Datum
    for x in movies:
        if (x.get("tmdb"), x.get("imdb"), x.get("watched_on")) == key:
            return False
    movies.append({
        "title": m.get("title"),
        "title_de": None,           # wird im Enrichment gefüllt
        "year": m.get("year"),
        "imdb": m.get("ids",{}).get("imdb"),
        "tmdb": m.get("ids",{}).get("tmdb"),
        "watched_on": watched_on,
        "poster": None, "backdrop": None,
        "runtime": None, "overview_de": None,
        "source": "trakt"
    })
    return True

def upsert_episode(episodes, it):
    e = it["episode"]; sh = it["show"]
    watched_on = as_date(it["watched_at"])
    key = (sh.get("ids",{}).get("tmdb"), e.get("season"), e.get("number"), watched_on)
    for x in episodes:
        if (x.get("tmdb"), x.get("season"), x.get("episode"), x.get("watched_on")) == key:
            return False
    episodes.append({
        "show": sh.get("title"),
        "show_title_de": None,      # Enrichment
        "year": sh.get("year"),
        "season": e.get("season"),
        "episode": e.get("number"),
        "plays": 1,
        "watched_on": watched_on,
        "trakt_show": sh.get("ids",{}).get("trakt"),
        "tvdb": sh.get("ids",{}).get("tvdb"),
        "imdb": sh.get("ids",{}).get("imdb"),
        "tmdb": sh.get("ids",{}).get("tmdb"),
        "slug": sh.get("ids",{}).get("slug"),
        "show_poster": None, "show_backdrop": None,
        "episode_still": None,
        "episode_title": None, "episode_title_de": None,
        "episode_runtime": None, "season_total_episodes": None,
        "show_total_episodes": None, "show_episode_run_time": None,
        "source": "trakt"
    })
    return True

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    movies = load_yaml(MOVIES_YAML)
    episodes = load_yaml(EPISODES_YAML)
    cur = load_cursor()

    params = {}
    # Wenn Cursor vorhanden → nur Neuigkeiten seitdem (start_at erwartet UTC ISO)
    if cur.get("last_seen"):
        params["start_at"] = cur["last_seen"]

    # Wir ziehen beide Typen getrennt, um einfach zu mappen
    added = 0

    # Movies
    for it in paged_get("https://api.trakt.tv/sync/history/movies", params=params):
        added += upsert_movie(movies, it)

    # Episodes
    for it in paged_get("https://api.trakt.tv/sync/history/episodes", params=params):
        added += upsert_episode(episodes, it)

    # Cursor fortschreiben = höchste seenTime aus dieser Runde (UTC now als Fallback)
    now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    cur["last_seen"] = now_iso
    save_cursor(cur)

    # Speichern (stabil sortiert: neueste zuerst)
    movies.sort(key=lambda x: (x.get("watched_on") or ""), reverse=True)
    episodes.sort(key=lambda x: (x.get("watched_on") or "", x.get("season") or 0, x.get("episode") or 0), reverse=True)
    dump_yaml(MOVIES_YAML, movies)
    dump_yaml(EPISODES_YAML, episodes)

    print(f"Added {added} new items. Movies: {len(movies)}, Episodes: {len(episodes)}")

if __name__ == "__main__":
    main()
