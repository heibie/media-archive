#!/usr/bin/env python3
"""
Enrich YAMLs (movies & episodes) with images + detailed metadata from TMDB,
including German titles (de-DE) and external IDs with Trakt fallback.

Adds debug mode (--debug) to print missing fields into logs.
"""

import os
import sys
import time
import argparse
import yaml
import requests
import statistics
from pathlib import Path


# ---------- IO helpers ----------
def load_yaml(p: Path):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or []


def dump_yaml(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--movies", default="watched_movies.yml", help="Path to movies YAML")
    ap.add_argument("--episodes", default="watched_episodes.yml", help="Path to episodes YAML")
    ap.add_argument("--outdir", default="enriched", help="Output directory")
    ap.add_argument("--tmdb-key", default=os.environ.get("TMDB_API_KEY"),
                    help="TMDB API key (or set TMDB_API_KEY)")
    ap.add_argument("--sleep", type=float, default=0.02, help="Sleep between API calls (seconds)")
    ap.add_argument("--lang", default="de-DE", help="Preferred language for localized titles")
    ap.add_argument("--debug", action="store_true", help="Print debug info for missing fields")
    args = ap.parse_args()

    if not args.tmdb_key:
        print("ERROR: TMDB API key missing. Set TMDB_API_KEY or pass --tmdb-key.", file=sys.stderr)
        sys.exit(1)

    # Sessions
    s = requests.Session()
    s.params = {"api_key": args.tmdb_key}
    s.headers.update({"Accept": "application/json"})

    # TMDB configuration
    cfg = s.get("https://api.themoviedb.org/3/configuration").json()
    base = cfg["images"]["secure_base_url"]
    poster_size = "w500" if "w500" in cfg["images"]["poster_sizes"] else cfg["images"]["poster_sizes"][-1]
    backdrop_size = "w780" if "w780" in cfg["images"]["backdrop_sizes"] else cfg["images"]["backdrop_sizes"][-1]
    still_size = "w300" if "w300" in cfg["images"]["still_sizes"] else cfg["images"]["still_sizes"][-1]

    def build_url(path, size):
        return f"{base}{size}{path}" if path else None

    # caches
    find_cache = {}
    movie_def_cache = {}
    movie_de_cache = {}
    tv_def_cache = {}
    tv_de_cache = {}
    tv_external_ids_cache = {}
    season_de_cache = {}
    episode_cache_de = {}
    episode_cache_def = {}
    trakt_show_ids_cache = {}

    def pause():
        time.sleep(args.sleep)

    # ---- helpers ----
    def find_by_imdb(imdb_id):
        if not imdb_id:
            return {}
        if imdb_id in find_cache:
            return find_cache[imdb_id]
        r = s.get(f"https://api.themoviedb.org/3/find/{imdb_id}",
                  params={"external_source": "imdb_id"})
        find_cache[imdb_id] = r.json() if r.status_code == 200 else {}
        return find_cache[imdb_id]

    def tv_external_ids(tv_id):
        if not tv_id:
            return {}
        if tv_id in tv_external_ids_cache:
            return tv_external_ids_cache[tv_id]
        r = s.get(f"https://api.themoviedb.org/3/tv/{tv_id}/external_ids")
        tv_external_ids_cache[tv_id] = r.json() if r.status_code == 200 else {}
        pause()
        return tv_external_ids_cache[tv_id]

    def trakt_show_ids(trakt_id=None, slug=None):
        client_id = os.environ.get("TRAKT_CLIENT_ID")
        if not client_id:
            return {}
        key = ("id", trakt_id) if trakt_id else ("slug", slug)
        if key in trakt_show_ids_cache:
            return trakt_show_ids_cache[key]
        headers = {
            "trakt-api-version": "2",
            "trakt-api-key": client_id,
            "Accept": "application/json",
        }
        if trakt_id:
            url = f"https://api.trakt.tv/shows/{trakt_id}?extended=ids"
        elif slug:
            url = f"https://api.trakt.tv/shows/{slug}?extended=ids"
        else:
            return {}
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code != 200:
                trakt_show_ids_cache[key] = {}
                return {}
            j = r.json() or {}
            ids = (j.get("ids") or {})
            out = {"imdb": ids.get("imdb"), "tvdb": ids.get("tvdb")}
            trakt_show_ids_cache[key] = out
            return out
        except Exception:
            trakt_show_ids_cache[key] = {}
            return {}

    # ---------------- Movies ----------------
    movies = load_yaml(Path(args.movies))
    for m in movies:
        mid = m.get("tmdb")
        if not mid and m.get("imdb"):
            res = find_by_imdb(m["imdb"]).get("movie_results") or []
            if res:
                mid = res[0]["id"]
        if not mid:
            continue

        if mid not in movie_def_cache:
            r_def = s.get(f"https://api.themoviedb.org/3/movie/{mid}")
            movie_def_cache[mid] = r_def.json() if r_def.status_code == 200 else None
            pause()
        j_def = movie_def_cache.get(mid) or {}

        if mid not in movie_de_cache:
            r_de = s.get(f"https://api.themoviedb.org/3/movie/{mid}",
                         params={"language": args.lang})
            movie_de_cache[mid] = r_de.json() if r_de.status_code == 200 else None
            pause()
        j_de = movie_de_cache.get(mid) or {}

        m["tmdb"] = mid
        m["poster"] = build_url(j_def.get("poster_path"), poster_size) or m.get("poster")
        m["backdrop"] = build_url(j_def.get("backdrop_path"), backdrop_size) or m.get("backdrop")
        m["runtime"] = j_def.get("runtime")
        if not m.get("title"):
            m["title"] = j_def.get("title")
        if not m.get("imdb"):
            m["imdb"] = j_def.get("imdb_id")
        m["title_de"] = j_de.get("title") or m.get("title_de")
        m["overview_de"] = j_de.get("overview") or m.get("overview_de")

        if args.debug and (not m.get("imdb") or not m.get("poster")):
            print(f"[DEBUG] Movie missing fields: title='{m.get('title')}', "
                  f"imdb={m.get('imdb')}, poster={m.get('poster')}")

    # ---------------- Episodes / TV ----------------
    episodes = load_yaml(Path(args.episodes))
    show_meta_cache = {}

    def tv_details_default(tv_id=None, imdb_id=None):
        tid = tv_id
        if not tid and imdb_id:
            res = find_by_imdb(imdb_id).get("tv_results") or []
            if res:
                tid = res[0]["id"]
        if not tid:
            return None, None
        if tid not in tv_def_cache:
            r = s.get(f"https://api.themoviedb.org/3/tv/{tid}")
            tv_def_cache[tid] = r.json() if r.status_code == 200 else None
            pause()
        return tid, tv_def_cache.get(tid)

    def tv_details_localized(tv_id, lang):
        key = (tv_id, lang)
        if key not in tv_de_cache:
            r = s.get(f"https://api.themoviedb.org/3/tv/{tv_id}",
                      params={"language": lang})
            tv_de_cache[key] = r.json() if r.status_code == 200 else None
            pause()
        return tv_de_cache.get(key)

    def season_details_localized(tv_id, season, lang):
        key = (tv_id, season, lang)
        if key not in season_de_cache:
            r = s.get(f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}",
                      params={"language": lang})
            season_de_cache[key] = r.json() if r.status_code == 200 else None
            pause()
        return season_de_cache.get(key)

    def episode_details_def(tv_id, season, ep):
        key = (tv_id, season, ep)
        if key not in episode_cache_def:
            r = s.get(f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{ep}")
            episode_cache_def[key] = r.json() if r.status_code == 200 else None
            pause()
        return episode_cache_def.get(key)

    def episode_details_de(tv_id, season, ep, lang):
        key = (tv_id, season, ep, lang)
        if key not in episode_cache_de:
            r = s.get(f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{ep}",
                      params={"language": lang})
            episode_cache_de[key] = r.json() if r.status_code == 200 else None
            pause()
        return episode_cache_de.get(key)

    for e in episodes:
        tv_id, tv_def = tv_details_default(tv_id=e.get("tmdb"), imdb_id=e.get("imdb"))
        e["tmdb"] = tv_id or e.get("tmdb")

        if tv_id:
            if tv_id not in show_meta_cache:
                tv_de = tv_details_localized(tv_id, args.lang)
                total_eps = tv_def.get("number_of_episodes") if tv_def else None
                run_times = (tv_def.get("episode_run_time") or []) if tv_def else []
                avg_rt = int(round(statistics.mean(run_times))) if run_times else None
                poster_path = (tv_def or {}).get("poster_path") or (tv_de or {}).get("poster_path")
                backdrop_path = (tv_def or {}).get("backdrop_path") or (tv_de or {}).get("backdrop_path")

                ex_ids = tv_external_ids(tv_id)
                imdb_id = ex_ids.get("imdb_id")
                tvdb_id = ex_ids.get("tvdb_id")

                show_meta_cache[tv_id] = {
                    "show_total_episodes": total_eps,
                    "show_episode_run_time": avg_rt,
                    "show_title_de": (tv_de.get("name") if tv_de else None),
                    "show_poster_url": build_url(poster_path, poster_size) if poster_path else None,
                    "show_backdrop_url": build_url(backdrop_path, backdrop_size) if backdrop_path else None,
                    "imdb_id": imdb_id,
                    "tvdb_id": tvdb_id,
                }

            meta = show_meta_cache[tv_id]
            e["show_total_episodes"] = meta["show_total_episodes"]
            e["show_episode_run_time"] = meta["show_episode_run_time"]
            e["show_title_de"] = meta["show_title_de"]

            if not e.get("imdb") and meta.get("imdb_id"):
                e["imdb"] = meta["imdb_id"]
            if not e.get("tvdb") and meta.get("tvdb_id"):
                e["tvdb"] = meta["tvdb_id"]

            if (not e.get("imdb") or not e.get("tvdb")):
                t_ids = trakt_show_ids(trakt_id=e.get("trakt_show"), slug=e.get("slug"))
                if not e.get("imdb") and t_ids.get("imdb"):
                    e["imdb"] = t_ids["imdb"]
                if not e.get("tvdb") and t_ids.get("tvdb"):
                    e["tvdb"] = t_ids["tvdb"]

            if not e.get("show_poster"):
                e["show_poster"] = meta["show_poster_url"]
            if not e.get("show_backdrop"):
                e["show_backdrop"] = meta["show_backdrop_url"]

            sn = e.get("season")
            en = e.get("episode")
            if sn is not None:
                s_de = season_details_localized(tv_id, int(sn), args.lang)
                if s_de and "episodes" in s_de:
                    e["season_total_episodes"] = len(s_de["episodes"])
                else:
                    e.setdefault("season_total_episodes", None)

            if sn is not None and en is not None:
                ed_de = episode_details_de(tv_id, int(sn), int(en), args.lang)
                ed_def = episode_details_def(tv_id, int(sn), int(en))

                if ed_de:
                    e["episode_title_de"] = ed_de.get("name")
                    e["episode_runtime"] = ed_de.get("runtime") or e.get("episode_runtime") \
                        or meta["show_episode_run_time"]
                    if not e.get("episode_still"):
                        e["episode_still"] = build_url(ed_de.get("still_path"), still_size)

                if not e.get("episode_title") and ed_def:
                    e["episode_title"] = ed_def.get("name")

        if args.debug and (not e.get("imdb") or not e.get("tvdb") or not e.get("episode_title_de")):
            print(f"[DEBUG] Episode missing fields: show='{e.get('show')}', "
                  f"season={e.get('season')}, ep={e.get('episode')}, "
                  f"imdb={e.get('imdb')}, tvdb={e.get('tvdb')}, "
                  f"ep_title_de={e.get('episode_title_de')}")

    # ---------- Write output ----------
    outdir = Path(args.outdir)
    dump_yaml(outdir / "watched_movies.yml", movies)
    dump_yaml(outdir / "watched_episodes.yml", episodes)

    print(f"✓ Enriched files written to: {outdir}")
    print("Tip: Use *_de titles with fallback in templates.")


if __name__ == "__main__":
    main()
