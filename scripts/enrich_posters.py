name: Nightly Enrichment

on:
  schedule:
    - cron: "30 2 * * *"
  workflow_dispatch:

jobs:
  enrich:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install pyyaml requests
      - name: Run TMDB enrichment
        env:
          TMDB_API_KEY: ${{ secrets.TMDB_API_KEY }}
        run: |
          python enrich_posters.py \
            --movies _data/watched_movies.yml \
            --episodes _data/watched_episodes.yml \
            --outdir _data \
            --lang de-DE
      - name: Commit enriched YAMLs
        run: |
          git config user.name  "tmdb-bot"
          git config user.email "tmdb-bot@users.noreply.github.com"
          git add _data/*.yml || true
          git diff --cached --quiet || git commit -m "chore(tmdb): enrich YAMLs"
          git push
