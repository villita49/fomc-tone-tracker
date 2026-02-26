# FOMC Tone Tracker

Live at: https://villita49.github.io/fomc-tone-tracker

## How it works
- Daily scraper runs via GitHub Actions at 14:00 UTC
- Scrapes all 13 Fed speech sources (Board + 12 regional banks)
- Scores each speech with Claude AI on hawk/dove scale
- Commits updated `scraper/corpus.json` to this repo
- GitHub Pages serves `index.html` which loads `corpus.json` automatically

## Setup
1. Add `ANTHROPIC_API_KEY` to repo secrets
2. Enable GitHub Pages (Settings → Pages → Source: main branch, /root)
3. Run workflow manually with `lookback_days=30` to backfill

## Local dev
Just open `index.html` in a browser.
