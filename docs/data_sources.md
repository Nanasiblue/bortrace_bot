# Data Sources

## Official Static Downloads

Source page:

- BOAT RACE official `ダウンロード・他`

Available:

- Race result downloads.
- Race program downloads.
- Racer term result downloads.

Notes:

- Existing `B*.TXT` and `K*.TXT` come from the official LZH downloads.
- Racer term result files are available by half-year and can supplement racer-level features.

## Official Race Pages

URL pattern:

```text
https://www.boatrace.jp/owpc/pc/race/{page}?hd=YYYYMMDD&jcd=CC&rno=R
```

Important pages:

- `racelist`: entries, racer profile fields, nationwide/local rates, motor/boat rates, deadline time.
- `beforeinfo`: exhibition time, tilt, parts exchange, start exhibition, live weather.
- `odds3t`: trifecta odds.
- `odds3f`: trio odds.
- `odds2tf`: exacta/quinella odds.
- `oddsk`: quinella place odds.
- `oddstf`: win/place odds.
- `raceresult`: finish order, race time, start info, payouts, popularity, weather, deciding move.

## Official Racer Pages

URL pattern:

```text
https://www.boatrace.jp/owpc/pc/data/racersearch/{page}?toban=RACER_ID
```

Important pages:

- `profile`: birthdate, height, weight, blood type, branch, home prefecture, term, current class, future entries.
- `back3`: recent three series results.
- `season`: term stats such as win rate, top-2 rate, top-3 rate, starts, average ST, F/L counts, ability index.
- `course`: course-specific entry rate, top-3 rate, average ST, start order.

## Collectability

Can collect for live/prediction:

- Entries.
- Deadline time.
- Racer profile and racer stats.
- Exhibition time, tilt, parts exchange.
- Start exhibition.
- Current weather.
- Current odds.

Can collect after result:

- Finish order.
- Final odds shown as closing odds.
- Payouts and popularity.
- Start timing and deciding move.
- Result weather.

Needs investigation:

- How far back official race HTML pages remain available.
- Whether odds pages reliably expose historical closing odds for all past dates.
- Whether live odds snapshots need repeated polling before deadline.

Unavailable from existing B/K alone:

- All pre-race odds.
- Odds time series.
- Vote volume and sales.
- Exact live odds at notification time.

## Recommended Storage

- `data/raw/official_pages/YYYYMMDD/{jcd}/{rno}/racelist.html`
- `data/raw/official_pages/YYYYMMDD/{jcd}/{rno}/beforeinfo.html`
- `data/raw/official_pages/YYYYMMDD/{jcd}/{rno}/odds3t.html`
- `data/raw/official_pages/YYYYMMDD/{jcd}/{rno}/raceresult.html`
- `data/processed/races.parquet`
- `data/processed/odds_snapshots.parquet`

For live operation, poll odds pages before deadline and store timestamped snapshots.

## Collection Scripts

Full static collection target:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_rebuild\scripts\collect_all_2021_20260714.ps1"
```

This runs:

- `download_range.ps1` for `B/K` LZH and TXT files.
- `collect_official_pages.ps1` for official HTML pages.

Official HTML collection is large. It is resumable because existing non-empty files are skipped.

Useful smaller runs:

```powershell
# B/K only
powershell -ExecutionPolicy Bypass -File "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_rebuild\scripts\collect_all_2021_20260714.ps1" -SkipOfficialPages

# Official pages only
powershell -ExecutionPolicy Bypass -File "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_rebuild\scripts\collect_all_2021_20260714.ps1" -SkipBk

# Test URL generation only
powershell -ExecutionPolicy Bypass -File "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_rebuild\scripts\collect_all_2021_20260714.ps1" -DryRun
```
