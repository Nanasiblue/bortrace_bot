# Status

## Data

- 2023-2025: existing data available.
- 2026-01-01 to 2026-07-14: available.
- 2026-02-01 to 2026-07-14 download report: 328 successes.
- 2021-2022: downloader script prepared, but Codex network is blocked in this environment.

## Download 2021-2022

Run from normal PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\ryuou\Documents\Codex\2026-07-14\c-users-ryuou-documents-codex-2026\bortrace_rebuild\scripts\download_range.ps1" -StartDate "2021-01-01" -EndDate "2022-12-31"
```

## Baseline Model

- Parsed races: 182,070
- Train rows: 154,146
- Test rows: 27,924
- Test year: 2026
- Model: NumPy softmax baseline
- Winner accuracy: 0.6088669245
- Winner log loss: 1.0833601111
- Upset Brier score: 0.1947547513
- Average predicted upset rate: 0.4510075659
- Actual upset rate: 0.4395502077

## Outputs

- `outputs/baseline_softmax_model.npz`
- `outputs/baseline_metrics.json`
- `outputs/feature_summary.csv`

## Next

- Add binary upset model.
- Add payout band / 荒れ model.
- Add EV and Kelly calculation layer.
- Add live odds scraper research.
- Compare LightGBM / CatBoost once dependencies are available.
