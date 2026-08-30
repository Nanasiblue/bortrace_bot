# Model Plan

## Data Roles

- `B*.TXT`: pre-race entries and racer information.
- `K*.TXT`: race results, exhibition data, environment, and payouts.
- Live scraping, added later: odds, exhibition updates, weather, and notification timing.

## Model Families

1. Finish probability model
   - Predicts each boat's 1st / top-2 / top-3 probability.
   - First implementation: race-level 6-class winner model.
   - Later implementation: boat-row ranking model.

2. Upset model
   - Binary target: boat 1 does not win.
   - Useful for notification filtering and wide strategy.

3.荒れ model
   - Binary targets such as payout over 5,000 / 10,000 / 30,000 yen.
   - Older data can be useful here because it teaches race structure rather than current racer strength.

4. EV and Kelly layer
   - Uses model probability plus live odds.
   - Backtests can use realized payout as a proxy, but production should use live odds.

## Candidate Algorithms

- Baseline: multinomial logistic regression implemented with NumPy.
- Main candidate: LightGBM multiclass and binary models.
- Comparison: CatBoost, XGBoost.
- Calibration: Platt or isotonic calibration when scikit-learn is available.

## Evaluation

- Accuracy and log loss for winner model.
- Brier score for upset /荒れ probabilities.
- Calibration tables by probability bucket.
- ROI, hit rate, bet count, and monthly stability for notification strategies.
- Venue-level drift checks.

## Time Splits

- Train: older years.
- Validation: most recent complete year or quarter.
- Test: newest holdout period.
- Never shuffle races across time for final evaluation.
