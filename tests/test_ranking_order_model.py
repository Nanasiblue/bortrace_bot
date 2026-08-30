import numpy as np
import pandas as pd

from bortrace.ranking_order_model import build_boat_ranking_features, ranking_relevance


def test_ranking_features_have_six_rows_and_relative_rank():
    row = {"month": 1, "jcd": 2, "rno": 3}
    for boat in range(1, 7):
        row[f"national_win_rate_{boat}"] = float(7 - boat)
    features = build_boat_ranking_features(pd.DataFrame([row]))
    assert len(features) == 6
    assert features["boat"].tolist() == [1, 2, 3, 4, 5, 6]
    assert features["national_win_rate_rank_high"].tolist() == [1, 2, 3, 4, 5, 6]


def test_relevance_and_constrained_sort_are_unique():
    row = {f"target_pos{i}": i - 1 for i in range(1, 7)}
    relevance = ranking_relevance(pd.DataFrame([row]))
    assert relevance.tolist() == [5, 4, 3, 2, 1, 0]
    order = np.argsort(-np.array([[0.2, 0.7, 0.1, 0.6, 0.4, 0.3]]), axis=1) + 1
    assert order.tolist() == [[2, 4, 5, 6, 1, 3]]
    assert sorted(order[0].tolist()) == [1, 2, 3, 4, 5, 6]
