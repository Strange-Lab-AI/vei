from __future__ import annotations

import json

from vei.dynamics.eval.reporting import (
    DYNAMICS_EVAL_METRICS_PATH,
    write_dynamics_eval_metrics,
)


def test_dynamics_eval_writes_metrics_json() -> None:
    metrics_path = write_dynamics_eval_metrics()
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert metrics_path == DYNAMICS_EVAL_METRICS_PATH
    assert metrics["factual_next_event_auroc"] >= 0.8
    assert metrics["counterfactual_rank_pct"] >= 0.65
    assert metrics["calibration_ece"] <= 0.08
    assert "macro_stock_spearman" in metrics
    assert "macro_credit_auroc" in metrics
    assert "macro_credit_brier" in metrics
    assert "macro_ferc_auroc" in metrics
    assert "macro_ferc_brier" in metrics
    assert metrics_path.exists()
