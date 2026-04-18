from __future__ import annotations

from vei.whatif.api import (
    attach_macro_outcomes_to_forecast_result,
    attach_macro_outcomes_to_historical_score,
)
from vei.context.api import WhatIfPublicContext, WhatIfPublicStockHistoryRow
from vei.whatif.models import (
    WhatIfCounterfactualEstimateDelta,
    WhatIfCounterfactualEstimateResult,
    WhatIfHistoricalScore,
)


def _macro_stock_context() -> WhatIfPublicContext:
    return WhatIfPublicContext(
        pack_name="enron_public_context",
        organization_domain="enron.com",
        stock_history=[
            WhatIfPublicStockHistoryRow(
                as_of="2001-05-02T00:00:00Z",
                label="2001-05-02 close",
                close=100.0,
                volume=100.0,
            ),
            WhatIfPublicStockHistoryRow(
                as_of="2001-05-03T00:00:00Z",
                label="2001-05-03 close",
                close=110.0,
                volume=100.0,
            ),
            WhatIfPublicStockHistoryRow(
                as_of="2001-05-04T00:00:00Z",
                label="2001-05-04 close",
                close=120.0,
                volume=100.0,
            ),
            WhatIfPublicStockHistoryRow(
                as_of="2001-05-07T00:00:00Z",
                label="2001-05-07 close",
                close=130.0,
                volume=100.0,
            ),
            WhatIfPublicStockHistoryRow(
                as_of="2001-05-08T00:00:00Z",
                label="2001-05-08 close",
                close=140.0,
                volume=100.0,
            ),
            WhatIfPublicStockHistoryRow(
                as_of="2001-05-09T00:00:00Z",
                label="2001-05-09 close",
                close=150.0,
                volume=100.0,
            ),
            WhatIfPublicStockHistoryRow(
                as_of="2001-05-10T00:00:00Z",
                label="2001-05-10 close",
                close=160.0,
                volume=100.0,
            ),
        ],
    )


def test_attach_macro_outcomes_to_historical_score_uses_repo_fixtures() -> None:
    score = WhatIfHistoricalScore(backend="historical", risk_score=0.9)

    updated = attach_macro_outcomes_to_historical_score(
        score,
        organization_domain="enron.com",
        branch_timestamp="2001-10-30T14:58:45Z",
    )

    assert updated.stock_return_5d is not None
    assert updated.stock_return_5d < 0
    assert updated.credit_action_30d == 1.0


def test_attach_macro_outcomes_to_historical_score_uses_prior_close_before_market_close() -> (
    None
):
    score = WhatIfHistoricalScore(backend="historical", risk_score=0.9)

    updated = attach_macro_outcomes_to_historical_score(
        score,
        organization_domain="enron.com",
        branch_timestamp="2001-05-03T15:00:00Z",
        public_context=_macro_stock_context(),
    )

    assert updated.stock_return_5d == 0.5


def test_attach_macro_outcomes_to_historical_score_uses_same_day_close_after_market_close() -> (
    None
):
    score = WhatIfHistoricalScore(backend="historical", risk_score=0.9)

    updated = attach_macro_outcomes_to_historical_score(
        score,
        organization_domain="enron.com",
        branch_timestamp="2001-05-03T21:30:00Z",
        public_context=_macro_stock_context(),
    )

    assert updated.stock_return_5d == 0.4545


def test_attach_macro_outcomes_to_historical_score_marks_ferc_horizon() -> None:
    score = WhatIfHistoricalScore(backend="historical", risk_score=0.9)

    updated = attach_macro_outcomes_to_historical_score(
        score,
        organization_domain="enron.com",
        branch_timestamp="2000-12-29T12:37:00Z",
    )

    assert updated.ferc_action_180d == 1.0


def test_attach_macro_outcomes_to_forecast_result_applies_prompt_shift() -> None:
    result = WhatIfCounterfactualEstimateResult(
        status="ok",
        backend="heuristic_baseline",
        prompt="Stop the strategy, preserve the record, and self-report to FERC immediately.",
        baseline=WhatIfHistoricalScore(backend="historical", risk_score=0.8),
        predicted=WhatIfHistoricalScore(backend="heuristic_baseline", risk_score=0.5),
        delta=WhatIfCounterfactualEstimateDelta(risk_score_delta=-0.3),
    )

    updated = attach_macro_outcomes_to_forecast_result(
        result,
        organization_domain="enron.com",
        branch_timestamp="2000-12-29T12:37:00Z",
    )

    assert updated.baseline.ferc_action_180d == 1.0
    assert updated.predicted.ferc_action_180d is not None
    assert updated.predicted.ferc_action_180d < updated.baseline.ferc_action_180d
    assert updated.delta.ferc_action_180d_delta is not None
    assert updated.delta.ferc_action_180d_delta < 0


def test_attach_macro_outcomes_to_forecast_result_keeps_old_ejepa_heads_empty() -> None:
    result = WhatIfCounterfactualEstimateResult(
        status="ok",
        backend="e_jepa",
        prompt="Keep the draft inside Enron and ask for review.",
        baseline=WhatIfHistoricalScore(backend="historical", risk_score=0.8),
        predicted=WhatIfHistoricalScore(backend="e_jepa", risk_score=0.6),
        delta=WhatIfCounterfactualEstimateDelta(risk_score_delta=-0.2),
    )

    updated = attach_macro_outcomes_to_forecast_result(
        result,
        organization_domain="enron.com",
        branch_timestamp="2001-10-30T14:58:45Z",
        supports_prediction=False,
        capability_note="macro heads unavailable",
    )

    assert updated.baseline.credit_action_30d == 1.0
    assert updated.predicted.stock_return_5d is None
    assert updated.predicted.credit_action_30d is None
    assert updated.predicted.ferc_action_180d is None
    assert updated.delta.stock_return_5d_delta is None
    assert updated.notes[-1] == "macro heads unavailable"
