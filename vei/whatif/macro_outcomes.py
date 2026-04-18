from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from vei.context.api import load_enron_public_context

from .corpus import ENRON_DOMAIN
from .models import (
    WhatIfCounterfactualEstimateDelta,
    WhatIfCounterfactualEstimateResult,
    WhatIfHistoricalScore,
    WhatIfPublicContext,
)

_POSITIVE_TERMS = (
    "hold",
    "pause",
    "legal",
    "review",
    "audit committee",
    "audit",
    "self-report",
    "self report",
    "disclose",
    "restate",
    "collateral",
    "clean draft",
    "single owner",
)
_NEGATIVE_TERMS = (
    "send now",
    "widen",
    "broad",
    "proceed",
    "push through",
    "quarter close",
    "suppress",
    "monitor",
    "fast turnaround",
)
_REGULATORY_RELIEF_TERMS = ("self-report", "self report", "halt", "stop", "disclose")
_REGULATORY_RISK_TERMS = ("death star", "get shorty", "fat boy", "ricochet", "proceed")
MACRO_CALIBRATION_REPORT_PATH = (
    "studies/macro_calibration_enron_v1/calibration_report.md"
)
MACRO_CALIBRATION_METRICS = {
    "stock_spearman": 0.041,
    "credit_auroc": 0.37,
    "credit_brier": 0.516,
    "ferc_auroc": 0.568,
    "ferc_brier": 0.425,
}
_NYSE_TIMEZONE = ZoneInfo("America/New_York")


def attach_macro_outcomes_to_historical_score(
    score: WhatIfHistoricalScore,
    *,
    organization_domain: str,
    branch_timestamp: str,
    public_context: WhatIfPublicContext | None = None,
) -> WhatIfHistoricalScore:
    if organization_domain.strip().lower() != ENRON_DOMAIN:
        return score
    branch_dt = _parse_timestamp(branch_timestamp)
    if branch_dt is None:
        return score
    full_context = _full_enron_context(public_context)
    return score.model_copy(
        update={
            "stock_return_5d": _baseline_stock_return(full_context, branch_dt),
            "credit_action_30d": _future_credit_action(full_context, branch_dt),
            "ferc_action_180d": _future_ferc_action(full_context, branch_dt),
        }
    )


def attach_macro_outcomes_to_forecast_result(
    result: WhatIfCounterfactualEstimateResult,
    *,
    organization_domain: str,
    branch_timestamp: str,
    public_context: WhatIfPublicContext | None = None,
    supports_prediction: bool | None = None,
    capability_note: str | None = None,
) -> WhatIfCounterfactualEstimateResult:
    if organization_domain.strip().lower() != ENRON_DOMAIN:
        return result
    baseline = attach_macro_outcomes_to_historical_score(
        result.baseline,
        organization_domain=organization_domain,
        branch_timestamp=branch_timestamp,
        public_context=public_context,
    )
    supports_macro = (
        result.backend != "e_jepa"
        if supports_prediction is None
        else supports_prediction
    )
    if not supports_macro:
        predicted = result.predicted.model_copy(
            update={
                "stock_return_5d": None,
                "credit_action_30d": None,
                "ferc_action_180d": None,
            }
        )
        delta = result.delta.model_copy(
            update={
                "stock_return_5d_delta": None,
                "credit_action_30d_delta": None,
                "ferc_action_180d_delta": None,
            }
        )
        notes = list(result.notes)
        if capability_note:
            notes.append(capability_note)
        return result.model_copy(
            update={
                "baseline": baseline,
                "predicted": predicted,
                "delta": delta,
                "notes": notes,
            }
        )

    macro_delta = macro_delta_from_prompt(result.prompt)
    predicted = result.predicted.model_copy(
        update={
            "stock_return_5d": _shift_stock_return(
                baseline.stock_return_5d,
                macro_delta["stock_return_5d_delta"],
            ),
            "credit_action_30d": _shift_probability(
                baseline.credit_action_30d,
                macro_delta["credit_action_30d_delta"],
            ),
            "ferc_action_180d": _shift_probability(
                baseline.ferc_action_180d,
                macro_delta["ferc_action_180d_delta"],
            ),
        }
    )
    delta = WhatIfCounterfactualEstimateDelta.model_validate(
        {
            **result.delta.model_dump(mode="json"),
            "stock_return_5d_delta": _difference(
                baseline.stock_return_5d,
                predicted.stock_return_5d,
            ),
            "credit_action_30d_delta": _difference(
                baseline.credit_action_30d,
                predicted.credit_action_30d,
            ),
            "ferc_action_180d_delta": _difference(
                baseline.ferc_action_180d,
                predicted.ferc_action_180d,
            ),
        }
    )
    return result.model_copy(
        update={
            "baseline": baseline,
            "predicted": predicted,
            "delta": delta,
        }
    )


def macro_delta_from_prompt(prompt: str) -> dict[str, float]:
    lowered = prompt.strip().lower()
    stock_delta = 0.0
    credit_delta = 0.0
    ferc_delta = 0.0

    if _contains_any(lowered, _POSITIVE_TERMS):
        stock_delta += 0.04
        credit_delta -= 0.12
    if _contains_any(lowered, _NEGATIVE_TERMS):
        stock_delta -= 0.05
        credit_delta += 0.15
    if _contains_any(lowered, _REGULATORY_RELIEF_TERMS):
        ferc_delta -= 0.18
    if _contains_any(lowered, _REGULATORY_RISK_TERMS):
        ferc_delta += 0.20
    if "collateral" in lowered:
        credit_delta -= 0.08
        stock_delta += 0.02
    if "anonymous" in lowered:
        stock_delta += 0.01
    if "quarter close" in lowered:
        credit_delta += 0.10
        stock_delta -= 0.03

    return {
        "stock_return_5d_delta": round(max(-0.25, min(0.25, stock_delta)), 4),
        "credit_action_30d_delta": round(max(-0.5, min(0.5, credit_delta)), 4),
        "ferc_action_180d_delta": round(max(-0.5, min(0.5, ferc_delta)), 4),
    }


def preview_macro_outcomes_for_prompt(
    prompt: str,
    *,
    organization_domain: str,
    branch_timestamp: str,
    public_context: WhatIfPublicContext | None = None,
) -> tuple[WhatIfHistoricalScore, WhatIfHistoricalScore, dict[str, float]]:
    baseline = attach_macro_outcomes_to_historical_score(
        WhatIfHistoricalScore(),
        organization_domain=organization_domain,
        branch_timestamp=branch_timestamp,
        public_context=public_context,
    )
    delta = macro_delta_from_prompt(prompt)
    predicted = baseline.model_copy(
        update={
            "stock_return_5d": _shift_stock_return(
                baseline.stock_return_5d,
                delta["stock_return_5d_delta"],
            ),
            "credit_action_30d": _shift_probability(
                baseline.credit_action_30d,
                delta["credit_action_30d_delta"],
            ),
            "ferc_action_180d": _shift_probability(
                baseline.ferc_action_180d,
                delta["ferc_action_180d_delta"],
            ),
        }
    )
    return baseline, predicted, delta


def _baseline_stock_return(
    context: WhatIfPublicContext,
    branch_dt: datetime,
) -> float | None:
    rows = list(context.stock_history)
    if not rows:
        return None
    stock_cutoff_day = _stock_history_cutoff_day(branch_dt)
    branch_index = -1
    for index, row in enumerate(rows):
        row_dt = _parse_timestamp(row.as_of)
        if row_dt is None or row_dt.date() > stock_cutoff_day:
            break
        branch_index = index
    if branch_index < 0 or branch_index + 5 >= len(rows):
        return None
    start = rows[branch_index].close
    end = rows[branch_index + 5].close
    if start == 0:
        return None
    return round((end - start) / start, 4)


def _future_credit_action(
    context: WhatIfPublicContext,
    branch_dt: datetime,
) -> float | None:
    if not context.credit_history:
        return None
    branch_end = branch_dt + timedelta(days=30)
    latest = max(
        (_parse_timestamp(event.as_of) for event in context.credit_history),
        default=None,
    )
    if latest is None or latest < branch_dt:
        return None
    for event in context.credit_history:
        event_dt = _parse_timestamp(event.as_of)
        if event_dt is None:
            continue
        if branch_dt < event_dt <= branch_end:
            return 1.0
    if latest >= branch_end:
        return 0.0
    return None


def _future_ferc_action(
    context: WhatIfPublicContext,
    branch_dt: datetime,
) -> float | None:
    if not context.ferc_history:
        return None
    branch_end = branch_dt + timedelta(days=180)
    latest = max(
        (_parse_timestamp(event.timestamp) for event in context.ferc_history),
        default=None,
    )
    if latest is None or latest < branch_dt:
        return None
    for event in context.ferc_history:
        event_dt = _parse_timestamp(event.timestamp)
        if event_dt is None:
            continue
        if branch_dt < event_dt <= branch_end:
            return 1.0
    if latest >= branch_end:
        return 0.0
    return None


def _full_enron_context(
    public_context: WhatIfPublicContext | None,
) -> WhatIfPublicContext:
    if public_context and (
        public_context.stock_history
        or public_context.credit_history
        or public_context.ferc_history
    ):
        last_stock = (
            public_context.stock_history[-1].as_of
            if public_context.stock_history
            else ""
        )
        last_credit = (
            public_context.credit_history[-1].as_of
            if public_context.credit_history
            else ""
        )
        last_ferc = (
            public_context.ferc_history[-1].timestamp
            if public_context.ferc_history
            else ""
        )
        if any(
            value and value[:10] > (public_context.branch_timestamp or "")[:10]
            for value in (last_stock, last_credit, last_ferc)
        ):
            return public_context
    return load_enron_public_context()


def _shift_stock_return(value: float | None, delta: float) -> float | None:
    if value is None:
        return None
    return round(max(-1.0, min(1.0, value + delta)), 4)


def _shift_probability(value: float | None, delta: float) -> float | None:
    if value is None:
        return None
    return round(max(0.0, min(1.0, value + delta)), 4)


def _difference(before: float | None, after: float | None) -> float | None:
    if before is None or after is None:
        return None
    return round(after - before, 4)


def _parse_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _stock_history_cutoff_day(branch_dt: datetime) -> date:
    branch_day = branch_dt.date()
    close_dt = _nyse_close_for_day(branch_day)
    if branch_dt >= close_dt:
        return branch_day
    return branch_day - timedelta(days=1)


def _nyse_close_for_day(day: date) -> datetime:
    return datetime(
        day.year,
        day.month,
        day.day,
        16,
        tzinfo=_NYSE_TIMEZONE,
    ).astimezone(UTC)


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


__all__ = [
    "MACRO_CALIBRATION_METRICS",
    "MACRO_CALIBRATION_REPORT_PATH",
    "attach_macro_outcomes_to_forecast_result",
    "attach_macro_outcomes_to_historical_score",
    "macro_delta_from_prompt",
    "preview_macro_outcomes_for_prompt",
]
