from __future__ import annotations

from pathlib import Path

from vei.whatif.api import estimate_counterfactual_delta
from vei.whatif.business_state import (
    assess_historical_business_state,
    describe_forecast_business_change,
)
from vei.whatif.interventions import intervention_tags
from vei.whatif.models import (
    WhatIfEpisodeManifest,
    WhatIfEventReference,
    WhatIfHistoricalScore,
    WhatIfCounterfactualEstimateDelta,
    WhatIfCounterfactualEstimateResult,
)


def _branch_event() -> WhatIfEventReference:
    return WhatIfEventReference(
        event_id="enron-branch-001",
        timestamp="2000-09-27T14:58:00Z",
        actor_id="debra.perlingiere@enron.com",
        target_id="external@cargill.com",
        event_type="message",
        thread_id="master-agreement-thread",
        subject="Master Agreement Draft",
        to_recipients=["external@cargill.com"],
        has_attachment_reference=True,
    )


def _baseline_forecast() -> WhatIfHistoricalScore:
    return WhatIfHistoricalScore(
        backend="historical",
        future_event_count=84,
        future_escalation_count=16,
        future_assignment_count=43,
        future_approval_count=0,
        future_external_event_count=64,
        risk_score=1.0,
        summary="Historical Enron future.",
    )


def _write_manifest(
    root: Path,
    *,
    branch_event: WhatIfEventReference | None = None,
    forecast: WhatIfHistoricalScore | None = None,
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    resolved_branch_event = branch_event or _branch_event()
    resolved_forecast = forecast or _baseline_forecast()
    manifest = WhatIfEpisodeManifest(
        source="enron",
        source_dir=Path("not-included-in-repo-example"),
        workspace_root=root,
        organization_name="Enron Corporation",
        organization_domain="enron.com",
        thread_id="master-agreement-thread",
        thread_subject=resolved_branch_event.subject,
        branch_event_id=resolved_branch_event.event_id,
        branch_timestamp=resolved_branch_event.timestamp,
        branch_event=resolved_branch_event,
        history_message_count=6,
        future_event_count=resolved_forecast.future_event_count,
        baseline_dataset_path="whatif_baseline_dataset.json",
        content_notice="Historical archive content.",
        actor_ids=[resolved_branch_event.actor_id],
        baseline_future_preview=[],
        forecast=resolved_forecast,
        public_context=None,
        historical_business_state=None,
    )
    (root / "episode_manifest.json").write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )


def test_historical_business_state_assessment_and_change_are_readable() -> None:
    branch_event = _branch_event()
    baseline = _baseline_forecast()
    predicted = baseline.model_copy(
        update={
            "future_event_count": 55,
            "future_assignment_count": 24,
            "future_approval_count": 2,
            "future_external_event_count": 12,
            "risk_score": 0.44,
        }
    )
    result = WhatIfCounterfactualEstimateResult(
        status="ok",
        backend="e_jepa_proxy",
        prompt="Keep the draft inside Enron, ask legal for review, and hold the outside send.",
        baseline=baseline,
        predicted=predicted,
        delta=WhatIfCounterfactualEstimateDelta(
            risk_score_delta=-0.56,
            future_event_delta=-29,
            escalation_delta=0,
            assignment_delta=-19,
            approval_delta=2,
            external_event_delta=-52,
        ),
    )

    assessment = assess_historical_business_state(
        branch_event=branch_event,
        forecast=baseline,
        organization_domain="enron.com",
        public_context=None,
    )
    change = describe_forecast_business_change(
        branch_event=branch_event,
        forecast_result=result,
        organization_domain="enron.com",
        public_context=None,
    )

    assert assessment.summary
    assert assessment.snapshot.exposure > assessment.snapshot.trust
    assert any(
        indicator.label == "Outside spread risk" for indicator in assessment.indicators
    )
    assert change.summary
    assert change.net_effect_score > 0
    assert any(
        impact.state_id == "exposure" and impact.effect == "better"
        for impact in change.impacts
    )
    assert any(
        consequence.consequence_id == "containment"
        and "safer" in consequence.summary.lower()
        for consequence in change.consequence_estimates
    )


def test_intervention_tags_recognize_existing_containment_prompts() -> None:
    stripped_tags = intervention_tags(
        "Remove the outside recipient and strip the attachment before it leaves."
    )
    internal_summary_tags = intervention_tags(
        "Remove the outside recipient and attachment, send only an internal summary, and keep the issue internal."
    )

    assert "external_removed" in stripped_tags
    assert "attachment_removed" in stripped_tags
    assert "external_removed" in internal_summary_tags


def test_intervention_tags_keep_active_outside_loop_risky() -> None:
    tags = intervention_tags(
        'Send "Master Agreement Draft" now, keep the outside loop active, and widen circulation for rapid comments.'
    )

    assert "external_removed" not in tags
    assert "widen_loop" in tags
    assert "send_now" in tags


def test_proxy_forecast_keeps_risk_flat_when_recorded_path_is_already_internal(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "episode"
    _write_manifest(
        workspace_root,
        branch_event=_branch_event().model_copy(
            update={
                "target_id": "gerald.nemec@enron.com",
                "to_recipients": ["gerald.nemec@enron.com"],
                "has_attachment_reference": False,
            }
        ),
        forecast=WhatIfHistoricalScore(
            backend="historical",
            future_event_count=5,
            future_escalation_count=0,
            future_assignment_count=1,
            future_approval_count=0,
            future_external_event_count=0,
            risk_score=0.4,
            summary="Recorded path already stays internal.",
        ),
    )

    result = estimate_counterfactual_delta(
        workspace_root,
        prompt="Keep this internal only.",
    )

    assert result.predicted.future_external_event_count == 0
    assert result.predicted.risk_score == result.baseline.risk_score
    assert any("already stays internal" in note for note in result.notes)


def test_proxy_forecast_attaches_business_state_change_for_enron_candidate_styles(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "episode"
    _write_manifest(workspace_root)

    hold_result = estimate_counterfactual_delta(
        workspace_root,
        prompt=(
            "Keep the draft inside Enron, ask legal for one more review, "
            "and hold the outside send until one owner clears it."
        ),
    )
    status_result = estimate_counterfactual_delta(
        workspace_root,
        prompt=(
            "Send a short no-attachment status note, promise a clean update soon, "
            "and keep one internal owner on the next step."
        ),
    )
    fast_result = estimate_counterfactual_delta(
        workspace_root,
        prompt=(
            'Send "Master Agreement Draft" now, keep the outside loop active, '
            "and widen circulation for rapid comments."
        ),
    )

    assert hold_result.business_state_change is not None
    assert status_result.business_state_change is not None
    assert fast_result.business_state_change is not None
    assert hold_result.predicted.risk_score < hold_result.baseline.risk_score
    assert status_result.predicted.risk_score < status_result.baseline.risk_score
    assert fast_result.predicted.risk_score >= fast_result.baseline.risk_score
    assert (
        hold_result.business_state_change.net_effect_score
        > status_result.business_state_change.net_effect_score
    )
    assert status_result.business_state_change.net_effect_score > 0
    assert fast_result.business_state_change.net_effect_score < 0
