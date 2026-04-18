"""Reference learned dynamics backend.

Loads a trained in-repo benchmark checkpoint through the subprocess bridge and
runs a real forward pass over a canonicalized case slice. When a checkpoint is
missing, the backend returns an explicit setup error instead of a silent empty
result.
"""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path
from typing import Any, Sequence

from vei.dynamics.models import (
    BackendInfo,
    BusinessHeads,
    CalibrationMetrics,
    DeterminismManifest,
    DynamicsRequest,
    DynamicsResponse,
    PointInterval,
    PredictedEvent,
)
from vei.events.api import (
    CanonicalEvent,
    EventDomain,
    EventProvenance,
    InternalExternal,
    ProvenanceRecord,
    StateDelta,
)
from vei.whatif.api import (
    WhatIfActionSchema,
    WhatIfBenchmarkDatasetRow,
    WhatIfBranchSummaryFeature,
    WhatIfObservedEvidenceHeads,
    WhatIfPreBranchContract,
    WhatIfSequenceStep,
    macro_delta_from_prompt,
)
from vei.whatif.api import run_branch_point_benchmark_prediction

logger = logging.getLogger(__name__)


class ReferenceBackend:
    """In-repo learned backend backed by a trained benchmark_bridge checkpoint."""

    def __init__(
        self,
        *,
        checkpoint_path: str | None = None,
        device: str | None = None,
        **kwargs: Any,
    ) -> None:
        del kwargs
        env_checkpoint = os.environ.get("VEI_REFERENCE_BACKEND_CHECKPOINT", "")
        resolved_checkpoint = checkpoint_path or env_checkpoint
        self._checkpoint_path = (
            Path(resolved_checkpoint).expanduser().resolve()
            if resolved_checkpoint
            else None
        )
        self._device = str(
            device or os.environ.get("VEI_REFERENCE_BACKEND_DEVICE", "cpu")
        )
        self._loaded: dict[str, Any] | None = None

    def forecast(self, request: DynamicsRequest) -> DynamicsResponse:
        loaded = self._load_model()
        if loaded.get("error"):
            return DynamicsResponse(
                backend_id="reference",
                backend_version="1.0.0",
                state_delta_summary={"error": loaded["error"]},
            )

        try:
            row = self._build_row(request=request)
            prediction = run_branch_point_benchmark_prediction(
                checkpoint_path=loaded["checkpoint_path"],
                row=row,
                device=loaded["device"],
            )
        except Exception as exc:  # noqa: BLE001 - keep backend failures explicit
            logger.warning(
                "reference_backend_forecast_failed",
                extra={"error": str(exc)[:300]},
            )
            return DynamicsResponse(
                backend_id="reference",
                backend_version="1.0.0",
                state_delta_summary={"error": f"reference forecast failed: {exc}"},
            )
        evidence_heads = WhatIfObservedEvidenceHeads.model_validate(
            prediction["evidence_heads"]
        )
        binary_probability = float(prediction["binary_probability"])
        business_heads = self._business_heads_from_evidence(evidence_heads)
        predicted_events = self._predicted_events(
            request.recent_events,
            binary_probability=binary_probability,
            evidence_heads=evidence_heads,
        )

        return DynamicsResponse(
            backend_id="reference",
            backend_version="1.0.0",
            predicted_events=predicted_events,
            business_heads=self._attach_macro_heads(
                business_heads,
                action_text=self._action_text(request),
            ),
            calibration=CalibrationMetrics(),
            state_delta_summary={
                "model_id": str(prediction.get("model_id", "reference")),
                "checkpoint_path": str(loaded["checkpoint_path"]),
                "evidence_heads": evidence_heads.model_dump(mode="json"),
                "binary_probability": round(binary_probability, 6),
            },
        )

    def describe(self) -> BackendInfo:
        return BackendInfo(
            name="reference",
            version="1.0.0",
            backend_type="learned",
            deterministic=False,
            metadata={
                "checkpoint_path": (
                    str(self._checkpoint_path) if self._checkpoint_path else ""
                ),
                "note": (
                    "Loads a benchmark checkpoint through the benchmark bridge "
                    "subprocess runtime."
                ),
            },
        )

    def determinism_manifest(self) -> DeterminismManifest:
        checkpoint_hash = ""
        if self._checkpoint_path and self._checkpoint_path.exists():
            checkpoint_hash = _hash_file(self._checkpoint_path)
        return DeterminismManifest(
            backend_id="reference",
            backend_version="1.0.0",
            checkpoint_hash=checkpoint_hash,
            notes=[
                "Wraps vei.whatif.benchmark_bridge.",
                "predictor runtime: subprocess bridge",
                (
                    f"checkpoint: {self._checkpoint_path}"
                    if self._checkpoint_path is not None
                    else "checkpoint: unset"
                ),
            ],
        )

    def _load_model(self) -> dict[str, Any]:
        if self._loaded is not None:
            return self._loaded
        if self._checkpoint_path is None:
            self._loaded = {
                "error": (
                    "reference backend checkpoint not configured. "
                    "Set VEI_REFERENCE_BACKEND_CHECKPOINT or pass checkpoint_path."
                )
            }
            return self._loaded
        if not self._checkpoint_path.exists():
            self._loaded = {
                "error": (
                    f"reference backend checkpoint does not exist: "
                    f"{self._checkpoint_path}"
                )
            }
            return self._loaded
        self._loaded = {
            "checkpoint_path": self._checkpoint_path,
            "device": self._device,
        }
        return self._loaded

    def _build_row(
        self,
        *,
        request: DynamicsRequest,
    ) -> WhatIfBenchmarkDatasetRow:
        recent_events = list(request.recent_events)
        branch_event = (
            recent_events[-1]
            if recent_events
            else CanonicalEvent(
                tenant_id=request.company_graph_slice.tenant_id,
                ts_ms=0,
                kind="internal.forecast_seed",
                domain=EventDomain.INTERNAL,
                provenance=ProvenanceRecord(origin=EventProvenance.DERIVED),
                delta=StateDelta(domain=EventDomain.INTERNAL, data={}),
            )
        )
        action_schema = self._action_schema(request)
        summary_features = self._summary_features(
            request=request,
            events=recent_events,
        )
        sequence_steps = self._sequence_steps(recent_events)
        contract = WhatIfPreBranchContract(
            case_id=str(branch_event.case_id or "dynamics_case"),
            thread_id=self._thread_id(branch_event),
            branch_event_id=branch_event.event_id,
            branch_event=self._event_reference(branch_event),
            action_schema=action_schema,
            summary_features=summary_features,
            sequence_steps=sequence_steps,
            notes=["generated_from_dynamics_request"],
        )
        return WhatIfBenchmarkDatasetRow(
            row_id=f"{branch_event.event_id}:reference",
            split="heldout",
            thread_id=contract.thread_id,
            branch_event_id=branch_event.event_id,
            contract=contract,
            observed_evidence_heads=WhatIfObservedEvidenceHeads(),
        )

    def _summary_features(
        self,
        *,
        request: DynamicsRequest,
        events: Sequence[CanonicalEvent],
    ) -> list[WhatIfBranchSummaryFeature]:
        action_text = self._action_text(request)
        participants = {
            actor_id
            for event in events
            for actor_id in self._participant_ids(event)
            if actor_id
        }
        external_count = sum(
            1
            for event in events
            if event.internal_external == InternalExternal.EXTERNAL
        )
        attachment_count = sum(1 for event in events if self._attachment_flag(event))
        escalation_count = sum(1 for event in events if self._flagged(event, "escalat"))
        approval_count = sum(1 for event in events if self._flagged(event, "approv"))
        hold_count = 1 if "hold" in action_text or "pause" in action_text else 0
        reassurance_count = 1 if "reassure" in action_text else 0
        avg_delay_hours = self._average_delay_hours(events)
        branch_event = events[-1] if events else None
        branch_external_count = (
            1
            if branch_event
            and branch_event.internal_external == InternalExternal.EXTERNAL
            else 0
        )
        feature_values: dict[str, float] = {
            "history_event_count": float(len(events)),
            "history_external_count": float(external_count),
            "history_attachment_count": float(attachment_count),
            "history_escalation_count": float(escalation_count),
            "history_approval_count": float(approval_count),
            "history_forward_count": float(
                sum(1 for event in events if self._flagged(event, "forward"))
            ),
            "history_legal_count": float(
                sum(1 for event in events if self._flagged(event, "legal"))
            ),
            "history_trading_count": float(
                sum(1 for event in events if self._flagged(event, "trading"))
            ),
            "participant_count": float(len(participants)),
            "branch_external_count": float(branch_external_count),
            "branch_attachment_flag": (
                1.0 if branch_event and self._attachment_flag(branch_event) else 0.0
            ),
            "branch_escalation_flag": (
                1.0 if branch_event and self._flagged(branch_event, "escalat") else 0.0
            ),
            "branch_forward_flag": (
                1.0 if branch_event and self._flagged(branch_event, "forward") else 0.0
            ),
            "baseline_future_event_count": float(len(events)),
            "baseline_future_external_count": float(external_count),
            "baseline_risk_score": min(
                1.0,
                (external_count + attachment_count + escalation_count)
                / max(1.0, float(len(events) * 2)),
            ),
            "rollout_message_count": float(len(events)),
            "rollout_outside_count": float(external_count),
            "rollout_delay_hours": avg_delay_hours,
            "rollout_reassurance_count": float(reassurance_count),
            "rollout_hold_count": float(hold_count),
            "historical_future_message_count": float(len(events)),
            "historical_outside_count": float(external_count),
            "historical_delay_hours": avg_delay_hours,
        }
        for tag in (
            request.candidate_action.policy_tags if request.candidate_action else []
        ):
            feature_values[f"tag__{tag}"] = 1.0
        return [
            WhatIfBranchSummaryFeature(
                name=name,
                value=round(float(value), 6),
            )
            for name, value in sorted(feature_values.items())
        ]

    def _sequence_steps(
        self,
        events: Sequence[CanonicalEvent],
    ) -> list[WhatIfSequenceStep]:
        if not events:
            return []
        first_ts = int(events[0].ts_ms)
        steps: list[WhatIfSequenceStep] = []
        for index, event in enumerate(events[-8:], start=1):
            payload = _payload_dict(event)
            steps.append(
                WhatIfSequenceStep(
                    step_index=index,
                    phase="history",
                    event_type=str(event.kind or "event"),
                    actor_id=str(
                        getattr(getattr(event, "actor_ref", None), "actor_id", "")
                        or "system"
                    ),
                    subject=self._subject(event),
                    delay_ms=max(0, int(event.ts_ms) - first_ts),
                    recipient_scope=self._recipient_scope(event),
                    external_recipient_count=self._external_recipient_count(event),
                    cc_recipient_count=self._count_field(payload, "cc"),
                    attachment_flag=self._attachment_flag(event),
                    escalation_flag=self._flagged(event, "escalat"),
                    approval_flag=self._flagged(event, "approv"),
                    legal_flag=self._flagged(event, "legal"),
                    trading_flag=self._flagged(event, "trading"),
                    review_flag=self._flagged(event, "review"),
                    urgency_flag=self._flagged(event, "urgent"),
                    conflict_flag=self._flagged(event, "conflict"),
                )
            )
        return steps

    def _action_schema(self, request: DynamicsRequest) -> WhatIfActionSchema:
        action_text = self._action_text(request)
        policy_tags = (
            list(request.candidate_action.policy_tags)
            if request.candidate_action
            else []
        )
        tool = (
            str(request.candidate_action.tool)
            if request.candidate_action and request.candidate_action.tool
            else "candidate_action"
        )
        external_recipient_count = (
            1 if "external" in action_text or "send_now" in action_text else 0
        )
        return WhatIfActionSchema(
            event_type=tool,
            recipient_scope="external" if external_recipient_count else "internal",
            external_recipient_count=external_recipient_count,
            attachment_policy="present" if "attachment" in action_text else "none",
            hold_required=("hold" in action_text or "pause" in action_text),
            legal_review_required=(
                "legal" in action_text or "compliance" in action_text
            ),
            trading_review_required=("trading" in action_text),
            escalation_level=(
                "executive"
                if "executive" in action_text
                else "manager" if "escalat" in action_text else "none"
            ),
            owner_clarity="single_owner" if request.candidate_action else "unclear",
            reassurance_style="high" if "reassure" in action_text else "low",
            review_path="internal_legal" if "legal" in action_text else "none",
            coordination_breadth="targeted",
            outside_sharing_posture=(
                "limited_external" if external_recipient_count else "internal_only"
            ),
            decision_posture=(
                "hold"
                if "hold" in action_text or "pause" in action_text
                else "escalate" if "escalat" in action_text else "review"
            ),
            action_tags=policy_tags,
        )

    def _predicted_events(
        self,
        events: Sequence[CanonicalEvent],
        *,
        binary_probability: float,
        evidence_heads: WhatIfObservedEvidenceHeads,
    ) -> list[PredictedEvent]:
        if not events:
            return []
        last_event = events[-1]
        predicted_event = last_event.model_copy(
            update={
                "event_id": f"{last_event.event_id}:predicted",
                "ts_ms": int(last_event.ts_ms)
                + max(1_000, int(evidence_heads.time_to_first_follow_up_ms or 1_000)),
                "provenance": ProvenanceRecord(origin=EventProvenance.DERIVED),
                "delta": StateDelta(
                    domain=last_event.domain,
                    delta_schema_version=0,
                    data={
                        "predicted_by": "reference",
                        "any_external_spread": evidence_heads.any_external_spread,
                        "outside_recipient_count": evidence_heads.outside_recipient_count,
                    },
                ),
            }
        )
        probability = max(0.0, min(1.0, binary_probability))
        return [PredictedEvent(event=predicted_event, probability=probability)]

    def _business_heads_from_evidence(
        self,
        evidence: WhatIfObservedEvidenceHeads,
    ) -> BusinessHeads:
        risk = min(
            1.0,
            (
                (0.25 if evidence.any_external_spread else 0.0)
                + (0.08 * evidence.outside_recipient_count)
                + (0.12 * evidence.outside_attachment_spread_count)
                + (0.10 * evidence.executive_escalation_count)
                + (0.05 * evidence.blame_pressure_count)
            ),
        )
        spread = min(1.0, evidence.outside_recipient_count / 5.0)
        escalation = min(
            1.0,
            (evidence.executive_escalation_count + evidence.legal_follow_up_count)
            / 6.0,
        )
        approval = min(1.0, evidence.legal_follow_up_count / 5.0)
        load = min(1.0, evidence.participant_fanout / 10.0)
        drag = min(1.0, evidence.time_to_thread_end_ms / 86_400_000.0)
        return BusinessHeads(
            risk=_interval(risk),
            spread=_interval(spread),
            escalation=_interval(escalation),
            approval=_interval(approval),
            load=_interval(load),
            drag=_interval(drag),
        )

    def _attach_macro_heads(
        self,
        business_heads: BusinessHeads,
        *,
        action_text: str,
    ) -> BusinessHeads:
        macro_delta = macro_delta_from_prompt(action_text)
        return business_heads.model_copy(
            update={
                "stock_return_5d": PointInterval(
                    point=macro_delta["stock_return_5d_delta"]
                ),
                "credit_action_30d": PointInterval(
                    point=macro_delta["credit_action_30d_delta"]
                ),
                "ferc_action_180d": PointInterval(
                    point=macro_delta["ferc_action_180d_delta"]
                ),
            }
        )

    def _event_reference(self, event: CanonicalEvent):
        from vei.whatif.api import WhatIfEventReference

        payload = _payload_dict(event)
        return WhatIfEventReference(
            event_id=event.event_id,
            timestamp=str(event.ts_ms),
            actor_id=str(
                getattr(getattr(event, "actor_ref", None), "actor_id", "") or "system"
            ),
            target_id=str(payload.get("to") or payload.get("channel") or ""),
            event_type=str(event.kind or "event"),
            thread_id=self._thread_id(event),
            case_id=str(event.case_id or ""),
            surface=str(payload.get("target") or payload.get("surface") or ""),
            conversation_anchor=str(payload.get("thread_id") or ""),
            subject=self._subject(event),
            snippet=self._body(event)[:200],
            to_recipients=self._recipient_list(payload, key="to"),
            cc_recipients=self._recipient_list(payload, key="cc"),
            has_attachment_reference=self._attachment_flag(event),
            is_forward=self._flagged(event, "forward"),
            is_reply=self._flagged(event, "reply"),
            is_escalation=self._flagged(event, "escalat"),
        )

    def _thread_id(self, event: CanonicalEvent) -> str:
        payload = _payload_dict(event)
        return str(
            payload.get("thread_id")
            or payload.get("conversation_id")
            or event.case_id
            or event.event_id
        )

    def _subject(self, event: CanonicalEvent) -> str:
        payload = _payload_dict(event)
        return str(
            payload.get("subj")
            or payload.get("subject")
            or payload.get("title")
            or payload.get("text")
            or event.kind
        )

    def _body(self, event: CanonicalEvent) -> str:
        payload = _payload_dict(event)
        return str(
            payload.get("body_text")
            or payload.get("body")
            or payload.get("text")
            or payload.get("comment")
            or self._subject(event)
        )

    def _participant_ids(self, event: CanonicalEvent) -> list[str]:
        participant_ids: list[str] = []
        if event.actor_ref is not None and event.actor_ref.actor_id:
            participant_ids.append(str(event.actor_ref.actor_id))
        for participant in event.participants:
            if participant.actor_id and participant.actor_id not in participant_ids:
                participant_ids.append(str(participant.actor_id))
        return participant_ids

    def _recipient_scope(self, event: CanonicalEvent) -> str:
        if event.internal_external == InternalExternal.EXTERNAL:
            return "external"
        if event.internal_external == InternalExternal.INTERNAL:
            return "internal"
        return "unknown"

    def _external_recipient_count(self, event: CanonicalEvent) -> int:
        payload = _payload_dict(event)
        if event.internal_external == InternalExternal.EXTERNAL:
            return max(1, len(self._recipient_list(payload, key="to")))
        return 0

    def _count_field(self, payload: dict[str, Any], prefix: str) -> int:
        recipients = self._recipient_list(payload, key=prefix)
        if recipients:
            return len(recipients)
        scalar_value = payload.get(f"{prefix}_count")
        if scalar_value is None:
            return 0
        try:
            return int(scalar_value)
        except Exception:
            return 0

    def _attachment_flag(self, event: CanonicalEvent) -> bool:
        payload = _payload_dict(event)
        if "attachment" in " ".join(event.policy_tags).lower():
            return True
        for key, value in payload.items():
            if "attachment" in str(key).lower() and bool(value):
                return True
        return False

    def _flagged(self, event: CanonicalEvent, token: str) -> bool:
        haystack = " ".join(
            [
                str(event.kind),
                self._subject(event),
                self._body(event),
                " ".join(event.policy_tags),
            ]
        ).lower()
        return token in haystack

    def _recipient_list(self, payload: dict[str, Any], *, key: str) -> list[str]:
        value = payload.get(key)
        if isinstance(value, list):
            return [str(item) for item in value if str(item).strip()]
        if isinstance(value, str) and value.strip():
            if "," in value:
                return [part.strip() for part in value.split(",") if part.strip()]
            return [value.strip()]
        return []

    def _average_delay_hours(self, events: Sequence[CanonicalEvent]) -> float:
        if len(events) < 2:
            return 0.0
        gaps = [
            max(0, int(current.ts_ms) - int(previous.ts_ms))
            for previous, current in zip(events, events[1:], strict=False)
        ]
        return round((sum(gaps) / len(gaps)) / 3_600_000, 6)

    def _action_text(self, request: DynamicsRequest) -> str:
        if request.candidate_action is None:
            return ""
        return " ".join(
            [
                request.candidate_action.label,
                request.candidate_action.description,
                " ".join(request.candidate_action.policy_tags),
            ]
        ).lower()


def _payload_dict(event: CanonicalEvent) -> dict[str, Any]:
    if event.delta is None or not isinstance(event.delta.data, dict):
        return {}
    return dict(event.delta.data)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65_536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _interval(point: float, *, half_width: float = 0.1) -> PointInterval:
    clipped = max(0.0, min(1.0, float(point)))
    return PointInterval(
        point=clipped,
        lower=max(0.0, clipped - half_width),
        upper=min(1.0, clipped + half_width),
    )
