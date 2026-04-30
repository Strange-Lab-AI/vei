from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Sequence

from vei.ui._public_demo_routes import (
    PUBLIC_DEMO_CAVEAT,
    PUBLIC_DEMO_DEFAULT_AS_OF,
    PUBLIC_DEMO_DEFAULT_SOURCE_ID,
    PUBLIC_DEMO_DEFAULT_TOPIC,
    _DEFAULT_ACTIONS,
)
from vei.whatif.api import load_world
from vei.whatif.benchmark import _build_pre_branch_contract
from vei.whatif.benchmark_bridge import (
    BenchmarkPreprocessor,
    TorchTrainer,
    _action_vector_width,
    _load_compatible_state_dict,
    _SEQUENCE_NUMERIC_WIDTH,
    _SEQUENCE_TOKEN_LIMIT,
)
from vei.whatif.models import WhatIfEvent
from vei.whatif.news_state_points import (
    _action_schema_for_candidate,
    _infer_candidate_type,
    build_news_state_point,
)

_TOPICS: tuple[tuple[str, str], ...] = (
    ("all_public_record", "All public record"),
    ("banking_markets", "Banking and markets"),
    ("government_policy", "Government policy"),
    ("public_order", "Public order"),
    ("slavery_petitions", "Slavery and petitions"),
    ("international", "International affairs"),
    ("labor_work", "Labor and work"),
    ("public_health_disaster", "Public health and disasters"),
    ("crime_courts", "Crime and courts"),
    ("agriculture_weather", "Agriculture and weather"),
    ("transport_infrastructure", "Transport and infrastructure"),
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export the public-history demo as static browser assets."
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path("docs/examples/news-public-history-demo/workspace"),
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    workspace = args.workspace.expanduser().resolve()
    output = args.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)

    manifest = _load_manifest(workspace)
    source_path = workspace / str(manifest.get("source_path", "context_snapshot.json"))
    checkpoint_path = workspace / str(
        manifest.get("jepa_checkpoint_path", "jepa_model.pt")
    )
    world = load_world(
        source="company_history",
        source_dir=source_path,
        include_situation_graph=False,
    )

    checkpoint = _load_checkpoint(checkpoint_path)
    preprocessor = BenchmarkPreprocessor.from_metadata(checkpoint["metadata"])
    export_model_onnx(
        checkpoint=checkpoint,
        preprocessor=preprocessor,
        output_path=output / "jepa_model.onnx",
    )
    bundle = build_static_bundle(
        manifest=manifest,
        world=world,
        preprocessor=preprocessor,
        checkpoint_path=checkpoint_path,
    )
    (output / "bundle.json").write_text(
        json.dumps(bundle, separators=(",", ":")),
        encoding="utf-8",
    )
    return 0


def _load_manifest(workspace: Path) -> dict[str, Any]:
    path = workspace / "public_demo_manifest.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _load_checkpoint(path: Path) -> dict[str, Any]:
    import torch

    return torch.load(path.expanduser().resolve(), map_location="cpu")


def export_model_onnx(
    *,
    checkpoint: dict[str, Any],
    preprocessor: BenchmarkPreprocessor,
    output_path: Path,
) -> None:
    import torch

    trainer = TorchTrainer(
        model_id=checkpoint["model_id"],
        preprocessor=preprocessor,
    )
    model = trainer.build_model(device="cpu")
    _load_compatible_state_dict(model, checkpoint["state_dict"])
    model.eval()

    class PublicHistoryJEPA(torch.nn.Module):
        def __init__(self, wrapped: Any) -> None:
            super().__init__()
            self.wrapped = wrapped

        def forward(
            self,
            summary: Any,
            action: Any,
            token_categorical: Any,
            token_numeric: Any,
        ) -> tuple[Any, Any, Any, Any, Any, Any]:
            output = self.wrapped(
                summary,
                action,
                token_categorical,
                token_numeric,
            )
            return (
                output["binary_logits"],
                output["regression"],
                output["business"],
                output["objective"],
                output["future_state"],
                output["predicted_latent"],
            )

    wrapper = PublicHistoryJEPA(model).eval()
    summary = torch.zeros(
        1,
        len(preprocessor.summary_feature_names),
        dtype=torch.float32,
    )
    action = torch.zeros(
        1,
        _action_vector_width(preprocessor),
        dtype=torch.float32,
    )
    token_categorical = torch.zeros(
        1,
        _SEQUENCE_TOKEN_LIMIT,
        3,
        dtype=torch.long,
    )
    token_numeric = torch.zeros(
        1,
        _SEQUENCE_TOKEN_LIMIT,
        _SEQUENCE_NUMERIC_WIDTH,
        dtype=torch.float32,
    )
    torch.onnx.export(
        wrapper,
        (summary, action, token_categorical, token_numeric),
        output_path,
        input_names=["summary", "action", "token_categorical", "token_numeric"],
        output_names=[
            "binary_logits",
            "regression",
            "business",
            "objective",
            "future_state",
            "predicted_latent",
        ],
        dynamic_axes={
            "summary": {0: "batch"},
            "action": {0: "batch"},
            "token_categorical": {0: "batch"},
            "token_numeric": {0: "batch"},
            "binary_logits": {0: "batch"},
            "regression": {0: "batch"},
            "business": {0: "batch"},
            "objective": {0: "batch"},
            "future_state": {0: "batch"},
            "predicted_latent": {0: "batch"},
        },
        opset_version=17,
        do_constant_folding=True,
        dynamo=False,
    )


def build_static_bundle(
    *,
    manifest: dict[str, Any],
    world: Any,
    preprocessor: BenchmarkPreprocessor,
    checkpoint_path: Path,
) -> dict[str, Any]:
    dates = sorted({event.timestamp[:10] for event in world.events})
    default_as_of = str(manifest.get("default_as_of") or PUBLIC_DEMO_DEFAULT_AS_OF)
    if default_as_of not in dates:
        dates.append(default_as_of)
        dates.sort()

    events: dict[str, dict[str, Any]] = {}
    states: dict[str, dict[str, Any]] = {}
    for topic, _label in _TOPICS:
        topic_states: dict[str, Any] = {}
        for day in dates:
            state_point = build_news_state_point(
                world,
                topic=topic,
                as_of=day,
                future_horizon_days=90,
                max_history_events=240,
                max_evidence_events=12,
                allow_empty_history=True,
            )
            encoded = _encoded_state_base(
                preprocessor=preprocessor,
                state_point=state_point,
                organization_domain=world.summary.organization_domain,
            )
            for event in state_point.evidence_events:
                events[event.event_id] = _event_payload(event)
            topic_states[day] = {
                "as_of": state_point.as_of,
                "state_summary": state_point.state_summary,
                "history_event_count": len(state_point.history_events),
                "future_event_count": len(state_point.future_events),
                "evidence_event_ids": [
                    event.event_id for event in state_point.evidence_events
                ],
                "headline": _headline_for_state(state_point.evidence_events),
                "summary": _round_floats(encoded["summary"]),
                "token_categorical_base": encoded["token_categorical_base"],
                "token_numeric_base": _round_floats(encoded["token_numeric_base"]),
                "action_index": encoded["action_index"],
            }
        states[topic] = topic_states

    return {
        "version": "1",
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "source": {
            "source_id": str(
                manifest.get("source_id") or PUBLIC_DEMO_DEFAULT_SOURCE_ID
            ),
            "title": str(
                manifest.get("title") or "Public History: AmericanStories News World"
            ),
            "summary": str(manifest.get("summary") or ""),
            "default_topic": str(
                manifest.get("default_topic") or PUBLIC_DEMO_DEFAULT_TOPIC
            ),
            "default_as_of": default_as_of,
            "first_timestamp": world.summary.first_timestamp,
            "last_timestamp": world.summary.last_timestamp,
            "event_count": world.summary.event_count,
        },
        "topics": [{"id": topic, "label": label} for topic, label in _TOPICS],
        "dates": dates,
        "events": events,
        "states": states,
        "suggested_candidate_actions": [
            item.model_dump(mode="json") for item in _DEFAULT_ACTIONS
        ],
        "model": {
            "model_id": "jepa_latent",
            "onnx_path": "jepa_model.onnx",
            "checkpoint_sha256": _file_sha256(checkpoint_path),
            "summary_feature_names": preprocessor.summary_feature_names,
            "action_tag_names": preprocessor.action_tag_names,
            "event_type_names": preprocessor.event_type_names,
            "target_mean": _round_floats(preprocessor.target_mean.tolist()),
            "target_std": _round_floats(preprocessor.target_std.tolist()),
            "business_mean": _round_floats(preprocessor.business_mean.tolist()),
            "business_std": _round_floats(preprocessor.business_std.tolist()),
            "objective_mean": _round_floats(preprocessor.objective_mean.tolist()),
            "objective_std": _round_floats(preprocessor.objective_std.tolist()),
            "objective_head_trained": preprocessor.objective_head_trained,
            "future_state_mean": _round_floats(preprocessor.future_state_mean.tolist()),
            "future_state_std": _round_floats(preprocessor.future_state_std.tolist()),
            "action_text_vector_width": preprocessor.action_text_vector_width,
            "sequence_token_limit": _SEQUENCE_TOKEN_LIMIT,
            "sequence_numeric_width": _SEQUENCE_NUMERIC_WIDTH,
        },
        "caveat": PUBLIC_DEMO_CAVEAT,
    }


def _encoded_state_base(
    *,
    preprocessor: BenchmarkPreprocessor,
    state_point: Any,
    organization_domain: str,
) -> dict[str, Any]:
    dummy_action = _action_schema_for_candidate(
        action="Hold for cross-source verification.",
        candidate_type=_infer_candidate_type("Hold for cross-source verification."),
    )
    contract = _build_pre_branch_contract(
        case_id=state_point.branch_event.case_id,
        thread_id=state_point.branch_event.thread_id,
        branch_event=state_point.branch_event,
        history_events=state_point.history_events,
        organization_domain=organization_domain,
        action_schema=dummy_action,
        notes=[
            "News state-point row.",
            "no_future_context=true",
            "state_point_not_historical_branch_event=true",
        ],
    )
    summary_values = preprocessor._encode_summary(contract.summary_features)
    token_categorical, token_numeric = preprocessor._encode_sequence(
        contract.sequence_steps,
        dummy_action,
        summary_values,
    )
    return {
        "summary": summary_values.tolist(),
        "token_categorical_base": [
            int(value) for value in token_categorical.reshape(-1).tolist()
        ],
        "token_numeric_base": token_numeric.reshape(-1).tolist(),
        "action_index": len(contract.sequence_steps[-(_SEQUENCE_TOKEN_LIMIT - 2) :]),
    }


def _event_payload(event: WhatIfEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "timestamp": event.timestamp,
        "subject": event.subject,
        "snippet": event.snippet,
        "actor_id": event.actor_id,
        "surface": event.surface,
    }


def _headline_for_state(events: Sequence[WhatIfEvent]) -> str:
    for event in reversed(events):
        subject = event.subject.strip()
        if subject and subject.lower() not in {"public record", "news"}:
            return subject
    return "Sparse public record"


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _round_floats(values: Sequence[float]) -> list[float]:
    return [round(float(value), 6) for value in values]


if __name__ == "__main__":
    raise SystemExit(main())
