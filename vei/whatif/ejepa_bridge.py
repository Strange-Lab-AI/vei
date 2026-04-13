from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from vei.whatif.corpus import (
    detect_whatif_source,
    event_by_id,
    event_reference,
    external_recipient_count as shared_external_recipient_count,
    has_external_recipients,
    internal_recipient_count as shared_internal_recipient_count,
    is_internal_recipient as shared_is_internal_recipient,
    load_company_history_world,
    load_mail_archive_world,
    load_enron_world,
    recipient_scope as shared_recipient_scope,
    thread_events,
)
from vei.whatif.interventions import intervention_tags
from vei.whatif.models import (
    WhatIfHistoricalScore,
    WhatIfCounterfactualEstimateArtifacts,
    WhatIfCounterfactualEstimateDelta,
    WhatIfCounterfactualEstimateResult,
)

_SELECTED_STATE_COLUMNS = [
    "obs_num__history_event_count",
    "obs_num__unique_participant_count",
    "obs_num__external_event_count",
    "obs_num__forward_event_count",
    "obs_num__escalation_event_count",
    "obs_num__assignment_event_count",
    "obs_num__approval_event_count",
    "obs_num__legal_event_count",
    "obs_num__trading_event_count",
    "obs_num__attachment_event_count",
    "obs_num__reply_event_count",
    "obs_num__current_risk_score",
]


def main() -> None:
    parser = argparse.ArgumentParser(add_help=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    forecast_parser = subparsers.add_parser("forecast")
    forecast_parser.add_argument("--request", required=True)
    forecast_parser.add_argument("--output", required=True)

    args = parser.parse_args()
    if args.command == "forecast":
        request_path = Path(args.request).expanduser().resolve()
        output_path = Path(args.output).expanduser().resolve()
        payload = json.loads(request_path.read_text(encoding="utf-8"))
        result = _run_forecast(payload)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")


def _run_forecast(payload: dict[str, Any]) -> WhatIfCounterfactualEstimateResult:
    cache_root = Path(str(payload["cache_root"])).expanduser().resolve()
    cache_root.mkdir(parents=True, exist_ok=True)
    source_dir = (
        Path(str(payload.get("source_dir", payload.get("rosetta_dir", ""))))
        .expanduser()
        .resolve()
    )
    source = str(payload.get("source", "") or "").strip().lower()
    if not source:
        source = detect_whatif_source(source_dir)
    thread_id = str(payload["thread_id"])
    branch_event_id = str(payload["branch_event_id"])
    prompt = str(payload["prompt"])
    llm_messages = payload.get("llm_messages", [])
    device = str(payload.get("device", "") or _default_device())
    epochs = int(payload.get("epochs", 4))
    batch_size = int(payload.get("batch_size", 64))
    force_retrain = bool(payload.get("force_retrain", False))

    world = _load_world(source=source, source_dir=source_dir)
    timeline = thread_events(world.events, thread_id)
    if not timeline:
        return WhatIfCounterfactualEstimateResult(
            status="error",
            backend="e_jepa",
            prompt=prompt,
            summary="The selected thread was not found in the historical world.",
            error=f"missing thread: {thread_id}",
        )
    branch_event = event_by_id(timeline, branch_event_id)
    if branch_event is None:
        return WhatIfCounterfactualEstimateResult(
            status="error",
            backend="e_jepa",
            prompt=prompt,
            summary="The selected branch event was not found in the thread.",
            error=f"missing branch event: {branch_event_id}",
        )
    training_thread_ids = _select_training_threads(
        world=world,
        thread_id=thread_id,
        branch_event=branch_event,
    )

    prepared, checkpoint_path, decoder_path, evaluation = _ensure_training_bundle(
        world=world,
        cache_root=cache_root,
        device=device,
        epochs=epochs,
        batch_size=batch_size,
        force_retrain=force_retrain,
        training_thread_ids=training_thread_ids,
    )
    result = _forecast_thread(
        prepared=prepared,
        checkpoint_path=checkpoint_path,
        decoder_path=decoder_path,
        timeline=timeline,
        branch_event_id=branch_event_id,
        prompt=prompt,
        llm_messages=llm_messages if isinstance(llm_messages, list) else [],
        organization_domain=world.summary.organization_domain,
        device=device,
    )
    if evaluation:
        result.notes.append(
            "E-JEPA eval: "
            f"surprise={evaluation.get('mean_surprise', 0):.4f}, "
            f"latent_mse={evaluation.get('latent_mse', 0):.4f}."
        )
    result.notes.append(
        f"Training slice: {len(training_thread_ids)} threads around the branch point."
    )
    result.artifacts = WhatIfCounterfactualEstimateArtifacts(
        cache_root=cache_root,
        dataset_root=prepared.root,
        checkpoint_path=checkpoint_path,
        decoder_path=decoder_path,
    )
    return result


def _ensure_training_bundle(
    *,
    world,
    cache_root: Path,
    device: str,
    epochs: int,
    batch_size: int,
    force_retrain: bool,
    training_thread_ids: Sequence[str],
):
    import pandas as pd

    from structured_jepa.schema import TrainConfig
    from structured_jepa.storage import (
        finalize_processed_dataset,
        load_processed_dataset,
    )
    from structured_jepa.training import (
        evaluate_model,
        fit_summary_decoder,
        train_model,
    )

    dataset_root = cache_root / "dataset"
    model_root = cache_root / "model"
    checkpoint_path = model_root / "model.pt"
    decoder_path = model_root / "decoder.json"
    evaluation_path = model_root / "evaluation.json"

    if force_retrain or not (dataset_root / "steps.parquet").exists():
        rows = _build_training_rows(world, training_thread_ids=training_thread_ids)
        frame = pd.DataFrame(rows)
        episode_lengths = frame.groupby("episode_id")["step_idx"].max().add(1)
        max_episode_length = (
            int(episode_lengths.max()) if not episode_lengths.empty else 0
        )
        context_length = max(1, min(8, max_episode_length - 1))
        prepared = finalize_processed_dataset(
            raw_steps=frame,
            output_dir=dataset_root,
            dataset_kind="timeseries",
            observation_numeric_columns=[
                "thread_age_s",
                "history_event_count",
                "unique_participant_count",
                "external_event_count",
                "forward_event_count",
                "escalation_event_count",
                "assignment_event_count",
                "approval_event_count",
                "legal_event_count",
                "trading_event_count",
                "attachment_event_count",
                "reply_event_count",
                "current_risk_score",
                "last_to_count",
                "last_cc_count",
                "last_bcc_count",
            ],
            observation_categorical_columns=[
                "last_event_type",
                "last_actor_id",
                "last_scope",
            ],
            action_numeric_columns=[
                "action_to_count",
                "action_cc_count",
                "action_bcc_count",
                "action_has_attachment",
                "action_is_forward",
                "action_is_reply",
                "action_is_escalation",
                "action_external_recipient_count",
                "action_internal_recipient_count",
                "action_consult_legal",
                "action_consult_trading",
            ],
            action_categorical_columns=[
                "action_actor_id",
                "action_event_type",
                "action_scope",
                "action_review_path",
            ],
            auxiliary_numeric_targets=[],
            metadata_columns=[
                "meta__event_id",
                "meta__thread_id",
                "meta__actor_id",
                "meta__subject",
            ],
            notes={
                "source": world.summary.source,
                "event_count": world.summary.event_count,
                "thread_count": world.summary.thread_count,
            },
            context_length=context_length,
        )
    else:
        prepared = load_processed_dataset(dataset_root)

    model_root.mkdir(parents=True, exist_ok=True)
    if force_retrain or not checkpoint_path.exists():
        artifacts = train_model(
            dataset_root=prepared.root,
            output_dir=model_root,
            train_config=TrainConfig(
                epochs=epochs,
                batch_size=batch_size,
                device=device,
            ),
        )
        checkpoint_path = Path(artifacts.model_path).expanduser().resolve()
    if force_retrain or not decoder_path.exists():
        decoder = fit_summary_decoder(
            dataset_root=prepared.root,
            checkpoint_path=checkpoint_path,
            columns=_SELECTED_STATE_COLUMNS,
            device=device,
        )
        decoder_path.write_text(json.dumps(decoder, indent=2), encoding="utf-8")
    evaluation: dict[str, Any] = {}
    if force_retrain or not evaluation_path.exists():
        evaluation_result = evaluate_model(
            dataset_root=prepared.root,
            checkpoint_path=checkpoint_path,
            device=device,
        )
        evaluation = evaluation_result.model_dump(mode="json")
        evaluation_path.write_text(json.dumps(evaluation, indent=2), encoding="utf-8")
    else:
        evaluation = json.loads(evaluation_path.read_text(encoding="utf-8"))
    return prepared, checkpoint_path, decoder_path, evaluation


def _select_training_threads(
    *,
    world,
    thread_id: str,
    branch_event,
    max_threads: int = 96,
    max_events: int = 6000,
) -> list[str]:
    target_thread = next(
        (thread for thread in world.threads if thread.thread_id == thread_id),
        None,
    )
    if target_thread is None:
        return [thread_id]
    target_actor_ids = {actor_id for actor_id in target_thread.actor_ids if actor_id}
    target_subject_tokens = _subject_tokens(target_thread.subject)
    selected: list[str] = [thread_id]
    event_budget = target_thread.event_count
    scored: list[tuple[int, int, str]] = []
    for thread in world.threads:
        if thread.thread_id == thread_id:
            continue
        score = 0
        actor_overlap = len(target_actor_ids & set(thread.actor_ids))
        if actor_overlap:
            score += actor_overlap * 5
        subject_overlap = len(target_subject_tokens & _subject_tokens(thread.subject))
        if subject_overlap:
            score += subject_overlap * 3
        if (
            branch_event.flags.has_attachment_reference
            and thread.attachment_event_count > 0
        ):
            score += 2
        if branch_event.flags.consult_legal_specialist and thread.legal_event_count > 0:
            score += 2
        if (
            branch_event.flags.consult_trading_specialist
            and thread.trading_event_count > 0
        ):
            score += 2
        if has_external_recipients(branch_event.flags.to_recipients) and (
            thread.external_recipient_event_count > 0
        ):
            score += 2
        if score <= 0:
            continue
        scored.append((score, thread.event_count, thread.thread_id))
    for _, thread_event_count, candidate_thread_id in sorted(
        scored,
        key=lambda item: (-item[0], -item[1], item[2]),
    ):
        if len(selected) >= max_threads:
            break
        if event_budget + thread_event_count > max_events:
            continue
        selected.append(candidate_thread_id)
        event_budget += thread_event_count
    return selected


def _build_training_rows(
    world,
    *,
    training_thread_ids: Sequence[str],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    allowed_thread_ids = set(training_thread_ids)
    organization_domain = str(world.summary.organization_domain or "").strip().lower()
    for thread in world.threads:
        if thread.thread_id not in allowed_thread_ids:
            continue
        timeline = thread_events(world.events, thread.thread_id)
        if not timeline:
            continue
        participants: set[str] = set()
        counters = {
            "external": 0,
            "forward": 0,
            "escalation": 0,
            "assignment": 0,
            "approval": 0,
            "legal": 0,
            "trading": 0,
            "attachment": 0,
            "reply": 0,
        }
        last_event_type = "__start__"
        last_actor_id = "__start__"
        last_scope = "none"
        last_to_count = 0.0
        last_cc_count = 0.0
        last_bcc_count = 0.0
        thread_start_ms = timeline[0].timestamp_ms
        previous_ms = thread_start_ms
        for index, event in enumerate(timeline):
            current_to_count = float(
                max(event.flags.to_count, len(event.flags.to_recipients))
            )
            current_cc_count = float(event.flags.cc_count)
            current_bcc_count = float(event.flags.bcc_count)
            rows.append(
                {
                    "episode_id": thread.thread_id,
                    "step_idx": index,
                    "timestamp": event.timestamp,
                    "delta_t_s": (
                        0.0
                        if index == 0
                        else max(0.0, (event.timestamp_ms - previous_ms) / 1000.0)
                    ),
                    "done": index == len(timeline) - 1,
                    "split": "train",
                    "action_name": _action_name(event),
                    "meta__event_id": event.event_id,
                    "meta__thread_id": event.thread_id,
                    "meta__actor_id": event.actor_id,
                    "meta__subject": event.subject,
                    "thread_age_s": max(
                        0.0, (event.timestamp_ms - thread_start_ms) / 1000.0
                    ),
                    "history_event_count": float(index),
                    "unique_participant_count": float(len(participants)),
                    "external_event_count": float(counters["external"]),
                    "forward_event_count": float(counters["forward"]),
                    "escalation_event_count": float(counters["escalation"]),
                    "assignment_event_count": float(counters["assignment"]),
                    "approval_event_count": float(counters["approval"]),
                    "legal_event_count": float(counters["legal"]),
                    "trading_event_count": float(counters["trading"]),
                    "attachment_event_count": float(counters["attachment"]),
                    "reply_event_count": float(counters["reply"]),
                    "current_risk_score": _risk_from_counts(counters),
                    "last_to_count": last_to_count,
                    "last_cc_count": last_cc_count,
                    "last_bcc_count": last_bcc_count,
                    "last_event_type": last_event_type,
                    "last_actor_id": last_actor_id,
                    "last_scope": last_scope,
                    "action_actor_id": event.actor_id or "__unknown__",
                    "action_event_type": event.event_type or "__unknown__",
                    "action_scope": _recipient_scope(
                        event.flags.to_recipients,
                        organization_domain=organization_domain,
                    ),
                    "action_review_path": _review_path(
                        consult_legal=event.flags.consult_legal_specialist,
                        consult_trading=event.flags.consult_trading_specialist,
                    ),
                    "action_to_count": current_to_count,
                    "action_cc_count": current_cc_count,
                    "action_bcc_count": current_bcc_count,
                    "action_has_attachment": float(
                        event.flags.has_attachment_reference
                    ),
                    "action_is_forward": float(event.flags.is_forward),
                    "action_is_reply": float(event.flags.is_reply),
                    "action_is_escalation": float(
                        event.flags.is_escalation or event.event_type == "escalation"
                    ),
                    "action_external_recipient_count": float(
                        _external_recipient_count(
                            event.flags.to_recipients,
                            organization_domain=organization_domain,
                        )
                    ),
                    "action_internal_recipient_count": float(
                        _internal_recipient_count(
                            event.flags.to_recipients,
                            organization_domain=organization_domain,
                        )
                    ),
                    "action_consult_legal": float(event.flags.consult_legal_specialist),
                    "action_consult_trading": float(
                        event.flags.consult_trading_specialist
                    ),
                }
            )
            _update_participants(participants, event)
            _update_counters(counters, event)
            last_event_type = event.event_type or "__unknown__"
            last_actor_id = event.actor_id or "__unknown__"
            last_scope = _recipient_scope(
                event.flags.to_recipients,
                organization_domain=organization_domain,
            )
            last_to_count = current_to_count
            last_cc_count = current_cc_count
            last_bcc_count = current_bcc_count
            previous_ms = event.timestamp_ms
    return rows


def _subject_tokens(subject: str) -> set[str]:
    tokens = {
        token.strip(".,:;!?()[]{}\"'").lower()
        for token in subject.split()
        if token.strip(".,:;!?()[]{}\"'")
    }
    return {
        token
        for token in tokens
        if len(token) >= 3 and token not in {"the", "and", "for", "re", "fw"}
    }


def _forecast_thread(
    *,
    prepared,
    checkpoint_path: Path,
    decoder_path: Path,
    timeline: Sequence[Any],
    branch_event_id: str,
    prompt: str,
    llm_messages: list[dict[str, Any]],
    organization_domain: str,
    device: str,
) -> WhatIfCounterfactualEstimateResult:
    import pandas as pd
    import torch

    from structured_jepa.readouts import apply_linear_readout
    from structured_jepa.storage import collate_step_batches, rows_to_step_batch
    from structured_jepa.training import load_trained_model

    model, schema = load_trained_model(checkpoint_path)
    model = model.to(device)
    decoder_payload = json.loads(decoder_path.read_text(encoding="utf-8"))
    weights = torch.tensor(decoder_payload.get("weights", []), dtype=torch.float32)
    selected_columns = [
        str(column) for column in decoder_payload.get("columns", []) if str(column)
    ]
    if not selected_columns:
        return WhatIfCounterfactualEstimateResult(
            status="error",
            backend="e_jepa",
            prompt=prompt,
            summary="The E-JEPA decoder did not contain any selected columns.",
            error="empty decoder columns",
        )
    thread_frame = (
        prepared.frame.loc[prepared.frame["episode_id"] == timeline[0].thread_id]
        .sort_values("step_idx")
        .reset_index(drop=True)
    )
    branch_matches = thread_frame.index[
        thread_frame["meta__event_id"] == branch_event_id
    ].tolist()
    if not branch_matches:
        return WhatIfCounterfactualEstimateResult(
            status="error",
            backend="e_jepa",
            prompt=prompt,
            summary="The selected branch event was not present in the E-JEPA dataset.",
            error=f"missing branch row: {branch_event_id}",
        )
    branch_index = branch_matches[0]
    branch_row = thread_frame.iloc[[branch_index]].copy().reset_index(drop=True)
    current_state = _raw_state_from_row(branch_row.iloc[0], schema, selected_columns)
    baseline_future = list(timeline[branch_index:])
    baseline_forecast = _baseline_forecast_from_events(baseline_future)
    actual_state = _actual_final_state(current_state, baseline_forecast)

    prompt_tags = intervention_tags(prompt)
    candidate_branch_row = _candidate_row_from_prompt(
        branch_row.iloc[0].copy(),
        prompt=prompt,
        tags=prompt_tags,
        organization_domain=organization_domain,
    )
    remaining_future_rows = (
        thread_frame.iloc[branch_index + 1 :].copy().reset_index(drop=True)
    )
    candidate_future_rows = _candidate_future_rows(
        future_rows=remaining_future_rows,
        prompt=prompt,
        tags=prompt_tags,
        llm_messages=llm_messages,
        organization_domain=organization_domain,
    )

    context_start = max(0, branch_index - schema.context_length + 1)
    context_frame = (
        thread_frame.iloc[context_start : branch_index + 1]
        .copy()
        .reset_index(drop=True)
    )
    context_frame.iloc[-1] = candidate_branch_row

    initial_batch = collate_step_batches(
        [rows_to_step_batch(context_frame, schema)]
    ).to(device)
    with torch.no_grad():
        state_history = model.encode_steps(initial_batch)
        action_history = model.encode_actions(initial_batch)
        predicted_latents: list[torch.Tensor] = []

        first_predicted = model.predict_next(
            state_history[:, -schema.context_length :],
            action_history[:, -schema.context_length :],
        )[:, -1:]
        predicted_latents.append(first_predicted)
        state_history = torch.cat([state_history, first_predicted], dim=1)

        for row in candidate_future_rows:
            action_frame = pd.DataFrame([row])
            action_batch = collate_step_batches(
                [rows_to_step_batch(action_frame, schema)]
            ).to(device)
            next_action = model.encode_actions(action_batch)
            action_history = torch.cat([action_history, next_action], dim=1)
            predicted = model.predict_next(
                state_history[:, -schema.context_length :],
                action_history[:, -schema.context_length :],
            )[:, -1:]
            predicted_latents.append(predicted)
            state_history = torch.cat([state_history, predicted], dim=1)

        final_latent = predicted_latents[-1].reshape(1, -1).cpu()
        decoded_final = apply_linear_readout(final_latent, weights).reshape(-1)

    predicted_state = _decode_state_summary(decoded_final, schema, selected_columns)
    predicted_forecast = _predicted_forecast_from_state(
        current_state=current_state,
        predicted_state=predicted_state,
        horizon_event_count=len(predicted_latents),
    )
    delta = WhatIfCounterfactualEstimateDelta(
        risk_score_delta=round(
            predicted_forecast.risk_score - baseline_forecast.risk_score, 3
        ),
        future_event_delta=predicted_forecast.future_event_count
        - baseline_forecast.future_event_count,
        escalation_delta=predicted_forecast.future_escalation_count
        - baseline_forecast.future_escalation_count,
        assignment_delta=predicted_forecast.future_assignment_count
        - baseline_forecast.future_assignment_count,
        approval_delta=predicted_forecast.future_approval_count
        - baseline_forecast.future_approval_count,
        external_event_delta=predicted_forecast.future_external_event_count
        - baseline_forecast.future_external_event_count,
    )

    actual_next_state = _actual_next_state(
        thread_frame=thread_frame,
        branch_index=branch_index,
        schema=schema,
        selected_columns=selected_columns,
        fallback=actual_state,
    )
    surprise_score = _state_distance(predicted_state, actual_next_state)
    notes = _forecast_notes(
        prompt=prompt,
        llm_messages=llm_messages,
        baseline=baseline_forecast,
        predicted=predicted_forecast,
    )
    summary = (
        f"E-JEPA predicts {predicted_forecast.future_event_count} follow-up events from the "
        f"branch point, with risk moving from {baseline_forecast.risk_score:.3f} "
        f"to {predicted_forecast.risk_score:.3f}."
    )
    return WhatIfCounterfactualEstimateResult(
        status="ok",
        backend="e_jepa",
        prompt=prompt,
        summary=summary,
        baseline=baseline_forecast,
        predicted=predicted_forecast,
        delta=delta,
        branch_event=event_reference(timeline[branch_index]),
        horizon_event_count=len(predicted_latents),
        surprise_score=round(surprise_score, 4),
        current_state_summary=current_state,
        predicted_state_summary=predicted_state,
        actual_state_summary=actual_state,
        notes=notes,
    )


def _candidate_future_rows(
    *,
    future_rows,
    prompt: str,
    tags: set[str],
    llm_messages: list[dict[str, Any]],
    organization_domain: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    llm_offset = 0
    for index in range(len(future_rows)):
        base_row = future_rows.iloc[index].copy()
        if llm_offset < len(llm_messages):
            rows.append(
                _candidate_row_from_message(
                    base_row,
                    message=llm_messages[llm_offset],
                    prompt=prompt,
                    tags=tags,
                    organization_domain=organization_domain,
                )
            )
            llm_offset += 1
            continue
        rows.append(
            _candidate_row_from_prompt(
                base_row,
                prompt=prompt,
                tags=tags,
                organization_domain=organization_domain,
            )
        )
    while llm_offset < len(llm_messages):
        rows.append(
            _candidate_row_from_message(
                future_rows.iloc[-1].copy() if len(future_rows) else {},
                message=llm_messages[llm_offset],
                prompt=prompt,
                tags=tags,
                organization_domain=organization_domain,
            )
        )
        llm_offset += 1
    return rows


def _candidate_row_from_message(
    base_row: Any,
    *,
    message: dict[str, Any],
    prompt: str,
    tags: set[str],
    organization_domain: str,
) -> dict[str, Any]:
    row = _coerce_row(base_row)
    recipient = str(message.get("to", "")).strip()
    body_text = str(message.get("body_text", "")).lower()
    subject = str(message.get("subject", "")).strip().lower()
    local_tags = set(tags)
    if "legal" in body_text or "compliance" in body_text:
        local_tags.update({"legal", "compliance"})
    if "trading" in body_text:
        local_tags.add("trading")
    if _is_internal_recipient(recipient, organization_domain=organization_domain):
        local_tags.add("external_removed")
    if "attachment" in body_text or "strip" in body_text:
        local_tags.add("attachment_removed")

    row["action_name"] = "counterfactual_email"
    row["action_actor_id"] = str(
        message.get("actor_id", row.get("action_actor_id", "__unknown__"))
    )
    row["action_event_type"] = "reply" if subject.startswith("re:") else "message"
    row["action_scope"] = _recipient_scope(
        [recipient] if recipient else [],
        organization_domain=organization_domain,
    )
    row["action_review_path"] = _review_path(
        consult_legal="legal" in local_tags or "compliance" in local_tags,
        consult_trading="trading" in local_tags,
    )
    row["action_to_count"] = 1.0 if recipient else 0.0
    row["action_cc_count"] = 0.0
    row["action_bcc_count"] = 0.0
    row["action_has_attachment"] = (
        0.0
        if "attachment_removed" in local_tags
        else float(row.get("action_has_attachment", 0.0))
    )
    row["action_is_forward"] = 1.0 if subject.startswith("fw:") else 0.0
    row["action_is_reply"] = 1.0 if subject.startswith("re:") else 0.0
    row["action_is_escalation"] = 1.0 if "escalate" in body_text else 0.0
    row["action_external_recipient_count"] = float(
        0
        if _is_internal_recipient(recipient, organization_domain=organization_domain)
        else 1
    )
    row["action_internal_recipient_count"] = float(
        1
        if _is_internal_recipient(recipient, organization_domain=organization_domain)
        else 0
    )
    row["action_consult_legal"] = float(
        "legal" in local_tags or "compliance" in local_tags
    )
    row["action_consult_trading"] = float("trading" in local_tags)
    row["meta__actor_id"] = row["action_actor_id"]
    if subject:
        row["meta__subject"] = subject
    return row


def _candidate_row_from_prompt(
    base_row: Any,
    *,
    prompt: str,
    tags: set[str],
    organization_domain: str,
) -> dict[str, Any]:
    row = _coerce_row(base_row)
    row["action_review_path"] = _review_path(
        consult_legal="legal" in tags
        or "compliance" in tags
        or bool(row.get("action_consult_legal", 0.0)),
        consult_trading="trading" in tags
        or bool(row.get("action_consult_trading", 0.0)),
    )
    if "external_removed" in tags:
        row["action_scope"] = "internal"
        row["action_external_recipient_count"] = 0.0
        row["action_internal_recipient_count"] = max(
            1.0 if organization_domain else 0.0,
            float(row.get("action_internal_recipient_count", 0.0) or 0.0),
        )
    if "attachment_removed" in tags:
        row["action_has_attachment"] = 0.0
    if "hold" in tags or "pause_forward" in tags:
        row["action_name"] = "hold"
        row["action_event_type"] = "hold"
        row["action_is_forward"] = 0.0
    if "executive_gate" in tags:
        row["action_is_escalation"] = 0.0
    if "legal" in tags or "compliance" in tags:
        row["action_consult_legal"] = 1.0
    if "trading" in tags:
        row["action_consult_trading"] = 1.0
    if "reply_immediately" in tags and row.get("action_event_type") == "message":
        row["action_event_type"] = "reply"
        row["action_is_reply"] = 1.0
    return row


def _baseline_forecast_from_events(events: Sequence[Any]) -> WhatIfHistoricalScore:
    future_event_count = len(events)
    future_escalation_count = sum(
        1
        for event in events
        if event.flags.is_escalation or event.event_type == "escalation"
    )
    future_assignment_count = sum(
        1 for event in events if event.event_type == "assignment"
    )
    future_approval_count = sum(1 for event in events if event.event_type == "approval")
    future_external_event_count = sum(
        1 for event in events if has_external_recipients(event.flags.to_recipients)
    )
    risk_score = min(
        1.0,
        (
            (future_escalation_count * 0.25)
            + (future_assignment_count * 0.15)
            + (future_external_event_count * 0.2)
            + max(0, future_event_count - future_approval_count) * 0.02
        ),
    )
    return WhatIfHistoricalScore(
        backend="historical",
        future_event_count=future_event_count,
        future_escalation_count=future_escalation_count,
        future_assignment_count=future_assignment_count,
        future_approval_count=future_approval_count,
        future_external_event_count=future_external_event_count,
        risk_score=round(risk_score, 3),
        summary=(
            f"{future_event_count} future events remain, including "
            f"{future_escalation_count} escalations and {future_external_event_count} external sends."
        ),
    )


def _predicted_forecast_from_state(
    *,
    current_state: dict[str, float],
    predicted_state: dict[str, float],
    horizon_event_count: int,
) -> WhatIfHistoricalScore:
    future_event_count = max(0, horizon_event_count)
    future_escalation_count = _clamp_count(
        predicted_state.get("escalation_event_count", 0.0),
        current_state.get("escalation_event_count", 0.0),
        ceiling=future_event_count,
    )
    future_assignment_count = _clamp_count(
        predicted_state.get("assignment_event_count", 0.0),
        current_state.get("assignment_event_count", 0.0),
        ceiling=future_event_count,
    )
    future_approval_count = _clamp_count(
        predicted_state.get("approval_event_count", 0.0),
        current_state.get("approval_event_count", 0.0),
        ceiling=future_event_count,
    )
    future_external_event_count = _clamp_count(
        predicted_state.get("external_event_count", 0.0),
        current_state.get("external_event_count", 0.0),
        ceiling=future_event_count,
    )
    risk_score = max(0.0, min(1.0, predicted_state.get("current_risk_score", 0.0)))
    return WhatIfHistoricalScore(
        backend="e_jepa",
        future_event_count=future_event_count,
        future_escalation_count=future_escalation_count,
        future_assignment_count=future_assignment_count,
        future_approval_count=future_approval_count,
        future_external_event_count=future_external_event_count,
        risk_score=round(risk_score, 3),
        summary=(
            f"Predicted {future_event_count} follow-up events, "
            f"{future_escalation_count} escalations, and "
            f"{future_external_event_count} external sends."
        ),
    )


def _actual_final_state(
    current_state: dict[str, float],
    baseline: WhatIfHistoricalScore,
) -> dict[str, float]:
    return {
        **current_state,
        "history_event_count": current_state.get("history_event_count", 0.0)
        + float(baseline.future_event_count),
        "external_event_count": current_state.get("external_event_count", 0.0)
        + float(baseline.future_external_event_count),
        "forward_event_count": current_state.get("forward_event_count", 0.0),
        "escalation_event_count": current_state.get("escalation_event_count", 0.0)
        + float(baseline.future_escalation_count),
        "assignment_event_count": current_state.get("assignment_event_count", 0.0)
        + float(baseline.future_assignment_count),
        "approval_event_count": current_state.get("approval_event_count", 0.0)
        + float(baseline.future_approval_count),
        "current_risk_score": baseline.risk_score,
    }


def _actual_next_state(
    *,
    thread_frame,
    branch_index: int,
    schema,
    selected_columns: Sequence[str],
    fallback: dict[str, float],
) -> dict[str, float]:
    next_index = branch_index + 1
    if next_index >= len(thread_frame):
        return fallback
    return _raw_state_from_row(thread_frame.iloc[next_index], schema, selected_columns)


def _raw_state_from_row(
    row: Any, schema, selected_columns: Sequence[str]
) -> dict[str, float]:
    values: dict[str, float] = {}
    for column in selected_columns:
        raw_value = float(row[column])
        source_name = _source_name_for_column(schema, column)
        values[source_name] = _denormalize_value(schema, column, raw_value)
    return values


def _decode_state_summary(
    decoded_vector, schema, selected_columns: Sequence[str]
) -> dict[str, float]:
    summary: dict[str, float] = {}
    for index, column in enumerate(selected_columns):
        source_name = _source_name_for_column(schema, column)
        summary[source_name] = _denormalize_value(
            schema, column, float(decoded_vector[index])
        )
    return summary


def _source_name_for_column(schema, column_name: str) -> str:
    for feature in schema.observation_numeric:
        if feature.name == column_name:
            return feature.source_column
    return column_name


def _denormalize_value(schema, column_name: str, value: float) -> float:
    for feature in schema.observation_numeric:
        if feature.name != column_name:
            continue
        return float((value * feature.std) + feature.mean)
    return float(value)


def _state_distance(
    predicted_state: dict[str, float],
    actual_state: dict[str, float],
) -> float:
    keys = sorted(set(predicted_state) & set(actual_state))
    if not keys:
        return 0.0
    squared = [
        (float(predicted_state[key]) - float(actual_state[key])) ** 2 for key in keys
    ]
    return sum(squared) / max(1, len(squared))


def _forecast_notes(
    *,
    prompt: str,
    llm_messages: Sequence[dict[str, Any]],
    baseline: WhatIfHistoricalScore,
    predicted: WhatIfHistoricalScore,
) -> list[str]:
    notes = [
        f"Branch prompt: {prompt}",
        f"Baseline external sends: {baseline.future_external_event_count}",
        f"Predicted external sends: {predicted.future_external_event_count}",
    ]
    if llm_messages:
        notes.append(
            f"Forecast was conditioned on {len(llm_messages)} generated counterfactual messages."
        )
    return notes


def _risk_from_counts(counters: dict[str, int]) -> float:
    risk = (
        counters["escalation"] * 0.12
        + counters["assignment"] * 0.08
        + counters["external"] * 0.18
        + counters["attachment"] * 0.04
        + counters["legal"] * 0.05
        + counters["trading"] * 0.05
    )
    return round(min(1.0, max(0.0, risk)), 3)


def _update_counters(counters: dict[str, int], event: Any) -> None:
    counters["external"] += int(has_external_recipients(event.flags.to_recipients))
    counters["forward"] += int(event.flags.is_forward)
    counters["escalation"] += int(
        event.flags.is_escalation or event.event_type == "escalation"
    )
    counters["assignment"] += int(event.event_type == "assignment")
    counters["approval"] += int(event.event_type == "approval")
    counters["legal"] += int(event.flags.consult_legal_specialist)
    counters["trading"] += int(event.flags.consult_trading_specialist)
    counters["attachment"] += int(event.flags.has_attachment_reference)
    counters["reply"] += int(event.flags.is_reply or event.event_type == "reply")


def _update_participants(participants: set[str], event: Any) -> None:
    for value in [event.actor_id, event.target_id, *event.flags.to_recipients]:
        token = str(value).strip()
        if token:
            participants.add(token)


def _review_path(*, consult_legal: bool, consult_trading: bool) -> str:
    if consult_legal and consult_trading:
        return "legal_trading"
    if consult_legal:
        return "legal"
    if consult_trading:
        return "trading"
    return "none"


def _recipient_scope(
    recipients: Sequence[str],
    *,
    organization_domain: str,
) -> str:
    return shared_recipient_scope(
        recipients,
        organization_domain=organization_domain,
        empty_value="none",
    )


def _external_recipient_count(
    recipients: Sequence[str],
    *,
    organization_domain: str,
) -> int:
    return shared_external_recipient_count(
        recipients,
        organization_domain=organization_domain,
    )


def _internal_recipient_count(
    recipients: Sequence[str],
    *,
    organization_domain: str,
) -> int:
    return shared_internal_recipient_count(
        recipients,
        organization_domain=organization_domain,
    )


def _is_internal_recipient(
    recipient: str,
    *,
    organization_domain: str,
) -> bool:
    return shared_is_internal_recipient(
        recipient,
        organization_domain=organization_domain,
    )


def _action_name(event: Any) -> str:
    if event.flags.is_forward:
        return "forward"
    if event.flags.is_escalation or event.event_type == "escalation":
        return "escalation"
    if event.flags.is_reply or event.event_type == "reply":
        return "reply"
    return event.event_type or "message"


def _coerce_row(base_row: Any) -> dict[str, Any]:
    if hasattr(base_row, "to_dict"):
        payload = dict(base_row.to_dict())
    elif isinstance(base_row, dict):
        payload = dict(base_row)
    else:
        payload = {}
    payload.setdefault("action_name", "message")
    payload.setdefault("action_actor_id", "__unknown__")
    payload.setdefault("action_event_type", "message")
    payload.setdefault("action_scope", "internal")
    payload.setdefault("action_review_path", "none")
    payload.setdefault("action_to_count", 1.0)
    payload.setdefault("action_cc_count", 0.0)
    payload.setdefault("action_bcc_count", 0.0)
    payload.setdefault("action_has_attachment", 0.0)
    payload.setdefault("action_is_forward", 0.0)
    payload.setdefault("action_is_reply", 0.0)
    payload.setdefault("action_is_escalation", 0.0)
    payload.setdefault("action_external_recipient_count", 0.0)
    payload.setdefault("action_internal_recipient_count", 1.0)
    payload.setdefault("action_consult_legal", 0.0)
    payload.setdefault("action_consult_trading", 0.0)
    return payload


def _load_world(*, source: str, source_dir: Path):
    normalized_source = source.strip().lower()
    if normalized_source == "enron":
        return load_enron_world(rosetta_dir=source_dir)
    if normalized_source == "company_history":
        return load_company_history_world(source_dir=source_dir)
    return load_mail_archive_world(source_dir=source_dir)


def _delta_as_count(predicted: float, current: float) -> int:
    return max(0, int(round(predicted - current)))


def _clamp_count(predicted: float, current: float, *, ceiling: int) -> int:
    return min(max(0, int(round(predicted - current))), max(0, ceiling))


def _default_device() -> str:
    import torch

    if torch.backends.mps.is_available():
        return "mps"
    if torch.cuda.is_available():
        return "cuda"
    return "cpu"


if __name__ == "__main__":
    main()
