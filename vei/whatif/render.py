from __future__ import annotations

from .models import (
    WhatIfEpisodeMaterialization,
    WhatIfEventSearchResult,
    WhatIfExperimentResult,
    WhatIfForecastResult,
    WhatIfLLMReplayResult,
    WhatIfReplaySummary,
    WhatIfResult,
    WhatIfWorld,
)


def _preview(text: str, *, limit: int = 280) -> str:
    stripped = " ".join(text.split())
    if len(stripped) <= limit:
        return stripped
    return stripped[: limit - 3].rstrip() + "..."


def render_world_summary(world: WhatIfWorld) -> str:
    lines = [
        f"# {world.source.title()} What-If Source",
        "",
        f"- Events: {world.summary.event_count}",
        f"- Threads: {world.summary.thread_count}",
        f"- Actors: {world.summary.actor_count}",
        f"- Custodians: {world.summary.custodian_count}",
    ]
    if world.summary.first_timestamp and world.summary.last_timestamp:
        lines.extend(
            [
                f"- Time range: {world.summary.first_timestamp} to {world.summary.last_timestamp}",
                "",
            ]
        )
    else:
        lines.append("")
    lines.append("## Supported Scenarios")
    for scenario in world.scenarios:
        lines.append(f"- `{scenario.scenario_id}`: {scenario.description}")
    return "\n".join(lines)


def render_result(result: WhatIfResult) -> str:
    lines = [
        f"# {result.scenario.title}",
        "",
        result.scenario.description,
        "",
        f"- Matched events: {result.matched_event_count}",
        f"- Affected threads: {result.affected_thread_count}",
        f"- Affected actors: {result.affected_actor_count}",
        f"- Blocked forwards: {result.blocked_forward_count}",
        f"- Blocked escalations: {result.blocked_escalation_count}",
        f"- Delayed assignments: {result.delayed_assignment_count}",
        "",
        f"Timeline impact: {result.timeline_impact}",
        "",
        "## Top Threads",
    ]
    if not result.top_threads:
        lines.append("- No matched threads.")
    else:
        for thread in result.top_threads:
            lines.append(
                f"- `{thread.thread_id}` {thread.subject} "
                f"({thread.affected_event_count} events, {thread.participant_count} participants)"
            )
    lines.extend(["", "## Top Actors"])
    if not result.top_actors:
        lines.append("- No matched actors.")
    else:
        for actor in result.top_actors:
            lines.append(
                f"- {actor.display_name} ({actor.actor_id}) "
                f"across {actor.affected_thread_count} threads"
            )
    lines.extend(["", "## Decision Branches"])
    for branch in result.decision_branches:
        lines.append(f"- {branch}")
    return "\n".join(lines)


def render_event_search(result: WhatIfEventSearchResult) -> str:
    lines = [
        "# What-If Event Search",
        "",
        f"- Matches: {result.match_count}",
        f"- Returned: {len(result.matches)}",
        f"- Truncated: {'yes' if result.truncated else 'no'}",
    ]
    if result.filters:
        lines.extend(["", "## Filters"])
        for key, value in result.filters.items():
            lines.append(f"- {key}: {value}")
    lines.extend(["", "## Events"])
    if not result.matches:
        lines.append("- No matching events.")
        return "\n".join(lines)
    for match in result.matches:
        reasons = ", ".join(match.reason_labels) or "none"
        lines.append(
            f"- `{match.event.event_id}` {match.event.timestamp} "
            f"{match.event.actor_id} -> "
            f"{', '.join(match.event.to_recipients) or match.event.target_id or '(none)'} | "
            f"{match.event.subject} | flags: {reasons}"
        )
    return "\n".join(lines)


def render_episode(materialization: WhatIfEpisodeMaterialization) -> str:
    lines = [
        "# What-If Episode Materialized",
        "",
        f"- Workspace: {materialization.workspace_root}",
        f"- Thread: `{materialization.thread_id}`",
        f"- Branch event: `{materialization.branch_event_id}`",
        f"- Branch actor: `{materialization.branch_event.actor_id}`",
        f"- Branch type: {materialization.branch_event.event_type}",
        f"- Seeded historical messages: {materialization.history_message_count}",
        f"- Scheduled future events: {materialization.future_event_count}",
        f"- Forecast risk score: {materialization.forecast.risk_score}",
    ]
    if materialization.baseline_future_preview:
        lines.extend(["", "## Baseline Future Preview"])
        for event in materialization.baseline_future_preview[:3]:
            lines.append(
                f"- `{event.event_id}` {event.event_type} from `{event.actor_id}`: {event.subject}"
            )
    return "\n".join(lines)


def render_replay(summary: WhatIfReplaySummary) -> str:
    lines = [
        "# What-If Replay",
        "",
        f"- Scheduled future events: {summary.scheduled_event_count}",
        f"- Delivered after tick: {summary.delivered_event_count}",
        f"- Current time: {summary.current_time_ms} ms",
        f"- Inbox count: {summary.inbox_count}",
        f"- Forecast risk score: {summary.forecast.risk_score}",
    ]
    if summary.top_subjects:
        lines.extend(["", "## Top Subjects"])
        for subject in summary.top_subjects:
            lines.append(f"- {subject}")
    if summary.baseline_future_preview:
        lines.extend(["", "## Baseline Preview"])
        for event in summary.baseline_future_preview[:3]:
            lines.append(
                f"- `{event.event_id}` {event.event_type} from `{event.actor_id}`: {event.subject}"
            )
    return "\n".join(lines)


def render_llm_result(result: WhatIfLLMReplayResult) -> str:
    lines = [
        "# LLM Counterfactual Replay",
        "",
        f"- Status: {result.status}",
        f"- Provider: {result.provider}",
        f"- Model: {result.model}",
        f"- Summary: {result.summary}",
        f"- Generated messages: {len(result.messages)}",
        f"- Delivered messages: {result.delivered_event_count}",
        f"- Inbox count: {result.inbox_count}",
    ]
    if result.notes:
        lines.extend(["", "## Notes"])
        for note in result.notes:
            lines.append(f"- {note}")
    if result.messages:
        lines.extend(["", "## Messages"])
        for message in result.messages:
            lines.append(
                f"- `{message.actor_id}` -> `{message.to}` after {message.delay_ms} ms: "
                f"{message.subject}"
            )
    return "\n".join(lines)


def render_forecast_result(result: WhatIfForecastResult) -> str:
    title = (
        "# E-JEPA Forecast" if result.backend == "e_jepa" else "# E-JEPA Proxy Forecast"
    )
    lines = [
        title,
        "",
        f"- Status: {result.status}",
        f"- Summary: {result.summary}",
        f"- Baseline risk: {result.baseline.risk_score}",
        f"- Predicted risk: {result.predicted.risk_score}",
        f"- Escalation delta: {result.delta.escalation_delta}",
        f"- External-send delta: {result.delta.external_event_delta}",
    ]
    if result.surprise_score is not None:
        lines.append(f"- Surprise score: {result.surprise_score}")
    if result.notes:
        lines.extend(["", "## Notes"])
        for note in result.notes:
            lines.append(f"- {note}")
    return "\n".join(lines)


def render_experiment(result: WhatIfExperimentResult) -> str:
    branch_event = result.materialization.branch_event
    original_recipient = (
        ", ".join(branch_event.to_recipients)
        or branch_event.target_id
        or "(none recorded)"
    )
    lines = [
        f"# {result.label}",
        "",
        f"- Selected thread: `{result.intervention.thread_id}`",
        f"- Branch event: `{result.intervention.branch_event_id}`",
        f"- Changed actor: `{branch_event.actor_id}`",
        f"- Historical event type: {branch_event.event_type}",
        f"- Historical subject: {branch_event.subject}",
        f"- Counterfactual prompt: {result.intervention.prompt}",
        f"- Baseline scheduled events: {result.baseline.scheduled_event_count}",
        f"- Baseline delivered events: {result.baseline.delivered_event_count}",
        f"- Baseline risk score: {result.baseline.forecast.risk_score}",
    ]
    lines.extend(
        [
            "",
            "## Changed Event",
            f"- When: {branch_event.timestamp}",
            f"- Historical sender: `{branch_event.actor_id}`",
            f"- Historical recipient: `{original_recipient}`",
            f"- Historical subject: {branch_event.subject}",
        ]
    )
    if branch_event.snippet:
        lines.append(f"- Historical excerpt: {_preview(branch_event.snippet)}")
    if result.materialization.baseline_future_preview:
        lines.extend(["", "## Historical Future"])
        for event in result.materialization.baseline_future_preview[:3]:
            lines.append(
                f"- `{event.event_id}` {event.event_type} from `{event.actor_id}`: {event.subject}"
            )
    if result.llm_result is not None:
        lines.extend(
            [
                "",
                "## Counterfactual Rollout",
                f"- Status: {result.llm_result.status}",
                f"- Summary: {result.llm_result.summary}",
                f"- Delivered messages: {result.llm_result.delivered_event_count}",
                f"- Inbox count: {result.llm_result.inbox_count}",
            ]
        )
        for message in result.llm_result.messages[:3]:
            lines.append(
                f"- `{message.actor_id}` -> `{message.to}` after {message.delay_ms} ms: {message.subject}"
            )
            lines.append(f"  { _preview(message.body_text, limit=180) }")
    if result.forecast_result is not None:
        lines.extend(
            [
                "",
                "## Predicted Outcome",
                f"- Status: {result.forecast_result.status}",
                f"- Backend: {result.forecast_result.backend}",
                f"- Summary: {result.forecast_result.summary}",
                f"- Predicted risk: {result.forecast_result.predicted.risk_score}",
                f"- External-send delta: {result.forecast_result.delta.external_event_delta}",
                f"- Escalation delta: {result.forecast_result.delta.escalation_delta}",
            ]
        )
    lines.extend(
        [
            "",
            "## Artifacts",
            f"- Result JSON: {result.artifacts.result_json_path}",
            f"- Overview Markdown: {result.artifacts.overview_markdown_path}",
        ]
    )
    if result.artifacts.llm_json_path is not None:
        lines.append(f"- LLM JSON: {result.artifacts.llm_json_path}")
    if result.artifacts.forecast_json_path is not None:
        lines.append(f"- Forecast JSON: {result.artifacts.forecast_json_path}")
    return "\n".join(lines)
