from __future__ import annotations

from .models import (
    WhatIfBusinessStateAssessment,
    WhatIfBusinessStateChange,
    WhatIfBenchmarkBuildResult,
    WhatIfBenchmarkEvalResult,
    WhatIfBenchmarkJudgeResult,
    WhatIfBenchmarkStudyResult,
    WhatIfBenchmarkTrainResult,
    WhatIfBranchCandidateResult,
    WhatIfDecisionScene,
    WhatIfEpisodeMaterialization,
    WhatIfEventSearchResult,
    WhatIfExperimentResult,
    WhatIfCounterfactualEstimateResult,
    WhatIfCaseContext,
    WhatIfLLMReplayResult,
    WhatIfPackRunResult,
    WhatIfPublicContext,
    WhatIfRankedExperimentResult,
    WhatIfReplaySummary,
    WhatIfResult,
    WhatIfSituationContext,
    WhatIfWorld,
)


def _preview(text: str, *, limit: int = 280) -> str:
    stripped = " ".join(text.split())
    if len(stripped) <= limit:
        return stripped
    return stripped[: limit - 3].rstrip() + "..."


def _public_context_lines(
    context: WhatIfPublicContext | None,
    *,
    max_financial: int = 4,
    max_news: int = 4,
) -> list[str]:
    if context is None:
        return []
    if (
        not context.financial_snapshots
        and not context.public_news_events
        and not context.stock_history
        and not context.credit_history
        and not context.ferc_history
    ):
        return []

    lines = [
        "## Public Company Context",
        f"- Financial checkpoints: {len(context.financial_snapshots)}",
        f"- Public news events: {len(context.public_news_events)}",
        f"- Market checkpoints: {len(context.stock_history)}",
        f"- Credit checkpoints: {len(context.credit_history)}",
        f"- Regulatory checkpoints: {len(context.ferc_history)}",
    ]
    for snapshot in context.financial_snapshots[-max(1, max_financial) :]:
        lines.append(f"- {snapshot.as_of[:10]} {snapshot.label}")
    for event in context.public_news_events[-max(1, max_news) :]:
        lines.append(f"- {event.timestamp[:10]} {event.headline}")
    for row in context.stock_history[-max(1, max_financial) :]:
        lines.append(f"- {row.as_of[:10]} close {row.close:.2f}")
    for event in context.credit_history[-max(1, max_news) :]:
        headline = event.headline or f"{event.agency} rating action"
        lines.append(f"- {event.as_of[:10]} {headline}")
    for event in context.ferc_history[-max(1, max_news) :]:
        lines.append(f"- {event.timestamp[:10]} {event.headline}")
    return ["", *lines]


def _business_state_assessment_lines(
    assessment: WhatIfBusinessStateAssessment | None,
    *,
    heading: str,
    max_items: int = 3,
) -> list[str]:
    if assessment is None:
        return []
    lines = ["", f"## {heading}", f"- Summary: {assessment.summary}"]
    for implication in assessment.implications[: max(1, max_items)]:
        lines.append(f"- {implication}")
    return lines


def _case_context_lines(
    context: WhatIfCaseContext | None,
    *,
    max_history: int = 3,
    max_records: int = 3,
) -> list[str]:
    if context is None:
        return []
    if not context.related_history and not context.records:
        return []
    lines = [
        "",
        "## Case Context",
        f"- Case id: {context.case_id}",
        f"- Related activity: {len(context.related_history)}",
        f"- Linked records: {len(context.records)}",
    ]
    for event in context.related_history[-max(1, max_history) :]:
        lines.append(
            f"- [{event.surface}] {event.timestamp[:10]} {event.actor_id}: {event.subject or event.thread_id}"
        )
    for record in context.records[: max(1, max_records)]:
        lines.append(
            f"- [{record.surface or record.provider}] {record.label}: {record.summary}"
        )
    return lines


def _situation_context_lines(
    context: WhatIfSituationContext | None,
    *,
    max_threads: int = 3,
    max_history: int = 3,
) -> list[str]:
    if context is None:
        return []
    if not context.related_threads and not context.related_history:
        return []
    lines = [
        "",
        "## Situation Context",
        f"- Situation id: {context.situation_id}",
        f"- Related threads: {len(context.related_threads)}",
        f"- Related activity: {len(context.related_history)}",
    ]
    for thread in context.related_threads[: max(1, max_threads)]:
        lines.append(f"- [{thread.surface}] {thread.subject or thread.thread_id}")
    for event in context.related_history[-max(1, max_history) :]:
        lines.append(
            f"- [{event.surface}] {event.timestamp[:10]} {event.actor_id}: {event.subject or event.thread_id}"
        )
    return lines


def _business_state_change_lines(
    change: WhatIfBusinessStateChange | None,
    *,
    heading: str = "Business State Change",
    max_items: int = 4,
) -> list[str]:
    if change is None:
        return []
    lines = [
        "",
        f"## {heading}",
        f"- Summary: {change.summary}",
        f"- Confidence: {change.confidence}",
        f"- Net effect score: {change.net_effect_score}",
    ]
    for impact in change.impacts[: max(1, max_items)]:
        lines.append(f"- {impact.summary}")
    for consequence in change.consequence_estimates[:3]:
        lines.append(f"- {consequence.summary}")
    return lines


def render_world_summary(world: WhatIfWorld) -> str:
    lines = [
        f"# {world.source.title()} What-If Source",
        "",
        f"- Events: {world.summary.event_count}",
        f"- Threads: {world.summary.thread_count}",
        f"- Cases: {len(world.cases)}",
        f"- Situation clusters: {len(world.situation_graph.clusters) if world.situation_graph else 0}",
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
    lines.extend(_public_context_lines(world.public_context))
    if lines[-1] != "":
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


def render_branch_candidates(result: WhatIfBranchCandidateResult) -> str:
    lines = [
        "# What-If Branch Candidates",
        "",
        f"- Returned: {result.returned_count}",
        f"- Truncated: {'yes' if result.truncated else 'no'}",
        "",
        "## Candidates",
    ]
    if not result.candidates:
        lines.append("- No branch candidates found.")
        return "\n".join(lines)
    for candidate in result.candidates:
        reasons = ", ".join(candidate.reasons) or "none"
        lines.append(
            f"- `{candidate.thread_id}` [{candidate.surface}] {candidate.subject} | "
            f"branch=`{candidate.branch_event_id}` | "
            f"history={candidate.history_event_count} | future={candidate.future_event_count} | "
            f"case={candidate.case_id or '(none)'} | "
            f"situation_surfaces={candidate.situation_surface_count} | "
            f"linked_records={candidate.linked_record_count} | "
            f"score={candidate.score} | reasons: {reasons}"
        )
    return "\n".join(lines)


def render_episode(materialization: WhatIfEpisodeMaterialization) -> str:
    lines = [
        "# What-If Episode Materialized",
        "",
        f"- Workspace: {materialization.workspace_root}",
        f"- Thread: `{materialization.thread_id}`",
        f"- Case: `{materialization.case_id or materialization.thread_id}`",
        f"- Surface: {materialization.surface}",
        f"- Branch event: `{materialization.branch_event_id}`",
        f"- Branch actor: `{materialization.branch_event.actor_id}`",
        f"- Branch type: {materialization.branch_event.event_type}",
        f"- Seeded historical messages: {materialization.history_message_count}",
        f"- Scheduled future events: {materialization.future_event_count}",
        f"- Forecast risk score: {materialization.forecast.risk_score}",
    ]
    lines.extend(_public_context_lines(materialization.public_context))
    lines.extend(_case_context_lines(materialization.case_context))
    lines.extend(_situation_context_lines(materialization.situation_context))
    lines.extend(
        _business_state_assessment_lines(
            materialization.historical_business_state,
            heading="Recorded Business State",
        )
    )
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
        f"- Surface: {summary.surface}",
        f"- Scheduled future events: {summary.scheduled_event_count}",
        f"- Delivered after tick: {summary.delivered_event_count}",
        f"- Current time: {summary.current_time_ms} ms",
        f"- Inbox count: {summary.inbox_count}",
        f"- Visible items: {summary.visible_item_count}",
        f"- Forecast risk score: {summary.forecast.risk_score}",
    ]
    if summary.top_items:
        lines.extend(["", "## Visible Items"])
        for item in summary.top_items:
            lines.append(f"- {item}")
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
        f"- Generated actions: {len(result.messages)}",
        f"- Delivered actions: {result.delivered_event_count}",
        f"- Inbox count: {result.inbox_count}",
    ]
    if result.notes:
        lines.extend(["", "## Notes"])
        for note in result.notes:
            lines.append(f"- {note}")
    if result.messages:
        lines.extend(["", "## Actions"])
        for message in result.messages:
            lines.append(
                f"- `{message.surface}` `{message.actor_id}` -> `{message.to}` after {message.delay_ms} ms: "
                f"{message.subject}"
            )
    return "\n".join(lines)


def render_forecast_result(result: WhatIfCounterfactualEstimateResult) -> str:
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
    if (
        result.baseline.stock_return_5d is not None
        or result.predicted.stock_return_5d is not None
    ):
        lines.append(
            f"- Stock return (5d): {result.baseline.stock_return_5d} -> {result.predicted.stock_return_5d}"
        )
    if (
        result.baseline.credit_action_30d is not None
        or result.predicted.credit_action_30d is not None
    ):
        lines.append(
            f"- Credit action (30d): {result.baseline.credit_action_30d} -> {result.predicted.credit_action_30d}"
        )
    if (
        result.baseline.ferc_action_180d is not None
        or result.predicted.ferc_action_180d is not None
    ):
        lines.append(
            f"- FERC action (180d): {result.baseline.ferc_action_180d} -> {result.predicted.ferc_action_180d}"
        )
    if result.surprise_score is not None:
        lines.append(f"- Surprise score: {result.surprise_score}")
    if result.notes:
        lines.extend(["", "## Notes"])
        for note in result.notes:
            lines.append(f"- {note}")
    lines.extend(_business_state_change_lines(result.business_state_change))
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
        if (
            result.forecast_result.baseline.stock_return_5d is not None
            or result.forecast_result.predicted.stock_return_5d is not None
        ):
            lines.append(
                "- Stock return (5d): "
                f"{result.forecast_result.baseline.stock_return_5d} -> "
                f"{result.forecast_result.predicted.stock_return_5d}"
            )
        if (
            result.forecast_result.baseline.credit_action_30d is not None
            or result.forecast_result.predicted.credit_action_30d is not None
        ):
            lines.append(
                "- Credit action (30d): "
                f"{result.forecast_result.baseline.credit_action_30d} -> "
                f"{result.forecast_result.predicted.credit_action_30d}"
            )
        if (
            result.forecast_result.baseline.ferc_action_180d is not None
            or result.forecast_result.predicted.ferc_action_180d is not None
        ):
            lines.append(
                "- FERC action (180d): "
                f"{result.forecast_result.baseline.ferc_action_180d} -> "
                f"{result.forecast_result.predicted.ferc_action_180d}"
            )
        lines.extend(
            _business_state_change_lines(result.forecast_result.business_state_change)
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


def render_ranked_experiment(result: WhatIfRankedExperimentResult) -> str:
    branch = result.materialization.branch_event
    lines = [
        f"# {result.label}",
        "",
        f"- Objective: {result.objective_pack.title}",
        f"- Historical subject: {branch.subject}",
        f"- Recommended candidate: {result.recommended_candidate_label or '(none)'}",
        f"- Baseline delivered events: {result.baseline.delivered_event_count}",
        "",
        "## Ranked Candidates",
    ]
    if not result.candidates:
        lines.append("- No candidates were scored.")
        return "\n".join(lines)
    for candidate in result.candidates:
        lines.extend(
            [
                f"- Rank {candidate.rank}: {candidate.intervention.label}",
                f"  Score {candidate.outcome_score.overall_score} across {candidate.rollout_count} rollouts",
                f"  {candidate.reason}",
                (
                    f"  Business state: {candidate.business_state_change.summary}"
                    if candidate.business_state_change is not None
                    else "  Business state: not available"
                ),
                (
                    f"  Signals: exposure={candidate.average_outcome_signals.exposure_risk}, "
                    f"delay={candidate.average_outcome_signals.delay_risk}, "
                    f"relationship={candidate.average_outcome_signals.relationship_protection}"
                ),
            ]
        )
        if candidate.shadow is not None:
            lines.append(
                f"  Shadow {candidate.shadow.backend}: {candidate.shadow.outcome_score.overall_score}"
            )
    lines.extend(
        [
            "",
            "## Artifacts",
            f"- Result JSON: {result.artifacts.result_json_path}",
            f"- Overview Markdown: {result.artifacts.overview_markdown_path}",
        ]
    )
    return "\n".join(lines)


def render_research_pack_run(result: WhatIfPackRunResult) -> str:
    lines = [
        f"# {result.pack.title}",
        "",
        result.pack.summary,
        "",
        f"- Pack id: `{result.pack.pack_id}`",
        f"- Integrated backends: {', '.join(result.integrated_backends)}",
        f"- Pilot backends: {', '.join(result.pilot_backends)}",
        (
            "- Dataset rows: "
            f"historical={result.dataset.historical_row_count}, "
            f"evaluation={result.dataset.evaluation_row_count}"
        ),
        (
            f"- Hypothesis pass rate: {result.hypothesis_pass_rate:.3f} "
            f"({result.hypothesis_pass_count}/{result.hypothesis_total_count})"
        ),
        "",
        "## Cases",
    ]
    for case in result.cases:
        lines.extend(
            [
                f"### {case.case.title}",
                f"- Event: `{case.case.event_id}`",
                f"- Thread: `{case.materialization.thread_id}`",
                f"- Historical subject: {case.materialization.branch_event.subject}",
            ]
        )
        for objective in case.objectives:
            lines.extend(
                [
                    "",
                    f"#### {objective.objective_pack.title}",
                    f"- Expected order matched: {'yes' if objective.expected_order_ok else 'no'}",
                    (
                        "- Recommended by LLM rollouts: "
                        f"{objective.recommended_candidate_label or '(none)'}"
                    ),
                ]
            )
            for backend, label in objective.backend_recommendations.items():
                lines.append(f"- {backend}: {label}")
            for candidate in objective.candidates:
                lines.extend(
                    [
                        f"- Rank {candidate.rank}: {candidate.candidate.label}",
                        f"  Expected: {candidate.expected_hypothesis}",
                        (
                            "  Score "
                            f"{candidate.outcome_score.overall_score} "
                            f"with stability {candidate.rank_stability:.3f}"
                        ),
                    ]
                )
                for backend_score in candidate.backend_scores:
                    lines.append(
                        "  "
                        f"{backend_score.backend}: rank {backend_score.rank}, "
                        f"score {backend_score.outcome_score.overall_score}, "
                        f"status {backend_score.status}"
                    )
        lines.append("")
    lines.extend(
        [
            "## Artifacts",
            f"- Result JSON: {result.artifacts.result_json_path}",
            f"- Scoreboard Markdown: {result.artifacts.overview_markdown_path}",
            f"- Dataset root: {result.artifacts.dataset_root}",
            f"- Pilot note: {result.artifacts.pilot_markdown_path}",
        ]
    )
    return "\n".join(lines)


def render_benchmark_build(result: WhatIfBenchmarkBuildResult) -> str:
    lines = [
        f"# {result.label}",
        "",
        "- Benchmark type: branch-point ranking v2",
        f"- Held-out pack: `{result.heldout_pack_id}`",
        f"- Train rows: {result.dataset.split_row_counts.get('train', 0)}",
        f"- Validation rows: {result.dataset.split_row_counts.get('validation', 0)}",
        f"- Test rows: {result.dataset.split_row_counts.get('test', 0)}",
        f"- Held-out cases: {len(result.cases)}",
        "",
        "## Held-Out Cases",
    ]
    for case in result.cases:
        lines.extend(
            [
                f"- {case.title}",
                f"  Event `{case.event_id}` in thread `{case.thread_id}`",
                f"  Family: {case.case_family}",
                f"  Candidates: {', '.join(candidate.label for candidate in case.candidates)}",
                "  Public context: "
                f"{len(case.public_context.financial_snapshots) if case.public_context else 0} financial, "
                f"{len(case.public_context.public_news_events) if case.public_context else 0} news",
            ]
        )
    lines.extend(
        [
            "",
            "## Artifacts",
            f"- Build manifest: {result.artifacts.manifest_path}",
            f"- Held-out cases: {result.artifacts.heldout_cases_path}",
            f"- Judge template: {result.artifacts.judge_template_path}",
            f"- Audit template: {result.artifacts.audit_template_path}",
            f"- Dossiers: {result.artifacts.dossier_root}",
        ]
    )
    return "\n".join(lines)


def render_benchmark_judge(result: WhatIfBenchmarkJudgeResult) -> str:
    lines = [
        f"# {result.judge_model}",
        "",
        f"- Build root: {result.build_root}",
        f"- Judged case-objectives: {len(result.judgments)}",
        f"- Audit queue: {len(result.audit_queue)}",
    ]
    if result.notes:
        lines.extend(["", "## Notes"])
        for note in result.notes:
            lines.append(f"- {note}")
    lines.extend(["", "## Judgments"])
    for judgment in result.judgments:
        lines.append(
            f"- {judgment.case_id} / {judgment.objective_pack_id}: "
            f"{', '.join(judgment.ordered_candidate_ids)}"
        )
    lines.extend(
        [
            "",
            "## Artifacts",
            f"- Judge result: {result.artifacts.result_path}",
            f"- Audit queue: {result.artifacts.audit_queue_path}",
        ]
    )
    return "\n".join(lines)


def render_benchmark_train(result: WhatIfBenchmarkTrainResult) -> str:
    lines = [
        f"# {result.model_id}",
        "",
        f"- Train loss: {result.train_loss}",
        f"- Validation loss: {result.validation_loss}",
        f"- Epochs: {result.epoch_count}",
        f"- Train rows: {result.train_row_count}",
        f"- Validation rows: {result.validation_row_count}",
    ]
    if result.notes:
        lines.extend(["", "## Notes"])
        for note in result.notes:
            lines.append(f"- {note}")
    lines.extend(
        [
            "",
            "## Artifacts",
            f"- Model: {result.artifacts.model_path}",
            f"- Metadata: {result.artifacts.metadata_path}",
            f"- Train result: {result.artifacts.train_result_path}",
        ]
    )
    return "\n".join(lines)


def render_benchmark_eval(result: WhatIfBenchmarkEvalResult) -> str:
    metrics = result.observed_metrics
    lines = [
        f"# {result.model_id}",
        "",
        "## Observed Future Forecasting",
        f"- AUROC any external spread: {metrics.auroc_any_external_spread}",
        f"- Brier any external spread: {metrics.brier_any_external_spread}",
        f"- Calibration error: {metrics.calibration_error_any_external_spread}",
        "",
        "## Counterfactual Ranking",
        (
            "- Dominance checks: "
            f"{result.dominance_summary.passed_checks}/{result.dominance_summary.total_checks} "
            f"({result.dominance_summary.pass_rate:.3f})"
        ),
        f"- Judged rankings: {result.judge_summary.judgment_count if result.judge_summary.available else 0}",
        f"- Audit records: {result.audit_summary.completed_count if result.audit_summary.available else 0}",
    ]
    if metrics.evidence_head_mae:
        lines.extend(["", "## Evidence Head Error"])
        for name, value in sorted(metrics.evidence_head_mae.items()):
            lines.append(f"- {name}: {value}")
    if metrics.business_head_mae:
        lines.extend(["", "## Business Head Error"])
        for name, value in sorted(metrics.business_head_mae.items()):
            lines.append(f"- {name}: {value}")
    if metrics.objective_score_mae:
        lines.extend(["", "## Objective Score Error"])
        for name, value in sorted(metrics.objective_score_mae.items()):
            lines.append(f"- {name}: {value}")
    if result.judge_summary.available:
        lines.extend(
            [
                "",
                "## Judge Summary",
                f"- Top-1 agreement: {result.judge_summary.top1_agreement}",
                f"- Pairwise accuracy: {result.judge_summary.pairwise_accuracy}",
                f"- Kendall tau: {result.judge_summary.kendall_tau}",
                f"- Uncertain rankings: {result.judge_summary.uncertainty_count}",
                f"- Low-confidence rankings: {result.judge_summary.low_confidence_count}",
            ]
        )
    if result.audit_summary.available:
        lines.extend(
            [
                "",
                "## Audit Summary",
                f"- Audit queue: {result.audit_summary.queue_count}",
                f"- Completed audits: {result.audit_summary.completed_count}",
                f"- Agreement rate: {result.audit_summary.agreement_rate}",
            ]
        )
    if result.panel_summary.available:
        lines.extend(
            [
                "",
                "## Legacy Panel Summary",
                f"- Panel judgments: {result.panel_summary.judgment_count}",
                f"- Top-1 agreement: {result.panel_summary.top1_agreement}",
                f"- Pairwise accuracy: {result.panel_summary.pairwise_accuracy}",
                f"- Kendall tau: {result.panel_summary.kendall_tau}",
            ]
        )
    if result.rollout_stress_summary.available:
        lines.extend(
            [
                "",
                "## Rollout Stress Test",
                (
                    "- Rollout agreement: "
                    f"{result.rollout_stress_summary.agreement_count}/"
                    f"{result.rollout_stress_summary.compared_case_objectives} "
                    f"({result.rollout_stress_summary.agreement_rate})"
                ),
            ]
        )
    lines.extend(["", "## Held-Out Cases"])
    for case in result.cases:
        lines.append(f"### {case.case.title}")
        for objective in case.objectives:
            lines.extend(
                [
                    f"- {objective.objective_pack.title}: {objective.recommended_candidate_label}",
                    f"  Expected order matched: {'yes' if objective.expected_order_ok else 'no'}",
                    f"  Candidate count: {len(objective.candidates)}",
                ]
            )
    lines.extend(
        [
            "",
            "## Artifacts",
            f"- Eval result: {result.artifacts.eval_result_path}",
            f"- Predictions: {result.artifacts.prediction_jsonl_path}",
        ]
    )
    return "\n".join(lines)


def render_benchmark_study(result: WhatIfBenchmarkStudyResult) -> str:
    lines = [
        f"# {result.label}",
        "",
        f"- Build root: {result.build_root}",
        f"- Models: {', '.join(result.models)}",
        f"- Seeds: {', '.join(str(seed) for seed in result.seeds)}",
        f"- Total runs: {len(result.runs)}",
    ]
    if result.ranked_model_ids:
        lines.extend(
            [
                "",
                "## Ranked Models",
                f"- {', '.join(result.ranked_model_ids)}",
            ]
        )
    lines.extend(["", "## Summary"])
    for summary in result.summaries:
        lines.extend(
            [
                f"- `{summary.model_id}` runs={summary.run_count} "
                f"dominance={summary.dominance_pass_rate.mean:.3f}"
                f"+/-{summary.dominance_pass_rate.std:.3f} "
                f"auroc={summary.observed_auroc_any_external_spread.mean:.3f}",
            ]
        )
        if summary.judge_top1_agreement is not None:
            lines.append(f"  judge_top1={summary.judge_top1_agreement.mean:.3f}")
        if summary.objective_pass_rates:
            objective_bits = ", ".join(
                f"{name}={metric.mean:.3f}"
                for name, metric in sorted(summary.objective_pass_rates.items())
            )
            lines.append(f"  objectives: {objective_bits}")
    lines.extend(
        [
            "",
            "## Artifacts",
            f"- Study result: {result.artifacts.result_path}",
            f"- Study overview: {result.artifacts.overview_path}",
        ]
    )
    return "\n".join(lines)


def render_decision_scene(scene: WhatIfDecisionScene) -> str:
    lines = [
        f"# Decision Scene — {scene.thread_subject or scene.thread_id}",
        "",
        f"- Source: {scene.source}",
        f"- Organization: {scene.organization_name} ({scene.organization_domain})",
        f"- Surface: {scene.surface}",
        f"- Thread: `{scene.thread_id}`",
        f"- Branch event: `{scene.branch_event_id}`",
        f"- History messages: {scene.history_message_count}",
        f"- Future events: {scene.future_event_count}",
    ]
    if scene.content_notice:
        lines.extend(["", f"> {scene.content_notice}"])
    if scene.branch_summary:
        lines.extend(["", "## Branch Point", scene.branch_summary])
    if scene.historical_action_summary:
        lines.append(scene.historical_action_summary)
    if scene.historical_outcome_summary:
        lines.append(scene.historical_outcome_summary)
    if scene.stakes_summary:
        lines.extend(["", "## Stakes", scene.stakes_summary])
    if scene.decision_question:
        lines.extend(["", "## Decision Question", scene.decision_question])
    if scene.history_preview:
        lines.extend(["", "## History Preview"])
        for event in scene.history_preview:
            lines.append(
                f"- [{event.surface}] {event.timestamp[:10]} "
                f"{event.actor_id}: {event.subject or event.thread_id}"
            )
    if scene.historical_future_preview:
        lines.extend(["", "## Historical Future Preview"])
        for event in scene.historical_future_preview:
            lines.append(
                f"- [{event.surface}] {event.timestamp[:10]} "
                f"{event.actor_id}: {event.subject or event.thread_id}"
            )
    if scene.candidate_options:
        lines.extend(["", "## Candidate Options"])
        for option in scene.candidate_options:
            lines.append(f"### {option.label} (`{option.option_id}`)")
            lines.append(option.summary)
    lines.extend(_public_context_lines(scene.public_context))
    lines.extend(
        _business_state_assessment_lines(
            scene.historical_business_state,
            heading="Historical Business State",
        )
    )
    lines.extend(_case_context_lines(scene.case_context))
    lines.extend(_situation_context_lines(scene.situation_context))
    return "\n".join(lines)
