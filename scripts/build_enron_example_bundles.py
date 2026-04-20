from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from vei.context.api import (
    build_canonical_history_bundle_from_rows,
    ContextSnapshot,
    canonical_history_paths,
    load_canonical_history_bundle,
    write_canonical_history_bundle,
    write_canonical_history_sidecars,
)
from vei.project_settings import resolve_llm_defaults
from vei.context.api import resolve_world_public_context
from vei.whatif.analysis import select_specific_event
from vei.whatif.artifacts import render_experiment_overview
from vei.whatif.api import (
    default_forecast_backend,
    list_supported_scenarios,
    run_counterfactual_experiment,
)
from vei.whatif.artifact_validation import validate_packaged_example_bundle
from vei.whatif._branch_context import build_branch_context
from vei.whatif.cases import assign_case_ids, build_case_summaries
from vei.whatif.counterfactual import (
    _attach_business_state_to_forecast_result,
    _baseline_tick_ms,
)
from vei.whatif.corpus._aggregation import build_actor_profiles, build_thread_summaries
from vei.whatif.corpus._enron import CONTENT_NOTICE, ENRON_DOMAIN, build_event
from vei.whatif.dynamics_bridge import run_dynamics_counterfactual
from vei.whatif.episode import materialize_episode, replay_episode_baseline
from vei.whatif.filenames import (
    EPISODE_MANIFEST_FILE,
    EXPERIMENT_RESULT_FILE,
    HEURISTIC_FORECAST_FILE,
    LLM_RESULT_FILE,
    PUBLIC_CONTEXT_FILE,
    REFERENCE_FORECAST_FILE,
    STUDIO_SAVED_FORECAST_FILES,
    WORKSPACE_DIRECTORY,
)
from vei.whatif._enron_history import build_enron_canonical_rows
from vei.whatif.models import (
    WhatIfExperimentArtifacts,
    WhatIfExperimentResult,
    WhatIfInterventionSpec,
    WhatIfLLMReplayResult,
    WhatIfWorld,
    WhatIfWorldSummary,
)
from vei.whatif.situations import build_situation_graph

try:
    from scripts.build_enron_business_state_example import build_example
    from scripts.enron_example_specs import (
        bundle_specs,
        load_case_register,
        rosetta_dir,
        spec_by_bundle_slug,
    )
    from scripts.package_enron_master_agreement_example import package_example
    from scripts.render_enron_timeline_asset import (
        render_timeline_image,
        render_timeline_markdown,
    )
except ModuleNotFoundError:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from build_enron_business_state_example import build_example
    from enron_example_specs import (
        bundle_specs,
        load_case_register,
        rosetta_dir,
        spec_by_bundle_slug,
    )
    from package_enron_master_agreement_example import package_example
    from render_enron_timeline_asset import (
        render_timeline_image,
        render_timeline_markdown,
    )


DEFAULT_ARTIFACTS_ROOT = Path("_vei_out/whatif_repo_examples")
MASTER_EVENT_ID = "enron_bcda1b925800af8c"
MASTER_THREAD_ID = "thr_e565b47423d035c9"
ENRON_FIXTURE_WINDOW = (
    "1998-01-01T00:00:00Z",
    "2001-12-31T23:59:59Z",
)
ENRON_STORY_OVERVIEW_FILE = "enron_story_overview.md"
ENRON_STORY_MANIFEST_FILE = "enron_story_manifest.json"
ENRON_EXPORTS_PREVIEW_FILE = "enron_exports_preview.json"
ENRON_PRESENTATION_MANIFEST_FILE = "enron_presentation_manifest.json"
ENRON_PRESENTATION_GUIDE_FILE = "enron_presentation_guide.md"


def _event_id_map() -> dict[str, str]:
    result = {"master_agreement": MASTER_EVENT_ID}
    register = load_case_register()
    for case_id, payload in register.items():
        event_id = str(payload.get("event_id") or "").strip()
        if event_id:
            result[case_id] = event_id
    pg_e_context = register.get("pg_e_power_deal_context")
    if isinstance(pg_e_context, dict):
        event_id = str(pg_e_context.get("event_id") or "").strip()
        if event_id:
            result["pg_e_power_deal"] = event_id
    return result


def _thread_id_map() -> dict[str, str]:
    result = {"master_agreement": MASTER_THREAD_ID}
    register = load_case_register()
    for case_id, payload in register.items():
        thread_id = str(payload.get("thread_id") or "").strip()
        if thread_id:
            result[case_id] = thread_id
    pg_e_context = register.get("pg_e_power_deal_context")
    if isinstance(pg_e_context, dict):
        thread_id = str(pg_e_context.get("thread_id") or "").strip()
        if thread_id:
            result["pg_e_power_deal"] = thread_id
    return result


def _load_thread_world(case_id: str) -> WhatIfWorld:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - guarded by extras
        raise RuntimeError("pyarrow is required for Enron bundle builds") from exc

    thread_id = _thread_id_map()[case_id]
    base = rosetta_dir()
    metadata_path = base / "enron_rosetta_events_metadata.parquet"
    metadata_rows = pq.read_table(
        metadata_path,
        columns=[
            "event_id",
            "timestamp",
            "actor_id",
            "target_id",
            "event_type",
            "thread_task_id",
            "artifacts",
        ],
        filters=[("thread_task_id", "=", thread_id)],
    ).to_pylist()
    events = [
        event
        for event in (build_event(row, "") for row in metadata_rows)
        if event is not None
    ]
    events.sort(key=lambda item: (item.timestamp_ms, item.event_id))
    events = assign_case_ids(events)
    threads = build_thread_summaries(events, organization_domain=ENRON_DOMAIN)
    actors = build_actor_profiles(events, organization_domain=ENRON_DOMAIN)
    cases = build_case_summaries(events)
    situation_graph = build_situation_graph(
        threads=threads,
        cases=cases,
        events=events,
    )
    summary = WhatIfWorldSummary(
        source="enron",
        organization_name="Enron Corporation",
        organization_domain=ENRON_DOMAIN,
        event_count=len(events),
        thread_count=len(threads),
        actor_count=len(actors),
        custodian_count=len(
            {
                custodian
                for actor in actors
                for custodian in actor.custodian_ids
                if custodian
            }
        ),
        first_timestamp=events[0].timestamp if events else "",
        last_timestamp=events[-1].timestamp if events else "",
        event_type_counts=dict(Counter(event.event_type for event in events)),
        key_actor_ids=[actor.actor_id for actor in actors[:5]],
    )
    public_context = (
        resolve_world_public_context(
            source="enron",
            source_dir=base,
            organization_name=summary.organization_name,
            organization_domain=summary.organization_domain,
            window_start=ENRON_FIXTURE_WINDOW[0],
            window_end=ENRON_FIXTURE_WINDOW[1],
        )
        if events
        else None
    )
    return WhatIfWorld(
        source="enron",
        source_dir=base,
        summary=summary,
        scenarios=list_supported_scenarios(),
        actors=actors,
        threads=threads,
        cases=cases,
        events=events,
        situation_graph=situation_graph,
        metadata={"content_notice": CONTENT_NOTICE},
        public_context=public_context,
    )


def _forecast_filename(bundle_root: Path) -> str:
    experiment_payload = json.loads(
        (bundle_root / EXPERIMENT_RESULT_FILE).read_text(encoding="utf-8")
    )
    artifacts = experiment_payload.get("artifacts")
    if isinstance(artifacts, dict):
        forecast_path = str(artifacts.get("forecast_json_path") or "").strip()
        if forecast_path:
            return Path(forecast_path).name
    for filename in STUDIO_SAVED_FORECAST_FILES:
        if (bundle_root / filename).exists():
            return filename
    raise FileNotFoundError(f"forecast result missing from {bundle_root}")


def _context_counts(
    public_context_payload: dict[str, object],
) -> tuple[int, int, int, int, int]:
    return (
        len(list(public_context_payload.get("financial_snapshots") or [])),
        len(list(public_context_payload.get("public_news_events") or [])),
        len(list(public_context_payload.get("stock_history") or [])),
        len(list(public_context_payload.get("credit_history") or [])),
        len(list(public_context_payload.get("ferc_history") or [])),
    )


def _bundle_ui_command(bundle_root: Path) -> str:
    return "\n".join(
        [
            "vei ui serve \\",
            f"  --root {bundle_root / WORKSPACE_DIRECTORY} \\",
            "  --host 127.0.0.1 \\",
            "  --port 3055",
        ]
    )


def _bundle_story_context(spec, bundle_root: Path) -> dict[str, Any]:
    workspace_root = bundle_root / WORKSPACE_DIRECTORY
    manifest_payload = json.loads(
        (workspace_root / EPISODE_MANIFEST_FILE).read_text(encoding="utf-8")
    )
    canonical_bundle = load_canonical_history_bundle(workspace_root)
    experiment_payload = json.loads(
        (bundle_root / EXPERIMENT_RESULT_FILE).read_text(encoding="utf-8")
    )
    comparison_payload = json.loads(
        (bundle_root / "whatif_business_state_comparison.json").read_text(
            encoding="utf-8"
        )
    )
    comparison_public_summary = comparison_payload.get("public_summary") or {}
    public_context_payload = json.loads(
        (workspace_root / PUBLIC_CONTEXT_FILE).read_text(encoding="utf-8")
    )
    history_count = int(manifest_payload.get("history_message_count") or 0)
    future_count = int(manifest_payload.get("future_event_count") or 0)
    branch_timestamp = str(manifest_payload.get("branch_timestamp") or "")[:10]
    branch_ts_ms = int(manifest_payload.get("branch_timestamp_ms") or 0)
    branch_event_id = str(manifest_payload.get("branch_event_id") or "")
    forecast_payload = experiment_payload.get("forecast_result") or {}
    business_change = forecast_payload.get("business_state_change") or {}
    top_candidate = (
        comparison_payload.get("candidates", [{}])[0]
        if comparison_payload.get("candidates")
        else {}
    )
    top_public_candidate = (
        comparison_public_summary.get("candidates", [{}])
        if isinstance(comparison_public_summary.get("candidates"), list)
        else [{}]
    )[0]
    forecast_filename = _forecast_filename(bundle_root)
    (
        financial_count,
        news_count,
        stock_count,
        credit_count,
        ferc_count,
    ) = _context_counts(public_context_payload)
    source_family_labels, domain_labels = _history_dimension_labels(
        canonical_bundle,
        branch_event_id=branch_event_id,
        branch_ts_ms=branch_ts_ms,
    )
    return {
        "workspace_root": workspace_root,
        "history_count": history_count,
        "future_count": future_count,
        "branch_timestamp": branch_timestamp,
        "branch_ts_ms": branch_ts_ms,
        "branch_event_id": branch_event_id,
        "forecast_payload": forecast_payload,
        "business_change": business_change,
        "top_candidate": top_candidate,
        "top_public_candidate": top_public_candidate,
        "comparison_public_summary": comparison_public_summary,
        "forecast_filename": forecast_filename,
        "financial_count": financial_count,
        "news_count": news_count,
        "stock_count": stock_count,
        "credit_count": credit_count,
        "ferc_count": ferc_count,
        "source_family_labels": source_family_labels,
        "domain_labels": domain_labels,
        "ui_command": _bundle_ui_command(bundle_root),
    }


def _write_bundle_readme(
    spec,
    bundle_root: Path,
    *,
    context: dict[str, Any] | None = None,
) -> None:
    bundle_context = context or _bundle_story_context(spec, bundle_root)
    history_count = int(bundle_context["history_count"])
    future_count = int(bundle_context["future_count"])
    branch_timestamp = str(bundle_context["branch_timestamp"])
    financial_count = int(bundle_context["financial_count"])
    news_count = int(bundle_context["news_count"])
    stock_count = int(bundle_context["stock_count"])
    credit_count = int(bundle_context["credit_count"])
    ferc_count = int(bundle_context["ferc_count"])
    source_family_labels = list(bundle_context["source_family_labels"])
    domain_labels = list(bundle_context["domain_labels"])
    forecast_filename = str(bundle_context["forecast_filename"])
    top_candidate = dict(bundle_context["top_candidate"])
    comparison_public_summary = dict(bundle_context["comparison_public_summary"])
    top_public_candidate = dict(bundle_context["top_public_candidate"])
    sibling_lines = [
        f"- [{other.title}](../{other.bundle_slug}/README.md)"
        for other in bundle_specs()
        if other.bundle_slug != spec.bundle_slug
    ]
    timeline_lines: list[str] = []
    if spec.bundle_slug == "enron-master-agreement-public-context":
        timeline_lines = [
            "",
            "## Bankruptcy Arc Timeline",
            "",
            "See [timeline_arc.md](timeline_arc.md) for the dated public timeline and [the rendered timeline image](../../assets/enron-whatif/enron-bankruptcy-arc-timeline.png) for the visual version that places this branch beside the PG&E, California, and Watkins follow-up examples.",
        ]
    story_lines: list[str] = []
    for paragraph in spec.story_lines:
        story_lines.extend([paragraph, ""])
    action_lines = [
        f"- **{candidate.label}**: {candidate.explanation}"
        for candidate in spec.candidates
    ]
    outcome_lines = [
        (
            f"- {row['label']}: {row['summary']} "
            f"({row['baseline_value']} -> {row['predicted_value']})"
        )
        for row in top_public_candidate.get("public_outcomes", [])
    ]
    readme = "\n".join(
        [
            f"# {spec.title}",
            "",
            spec.lead,
            "",
            "## Open It In Studio",
            "",
            "```bash",
            str(bundle_context["ui_command"]),
            "```",
            "",
            "Open `http://127.0.0.1:3055`.",
            "",
            (
                f"![Saved forecast panel](../../assets/enron-whatif/"
                f"{spec.screenshot_stem}-forecast.png)"
            ),
            "",
            (
                f"![Saved ranking panel](../../assets/enron-whatif/"
                f"{spec.screenshot_stem}-ranking.png)"
            ),
            "",
            "## Branch Point",
            "",
            f"- {spec.branch_point}",
            "",
            "## What Actually Happened",
            "",
            f"- {spec.actual_happened}",
            "",
            "## Actions We Can Take",
            "",
            *action_lines,
            "",
            "## Predicted Effect On The Company",
            "",
            (
                f"- Recorded future events after the historical branch: "
                f"{comparison_public_summary.get('recorded_future_event_count') or future_count}"
            ),
            f"- Current top-ranked action: {top_candidate.get('label') or '(none)'}",
            (
                "- Short readout: "
                f"{top_public_candidate.get('short_explanation') or 'Saved forecast summary.'}"
            ),
            *outcome_lines,
            "",
            "## Why This Branch Matters",
            "",
            *story_lines,
            "## Bundle Facts",
            "",
            (
                f"- Saved branch scene: {history_count} prior events and "
                f"{future_count} recorded future events"
            ),
            (
                f"- Public-company slice at {branch_timestamp}: "
                f"{financial_count} financial checkpoints, {news_count} public news items, "
                f"{stock_count} market checkpoints, {credit_count} credit checkpoints, "
                f"and {ferc_count} regulatory checkpoints"
            ),
            (
                "- Prior timeline source families: "
                f"{', '.join(source_family_labels) if source_family_labels else 'unknown'}"
            ),
            (
                "- Prior timeline domains: "
                f"{', '.join(domain_labels) if domain_labels else 'unknown'}"
            ),
            f"- Bundle role: `{spec.role}`",
            f"- Saved LLM path: {spec.primary_prompt}",
            f"- Saved forecast file: `{forecast_filename}`",
            "",
            "## Saved Files",
            "",
            "- `workspace/`: saved workspace you can open in Studio",
            "- `whatif_experiment_overview.md`: short human-readable run summary",
            "- `whatif_experiment_result.json`: saved combined result for the example bundle",
            "- `whatif_llm_result.json`: bounded message-path result",
            f"- `{forecast_filename}`: saved forecast result",
            "- `whatif_business_state_comparison.md`: ranked comparison in business language",
            "- `whatif_business_state_comparison.json`: structured comparison payload",
            f"- `{ENRON_STORY_OVERVIEW_FILE}`: presenter-facing branch summary",
            f"- `{ENRON_STORY_MANIFEST_FILE}`: structured demo manifest",
            f"- `{ENRON_EXPORTS_PREVIEW_FILE}`: export preview for timeline and forecast artifacts",
            f"- `{ENRON_PRESENTATION_MANIFEST_FILE}`: presentation beat manifest",
            f"- `{ENRON_PRESENTATION_GUIDE_FILE}`: operator guide for bundle demos",
            "",
            "## Other Enron Examples",
            "",
            *sibling_lines,
            *timeline_lines,
            "",
            "## Refresh",
            "",
            "```bash",
            f"python scripts/build_enron_example_bundles.py --bundle {spec.bundle_slug}",
            f"python scripts/validate_whatif_artifacts.py {spec.output_root}",
            f"python scripts/capture_enron_bundle_screenshots.py --bundle {spec.bundle_slug}",
            "```",
            "",
            "## Constraint",
            "",
            (
                "This repo now carries a small checked-in Enron Rosetta sample for the "
                "saved bundles and smoke checks. Fetch the full archive with "
                "`make fetch-enron-full` when you want full training, full benchmark "
                "builds, or full archive validation."
            ),
            "",
            (
                "The macro heads in these saved bundles stay advisory context beside "
                "the email-path evidence. See "
                "[the current calibration report](../../../studies/macro_calibration_enron_v1/calibration_report.md) "
                "before making any stronger claim."
            ),
        ]
    )
    (bundle_root / "README.md").write_text(readme + "\n", encoding="utf-8")


def _bundle_exports_preview(context: dict[str, Any]) -> list[dict[str, object]]:
    return [
        {
            "name": "Canonical Timeline Export",
            "summary": (
                f"{context['history_count']} prior canonical events from "
                f"{', '.join(context['source_family_labels'])} before the branch point."
            ),
            "paths": [
                "workspace/context_snapshot.json",
                "workspace/canonical_events.jsonl",
                "workspace/canonical_event_index.json",
            ],
        },
        {
            "name": "Learned Forecast Export",
            "summary": (
                f"Saved reference forecast in {context['forecast_filename']} plus the "
                "combined what-if experiment result."
            ),
            "paths": [
                "whatif_experiment_result.json",
                str(context["forecast_filename"]),
                "whatif_business_state_comparison.json",
            ],
        },
        {
            "name": "Public Context Export",
            "summary": (
                f"Dated public-company slice with {context['financial_count']} financial, "
                f"{context['news_count']} news, {context['stock_count']} market, "
                f"{context['credit_count']} credit, and {context['ferc_count']} "
                "regulatory checkpoints."
            ),
            "paths": ["workspace/whatif_public_context.json", "timeline_arc.md"],
        },
    ]


def _bundle_story_manifest(
    spec, bundle_root: Path, context: dict[str, Any]
) -> dict[str, object]:
    return {
        "manifest_version": 1,
        "bundle_slug": spec.bundle_slug,
        "title": spec.title,
        "organization_name": "Enron Corporation",
        "source_mode": "real_history",
        "benchmark_role": spec.role,
        "lead": spec.lead,
        "branch_point": spec.branch_point,
        "actual_happened": spec.actual_happened,
        "branch_timestamp": context["branch_timestamp"],
        "history_event_count": context["history_count"],
        "future_event_count": context["future_count"],
        "source_families": list(context["source_family_labels"]),
        "domains": list(context["domain_labels"]),
        "forecast_file": context["forecast_filename"],
        "public_objective_pack_id": spec.public_objective_pack_id,
        "top_candidate": dict(context["top_candidate"]).get("label") or "",
        "workspace_root": str((bundle_root / WORKSPACE_DIRECTORY).resolve()),
        "ui_command": str(context["ui_command"]),
        "files": {
            "overview": ENRON_STORY_OVERVIEW_FILE,
            "exports_preview": ENRON_EXPORTS_PREVIEW_FILE,
            "presentation_manifest": ENRON_PRESENTATION_MANIFEST_FILE,
            "presentation_guide": ENRON_PRESENTATION_GUIDE_FILE,
        },
    }


def _bundle_presentation_manifest(
    spec,
    bundle_root: Path,
    context: dict[str, Any],
) -> dict[str, object]:
    top_candidate = (
        dict(context["top_candidate"]).get("label") or "the saved top-ranked action"
    )
    return {
        "opening_hook": (
            "Open a real Enron branch with the mail thread, the dated public-company "
            "timeline, and the learned reference forecast already lined up."
        ),
        "demo_goal": (
            "Show one real-history Enron branch on the shared canonical timeline with "
            "the saved forecast, the ranked actions, and the thicker company context."
        ),
        "presenter_setup": {
            "organization_name": "Enron Corporation",
            "bundle_slug": spec.bundle_slug,
            "bundle_role": spec.role,
            "workspace_root": str((bundle_root / WORKSPACE_DIRECTORY).resolve()),
            "ui_command": str(context["ui_command"]),
        },
        "primitives": [
            {
                "name": "Historical Branch",
                "current_value": spec.branch_point,
                "what_it_means": "One real dated branch point from the Enron archive.",
            },
            {
                "name": "Timeline",
                "current_value": ", ".join(context["source_family_labels"]),
                "what_it_means": "Mail plus dated public-company context on one shared chronology.",
            },
            {
                "name": "Forecast",
                "current_value": context["forecast_filename"],
                "what_it_means": "Repo-owned learned reference result that ships with the bundle.",
            },
        ],
        "beats": [
            {
                "title": "Open the saved bundle",
                "studio_view": "whatif",
                "operator_action": "Start in Studio with the saved Enron workspace open.",
                "read_it_as": "This is a repo-owned real-history branch, not a synthetic script.",
            },
            {
                "title": "Show the mixed timeline",
                "studio_view": "timeline",
                "operator_action": "Filter the Company Timeline and point out mail plus public-context rows before the branch.",
                "read_it_as": "The branch lives inside one shared chronology rather than beside detached side panels.",
            },
            {
                "title": "Anchor the real decision",
                "studio_view": "whatif",
                "operator_action": "Read the branch point and why this thread matters.",
                "read_it_as": "The choice is concrete and dated, and the saved bundle keeps the real lead-up in view.",
            },
            {
                "title": "Show the candidate ranking",
                "studio_view": "whatif",
                "operator_action": "Open the ranked comparison and call out the current top candidate.",
                "read_it_as": f"The current saved winner is {top_candidate}.",
            },
            {
                "title": "Show the learned forecast",
                "studio_view": "whatif",
                "operator_action": "Open the saved forecast panel and keep the reference backend visible.",
                "read_it_as": "The shipped learned path is the default saved result for this bundle.",
            },
            {
                "title": "Keep the macro claims narrow",
                "studio_view": "whatif",
                "operator_action": "Point at the public-company slice and note the current calibration caveat.",
                "read_it_as": "The macro fields stay advisory context beside the real thread evidence.",
            },
            {
                "title": "Close on the shared product surface",
                "studio_view": "whatif",
                "operator_action": "End on the bundle files and export preview.",
                "read_it_as": "The same saved bundle now carries the branch, the chronology, the forecast, and the demo guide in one place.",
            },
        ],
        "closing_argument": (
            "This bundle works as a complete Enron branch case because the branch, the "
            "lead-up timeline, the ranked alternatives, and the saved forecast all live "
            "on one surface."
        ),
        "operator_commands": [
            str(context["ui_command"]),
            f"python scripts/build_enron_example_bundles.py --bundle {spec.bundle_slug}",
            f"python scripts/validate_enron_example_bundles.py --root {bundle_root.parent}",
        ],
    }


def _render_enron_story_overview(
    spec,
    context: dict[str, Any],
    story_manifest: dict[str, object],
    presentation_manifest: dict[str, object],
) -> str:
    source_families = ", ".join(context["source_family_labels"]) or "unknown"
    domains = ", ".join(context["domain_labels"]) or "unknown"
    top_candidate = dict(context["top_candidate"]).get("label") or "(none)"
    return "\n".join(
        [
            "# VEI Story · Enron Corporation",
            "",
            "VEI is one shared timeline and replay surface. This bundle shows the real-history path: one dated Enron decision, the prior branch-local chronology, the public-company side data, and the shipped learned forecast in one saved package.",
            "",
            "## Bundle",
            "",
            f"- Title: `{spec.title}`",
            f"- Bundle slug: `{spec.bundle_slug}`",
            f"- Bundle role: `{spec.role}`",
            f"- Branch point: {spec.branch_point}",
            f"- What actually happened: {spec.actual_happened}",
            f"- Branch date: `{context['branch_timestamp']}`",
            f"- Prior events: `{context['history_count']}`",
            f"- Recorded future events: `{context['future_count']}`",
            f"- Source families: `{source_families}`",
            f"- Domains: `{domains}`",
            f"- Saved forecast file: `{context['forecast_filename']}`",
            f"- Top ranked candidate: `{top_candidate}`",
            "",
            "## Why it matters",
            "",
            *[line for paragraph in spec.story_lines for line in (paragraph, "")],
            "## Open it",
            "",
            "```bash",
            str(context["ui_command"]),
            "```",
            "",
            "## Demo files",
            "",
            f"- Story manifest: `{ENRON_STORY_MANIFEST_FILE}`",
            f"- Exports preview: `{ENRON_EXPORTS_PREVIEW_FILE}`",
            f"- Presentation manifest: `{ENRON_PRESENTATION_MANIFEST_FILE}`",
            f"- Presentation guide: `{ENRON_PRESENTATION_GUIDE_FILE}`",
            "",
            "## Structured notes",
            "",
            f"- Story manifest role: `{story_manifest['benchmark_role']}`",
            f"- Presentation beats: `{len(presentation_manifest['beats'])}`",
            "",
        ]
    )


def _render_enron_presentation_guide(
    presentation_manifest: dict[str, object],
) -> str:
    lines = [
        "# VEI World Briefing Guide · Enron Corporation",
        "",
        "This guide walks one real-history Enron bundle from the shared chronology to the saved learned forecast.",
        "",
        "## Opening Hook",
        "",
        str(presentation_manifest["opening_hook"]),
        "",
        "## Demo Goal",
        "",
        str(presentation_manifest["demo_goal"]),
        "",
        "## Walkthrough Flow",
        "",
    ]
    for index, beat in enumerate(presentation_manifest["beats"], start=1):
        lines.extend(
            [
                f"### Step {index} · {beat['title']}",
                "",
                f"- Studio view: `{beat['studio_view']}`",
                f"- Operator action: {beat['operator_action']}",
                f"- Read it as: {beat['read_it_as']}",
                "",
            ]
        )
    lines.extend(
        [
            "## Closing Argument",
            "",
            str(presentation_manifest["closing_argument"]),
            "",
            "## Operator Commands",
            "",
        ]
    )
    for command in presentation_manifest["operator_commands"]:
        lines.append(f"- `{command}`")
    lines.append("")
    return "\n".join(lines)


def _write_bundle_story_files(
    spec,
    bundle_root: Path,
    *,
    context: dict[str, Any] | None = None,
) -> None:
    bundle_context = context or _bundle_story_context(spec, bundle_root)
    story_manifest = _bundle_story_manifest(spec, bundle_root, bundle_context)
    exports_preview = _bundle_exports_preview(bundle_context)
    presentation_manifest = _bundle_presentation_manifest(
        spec, bundle_root, bundle_context
    )

    (bundle_root / ENRON_STORY_MANIFEST_FILE).write_text(
        json.dumps(story_manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    (bundle_root / ENRON_EXPORTS_PREVIEW_FILE).write_text(
        json.dumps(exports_preview, indent=2) + "\n",
        encoding="utf-8",
    )
    (bundle_root / ENRON_PRESENTATION_MANIFEST_FILE).write_text(
        json.dumps(presentation_manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    (bundle_root / ENRON_STORY_OVERVIEW_FILE).write_text(
        _render_enron_story_overview(
            spec,
            bundle_context,
            story_manifest,
            presentation_manifest,
        ),
        encoding="utf-8",
    )
    (bundle_root / ENRON_PRESENTATION_GUIDE_FILE).write_text(
        _render_enron_presentation_guide(presentation_manifest),
        encoding="utf-8",
    )


def _write_casebook_overview() -> None:
    proof_specs = [spec for spec in bundle_specs() if spec.role == "proof"]
    narrative_specs = [spec for spec in bundle_specs() if spec.role == "narrative"]
    lines = [
        "# Enron Casebook",
        "",
        "The Enron surface is split into two layers.",
        "",
        "- `proof`: the technical flagship cases with the clearest downstream tails or operational forks",
        "- `narrative`: the strongest governance and disclosure stories for essay, demo, and presentation use",
        "",
        "## Proof examples",
        "",
    ]
    for spec in proof_specs:
        lines.extend(
            [
                f"- [{spec.title}](examples/{spec.bundle_slug}/README.md)",
                f"  - Branch point: {spec.branch_point}",
                f"  - What actually happened: {spec.actual_happened}",
            ]
        )
    lines.extend(["", "## Narrative examples", ""])
    for spec in narrative_specs:
        lines.extend(
            [
                f"- [{spec.title}](examples/{spec.bundle_slug}/README.md)",
                f"  - Branch point: {spec.branch_point}",
                f"  - What actually happened: {spec.actual_happened}",
            ]
        )
    lines.extend(
        [
            "",
            "## Public reading order",
            "",
            "- Start with Master Agreement for the long-tail technical proof.",
            "- Use Baxter, PG&E, California, and Braveheart to show range across communication, commercial, regulatory, and accounting forks.",
            "- Use Watkins, Q3 disclosure review, and Skilling resignation materials as the narrative governance set.",
            "",
        ]
    )
    Path("docs/ENRON_CASEBOOK.md").write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


def _history_dimension_labels(
    canonical_bundle,
    *,
    branch_event_id: str,
    branch_ts_ms: int,
) -> tuple[list[str], list[str]]:
    if canonical_bundle is None:
        return [], []

    source_families: set[str] = set()
    domains: set[str] = set()
    for row in canonical_bundle.index.rows:
        if row.event_id == branch_event_id:
            continue
        if branch_ts_ms and row.ts_ms >= branch_ts_ms:
            continue
        metadata = row.metadata or {}
        source_family = str(metadata.get("source_family") or "").strip().lower()
        if source_family:
            source_families.add(source_family)
        domain = str(row.domain or "").strip().lower()
        if domain:
            domains.add(domain)
    return sorted(source_families), sorted(domains)


def build_bundle(
    spec,
    *,
    artifacts_root: Path,
    provider: str,
    model: str,
    refresh_llm: bool,
) -> Path:
    world = _load_thread_world(spec.case_id)
    event_id = _event_id_map()[spec.case_id]
    output_root = spec.output_root.resolve()
    if refresh_llm:
        result = run_counterfactual_experiment(
            world,
            artifacts_root=artifacts_root,
            label=spec.run_label,
            counterfactual_prompt=spec.primary_prompt,
            event_id=event_id,
            mode="both",
            forecast_backend=default_forecast_backend(),
            provider=provider,
            model=model,
            seed=42042,
        )
    else:
        result = _rebuild_saved_experiment_without_llm(
            spec=spec,
            world=world,
            artifacts_root=artifacts_root,
            output_root=output_root,
            event_id=event_id,
        )
    package_example(result.artifacts.root, output_root)
    workspace_root = output_root / WORKSPACE_DIRECTORY
    snapshot = ContextSnapshot.model_validate_json(
        (workspace_root / "context_snapshot.json").read_text(encoding="utf-8")
    )
    write_canonical_history_sidecars(snapshot, workspace_root / "context_snapshot.json")
    _rewrite_canonical_sidecars_with_public_history(
        spec=spec,
        workspace_root=workspace_root,
        world=world,
    )
    build_example(
        output_root,
        label=spec.comparison_label,
        objective_pack_id=spec.objective_pack_id,
        public_objective_pack_id=spec.public_objective_pack_id,
        bundle_role=spec.role,
        branch_point=spec.branch_point,
        actual_happened=spec.actual_happened,
        candidates=[
            {
                "label": candidate.label,
                "prompt": candidate.prompt,
                "explanation": candidate.explanation,
            }
            for candidate in spec.candidates
        ],
    )
    bundle_context = _bundle_story_context(spec, output_root)
    _write_bundle_readme(spec, output_root, context=bundle_context)
    _write_bundle_story_files(spec, output_root, context=bundle_context)
    issues = validate_packaged_example_bundle(output_root)
    forecast_filename = _forecast_filename(output_root)
    if forecast_filename != REFERENCE_FORECAST_FILE:
        issues.append(
            f"expected {REFERENCE_FORECAST_FILE} as the saved forecast, got {forecast_filename!r}"
        )
    heuristic_path = output_root / HEURISTIC_FORECAST_FILE
    if heuristic_path.exists():
        issues.append(
            f"heuristic baseline should stay debug-only and out of the saved bundle: {heuristic_path}"
        )
    history_paths = canonical_history_paths(workspace_root / "context_snapshot.json")
    if not history_paths.events_path.exists():
        issues.append(f"missing bundle artifact: {history_paths.events_path}")
    if not history_paths.index_path.exists():
        issues.append(f"missing bundle artifact: {history_paths.index_path}")
    if issues:
        joined = "\n".join(f"- {issue}" for issue in issues)
        raise ValueError(f"bundle validation failed for {spec.bundle_slug}:\n{joined}")
    return output_root


def _rewrite_canonical_sidecars_with_public_history(
    *,
    spec,
    workspace_root: Path,
    world: WhatIfWorld,
) -> None:
    branch_context = build_branch_context(
        world,
        event_id=_event_id_map()[spec.case_id],
        organization_domain=world.summary.organization_domain,
    )
    canonical_rows = build_enron_canonical_rows(
        public_context=world.public_context,
        branch_event=branch_context.branch_event,
        organization_domain=world.summary.organization_domain,
        past_events=branch_context.past_events,
    )

    bundle = build_canonical_history_bundle_from_rows(
        organization_name=world.summary.organization_name,
        organization_domain=world.summary.organization_domain,
        captured_at="",
        snapshot_role="workspace_seed",
        source_providers=sorted({row.provider for row in canonical_rows}),
        rows=canonical_rows,
    )
    write_canonical_history_bundle(bundle, workspace_root / "context_snapshot.json")


def _rebuild_saved_experiment_without_llm(
    *,
    spec,
    world: WhatIfWorld,
    artifacts_root: Path,
    output_root: Path,
    event_id: str,
) -> WhatIfExperimentResult:
    root = artifacts_root.expanduser().resolve() / spec.run_label
    workspace_root = root / WORKSPACE_DIRECTORY
    selection = select_specific_event(
        world,
        thread_id=None,
        event_id=event_id,
        prompt=spec.primary_prompt,
    )
    materialization = materialize_episode(
        world,
        root=workspace_root,
        event_id=event_id,
    )
    baseline = replay_episode_baseline(
        workspace_root,
        tick_ms=_baseline_tick_ms(materialization.baseline_dataset_path),
        seed=42042,
    )
    llm_result = _load_saved_llm_result(output_root)
    forecast_result = run_dynamics_counterfactual(
        world=world,
        materialization=materialization,
        prompt=spec.primary_prompt,
        forecast_backend=default_forecast_backend(),
        allow_proxy_fallback=True,
        llm_messages=llm_result.messages if llm_result is not None else None,
        seed=42042,
        ejepa_epochs=4,
        ejepa_batch_size=64,
        ejepa_force_retrain=False,
        ejepa_device=None,
    )
    forecast_result = _attach_business_state_to_forecast_result(
        forecast_result,
        branch_event=materialization.branch_event,
        organization_domain=materialization.organization_domain,
        public_context=materialization.public_context,
    )

    result_path = root / EXPERIMENT_RESULT_FILE
    overview_path = root / "whatif_experiment_overview.md"
    llm_path = root / LLM_RESULT_FILE if llm_result is not None else None
    forecast_path = root / REFERENCE_FORECAST_FILE
    root.mkdir(parents=True, exist_ok=True)

    result = WhatIfExperimentResult(
        mode="both" if llm_result is not None else "e_jepa",
        label=spec.run_label,
        intervention=WhatIfInterventionSpec(
            label=spec.run_label,
            prompt=spec.primary_prompt,
            objective=(
                selection.scenario.description
                if selection.scenario.description
                else "counterfactual replay"
            ),
            scenario_id=selection.scenario.scenario_id,
            thread_id=materialization.thread_id,
            branch_event_id=materialization.branch_event_id,
        ),
        selection=selection,
        materialization=materialization,
        baseline=baseline,
        llm_result=llm_result,
        forecast_result=forecast_result,
        artifacts=WhatIfExperimentArtifacts(
            root=root,
            result_json_path=result_path,
            overview_markdown_path=overview_path,
            llm_json_path=llm_path,
            forecast_json_path=forecast_path,
        ),
    )
    result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    overview_path.write_text(
        render_experiment_overview(result),
        encoding="utf-8",
    )
    forecast_path.write_text(
        forecast_result.model_dump_json(indent=2),
        encoding="utf-8",
    )
    if llm_path is not None and llm_result is not None:
        llm_path.write_text(llm_result.model_dump_json(indent=2), encoding="utf-8")
    return result


def _load_saved_llm_result(output_root: Path) -> WhatIfLLMReplayResult | None:
    llm_path = output_root / LLM_RESULT_FILE
    if not llm_path.exists():
        return None
    payload = json.loads(llm_path.read_text(encoding="utf-8"))
    if not payload:
        return None
    return WhatIfLLMReplayResult.model_validate(payload)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the repo-owned Enron saved example bundles."
    )
    parser.add_argument(
        "--bundle",
        action="append",
        default=None,
        help="Optional bundle slug to build. Pass multiple times to build a subset.",
    )
    parser.add_argument(
        "--artifacts-root",
        type=Path,
        default=DEFAULT_ARTIFACTS_ROOT,
        help="Scratch root for freshly generated what-if runs.",
    )
    parser.add_argument(
        "--refresh-llm",
        action="store_true",
        help="Re-run the live LLM path instead of reusing the saved LLM result.",
    )
    parser.add_argument("--provider", default=None, help="Optional LLM provider.")
    parser.add_argument("--model", default=None, help="Optional LLM model.")
    args = parser.parse_args()

    selected_specs = (
        [spec_by_bundle_slug(bundle_slug) for bundle_slug in args.bundle]
        if args.bundle
        else list(bundle_specs())
    )
    provider, model = resolve_llm_defaults(
        provider=args.provider,
        model=args.model,
    )
    for spec in selected_specs:
        output_root = build_bundle(
            spec,
            artifacts_root=args.artifacts_root.resolve(),
            provider=provider,
            model=model,
            refresh_llm=args.refresh_llm,
        )
        print(f"built: {output_root}")
    _write_casebook_overview()
    render_timeline_image()
    render_timeline_markdown()


if __name__ == "__main__":
    main()
