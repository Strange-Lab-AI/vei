from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from vei.project_settings import resolve_llm_defaults
from vei.context.api import resolve_world_public_context
from vei.whatif.api import list_supported_scenarios, run_counterfactual_experiment
from vei.whatif.artifact_validation import validate_packaged_example_bundle
from vei.whatif.cases import assign_case_ids, build_case_summaries
from vei.whatif.corpus._aggregation import build_actor_profiles, build_thread_summaries
from vei.whatif.corpus._enron import CONTENT_NOTICE, ENRON_DOMAIN, build_event
from vei.whatif.filenames import (
    EPISODE_MANIFEST_FILE,
    EXPERIMENT_RESULT_FILE,
    PUBLIC_CONTEXT_FILE,
    STUDIO_SAVED_FORECAST_FILES,
    WORKSPACE_DIRECTORY,
)
from vei.whatif.models import WhatIfWorld, WhatIfWorldSummary
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
        for event in (
            build_event(row, "")
            for row in metadata_rows
        )
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


def _context_counts(public_context_payload: dict[str, object]) -> tuple[int, int, int, int, int]:
    return (
        len(list(public_context_payload.get("financial_snapshots") or [])),
        len(list(public_context_payload.get("public_news_events") or [])),
        len(list(public_context_payload.get("stock_history") or [])),
        len(list(public_context_payload.get("credit_history") or [])),
        len(list(public_context_payload.get("ferc_history") or [])),
    )


def _write_bundle_readme(spec, bundle_root: Path) -> None:
    workspace_root = bundle_root / WORKSPACE_DIRECTORY
    manifest_payload = json.loads(
        (workspace_root / EPISODE_MANIFEST_FILE).read_text(encoding="utf-8")
    )
    experiment_payload = json.loads(
        (bundle_root / EXPERIMENT_RESULT_FILE).read_text(encoding="utf-8")
    )
    comparison_payload = json.loads(
        (bundle_root / "whatif_business_state_comparison.json").read_text(
            encoding="utf-8"
        )
    )
    public_context_payload = json.loads(
        (workspace_root / PUBLIC_CONTEXT_FILE).read_text(encoding="utf-8")
    )
    history_count = int(manifest_payload.get("history_message_count") or 0)
    future_count = int(manifest_payload.get("future_event_count") or 0)
    branch_timestamp = str(manifest_payload.get("branch_timestamp") or "")[:10]
    forecast_payload = experiment_payload.get("forecast_result") or {}
    business_change = forecast_payload.get("business_state_change") or {}
    top_candidate = (
        comparison_payload.get("candidates", [{}])[0]
        if comparison_payload.get("candidates")
        else {}
    )
    forecast_filename = _forecast_filename(bundle_root)
    (
        financial_count,
        news_count,
        stock_count,
        credit_count,
        ferc_count,
    ) = _context_counts(public_context_payload)
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
    readme = "\n".join(
        [
            f"# {spec.title}",
            "",
            spec.lead,
            "",
            "## Open It In Studio",
            "",
            "```bash",
            "vei ui serve \\",
            f"  --root {spec.output_root / WORKSPACE_DIRECTORY} \\",
            "  --host 127.0.0.1 \\",
            "  --port 3055",
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
            "## Why This Branch Matters",
            "",
            *story_lines,
            "## What This Example Covers",
            "",
            f"- Historical branch point: {spec.branch_point}",
            (
                f"- Saved branch scene: {history_count} prior messages and "
                f"{future_count} recorded future events"
            ),
            (
                f"- Public-company slice at {branch_timestamp}: "
                f"{financial_count} financial checkpoints, {news_count} public news items, "
                f"{stock_count} market checkpoints, {credit_count} credit checkpoints, "
                f"and {ferc_count} regulatory checkpoints"
            ),
            f"- Saved LLM path: {spec.primary_prompt}",
            f"- Saved forecast file: `{forecast_filename}`",
            (
                f"- Business-state readout: "
                f"{business_change.get('summary') or forecast_payload.get('summary') or 'Saved forecast summary.'}"
            ),
            (
                f"- Top ranked candidate: "
                f"{top_candidate.get('label') or '(none)'}"
            ),
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
                "This repo now carries the Rosetta parquet archive, the source cache, "
                "and the raw Enron mail tar under `data/enron/`, so a fresh clone can "
                "open these saved examples and rebuild them without reaching into a "
                "sibling checkout."
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


def build_bundle(spec, *, artifacts_root: Path, provider: str, model: str) -> Path:
    world = _load_thread_world(spec.case_id)
    event_id = _event_id_map()[spec.case_id]
    result = run_counterfactual_experiment(
        world,
        artifacts_root=artifacts_root,
        label=spec.run_label,
        counterfactual_prompt=spec.primary_prompt,
        event_id=event_id,
        mode="both",
        forecast_backend="heuristic_baseline",
        provider=provider,
        model=model,
        seed=42042,
    )
    output_root = spec.output_root.resolve()
    package_example(result.artifacts.root, output_root)
    build_example(
        output_root,
        label=spec.comparison_label,
        objective_pack_id=spec.objective_pack_id,
        candidates=[
            {"label": candidate.label, "prompt": candidate.prompt}
            for candidate in spec.candidates
        ],
    )
    _write_bundle_readme(spec, output_root)
    issues = validate_packaged_example_bundle(output_root)
    if issues:
        joined = "\n".join(f"- {issue}" for issue in issues)
        raise ValueError(f"bundle validation failed for {spec.bundle_slug}:\n{joined}")
    return output_root


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
        )
        print(f"built: {output_root}")
    render_timeline_image()
    render_timeline_markdown()


if __name__ == "__main__":
    main()
