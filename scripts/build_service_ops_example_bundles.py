from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

from vei.context.api import (
    ContextSnapshot,
    canonical_history_paths,
    load_canonical_history_bundle,
    write_canonical_history_bundle,
    write_canonical_history_sidecars,
)
from vei.verticals.demo import VerticalDemoSpec, prepare_vertical_story
from vei.whatif.api import (
    default_forecast_backend,
    export_workspace_history_snapshot,
    load_world,
    run_counterfactual_experiment,
)
from vei.whatif.artifact_validation import validate_packaged_example_bundle
from vei.whatif.filenames import (
    EPISODE_MANIFEST_FILE,
    EXPERIMENT_OVERVIEW_FILE,
    EXPERIMENT_RESULT_FILE,
    HEURISTIC_FORECAST_FILE,
    REFERENCE_FORECAST_FILE,
    SCRUBBED_PATH_PLACEHOLDER,
    WORKSPACE_DIRECTORY,
)

try:
    from scripts.build_enron_business_state_example import build_example
except ModuleNotFoundError:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from build_enron_business_state_example import build_example


EXAMPLES_ROOT = Path("docs/examples")
DEFAULT_SOURCE_ROOT = Path("_vei_out/service_ops_story_sources")
DEFAULT_BUILD_ROOT = Path("_vei_out/service_ops_bundle_builds")


@dataclass(frozen=True)
class CandidatePrompt:
    label: str
    prompt: str


@dataclass(frozen=True)
class ServiceOpsBundleSpec:
    bundle_slug: str
    title: str
    summary: str
    scenario_variant: str
    contract_variant: str
    branch_thread_id: str
    primary_prompt: str
    objective_pack_id: str
    comparison_label: str
    candidates: tuple[CandidatePrompt, ...]

    @property
    def output_root(self) -> Path:
        return EXAMPLES_ROOT / self.bundle_slug


def bundle_specs() -> tuple[ServiceOpsBundleSpec, ...]:
    return (
        ServiceOpsBundleSpec(
            bundle_slug="clearwater-dispatch-recovery",
            title="Clearwater Dispatch Recovery",
            summary=(
                "Synthetic Clearwater branch on the VIP outage command thread after the "
                "dispatch recovery work has started."
            ),
            scenario_variant="service_day_collision",
            contract_variant="protect_sla",
            branch_thread_id="tickets:JRA-CFS-10",
            primary_prompt=(
                "Lock the billing hold, assign the certified backup technician, and "
                "send one accountable customer-safe ETA update before the SLA slips."
            ),
            objective_pack_id="reduce_delay",
            comparison_label="clearwater_dispatch_recovery_business_state_comparison",
            candidates=(
                CandidatePrompt(
                    label="Stabilize the full service loop",
                    prompt=(
                        "Assign the certified backup technician, keep the disputed invoice "
                        "on hold, and give one confirmed ETA update across dispatch and billing."
                    ),
                ),
                CandidatePrompt(
                    label="Dispatch first, finance later",
                    prompt=(
                        "Reroute the technician immediately and leave billing active until "
                        "after the field visit is already underway."
                    ),
                ),
                CandidatePrompt(
                    label="Escalate without a clean handoff",
                    prompt=(
                        "Escalate to leadership for visibility, but leave the dispatch note "
                        "and billing handoff to be cleaned up later."
                    ),
                ),
            ),
        ),
        ServiceOpsBundleSpec(
            bundle_slug="clearwater-billing-dispute-reopened",
            title="Clearwater Billing Dispute Reopened",
            summary=(
                "Synthetic Clearwater branch on the billing dispute thread once the same "
                "morning starts pulling finance back into the service response."
            ),
            scenario_variant="billing_dispute_reopened",
            contract_variant="protect_revenue",
            branch_thread_id="tickets:JRA-CFS-12",
            primary_prompt=(
                "Keep the disputed invoice on hold, document one shared timeline in the "
                "handoff note, and route a single accountable manager update before finance acts."
            ),
            objective_pack_id="contain_exposure",
            comparison_label="clearwater_billing_dispute_business_state_comparison",
            candidates=(
                CandidatePrompt(
                    label="Contain finance risk early",
                    prompt=(
                        "Hold the disputed invoice, tie the service response into the billing "
                        "record, and keep collections quiet until the field story is stable."
                    ),
                ),
                CandidatePrompt(
                    label="Push collections anyway",
                    prompt=(
                        "Leave the collection follow-up active while dispatch works the outage "
                        "and hope the customer accepts the split message."
                    ),
                ),
                CandidatePrompt(
                    label="Escalate without shared notes",
                    prompt=(
                        "Ask finance leadership to review the dispute, but leave the shared "
                        "handoff note unfinished while the morning stays hot."
                    ),
                ),
            ),
        ),
        ServiceOpsBundleSpec(
            bundle_slug="clearwater-technician-no-show",
            title="Clearwater Technician No-Show",
            summary=(
                "Synthetic Clearwater branch on the backup dispatch routing thread after the "
                "technician no-show becomes the main customer-trust risk."
            ),
            scenario_variant="technician_no_show",
            contract_variant="protect_customer_trust",
            branch_thread_id="tickets:JRA-CFS-11",
            primary_prompt=(
                "Escalate the no-show immediately, set one realistic ETA with the customer, "
                "and keep the field-to-billing handoff synchronized."
            ),
            objective_pack_id="protect_relationship",
            comparison_label="clearwater_no_show_business_state_comparison",
            candidates=(
                CandidatePrompt(
                    label="Own the no-show fast",
                    prompt=(
                        "Escalate the technician no-show, reset the ETA with one accountable "
                        "customer update, and keep the dispatch and billing notes aligned."
                    ),
                ),
                CandidatePrompt(
                    label="Hide the delay",
                    prompt=(
                        "Keep searching for a replacement quietly and avoid updating the "
                        "customer until a new technician is already confirmed."
                    ),
                ),
                CandidatePrompt(
                    label="Fix dispatch only",
                    prompt=(
                        "Swap the technician quickly, but leave the customer message and the "
                        "billing note for a later clean-up pass."
                    ),
                ),
            ),
        ),
    )


def spec_by_bundle_slug(bundle_slug: str) -> ServiceOpsBundleSpec:
    for spec in bundle_specs():
        if spec.bundle_slug == bundle_slug:
            return spec
    raise KeyError(f"unknown service-ops bundle slug: {bundle_slug}")


def build_bundle(spec: ServiceOpsBundleSpec, *, source_root: Path) -> Path:
    story_workspace_root = source_root.expanduser().resolve() / spec.bundle_slug
    if story_workspace_root.exists():
        shutil.rmtree(story_workspace_root)
    story = prepare_vertical_story(
        VerticalDemoSpec(
            vertical_name="service_ops",
            workspace_root=story_workspace_root,
            scenario_variant=spec.scenario_variant,
            contract_variant=spec.contract_variant,
            compare_runner="scripted",
            overwrite=True,
            seed=42042,
            max_steps=18,
        )
    )

    snapshot_path = export_workspace_history_snapshot(story.workspace_root)
    exported_history_bundle = load_canonical_history_bundle(snapshot_path)
    world = load_world(source="company_history", source_dir=snapshot_path)
    event_id = _latest_thread_event_id(world, spec.branch_thread_id)

    output_root = spec.output_root.resolve()
    build_root = DEFAULT_BUILD_ROOT.resolve() / spec.bundle_slug
    if build_root.exists():
        shutil.rmtree(build_root)
    result = run_counterfactual_experiment(
        world,
        artifacts_root=build_root.parent,
        label=build_root.name,
        counterfactual_prompt=spec.primary_prompt,
        event_id=event_id,
        mode="e_jepa",
        forecast_backend=default_forecast_backend(),
        seed=42042,
    )
    _package_bundle(
        result.artifacts.root,
        output_root,
        exported_history_bundle=exported_history_bundle,
    )

    build_example(
        output_root,
        label=spec.comparison_label,
        objective_pack_id=spec.objective_pack_id,
        candidates=[
            {"label": candidate.label, "prompt": candidate.prompt}
            for candidate in spec.candidates
        ],
    )
    _copy_story_files(story.workspace_root, output_root)
    _write_bundle_readme(spec, output_root, result.materialization.branch_event_id)
    issues = validate_bundle(output_root)
    if issues:
        joined = "\n".join(f"- {issue}" for issue in issues)
        raise ValueError(
            f"service-ops bundle validation failed for {spec.bundle_slug}:\n{joined}"
        )
    return output_root


def _latest_thread_event_id(world, thread_id: str) -> str:
    matches = [event for event in world.events if event.thread_id == thread_id]
    if not matches:
        raise ValueError(
            f"thread not found in exported Clearwater history: {thread_id}"
        )
    matches.sort(key=lambda event: (event.timestamp_ms, event.event_id))
    return matches[-1].event_id


def _copy_story_files(source_workspace_root: Path, output_root: Path) -> None:
    copies = {
        "story_overview.md": "clearwater_story_overview.md",
        "story_manifest.json": "clearwater_story_manifest.json",
        "exports_preview.json": "clearwater_exports_preview.json",
        "presentation_manifest.json": "clearwater_presentation_manifest.json",
        "presentation_guide.md": "clearwater_presentation_guide.md",
    }
    for source_name, dest_name in copies.items():
        source_path = source_workspace_root / source_name
        if not source_path.exists():
            continue
        shutil.copy2(source_path, output_root / dest_name)


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _package_bundle(
    source_root: Path,
    output_root: Path,
    *,
    exported_history_bundle,
) -> None:
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    experiment_payload = _read_json(source_root / EXPERIMENT_RESULT_FILE)
    forecast_filename = _resolve_forecast_filename(source_root, experiment_payload)
    manifest_payload = _read_json(
        source_root / WORKSPACE_DIRECTORY / EPISODE_MANIFEST_FILE
    )
    snapshot_payload = _read_json(
        source_root / WORKSPACE_DIRECTORY / "context_snapshot.json"
    )
    public_context_payload = _read_json(
        source_root / WORKSPACE_DIRECTORY / "whatif_public_context.json"
    )
    forecast_payload = _read_json(source_root / forecast_filename)

    shutil.copy2(
        source_root / EXPERIMENT_OVERVIEW_FILE,
        output_root / EXPERIMENT_OVERVIEW_FILE,
    )
    _write_json(
        output_root / EXPERIMENT_RESULT_FILE,
        _rewrite_experiment_result(
            experiment_payload,
            forecast_filename=forecast_filename,
        ),
    )
    _write_json(
        output_root / forecast_filename, _rewrite_forecast_result(forecast_payload)
    )
    _write_json(
        output_root / WORKSPACE_DIRECTORY / EPISODE_MANIFEST_FILE,
        _rewrite_manifest(manifest_payload),
    )
    _write_json(
        output_root / WORKSPACE_DIRECTORY / "context_snapshot.json",
        snapshot_payload,
    )
    _write_json(
        output_root / WORKSPACE_DIRECTORY / "whatif_public_context.json",
        public_context_payload,
    )
    shutil.copy2(
        source_root / WORKSPACE_DIRECTORY / "whatif_baseline_dataset.json",
        output_root / WORKSPACE_DIRECTORY / "whatif_baseline_dataset.json",
    )

    if exported_history_bundle is not None:
        write_canonical_history_bundle(
            exported_history_bundle,
            output_root / WORKSPACE_DIRECTORY / "context_snapshot.json",
        )
        return

    snapshot_payload = _read_json(
        output_root / WORKSPACE_DIRECTORY / "context_snapshot.json"
    )
    snapshot = ContextSnapshot.model_validate(snapshot_payload)
    write_canonical_history_sidecars(
        snapshot,
        output_root / WORKSPACE_DIRECTORY / "context_snapshot.json",
    )


def _resolve_forecast_filename(
    source_root: Path,
    experiment_payload: dict[str, object],
) -> str:
    artifacts = experiment_payload.get("artifacts")
    if isinstance(artifacts, dict):
        raw_path = str(artifacts.get("forecast_json_path") or "").strip()
        if raw_path:
            filename = Path(raw_path).name
            if filename and (source_root / filename).exists():
                return filename
    if (source_root / REFERENCE_FORECAST_FILE).exists():
        return REFERENCE_FORECAST_FILE
    if (source_root / HEURISTIC_FORECAST_FILE).exists():
        return HEURISTIC_FORECAST_FILE
    raise FileNotFoundError(f"forecast result not found under {source_root}")


def _rewrite_manifest(payload: dict[str, object]) -> dict[str, object]:
    updated = dict(payload)
    updated["source_dir"] = SCRUBBED_PATH_PLACEHOLDER
    updated["workspace_root"] = WORKSPACE_DIRECTORY
    return updated


def _rewrite_forecast_result(payload: dict[str, object]) -> dict[str, object]:
    updated = dict(payload)
    artifacts = updated.get("artifacts")
    if isinstance(artifacts, dict):
        updated["artifacts"] = {
            key: SCRUBBED_PATH_PLACEHOLDER for key in artifacts.keys()
        }
    return updated


def _rewrite_experiment_result(
    payload: dict[str, object],
    *,
    forecast_filename: str,
) -> dict[str, object]:
    updated = dict(payload)

    materialization = dict(updated.get("materialization") or {})
    if materialization:
        materialization["manifest_path"] = (
            f"{WORKSPACE_DIRECTORY}/{EPISODE_MANIFEST_FILE}"
        )
        materialization["bundle_path"] = SCRUBBED_PATH_PLACEHOLDER
        materialization["context_snapshot_path"] = (
            f"{WORKSPACE_DIRECTORY}/context_snapshot.json"
        )
        materialization["baseline_dataset_path"] = (
            f"{WORKSPACE_DIRECTORY}/whatif_baseline_dataset.json"
        )
        materialization["workspace_root"] = WORKSPACE_DIRECTORY
        updated["materialization"] = materialization

    baseline = dict(updated.get("baseline") or {})
    if baseline:
        baseline["workspace_root"] = WORKSPACE_DIRECTORY
        baseline["baseline_dataset_path"] = (
            f"{WORKSPACE_DIRECTORY}/whatif_baseline_dataset.json"
        )
        updated["baseline"] = baseline

    forecast_result = updated.get("forecast_result")
    if isinstance(forecast_result, dict):
        updated["forecast_result"] = _rewrite_forecast_result(forecast_result)

    artifacts = dict(updated.get("artifacts") or {})
    if artifacts:
        artifacts["root"] = "."
        artifacts["result_json_path"] = EXPERIMENT_RESULT_FILE
        artifacts["overview_markdown_path"] = EXPERIMENT_OVERVIEW_FILE
        artifacts["llm_json_path"] = None
        artifacts["forecast_json_path"] = forecast_filename
        updated["artifacts"] = artifacts
    return updated


def _write_bundle_readme(
    spec: ServiceOpsBundleSpec,
    output_root: Path,
    branch_event_id: str,
) -> None:
    workspace_root = output_root / WORKSPACE_DIRECTORY
    manifest_payload = json.loads(
        (workspace_root / EPISODE_MANIFEST_FILE).read_text(encoding="utf-8")
    )
    experiment_payload = json.loads(
        (output_root / EXPERIMENT_RESULT_FILE).read_text(encoding="utf-8")
    )
    comparison_payload = json.loads(
        (output_root / "whatif_business_state_comparison.json").read_text(
            encoding="utf-8"
        )
    )
    history_bundle = load_canonical_history_bundle(
        workspace_root / "context_snapshot.json"
    )
    prior_rows = []
    source_families: set[str] = set()
    if history_bundle is not None:
        branch_timestamp = str(manifest_payload.get("branch_timestamp") or "").strip()
        prior_rows = [
            row
            for row in history_bundle.index.rows
            if row.event_id != branch_event_id
            and (not branch_timestamp or row.timestamp <= branch_timestamp)
        ]
        source_families = {
            str(row.metadata.get("source_family") or "").strip().lower()
            or str(row.provider or "").strip().lower()
            or str(row.surface or "").strip().lower()
            for row in prior_rows
            if (
                str(row.metadata.get("source_family") or "").strip()
                or str(row.provider or "").strip()
                or str(row.surface or "").strip()
            )
        }
    forecast_payload = experiment_payload.get("forecast_result") or {}
    top_candidate = (
        comparison_payload.get("candidates", [{}])[0]
        if comparison_payload.get("candidates")
        else {}
    )
    lines = [
        f"# {spec.title}",
        "",
        "Synthetic Clearwater what-if bundle built from the repo-owned service-ops story workspace.",
        "",
        f"- Scenario variant: `{spec.scenario_variant}`",
        f"- Contract variant: `{spec.contract_variant}`",
        f"- Branch thread: `{manifest_payload.get('thread_id')}`",
        f"- Saved forecast file: `{REFERENCE_FORECAST_FILE}`",
        f"- Prior canonical events in the saved timeline: `{len(prior_rows)}`",
        f"- Source families in the saved timeline: `{', '.join(sorted(source_families))}`",
        "",
        "## Branch",
        spec.summary,
        "",
        f"- Branch subject: {manifest_payload.get('thread_subject')}",
        f"- Branch event id: `{manifest_payload.get('branch_event_id')}`",
        f"- Recorded future events: `{manifest_payload.get('future_event_count')}`",
        "",
        "## Saved forecast",
        (
            f"- Learned backend: `{forecast_payload.get('backend')}`"
            if forecast_payload
            else "- Learned backend: unavailable"
        ),
        (
            f"- Forecast summary: {forecast_payload.get('summary')}"
            if forecast_payload
            else "- Forecast summary: unavailable"
        ),
        "",
        "## Saved ranked comparison",
        f"- Top candidate: {top_candidate.get('label') or 'n/a'}",
        (
            f"- Top business-state summary: "
            f"{((top_candidate.get('business_state_change') or {}).get('summary') or 'n/a')}"
        ),
        "",
        "## Open in Studio",
        "```bash",
        f"vei ui serve --root {workspace_root} --host 127.0.0.1 --port 3056",
        "```",
        "",
        "## Bundle files",
        "- `workspace/context_snapshot.json`: saved workspace seed",
        "- `workspace/canonical_events.jsonl`: saved canonical timeline",
        "- `workspace/canonical_event_index.json`: saved searchable timeline index",
        "- `whatif_experiment_overview.md`: saved what-if summary",
        "- `whatif_reference_result.json`: saved learned forecast",
        "- `whatif_business_state_comparison.md`: saved candidate comparison",
        "- `clearwater_story_overview.md`: source synthetic story walkthrough",
        "",
    ]
    (output_root / "README.md").write_text("\n".join(lines), encoding="utf-8")


def validate_bundle(bundle_root: Path) -> list[str]:
    issues = validate_packaged_example_bundle(bundle_root)
    experiment_payload = json.loads(
        (bundle_root / EXPERIMENT_RESULT_FILE).read_text(encoding="utf-8")
    )
    forecast_path = str(
        ((experiment_payload.get("artifacts") or {}).get("forecast_json_path") or "")
    ).strip()
    if forecast_path != REFERENCE_FORECAST_FILE:
        issues.append(
            f"expected {REFERENCE_FORECAST_FILE} as the saved forecast, got {forecast_path!r}"
        )
    if not (bundle_root / REFERENCE_FORECAST_FILE).exists():
        issues.append(
            f"missing bundle artifact: {bundle_root / REFERENCE_FORECAST_FILE}"
        )
    if (bundle_root / HEURISTIC_FORECAST_FILE).exists():
        issues.append(
            f"heuristic baseline should stay debug-only and out of the saved bundle: "
            f"{bundle_root / HEURISTIC_FORECAST_FILE}"
        )

    workspace_root = bundle_root / WORKSPACE_DIRECTORY
    history_paths = canonical_history_paths(workspace_root / "context_snapshot.json")
    if not history_paths.events_path.exists():
        issues.append(f"missing bundle artifact: {history_paths.events_path}")
    if not history_paths.index_path.exists():
        issues.append(f"missing bundle artifact: {history_paths.index_path}")

    history_bundle = load_canonical_history_bundle(
        workspace_root / "context_snapshot.json"
    )
    if history_bundle is None:
        issues.append("missing canonical history bundle")
        return issues

    manifest_payload = json.loads(
        (workspace_root / EPISODE_MANIFEST_FILE).read_text(encoding="utf-8")
    )
    branch_event_id = str(manifest_payload.get("branch_event_id") or "").strip()
    branch_timestamp = str(manifest_payload.get("branch_timestamp") or "").strip()
    prior_rows = [
        row
        for row in history_bundle.index.rows
        if row.event_id != branch_event_id
        and (not branch_timestamp or row.timestamp <= branch_timestamp)
    ]
    if len(prior_rows) < 30:
        issues.append(
            f"expected at least 30 prior canonical events, found {len(prior_rows)}"
        )

    source_families = {
        str(row.metadata.get("source_family") or "").strip().lower()
        or str(row.provider or "").strip().lower()
        or str(row.surface or "").strip().lower()
        for row in prior_rows
        if (
            str(row.metadata.get("source_family") or "").strip()
            or str(row.provider or "").strip()
            or str(row.surface or "").strip()
        )
    }
    if len(source_families) < 3:
        issues.append(
            "expected at least 3 source families or domains in the saved timeline, "
            f"found {sorted(source_families)}"
        )
    return issues


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the repo-owned synthetic Clearwater what-if bundles."
    )
    parser.add_argument(
        "--bundle",
        action="append",
        default=None,
        help="Optional bundle slug to build. Pass multiple times to build a subset.",
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=DEFAULT_SOURCE_ROOT,
        help="Scratch root for the intermediate service-ops story workspaces.",
    )
    args = parser.parse_args()

    selected_specs = (
        [spec_by_bundle_slug(bundle_slug) for bundle_slug in args.bundle]
        if args.bundle
        else list(bundle_specs())
    )
    for spec in selected_specs:
        output_root = build_bundle(spec, source_root=args.source_root.resolve())
        print(f"built: {output_root}")


if __name__ == "__main__":
    main()
