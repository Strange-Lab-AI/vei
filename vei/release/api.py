from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Sequence

from vei.benchmark.api import run_benchmark_batch
from vei.benchmark.models import BenchmarkCaseSpec
from vei.corpus.api import generate_corpus
from vei.corpus.models import CorpusBundle
from vei.data.models import VEIDataset
from vei.data.rollout import rollout_procurement
from vei.quality.api import filter_workflow_corpus
from vei.quality.models import QualityFilterReport
from vei.release.models import (
    BenchmarkReleaseResult,
    DatasetReleaseResult,
    NightlyReleaseResult,
    ReleaseArtifact,
    ReleaseManifest,
)

DatasetKind = Literal["auto", "vei_dataset", "corpus", "quality_report"]


def build_release_version(
    *, prefix: str | None = None, when: datetime | None = None
) -> str:
    moment = when or datetime.now(timezone.utc)
    stamp = moment.strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{stamp}" if prefix else stamp


def export_dataset_release(
    *,
    input_path: Path,
    release_root: Path,
    version: str,
    label: str,
    dataset_kind: DatasetKind = "auto",
    attachments: Sequence[Path] | None = None,
    metadata: Dict[str, Any] | None = None,
) -> DatasetReleaseResult:
    payload = _load_json(input_path)
    normalized_kind, summary = _summarize_dataset_payload(payload, dataset_kind)
    release_dir = release_root / "datasets" / version / _slugify(label)
    data_dir = release_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    copied_path = data_dir / input_path.name
    shutil.copy2(input_path, copied_path)
    for attachment in attachments or []:
        if not attachment.exists():
            raise FileNotFoundError(f"attachment not found: {attachment}")
        shutil.copy2(attachment, data_dir / attachment.name)

    source = {
        "input_path": str(input_path),
        "dataset_kind": normalized_kind,
    }
    manifest = ReleaseManifest(
        release_id=f"dataset-{version}-{_slugify(label)}",
        version=version,
        kind="dataset",
        label=label,
        created_at_utc=_now_utc(),
        root_dir=str(release_dir),
        source=source,
        metadata={**summary, **dict(metadata or {})},
    )
    manifest_path = _write_release_manifest(release_dir, manifest)
    finalized = _finalize_manifest(release_dir, manifest_path)
    return DatasetReleaseResult(
        manifest=finalized,
        release_dir=release_dir,
        manifest_path=manifest_path,
    )


def snapshot_benchmark_release(
    *,
    benchmark_dir: Path,
    release_root: Path,
    version: str,
    label: str,
    metadata: Dict[str, Any] | None = None,
) -> BenchmarkReleaseResult:
    if not benchmark_dir.exists():
        raise FileNotFoundError(f"benchmark directory not found: {benchmark_dir}")

    release_dir = release_root / "benchmarks" / version / _slugify(label)
    release_dir.mkdir(parents=True, exist_ok=True)
    copied = _copy_benchmark_tree(benchmark_dir, release_dir / "snapshot")
    summary = _benchmark_summary(benchmark_dir)
    manifest = ReleaseManifest(
        release_id=f"benchmark-{version}-{_slugify(label)}",
        version=version,
        kind="benchmark",
        label=label,
        created_at_utc=_now_utc(),
        root_dir=str(release_dir),
        source={"benchmark_dir": str(benchmark_dir), "copied_files": copied},
        metadata={**summary, **dict(metadata or {})},
    )
    manifest_path = _write_release_manifest(release_dir, manifest)
    finalized = _finalize_manifest(release_dir, manifest_path)
    return BenchmarkReleaseResult(
        manifest=finalized,
        release_dir=release_dir,
        manifest_path=manifest_path,
    )


def run_nightly_release(
    *,
    release_root: Path,
    workspace_root: Path,
    version: str,
    seed: int = 42042,
    environment_count: int = 25,
    scenarios_per_environment: int = 20,
    realism_threshold: float = 0.55,
    rollout_episodes: int = 3,
    rollout_scenario: str = "multi_channel",
    benchmark_scenarios: Sequence[str] | None = None,
    llm_model: str | None = None,
    llm_provider: str = "auto",
) -> NightlyReleaseResult:
    benchmark_names = list(benchmark_scenarios or ["multi_channel"])
    workspace = workspace_root / version
    workspace.mkdir(parents=True, exist_ok=True)

    corpus_path = workspace / "corpus" / "generated_corpus.json"
    corpus_bundle = generate_corpus(
        seed=seed,
        environment_count=environment_count,
        scenarios_per_environment=scenarios_per_environment,
    )
    _write_json(corpus_path, corpus_bundle.model_dump())

    quality_report = filter_workflow_corpus(
        corpus_bundle.workflows,
        realism_threshold=realism_threshold,
    )
    quality_path = workspace / "corpus" / "filter_report.json"
    _write_json(
        quality_path,
        {
            **quality_report.model_dump(),
            "summary": {
                "accepted": len(quality_report.accepted),
                "rejected": len(quality_report.rejected),
            },
        },
    )

    rollout_dataset = rollout_procurement(
        episodes=rollout_episodes,
        seed=seed,
        scenario_name=rollout_scenario,
    )
    rollout_path = workspace / "datasets" / "rollout.json"
    _write_json(rollout_path, rollout_dataset.model_dump())

    corpus_release = export_dataset_release(
        input_path=corpus_path,
        release_root=release_root,
        version=version,
        label="corpus",
        dataset_kind="corpus",
        attachments=[quality_path],
        metadata={
            "realism_threshold": realism_threshold,
            "accepted_workflows": len(quality_report.accepted),
            "rejected_workflows": len(quality_report.rejected),
        },
    )
    rollout_release = export_dataset_release(
        input_path=rollout_path,
        release_root=release_root,
        version=version,
        label="rollout",
        dataset_kind="vei_dataset",
        metadata={
            "episodes": rollout_episodes,
            "scenario_name": rollout_scenario,
        },
    )

    benchmark_dir = workspace / "benchmarks" / "scripted"
    scripted_specs = [
        BenchmarkCaseSpec(
            runner="scripted",
            scenario_name=scenario_name,
            seed=seed,
            artifacts_dir=benchmark_dir / scenario_name,
            branch=scenario_name,
            score_mode="full",
        )
        for scenario_name in benchmark_names
    ]
    scripted_batch = run_benchmark_batch(
        scripted_specs,
        run_id=f"scripted-{version}",
        output_dir=benchmark_dir,
    )
    benchmark_release = snapshot_benchmark_release(
        benchmark_dir=benchmark_dir,
        release_root=release_root,
        version=version,
        label="scripted-benchmark",
        metadata={
            "runner": "scripted",
            "summary": scripted_batch.summary.model_dump(),
        },
    )

    llm_release: BenchmarkReleaseResult | None = None
    if llm_model:
        llm_dir = workspace / "benchmarks" / "llm"
        llm_specs = [
            BenchmarkCaseSpec(
                runner="llm",
                scenario_name=scenario_name,
                seed=seed,
                artifacts_dir=llm_dir / scenario_name,
                branch=scenario_name,
                score_mode="full",
                model=llm_model,
                provider=llm_provider,
                max_steps=18,
            )
            for scenario_name in benchmark_names
        ]
        llm_batch = run_benchmark_batch(
            llm_specs,
            run_id=f"llm-{version}",
            output_dir=llm_dir,
        )
        llm_release = snapshot_benchmark_release(
            benchmark_dir=llm_dir,
            release_root=release_root,
            version=version,
            label="llm-benchmark",
            metadata={
                "runner": "llm",
                "model": llm_model,
                "provider": llm_provider,
                "summary": llm_batch.summary.model_dump(),
            },
        )

    nightly_dir = release_root / "nightly" / version
    nightly_dir.mkdir(parents=True, exist_ok=True)
    nightly_manifest = ReleaseManifest(
        release_id=f"nightly-{version}",
        version=version,
        kind="nightly",
        label="nightly",
        created_at_utc=_now_utc(),
        root_dir=str(nightly_dir),
        source={
            "workspace_root": str(workspace),
            "seed": seed,
        },
        metadata={
            "environment_count": environment_count,
            "scenarios_per_environment": scenarios_per_environment,
            "realism_threshold": realism_threshold,
            "rollout_episodes": rollout_episodes,
            "rollout_scenario": rollout_scenario,
            "benchmark_scenarios": benchmark_names,
            "corpus_release": corpus_release.manifest.release_id,
            "rollout_release": rollout_release.manifest.release_id,
            "benchmark_release": benchmark_release.manifest.release_id,
            "llm_benchmark_release": (
                llm_release.manifest.release_id if llm_release is not None else None
            ),
        },
    )
    _write_json(
        nightly_dir / "summary.json",
        {
            "corpus_release": corpus_release.manifest.model_dump(mode="json"),
            "rollout_release": rollout_release.manifest.model_dump(mode="json"),
            "benchmark_release": benchmark_release.manifest.model_dump(mode="json"),
            "llm_benchmark_release": (
                llm_release.manifest.model_dump(mode="json")
                if llm_release is not None
                else None
            ),
        },
    )
    nightly_manifest_path = _write_release_manifest(nightly_dir, nightly_manifest)
    finalized_nightly = _finalize_manifest(nightly_dir, nightly_manifest_path)
    return NightlyReleaseResult(
        manifest=finalized_nightly,
        release_dir=nightly_dir,
        manifest_path=nightly_manifest_path,
        corpus_release=corpus_release,
        rollout_release=rollout_release,
        benchmark_release=benchmark_release,
        llm_benchmark_release=llm_release,
    )


def _summarize_dataset_payload(
    payload: Dict[str, Any], dataset_kind: DatasetKind
) -> tuple[str, Dict[str, Any]]:
    kind = dataset_kind
    if kind == "auto":
        if "events" in payload:
            kind = "vei_dataset"
        elif "environments" in payload and "workflows" in payload:
            kind = "corpus"
        elif "accepted" in payload and "rejected" in payload:
            kind = "quality_report"
        else:
            kind = "vei_dataset"

    if kind == "vei_dataset":
        dataset = VEIDataset.model_validate(payload)
        channels = sorted({event.channel for event in dataset.events})
        return kind, {
            "events": len(dataset.events),
            "channels": channels,
            "time_span_ms": (
                max((event.time_ms for event in dataset.events), default=0)
                - min((event.time_ms for event in dataset.events), default=0)
                if dataset.events
                else 0
            ),
        }
    if kind == "corpus":
        bundle = CorpusBundle.model_validate(payload)
        return kind, {
            "environments": len(bundle.environments),
            "workflows": len(bundle.workflows),
            "seed": bundle.seed,
        }
    if kind == "quality_report":
        report = QualityFilterReport.model_validate(payload)
        return kind, {
            "accepted": len(report.accepted),
            "rejected": len(report.rejected),
        }
    raise ValueError(f"unsupported dataset kind: {dataset_kind}")


def _benchmark_summary(benchmark_dir: Path) -> Dict[str, Any]:
    summary_path = benchmark_dir / "benchmark_summary.json"
    if summary_path.exists():
        payload = _load_json(summary_path)
        return {
            "run_id": payload.get("run_id"),
            "summary": payload.get("summary", {}),
        }
    aggregate_path = benchmark_dir / "aggregate_results.json"
    if aggregate_path.exists():
        payload = _load_json(aggregate_path)
        return {"aggregate_results": len(payload) if isinstance(payload, list) else 0}
    return {}


def _copy_benchmark_tree(source_dir: Path, destination_dir: Path) -> int:
    copied = 0
    for path in sorted(source_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(source_dir)
        target = destination_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        copied += 1
    return copied


def _write_release_manifest(release_dir: Path, manifest: ReleaseManifest) -> Path:
    manifest_path = release_dir / "manifest.json"
    _write_json(manifest_path, manifest.model_dump(mode="json"))
    return manifest_path


def _finalize_manifest(release_dir: Path, manifest_path: Path) -> ReleaseManifest:
    manifest = ReleaseManifest.model_validate(_load_json(manifest_path))
    artifacts = _collect_artifacts(release_dir)
    manifest = manifest.model_copy(update={"artifacts": artifacts})
    _write_json(manifest_path, manifest.model_dump(mode="json"))
    checksums_path = release_dir / "CHECKSUMS.txt"
    checksums_path.write_text(_format_checksums(artifacts), encoding="utf-8")
    artifacts = _collect_artifacts(release_dir)
    manifest = manifest.model_copy(update={"artifacts": artifacts})
    _write_json(manifest_path, manifest.model_dump(mode="json"))
    return manifest


def _collect_artifacts(release_dir: Path) -> list[ReleaseArtifact]:
    artifacts: list[ReleaseArtifact] = []
    for path in sorted(release_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(release_dir)
        artifacts.append(
            ReleaseArtifact(
                path=str(rel),
                sha256=_sha256(path),
                size_bytes=path.stat().st_size,
                kind=_artifact_kind(path),
            )
        )
    return artifacts


def _format_checksums(artifacts: Iterable[ReleaseArtifact]) -> str:
    lines = [f"{artifact.sha256}  {artifact.path}" for artifact in artifacts]
    return "\n".join(lines) + "\n"


def _artifact_kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if path.name == "manifest.json":
        return "manifest"
    if path.name == "CHECKSUMS.txt":
        return "checksums"
    if suffix == ".jsonl":
        return "trace"
    if suffix == ".log":
        return "log"
    return "data"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _load_json(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object payload in {path}")
    return payload


def _slugify(value: str) -> str:
    safe = [ch.lower() if ch.isalnum() else "-" for ch in value.strip()]
    collapsed = "".join(safe)
    while "--" in collapsed:
        collapsed = collapsed.replace("--", "-")
    return collapsed.strip("-") or "release"


def _now_utc() -> str:
    override = os.environ.get("VEI_RELEASE_CREATED_AT")
    if override:
        return override
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


__all__ = [
    "BenchmarkReleaseResult",
    "DatasetReleaseResult",
    "NightlyReleaseResult",
    "ReleaseArtifact",
    "ReleaseManifest",
    "build_release_version",
    "export_dataset_release",
    "run_nightly_release",
    "snapshot_benchmark_release",
]
