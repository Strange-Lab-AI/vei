from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from vei.whatif_filenames import CONTEXT_SNAPSHOT_FILE, EPISODE_MANIFEST_FILE
from ._helpers import load_episode_snapshot
from .episode import load_episode_manifest


def resolve_whatif_rosetta_dir(workspace_root: Path) -> Path | None:
    candidates: list[Path] = [workspace_root / "rosetta"]
    manifest_source_dir = _resolve_manifest_source_dir(
        workspace_root,
        expected_source="enron",
    )
    if manifest_source_dir is not None:
        candidates.append(manifest_source_dir)
    configured = os.environ.get("VEI_WHATIF_ROSETTA_DIR", "").strip()
    if configured:
        candidates.append(Path(configured).expanduser())
    candidates.append(
        workspace_root.parent
        / "human_v_llm_messages_experiment"
        / "experiments"
        / "org_simulator"
        / "rosetta"
    )
    candidates.append(
        workspace_root.parent.parent
        / "human_v_llm_messages_experiment"
        / "experiments"
        / "org_simulator"
        / "rosetta"
    )
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if (resolved / "enron_rosetta_events_metadata.parquet").exists():
            return resolved
    return None


def resolve_whatif_mail_archive_path(workspace_root: Path) -> Path | None:
    candidates: list[Path] = []
    manifest_source_dir = _resolve_manifest_source_dir(
        workspace_root,
        expected_source="mail_archive",
    )
    if manifest_source_dir is not None:
        candidates.append(manifest_source_dir)
    saved_workspace_archive = _workspace_saved_mail_archive_path(workspace_root)
    if saved_workspace_archive is not None:
        candidates.append(saved_workspace_archive)
    configured = os.environ.get("VEI_WHATIF_SOURCE_DIR", "").strip()
    if configured:
        candidates.append(Path(configured).expanduser())
    archive_override = os.environ.get("VEI_WHATIF_ARCHIVE_PATH", "").strip()
    if archive_override:
        candidates.append(Path(archive_override).expanduser())
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if _looks_like_mail_archive_payload(resolved):
            return resolved
    return None


def resolve_whatif_company_history_path(workspace_root: Path) -> Path | None:
    candidates: list[Path] = []
    manifest_source_dir = _resolve_manifest_source_dir(
        workspace_root,
        expected_source="company_history",
    )
    if manifest_source_dir is not None:
        candidates.append(manifest_source_dir)
    saved_workspace_bundle = _workspace_saved_company_history_path(workspace_root)
    if saved_workspace_bundle is not None:
        candidates.append(saved_workspace_bundle)
    configured = os.environ.get("VEI_WHATIF_SOURCE_DIR", "").strip()
    if configured:
        candidates.append(Path(configured).expanduser())
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if _looks_like_non_mail_history_bundle_payload(resolved):
            return resolved
    return None


def resolve_whatif_source_path(
    workspace_root: Path,
    *,
    requested_source: str | None = None,
) -> tuple[str, Path] | None:
    normalized = (requested_source or "").strip().lower()
    if not normalized or normalized == "auto":
        normalized = (
            (
                _workspace_whatif_source_hint(workspace_root)
                or os.environ.get("VEI_WHATIF_SOURCE")
                or "auto"
            )
            .strip()
            .lower()
        )
    if normalized in {"", "auto", "company_history"}:
        company_history_path = resolve_whatif_company_history_path(workspace_root)
        if company_history_path is not None:
            return ("company_history", company_history_path)
    if normalized in {"", "auto", "mail_archive"}:
        archive_path = resolve_whatif_mail_archive_path(workspace_root)
        if archive_path is not None:
            return ("mail_archive", archive_path)
    if normalized in {"", "auto", "enron"}:
        rosetta_dir = resolve_whatif_rosetta_dir(workspace_root)
        if rosetta_dir is not None:
            return ("enron", rosetta_dir)
    return None


def _resolve_manifest_source_dir(
    workspace_root: Path,
    *,
    expected_source: str,
) -> Path | None:
    manifest_path = workspace_root / EPISODE_MANIFEST_FILE
    if not manifest_path.exists():
        return None
    try:
        manifest = load_episode_manifest(workspace_root)
    except ValueError:
        return None
    if manifest.source != expected_source:
        return None
    candidate = Path(manifest.source_dir).expanduser()
    if not candidate.exists():
        return None
    if (
        expected_source == "enron"
        and not (candidate / "enron_rosetta_events_metadata.parquet").exists()
    ):
        return None
    return candidate


def _workspace_saved_mail_archive_path(workspace_root: Path) -> Path | None:
    candidate = workspace_root / CONTEXT_SNAPSHOT_FILE
    if _looks_like_mail_archive_payload(candidate):
        return candidate
    return None


def _workspace_saved_company_history_path(workspace_root: Path) -> Path | None:
    candidate = workspace_root / CONTEXT_SNAPSHOT_FILE
    if _looks_like_non_mail_history_bundle_payload(candidate):
        return candidate
    return None


def _workspace_whatif_source_hint(workspace_root: Path) -> str | None:
    manifest_path = workspace_root / EPISODE_MANIFEST_FILE
    if manifest_path.exists():
        try:
            manifest = load_episode_manifest(workspace_root)
        except ValueError:
            manifest = None
        if manifest is not None and manifest.source:
            normalized_source = str(manifest.source).strip().lower()
            if normalized_source == "enron":
                if resolve_whatif_rosetta_dir(workspace_root) is not None:
                    return "enron"
                if _workspace_saved_mail_archive_path(workspace_root) is not None:
                    return "mail_archive"
            return normalized_source
    if _workspace_saved_mail_archive_path(workspace_root) is not None:
        return "mail_archive"
    if _workspace_saved_company_history_path(workspace_root) is not None:
        return "company_history"
    if (workspace_root / "rosetta" / "enron_rosetta_events_metadata.parquet").exists():
        return "enron"
    return None


def _looks_like_mail_archive_payload(path: Path) -> bool:
    payload = _read_history_payload(path)
    if payload is None:
        return False
    if isinstance(payload.get("threads"), list):
        return True
    sources = payload.get("sources", [])
    if not isinstance(sources, list):
        return False
    for source in sources:
        if not isinstance(source, dict):
            continue
        provider = str(source.get("provider", "")).strip().lower()
        if provider in {"mail_archive", "gmail"}:
            return True
    return False


def _looks_like_non_mail_history_bundle_payload(path: Path) -> bool:
    payload = _read_history_payload(path)
    if payload is None:
        return False
    sources = payload.get("sources", [])
    if not isinstance(sources, list):
        return False
    for source in sources:
        if not isinstance(source, dict):
            continue
        provider = str(source.get("provider", "")).strip().lower()
        if provider and provider not in {"mail_archive", "gmail"}:
            return True
    return False


def _read_history_payload(path: Path) -> dict[str, Any] | None:
    snapshot_path = _history_snapshot_file(path)
    if snapshot_path is None or snapshot_path.suffix.lower() != ".json":
        return None
    try:
        payload = load_episode_snapshot(snapshot_path)
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("threads"), list):
        return payload
    if isinstance(payload.get("sources"), list):
        return payload
    return None


def _history_snapshot_file(path: Path) -> Path | None:
    if not path.exists():
        return None
    if path.is_file():
        return path
    candidate = path / CONTEXT_SNAPSHOT_FILE
    if candidate.exists():
        return candidate
    return None


__all__ = [
    "resolve_whatif_company_history_path",
    "resolve_whatif_mail_archive_path",
    "resolve_whatif_rosetta_dir",
    "resolve_whatif_source_path",
]
