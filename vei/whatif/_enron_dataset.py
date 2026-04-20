from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_DATA_ROOT = _ROOT / "data" / "enron"
_REPO_SAMPLE_ROSETTA_DIR = _DATA_ROOT / "rosetta"
_RELEASE_MANIFEST_PATH = _DATA_ROOT / "full_dataset_release.json"
_DATASET_MARKER_FILENAME = "enron_rosetta_dataset.json"


@dataclass(frozen=True)
class EnronFullDatasetRelease:
    version: str
    release_tag: str
    asset_name: str
    asset_url: str
    sha256: str
    cache_root: str
    extract_root: str
    rosetta_relpath: str
    source_relpath: str
    raw_relpath: str

    @property
    def resolved_cache_root(self) -> Path:
        return Path(self.cache_root).expanduser()

    @property
    def resolved_extract_root(self) -> Path:
        return self.resolved_cache_root / self.extract_root

    @property
    def resolved_rosetta_dir(self) -> Path:
        return self.resolved_extract_root / self.rosetta_relpath

    @property
    def resolved_source_dir(self) -> Path:
        return self.resolved_extract_root / self.source_relpath

    @property
    def resolved_raw_dir(self) -> Path:
        return self.resolved_extract_root / self.raw_relpath


def repo_enron_sample_rosetta_dir() -> Path:
    return _REPO_SAMPLE_ROSETTA_DIR


def enron_dataset_marker_path(rosetta_dir: Path) -> Path:
    return rosetta_dir / _DATASET_MARKER_FILENAME


def load_enron_dataset_marker(rosetta_dir: Path) -> dict[str, object] | None:
    marker_path = enron_dataset_marker_path(rosetta_dir)
    if not marker_path.exists():
        return None
    try:
        payload = json.loads(marker_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def is_enron_sample_rosetta_dir(rosetta_dir: Path) -> bool:
    marker = load_enron_dataset_marker(rosetta_dir)
    if isinstance(marker, dict):
        return str(marker.get("dataset_kind") or "").strip().lower() == "sample"
    return rosetta_dir.expanduser().resolve() == _REPO_SAMPLE_ROSETTA_DIR.resolve()


def is_enron_full_rosetta_dir(rosetta_dir: Path) -> bool:
    marker = load_enron_dataset_marker(rosetta_dir)
    if isinstance(marker, dict):
        return str(marker.get("dataset_kind") or "").strip().lower() == "full"
    return False


def load_enron_full_dataset_release() -> EnronFullDatasetRelease:
    payload = json.loads(_RELEASE_MANIFEST_PATH.read_text(encoding="utf-8"))
    return EnronFullDatasetRelease(
        version=str(payload["version"]),
        release_tag=str(payload["release_tag"]),
        asset_name=str(payload["asset_name"]),
        asset_url=str(payload["asset_url"]),
        sha256=str(payload["sha256"]),
        cache_root=str(payload["cache_root"]),
        extract_root=str(payload["extract_root"]),
        rosetta_relpath=str(payload["rosetta_relpath"]),
        source_relpath=str(payload["source_relpath"]),
        raw_relpath=str(payload["raw_relpath"]),
    )


def resolve_cached_full_enron_rosetta_dir() -> Path | None:
    try:
        release = load_enron_full_dataset_release()
    except FileNotFoundError:
        return None
    candidate = release.resolved_rosetta_dir
    if (candidate / "enron_rosetta_events_metadata.parquet").exists():
        return candidate.resolve()
    return None


def require_full_enron_rosetta_dir(rosetta_dir: Path, *, purpose: str) -> Path:
    resolved = rosetta_dir.expanduser().resolve()
    metadata_path = resolved / "enron_rosetta_events_metadata.parquet"
    if not metadata_path.exists():
        raise RuntimeError(f"No Enron Rosetta archive found for {purpose}: {resolved}")
    if is_enron_sample_rosetta_dir(resolved):
        raise RuntimeError(
            f"{purpose} requires the full Enron archive. "
            "Run `make fetch-enron-full` or set VEI_WHATIF_ROSETTA_DIR."
        )
    return resolved


__all__ = [
    "EnronFullDatasetRelease",
    "enron_dataset_marker_path",
    "is_enron_full_rosetta_dir",
    "is_enron_sample_rosetta_dir",
    "load_enron_dataset_marker",
    "load_enron_full_dataset_release",
    "repo_enron_sample_rosetta_dir",
    "require_full_enron_rosetta_dir",
    "resolve_cached_full_enron_rosetta_dir",
]
