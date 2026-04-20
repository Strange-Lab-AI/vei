from __future__ import annotations

import hashlib
import json
import tarfile
from pathlib import Path

import pytest

from scripts import fetch_enron_full_dataset as fetch_script


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _write_manifest(
    root: Path,
    *,
    asset_url: str,
    sha256: str,
) -> Path:
    manifest_path = root / "full_dataset_release.json"
    manifest_path.write_text(
        json.dumps(
            {
                "version": "test-v1",
                "release_tag": "enron-dataset-test-v1",
                "asset_name": "enron-full-test.tar.gz",
                "asset_url": asset_url,
                "sha256": sha256,
                "cache_root": str(root / "cache"),
                "extract_root": "extract",
                "rosetta_relpath": "rosetta",
                "source_relpath": "source",
                "raw_relpath": "raw",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest_path


def _write_release_asset(root: Path) -> Path:
    staging_root = root / "staging"
    rosetta_root = staging_root / "rosetta"
    source_root = staging_root / "source"
    raw_root = staging_root / "raw"
    rosetta_root.mkdir(parents=True)
    source_root.mkdir(parents=True)
    raw_root.mkdir(parents=True)
    (rosetta_root / "enron_rosetta_events_metadata.parquet").write_bytes(b"meta")
    (rosetta_root / "enron_rosetta_events_content.parquet").write_bytes(b"content")
    (source_root / "enron_rosetta_source.parquet").write_bytes(b"source")
    (raw_root / "enron_mail_20150507.tar.gz").write_bytes(b"raw")
    asset_path = root / "enron-full-test.tar.gz"
    with tarfile.open(asset_path, "w:gz") as archive:
        archive.add(rosetta_root, arcname="rosetta")
        archive.add(source_root, arcname="source")
        archive.add(raw_root, arcname="raw")
    return asset_path


def test_fetch_full_dataset_downloads_extracts_and_marks_full_archive(
    tmp_path: Path,
) -> None:
    asset_path = _write_release_asset(tmp_path)
    manifest_path = _write_manifest(
        tmp_path,
        asset_url=asset_path.resolve().as_uri(),
        sha256=_sha256(asset_path),
    )

    payload = fetch_script.fetch_full_dataset(manifest_path=manifest_path)

    rosetta_dir = Path(payload["rosetta_dir"])
    assert (rosetta_dir / "enron_rosetta_events_metadata.parquet").exists()
    assert (rosetta_dir / "enron_rosetta_events_content.parquet").exists()
    marker_payload = json.loads(
        (rosetta_dir / "enron_rosetta_dataset.json").read_text(encoding="utf-8")
    )
    assert marker_payload["dataset_kind"] == "full"


def test_fetch_full_dataset_rejects_checksum_mismatch(tmp_path: Path) -> None:
    asset_path = _write_release_asset(tmp_path)
    manifest_path = _write_manifest(
        tmp_path,
        asset_url=asset_path.resolve().as_uri(),
        sha256="deadbeef",
    )

    with pytest.raises(RuntimeError, match="checksum mismatch"):
        fetch_script.fetch_full_dataset(manifest_path=manifest_path)
