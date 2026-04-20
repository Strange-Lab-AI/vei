#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tarfile
import tempfile
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

from vei.whatif._enron_dataset import (
    enron_dataset_marker_path,
    load_enron_full_dataset_release,
)

DEFAULT_REPOSITORY = "strangeloopcanon/vei"
ALLOWED_URL_SCHEMES = {"https", "file"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and extract the full Enron dataset release asset."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Optional release manifest override.",
    )
    return parser.parse_args()


def _load_release(manifest_path: Path | None):
    if manifest_path is None:
        return load_enron_full_dataset_release()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    from vei.whatif._enron_dataset import EnronFullDatasetRelease

    return EnronFullDatasetRelease(
        version=str(payload["version"]),
        release_tag=str(payload["release_tag"]),
        asset_name=str(payload["asset_name"]),
        asset_url=str(payload.get("asset_url") or ""),
        sha256=str(payload["sha256"]),
        cache_root=str(payload["cache_root"]),
        extract_root=str(payload["extract_root"]),
        rosetta_relpath=str(payload["rosetta_relpath"]),
        source_relpath=str(payload["source_relpath"]),
        raw_relpath=str(payload["raw_relpath"]),
    )


def _asset_url(release) -> str:
    override = os.environ.get("VEI_ENRON_FULL_DATASET_URL", "").strip()
    if override:
        return override
    if release.asset_url.strip():
        return release.asset_url.strip()
    return (
        f"https://github.com/{DEFAULT_REPOSITORY}/releases/download/"
        f"{release.release_tag}/{release.asset_name}"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _validated_download_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme.strip().lower()
    if scheme not in ALLOWED_URL_SCHEMES:
        raise RuntimeError(
            f"unsupported dataset download scheme: {scheme or '<missing>'}"
        )
    return url


def _download_asset(*, url: str, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    safe_url = _validated_download_url(url)
    with urllib.request.urlopen(safe_url) as response, target_path.open("wb") as handle:  # nosec B310
        shutil.copyfileobj(response, handle)


def _extract_member_path(root: Path, member_name: str) -> Path:
    destination = (root / member_name).resolve()
    try:
        destination.relative_to(root.resolve())
    except ValueError as exc:
        raise RuntimeError(f"unsafe archive member: {member_name}") from exc
    return destination


def _extract_release_archive(*, asset_path: Path, target_root: Path) -> None:
    with tarfile.open(asset_path, "r:gz") as archive:
        for member in archive.getmembers():
            if member.islnk() or member.issym():
                raise RuntimeError(f"unsupported archive link member: {member.name}")
            _extract_member_path(target_root, member.name)
            archive.extract(member, target_root)


def _write_full_marker(rosetta_dir: Path, *, version: str) -> None:
    marker_path = enron_dataset_marker_path(rosetta_dir)
    marker_path.write_text(
        json.dumps(
            {
                "dataset_kind": "full",
                "version": version,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def fetch_full_dataset(*, manifest_path: Path | None = None) -> dict[str, str]:
    release = _load_release(manifest_path)
    if not release.sha256.strip():
        raise RuntimeError(
            "full_dataset_release.json is missing the sha256 for the release asset"
        )

    rosetta_dir = release.resolved_rosetta_dir
    if (rosetta_dir / "enron_rosetta_events_metadata.parquet").exists():
        _write_full_marker(rosetta_dir, version=release.version)
        return _result_payload(release=release)

    asset_path = release.resolved_cache_root / release.asset_name
    expected_sha = release.sha256.strip().lower()
    if asset_path.exists():
        if _sha256(asset_path) != expected_sha:
            asset_path.unlink()

    if not asset_path.exists():
        _download_asset(url=_asset_url(release), target_path=asset_path)

    observed_sha = _sha256(asset_path)
    if observed_sha != expected_sha:
        raise RuntimeError(
            f"downloaded asset checksum mismatch: expected {expected_sha}, got {observed_sha}"
        )

    extract_root = release.resolved_extract_root
    extract_root.parent.mkdir(parents=True, exist_ok=True)
    temp_root = Path(
        tempfile.mkdtemp(
            prefix=f"{release.version}-",
            dir=str(extract_root.parent),
        )
    )
    try:
        _extract_release_archive(asset_path=asset_path, target_root=temp_root)
        if extract_root.exists():
            shutil.rmtree(extract_root)
        temp_root.rename(extract_root)
    except Exception:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise

    _write_full_marker(release.resolved_rosetta_dir, version=release.version)
    return _result_payload(release=release)


def _result_payload(*, release) -> dict[str, str]:
    return {
        "version": release.version,
        "release_tag": release.release_tag,
        "asset_name": release.asset_name,
        "cache_root": str(release.resolved_cache_root),
        "extract_root": str(release.resolved_extract_root),
        "rosetta_dir": str(release.resolved_rosetta_dir),
        "source_dir": str(release.resolved_source_dir),
        "raw_dir": str(release.resolved_raw_dir),
    }


def main() -> None:
    args = _parse_args()
    payload = fetch_full_dataset(manifest_path=args.manifest)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
