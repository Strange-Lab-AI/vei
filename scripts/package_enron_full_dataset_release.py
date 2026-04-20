#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tarfile
import tempfile
from pathlib import Path

from vei.whatif._enron_dataset import load_enron_full_dataset_release


def _default_full_dirs() -> tuple[Path, Path, Path]:
    release = load_enron_full_dataset_release()
    return (
        release.resolved_rosetta_dir,
        release.resolved_source_dir,
        release.resolved_raw_dir,
    )


def _parse_args() -> argparse.Namespace:
    default_rosetta_dir, default_source_dir, default_raw_dir = _default_full_dirs()
    parser = argparse.ArgumentParser(
        description="Package the full Enron dataset into a release asset."
    )
    parser.add_argument(
        "--rosetta-dir",
        type=Path,
        default=default_rosetta_dir,
        help="Full Rosetta parquet directory.",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=default_source_dir,
        help="Full normalized source directory.",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=default_raw_dir,
        help="Full raw archive directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("_vei_out/releases/enron-full-dataset"),
        help="Directory for the packaged release asset and checksum.",
    )
    return parser.parse_args()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def package_release(
    *,
    rosetta_dir: Path,
    source_dir: Path,
    raw_dir: Path,
    output_dir: Path,
) -> dict[str, str]:
    release = load_enron_full_dataset_release()
    for source_path, label in (
        (rosetta_dir, "rosetta"),
        (source_dir, "source"),
        (raw_dir, "raw"),
    ):
        if not source_path.exists():
            raise FileNotFoundError(
                f"Missing full Enron {label} directory: {source_path}. "
                "Fetch the full dataset first with `make fetch-enron-full`."
            )
    output_dir.mkdir(parents=True, exist_ok=True)
    asset_path = output_dir / release.asset_name
    checksum_path = output_dir / f"{release.asset_name}.sha256"

    temp_root = Path(tempfile.mkdtemp(prefix="enron-full-release-"))
    try:
        for source_path, relative_name in (
            (rosetta_dir, "rosetta"),
            (source_dir, "source"),
            (raw_dir, "raw"),
        ):
            target_path = temp_root / relative_name
            shutil.copytree(source_path, target_path)

        marker_path = temp_root / "rosetta" / "enron_rosetta_dataset.json"
        marker_path.write_text(
            json.dumps(
                {
                    "dataset_kind": "full",
                    "version": release.version,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        with tarfile.open(asset_path, "w:gz") as archive:
            for relative_name in ("rosetta", "source", "raw"):
                archive.add(temp_root / relative_name, arcname=relative_name)
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)

    digest = _sha256(asset_path)
    checksum_path.write_text(f"{digest}  {asset_path.name}\n", encoding="utf-8")
    return {
        "asset_path": str(asset_path.resolve()),
        "checksum_path": str(checksum_path.resolve()),
        "sha256": digest,
    }


def main() -> None:
    args = _parse_args()
    payload = package_release(
        rosetta_dir=args.rosetta_dir.expanduser().resolve(),
        source_dir=args.source_dir.expanduser().resolve(),
        raw_dir=args.raw_dir.expanduser().resolve(),
        output_dir=args.output_dir.expanduser().resolve(),
    )
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
