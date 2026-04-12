from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from vei import __version__ as vei_version
from vei.blueprint import BlueprintAsset, compile_blueprint, get_facade_plugin


def build_reproducibility_record(
    *,
    seed: int,
    blueprint_asset_path: str | Path | None,
    contract_path: str | Path | None,
) -> dict[str, Any]:
    blueprint_path = (
        Path(blueprint_asset_path).expanduser().resolve()
        if blueprint_asset_path is not None
        else None
    )
    contract_file = (
        Path(contract_path).expanduser().resolve()
        if contract_path is not None
        else None
    )
    facade_versions: dict[str, str] = {}
    if blueprint_path is not None and blueprint_path.exists():
        asset = BlueprintAsset.model_validate_json(
            blueprint_path.read_text(encoding="utf-8")
        )
        compiled = compile_blueprint(asset)
        facade_versions = {
            facade.name: str(get_facade_plugin(facade.name).version or vei_version)
            for facade in compiled.facades
        }

    record = {
        "seed": int(seed),
        "vei_version": vei_version,
        "blueprint_hash": _file_hash(blueprint_path),
        "contract_version": _file_hash(contract_file),
        "facade_versions": facade_versions,
    }
    record["record_id"] = _payload_hash(record)
    return record


def merge_reproducibility_metadata(
    metadata: dict[str, Any],
    *,
    seed: int,
    blueprint_asset_path: str | Path | None,
    contract_path: str | Path | None,
) -> dict[str, Any]:
    next_metadata = dict(metadata)
    next_metadata["reproducibility"] = build_reproducibility_record(
        seed=seed,
        blueprint_asset_path=blueprint_asset_path,
        contract_path=contract_path,
    )
    return next_metadata


def _file_hash(path: Path | None) -> str:
    if path is None or not path.exists():
        return ""
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]
