from __future__ import annotations

import json
import shutil
import stat
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Iterable

from vei.context.models import ContextSnapshot, ContextSourceResult
from vei.context.providers.base import iso_now
from vei.context.providers.crm import capture_from_export as capture_crm_export
from vei.context.providers.gmail import capture_from_mbox
from vei.context.providers.google import capture_from_export as capture_google_export
from vei.context.providers.jira import capture_from_export as capture_jira_export
from vei.context.providers.salesforce import (
    capture_from_export as capture_salesforce_export,
)
from vei.context.providers.slack import capture_from_export as capture_slack_export
from vei.imports.api import normalize_identity_import_package


def extract_archive_if_needed(path: Path) -> Path:
    if not path.is_file():
        return path

    suffix_lower = path.name.lower()
    tmp = Path(tempfile.mkdtemp(prefix="vei_normalize_"))
    try:
        if suffix_lower.endswith(".zip"):
            with zipfile.ZipFile(path, "r") as archive:
                _extract_zip_archive(archive, tmp)
        elif suffix_lower.endswith((".tar.gz", ".tgz")):
            with tarfile.open(path, "r:gz") as archive:
                _extract_tar_archive(archive, tmp)
        elif suffix_lower.endswith(".tar"):
            with tarfile.open(path, "r:") as archive:
                _extract_tar_archive(archive, tmp)
        else:
            shutil.rmtree(tmp, ignore_errors=True)
            return path
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise

    children = [child for child in tmp.iterdir() if not child.name.startswith(".")]
    if len(children) == 1 and children[0].is_dir():
        return children[0]
    return tmp


def load_existing_snapshot(root: Path) -> ContextSnapshot | None:
    if root.is_file():
        if root.name == "context_snapshot.json":
            return snapshot_from_path(root)
        if root.suffix.lower() == ".json":
            try:
                return snapshot_from_path(root)
            except ValueError:
                return None
        return None
    candidate = root / "context_snapshot.json"
    if candidate.exists():
        return snapshot_from_path(candidate)
    return None


def snapshot_from_path(path: Path) -> ContextSnapshot:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("sources"), list):
        return ContextSnapshot.model_validate(payload)
    if isinstance(payload, dict) and isinstance(payload.get("threads"), list):
        return ContextSnapshot(
            organization_name=str(payload.get("organization_name") or "").strip(),
            organization_domain=str(payload.get("organization_domain") or "").strip(),
            captured_at=str(payload.get("captured_at") or iso_now()).strip(),
            sources=[
                ContextSourceResult(
                    provider="mail_archive",
                    captured_at=str(payload.get("captured_at") or iso_now()).strip(),
                    status="ok" if payload.get("threads") else "empty",
                    record_counts={
                        "threads": len(payload.get("threads", [])),
                        "actors": len(payload.get("actors", [])),
                    },
                    data={
                        "threads": payload.get("threads", []),
                        "actors": payload.get("actors", []),
                    },
                )
            ],
        )
    raise ValueError(f"unsupported snapshot payload: {path}")


def looks_like_import_package(root: Path) -> bool:
    if root.is_file():
        return False
    package_path = root / "package.json"
    if not package_path.exists():
        return False
    try:
        payload = json.loads(package_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and isinstance(payload.get("sources"), list)


def snapshot_from_import_package(root: Path) -> ContextSnapshot:
    artifacts = normalize_identity_import_package(root)
    package = artifacts.package
    bundle = artifacts.normalized_bundle
    if bundle is None:
        raise ValueError(f"import package could not be normalized: {package.name}")

    graphs = bundle.capability_graphs
    identity_graph = graphs.identity_graph
    doc_graph = graphs.doc_graph
    work_graph = graphs.work_graph
    revenue_graph = graphs.revenue_graph

    google_source = ContextSourceResult(
        provider="google",
        captured_at=iso_now(),
        status=(
            "ok"
            if doc_graph.documents or doc_graph.drive_shares or identity_graph.users
            else "empty"
        ),
        record_counts={
            "users": len(identity_graph.users),
            "documents": len(doc_graph.documents),
            "drive_shares": len(doc_graph.drive_shares),
        },
        data={
            "users": [
                {
                    "id": user.user_id,
                    "email": user.email,
                    "name": user.display_name
                    or f"{user.first_name} {user.last_name}".strip(),
                    "org_unit": user.department or "",
                    "suspended": user.status != "ACTIVE",
                    "is_admin": False,
                }
                for user in identity_graph.users
            ],
            "documents": [
                {
                    "doc_id": doc.doc_id,
                    "title": doc.title,
                    "body": doc.body,
                    "tags": list(doc.tags),
                }
                for doc in doc_graph.documents
            ],
            "drive_shares": [
                share.model_dump(mode="json") for share in doc_graph.drive_shares
            ],
        },
    )

    jira_source = ContextSourceResult(
        provider="jira",
        captured_at=iso_now(),
        status="ok" if work_graph.tickets else "empty",
        record_counts={"issues": len(work_graph.tickets)},
        data={
            "issues": [
                {
                    "ticket_id": ticket.ticket_id,
                    "title": ticket.title,
                    "status": ticket.status,
                    "assignee": ticket.assignee or "",
                    "description": ticket.description or "",
                    "updated": "",
                    "comments": [],
                }
                for ticket in work_graph.tickets
            ]
        },
    )

    revenue_provider = revenue_provider_name(package.metadata, package.sources)
    revenue_source = ContextSourceResult(
        provider=revenue_provider,
        captured_at=iso_now(),
        status=(
            "ok"
            if revenue_graph.deals or revenue_graph.contacts or revenue_graph.companies
            else "empty"
        ),
        record_counts={
            "companies": len(revenue_graph.companies),
            "contacts": len(revenue_graph.contacts),
            "deals": len(revenue_graph.deals),
        },
        data={
            "companies": [
                company.model_dump(mode="json") for company in revenue_graph.companies
            ],
            "contacts": [
                contact.model_dump(mode="json") for contact in revenue_graph.contacts
            ],
            "deals": [deal.model_dump(mode="json") for deal in revenue_graph.deals],
        },
    )

    return ContextSnapshot(
        organization_name=package.organization_name,
        organization_domain=package.organization_domain,
        captured_at=iso_now(),
        sources=merge_source_results([google_source, jira_source, revenue_source]),
        metadata={"import_package": package.name},
    )


def detect_export_sources(root: Path) -> list[ContextSourceResult]:
    results: list[ContextSourceResult] = []
    if (
        root.is_dir()
        and (root / "users.json").exists()
        and (root / "channels.json").exists()
    ):
        results.append(coerce_empty(capture_slack_export(root)))

    for mbox_path in gmail_export_paths(root):
        results.append(coerce_empty(capture_from_mbox(mbox_path)))

    for provider, loader in (
        ("jira", capture_jira_export),
        ("google", capture_google_export),
        ("crm", capture_crm_export),
        ("salesforce", capture_salesforce_export),
    ):
        export_path = provider_export_path(root, provider)
        if export_path is None:
            continue
        results.append(coerce_empty(loader(export_path)))
    return results


def merge_source_results(
    sources: list[ContextSourceResult],
) -> list[ContextSourceResult]:
    merged: dict[str, ContextSourceResult] = {}
    for source in sources:
        current = merged.get(source.provider)
        if current is None:
            merged[source.provider] = source
            continue
        merged[source.provider] = ContextSourceResult.model_validate(
            {
                **current.model_dump(mode="python"),
                "captured_at": max(current.captured_at, source.captured_at),
                "status": merge_status(current.status, source.status),
                "record_counts": merge_counts(
                    current.record_counts,
                    source.record_counts,
                ),
                "data": merge_data(current.data, source.data),
                "error": source.error or current.error,
            }
        )
    return list(merged.values())


def cleanup_temp_dir(path: Path, *, original: Path) -> None:
    if path == original:
        return
    tmp_dir = str(path.parent) if path != path.parent else str(path)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def revenue_provider_name(metadata: dict[str, Any], sources: Iterable[Any]) -> str:
    if str(metadata.get("crm_provider") or "").strip().lower() == "salesforce":
        return "salesforce"
    for source in sources:
        source_system = str(getattr(source, "source_system", "") or "").strip().lower()
        if "salesforce" in source_system:
            return "salesforce"
    return "crm"


def provider_export_path(root: Path, provider: str) -> Path | None:
    if root.is_file():
        return root if provider in root.name.lower() else None
    names = {
        "jira": ("jira.json", "jira.csv", "issues.json", "issues.csv"),
        "google": (
            "google.json",
            "google_docs.json",
            "google_docs.csv",
            "drive_export.json",
            "drive_export.csv",
            "google_drive_shares.csv",
        ),
        "crm": ("crm.json", "crm.csv", "crm_deals.json", "crm_deals.csv"),
        "salesforce": (
            "salesforce.json",
            "salesforce.csv",
            "salesforce_deals.json",
            "salesforce_deals.csv",
        ),
    }
    for name in names.get(provider, ()):
        candidate = root / name
        if candidate.exists():
            return candidate
    raw_dir = root / "raw"
    if raw_dir.is_dir():
        return provider_export_path(raw_dir, provider)
    return None


def gmail_export_paths(root: Path) -> list[Path]:
    if root.is_file():
        return [root] if root.suffix.lower() in {".mbox", ".mbx"} else []
    return sorted(root.rglob("*.mbox")) + sorted(root.rglob("*.mbx"))


def coerce_empty(source: ContextSourceResult) -> ContextSourceResult:
    if source.status == "ok" and not any(source.record_counts.values()):
        return source.model_copy(update={"status": "empty"})
    return source


def merge_status(left: str, right: str) -> str:
    priorities = {"error": 3, "partial": 2, "ok": 1, "empty": 0}
    return left if priorities.get(left, -1) >= priorities.get(right, -1) else right


def merge_counts(left: dict[str, int], right: dict[str, int]) -> dict[str, int]:
    merged = dict(left)
    for key, value in right.items():
        merged[key] = int(merged.get(key, 0)) + int(value)
    return merged


def merge_data(left: Any, right: Any) -> dict[str, Any]:
    left_mapping = payload_mapping(left)
    right_mapping = payload_mapping(right)
    merged = dict(left_mapping)
    for key, value in right_mapping.items():
        if key not in merged:
            merged[key] = value
            continue
        current = merged[key]
        if isinstance(current, list) and isinstance(value, list):
            merged[key] = current + value
            continue
        if isinstance(current, dict) and isinstance(value, dict):
            merged[key] = {**current, **value}
            continue
        merged[key] = value
    return merged


def payload_mapping(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="python")
    if isinstance(value, dict):
        return value
    return {}


def _extract_zip_archive(archive: zipfile.ZipFile, destination: Path) -> None:
    for member in archive.infolist():
        if not member.filename:
            continue
        if _zip_info_is_link(member):
            raise ValueError(f"unsafe archive member: {member.filename}")
        target = _safe_archive_destination(destination, member.filename)
        if member.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        with archive.open(member, "r") as source, target.open("wb") as sink:
            shutil.copyfileobj(source, sink)


def _extract_tar_archive(archive: tarfile.TarFile, destination: Path) -> None:
    for member in archive.getmembers():
        if not member.name:
            continue
        if not member.isfile() and not member.isdir():
            raise ValueError(f"unsafe archive member: {member.name}")
        target = _safe_archive_destination(destination, member.name)
        if member.isdir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        extracted = archive.extractfile(member)
        if extracted is None:
            raise ValueError(f"unsafe archive member: {member.name}")
        target.parent.mkdir(parents=True, exist_ok=True)
        with extracted, target.open("wb") as sink:
            shutil.copyfileobj(extracted, sink)


def _safe_archive_destination(destination: Path, member_name: str) -> Path:
    normalized_name = member_name.replace("\\", "/")
    member_path = Path(normalized_name)
    if member_path.is_absolute() or any(part == ".." for part in member_path.parts):
        raise ValueError(f"unsafe archive member: {member_name}")
    target = (destination / member_path).resolve()
    destination_root = destination.resolve()
    if target != destination_root and destination_root not in target.parents:
        raise ValueError(f"unsafe archive member: {member_name}")
    return target


def _zip_info_is_link(info: zipfile.ZipInfo) -> bool:
    mode = info.external_attr >> 16
    return stat.S_ISLNK(mode)
