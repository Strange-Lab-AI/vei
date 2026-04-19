from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from .storage import PreparedDataset, finalize_processed_dataset
from .utils import make_split_map, parse_timestamp_series

logger = logging.getLogger(__name__)


def prepare_vei_context_dataset(
    *,
    snapshot_paths: Sequence[str | Path],
    output_dir: str | Path,
    diff_paths: Sequence[str | Path] | None = None,
    seed: int = 7,
    train_fraction: float = 0.7,
    val_fraction: float = 0.15,
) -> PreparedDataset:
    logger.info(
        "vei_context_preparation_start",
        extra={"snapshot_count": len(snapshot_paths)},
    )
    snapshots = [_load_json(path) for path in snapshot_paths]
    if not snapshots:
        raise ValueError("at least one context snapshot is required")

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for snapshot in snapshots:
        organization_name = str(snapshot.get("organization_name", "unknown_org"))
        grouped[organization_name].append(snapshot)
    grouped = {
        organization_name: sorted(items, key=lambda item: str(item.get("captured_at", "")))
        for organization_name, items in grouped.items()
    }

    diff_map = _build_diff_map(diff_paths or [])
    rows: list[dict[str, object]] = []
    issue_statuses: set[str] = set()
    issue_priorities: set[str] = set()
    issue_types: set[str] = set()
    okta_statuses: set[str] = set()

    for organization_name, org_snapshots in grouped.items():
        sorted_snapshots = org_snapshots
        timestamps = parse_timestamp_series(
            pd.Series([item["captured_at"] for item in sorted_snapshots])
        )

        for index, snapshot in enumerate(sorted_snapshots):
            provider_map = _provider_map(snapshot)
            slack = provider_map.get("slack", {})
            jira = provider_map.get("jira", {})
            google = provider_map.get("google", {})
            okta = provider_map.get("okta", {})

            row: dict[str, Any] = {
                "episode_id": organization_name,
                "step_idx": index,
                "timestamp": str(snapshot.get("captured_at", "")),
                "delta_t_s": 0.0
                if index == 0
                else float((timestamps.iloc[index] - timestamps.iloc[index - 1]).total_seconds()),
                "done": index == len(sorted_snapshots) - 1,
                "split": "train",
                "action_name": "__none__",
                "meta__organization_domain": str(snapshot.get("organization_domain", "")),
            }

            slack_channels = _source_items(slack, "channels")
            slack_users = _source_items(slack, "users")
            row["slack_channel_count"] = len(slack_channels)
            row["slack_unread_total"] = sum(
                _safe_int(item.get("unread", 0)) for item in slack_channels
            )
            row["slack_message_count"] = sum(
                _list_length(item.get("messages", [])) for item in slack_channels
            )
            row["slack_user_count"] = len(slack_users)
            row["slack_deleted_user_count"] = sum(bool(item.get("deleted")) for item in slack_users)
            row["slack_bot_count"] = sum(bool(item.get("is_bot")) for item in slack_users)
            row["slack_provider_status"] = str(slack.get("status", "__missing__"))

            jira_issues = _source_items(jira, "issues")
            jira_projects = _source_items(jira, "projects")
            row["jira_issue_count"] = len(jira_issues)
            row["jira_project_count"] = len(jira_projects)
            row["jira_provider_status"] = str(jira.get("status", "__missing__"))
            for issue in jira_issues:
                issue_statuses.add(str(issue.get("status", "__missing__")).lower())
                issue_priorities.add(str(issue.get("priority", "__missing__")).lower())
                issue_types.add(str(issue.get("issue_type", "__missing__")).lower())

            google_users = _source_items(google, "users")
            google_documents = _source_items(google, "documents")
            row["google_user_count"] = len(google_users)
            row["google_suspended_count"] = sum(
                bool(item.get("suspended")) for item in google_users
            )
            row["google_admin_count"] = sum(bool(item.get("is_admin")) for item in google_users)
            row["google_org_unit_count"] = len(
                {
                    str(item.get("org_unit", ""))
                    for item in google_users
                    if str(item.get("org_unit", ""))
                }
            )
            row["google_document_count"] = len(google_documents)
            row["google_shared_document_count"] = sum(
                bool(item.get("shared")) for item in google_documents
            )
            row["google_provider_status"] = str(google.get("status", "__missing__"))

            okta_users = _source_items(okta, "users")
            okta_groups = _source_items(okta, "groups")
            okta_apps = _source_items(okta, "applications")
            row["okta_user_count"] = len(okta_users)
            row["okta_group_count"] = len(okta_groups)
            row["okta_application_count"] = len(okta_apps)
            row["okta_provider_status"] = str(okta.get("status", "__missing__"))
            for user in okta_users:
                raw_profile = user.get("profile")
                profile: dict[str, Any] = raw_profile if isinstance(raw_profile, dict) else {}
                status = str(user.get("status", profile.get("status", "__missing__"))).lower()
                okta_statuses.add(status)

            diff = diff_map.get(str(snapshot.get("captured_at", "")), {})
            row["diff_added_total"] = _safe_int(diff.get("added_total", 0))
            row["diff_removed_total"] = _safe_int(diff.get("removed_total", 0))
            row["diff_changed_total"] = _safe_int(diff.get("changed_total", 0))
            changed_by_domain = diff.get("changed_by_domain", {})
            if not isinstance(changed_by_domain, dict):
                changed_by_domain = {}
            for domain, count in changed_by_domain.items():
                row[f"diff_changed__{domain}"] = int(count)

            rows.append(row)

    steps = pd.DataFrame(rows)
    for status in sorted(issue_statuses):
        column = f"jira_status__{status}"
        steps[column] = steps.apply(
            lambda row, status=status: _count_field_for_snapshot(
                grouped[row["episode_id"]][int(row["step_idx"])],
                "jira",
                "issues",
                "status",
                status,
            ),
            axis=1,
        )
    for priority in sorted(issue_priorities):
        column = f"jira_priority__{priority}"
        steps[column] = steps.apply(
            lambda row, priority=priority: _count_field_for_snapshot(
                grouped[row["episode_id"]][int(row["step_idx"])],
                "jira",
                "issues",
                "priority",
                priority,
            ),
            axis=1,
        )
    for issue_type in sorted(issue_types):
        column = f"jira_type__{issue_type}"
        steps[column] = steps.apply(
            lambda row, issue_type=issue_type: _count_field_for_snapshot(
                grouped[row["episode_id"]][int(row["step_idx"])],
                "jira",
                "issues",
                "issue_type",
                issue_type,
            ),
            axis=1,
        )
    for status in sorted(okta_statuses):
        column = f"okta_status__{status}"
        steps[column] = steps.apply(
            lambda row, status=status: _count_okta_status(
                grouped[row["episode_id"]][int(row["step_idx"])], status
            ),
            axis=1,
        )

    split_map = make_split_map(
        steps["episode_id"].tolist(),
        seed=seed,
        train_fraction=train_fraction,
        val_fraction=val_fraction,
    )
    steps["split"] = steps["episode_id"].map(split_map)

    observation_numeric_columns = [
        "delta_t_s",
        "slack_channel_count",
        "slack_unread_total",
        "slack_message_count",
        "slack_user_count",
        "slack_deleted_user_count",
        "slack_bot_count",
        "jira_issue_count",
        "jira_project_count",
        "google_user_count",
        "google_suspended_count",
        "google_admin_count",
        "google_org_unit_count",
        "google_document_count",
        "google_shared_document_count",
        "okta_user_count",
        "okta_group_count",
        "okta_application_count",
        *[column for column in steps.columns if column.startswith("jira_status__")],
        *[column for column in steps.columns if column.startswith("jira_priority__")],
        *[column for column in steps.columns if column.startswith("jira_type__")],
        *[column for column in steps.columns if column.startswith("okta_status__")],
    ]
    observation_categorical_columns = [
        "slack_provider_status",
        "jira_provider_status",
        "google_provider_status",
        "okta_provider_status",
    ]
    auxiliary_numeric_targets = [
        "diff_added_total",
        "diff_removed_total",
        "diff_changed_total",
        *[column for column in steps.columns if column.startswith("diff_changed__")],
    ]

    logger.info(
        "vei_context_preparation_complete",
        extra={"rows": len(steps), "episodes": steps["episode_id"].nunique()},
    )
    return finalize_processed_dataset(
        raw_steps=steps,
        output_dir=output_dir,
        dataset_kind="vei_context",
        observation_numeric_columns=observation_numeric_columns,
        observation_categorical_columns=observation_categorical_columns,
        action_numeric_columns=[],
        action_categorical_columns=[],
        auxiliary_numeric_targets=auxiliary_numeric_targets,
        metadata_columns=["meta__organization_domain"],
        notes={"snapshot_count": len(snapshot_paths)},
    )


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))


def _build_diff_map(paths: Sequence[str | Path]) -> dict[str, dict[str, Any]]:
    diff_map: dict[str, dict[str, Any]] = {}
    for path in paths:
        payload = _load_json(path)
        entries = payload.get("entries", [])
        if not isinstance(entries, list):
            entries = []
        changed_by_domain: Counter[str] = Counter()
        for entry in entries:
            if isinstance(entry, dict):
                changed_by_domain[str(entry.get("domain", "unknown"))] += 1
        diff_map[str(payload.get("after_captured_at", ""))] = {
            "added_total": sum(
                1 for entry in entries if isinstance(entry, dict) and entry.get("kind") == "added"
            ),
            "removed_total": sum(
                1 for entry in entries if isinstance(entry, dict) and entry.get("kind") == "removed"
            ),
            "changed_total": sum(
                1 for entry in entries if isinstance(entry, dict) and entry.get("kind") == "changed"
            ),
            "changed_by_domain": dict(changed_by_domain),
        }
    return diff_map


def _source_items(source: object, key: str) -> list[dict[str, Any]]:
    if not isinstance(source, dict):
        return []
    value = source.get("data", {})
    if not isinstance(value, dict):
        return []
    items = value.get(key, [])
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _provider_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    sources = snapshot.get("sources", [])
    if not isinstance(sources, list):
        return {}

    provider_map: dict[str, dict[str, Any]] = {}
    for source in sources:
        if not isinstance(source, dict):
            continue
        provider_name = str(source.get("provider", ""))
        provider_map[provider_name] = source
    return provider_map


def _count_field_for_snapshot(
    snapshot: dict[str, Any],
    provider_name: str,
    container_key: str,
    field_name: str,
    expected_value: str,
) -> int:
    provider_map = _provider_map(snapshot)
    items = _source_items(provider_map.get(provider_name, {}), container_key)
    return sum(str(item.get(field_name, "__missing__")).lower() == expected_value for item in items)


def _count_okta_status(snapshot: dict[str, Any], status: str) -> int:
    provider_map = _provider_map(snapshot)
    items = _source_items(provider_map.get("okta", {}), "users")
    count = 0
    for item in items:
        raw_profile = item.get("profile")
        profile: dict[str, Any] = raw_profile if isinstance(raw_profile, dict) else {}
        item_status = str(item.get("status", profile.get("status", "__missing__"))).lower()
        if item_status == status:
            count += 1
    return count


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _list_length(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0
