from __future__ import annotations

from typing import Optional

from vei.blueprint.models import (
    BlueprintAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintCommGraphAsset,
    BlueprintDocGraphAsset,
    BlueprintDocumentAsset,
    BlueprintIdentityApplicationAsset,
    BlueprintIdentityGraphAsset,
    BlueprintIdentityGroupAsset,
    BlueprintIdentityUserAsset,
    BlueprintSlackChannelAsset,
    BlueprintSlackMessageAsset,
    BlueprintTicketAsset,
    BlueprintWorkGraphAsset,
)

from .models import ContextSnapshot, ContextSourceResult


def hydrate_snapshot_to_blueprint(
    snapshot: ContextSnapshot,
    *,
    scenario_name: str = "captured_context",
    workflow_name: str = "captured_context",
) -> BlueprintAsset:
    slack_source = snapshot.source_for("slack")
    jira_source = snapshot.source_for("jira")
    google_source = snapshot.source_for("google")
    okta_source = snapshot.source_for("okta")

    comm_graph = _build_comm_graph(slack_source)
    doc_graph = _build_doc_graph(google_source)
    work_graph = _build_work_graph(jira_source)
    identity_graph = _build_identity_graph(okta_source, google_source)

    facades = _infer_facades(slack_source, jira_source, google_source, okta_source)

    return BlueprintAsset(
        name=f"{snapshot.organization_name.lower().replace(' ', '_')}.blueprint",
        title=snapshot.organization_name,
        description=f"Context capture for {snapshot.organization_name}",
        scenario_name=scenario_name,
        family_name="captured_context",
        workflow_name=workflow_name,
        workflow_variant="captured_context",
        requested_facades=facades,
        capability_graphs=BlueprintCapabilityGraphsAsset(
            organization_name=snapshot.organization_name,
            organization_domain=snapshot.organization_domain,
            scenario_brief=f"Captured operational state of {snapshot.organization_name}",
            comm_graph=comm_graph,
            doc_graph=doc_graph,
            work_graph=work_graph,
            identity_graph=identity_graph,
            metadata={
                "source": "context_capture",
                "captured_at": snapshot.captured_at,
                "providers": [s.provider for s in snapshot.sources],
            },
        ),
        metadata={
            "source": "context_capture",
            "captured_at": snapshot.captured_at,
        },
    )


def _build_comm_graph(
    slack_source: Optional[ContextSourceResult],
) -> Optional[BlueprintCommGraphAsset]:
    if not slack_source or slack_source.status == "error":
        return None

    channels_data = slack_source.data.get("channels", [])
    if not channels_data:
        return None

    channels: list[BlueprintSlackChannelAsset] = []
    for ch in channels_data:
        if not isinstance(ch, dict):
            continue
        messages = [
            BlueprintSlackMessageAsset(
                ts=str(m.get("ts", "")),
                user=str(m.get("user", "unknown")),
                text=str(m.get("text", "")),
                thread_ts=m.get("thread_ts"),
            )
            for m in ch.get("messages", [])
            if isinstance(m, dict)
        ]
        channels.append(
            BlueprintSlackChannelAsset(
                channel=str(ch.get("channel", "")),
                messages=messages,
                unread=int(ch.get("unread", 0) or 0),
            )
        )

    return BlueprintCommGraphAsset(
        slack_initial_message="Context captured from live Slack workspace.",
        slack_channels=channels,
    )


def _build_doc_graph(
    google_source: Optional[ContextSourceResult],
) -> Optional[BlueprintDocGraphAsset]:
    if not google_source or google_source.status == "error":
        return None

    docs_data = google_source.data.get("documents", [])
    if not docs_data:
        return None

    documents = [
        BlueprintDocumentAsset(
            doc_id=str(d.get("doc_id", f"doc-{i}")),
            title=str(d.get("title", "")),
            body=str(d.get("body", "")),
            tags=[t for t in [d.get("mime_type", "")] if t],
        )
        for i, d in enumerate(docs_data)
        if isinstance(d, dict)
    ]

    return BlueprintDocGraphAsset(documents=documents)


def _build_work_graph(
    jira_source: Optional[ContextSourceResult],
) -> Optional[BlueprintWorkGraphAsset]:
    if not jira_source or jira_source.status == "error":
        return None

    issues_data = jira_source.data.get("issues", [])
    if not issues_data:
        return None

    tickets = [
        BlueprintTicketAsset(
            ticket_id=str(issue.get("ticket_id", f"ISSUE-{i}")),
            title=str(issue.get("title", "")),
            status=str(issue.get("status", "open")),
            assignee=str(issue.get("assignee", "unassigned")),
            description=str(issue.get("description", "")),
        )
        for i, issue in enumerate(issues_data)
        if isinstance(issue, dict)
    ]

    return BlueprintWorkGraphAsset(tickets=tickets)


def _build_identity_graph(
    okta_source: Optional[ContextSourceResult],
    google_source: Optional[ContextSourceResult],
) -> Optional[BlueprintIdentityGraphAsset]:
    users: list[BlueprintIdentityUserAsset] = []
    groups: list[BlueprintIdentityGroupAsset] = []
    apps: list[BlueprintIdentityApplicationAsset] = []

    if okta_source and okta_source.status != "error":
        for u in okta_source.data.get("users", []):
            if not isinstance(u, dict):
                continue
            profile = u.get("profile") or {}
            users.append(
                BlueprintIdentityUserAsset(
                    user_id=str(u.get("id", "")),
                    email=str(profile.get("login", profile.get("email", ""))),
                    first_name=str(profile.get("firstName", "")),
                    last_name=str(profile.get("lastName", "")),
                    display_name=str(
                        profile.get("displayName", profile.get("firstName", ""))
                    ),
                    department=str(profile.get("department", "")),
                    title=str(profile.get("title", "")),
                    status=str(u.get("status", "active")),
                    groups=u.get("group_ids", []),
                )
            )
        for g in okta_source.data.get("groups", []):
            if not isinstance(g, dict):
                continue
            profile = g.get("profile") or {}
            groups.append(
                BlueprintIdentityGroupAsset(
                    group_id=str(g.get("id", "")),
                    name=str(profile.get("name", g.get("id", ""))),
                    members=g.get("members", []),
                )
            )
        for a in okta_source.data.get("applications", []):
            if not isinstance(a, dict):
                continue
            apps.append(
                BlueprintIdentityApplicationAsset(
                    app_id=str(a.get("id", "")),
                    name=str(a.get("label", a.get("name", ""))),
                    status=str(a.get("status", "active")),
                    assignments=a.get("assignments", []),
                )
            )

    if google_source and google_source.status != "error":
        for u in google_source.data.get("users", []):
            if not isinstance(u, dict):
                continue
            if any(existing.email == u.get("email") for existing in users):
                continue
            full_name = str(u.get("name", ""))
            name_parts = full_name.split(" ", 1)
            users.append(
                BlueprintIdentityUserAsset(
                    user_id=str(u.get("id", "")),
                    email=str(u.get("email", "")),
                    first_name=name_parts[0],
                    last_name=name_parts[1] if len(name_parts) > 1 else "",
                    display_name=full_name,
                    department=str(u.get("org_unit", "")),
                    status="suspended" if u.get("suspended") else "active",
                )
            )

    if not users and not groups and not apps:
        return None

    return BlueprintIdentityGraphAsset(
        users=users,
        groups=groups,
        applications=apps,
    )


def _infer_facades(
    slack_source: Optional[ContextSourceResult],
    jira_source: Optional[ContextSourceResult],
    google_source: Optional[ContextSourceResult],
    okta_source: Optional[ContextSourceResult],
) -> list[str]:
    facades = []
    if slack_source and slack_source.status != "error":
        facades.append("slack")
    if jira_source and jira_source.status != "error":
        facades.extend(["jira", "servicedesk"])
    if google_source and google_source.status != "error":
        facades.append("docs")
    if okta_source and okta_source.status != "error":
        facades.append("identity")
    return facades
