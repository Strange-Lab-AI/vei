from __future__ import annotations

from typing import Optional

from vei.blueprint.models import (
    BlueprintAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintCommGraphAsset,
    BlueprintCrmCompanyAsset,
    BlueprintCrmContactAsset,
    BlueprintCrmDealAsset,
    BlueprintDocGraphAsset,
    BlueprintDocumentAsset,
    BlueprintIdentityApplicationAsset,
    BlueprintIdentityGraphAsset,
    BlueprintIdentityGroupAsset,
    BlueprintKnowledgeGraphAsset,
    BlueprintIdentityUserAsset,
    BlueprintMailMessageAsset,
    BlueprintMailThreadAsset,
    BlueprintRevenueGraphAsset,
    BlueprintSlackChannelAsset,
    BlueprintSlackMessageAsset,
    BlueprintTicketAsset,
    BlueprintWorkGraphAsset,
)

from ._hydrate_inputs import build_hydrate_source_inputs
from ._hydrate_knowledge import build_knowledge_graph
from .models import ContextSnapshot
from .models import (
    CrmSourceData,
    GmailSourceData,
    GoogleSourceData,
    MailArchiveSourceData,
    OktaSourceData,
    SlackSourceData,
    TeamsSourceData,
    JiraSourceData,
)


def hydrate_snapshot_to_blueprint(
    snapshot: ContextSnapshot,
    *,
    scenario_name: str = "captured_context",
    workflow_name: str = "captured_context",
) -> BlueprintAsset:
    inputs = build_hydrate_source_inputs(snapshot)

    comm_graph = _build_comm_graph(inputs.slack_data, inputs.teams_data)
    mail_threads = _build_mail_threads(inputs.gmail_data, inputs.mail_archive_data)
    if comm_graph and mail_threads:
        comm_graph.mail_threads = mail_threads
    elif mail_threads:
        comm_graph = BlueprintCommGraphAsset(mail_threads=mail_threads)

    doc_graph = _build_doc_graph(inputs.google_data)
    work_graph = _build_work_graph(inputs.jira_data)
    revenue_graph = _build_revenue_graph(
        inputs.crm_data,
        inputs.salesforce_data,
        crm_provider=("crm" if inputs.crm_data is not None else None),
        salesforce_provider=(
            "salesforce" if inputs.salesforce_data is not None else None
        ),
    )
    identity_graph = _build_identity_graph(
        inputs.okta_data,
        inputs.google_data,
        inputs.mail_archive_data,
    )
    knowledge_graph = build_knowledge_graph(
        snapshot,
        slack_data=inputs.slack_data,
        gmail_data=inputs.gmail_data,
        mail_archive_data=inputs.mail_archive_data,
        google_data=inputs.google_data,
        jira_data=inputs.jira_data,
        notion_data=inputs.notion_data,
        linear_data=inputs.linear_data,
        granola_data=inputs.granola_data,
    )

    facades = _infer_facades(
        inputs.slack_data,
        inputs.jira_data,
        inputs.google_data,
        inputs.okta_data,
        inputs.gmail_data,
        inputs.mail_archive_data,
        inputs.teams_data,
        inputs.crm_data,
        inputs.salesforce_data,
        knowledge_graph,
    )

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
            revenue_graph=revenue_graph,
            knowledge_graph=knowledge_graph,
            metadata={
                "source": "context_capture",
                "captured_at": snapshot.captured_at,
                "providers": inputs.providers,
            },
        ),
        metadata={
            "source": "context_capture",
            "captured_at": snapshot.captured_at,
        },
    )


def _build_comm_graph(
    slack_data: SlackSourceData | None,
    teams_data: TeamsSourceData | None = None,
) -> Optional[BlueprintCommGraphAsset]:
    channels: list[BlueprintSlackChannelAsset] = []

    if slack_data is not None:
        for ch in slack_data.channels:
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

    if teams_data is not None:
        for ch in teams_data.channels:
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

    if not channels:
        return None

    return BlueprintCommGraphAsset(
        slack_initial_message="Context captured from live workspace.",
        slack_channels=channels,
    )


def _build_mail_from_gmail(
    gmail_data: GmailSourceData | None,
) -> list[BlueprintMailThreadAsset]:
    if gmail_data is None:
        return []

    threads_data = gmail_data.threads
    result: list[BlueprintMailThreadAsset] = []

    for thread in threads_data:
        if not isinstance(thread, dict):
            continue
        messages = thread.get("messages", [])
        mail_messages = [
            BlueprintMailMessageAsset(
                from_address=str(m.get("from", "")),
                to_address=str(m.get("to", "")),
                subject=str(m.get("subject", "")),
                body_text=str(m.get("snippet", "")),
                unread=bool(m.get("unread", False)),
            )
            for m in messages
            if isinstance(m, dict)
        ]
        if not mail_messages:
            continue
        labels = []
        if messages and isinstance(messages[0], dict):
            labels = messages[0].get("labels", [])
        category = "internal"
        if any(lb in labels for lb in ["CATEGORY_PROMOTIONS", "CATEGORY_SOCIAL"]):
            category = "external"
        elif any(lb in labels for lb in ["IMPORTANT", "STARRED"]):
            category = "important"

        result.append(
            BlueprintMailThreadAsset(
                thread_id=str(thread.get("thread_id", "")),
                title=str(thread.get("subject", "")),
                category=category,
                messages=mail_messages,
            )
        )

    return result


def _build_mail_from_archive(
    mail_archive_data: MailArchiveSourceData | None,
) -> list[BlueprintMailThreadAsset]:
    if mail_archive_data is None:
        return []

    result: list[BlueprintMailThreadAsset] = []
    for thread in mail_archive_data.threads:
        if not isinstance(thread, dict):
            continue
        mail_messages: list[BlueprintMailMessageAsset] = []
        for message in thread.get("messages", []):
            if not isinstance(message, dict):
                continue
            raw_time_ms = message.get("time_ms")
            mail_messages.append(
                BlueprintMailMessageAsset(
                    from_address=str(message.get("from", "")),
                    to_address=str(message.get("to", "")),
                    subject=str(message.get("subj", message.get("subject", ""))),
                    body_text=str(
                        message.get(
                            "body_text",
                            message.get("snippet", message.get("content", "")),
                        )
                    ),
                    unread=bool(message.get("unread", False)),
                    time_ms=(
                        int(raw_time_ms)
                        if isinstance(raw_time_ms, (int, float, str))
                        else None
                    ),
                )
            )
        if not mail_messages:
            continue
        result.append(
            BlueprintMailThreadAsset(
                thread_id=str(thread.get("thread_id", "")),
                title=str(thread.get("title", thread.get("subject", ""))),
                category=str(thread.get("category", "archive")),
                messages=mail_messages,
            )
        )
    return result


def _build_mail_threads(
    gmail_data: GmailSourceData | None,
    mail_archive_data: MailArchiveSourceData | None,
) -> list[BlueprintMailThreadAsset]:
    archive_threads = _build_mail_from_archive(mail_archive_data)
    gmail_threads = _build_mail_from_gmail(gmail_data)
    if not archive_threads:
        return gmail_threads
    if not gmail_threads:
        return archive_threads
    merged: dict[str, BlueprintMailThreadAsset] = {}
    for index, thread in enumerate(archive_threads):
        key = thread.thread_id or f"archive-thread-{index}"
        merged[key] = thread
    for index, thread in enumerate(gmail_threads):
        key = thread.thread_id or f"gmail-thread-{index}"
        merged.setdefault(key, thread)
    return list(merged.values())


def _build_doc_graph(
    google_data: GoogleSourceData | None,
) -> Optional[BlueprintDocGraphAsset]:
    if google_data is None:
        return None

    docs_data = google_data.documents
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
    jira_data: JiraSourceData | None,
) -> Optional[BlueprintWorkGraphAsset]:
    if jira_data is None:
        return None

    issues_data = jira_data.issues
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
    okta_data: OktaSourceData | None,
    google_data: GoogleSourceData | None,
    mail_archive_data: MailArchiveSourceData | None = None,
) -> Optional[BlueprintIdentityGraphAsset]:
    users: list[BlueprintIdentityUserAsset] = []
    groups: list[BlueprintIdentityGroupAsset] = []
    apps: list[BlueprintIdentityApplicationAsset] = []

    if okta_data is not None:
        for u in okta_data.users:
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
        for g in okta_data.groups:
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
        for a in okta_data.applications:
            if not isinstance(a, dict):
                continue
            apps.append(
                BlueprintIdentityApplicationAsset(
                    app_id=str(a.get("id", "")),
                    label=str(a.get("label", a.get("name", ""))),
                    status=str(a.get("status", "active")),
                    assignments=a.get("assignments", []),
                )
            )

    if google_data is not None:
        for u in google_data.users:
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

    if mail_archive_data is not None:
        _append_mail_archive_users(users, mail_archive_data)

    if not users and not groups and not apps:
        return None

    return BlueprintIdentityGraphAsset(
        users=users,
        groups=groups,
        applications=apps,
    )


def _build_revenue_graph(
    crm_data: CrmSourceData | None,
    salesforce_data: CrmSourceData | None = None,
    *,
    crm_provider: str | None = None,
    salesforce_provider: str | None = None,
) -> Optional[BlueprintRevenueGraphAsset]:
    sources = [data for data in (crm_data, salesforce_data) if data is not None]
    if not sources:
        return None

    companies = [
        BlueprintCrmCompanyAsset(
            id=str(item.get("id", item.get("company_id", f"COMP-{index}"))),
            name=str(item.get("name", "")),
            domain=str(item.get("domain", "")),
        )
        for data in sources
        for index, item in enumerate(data.companies)
        if isinstance(item, dict)
    ]
    contacts = [
        BlueprintCrmContactAsset(
            id=str(item.get("id", item.get("contact_id", f"CONTACT-{index}"))),
            email=str(item.get("email", "")),
            first_name=str(item.get("first_name", "")),
            last_name=str(item.get("last_name", "")),
            company_id=(str(item.get("company_id", "")).strip() or None),
        )
        for data in sources
        for index, item in enumerate(data.contacts)
        if isinstance(item, dict)
    ]
    deals = [
        BlueprintCrmDealAsset(
            id=str(item.get("id", item.get("deal_id", f"DEAL-{index}"))),
            name=str(item.get("name", item.get("title", ""))),
            amount=_crm_amount(item.get("amount", item.get("amount_usd", 0.0))),
            stage=str(item.get("stage", "open")),
            owner=str(item.get("owner", "")),
            contact_id=(str(item.get("contact_id", "")).strip() or None),
            company_id=(str(item.get("company_id", "")).strip() or None),
        )
        for data in sources
        for index, item in enumerate(data.deals)
        if isinstance(item, dict)
    ]

    if not companies and not contacts and not deals:
        return None

    source_providers = [
        provider
        for provider in (crm_provider, salesforce_provider)
        if provider is not None
    ]
    return BlueprintRevenueGraphAsset(
        companies=_dedupe_crm_companies(companies),
        contacts=_dedupe_crm_contacts(contacts),
        deals=_dedupe_crm_deals(deals),
        metadata={
            "source_provider": source_providers[0],
            "source_providers": source_providers,
        },
    )


def _crm_amount(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    negative = text.startswith("(") and text.endswith(")")
    cleaned = (
        text.replace("$", "").replace(",", "").replace("(", "").replace(")", "").strip()
    )
    try:
        amount = float(cleaned)
    except ValueError:
        return 0.0
    if negative:
        return -amount
    return amount


def _dedupe_crm_companies(
    companies: list[BlueprintCrmCompanyAsset],
) -> list[BlueprintCrmCompanyAsset]:
    deduped: dict[str, BlueprintCrmCompanyAsset] = {}
    for company in companies:
        deduped.setdefault(company.id, company)
    return list(deduped.values())


def _dedupe_crm_contacts(
    contacts: list[BlueprintCrmContactAsset],
) -> list[BlueprintCrmContactAsset]:
    deduped: dict[str, BlueprintCrmContactAsset] = {}
    for contact in contacts:
        deduped.setdefault(contact.id, contact)
    return list(deduped.values())


def _dedupe_crm_deals(
    deals: list[BlueprintCrmDealAsset],
) -> list[BlueprintCrmDealAsset]:
    deduped: dict[str, BlueprintCrmDealAsset] = {}
    for deal in deals:
        deduped.setdefault(deal.id, deal)
    return list(deduped.values())


def _infer_facades(
    slack_data: SlackSourceData | None,
    jira_data: JiraSourceData | None,
    google_data: GoogleSourceData | None,
    okta_data: OktaSourceData | None,
    gmail_data: GmailSourceData | None = None,
    mail_archive_data: MailArchiveSourceData | None = None,
    teams_data: TeamsSourceData | None = None,
    crm_data: CrmSourceData | None = None,
    salesforce_data: CrmSourceData | None = None,
    knowledge_graph: BlueprintKnowledgeGraphAsset | None = None,
) -> list[str]:
    facades: list[str] = []
    if slack_data is not None:
        facades.append("slack")
    if teams_data is not None:
        if "slack" not in facades:
            facades.append("slack")
    if jira_data is not None:
        facades.extend(["jira", "servicedesk"])
    if google_data is not None:
        facades.append("docs")
    if gmail_data is not None or mail_archive_data is not None:
        if "mail" not in facades:
            facades.append("mail")
    if mail_archive_data is not None and "identity" not in facades:
        facades.append("identity")
    if okta_data is not None:
        facades.append("identity")
    if crm_data is not None or salesforce_data is not None:
        facades.append("crm")
    if knowledge_graph is not None:
        facades.append("knowledge")
    return facades


def _append_mail_archive_users(
    users: list[BlueprintIdentityUserAsset],
    payload: MailArchiveSourceData,
) -> None:
    seen = {item.email.lower() for item in users if item.email}
    actors = payload.actors
    for actor in actors:
        if not isinstance(actor, dict):
            continue
        email = str(actor.get("email", "")).strip()
        if not email or email.lower() in seen:
            continue
        first_name, last_name = _split_actor_name(
            str(actor.get("display_name", "")) or email
        )
        users.append(
            BlueprintIdentityUserAsset(
                user_id=str(actor.get("actor_id", email)),
                email=email,
                first_name=first_name,
                last_name=last_name,
                login=email,
                display_name=(
                    str(actor.get("display_name", "")).strip()
                    or f"{first_name} {last_name}".strip()
                ),
                department=str(actor.get("department", "")) or None,
                title=str(actor.get("title", "")) or None,
                status="ACTIVE",
            )
        )
        seen.add(email.lower())

    if actors:
        return

    derived: dict[str, BlueprintIdentityUserAsset] = {}
    for thread in payload.threads:
        if not isinstance(thread, dict):
            continue
        for message in thread.get("messages", []):
            if not isinstance(message, dict):
                continue
            for key in ("from", "to"):
                email = str(message.get(key, "")).strip()
                lowered = email.lower()
                if not email or lowered in seen or lowered in derived:
                    continue
                first_name, last_name = _split_actor_name(email)
                derived[lowered] = BlueprintIdentityUserAsset(
                    user_id=email,
                    email=email,
                    first_name=first_name,
                    last_name=last_name,
                    login=email,
                    display_name=f"{first_name} {last_name}".strip(),
                    status="ACTIVE",
                )
    users.extend(derived.values())


def _split_actor_name(value: str) -> tuple[str, str]:
    cleaned = value.split("@", 1)[0].replace(".", " ").replace("_", " ").strip()
    parts = [item for item in cleaned.split() if item]
    if not parts:
        return ("Unknown", "Actor")
    if len(parts) == 1:
        token = parts[0].capitalize()
        return (token, "Actor")
    return (parts[0].capitalize(), parts[-1].capitalize())
