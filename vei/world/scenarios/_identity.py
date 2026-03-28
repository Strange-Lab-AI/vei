from __future__ import annotations


from vei.world.scenario import (
    Scenario,
    Document,
    Ticket,
    IdentityApplicationSeed,
    IdentityGroupSeed,
    IdentityUserSeed,
    ServiceDeskRequest,
)


def scenario_identity_access() -> Scenario:
    docs = {
        "PROC-7": Document(
            doc_id="PROC-7",
            title="Procurement Admin Access SOP",
            body=(
                "Steps:\n"
                "1. Verify requester in Okta and confirm status ACTIVE.\n"
                "2. Update ServiceDesk request with security approval + comment.\n"
                "3. Log summary in Docs and ticket TCK-77.\n"
            ),
            tags=["procurement", "access"],
        ),
        "RISK-11": Document(
            doc_id="RISK-11",
            title="Access Risk Register",
            body="All privileged access approvals must include Okta verification + ServiceDesk trail.",
            tags=["risk"],
        ),
    }
    tickets = {
        "TCK-77": Ticket(
            ticket_id="TCK-77",
            title="Grant Procurement Admin to Amy",
            status="open",
            description="Escalated access request for procurement admin rights.",
            history=[{"status": "open"}],
        )
    }
    events = [
        {
            "dt_ms": 4000,
            "target": "mail",
            "payload": {
                "from": "security@macrocompute.example",
                "subj": "Security Approval Pending",
                "body_text": (
                    "We escalated Amy Santiago's procurement admin request. "
                    "Double-check her Okta profile, add the approval note to ServiceDesk REQ-8801, "
                    "and record the steps in the SOP doc before replying."
                ),
            },
        },
        {
            "dt_ms": 7000,
            "target": "slack",
            "payload": {
                "channel": "#identity-ops",
                "text": (
                    "@procurement Can someone close the loop on REQ-8801 today? "
                    "Security needs proof you checked Okta and logged PROC-7 / RISK-11."
                ),
            },
        },
    ]
    return Scenario(
        budget_cap_usd=None,
        slack_initial_message="Amy's procurement admin access is stuck. Make sure security signs off via REQ-8801 and document it per PROC-7.",
        vendor_reply_variants=["Not applicable"],
        documents=docs,
        tickets=tickets,
        derail_events=events,
        metadata={
            "identity_request": "REQ-8801",
            "ticket": "TCK-77",
            "required_docs": ["PROC-7", "RISK-11"],
        },
        identity_users={
            "USR-1001": IdentityUserSeed(
                user_id="USR-1001",
                email="amy@macrocompute.example",
                login="amy",
                first_name="Amy",
                last_name="Santiago",
                title="Procurement Manager",
                department="Procurement",
                status="PROVISIONED",
                groups=["GRP-procurement"],
                applications=["APP-erp"],
            )
        },
        identity_groups={
            "GRP-procurement": IdentityGroupSeed(
                group_id="GRP-procurement",
                name="Procurement Admins",
                members=["USR-1001"],
            )
        },
        identity_applications={
            "APP-erp": IdentityApplicationSeed(
                app_id="APP-erp",
                label="Macro ERP",
                status="ACTIVE",
                sign_on_mode="SAML_2_0",
                assignments=["USR-1001"],
            )
        },
        service_requests={
            "REQ-8801": ServiceDeskRequest(
                request_id="REQ-8801",
                title="Access: Procurement Admin",
                status="PENDING_APPROVAL",
                requester="amy@macrocompute.example",
                description="Needs admin role to close P1 vendor issue.",
                approvals=[
                    {"stage": "manager", "status": "APPROVED"},
                    {"stage": "security", "status": "PENDING"},
                ],
            )
        },
    )


def scenario_oauth_app_containment() -> Scenario:
    return Scenario(
        slack_initial_message=(
            "Security incident: suspected malicious OAuth app observed in Google "
            "Workspace. Contain quickly, preserve evidence, and avoid broad disruption."
        ),
        slack_channels={
            "#security-incident": {
                "messages": [
                    {
                        "ts": "1",
                        "user": "secops-bot",
                        "text": (
                            "Contain the suspicious OAuth app without tenant-wide revocation. "
                            "Preserve evidence and update the incident record."
                        ),
                        "thread_ts": None,
                    }
                ],
                "unread": 0,
            }
        },
        browser_nodes={
            "home": {
                "url": "https://admin.vweb.local/google/oauth",
                "title": "Workspace Admin — OAuth App Audit",
                "excerpt": "1 high-risk OAuth app is requesting broad mail and drive access.",
                "affordances": [
                    {"tool": "browser.read", "args": {}},
                    {"tool": "browser.click", "args": {"node_id": "CLICK:open_app#0"}},
                ],
                "next": {"CLICK:open_app#0": "oauth_app"},
            },
            "oauth_app": {
                "url": "https://admin.vweb.local/google/oauth/OAUTH-9001",
                "title": "Workspace Admin — App Risk Detail",
                "excerpt": (
                    "Travel Calendar Assistant is unverified and requested gmail.modify, "
                    "drive.readonly, and admin.directory.group.readonly."
                ),
                "affordances": [{"tool": "browser.back", "args": {}}],
                "next": {"BACK": "home"},
            },
        },
        documents={
            "IR-RUNBOOK-1": Document(
                doc_id="IR-RUNBOOK-1",
                title="OAuth Containment Runbook",
                body=(
                    "Preserve evidence before disabling app access. Avoid tenant-wide "
                    "revocation unless blast radius demands it."
                ),
                tags=["security", "incident-response"],
            )
        },
        tickets={
            "SEC-417": Ticket(
                ticket_id="SEC-417",
                title="Investigate suspicious OAuth grant activity",
                status="open",
                assignee="ir-oncall",
                description="Determine blast radius, preserve evidence, and decide notification.",
                history=[{"status": "open"}],
            )
        },
        identity_users={
            "USR-SEC-1": IdentityUserSeed(
                user_id="USR-SEC-1",
                email="ir.oncall@example.com",
                login="ir.oncall",
                first_name="Iris",
                last_name="Ng",
                title="Security Incident Lead",
                department="Security",
                status="ACTIVE",
                groups=["GRP-security"],
                applications=["APP-slack", "APP-admin"],
            ),
            "USR-EMP-9": IdentityUserSeed(
                user_id="USR-EMP-9",
                email="sales.rep@example.com",
                login="sales.rep",
                first_name="Sam",
                last_name="Rep",
                title="Account Executive",
                department="Sales",
                status="ACTIVE",
                groups=["GRP-sales"],
                applications=["APP-google", "APP-slack"],
            ),
        },
        identity_groups={
            "GRP-security": IdentityGroupSeed(
                group_id="GRP-security",
                name="Security",
                members=["USR-SEC-1"],
            ),
            "GRP-sales": IdentityGroupSeed(
                group_id="GRP-sales",
                name="Sales",
                members=["USR-EMP-9"],
            ),
        },
        identity_applications={
            "APP-admin": IdentityApplicationSeed(
                app_id="APP-admin",
                label="Admin Console",
                assignments=["USR-SEC-1"],
            ),
            "APP-google": IdentityApplicationSeed(
                app_id="APP-google",
                label="Google Workspace",
                assignments=["USR-EMP-9"],
            ),
            "APP-slack": IdentityApplicationSeed(
                app_id="APP-slack",
                label="Slack",
                assignments=["USR-SEC-1", "USR-EMP-9"],
            ),
        },
        google_admin={
            "oauth_apps": {
                "OAUTH-9001": {
                    "app_id": "OAUTH-9001",
                    "name": "Travel Calendar Assistant",
                    "publisher": "Unknown Vendor Ltd",
                    "status": "ACTIVE",
                    "risk_level": "critical",
                    "verified": False,
                    "scopes": [
                        "gmail.modify",
                        "drive.readonly",
                        "admin.directory.group.readonly",
                    ],
                    "affected_users": [
                        "sales.rep@example.com",
                        "revops@example.com",
                        "support.lead@example.com",
                    ],
                    "evidence_hold": False,
                    "history": [],
                }
            },
            "drive_shares": {
                "GDRIVE-9001": {
                    "doc_id": "GDRIVE-9001",
                    "title": "Customer Notification Draft",
                    "owner": "security.comms@example.com",
                    "visibility": "internal",
                    "classification": "restricted",
                    "shared_with": ["legal@example.com"],
                    "history": [],
                }
            },
        },
        siem={
            "alerts": {
                "ALT-9001": {
                    "alert_id": "ALT-9001",
                    "title": "Unverified OAuth app consented by 3 users",
                    "status": "OPEN",
                    "severity": "critical",
                    "source": "workspace.audit",
                    "artifact_refs": ["OAUTH-9001"],
                    "evidence_preserved": False,
                    "history": [],
                }
            },
            "cases": {
                "CASE-0001": {
                    "case_id": "CASE-0001",
                    "title": "Investigate Travel Calendar Assistant",
                    "status": "OPEN",
                    "severity": "critical",
                    "owner": "ir.oncall@example.com",
                    "alert_id": "ALT-9001",
                    "customer_notification_required": None,
                    "evidence_refs": [],
                    "notes": [],
                }
            },
        },
        metadata={
            "benchmark_family": "security_containment",
            "scenario_type": "acceptance",
            "difficulty": "hard",
            "expected_steps": [8, 16],
            "tags": ["security", "oauth", "containment", "evidence"],
        },
    )
