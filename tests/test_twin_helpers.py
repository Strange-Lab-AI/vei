from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

import vei.twin.gateway as gateway
from vei.blueprint.api import build_blueprint_asset_for_example
from vei.blueprint.models import (
    BlueprintCrmCompanyAsset,
    BlueprintCrmContactAsset,
    BlueprintCrmDealAsset,
    BlueprintDocumentAsset,
    BlueprintEnvironmentAsset,
    BlueprintIdentityApplicationAsset,
    BlueprintIdentityGroupAsset,
    BlueprintIdentityUserAsset,
    BlueprintMailMessageAsset,
    BlueprintMailThreadAsset,
    BlueprintSlackChannelAsset,
    BlueprintSlackMessageAsset,
    BlueprintTicketAsset,
)
from vei.context.api import hydrate_blueprint
from vei.context.models import ContextSnapshot, ContextSourceResult
from vei.router.errors import MCPError
from vei.twin.api import (
    _apply_density,
    _apply_named_team_expansion,
    _apply_surface_filter,
    _apply_synthetic_expansion,
    _default_gateway_surfaces,
    _internal_placeholder_domains,
    _looks_placeholder_domain,
    _mask_external_email,
    _mask_external_contacts,
    _merge_capability_graphs,
    _merge_environment,
    _merge_models,
    _read_model,
    _role_names_for_archetype,
    _rewrite_email,
    _should_rewrite_domain,
    _write_json,
    build_customer_twin,
    build_customer_twin_asset,
    build_twin_matrix,
    load_twin_matrix,
)
from vei.twin.matrix import (
    _contract_variant_for_level,
    _default_archetypes,
    _organization_domain,
    _organization_name,
    _resolve_snapshot,
    _scenario_variant_for_level,
    _synthetic_strength_for_density,
    _team_expansion_for_density,
)
from vei.twin.models import CompatibilitySurfaceSpec, ContextMoldConfig


class _PeekRuntime:
    def __init__(self, payload):
        self.payload = payload

    def peek(self, _tool: str, _args: dict[str, object]):
        return self.payload


def _request(headers: dict[str, str] | None = None) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [
                (key.lower().encode("utf-8"), value.encode("utf-8"))
                for key, value in (headers or {}).items()
            ],
            "query_string": b"",
        }
    )


def _register_proxy_agent(
    client: TestClient,
    auth_headers: dict[str, str],
    *,
    agent_id: str = "scout-proxy",
    allowed_surfaces: list[str] | None = None,
) -> dict[str, str]:
    response = client.post(
        "/api/governor/agents",
        headers=auth_headers,
        json={
            "agent_id": agent_id,
            "name": "Scout",
            "mode": "proxy",
            "allowed_surfaces": allowed_surfaces or [],
        },
    )
    assert response.status_code == 201
    return {
        **auth_headers,
        "X-VEI-Agent-Id": agent_id,
        "X-VEI-Agent-Name": "Scout",
        "X-VEI-Agent-Role": "operator",
        "X-VEI-Agent-Team": "revops",
        "User-Agent": "pytest-suite",
    }


def _sample_snapshot() -> ContextSnapshot:
    return ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.ai",
        captured_at="2026-03-24T16:00:00+00:00",
        sources=[
            ContextSourceResult(
                provider="slack",
                captured_at="2026-03-24T16:00:00+00:00",
                record_counts={"channels": 1, "messages": 3},
                data={
                    "channels": [
                        {
                            "channel": "#revops-war-room",
                            "messages": [
                                {
                                    "ts": "1710300000.000100",
                                    "user": "maya.ops",
                                    "text": "Renewal is exposed unless we land the fix today.",
                                },
                                {
                                    "ts": "1710300060.000200",
                                    "user": "evan.sales",
                                    "text": "Jordan needs one accountable owner.",
                                },
                                {
                                    "ts": "1710300120.000300",
                                    "user": "maya.ops",
                                    "text": "Drafting the recovery note now.",
                                    "thread_ts": "1710300060.000200",
                                },
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="jira",
                captured_at="2026-03-24T16:00:00+00:00",
                record_counts={"issues": 2},
                data={
                    "issues": [
                        {
                            "ticket_id": "ACME-101",
                            "title": "Renewal blocker: onboarding API still timing out",
                            "status": "open",
                            "assignee": "maya.ops",
                            "description": "Customer onboarding export is timing out on larger tenants.",
                        },
                        {
                            "ticket_id": "ACME-102",
                            "title": "Prepare customer-safe release note",
                            "status": "in_progress",
                            "assignee": "evan.sales",
                            "description": "Release note needs an ETA and rollback summary.",
                        },
                    ]
                },
            ),
            ContextSourceResult(
                provider="google",
                captured_at="2026-03-24T16:00:00+00:00",
                record_counts={"documents": 1},
                data={
                    "documents": [
                        {
                            "doc_id": "DOC-ACME-001",
                            "title": "Renewal Recovery Plan",
                            "body": "Goal: restore onboarding reliability and protect trust.",
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="gmail",
                captured_at="2026-03-24T16:00:00+00:00",
                record_counts={"threads": 1},
                data={
                    "threads": [
                        {
                            "thread_id": "thr-001",
                            "subject": "Renewal risk review",
                            "messages": [
                                {
                                    "from": "jordan.blake@apexfinancial.example.com",
                                    "to": "support@acme.ai",
                                    "subject": "Renewal risk review",
                                    "snippet": "Need a clear owner and timeline today.",
                                    "unread": True,
                                }
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="okta",
                captured_at="2026-03-24T16:00:00+00:00",
                record_counts={"users": 1, "groups": 1, "applications": 1},
                data={
                    "users": [
                        {
                            "id": "okta-u-001",
                            "status": "active",
                            "profile": {
                                "login": "maya@acme.ai",
                                "email": "maya@acme.ai",
                                "firstName": "Maya",
                                "lastName": "Ops",
                                "displayName": "Maya Ops",
                                "department": "RevOps",
                                "title": "Revenue Operations Lead",
                            },
                            "group_ids": ["grp-ops"],
                        }
                    ],
                    "groups": [
                        {
                            "id": "grp-ops",
                            "profile": {"name": "Revenue Operations"},
                            "members": ["okta-u-001"],
                        }
                    ],
                    "applications": [
                        {
                            "id": "app-001",
                            "label": "Jira Cloud",
                            "status": "active",
                            "assignments": ["okta-u-001"],
                        }
                    ],
                },
            ),
        ],
    )


def test_customer_twin_asset_applies_mold_filters_masks_and_expansion() -> None:
    asset = build_customer_twin_asset(
        _sample_snapshot(),
        organization_name="Acme Cloud",
        organization_domain="acme.ai",
        mold=ContextMoldConfig(
            archetype="b2b_saas",
            density_level="small",
            named_team_expansion="expanded",
            redaction_mode="mask",
            included_surfaces=["slack", "mail", "identity"],
            synthetic_expansion_strength="strong",
        ),
    )

    assert asset.metadata["customer_twin_mold"]["density_level"] == "small"
    assert asset.capability_graphs is not None
    assert asset.capability_graphs.doc_graph is None
    assert asset.capability_graphs.work_graph is None
    assert asset.capability_graphs.revenue_graph is None
    assert asset.capability_graphs.identity_graph is not None
    assert len(asset.capability_graphs.identity_graph.users) >= 8
    assert len(asset.capability_graphs.comm_graph.slack_channels) <= 3
    assert all(
        len(channel.messages) <= 4
        for channel in asset.capability_graphs.comm_graph.slack_channels
    )
    assert all(
        user.email.endswith("@acme.ai")
        for user in asset.capability_graphs.identity_graph.users
    )


def test_twin_api_helper_functions_cover_io_and_matrix_round_trip(
    tmp_path: Path,
) -> None:
    mold = ContextMoldConfig(archetype="real_estate_management", density_level="large")
    path = tmp_path / "mold.json"
    _write_json(path, mold.model_dump(mode="json"))
    assert _read_model(path, ContextMoldConfig) == mold
    assert _looks_placeholder_domain("northstar.example.com")
    assert not _looks_placeholder_domain("acme.ai")
    assert _should_rewrite_domain("northstar.example.com", {"northstar.example.com"})
    assert not _should_rewrite_domain("customer.com", {"northstar.example.com"})
    assert (
        _rewrite_email(
            "maya@northstar.example.com",
            "acme.ai",
            internal_domains={"northstar.example.com"},
        )
        == "maya@acme.ai"
    )
    assert _mask_external_email("buyer@example.net", "acme.ai").endswith(
        "@masked.example.com"
    )
    assert _mask_external_email("maya@acme.ai", "acme.ai") == "maya@acme.ai"
    merged = _merge_models(
        [CompatibilitySurfaceSpec(name="slack", title="Slack", base_path="/slack/api")],
        [CompatibilitySurfaceSpec(name="jira", title="Jira", base_path="/jira/api")],
        key=lambda item: item.name,
    )
    assert {item.name for item in merged} == {"slack", "jira"}
    assert [item.name for item in _default_gateway_surfaces()] == [
        "slack",
        "jira",
        "graph",
        "salesforce",
    ]

    with pytest.raises(ValueError, match="provide either snapshot or provider_configs"):
        build_customer_twin(
            tmp_path / "invalid-both",
            snapshot=_sample_snapshot(),
            provider_configs=[],
            organization_name="Acme Cloud",
        )
    with pytest.raises(ValueError, match="snapshot or provider_configs is required"):
        build_customer_twin(tmp_path / "invalid-none")

    matrix = build_twin_matrix(
        tmp_path / "matrix",
        snapshot=_sample_snapshot(),
        archetypes=["b2b_saas"],
        density_levels=["small"],
        crisis_levels=["calm"],
        seeds=[7],
    )
    loaded = load_twin_matrix(tmp_path / "matrix")
    assert matrix.template.organization_name == "Acme Cloud"
    assert len(matrix.variants) == 1
    assert loaded.variants[0].variant_id == matrix.variants[0].variant_id


def test_twin_api_merge_and_expansion_helpers_cover_remaining_paths() -> None:
    base = build_blueprint_asset_for_example("acquired_user_cutover")
    captured = hydrate_blueprint(
        _sample_snapshot(),
        scenario_name=base.scenario_name,
        workflow_name=base.workflow_name or base.scenario_name,
    )
    base_env = BlueprintEnvironmentAsset(
        organization_name="MacroCompute",
        organization_domain="macro.example.com",
        slack_channels=[
            BlueprintSlackChannelAsset(
                channel="#ops",
                messages=[
                    BlueprintSlackMessageAsset(
                        ts="1.0",
                        user="maya.ops",
                        text="Current owner note",
                    )
                ],
            )
        ],
        mail_threads=[
            BlueprintMailThreadAsset(
                thread_id="thr-1",
                messages=[
                    BlueprintMailMessageAsset(
                        from_address="ops@macro.example.com",
                        to_address="buyer@customer.example.com",
                        subject="Status",
                        body_text="Initial note",
                    )
                ],
            )
        ],
        documents=[
            BlueprintDocumentAsset(
                doc_id="DOC-1", title="Runbook", body="Current runbook"
            )
        ],
        tickets=[
            BlueprintTicketAsset(
                ticket_id="TCK-1",
                title="Existing issue",
                status="open",
                assignee="maya.ops",
            )
        ],
        identity_users=[
            BlueprintIdentityUserAsset(
                user_id="USR-1",
                email="maya@macro.example.com",
                first_name="Maya",
                last_name="Ops",
                login="maya@macro.example.com",
            )
        ],
        identity_groups=[
            BlueprintIdentityGroupAsset(group_id="GRP-1", name="Revenue Ops")
        ],
        identity_applications=[
            BlueprintIdentityApplicationAsset(app_id="APP-1", label="Jira")
        ],
        crm_companies=[
            BlueprintCrmCompanyAsset(
                id="ACC-1", name="Macro", domain="macro.example.com"
            )
        ],
        crm_contacts=[
            BlueprintCrmContactAsset(
                id="CON-1",
                email="buyer@customer.example.com",
                first_name="Buyer",
                last_name="One",
                company_id="ACC-1",
            )
        ],
        crm_deals=[
            BlueprintCrmDealAsset(
                id="DEAL-1",
                name="Expansion",
                amount=1200,
                stage="Open",
                owner="maya.ops",
                company_id="ACC-1",
                contact_id="CON-1",
            )
        ],
        metadata={"source_providers": ["base"]},
    )
    captured_env = BlueprintEnvironmentAsset(
        organization_name="Acme Cloud",
        organization_domain="acme.ai",
        scenario_brief="Protect the renewal.",
        slack_channels=[
            BlueprintSlackChannelAsset(
                channel="#revops-war-room",
                messages=[
                    BlueprintSlackMessageAsset(
                        ts="2.0",
                        user="evan.sales",
                        text="Customer-safe note is ready.",
                    )
                ],
            )
        ],
        mail_threads=[
            BlueprintMailThreadAsset(
                thread_id="thr-2",
                messages=[
                    BlueprintMailMessageAsset(
                        from_address="seller@external.example.com",
                        to_address="ops@acme.ai",
                        subject="Renewal risk review",
                        body_text="Need an accountable owner.",
                    )
                ],
            )
        ],
        documents=[
            BlueprintDocumentAsset(
                doc_id="DOC-2",
                title="Recovery plan",
                body="Protect trust and stabilize onboarding.",
            )
        ],
        tickets=[
            BlueprintTicketAsset(
                ticket_id="TCK-2",
                title="Prepare customer-safe update",
                status="in_progress",
            )
        ],
        identity_users=[
            BlueprintIdentityUserAsset(
                user_id="USR-2",
                email="maya@acme.ai",
                first_name="Maya",
                last_name="Ops",
                login="maya@acme.ai",
            )
        ],
        identity_groups=[BlueprintIdentityGroupAsset(group_id="GRP-2", name="Support")],
        identity_applications=[
            BlueprintIdentityApplicationAsset(app_id="APP-2", label="Salesforce")
        ],
        crm_companies=[
            BlueprintCrmCompanyAsset(id="ACC-2", name="Acme", domain="acme.ai")
        ],
        crm_contacts=[
            BlueprintCrmContactAsset(
                id="CON-2",
                email="buyer@acme.ai",
                first_name="Buyer",
                last_name="Two",
                company_id="ACC-2",
            )
        ],
        crm_deals=[
            BlueprintCrmDealAsset(
                id="DEAL-2",
                name="Renewal",
                amount=2200,
                stage="Negotiation",
                owner="evan.sales",
                company_id="ACC-2",
                contact_id="CON-2",
            )
        ],
        metadata={"source_providers": ["captured"]},
    )

    merged_env = _merge_environment(
        base_env,
        captured_env,
        organization_name="Acme Cloud",
        organization_domain="acme.ai",
    )
    assert merged_env is not None
    assert merged_env.organization_name == "Acme Cloud"
    assert merged_env.organization_domain == "acme.ai"
    assert merged_env.slack_channels
    assert (
        _merge_environment(
            None,
            None,
            organization_name="Acme Cloud",
            organization_domain="acme.ai",
        )
        is None
    )

    merged_graphs = _merge_capability_graphs(
        base.capability_graphs,
        captured.capability_graphs,
        organization_name="Acme Cloud",
        organization_domain="acme.ai",
    )
    assert merged_graphs is not None
    assert merged_graphs.organization_domain == "acme.ai"
    assert merged_graphs.comm_graph is not None
    assert (
        _merge_capability_graphs(
            None,
            None,
            organization_name="Acme Cloud",
            organization_domain="acme.ai",
        )
        is None
    )

    asset = build_blueprint_asset_for_example("acquired_user_cutover")
    _apply_surface_filter(asset, ["slack", "identity"])
    assert asset.capability_graphs is not None
    assert asset.capability_graphs.doc_graph is None
    assert asset.capability_graphs.work_graph is None
    assert asset.capability_graphs.revenue_graph is None

    asset_with_contacts = build_blueprint_asset_for_example("acquired_user_cutover")
    asset_with_contacts.environment = base_env.model_copy(deep=True)
    _mask_external_contacts(asset_with_contacts, "acme.ai")
    assert asset_with_contacts.environment is not None
    assert any(
        message.from_address.endswith("@masked.example.com")
        or message.to_address.endswith("@masked.example.com")
        for thread in asset_with_contacts.environment.mail_threads
        for message in thread.messages
    )

    small_asset = build_blueprint_asset_for_example("acquired_user_cutover")
    _apply_density(small_asset, "small")
    assert small_asset.capability_graphs is not None
    assert len(small_asset.capability_graphs.comm_graph.slack_channels) <= 3

    named_team_asset = build_blueprint_asset_for_example("acquired_user_cutover")
    _apply_named_team_expansion(
        named_team_asset,
        "Acme Cloud",
        "acme.ai",
        ContextMoldConfig(
            archetype="digital_marketing_agency",
            named_team_expansion="expanded",
        ),
    )
    assert named_team_asset.capability_graphs is not None
    assert named_team_asset.capability_graphs.identity_graph is not None
    assert len(named_team_asset.capability_graphs.identity_graph.users) >= 8

    synthetic_asset = build_blueprint_asset_for_example("acquired_user_cutover")
    before_slack_messages = len(
        synthetic_asset.capability_graphs.comm_graph.slack_channels[0].messages
    )
    _apply_synthetic_expansion(
        synthetic_asset,
        "Acme Cloud",
        "acme.ai",
        ContextMoldConfig(
            archetype="b2b_saas",
            density_level="large",
            synthetic_expansion_strength="strong",
        ),
    )
    assert (
        len(synthetic_asset.capability_graphs.comm_graph.slack_channels[0].messages)
        > before_slack_messages
    )
    assert _internal_placeholder_domains(base)
    assert _role_names_for_archetype("digital_marketing_agency")[-1]["department"] == (
        "Media"
    )
    assert _role_names_for_archetype("storage_solutions")[-1]["department"] == (
        "Capacity"
    )


def test_twin_matrix_helper_functions_cover_selection_logic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _sample_snapshot()
    assert _default_archetypes(snapshot) == ["b2b_saas"]
    assert _default_archetypes(None) == [
        "b2b_saas",
        "digital_marketing_agency",
        "real_estate_management",
        "storage_solutions",
    ]
    assert _organization_name(None, fallback="Fallback Org") == "Fallback Org"
    assert _organization_name(snapshot, fallback="Fallback Org") == "Acme Cloud"
    assert _organization_domain(snapshot) == "acme.ai"
    assert _organization_domain(None) == ""
    assert _scenario_variant_for_level("real_estate_management", "calm") == (
        "tenant_opening_conflict"
    )
    assert _scenario_variant_for_level("real_estate_management", "adversarial") == (
        "double_booked_unit"
    )
    assert _contract_variant_for_level("real_estate_management", "escalated") == (
        "minimize_tenant_disruption"
    )
    assert _synthetic_strength_for_density("small") == "light"
    assert _synthetic_strength_for_density("large") == "strong"
    assert _team_expansion_for_density("small") == "minimal"
    assert _team_expansion_for_density("large") == "expanded"
    assert (
        _resolve_snapshot(
            snapshot=snapshot,
            provider_configs=None,
            organization_name=None,
            organization_domain="",
        )
        == snapshot
    )

    monkeypatch.setattr(
        "vei.twin.matrix.capture_context",
        lambda configs, organization_name, organization_domain: snapshot,
    )
    assert (
        _resolve_snapshot(
            snapshot=None,
            provider_configs=[],
            organization_name="Acme Cloud",
            organization_domain="acme.ai",
        )
        == snapshot
    )
    with pytest.raises(ValueError, match="organization_name is required"):
        _resolve_snapshot(
            snapshot=None,
            provider_configs=[],
            organization_name=None,
            organization_domain="",
        )


def test_twin_gateway_helper_functions_cover_payload_transforms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(
        {
            "Authorization": "Bearer secret-token",
            "X-VEI-Agent-Name": "Scout",
            "X-VEI-Agent-Role": "operator",
            "X-VEI-Agent-Team": "revops",
            "User-Agent": "pytest-suite",
        }
    )
    assert gateway._slack_auth_ok(request, "secret-token") is True
    gateway._require_bearer(request, "secret-token")
    with pytest.raises(HTTPException, match="invalid bearer token"):
        gateway._require_bearer(_request(), "secret-token")

    runtime = _PeekRuntime(["#ops", "#finance"])
    channel_id = gateway._slack_channel_id("#finance")
    assert gateway._resolve_slack_channel_name(runtime, channel_id) == "#finance"
    assert gateway._resolve_slack_channel_name(runtime, "ops") == "#ops"
    with pytest.raises(HTTPException, match="channel is required"):
        gateway._resolve_slack_channel_name(_PeekRuntime([]), "")

    message = {
        "id": "msg-1",
        "from": "maya@acme.ai",
        "to": "ops@acme.ai",
        "subj": "Update",
        "body_text": "Budget approved",
        "unread": True,
        "time": 1_710_000_000_000,
    }
    summary = gateway._graph_message_summary(message)
    assert summary["isRead"] is False
    assert gateway._graph_message(message, {"body_text": "Full body"})["body"][
        "content"
    ] == ("Full body")
    assert gateway._find_mail_message([message], "msg-1")["subj"] == "Update"
    assert gateway._find_mail_message([], "missing") == {"id": "missing"}
    assert (
        gateway._graph_first_recipient([{"emailAddress": {"address": "ops@acme.ai"}}])
        == "ops@acme.ai"
    )
    assert gateway._graph_email_address({"address": "maya@acme.ai"}) == "maya@acme.ai"
    assert gateway._graph_body_content({"content": "Hello"}) == "Hello"
    assert gateway._graph_body_content("Plain text") == "Plain text"
    assert gateway._graph_attendees(
        [{"emailAddress": {"address": "ops@acme.ai"}}, "ignored"]
    ) == ["ops@acme.ai"]
    assert gateway._graph_datetime_to_ms("2026-03-24T16:00:00Z") > 0
    assert (
        gateway._graph_event(
            {
                "event_id": "evt-1",
                "title": "Standup",
                "start_ms": 1000,
                "end_ms": 2000,
                "attendees": ["ops@acme.ai"],
                "organizer": "maya@acme.ai",
                "location": "Room 1",
                "description": "Check in",
                "status": "CANCELED",
            }
        )["isCancelled"]
        is True
    )

    assert (
        gateway._jira_issue(
            {
                "issue_id": "ACME-101",
                "title": "Fix onboarding",
                "status": "open",
                "assignee": "Maya Ops",
                "priority": "P1",
                "labels": ["urgent"],
                "comment_count": 2,
            }
        )["fields"]["comment"]["total"]
        == 2
    )
    assert gateway._jira_transitions("open")[0]["id"] == "in_progress"
    assert gateway._extract_jql_value(
        "status = 'open' AND assignee = maya.ops", "assignee"
    ) == ("maya.ops")
    assert gateway._extract_jql_value("priority = high", "status") is None

    def fake_dispatch(_runtime, _request_obj, **kwargs):
        if kwargs["resolved_tool"] == "jira.list_issues":
            return [
                {
                    "issue_id": "ACME-101",
                    "title": "Fix onboarding",
                    "status": "open",
                    "assignee": "maya.ops",
                },
                {
                    "issue_id": "ACME-102",
                    "title": "Prepare note",
                    "status": "in_progress",
                    "assignee": "evan.sales",
                },
            ]
        if kwargs["resolved_tool"] == "salesforce.opportunity.list":
            return [
                {"id": "OPP-1", "name": "Expansion", "stage": "Open", "amount": 1200}
            ]
        if kwargs["resolved_tool"] == "salesforce.contact.list":
            return [
                {
                    "id": "CON-1",
                    "email": "buyer@acme.ai",
                    "first_name": "Buyer",
                    "last_name": "One",
                    "company_id": "ACC-1",
                }
            ]
        return [{"id": "ACC-1", "name": "Acme", "domain": "acme.ai"}]

    monkeypatch.setattr(gateway, "_dispatch_request", fake_dispatch)
    jira_payload = gateway._jira_search(
        _PeekRuntime([]),
        request,
        {
            "jql": "status = 'open' AND assignee = maya.ops",
            "maxResults": "1",
            "startAt": "1",
        },
    )
    assert jira_payload["total"] == 2
    assert len(jira_payload["issues"]) == 1
    assert gateway._jira_project_key(_PeekRuntime([{"issue_id": "ACME-101"}])) == "ACME"
    assert gateway._jira_project_key(_PeekRuntime([])) == "VEI"

    assert (
        gateway._salesforce_query(
            _PeekRuntime([]),
            request,
            "SELECT Id, Name FROM Opportunity LIMIT 1",
        )["records"][0]["Id"]
        == "OPP-1"
    )
    assert (
        gateway._salesforce_query(
            _PeekRuntime([]),
            request,
            "SELECT Id, Email FROM Contact LIMIT 1",
        )["records"][0]["Id"]
        == "CON-1"
    )
    assert (
        gateway._salesforce_query(
            _PeekRuntime([]),
            request,
            "SELECT Id, Name FROM Account LIMIT 1",
        )["records"][0]["Id"]
        == "ACC-1"
    )

    assert gateway._salesforce_opportunity({"id": "OPP-1", "name": "Expansion"})[
        "Id"
    ] == ("OPP-1")
    assert gateway._salesforce_contact({"id": "CON-1", "email": "buyer@acme.ai"})[
        "Id"
    ] == ("CON-1")
    assert gateway._salesforce_account({"id": "ACC-1", "name": "Acme"})["Id"] == "ACC-1"

    mcp_error = MCPError("tool_failed", "dispatch failed")
    assert gateway._http_exception(mcp_error).status_code == 400
    assert gateway._provider_error_code(mcp_error) == "tool_failed"
    assert gateway._error_payload(mcp_error) == {
        "code": "tool_failed",
        "message": "dispatch failed",
    }
    assert gateway._provider_error_code(ValueError("bad args")) == "invalid_args"
    assert gateway._channel_for_focus("crm") == "Revenue"
    assert gateway._object_refs(
        {"channel": "#ops", "issue_id": "ACME-101", "id": "evt-1"},
        {"id": "evt-1", "ticket_id": "ACME-101"},
    ) == ["#ops", "ACME-101", "evt-1"]
    agent = gateway._request_agent_identity(request)
    assert agent is not None and agent.name == "Scout"


def test_twin_gateway_exposes_additional_provider_routes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "customer_twin"
    bundle = build_customer_twin(
        root,
        snapshot=_sample_snapshot(),
        organization_domain="acme.ai",
    )
    auth_headers = {"Authorization": f"Bearer {bundle.gateway.auth_token}"}

    original_dispatch = gateway._dispatch_request

    def patched_dispatch(runtime, request, **kwargs):
        if kwargs.get("external_tool") in {
            "jira.issue.update",
            "salesforce.opportunity.update",
        }:
            return {}
        return original_dispatch(runtime, request, **kwargs)

    monkeypatch.setattr(gateway, "_dispatch_request", patched_dispatch)

    with TestClient(gateway.create_twin_gateway_app(root)) as client:
        agent_headers = _register_proxy_agent(client, auth_headers)
        assert client.get("/").json()["status_path"] == "/api/twin"
        assert client.get("/healthz").json()["ok"] is True
        assert client.get("/slack/api/users.list").json()["error"] == "invalid_auth"

        channels = client.get(
            "/slack/api/conversations.list",
            headers=agent_headers,
        ).json()["channels"]
        channel_id = channels[0]["id"]

        history = client.get(
            "/slack/api/conversations.history",
            headers=agent_headers,
            params={"channel": channel_id},
        ).json()
        assert history["ok"] is True
        thread_ts = history["messages"][1]["ts"]
        replies = client.get(
            "/slack/api/conversations.replies",
            headers=agent_headers,
            params={"channel": channel_id, "ts": thread_ts},
        ).json()
        assert replies["ok"] is True
        assert (
            client.post(
                "/slack/api/reactions.add",
                headers=agent_headers,
                json={
                    "channel": channel_id,
                    "timestamp": thread_ts,
                    "name": "thumbsup",
                },
            ).json()["ok"]
            is True
        )
        assert client.get("/slack/api/users.list", headers=agent_headers).json()[
            "members"
        ]

        project_payload = client.get(
            "/jira/rest/api/3/project", headers=agent_headers
        ).json()
        assert project_payload[0]["name"] == "Acme Cloud"
        jira_search = client.get(
            "/jira/rest/api/3/search",
            headers=agent_headers,
            params={"jql": "status = open", "maxResults": 1},
        ).json()
        assert jira_search["issues"]
        issue_id = jira_search["issues"][0]["id"]
        assert (
            client.get(
                f"/jira/rest/api/3/issue/{issue_id}", headers=agent_headers
            ).json()["id"]
            == issue_id
        )
        assert client.get(
            f"/jira/rest/api/3/issue/{issue_id}/transitions",
            headers=agent_headers,
        ).json()["transitions"]
        assert (
            client.post(
                f"/jira/rest/api/3/issue/{issue_id}/comment",
                headers=agent_headers,
                json={"body": "We have an owner and next step."},
            ).status_code
            == 201
        )
        assert (
            client.post(
                f"/jira/rest/api/3/issue/{issue_id}/transitions",
                headers=agent_headers,
                json={"transition": {"id": "in_progress"}},
            ).status_code
            == 204
        )
        created_issue = client.post(
            "/jira/rest/api/3/issue",
            headers=agent_headers,
            json={
                "fields": {
                    "summary": "Document the customer-safe plan",
                    "description": "Need a short ETA and owner.",
                    "priority": {"name": "P2"},
                }
            },
        )
        assert created_issue.status_code == 201
        assert (
            client.put(
                f"/jira/rest/api/3/issue/{created_issue.json()['id']}",
                headers=agent_headers,
                json={"fields": {"summary": "Updated summary"}},
            ).status_code
            == 204
        )

        messages = client.get("/graph/v1.0/me/messages", headers=agent_headers).json()[
            "value"
        ]
        assert messages
        assert (
            client.get(
                f"/graph/v1.0/me/messages/{messages[0]['id']}",
                headers=agent_headers,
            ).status_code
            == 200
        )
        assert (
            client.post(
                "/graph/v1.0/me/sendMail",
                headers=agent_headers,
                json={
                    "message": {
                        "toRecipients": [
                            {"emailAddress": {"address": "buyer@acme.ai"}}
                        ],
                        "subject": "Customer update",
                        "body": {"content": "We have an owner and timeline."},
                    }
                },
            ).status_code
            == 202
        )
        events_response = client.get("/graph/v1.0/me/events", headers=agent_headers)
        assert events_response.status_code == 200
        assert "value" in events_response.json()
        created_event = client.post(
            "/graph/v1.0/me/events",
            headers=agent_headers,
            json={
                "subject": "Renewal standup",
                "start": {"dateTime": "2026-03-25T10:00:00Z"},
                "end": {"dateTime": "2026-03-25T10:30:00Z"},
                "attendees": [{"emailAddress": {"address": "ops@acme.ai"}}],
                "location": {"displayName": "Room 1"},
                "body": {"content": "Review blockers and owner."},
            },
        )
        assert created_event.status_code == 201
        assert created_event.json()["id"]

        opp_query = client.get(
            "/salesforce/services/data/v60.0/query",
            headers=agent_headers,
            params={"q": "SELECT Id, Name FROM Opportunity LIMIT 1"},
        ).json()
        assert opp_query["records"]
        opp_id = opp_query["records"][0]["Id"]
        assert (
            client.get(
                f"/salesforce/services/data/v60.0/sobjects/Opportunity/{opp_id}",
                headers=agent_headers,
            ).status_code
            == 200
        )
        created_opp = client.post(
            "/salesforce/services/data/v60.0/sobjects/Opportunity",
            headers=agent_headers,
            json={"Name": "Expansion", "Amount": 12000, "StageName": "Qualification"},
        )
        assert created_opp.status_code == 201
        assert (
            client.patch(
                f"/salesforce/services/data/v60.0/sobjects/Opportunity/{opp_id}",
                headers=agent_headers,
                json={"StageName": "Closed Won", "Amount": 13000},
            ).status_code
            == 204
        )
        assert (
            client.post(
                "/salesforce/services/data/v60.0/sobjects/Task",
                headers=agent_headers,
                json={"WhatId": opp_id, "Description": "Customer follow-up logged."},
            ).status_code
            == 201
        )

        contact_id = client.get(
            "/salesforce/services/data/v60.0/query",
            headers=agent_headers,
            params={"q": "SELECT Id, Email FROM Contact LIMIT 1"},
        ).json()["records"][0]["Id"]
        account_id = client.get(
            "/salesforce/services/data/v60.0/query",
            headers=agent_headers,
            params={"q": "SELECT Id, Name FROM Account LIMIT 1"},
        ).json()["records"][0]["Id"]
        assert (
            client.get(
                f"/salesforce/services/data/v60.0/sobjects/Contact/{contact_id}",
                headers=agent_headers,
            ).status_code
            == 200
        )
        assert (
            client.get(
                f"/salesforce/services/data/v60.0/sobjects/Account/{account_id}",
                headers=agent_headers,
            ).status_code
            == 200
        )

        twin_payload = client.get("/api/twin").json()
        assert any(
            agent.get("name") == "Scout"
            for agent in twin_payload["runtime"]["metadata"]["agents"]
        )
        assert twin_payload["runtime"]["metadata"]["last_agent"]["source"]
