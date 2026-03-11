from __future__ import annotations

from typing import Any, Dict, Sequence

from vei.router.tool_providers import PrefixToolProvider
from vei.router.tool_registry import ToolSpec
from vei.sdk import (
    SessionHook,
    build_blueprint_asset_for_example_entry,
    build_blueprint_asset_for_family_entry,
    build_grounding_bundle_example_entry,
    compile_blueprint_entry,
    compile_identity_governance_bundle_entry,
    create_world_session_from_blueprint_entry,
    create_session,
    filter_enterprise_corpus,
    generate_enterprise_corpus,
    get_benchmark_family_workflow_spec,
    get_benchmark_family_workflow_variant,
    get_showcase_example_entry,
    get_scenario_manifest,
    list_scenario_manifest,
    list_benchmark_family_workflow_variants,
    list_blueprint_builder_examples_entries,
    list_grounding_bundle_example_entries,
    list_showcase_example_entries,
    run_benchmark_family_workflow,
    run_workflow_spec,
    validate_benchmark_family_workflow,
    validate_workflow_spec,
)


class _EchoProvider(PrefixToolProvider):
    def __init__(self) -> None:
        super().__init__(name="echo_provider", prefixes=("ext.",))

    def specs(self) -> Sequence[ToolSpec]:
        return (
            ToolSpec(
                name="ext.echo",
                description="Echo payload for SDK contract tests.",
                returns="object",
            ),
        )

    def call(self, tool: str, args: Dict[str, Any]) -> Dict[str, Any]:
        if tool == "ext.echo":
            return {"ok": True, "payload": dict(args)}
        raise RuntimeError(f"unsupported tool for _EchoProvider: {tool}")


class _CaptureHook(SessionHook):
    def __init__(self) -> None:
        self.before_calls: list[tuple[str, Dict[str, Any]]] = []
        self.after_calls: list[tuple[str, Dict[str, Any]]] = []

    def before_call(self, tool: str, args: Dict[str, Any]) -> None:
        self.before_calls.append((tool, dict(args)))

    def after_call(
        self, tool: str, args: Dict[str, Any], result: Dict[str, Any]
    ) -> None:
        self.after_calls.append((tool, dict(result)))


def _workflow_spec() -> Dict[str, Any]:
    return {
        "name": "sdk-contract-workflow",
        "objective": {
            "statement": "Read browser context and post approval note.",
            "success": ["context read", "approval posted"],
        },
        "world": {"catalog": "multi_channel"},
        "steps": [
            {
                "step_id": "read",
                "description": "Read browser state",
                "tool": "browser.read",
                "args": {},
            },
            {
                "step_id": "approve",
                "description": "Post approval in procurement channel",
                "tool": "slack.send_message",
                "args": {
                    "channel": "#procurement",
                    "text": "Approval request for budget $2400 with quote attached.",
                },
                "expect": [
                    {"kind": "result_contains", "field": "ts", "contains": ""},
                ],
            },
        ],
        "success_assertions": [
            {"kind": "pending_max", "field": "total", "max_value": 20}
        ],
    }


def test_sdk_session_supports_observe_and_tool_calls() -> None:
    session = create_session(seed=42042, scenario_name="multi_channel")
    observation = session.observe()
    assert isinstance(observation.get("action_menu"), list)

    browser = session.call_tool("browser.read", {})
    assert "url" in browser
    assert "title" in browser


def test_sdk_session_supports_custom_tool_provider_registration() -> None:
    session = create_session(seed=42042, scenario_name="multi_channel")
    session.register_tool_provider(_EchoProvider())

    result = session.call_tool("ext.echo", {"message": "hello"})
    assert result["ok"] is True
    assert result["payload"]["message"] == "hello"


def test_sdk_session_supports_call_hooks() -> None:
    session = create_session(seed=42042, scenario_name="multi_channel")
    hook = _CaptureHook()
    session.register_hook(hook)

    _ = session.call_tool("browser.read", {})

    assert len(hook.before_calls) == 1
    assert hook.before_calls[0][0] == "browser.read"
    assert len(hook.after_calls) == 1
    assert hook.after_calls[0][0] == "browser.read"
    assert "url" in hook.after_calls[0][1]


def test_sdk_workflow_helpers_compile_validate_and_run() -> None:
    spec = _workflow_spec()
    validation = validate_workflow_spec(spec, seed=7)
    assert validation.ok

    result = run_workflow_spec(spec, seed=7, connector_mode="sim")
    assert result.ok
    assert result.static_validation.ok
    assert result.dynamic_validation.ok
    assert len(result.steps) == 2


def test_sdk_validate_reports_unknown_tool() -> None:
    spec = _workflow_spec()
    spec["steps"][1]["tool"] = "unknown.tool"
    report = validate_workflow_spec(
        spec,
        seed=7,
        available_tools=["browser.read", "slack.send_message"],
    )
    assert not report.ok
    assert any(issue.code == "tool.unavailable" for issue in report.issues)


def test_sdk_corpus_helpers_generate_and_filter() -> None:
    bundle = generate_enterprise_corpus(
        seed=42042,
        environment_count=2,
        scenarios_per_environment=3,
    )
    report = filter_enterprise_corpus(bundle, realism_threshold=0.0)

    assert len(bundle.workflows) == 6
    assert len(report.accepted) + len(report.rejected) == len(bundle.workflows)


def test_sdk_scenario_manifest_helpers() -> None:
    manifest = get_scenario_manifest("multi_channel")
    assert manifest.name == "multi_channel"
    assert "slack" in manifest.tool_families
    assert "mail" in manifest.tool_families
    assert "docs" in manifest.tool_families
    assert "tickets" in manifest.tool_families
    assert "okta" in manifest.tool_families
    assert "servicedesk" in manifest.tool_families

    all_entries = list_scenario_manifest()
    assert all_entries
    assert any(entry.name == "multi_channel" for entry in all_entries)


def test_sdk_benchmark_family_workflow_helpers() -> None:
    workflow = get_benchmark_family_workflow_spec("security_containment")
    assert workflow.name == "security_containment"
    assert workflow.metadata["workflow_variant"] == "customer_notify"

    variant = get_benchmark_family_workflow_variant(
        "security_containment", "internal_only_review"
    )
    assert variant.variant_name == "internal_only_review"
    assert any(
        item.name == "notification_required" and item.value is False
        for item in variant.parameters
    )

    variants = list_benchmark_family_workflow_variants("revenue_incident_mitigation")
    assert {item.variant_name for item in variants} == {
        "revenue_ops_flightdeck",
        "kill_switch_backstop",
        "canary_floor",
    }

    validation = validate_benchmark_family_workflow("security_containment", seed=9)
    assert validation.ok

    result = run_benchmark_family_workflow(
        "security_containment",
        variant_name="internal_only_review",
        seed=9,
    )
    assert result.ok
    assert result.workflow_name == "security_containment"
    assert result.final_state["scenario"]["metadata"]["workflow_variant"] == (
        "internal_only_review"
    )


def test_sdk_blueprint_helpers_compile_assets() -> None:
    asset = build_blueprint_asset_for_family_entry(
        "revenue_incident_mitigation", variant_name="revenue_ops_flightdeck"
    )
    compiled = compile_blueprint_entry(asset)

    assert asset.workflow_variant == "revenue_ops_flightdeck"
    assert compiled.asset.name == asset.name
    assert "spreadsheet" in {item.name for item in compiled.facades}


def test_sdk_blueprint_builder_helpers_compile_and_open_world() -> None:
    asset = build_blueprint_asset_for_example_entry("acquired_user_cutover")
    compiled = compile_blueprint_entry(asset)
    session = create_world_session_from_blueprint_entry(asset, seed=5)

    assert "acquired_user_cutover" in list_blueprint_builder_examples_entries()
    assert compiled.environment_summary is not None
    assert compiled.environment_summary.hris_employee_count == 2
    assert compiled.graph_summaries
    slack = session.observe("slack")
    graphs = session.capability_graphs()
    orientation = session.orientation()
    assert slack["focus"] == "slack"
    assert "#sales-cutover" in slack["summary"]
    assert graphs.identity_graph is not None
    assert graphs.identity_graph.policies[0].title.startswith("Wave 2")
    assert orientation.organization_name == "MacroCompute"
    assert orientation.active_policies[0].policy_id == "POL-WAVE2"


def test_sdk_grounding_bundle_helpers_round_trip_to_blueprint() -> None:
    bundle = build_grounding_bundle_example_entry("acquired_user_cutover")
    manifests = list_grounding_bundle_example_entries()
    asset = compile_identity_governance_bundle_entry(bundle)
    compiled = compile_blueprint_entry(asset)

    assert any(item.name == "acquired_user_cutover" for item in manifests)
    assert bundle.workflow_seed.employee_id == "EMP-2201"
    assert asset.capability_graphs is not None
    assert compiled.metadata["scenario_materialization"] == "capability_graphs"


def test_sdk_showcase_helpers_list_complex_examples() -> None:
    example = get_showcase_example_entry("checkout_revenue_flightdeck")
    assert example.family_name == "revenue_incident_mitigation"
    assert "spreadsheet" in example.key_surfaces

    names = {item.name for item in list_showcase_example_entries()}
    assert {
        "oauth_incident_chain",
        "acquired_seller_cutover",
        "checkout_revenue_flightdeck",
    } <= names
