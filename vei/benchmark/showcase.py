from __future__ import annotations

from typing import Dict, List, Sequence

from vei.benchmark.models import (
    BenchmarkShowcaseExample,
    BenchmarkShowcaseExampleResult,
    BenchmarkShowcaseResult,
)

_SHOWCASE_CATALOG: Dict[str, BenchmarkShowcaseExample] = {
    "oauth_incident_chain": BenchmarkShowcaseExample(
        name="oauth_incident_chain",
        title="OAuth Incident Chain",
        description=(
            "Contain a suspicious OAuth app while preserving evidence, updating the "
            "incident record, and coordinating security artifacts without broad user disruption."
        ),
        family_name="security_containment",
        workflow_variant="customer_notify",
        key_surfaces=["google_admin", "siem", "jira", "docs", "slack"],
        proves=[
            "Evidence-preserving containment across admin and investigation systems",
            "Targeted action instead of tenant-wide disruption",
            "Incident artifacts and security comms updated from the same run",
        ],
    ),
    "acquired_seller_cutover": BenchmarkShowcaseExample(
        name="acquired_seller_cutover",
        title="Acquired Seller Cutover",
        description=(
            "Resolve an acquired seller into the corporate identity graph, preserve "
            "least privilege, remove oversharing, and hand off opportunities before a virtual deadline."
        ),
        family_name="enterprise_onboarding_migration",
        workflow_variant="manager_cutover",
        key_surfaces=[
            "hris",
            "okta",
            "google_admin",
            "salesforce",
            "jira",
            "docs",
            "slack",
        ],
        proves=[
            "Cross-system identity and ownership migration",
            "Least-privilege enforcement with document-sharing cleanup",
            "Deadline-aware handoff with manager-facing artifacts",
        ],
    ),
    "checkout_revenue_flightdeck": BenchmarkShowcaseExample(
        name="checkout_revenue_flightdeck",
        title="Checkout Revenue Flight Deck",
        description=(
            "Mitigate a revenue-critical checkout incident with safe flag control, "
            "spreadsheet impact quantification, updated comms, and CRM/ticket follow-through."
        ),
        family_name="revenue_incident_mitigation",
        workflow_variant="revenue_ops_flightdeck",
        key_surfaces=[
            "datadog",
            "pagerduty",
            "feature_flags",
            "spreadsheet",
            "docs",
            "crm",
            "tickets",
            "slack",
        ],
        proves=[
            "Long-horizon mixed-stack recovery inside one branchable world",
            "Revenue quantification and operational mitigation in the same flow",
            "Contract-graded artifact and comms follow-through",
        ],
    ),
}


def get_showcase_example(name: str) -> BenchmarkShowcaseExample:
    key = name.strip().lower()
    if key not in _SHOWCASE_CATALOG:
        raise KeyError(f"unknown showcase example: {name}")
    return _SHOWCASE_CATALOG[key]


def list_showcase_examples() -> List[BenchmarkShowcaseExample]:
    return sorted(_SHOWCASE_CATALOG.values(), key=lambda item: item.name)


def resolve_showcase_examples(
    names: Sequence[str] | None = None,
) -> List[BenchmarkShowcaseExample]:
    selected = [name.strip().lower() for name in (names or []) if name.strip()]
    if not selected:
        return list_showcase_examples()
    resolved: List[BenchmarkShowcaseExample] = []
    seen: set[str] = set()
    for name in selected:
        example = get_showcase_example(name)
        if example.name in seen:
            continue
        seen.add(example.name)
        resolved.append(example)
    return resolved


def render_showcase_overview(result: BenchmarkShowcaseResult) -> str:
    lines: List[str] = [
        "# VEI Complex Example Showcase",
        "",
        f"Run ID: `{result.run_id}`",
        f"Examples: `{result.example_count}`",
        f"Workflow baselines succeeded: `{result.baseline_success_count}/{result.example_count}`",
        f"Comparison runs succeeded: `{result.comparison_success_count}/{result.example_count}`",
        "",
    ]
    for item in result.examples:
        demo = item.demo
        lines.extend(
            [
                f"## {item.example.title}",
                "",
                item.example.description,
                "",
                f"- Family: `{item.example.family_name}`",
                f"- Workflow variant: `{item.example.workflow_variant}`",
                f"- Key surfaces: {', '.join(f'`{surface}`' for surface in item.example.key_surfaces)}",
                f"- Baseline: `{demo.baseline_score:.3f}` with `{demo.baseline_assertions_passed}/{demo.baseline_assertions_total}` assertions",
                f"- Comparison ({demo.compare_runner}): `{demo.comparison_score:.3f}` with `{demo.comparison_assertions_passed}/{demo.comparison_assertions_total}` assertions",
                f"- Demo bundle: `{demo.demo_dir}`",
                "",
                "What it proves:",
            ]
        )
        for bullet in item.example.proves:
            lines.append(f"- {bullet}")
        lines.extend(
            [
                "",
                "Key artifacts:",
                f"- Blueprint asset: `{demo.baseline_blueprint_asset_path}`",
                f"- Compiled blueprint: `{demo.baseline_blueprint_path}`",
                f"- Contract: `{demo.baseline_contract_path}`",
                f"- Report: `{demo.report_markdown_path}`",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


__all__ = [
    "BenchmarkShowcaseExample",
    "BenchmarkShowcaseExampleResult",
    "BenchmarkShowcaseResult",
    "get_showcase_example",
    "list_showcase_examples",
    "render_showcase_overview",
    "resolve_showcase_examples",
]
