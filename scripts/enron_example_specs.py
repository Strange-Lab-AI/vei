from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class EnronExampleCandidate:
    label: str
    prompt: str


@dataclass(frozen=True)
class EnronExampleSpec:
    case_id: str
    bundle_slug: str
    title: str
    primary_prompt: str
    lead: str
    branch_point: str
    objective_pack_id: str
    comparison_label: str
    candidates: tuple[EnronExampleCandidate, ...]

    @property
    def output_root(self) -> Path:
        return Path("docs/examples") / self.bundle_slug

    @property
    def run_label(self) -> str:
        return f"{self.case_id}_saved_bundle_20260417"

    @property
    def screenshot_stem(self) -> str:
        return self.bundle_slug.replace("enron-", "")


def rosetta_dir() -> Path:
    return Path("data/enron/rosetta").resolve()


def load_case_register() -> dict[str, dict[str, str]]:
    payload = json.loads(Path("data/enron/enron_case_event_register.json").read_text())
    events = payload.get("events")
    if not isinstance(events, list):
        raise ValueError("invalid Enron case register")
    result: dict[str, dict[str, str]] = {}
    for item in events:
        if not isinstance(item, dict):
            continue
        case_id = str(item.get("case_id") or "").strip()
        if case_id:
            result[case_id] = {key: str(value) for key, value in item.items()}
    return result


def bundle_specs() -> tuple[EnronExampleSpec, ...]:
    return (
        EnronExampleSpec(
            case_id="master_agreement",
            bundle_slug="enron-master-agreement-public-context",
            title="Enron Master Agreement Example",
            primary_prompt=(
                "Keep the draft inside Enron, ask Gerald Nemec and Sara Shackleton "
                "for review, and hold the outside send."
            ),
            lead=(
                "This example keeps the original Master Agreement branch point in the "
                "repo, now with the wider Enron macro context and the newer stock, "
                "credit, and regulatory fixtures attached."
            ),
            branch_point=(
                "Debra Perlingiere is about to send the Master Agreement draft to "
                "Cargill on September 27, 2000."
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_master_agreement_business_state_comparison_20260417",
            candidates=(
                EnronExampleCandidate(
                    label="Hold for internal review",
                    prompt=(
                        "Keep the draft inside Enron, ask Gerald Nemec and Sara "
                        "Shackleton for review, and hold the outside send."
                    ),
                ),
                EnronExampleCandidate(
                    label="Send a narrow status note",
                    prompt=(
                        "Send Cargill a short no-attachment status note, promise a "
                        "clean draft soon, and keep one internal legal owner on the "
                        "next step."
                    ),
                ),
                EnronExampleCandidate(
                    label="Push for fast turnaround",
                    prompt=(
                        "Send the draft now, keep the outside loop active, and widen "
                        "circulation for rapid comments and turnaround."
                    ),
                ),
            ),
        ),
        EnronExampleSpec(
            case_id="watkins_memo_release_path",
            bundle_slug="enron-watkins-memo",
            title="Enron Watkins Memo Example",
            primary_prompt=(
                "Escalate the memo to Ken Lay, the audit committee, and internal "
                "legal, preserve the written record, and pause any broad reassurance "
                "until the accounting questions are reviewed."
            ),
            lead=(
                "This example anchors the bankruptcy arc on the Watkins memo path and "
                "shows how the what-if flow reads an internal accounting warning "
                "against the public trust collapse already underway."
            ),
            branch_point=(
                "Sherron Watkins is preserving the August warning path after "
                "Skilling's resignation and before the public collapse fully lands."
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_watkins_memo_business_state_comparison_20260417",
            candidates=(
                EnronExampleCandidate(
                    label="Escalate to audit committee",
                    prompt=(
                        "Escalate the memo to Ken Lay, the audit committee, and "
                        "internal legal, preserve the written record, and copy "
                        "Arthur Andersen on the factual accounting questions."
                    ),
                ),
                EnronExampleCandidate(
                    label="Route through Vinson & Elkins",
                    prompt=(
                        "Keep the memo inside Enron, route it through internal legal "
                        "and Vinson & Elkins for review, and hold broader escalation "
                        "until that review is complete."
                    ),
                ),
                EnronExampleCandidate(
                    label="Send the warning anonymously",
                    prompt=(
                        "Strip the sender identity, send the warning as an anonymous "
                        "memo to Ken Lay and the audit committee, and keep the "
                        "distribution narrow."
                    ),
                ),
                EnronExampleCandidate(
                    label="Suppress and monitor",
                    prompt=(
                        "Keep the memo inside a very small internal loop, do not "
                        "escalate it further, and monitor the accounting story "
                        "quietly for a few days."
                    ),
                ),
            ),
        ),
        EnronExampleSpec(
            case_id="california_crisis_order",
            bundle_slug="enron-california-crisis-strategy",
            title="Enron California Crisis Strategy Example",
            primary_prompt=(
                "Pause the strategy, preserve the record, alert legal and compliance, "
                "and prepare a self-report path instead of continuing the trading play."
            ),
            lead=(
                "This example moves the branch point into the California power-crisis "
                "conduct and lets the saved comparison sit inside the FERC and refund "
                "timeline that now ships with the repo."
            ),
            branch_point=(
                "Tim Belden's desk receives a preservation order tied to the "
                "California crisis while the trading strategy is still active."
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_california_crisis_business_state_comparison_20260417",
            candidates=(
                EnronExampleCandidate(
                    label="Halt and self-report",
                    prompt=(
                        "Pause the strategy, preserve the record, alert legal and "
                        "compliance, and prepare a self-report path to FERC and the "
                        "California authorities."
                    ),
                ),
                EnronExampleCandidate(
                    label="Seek Skilling sign-off",
                    prompt=(
                        "Preserve the record, memo the issue to Jeff Skilling for an "
                        "explicit decision, and keep the current strategy on hold "
                        "until leadership answers."
                    ),
                ),
                EnronExampleCandidate(
                    label="Proceed with narrow circulation",
                    prompt=(
                        "Keep the strategy moving, preserve the order inside a narrow "
                        "legal and trading loop, and avoid broad distribution while "
                        "continuing the desk play."
                    ),
                ),
                EnronExampleCandidate(
                    label="Proceed and widen",
                    prompt=(
                        "Keep the strategy moving, widen the internal circulation for "
                        "rapid comments, and keep the desk fully active despite the "
                        "preservation order."
                    ),
                ),
            ),
        ),
        EnronExampleSpec(
            case_id="pg_e_power_deal",
            bundle_slug="enron-pge-power-deal",
            title="Enron PG&E Power Deal Example",
            primary_prompt=(
                "Hold the deal until PG&E credit is rechecked, ask for collateral, "
                "and keep legal and credit on one internal review loop."
            ),
            lead=(
                "This example ties a commercial legal thread to the widening credit "
                "story around PG&E and gives the saved forecast a cleaner macro-credit "
                "hook than the original contract-only example."
            ),
            branch_point=(
                "Sara Shackleton is moving a PG&E financial power deal while the "
                "counterparty's macro-credit picture is deteriorating."
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_pge_power_deal_business_state_comparison_20260417",
            candidates=(
                EnronExampleCandidate(
                    label="Hold for credit re-check",
                    prompt=(
                        "Hold the PG&E deal until the credit team re-checks the "
                        "counterparty, keep legal inside the loop, and avoid the "
                        "outside push."
                    ),
                ),
                EnronExampleCandidate(
                    label="Restructure with collateral",
                    prompt=(
                        "Keep the deal alive, ask for collateral and tighter credit "
                        "protections, and route the revision through legal and credit "
                        "before any outside send."
                    ),
                ),
                EnronExampleCandidate(
                    label="Push to close this quarter",
                    prompt=(
                        "Push to close the PG&E deal before quarter end, keep the "
                        "outside loop active, and ask for fast comments on the current "
                        "draft."
                    ),
                ),
                EnronExampleCandidate(
                    label="Accept as-is",
                    prompt=(
                        "Accept the deal as written, keep the current outside loop "
                        "moving, and avoid adding new credit conditions."
                    ),
                ),
            ),
        ),
    )


def spec_by_case_id(case_id: str) -> EnronExampleSpec:
    normalized = case_id.strip()
    for spec in bundle_specs():
        if spec.case_id == normalized:
            return spec
    raise KeyError(f"unknown Enron example case: {case_id}")


def spec_by_bundle_slug(bundle_slug: str) -> EnronExampleSpec:
    normalized = bundle_slug.strip()
    for spec in bundle_specs():
        if spec.bundle_slug == normalized:
            return spec
    raise KeyError(f"unknown Enron example bundle: {bundle_slug}")


__all__ = [
    "EnronExampleCandidate",
    "EnronExampleSpec",
    "bundle_specs",
    "load_case_register",
    "rosetta_dir",
    "spec_by_bundle_slug",
    "spec_by_case_id",
]
