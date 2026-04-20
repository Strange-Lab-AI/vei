from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from vei.whatif._enron_dataset import repo_enron_sample_rosetta_dir

EnronBundleRole = Literal["proof", "narrative"]
PUBLIC_OBJECTIVE_PACK_ID = "protect_company_default"


@dataclass(frozen=True)
class EnronExampleCandidate:
    label: str
    prompt: str
    explanation: str


@dataclass(frozen=True)
class EnronExampleSpec:
    case_id: str
    bundle_slug: str
    role: EnronBundleRole
    title: str
    primary_prompt: str
    lead: str
    branch_point: str
    actual_happened: str
    story_lines: tuple[str, ...]
    objective_pack_id: str
    comparison_label: str
    candidates: tuple[EnronExampleCandidate, ...]
    public_objective_pack_id: str = PUBLIC_OBJECTIVE_PACK_ID

    @property
    def output_root(self) -> Path:
        return Path("docs/examples") / self.bundle_slug

    @property
    def run_label(self) -> str:
        return f"{self.case_id}_saved_bundle_20260419"

    @property
    def screenshot_stem(self) -> str:
        return self.bundle_slug.replace("enron-", "")


def rosetta_dir() -> Path:
    resolved = repo_enron_sample_rosetta_dir()
    if not resolved.exists():
        raise RuntimeError(
            "No checked-in Enron Rosetta sample found under data/enron/rosetta."
        )
    return resolved.resolve()


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
            role="proof",
            title="Enron Master Agreement Example",
            primary_prompt=(
                "Keep the draft inside Enron, ask Gerald Nemec and Sara Shackleton "
                "for review, and hold the outside send."
            ),
            lead=(
                "This is the default long-tail technical proof case. It keeps the "
                "visible downstream mail tail while placing the contract choice inside "
                "the richer Enron company timeline."
            ),
            branch_point=(
                "Debra Perlingiere is about to send the Master Agreement draft to "
                "Cargill on September 27, 2000."
            ),
            actual_happened=(
                "The draft went outside quickly, then the thread widened into a long "
                "reassignment and redline tail with no visible formal signoff."
            ),
            story_lines=(
                "This is the clearest proof case because it has the largest visible "
                "recorded tail after the branch. You can point at the same decision, "
                "the actual downstream chain, and the ranked alternate moves in one view.",
                "The company timeline around it is thicker now, so the branch reads as "
                "a company event rather than a detached contract email.",
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_master_agreement_business_state_comparison_20260419",
            candidates=(
                EnronExampleCandidate(
                    label="Internal legal review",
                    prompt=(
                        "Keep the draft inside Enron, ask Gerald Nemec and Sara "
                        "Shackleton for review, and hold the outside send."
                    ),
                    explanation=(
                        "Keep ownership narrow and get legal review before any outside send."
                    ),
                ),
                EnronExampleCandidate(
                    label="Narrow status note",
                    prompt=(
                        "Send Cargill a short status note with no attachment, promise "
                        "a clean draft after review, and keep one internal legal owner "
                        "on the next step."
                    ),
                    explanation=("Acknowledge the request without sending the draft."),
                ),
                EnronExampleCandidate(
                    label="Controlled external send",
                    prompt=(
                        "Send the draft to Cargill with explicit caveats that it is "
                        "for review only, keep Gerald Nemec on the next step, and hold "
                        "wider circulation."
                    ),
                    explanation=(
                        "Send outside once, with explicit limits and a tight reply loop."
                    ),
                ),
                EnronExampleCandidate(
                    label="Fast outside circulation",
                    prompt=(
                        "Send the draft now, keep the outside loop active, and widen "
                        "circulation for rapid comments and turnaround."
                    ),
                    explanation=("Move fast and widen the loop for speed."),
                ),
            ),
        ),
        EnronExampleSpec(
            case_id="pg_e_power_deal",
            bundle_slug="enron-pge-power-deal",
            role="proof",
            title="Enron PG&E Power Deal Example",
            primary_prompt=(
                "Hold the deal until PG&E credit is rechecked, ask for collateral, "
                "and keep legal and credit on one internal review loop."
            ),
            lead=(
                "This is the clean commercial judgment case. It puts a real counterparty "
                "decision under credit pressure on the same saved timeline and forecast surface."
            ),
            branch_point=(
                "Sara Shackleton is moving a PG&E financial power deal while the "
                "counterparty credit picture is deteriorating."
            ),
            actual_happened=(
                "The deal thread kept moving through the legal and commercial loop "
                "while the wider PG&E situation worsened."
            ),
            story_lines=(
                "This case is strong because more than one move looks plausible. The "
                "question is not only safety. The question is whether Enron should slow "
                "down, restructure, or still push the deal through.",
                "It also gives the proof set a commercial and credit branch instead of "
                "only legal or governance branches.",
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_pge_power_deal_business_state_comparison_20260419",
            candidates=(
                EnronExampleCandidate(
                    label="Hold for credit re-check",
                    prompt=(
                        "Hold the PG&E deal until the credit team re-checks the "
                        "counterparty, keep legal inside the loop, and avoid the outside push."
                    ),
                    explanation=(
                        "Pause the deal and re-check counterparty credit before moving."
                    ),
                ),
                EnronExampleCandidate(
                    label="Restructure with collateral",
                    prompt=(
                        "Keep the deal alive, ask for collateral and tighter credit "
                        "protections, and route the revision through legal and credit "
                        "before any outside send."
                    ),
                    explanation=("Keep the deal alive, but rewrite the risk terms."),
                ),
                EnronExampleCandidate(
                    label="Close with tighter approval",
                    prompt=(
                        "Keep the deal moving, require a tighter executive and credit "
                        "sign-off path, and limit changes to what is needed for close."
                    ),
                    explanation=(
                        "Keep momentum, but require a visibly tighter approval path."
                    ),
                ),
                EnronExampleCandidate(
                    label="Push quarter-end close",
                    prompt=(
                        "Push to close the PG&E deal before quarter end, keep the "
                        "outside loop active, and ask for fast comments on the current draft."
                    ),
                    explanation=(
                        "Favor speed and quarter-end timing over extra credit review."
                    ),
                ),
            ),
        ),
        EnronExampleSpec(
            case_id="california_crisis_order",
            bundle_slug="enron-california-crisis-strategy",
            role="proof",
            title="Enron California Crisis Strategy Example",
            primary_prompt=(
                "Pause the strategy, preserve the record, alert legal and compliance, "
                "and prepare a self-report path instead of continuing the trading play."
            ),
            lead=(
                "This is the regulatory conduct case. It puts a preservation order, an "
                "active trading posture, and a narrow fork about halting versus continuing "
                "onto one saved branch."
            ),
            branch_point=(
                "Tim Belden's desk receives a preservation order tied to the California "
                "crisis while the trading strategy is still active."
            ),
            actual_happened=(
                "The preservation-order thread stayed inside the active crisis loop "
                "while the desk was still deciding how far to halt or continue."
            ),
            story_lines=(
                "This case is useful because the fork is mechanically clear. Preserve "
                "and halt. Preserve and seek executive sign-off. Continue in a narrow loop. "
                "Or continue and widen.",
                "It gives the proof set the cleanest legal and operational branch.",
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_california_crisis_business_state_comparison_20260419",
            candidates=(
                EnronExampleCandidate(
                    label="Preserve and halt",
                    prompt=(
                        "Pause the strategy, preserve the record, alert legal and "
                        "compliance, and prepare a self-report path to FERC and the "
                        "California authorities."
                    ),
                    explanation=(
                        "Stop the play, preserve the record, and open the legal path."
                    ),
                ),
                EnronExampleCandidate(
                    label="Preserve and seek executive sign-off",
                    prompt=(
                        "Preserve the record, memo the issue to Jeff Skilling for an "
                        "explicit decision, and keep the current strategy on hold until "
                        "leadership answers."
                    ),
                    explanation=(
                        "Preserve the record, but wait for top-level approval before acting."
                    ),
                ),
                EnronExampleCandidate(
                    label="Continue in a narrow loop",
                    prompt=(
                        "Keep the strategy moving, preserve the order inside a narrow "
                        "legal and trading loop, and avoid broad distribution while "
                        "continuing the desk play."
                    ),
                    explanation=("Keep going, but keep the circle tight."),
                ),
                EnronExampleCandidate(
                    label="Continue and widen",
                    prompt=(
                        "Keep the strategy moving, widen the internal circulation for "
                        "rapid comments, and keep the desk fully active despite the "
                        "preservation order."
                    ),
                    explanation=("Keep going and broaden the loop."),
                ),
            ),
        ),
        EnronExampleSpec(
            case_id="baxter_press_release",
            bundle_slug="enron-baxter-press-release",
            role="proof",
            title="Enron Baxter Press Release Example",
            primary_prompt=(
                "Hold the release for board and legal alignment, keep the language factual, "
                "and avoid broad reassurance until the internal record is stable."
            ),
            lead=(
                "This is the crisis-communications proof case. It shows a real public-facing "
                "branch where messaging quality and timing both matter."
            ),
            branch_point=(
                "The Cliff Baxter press-release loop is active and the company has to decide "
                "how transparent, delayed, or reassuring the public message should be."
            ),
            actual_happened=(
                "The communications loop moved through a tight internal chain while the "
                "company shaped how much to say and how fast to say it."
            ),
            story_lines=(
                "This is a better public-message case than Watkins for technical proof. "
                "It still has real stakes, but it also has a clearer downstream tail and a "
                "public-facing branch that readers understand quickly.",
                "It gives the proof set a crisis-communications case that is about more than "
                "pure suppression versus escalation.",
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_baxter_press_release_business_state_comparison_20260419",
            candidates=(
                EnronExampleCandidate(
                    label="Transparent fact-based release",
                    prompt=(
                        "Publish a factual release that states what is known, avoids spin, "
                        "and routes follow-up questions through one legal and communications owner."
                    ),
                    explanation=(
                        "Say only what is known and keep the wording factual."
                    ),
                ),
                EnronExampleCandidate(
                    label="Board and legal alignment delay",
                    prompt=(
                        "Hold the release for board and legal alignment, keep the language factual, "
                        "and avoid broad reassurance until the internal record is stable."
                    ),
                    explanation=(
                        "Delay briefly for tighter internal alignment before releasing."
                    ),
                ),
                EnronExampleCandidate(
                    label="Vague reassurance release",
                    prompt=(
                        "Issue a quick reassurance-heavy release, keep details thin, and aim to "
                        "steady the market reaction before the full internal review catches up."
                    ),
                    explanation=("Release fast, but lean on vague reassurance."),
                ),
                EnronExampleCandidate(
                    label="Narrow internal handling",
                    prompt=(
                        "Keep the communications draft inside a very small executive and legal loop, "
                        "delay public release, and avoid wider internal circulation."
                    ),
                    explanation=(
                        "Keep the matter tightly internal and delay the public line."
                    ),
                ),
            ),
        ),
        EnronExampleSpec(
            case_id="braveheart_forward",
            bundle_slug="enron-braveheart-forward",
            role="proof",
            title="Enron Braveheart Forward Example",
            primary_prompt=(
                "Open an accounting review on Braveheart, preserve the working record, "
                "and require explicit disclosure review before the structure keeps moving."
            ),
            lead=(
                "This is the accounting-structure proof case. It is denser than the others, "
                "but it shows the system handling a real internal financing and disclosure fork."
            ),
            branch_point=(
                "The Braveheart structure is being forwarded through the valuation and review "
                "chain as the company decides whether to reopen the accounting question."
            ),
            actual_happened=(
                "The thread kept moving through a narrow finance and legal chain tied to the "
                "larger broadband and structure story."
            ),
            story_lines=(
                "This case is stronger than a simple safe-versus-risky story. A full stop, a "
                "restructure, a narrow review, and quiet continuation each carry different costs.",
                "It gives the proof set a hard accounting and disclosure branch that is not just "
                "a message-routing problem.",
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_braveheart_forward_business_state_comparison_20260419",
            candidates=(
                EnronExampleCandidate(
                    label="Open accounting review",
                    prompt=(
                        "Open an accounting review on Braveheart, preserve the working record, "
                        "and require explicit disclosure review before the structure keeps moving."
                    ),
                    explanation=(
                        "Reopen the accounting question formally before moving."
                    ),
                ),
                EnronExampleCandidate(
                    label="Restructure with explicit disclosure",
                    prompt=(
                        "Keep the structure alive, rewrite it with explicit disclosure language, "
                        "and route the revision through finance, legal, and disclosure review."
                    ),
                    explanation=(
                        "Keep the transaction alive, but change the structure and disclosure."
                    ),
                ),
                EnronExampleCandidate(
                    label="Narrow finance and legal review",
                    prompt=(
                        "Keep the work inside a narrow finance and legal chain, ask for a quick "
                        "internal review, and delay broader disclosure discussions."
                    ),
                    explanation=(
                        "Use a narrow internal review before broadening the loop."
                    ),
                ),
                EnronExampleCandidate(
                    label="Keep it moving quietly",
                    prompt=(
                        "Keep the transaction moving quietly through the existing chain, avoid "
                        "new disclosure language, and limit the internal loop."
                    ),
                    explanation=(
                        "Preserve momentum and keep the structure moving quietly."
                    ),
                ),
            ),
        ),
        EnronExampleSpec(
            case_id="watkins_followup_questions",
            bundle_slug="enron-watkins-follow-up",
            role="narrative",
            title="Enron Watkins Follow-up Example",
            primary_prompt=(
                "Escalate the follow-up note to Ken Lay, the audit committee, and "
                "internal legal, preserve the written record, and pause broad reassurance."
            ),
            lead=(
                "This is the main narrative governance case. It is the strongest moral fork in "
                "the set, even though it has a thinner recorded downstream tail than Master Agreement."
            ),
            branch_point=(
                "Sherron Watkins is writing a follow-up note that preserves her account of the "
                "questions she says she raised to Ken Lay on August 22, 2001."
            ),
            actual_happened=(
                "The follow-up note became a narrow internal preserved record during the wider "
                "disclosure spiral."
            ),
            story_lines=(
                "This is the case to use when you want the clearest human and governance story. "
                "The branch is simple to explain and the stakes are obvious.",
                "Use it as a narrative case, not as the main technical proof case.",
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_watkins_followup_business_state_comparison_20260419",
            candidates=(
                EnronExampleCandidate(
                    label="Formal audit escalation",
                    prompt=(
                        "Escalate the follow-up note to Ken Lay, the audit committee, and "
                        "internal legal, preserve the written record, and copy Arthur Andersen "
                        "on the factual accounting questions."
                    ),
                    explanation=("Turn the note into a formal accounting escalation."),
                ),
                EnronExampleCandidate(
                    label="Board and legal preservation",
                    prompt=(
                        "Preserve the note for the board and internal legal, tighten the record, "
                        "and delay broader reassurance until the accounting questions are reviewed."
                    ),
                    explanation=(
                        "Preserve the record inside the board and legal path."
                    ),
                ),
                EnronExampleCandidate(
                    label="Narrow internal escalation",
                    prompt=(
                        "Send the follow-up note to Ken Lay and a narrow internal legal loop, "
                        "ask for a direct response, and avoid opening a wider review yet."
                    ),
                    explanation=("Warn upward, but keep the loop narrow."),
                ),
                EnronExampleCandidate(
                    label="Suppress and monitor",
                    prompt=(
                        "Keep the follow-up note inside a very small internal loop, do not "
                        "escalate it further, and monitor the accounting story quietly."
                    ),
                    explanation=(
                        "Keep the concern private and avoid formal escalation."
                    ),
                ),
            ),
        ),
        EnronExampleSpec(
            case_id="q3_disclosure_review",
            bundle_slug="enron-q3-disclosure-review",
            role="narrative",
            title="Enron Q3 Disclosure Review Example",
            primary_prompt=(
                "Open the review with fuller disclosure language about the overhang, "
                "tighten the internal record, and route the draft through disclosure counsel."
            ),
            lead=(
                "This is the disclosure narrative case. It makes the October 2001 review path "
                "legible without relying on a single whistleblower note."
            ),
            branch_point=(
                "Third-quarter review material is moving while the company is deciding how much "
                "to say about the growing accounting and liquidity overhang."
            ),
            actual_happened=(
                "The review material kept moving through management, finance, and legal during "
                "the disclosure crisis."
            ),
            story_lines=(
                "This case is useful because two or three options can look defensible at first glance. "
                "That makes it a better narrative example for explaining why ranking the actions matters.",
                "It also sits closer to public disclosure mechanics than Watkins does.",
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_q3_disclosure_review_business_state_comparison_20260419",
            candidates=(
                EnronExampleCandidate(
                    label="Full disclosure with overhang language",
                    prompt=(
                        "Open the review with fuller disclosure language about the overhang, "
                        "tighten the internal record, and route the draft through disclosure counsel."
                    ),
                    explanation=("Use fuller public language about the overhang."),
                ),
                EnronExampleCandidate(
                    label="Restricted lender-only disclosure",
                    prompt=(
                        "Keep the fuller overhang discussion inside lender and advisor channels, "
                        "while holding the broader public draft narrower."
                    ),
                    explanation=(
                        "Tell the lenders more than the broader public draft."
                    ),
                ),
                EnronExampleCandidate(
                    label="Management-only review before disclosure",
                    prompt=(
                        "Keep the draft inside management and legal for one more review turn "
                        "before any fuller disclosure language goes out."
                    ),
                    explanation=(
                        "Delay the broader language for another management review pass."
                    ),
                ),
                EnronExampleCandidate(
                    label="Narrow footnote and continue",
                    prompt=(
                        "Keep the disclosure narrow, rely on a tighter footnote treatment, "
                        "and continue with the current public line."
                    ),
                    explanation=("Keep the language narrow and continue."),
                ),
            ),
        ),
        EnronExampleSpec(
            case_id="skilling_resignation_materials",
            bundle_slug="enron-skilling-resignation-materials",
            role="narrative",
            title="Enron Skilling Resignation Materials Example",
            primary_prompt=(
                "Draft a candid transition message, keep the executive record aligned, "
                "and avoid aggressive reassurance language."
            ),
            lead=(
                "This is the executive-transition narrative case. It shows the same saved bundle "
                "surface working on a trust and messaging branch rather than a contract or trading branch."
            ),
            branch_point=(
                "The company is drafting materials around Jeff Skilling's resignation and has to "
                "decide how candid, controlled, or aggressively reassuring the message should be."
            ),
            actual_happened=(
                "The resignation materials moved through a controlled executive communications loop."
            ),
            story_lines=(
                "This case gives the narrative set a leadership-trust branch. Readers can follow it "
                "quickly because the public meaning of the choice is clear.",
                "It is also useful for presentation because the scene is legible even without deep accounting context.",
            ),
            objective_pack_id="contain_exposure",
            comparison_label="enron_skilling_resignation_materials_business_state_comparison_20260419",
            candidates=(
                EnronExampleCandidate(
                    label="Candid transition materials",
                    prompt=(
                        "Draft a candid transition message, keep the executive record aligned, "
                        "and avoid aggressive reassurance language."
                    ),
                    explanation=("Use a more candid transition explanation."),
                ),
                EnronExampleCandidate(
                    label="Controlled transition message",
                    prompt=(
                        "Keep the message disciplined and factual, coordinate the release through "
                        "legal and communications, and avoid extra claims."
                    ),
                    explanation=("Keep the message controlled and factual."),
                ),
                EnronExampleCandidate(
                    label="Narrow internal-only draft",
                    prompt=(
                        "Keep the draft inside a very small executive and legal loop, delay wider "
                        "circulation, and hold on public messaging until the internal record is tight."
                    ),
                    explanation=("Hold the draft in a very small internal loop."),
                ),
                EnronExampleCandidate(
                    label="Aggressive reassurance messaging",
                    prompt=(
                        "Push a strong reassurance-heavy transition message quickly, emphasize stability, "
                        "and downplay uncertainty around the change."
                    ),
                    explanation=("Lean hard on reassurance and stability."),
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
    "EnronBundleRole",
    "EnronExampleCandidate",
    "EnronExampleSpec",
    "PUBLIC_OBJECTIVE_PACK_ID",
    "bundle_specs",
    "load_case_register",
    "rosetta_dir",
    "spec_by_bundle_slug",
    "spec_by_case_id",
]
