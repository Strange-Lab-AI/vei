from __future__ import annotations

from dataclasses import dataclass

DEFAULT_BENCHMARK_PACK_ID = "enron_business_outcome_v1"


@dataclass(frozen=True)
class BenchmarkCaseSeed:
    case_id: str
    title: str
    event_id: str
    summary: str
    family: str


BENCHMARK_CASE_PACKS: dict[str, list[BenchmarkCaseSeed]] = {
    DEFAULT_BENCHMARK_PACK_ID: [
        BenchmarkCaseSeed(
            case_id="master_agreement",
            title="Master Agreement",
            event_id="enron_bcda1b925800af8c",
            summary="Debra Perlingiere sends a draft Master Agreement to Cargill.",
            family="outside_sharing",
        ),
        BenchmarkCaseSeed(
            case_id="btu_weekly",
            title="BTU Weekly",
            event_id="enron_7e7afce27432edae",
            summary="Vince Kaminski forwards BTU Weekly and its PDF to a personal address.",
            family="outside_sharing",
        ),
        BenchmarkCaseSeed(
            case_id="draft_position_paper",
            title="Draft Position Paper",
            event_id="enron_0a8a8985b6ae0d47",
            summary="A draft position paper is circulated to a broad outside group with an attachment.",
            family="outside_sharing",
        ),
        BenchmarkCaseSeed(
            case_id="vendor_policy_mailing",
            title="Conflict of Interest Policy Mailing to Vendors",
            event_id="enron_19d89fb317f5a309",
            summary="A conflict-of-interest policy message is prepared for wide vendor-facing distribution.",
            family="outside_sharing",
        ),
        BenchmarkCaseSeed(
            case_id="credit_derivatives_confidentiality",
            title="Credit Derivatives Confidentiality",
            event_id="enron_466a009e2ef0589f",
            summary="Draft confidentiality policies and procedures arrive from outside counsel.",
            family="legal_contract",
        ),
        BenchmarkCaseSeed(
            case_id="arbitration_guidance",
            title="Arbitration Guidance",
            event_id="enron_9ae972719b28ab61",
            summary="Arbitration guidance from outside counsel enters the Enron legal loop.",
            family="legal_contract",
        ),
        BenchmarkCaseSeed(
            case_id="kwb_power_contract",
            title="Master Bilateral Power Contract with KWB",
            event_id="enron_8c6d0f20336b4233",
            summary="A master bilateral power contract is moved through the legal chain for review.",
            family="legal_contract",
        ),
        BenchmarkCaseSeed(
            case_id="hlp_swap_agreement",
            title="Master Swap Agreement for HL&P",
            event_id="enron_42163faeb708e6f9",
            summary="A master swap agreement is being moved toward execution with legal oversight.",
            family="legal_contract",
        ),
        BenchmarkCaseSeed(
            case_id="pg_e_power_deal",
            title="PG&E Financial Power Deal",
            event_id="enron_e2e504e2ff9e60de",
            summary="A financial power deal is moving with counterpart and legal pressure.",
            family="commercial_counterparty",
        ),
        BenchmarkCaseSeed(
            case_id="cargill_internal",
            title="Cargill",
            event_id="enron_2407d1c23ac89a9d",
            summary="An internal Cargill thread is deciding how fast to move externally.",
            family="commercial_counterparty",
        ),
        BenchmarkCaseSeed(
            case_id="cargill_contract",
            title="Cargill Inc. - EW5791.1",
            event_id="enron_93d3f89640254c20",
            summary="The Cargill contract thread is moving through legal and commercial review.",
            family="commercial_counterparty",
        ),
        BenchmarkCaseSeed(
            case_id="credit_suisse_products",
            title="Credit Suisse Financial Products",
            event_id="enron_ad01cc9ea53ea66c",
            summary="A Credit Suisse thread is balancing counterparty speed against contract control.",
            family="commercial_counterparty",
        ),
        BenchmarkCaseSeed(
            case_id="ferc_weekly_report",
            title="Weekly FERC Gas Regulatory Report",
            event_id="enron_405ee04fb4ce3ff4",
            summary="A regulatory report is forwarded with legal and trading cues.",
            family="executive_regulatory",
        ),
        BenchmarkCaseSeed(
            case_id="urgent_etol_swap",
            title="Urgent ETOL Interest Rate Swap",
            event_id="enron_6553187cb07f8fd4",
            summary="An urgent interest-rate swap issue appears with time pressure and escalation risk.",
            family="executive_regulatory",
        ),
        BenchmarkCaseSeed(
            case_id="risk_management_policy",
            title="Risk Management Policy",
            event_id="enron_ab19d817c2d17b52",
            summary="A risk-management policy thread is deciding how broadly to circulate and escalate.",
            family="executive_regulatory",
        ),
        BenchmarkCaseSeed(
            case_id="market_descriptions_review",
            title="Legal and Regulatory Review of Market Descriptions",
            event_id="enron_5aac5c32d0e600c7",
            summary="A legal and regulatory review loop is forming around market descriptions.",
            family="executive_regulatory",
        ),
        BenchmarkCaseSeed(
            case_id="restructured_transaction",
            title="Please review - Re-structured Transaction",
            event_id="enron_99c869a8cce2ba3d",
            summary="A re-structured transaction is asking for coordinated review across the business.",
            family="coordination_strain",
        ),
        BenchmarkCaseSeed(
            case_id="nordic_master_agreement",
            title="Draft Nordic Power Master Agreement",
            event_id="enron_6251001cfaddf794",
            summary="A Nordic master agreement draft is moving through a broad review loop.",
            family="coordination_strain",
        ),
        BenchmarkCaseSeed(
            case_id="nerc_review",
            title="NERC - Please Review ASAP",
            event_id="enron_1de5c535ad6e7187",
            summary="A NERC review request arrives with urgency and outside counsel involvement.",
            family="coordination_strain",
        ),
        BenchmarkCaseSeed(
            case_id="confirmations_policy",
            title="Policy Draft for Confirmations",
            event_id="enron_21ee94d467ec9155",
            summary="A confirmations policy draft is deciding between narrow ownership and wider review.",
            family="coordination_strain",
        ),
        BenchmarkCaseSeed(
            case_id="performance_review_time",
            title="Performance Review Time",
            event_id="enron_0c37f675442a695d",
            summary="A performance-review thread is deciding how broadly to route sensitive personnel feedback.",
            family="org_heat",
        ),
        BenchmarkCaseSeed(
            case_id="performance_review_portz",
            title="Performance Review for David Portz",
            event_id="enron_bddda6f972ee9260",
            summary="A personnel-review thread is choosing between tight handling and wider escalation.",
            family="org_heat",
        ),
        BenchmarkCaseSeed(
            case_id="paralegal_position",
            title="Paralegal Position",
            event_id="enron_543752207e4316e1",
            summary="A hiring thread is deciding how tightly to manage a legal staffing decision.",
            family="org_heat",
        ),
        BenchmarkCaseSeed(
            case_id="interview_senior_counsel",
            title="Interview - Senior Counsel Position",
            event_id="enron_8fc6ac3218dfe61d",
            summary="A senior-counsel interview thread is choosing between private handling and wider alignment.",
            family="org_heat",
        ),
        BenchmarkCaseSeed(
            case_id="watkins_followup_questions",
            title="Watkins Follow-up Questions",
            event_id="enron_d8d296de473f63be",
            summary="A Watkins follow-up note preserves the questions she says she raised to Ken Lay and turns them into a concrete escalation branch.",
            family="whistleblower",
        ),
        BenchmarkCaseSeed(
            case_id="california_crisis_order",
            title="California Crisis Preservation Order",
            event_id="enron_99afc1ca73d1a1bb",
            summary="A California Attorney General preservation order lands on the trading desk while the crisis strategy is active.",
            family="market_manipulation",
        ),
        BenchmarkCaseSeed(
            case_id="baxter_press_release",
            title="Baxter Press Release",
            event_id="enron_f9851a464c7fa074",
            summary="The Cliff Baxter resignation press-release loop becomes a crisis-communications branch point.",
            family="crisis_communication",
        ),
        BenchmarkCaseSeed(
            case_id="q3_disclosure_review",
            title="Q3 Disclosure Review",
            event_id="enron_da8450d95fa6346a",
            summary="Third-quarter review material is moving during the October 2001 disclosure crisis.",
            family="crisis_communication",
        ),
        BenchmarkCaseSeed(
            case_id="ees_preholiday_update",
            title="EES Pre-holiday Update",
            event_id="enron_3f324462154fdf8f",
            summary="The year-end EES update captures accounting and coordination pressure at a disclosure-sensitive moment.",
            family="accounting_disclosure",
        ),
        BenchmarkCaseSeed(
            case_id="braveheart_forward",
            title="Braveheart",
            event_id="enron_c13a9a082c39a49d",
            summary="The Braveheart thread links the broadband structure story to a real internal branch point.",
            family="accounting_disclosure",
        ),
        BenchmarkCaseSeed(
            case_id="skilling_resignation_materials",
            title="Skilling Resignation Materials",
            event_id="enron_f225c917c6f1076f",
            summary="Draft communications materials around Skilling's resignation create a live trust and messaging branch point.",
            family="crisis_communication",
        ),
    ]
}


__all__ = [
    "BENCHMARK_CASE_PACKS",
    "BenchmarkCaseSeed",
    "DEFAULT_BENCHMARK_PACK_ID",
]
