from __future__ import annotations

from collections import Counter
from hashlib import sha256
from typing import Any, Sequence

from pydantic import BaseModel, Field

from .models import WhatIfWorld


class DoctrineEvidenceCitation(BaseModel):
    citation_id: str
    source: str
    event_id: str = ""
    thread_id: str = ""
    timestamp: str = ""
    timestamp_ms: int = 0
    excerpt: str
    signal_tags: list[str] = Field(default_factory=list)


class DoctrinePacket(BaseModel):
    schema_version: str = "doctrine_packet_v2"
    packet_id: str
    tenant_id: str
    display_name: str
    organization_kind: str
    objective_policy_id: str
    mission: str
    primary_objectives: list[str] = Field(default_factory=list)
    decision_priorities: dict[str, float] = Field(default_factory=dict)
    out_of_scope_signals: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)
    evidence_citations: list[DoctrineEvidenceCitation] = Field(default_factory=list)
    archive_signal_counts: dict[str, int] = Field(default_factory=dict)
    extraction_method: str = "archive_keyword_v1"
    provenance: dict[str, Any] = Field(default_factory=dict)


class DoctrineDecisionProfile(BaseModel):
    decision_class: str
    relevance_score: float
    objective_policy_id: str
    candidate_policy_hint: str
    rationale: str
    out_of_scope: bool = False
    summary_features: dict[str, float] = Field(default_factory=dict)
    action_tags: list[str] = Field(default_factory=list)


_DECISION_CLASSES = (
    "routine_admin",
    "personal_network_favor",
    "marketing_opportunity",
    "product_delivery",
    "data_research_privacy",
    "customer_commercial",
    "governance_risk",
    "news_public_policy",
    "coordination_execution",
)

_GENERIC_OBJECTIVES = [
    "minimize enterprise risk",
    "protect commercial position",
    "preserve stakeholder trust",
    "maintain execution velocity",
    "reduce organizational strain",
]


def build_doctrine_packet_for_world(
    *,
    tenant_id: str,
    display_name: str,
    world: WhatIfWorld,
    human_override: dict[str, Any] | None = None,
    max_timestamp_ms: int | None = None,
) -> DoctrinePacket:
    citations = _world_evidence_citations(world, max_timestamp_ms=max_timestamp_ms)
    evidence = [citation.excerpt for citation in citations]
    return build_doctrine_packet(
        tenant_id=tenant_id,
        display_name=display_name or world.summary.organization_name or tenant_id,
        organization_name=world.summary.organization_name,
        organization_domain=world.summary.organization_domain,
        source=world.summary.source,
        evidence=evidence,
        evidence_citations=citations,
        human_override=human_override,
        max_timestamp_ms=max_timestamp_ms,
    )


def build_doctrine_packet(
    *,
    tenant_id: str,
    display_name: str,
    organization_name: str = "",
    organization_domain: str = "",
    source: str = "",
    evidence: Sequence[str] = (),
    evidence_citations: Sequence[DoctrineEvidenceCitation] = (),
    human_override: dict[str, Any] | None = None,
    max_timestamp_ms: int | None = None,
) -> DoctrinePacket:
    lowered = " ".join(
        [
            tenant_id,
            display_name,
            organization_name,
            organization_domain,
            source,
            *evidence,
        ]
    ).lower()
    normalized_tenant = _slug(
        tenant_id or display_name or organization_name or "tenant"
    )
    archive_text = " ".join([source, *evidence]).lower()
    signal_counts = _archive_signal_counts(archive_text)
    inferred_kind = _infer_organization_kind(
        lowered=lowered,
        archive_text=archive_text,
        signal_counts=signal_counts,
    )
    extraction_method = (
        "human_override_v1"
        if human_override
        else (
            "archive_keyword_v1"
            if sum(signal_counts.values()) > 0
            else "tenant_name_fallback_v1"
        )
    )
    provenance = {
        "source": source,
        "archive_evidence_count": len(evidence),
        "citation_count": len(evidence_citations),
        "signal_counts": dict(signal_counts),
        "fallback_used": extraction_method == "tenant_name_fallback_v1",
        "max_timestamp_ms": max_timestamp_ms,
        "branch_safe": max_timestamp_ms is not None,
    }
    if human_override:
        provenance["human_override_fields"] = sorted(human_override)
    if inferred_kind == "historical_news_corpus":
        return _packet_with_override(
            DoctrinePacket(
                packet_id=f"{normalized_tenant}:news_public_world_v1",
                tenant_id=tenant_id,
                display_name=display_name,
                organization_kind="historical_news_corpus",
                objective_policy_id="active_news_public_world_v1",
                mission=(
                    "Use the historical news timeline as an outside-world state model: "
                    "understand public events, policy/market signals, and plausible "
                    "responses from the evidence available as of the chosen date."
                ),
                primary_objectives=[
                    "surface useful public-world action under uncertainty",
                    "separate active advisories, policy memos, actor maps, and holds",
                    "avoid treating OCR or ingestion repair as the real decision",
                ],
                decision_priorities={
                    "news_public_policy": 1.30,
                    "governance_risk": 1.10,
                    "customer_commercial": 1.00,
                    "coordination_execution": 0.90,
                    "routine_admin": 0.05,
                },
                out_of_scope_signals=[
                    "ocr error",
                    "ingestion",
                    "data pipeline",
                    "parser",
                ],
                evidence=list(evidence[:5]),
                evidence_citations=list(evidence_citations[:8]),
                archive_signal_counts=dict(signal_counts),
                extraction_method=extraction_method,
                provenance=provenance,
            ),
            human_override,
        )
    if inferred_kind == "startup_product_market":
        return _packet_with_override(
            DoctrinePacket(
                packet_id=f"{normalized_tenant}:startup_product_market_v1",
                tenant_id=tenant_id,
                display_name=display_name,
                organization_kind="startup_product_market",
                objective_policy_id="startup_product_market_v1",
                mission=(
                    "Optimize startup learning and execution: product usefulness, "
                    "pilot/customer progress, credible marketing opportunities, and "
                    "focused founder/operator time."
                ),
                primary_objectives=[
                    "advance real product, pilot, customer, or go-to-market learning",
                    "keep stakeholder communication crisp and bounded",
                    "avoid mistaking personal favors or generic networking for business strategy",
                ],
                decision_priorities={
                    "product_delivery": 1.30,
                    "customer_commercial": 1.25,
                    "marketing_opportunity": 1.20,
                    "coordination_execution": 0.85,
                    "personal_network_favor": 0.05,
                    "routine_admin": 0.05,
                },
                out_of_scope_signals=[
                    "personal favor",
                    "friend intro",
                    "vc intro unrelated to business",
                    "generic networking",
                ],
                evidence=list(evidence[:5]),
                evidence_citations=list(evidence_citations[:8]),
                archive_signal_counts=dict(signal_counts),
                extraction_method=extraction_method,
                provenance=provenance,
            ),
            human_override,
        )
    if inferred_kind == "consumer_data_research":
        return _packet_with_override(
            DoctrinePacket(
                packet_id=f"{normalized_tenant}:consumer_data_research_v1",
                tenant_id=tenant_id,
                display_name=display_name,
                organization_kind="consumer_data_research",
                objective_policy_id="consumer_data_research_v1",
                mission=(
                    "Optimize a consumer-data and passive-tracking research business: "
                    "research feasibility, consent/privacy trust, enterprise buyer value, "
                    "and operationally realistic delivery."
                ),
                primary_objectives=[
                    "make passive-tracking/data research commercially usable",
                    "protect consent, privacy, and respondent trust",
                    "separate true product/research decisions from routine admin",
                ],
                decision_priorities={
                    "data_research_privacy": 1.35,
                    "customer_commercial": 1.20,
                    "product_delivery": 1.10,
                    "marketing_opportunity": 0.95,
                    "coordination_execution": 0.85,
                    "routine_admin": 0.05,
                },
                out_of_scope_signals=[
                    "routine inbox processing",
                    "spam",
                    "promotion",
                    "property selling",
                    "real estate",
                    "nw2 london",
                    "application for ios developer position",
                    "job application",
                ],
                evidence=list(evidence[:5]),
                evidence_citations=list(evidence_citations[:8]),
                archive_signal_counts=dict(signal_counts),
                extraction_method=extraction_method,
                provenance=provenance,
            ),
            human_override,
        )
    if inferred_kind == "energy_trading_governance":
        return _packet_with_override(
            DoctrinePacket(
                packet_id=f"{normalized_tenant}:governance_risk_v1",
                tenant_id=tenant_id,
                display_name=display_name,
                organization_kind="energy_trading_governance",
                objective_policy_id="governance_risk_v1",
                mission=(
                    "Optimize governance-safe commercial action in an energy trading "
                    "and finance organization: disclosure quality, accounting/control "
                    "pressure, legal review, counterparty confidence, and liquidity trust."
                ),
                primary_objectives=[
                    "reduce governance, disclosure, accounting, and legal exposure",
                    "preserve counterparty and market confidence",
                    "keep commercial moves tied to accountable evidence and approvals",
                ],
                decision_priorities={
                    "governance_risk": 1.35,
                    "customer_commercial": 1.15,
                    "coordination_execution": 0.95,
                    "product_delivery": 0.70,
                    "routine_admin": 0.05,
                },
                out_of_scope_signals=["low-value admin", "pure scheduling"],
                evidence=list(evidence[:5]),
                evidence_citations=list(evidence_citations[:8]),
                archive_signal_counts=dict(signal_counts),
                extraction_method=extraction_method,
                provenance=provenance,
            ),
            human_override,
        )
    return _packet_with_override(
        DoctrinePacket(
            packet_id=f"{normalized_tenant}:balanced_business_v1",
            tenant_id=tenant_id,
            display_name=display_name,
            organization_kind="generic_company",
            objective_policy_id="balanced_business_v1",
            mission="Optimize the company-specific decision against explicit business outcomes.",
            primary_objectives=list(_GENERIC_OBJECTIVES),
            decision_priorities={
                "product_delivery": 1.0,
                "customer_commercial": 1.0,
                "governance_risk": 1.0,
                "coordination_execution": 1.0,
                "routine_admin": 0.05,
            },
            out_of_scope_signals=[],
            evidence=list(evidence[:5]),
            evidence_citations=list(evidence_citations[:8]),
            archive_signal_counts=dict(signal_counts),
            extraction_method=extraction_method,
            provenance=provenance,
        ),
        human_override,
    )


def classify_doctrine_decision(
    packet: DoctrinePacket,
    *,
    text: str,
) -> DoctrineDecisionProfile:
    lowered = text.lower()
    decision_class = _decision_class_for_text(packet, lowered)
    relevance = float(packet.decision_priorities.get(decision_class, 0.90))
    out_signal = _signal_hits(lowered, packet.out_of_scope_signals)
    if decision_class in {"personal_network_favor", "routine_admin"}:
        out_of_scope = True
    elif packet.objective_policy_id == "consumer_data_research_v1":
        out_of_scope = out_signal and decision_class not in {
            "data_research_privacy",
            "customer_commercial",
            "marketing_opportunity",
        }
    else:
        out_of_scope = out_signal and decision_class not in {
            "product_delivery",
            "customer_commercial",
        }
    if out_of_scope:
        relevance = min(relevance, 0.10)
    rationale = _decision_rationale(packet, decision_class, lowered, out_of_scope)
    features = doctrine_summary_features(
        packet, decision_class, relevance, out_of_scope
    )
    tags = doctrine_action_tags(packet, decision_class, out_of_scope=out_of_scope)
    return DoctrineDecisionProfile(
        decision_class=decision_class,
        relevance_score=round(relevance, 3),
        objective_policy_id=packet.objective_policy_id,
        candidate_policy_hint=_candidate_policy_hint(packet, decision_class),
        rationale=rationale,
        out_of_scope=out_of_scope,
        summary_features=features,
        action_tags=tags,
    )


def doctrine_summary_features(
    packet: DoctrinePacket,
    decision_class: str,
    relevance_score: float,
    out_of_scope: bool,
) -> dict[str, float]:
    values = {
        "doctrine_relevance_score": float(relevance_score),
        "doctrine_out_of_scope": 1.0 if out_of_scope else 0.0,
        "doctrine_priority_product_delivery": float(
            packet.decision_priorities.get("product_delivery", 0.0)
        ),
        "doctrine_priority_customer_commercial": float(
            packet.decision_priorities.get("customer_commercial", 0.0)
        ),
        "doctrine_priority_governance_risk": float(
            packet.decision_priorities.get("governance_risk", 0.0)
        ),
        "doctrine_priority_data_research_privacy": float(
            packet.decision_priorities.get("data_research_privacy", 0.0)
        ),
        "doctrine_priority_marketing_opportunity": float(
            packet.decision_priorities.get("marketing_opportunity", 0.0)
        ),
    }
    for known_class in _DECISION_CLASSES:
        values[f"decision_class_{known_class}"] = (
            1.0 if decision_class == known_class else 0.0
        )
    return values


def doctrine_action_tags(
    packet: DoctrinePacket,
    decision_class: str,
    *,
    out_of_scope: bool = False,
) -> list[str]:
    tags = {
        f"objective_policy:{packet.objective_policy_id}",
        f"organization_kind:{packet.organization_kind}",
        f"decision_class:{decision_class}",
    }
    if out_of_scope:
        tags.add("decision_class:out_of_scope")
    return sorted(tags)


def doctrine_prompt_lines(
    packet: DoctrinePacket,
    profile: DoctrineDecisionProfile,
) -> list[str]:
    objectives = "; ".join(packet.primary_objectives)
    out_of_scope = "; ".join(packet.out_of_scope_signals) or "none declared"
    citations = "; ".join(
        citation.citation_id for citation in packet.evidence_citations[:3]
    )
    return [
        "Doctrine packet:",
        f"- Packet id: {packet.packet_id}",
        f"- Extraction method: {packet.extraction_method}",
        f"- Organization kind: {packet.organization_kind}",
        f"- Mission: {packet.mission}",
        f"- Primary objectives: {objectives}",
        f"- Out-of-scope signals: {out_of_scope}",
        f"- Evidence citations: {citations or 'none'}",
        f"- Classified decision class: {profile.decision_class}",
        f"- Decision relevance score: {profile.relevance_score:.3f}",
        f"- Candidate policy hint: {profile.candidate_policy_hint}",
        f"- Doctrine rationale: {profile.rationale}",
    ]


def doctrine_manifest_payload(
    packet: DoctrinePacket,
    profile: DoctrineDecisionProfile,
) -> dict[str, Any]:
    return {
        "doctrine_packet_id": packet.packet_id,
        "organization_kind": packet.organization_kind,
        "mission": packet.mission,
        "objective_policy_id": packet.objective_policy_id,
        "primary_objectives": list(packet.primary_objectives),
        "decision_priorities": dict(packet.decision_priorities),
        "out_of_scope_signals": list(packet.out_of_scope_signals),
        "doctrine_decision_class": profile.decision_class,
        "decision_relevance_score": profile.relevance_score,
        "candidate_policy_hint": profile.candidate_policy_hint,
        "doctrine_rationale": profile.rationale,
        "doctrine_out_of_scope": profile.out_of_scope,
        "doctrine_evidence": list(packet.evidence),
        "doctrine_text_sha256": doctrine_text_sha256(packet),
        "doctrine_extraction_method": packet.extraction_method,
        "doctrine_archive_signal_counts": dict(packet.archive_signal_counts),
    }


def doctrine_packet_text(packet: DoctrinePacket) -> str:
    citation_lines = [
        (
            f"- [{citation.citation_id}] {citation.timestamp or 'undated'} "
            f"{citation.source}: {citation.excerpt}"
        )
        for citation in packet.evidence_citations[:8]
    ]
    return "\n".join(
        [
            f"Doctrine packet: {packet.packet_id}",
            f"Schema version: {packet.schema_version}",
            f"Extraction method: {packet.extraction_method}",
            f"Tenant: {packet.display_name} ({packet.tenant_id})",
            f"Organization kind: {packet.organization_kind}",
            f"Objective policy: {packet.objective_policy_id}",
            f"Mission: {packet.mission}",
            "Primary objectives:",
            *[f"- {item}" for item in packet.primary_objectives],
            "Decision priorities:",
            *[
                f"- {name}: {value:.3f}"
                for name, value in sorted(packet.decision_priorities.items())
            ],
            "Out of scope signals:",
            *[f"- {item}" for item in packet.out_of_scope_signals],
            "Archive signal counts:",
            *[
                f"- {name}: {value}"
                for name, value in sorted(packet.archive_signal_counts.items())
            ],
            "Evidence citations:",
            *(citation_lines or ["- none"]),
        ]
    ).strip()


def doctrine_text_sha256(packet: DoctrinePacket) -> str:
    return sha256(doctrine_packet_text(packet).encode("utf-8")).hexdigest()


def _packet_with_override(
    packet: DoctrinePacket,
    override: dict[str, Any] | None,
) -> DoctrinePacket:
    if not override:
        return packet
    allowed = {
        "organization_kind",
        "objective_policy_id",
        "mission",
        "primary_objectives",
        "decision_priorities",
        "out_of_scope_signals",
    }
    updates = {key: value for key, value in override.items() if key in allowed}
    if not updates:
        return packet
    provenance = dict(packet.provenance)
    provenance["human_override_applied"] = sorted(updates)
    return packet.model_copy(update={**updates, "provenance": provenance})


def _decision_class_for_text(packet: DoctrinePacket, text: str) -> str:
    if packet.objective_policy_id == "active_news_public_world_v1":
        return "news_public_policy"
    governance_terms = (
        "legal",
        "compliance",
        "board",
        "audit",
        "accounting",
        "disclosure",
        "regulatory",
        "sec",
        "credit",
        "rating",
        "liquidity",
        "agreement",
        "master agreement",
        "counterparty",
    )
    if packet.objective_policy_id == "governance_risk_v1":
        if _hits(text, governance_terms):
            return "governance_risk"
        if _hits(
            text,
            (
                "contract",
                "deal",
                "auction",
                "trading",
                "market",
                "counterparty",
                "service",
            ),
        ):
            return "customer_commercial"
    if _hits(
        text,
        (
            "relieving letter",
            "salary slip",
            "salary slips",
            "final payment slip",
            "exit document",
            "exit documents",
            "job application",
            "application for ios developer",
            "property selling",
            "real estate",
            "interview process",
            "hiring process",
            "job interview",
            "resume",
            "cv",
            "franchise tax",
            "tax due",
            "late fee",
            "late fees",
        ),
    ):
        return "routine_admin"
    if packet.objective_policy_id == "startup_product_market_v1" and _hits(
        text,
        ("intro", "introduce", "introduction", "vc", "venture", "investor", "aaron"),
    ):
        if not _hits(
            text,
            (
                "pilot",
                "customer",
                "product",
                "demo",
                "deal",
                "contract",
                "commercial",
                "acorns",
                "seam",
            ),
        ):
            return "personal_network_favor"
    if _hits(
        text,
        (
            "marketing",
            "event",
            "conference",
            "elc",
            "case study",
            "webinar",
            "podcast",
            "press",
            "pilot program",
            "launch",
        ),
    ):
        return "marketing_opportunity"
    if _hits(
        text,
        (
            "passive tracking",
            "tracking",
            "consent",
            "privacy",
            "panel",
            "survey",
            "respondent",
            "research",
            "data",
            "permission",
        ),
    ):
        return "data_research_privacy"
    if _hits(
        text,
        (
            "bug",
            "issue",
            "ticket",
            "clickup",
            "app",
            "dashboard",
            "api",
            "feature",
            "release",
            "copy",
            "gif",
            "page",
        ),
    ):
        return "product_delivery"
    if _hits(
        text,
        (
            "customer",
            "client",
            "pilot",
            "contract",
            "proposal",
            "deal",
            "invoice",
            "payment",
            "seam",
            "acorns",
            "enterprise",
            "partner",
        ),
    ):
        return "customer_commercial"
    if _hits(
        text,
        governance_terms,
    ):
        return "governance_risk"
    return "coordination_execution"


def _candidate_policy_hint(packet: DoctrinePacket, decision_class: str) -> str:
    if decision_class == "routine_admin":
        return "Treat as routine admin unless it exposes a real product, customer, governance, or research decision."
    if decision_class == "personal_network_favor":
        return "Either decline or handle as a bounded personal favor; do not treat as core strategy."
    if decision_class == "marketing_opportunity":
        return "Use concrete yes/no or scoped marketing options; avoid adding process complexity unless claims/data risk exists."
    if decision_class == "data_research_privacy":
        return "Compare commercially useful research action against consent, privacy, and trust constraints."
    if decision_class == "product_delivery":
        return "Compare owner-led fix, customer status, fast ship, pilot, and escalation paths."
    if decision_class == "governance_risk":
        return "Compare accountable review, evidence control, disclosure/legal escalation, and commercial execution."
    if decision_class == "news_public_policy":
        return "Compare public advisories, policy/market memos, watches, actor maps, coordination, and one true hold."
    if packet.objective_policy_id == "startup_product_market_v1":
        return "Prefer options that create startup learning or customer/market progress with bounded risk."
    return "Generate broad operational options that match the stated objective policy."


def _decision_rationale(
    packet: DoctrinePacket,
    decision_class: str,
    text: str,
    out_of_scope: bool,
) -> str:
    if out_of_scope:
        return "Matches doctrine out-of-scope/network-favor signals, so it is deprioritized for strategic selection."
    priority = packet.decision_priorities.get(decision_class, 0.9)
    return (
        f"Classified as {decision_class} under {packet.objective_policy_id}; "
        f"tenant priority={priority:.2f}."
    )


def _world_evidence_citations(
    world: WhatIfWorld,
    *,
    max_timestamp_ms: int | None = None,
) -> list[DoctrineEvidenceCitation]:
    candidates: list[DoctrineEvidenceCitation] = []
    ordered_events = sorted(
        (
            event
            for event in world.events
            if max_timestamp_ms is None or event.timestamp_ms <= max_timestamp_ms
        ),
        key=lambda item: (item.timestamp_ms, item.event_id),
    )
    for event in ordered_events[:200]:
        text = " ".join(
            value
            for value in (event.subject, event.snippet, event.actor_id, event.target_id)
            if value
        ).strip()
        if text:
            tags = _signal_tags(text.lower())
            citation_id = _slug(f"{event.thread_id}_{event.event_id}")[:96]
            candidates.append(
                DoctrineEvidenceCitation(
                    citation_id=citation_id,
                    source=event.surface or world.summary.source or world.source,
                    event_id=event.event_id,
                    thread_id=event.thread_id,
                    timestamp=event.timestamp,
                    timestamp_ms=event.timestamp_ms,
                    excerpt=text[:320],
                    signal_tags=tags,
                )
            )
    if not candidates:
        return []
    scored = Counter()
    by_id = {citation.citation_id: citation for citation in candidates}
    for citation in candidates:
        scored[citation.citation_id] = len(citation.signal_tags)
    ranked = [by_id[citation_id] for citation_id, _count in scored.most_common(8)]
    if len(ranked) >= 3:
        return ranked
    return candidates[:8]


def _archive_signal_counts(text: str) -> Counter[str]:
    counts: Counter[str] = Counter()
    for signal, terms in _ARCHIVE_SIGNAL_TERMS.items():
        counts[signal] = sum(1 for term in terms if term in text)
    return counts


def _signal_tags(text: str) -> list[str]:
    return sorted(
        signal
        for signal, terms in _ARCHIVE_SIGNAL_TERMS.items()
        if any(term in text for term in terms)
    )


_ARCHIVE_SIGNAL_TERMS: dict[str, tuple[str, ...]] = {
    "consumer_data_research": (
        "passive tracking",
        "tracking",
        "consent",
        "privacy",
        "respondent",
        "panel",
        "survey",
        "research",
        "data collection",
        "permission",
    ),
    "startup_product_market": (
        "pilot",
        "poc",
        "customer",
        "product",
        "demo",
        "gtm",
        "go-to-market",
        "marketing",
        "launch",
        "ticket",
        "clickup",
        "onboarding",
        "acorns",
        "seam",
    ),
    "energy_trading_governance": (
        "legal",
        "compliance",
        "audit",
        "accounting",
        "disclosure",
        "regulatory",
        "sec",
        "credit",
        "rating",
        "liquidity",
        "master agreement",
        "counterparty",
    ),
    "historical_news_corpus": (
        "newspaper",
        "public",
        "policy",
        "bank",
        "labor",
        "weather",
        "crime",
        "court",
        "legislature",
        "panic",
    ),
}


def _infer_organization_kind(
    *,
    lowered: str,
    archive_text: str,
    signal_counts: Counter[str],
) -> str:
    best_signal = ""
    best_count = 0
    if signal_counts:
        best_signal, best_count = signal_counts.most_common(1)[0]
    tied_signals = {
        signal
        for signal, count in signal_counts.items()
        if count and count == best_count
    }
    if (
        "historical-news" in lowered
        or "pleias" in lowered
        or "americanstories" in lowered
    ):
        return "historical_news_corpus"
    if "passive tracking" in archive_text:
        return "consumer_data_research"
    if best_count >= 2:
        if (
            "powr" in lowered or "power of you" in lowered or "powrofyou" in lowered
        ) and "consumer_data_research" in tied_signals:
            return "consumer_data_research"
        if "enron" in lowered and "energy_trading_governance" in tied_signals:
            return "energy_trading_governance"
        if (
            "dispatch" in lowered or "thedispatch" in lowered
        ) and "startup_product_market" in tied_signals:
            return "startup_product_market"
        return best_signal
    if "powr" in lowered or "power of you" in lowered or "powrofyou" in lowered:
        return "consumer_data_research"
    if "enron" in lowered:
        return "energy_trading_governance"
    if "dispatch" in lowered or "thedispatch" in lowered:
        return "startup_product_market"
    if best_count > 0:
        return best_signal
    return "generic_company"


def _is_news(text: str) -> bool:
    return (
        "historical-news" in text
        or "pleias" in text
        or "americanstories" in text
        or "historical news" in text
    )


def _hits(text: str, terms: Sequence[str]) -> bool:
    return any(term in text for term in terms)


def _signal_hits(text: str, signals: Sequence[str]) -> bool:
    normalized_text = " ".join(
        "".join(ch.lower() if ch.isalnum() else " " for ch in text).split()
    )
    padded = f" {normalized_text} "
    for signal in signals:
        normalized_signal = " ".join(
            "".join(ch.lower() if ch.isalnum() else " " for ch in signal).split()
        )
        if not normalized_signal:
            continue
        if " " in normalized_signal:
            if normalized_signal in normalized_text:
                return True
        elif f" {normalized_signal} " in padded:
            return True
    return False


def _slug(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in value.strip())
    return "_".join(part for part in cleaned.split("_") if part) or "tenant"


__all__ = [
    "DoctrineDecisionProfile",
    "DoctrineEvidenceCitation",
    "DoctrinePacket",
    "build_doctrine_packet",
    "build_doctrine_packet_for_world",
    "classify_doctrine_decision",
    "doctrine_action_tags",
    "doctrine_manifest_payload",
    "doctrine_packet_text",
    "doctrine_prompt_lines",
    "doctrine_summary_features",
    "doctrine_text_sha256",
]
