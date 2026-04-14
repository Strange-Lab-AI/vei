"""Anonymization pipeline for VEI context snapshots.

Sits between capture_context() and hydrate_blueprint() to strip PII
from real enterprise data before it enters the simulation.
"""

from __future__ import annotations

from vei.anonymize.replacers import DeterministicReplacer
from vei.context.api import ContextSnapshot, ContextSourceResult


def anonymize_snapshot(
    snapshot: ContextSnapshot,
    *,
    salt: str = "vei-anon-2024",
) -> ContextSnapshot:
    """Produce a pseudonymized copy of a ContextSnapshot.

    Uses deterministic hashing so the same real identity always maps to
    the same fake identity — preserving referential integrity across
    Slack channels, email threads, tickets, and identity records.
    """
    replacer = DeterministicReplacer(salt=salt)
    anonymized_sources = [
        _anonymize_source(source, replacer) for source in snapshot.sources
    ]
    return ContextSnapshot(
        version=snapshot.version,
        organization_name=replacer.replace_name(snapshot.organization_name),
        organization_domain=_anonymize_domain(snapshot.organization_domain, replacer),
        captured_at=snapshot.captured_at,
        sources=anonymized_sources,
        metadata={
            **dict(snapshot.metadata),
            "anonymized": True,
            "anonymization_salt_hash": _salt_fingerprint(salt),
        },
    )


def _anonymize_source(
    source: ContextSourceResult,
    replacer: DeterministicReplacer,
) -> ContextSourceResult:
    anonymized_data = replacer.replace_in_dict(dict(source.data))
    return ContextSourceResult(
        provider=source.provider,
        captured_at=source.captured_at,
        status=source.status,
        record_counts=dict(source.record_counts),
        data=anonymized_data,
        error=source.error,
    )


def _anonymize_domain(domain: str, replacer: DeterministicReplacer) -> str:
    if not domain:
        return domain
    return replacer.replace_email(f"org@{domain}").split("@", 1)[-1]


def _salt_fingerprint(salt: str) -> str:
    import hashlib

    return hashlib.sha256(salt.encode("utf-8"), usedforsecurity=False).hexdigest()[:12]
