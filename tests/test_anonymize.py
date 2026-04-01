"""Tests for the anonymize module: detectors, replacers, primitives, and API."""

from __future__ import annotations

from vei.anonymize.detectors import detect_pii, has_pii
from vei.anonymize.primitives import (
    deterministic_hash,
    pseudonymize_email,
    pseudonymize_name,
    redact_numeric_sequences,
)
from vei.anonymize.replacers import DeterministicReplacer
from vei.anonymize.api import anonymize_snapshot, _salt_fingerprint
from vei.context.models import ContextSnapshot, ContextSourceResult

# -- detectors ---------------------------------------------------------------


def test_detect_email() -> None:
    matches = detect_pii("Contact alice@example.com for details")
    assert len(matches) == 1
    assert matches[0].kind == "email"
    assert matches[0].value == "alice@example.com"


def test_detect_phone() -> None:
    matches = detect_pii("Call (555) 123-4567 now")
    phones = [m for m in matches if m.kind == "phone"]
    assert len(phones) == 1
    assert "555" in phones[0].value


def test_detect_ssn() -> None:
    matches = detect_pii("SSN: 123-45-6789")
    ssns = [m for m in matches if m.kind == "ssn"]
    assert len(ssns) == 1
    assert ssns[0].value == "123-45-6789"


def test_detect_ip_non_version_pattern() -> None:
    matches = detect_pii("Server at 999.168.1.100")
    ips = [m for m in matches if m.kind == "ip"]
    assert len(ips) == 1
    assert ips[0].value == "999.168.1.100"


def test_standard_ip_filtered_as_version_number() -> None:
    matches = detect_pii("Server at 192.168.1.100")
    ips = [m for m in matches if m.kind == "ip"]
    assert len(ips) == 0


def test_version_number_not_detected_as_ip() -> None:
    matches = detect_pii("Version 1.2.3.4 is stable")
    ips = [m for m in matches if m.kind == "ip"]
    assert len(ips) == 0


def test_has_pii_positive() -> None:
    assert has_pii("Email me at bob@test.com")


def test_has_pii_negative() -> None:
    assert not has_pii("No personal data here")


def test_detect_multiple_types() -> None:
    text = "alice@corp.com called 555-123-4567 SSN 999-88-7777"
    matches = detect_pii(text)
    kinds = {m.kind for m in matches}
    assert kinds == {"email", "phone", "ssn"}


def test_matches_sorted_by_position() -> None:
    text = "SSN 111-22-3333 email a@b.com"
    matches = detect_pii(text)
    positions = [m.start for m in matches]
    assert positions == sorted(positions)


# -- replacers ----------------------------------------------------------------


def test_replacer_email_deterministic() -> None:
    r = DeterministicReplacer(salt="test")
    a = r.replace_email("alice@corp.com")
    b = r.replace_email("alice@corp.com")
    assert a == b
    assert "@" in a
    assert a != "alice@corp.com"


def test_replacer_different_emails_different_output() -> None:
    r = DeterministicReplacer(salt="test")
    a = r.replace_email("alice@corp.com")
    b = r.replace_email("bob@corp.com")
    assert a != b


def test_replacer_phone() -> None:
    r = DeterministicReplacer(salt="test")
    result = r.replace_phone("(555) 123-4567")
    assert result != "(555) 123-4567"
    assert result == r.replace_phone("(555) 123-4567")


def test_replacer_ssn() -> None:
    r = DeterministicReplacer(salt="test")
    result = r.replace_ssn("123-45-6789")
    assert result != "123-45-6789"
    assert "-" in result


def test_replacer_ip() -> None:
    r = DeterministicReplacer(salt="test")
    result = r.replace_ip("192.168.1.1")
    assert result.startswith("10.")
    assert result != "192.168.1.1"


def test_replacer_name() -> None:
    r = DeterministicReplacer(salt="test")
    result = r.replace_name("Alice Johnson")
    assert result != "Alice Johnson"
    assert " " in result


def test_replacer_text_replaces_all_pii() -> None:
    r = DeterministicReplacer(salt="test")
    text = "Email alice@corp.com, SSN 123-45-6789, IP 10.0.0.1"
    result = r.replace_text(text)
    assert "alice@corp.com" not in result
    assert "123-45-6789" not in result


def test_replacer_in_dict_recursive() -> None:
    r = DeterministicReplacer(salt="test")
    data = {
        "name": "alice@corp.com",
        "nested": {"phone": "(555) 123-4567"},
        "items": ["bob@test.com", 42],
    }
    result = r.replace_in_dict(data)
    assert "alice@corp.com" not in str(result)
    assert "bob@test.com" not in str(result)
    assert result["items"][1] == 42


def test_replacer_preserves_non_string_values() -> None:
    r = DeterministicReplacer(salt="test")
    data = {"count": 5, "flag": True, "value": None}
    result = r.replace_in_dict(data)
    assert result == data


# -- primitives ---------------------------------------------------------------


def test_deterministic_hash_stable() -> None:
    a = deterministic_hash("hello", salt="test")
    b = deterministic_hash("hello", salt="test")
    assert a == b
    assert len(a) == 12


def test_deterministic_hash_different_inputs() -> None:
    a = deterministic_hash("hello", salt="test")
    b = deterministic_hash("world", salt="test")
    assert a != b


def test_pseudonymize_email_format() -> None:
    result = pseudonymize_email("alice@corp.com", salt="test")
    assert "@" in result
    assert result.endswith(".example")
    assert result != "alice@corp.com"


def test_pseudonymize_name() -> None:
    result = pseudonymize_name("Alice", salt="test")
    assert result.startswith("User-")
    assert result != "Alice"


def test_redact_numeric_sequences() -> None:
    assert redact_numeric_sequences("abc12345678def") == "abc1234def"
    assert redact_numeric_sequences("short 123 ok") == "short 123 ok"


# -- api (snapshot anonymization) -------------------------------------------


def test_anonymize_snapshot() -> None:
    snap = ContextSnapshot(
        version="1",
        organization_name="Acme Corp",
        organization_domain="acme.com",
        captured_at="2024-01-01T00:00:00Z",
        sources=[
            ContextSourceResult(
                provider="slack",
                captured_at="2024-01-01T00:00:00Z",
                status="ok",
                record_counts={"messages": 5},
                data={"channel": "alice@acme.com sent a note"},
            ),
        ],
        metadata={},
    )
    result = anonymize_snapshot(snap, salt="test-salt")
    assert result.organization_name != "Acme Corp"
    assert result.organization_domain != "acme.com"
    assert result.metadata.get("anonymized") is True
    assert "anonymization_salt_hash" in result.metadata
    assert len(result.sources) == 1
    assert "alice@acme.com" not in str(result.sources[0].data)


def test_salt_fingerprint_deterministic() -> None:
    a = _salt_fingerprint("salt")
    b = _salt_fingerprint("salt")
    assert a == b
    assert len(a) == 12


def test_anonymize_empty_domain() -> None:
    snap = ContextSnapshot(
        version="1",
        organization_name="Test",
        organization_domain="",
        captured_at="2024-01-01T00:00:00Z",
        sources=[],
        metadata={},
    )
    result = anonymize_snapshot(snap)
    assert result.organization_domain == ""
