from __future__ import annotations

import re
from typing import Any, Dict

_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_PHONE_RE = re.compile(r"\b(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}\b")
_KEY_RE = re.compile(r"\b(?:sk|pk|api|token)[_\-]?[A-Za-z0-9]{8,}\b", re.IGNORECASE)


def redact_text(value: str) -> str:
    redacted = _EMAIL_RE.sub("[REDACTED_EMAIL]", value)
    redacted = _PHONE_RE.sub("[REDACTED_PHONE]", redacted)
    redacted = _KEY_RE.sub("[REDACTED_KEY]", redacted)
    return redacted


def redact_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): redact_payload(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_payload(v) for v in value]
    if isinstance(value, tuple):
        return [redact_payload(v) for v in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def redact_mapping(payload: Dict[str, Any]) -> Dict[str, Any]:
    return redact_payload(payload)
