"""Redaction checks for training data batches.

Plugs vei.connectors.redaction; ensures no emails, phones, or secret
tokens appear in training samples.
"""

from __future__ import annotations

import re
from typing import List

from vei.dynamics.feed.canonical_feed import TrainingSample

_EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
_PHONE_RE = re.compile(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b")
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_API_KEY_RE = re.compile(r"\b(sk-|pk_|AKIA|ghp_|gho_)[A-Za-z0-9]{10,}\b")


def check_sample_for_pii(sample: TrainingSample) -> List[str]:
    """Return a list of PII violations found in a training sample."""
    violations: List[str] = []
    text = sample.model_dump_json()
    if _EMAIL_RE.search(text):
        violations.append("email_address_found")
    if _PHONE_RE.search(text):
        violations.append("phone_number_found")
    if _SSN_RE.search(text):
        violations.append("ssn_found")
    if _API_KEY_RE.search(text):
        violations.append("api_key_found")
    return violations


def assert_batch_clean(samples: List[TrainingSample]) -> None:
    """Raise if any sample in the batch contains PII."""
    for sample in samples:
        violations = check_sample_for_pii(sample)
        if violations:
            raise ValueError(f"PII found in sample {sample.sample_id}: {violations}")
