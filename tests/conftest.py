from __future__ import annotations

import os
from pathlib import Path
import pytest


def _simple_load_env(dotenv_path: Path) -> None:
    try:
        with dotenv_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                os.environ.setdefault(key, val)
    except Exception:
        pass


# Load .env before tests are collected so skipif() can see secrets
def pytest_configure(config):  # type: ignore[override]
    repo_root = Path(__file__).resolve().parents[1]
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        try:
            from dotenv import load_dotenv  # type: ignore

            load_dotenv(dotenv_path=str(dotenv_path), override=False)
        except Exception:
            _simple_load_env(dotenv_path)


# Force anyio to use asyncio backend to avoid optional trio dependency
@pytest.fixture
def anyio_backend():  # type: ignore[override]
    return "asyncio"


@pytest.fixture
def sample_snapshot():
    """Minimal Acme Cloud ContextSnapshot shared across test files.

    Tests that need richer data should extend this or build their own.
    """
    from vei.context.models import ContextSnapshot, ContextSourceResult

    return ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.ai",
        captured_at="2026-03-24T16:00:00+00:00",
        sources=[
            ContextSourceResult(
                provider="slack",
                captured_at="2026-03-24T16:00:00+00:00",
                status="ok",
                record_counts={"channels": 1, "messages": 1},
                data={
                    "channels": [
                        {
                            "channel": "#revops-war-room",
                            "unread": 1,
                            "messages": [
                                {
                                    "ts": "1710300000.000100",
                                    "user": "maya.ops",
                                    "text": "Renewal is exposed unless we land the onboarding fix today.",
                                }
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="jira",
                captured_at="2026-03-24T16:00:00+00:00",
                status="ok",
                record_counts={"issues": 1},
                data={
                    "issues": [
                        {
                            "ticket_id": "ACME-101",
                            "title": "Renewal blocker: onboarding API timing out",
                            "status": "open",
                            "assignee": "maya.ops",
                            "description": "Customer onboarding export is timing out.",
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="gmail",
                captured_at="2026-03-24T16:00:00+00:00",
                status="ok",
                record_counts={"threads": 1, "messages": 1},
                data={
                    "threads": [
                        {
                            "thread_id": "thr-001",
                            "subject": "Renewal risk review",
                            "messages": [
                                {
                                    "from": "jordan@apexfinancial.example.com",
                                    "to": "support@acme.ai",
                                    "subject": "Renewal risk review",
                                    "snippet": "Need a clear owner and a confirmed timeline.",
                                    "labels": ["IMPORTANT"],
                                    "unread": True,
                                }
                            ],
                        }
                    ]
                },
            ),
        ],
    )
