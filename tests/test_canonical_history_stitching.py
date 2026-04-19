from __future__ import annotations

from vei.context.api import (
    ContextSnapshot,
    ContextSourceResult,
    build_canonical_history_bundle,
)


def test_canonical_history_ignores_junk_tokens_for_case_ids() -> None:
    snapshot = ContextSnapshot(
        organization_name="Dispatch",
        organization_domain="thedispatch.ai",
        sources=[
            ContextSourceResult(
                provider="gmail",
                captured_at="2026-04-18T12:00:00Z",
                status="ok",
                data={
                    "threads": [
                        {
                            "thread_id": "thread-utf8",
                            "subject": "=?UTF-8?Q?Security_alert?=",
                            "messages": [
                                {
                                    "message_id": "<utf8@dispatch.ai>",
                                    "from": "notify@google.com",
                                    "to": "admin@thedispatch.ai",
                                    "subject": "=?UTF-8?Q?Security_alert?=",
                                    "date": "Tue, 09 Jan 2024 21:36:03 +0000",
                                    "snippet": "Recovery email was changed.",
                                }
                            ],
                        },
                        {
                            "thread_id": "thread-doctype",
                            "subject": "DOCTYPE review",
                            "messages": [
                                {
                                    "message_id": "<doctype@dispatch.ai>",
                                    "from": "ops@thedispatch.ai",
                                    "to": "jon@thedispatch.ai",
                                    "subject": "DOCTYPE review",
                                    "date": "Tue, 09 Jan 2024 22:00:00 +0000",
                                    "snippet": "DOCTYPE review attached.",
                                }
                            ],
                        },
                        {
                            "thread_id": "thread-gpt4",
                            "subject": "GPT-4 rollout notes",
                            "messages": [
                                {
                                    "message_id": "<gpt4@dispatch.ai>",
                                    "from": "ops@thedispatch.ai",
                                    "to": "jon@thedispatch.ai",
                                    "subject": "GPT-4 rollout notes",
                                    "date": "Tue, 09 Jan 2024 22:30:00 +0000",
                                    "snippet": "GPT-4 notes attached.",
                                }
                            ],
                        },
                    ],
                    "profile": {},
                },
            )
        ],
    )

    bundle = build_canonical_history_bundle(snapshot)
    case_ids = {row.case_id for row in bundle.index.rows}

    assert "case:UTF-8" not in case_ids
    assert "case:DOCTYPE" not in case_ids
    assert "case:GPT-4" not in case_ids
