from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from fastapi.testclient import TestClient

from vei.ui import api as ui_api
from vei.ui import _public_demo_routes as public_demo_routes

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLIC_DEMO_ROOT = REPO_ROOT / "docs/examples/news-public-history-demo/workspace"


def test_public_demo_models_validate_defaults() -> None:
    request = ui_api.PublicDemoChatRequest(message="What was visible?")

    assert request.source_id == "news_americanstories_public_world"
    assert request.as_of == "1837-09-06"
    assert request.topic == "all_public_record"


def test_public_demo_status_and_chat_only_use_pre_cutoff_evidence() -> None:
    client = TestClient(ui_api.create_ui_app(PUBLIC_DEMO_ROOT))

    status_response = client.get("/api/workspace/public-demo")

    assert status_response.status_code == 200
    status = status_response.json()
    assert status["available"] is True
    assert status["source"]["source_id"] == "news_americanstories_public_world"
    assert status["source"]["default_topic"] == "all_public_record"
    assert status["source"]["event_count"] >= 400
    assert status["source"]["first_timestamp"] == "1836-01-06T00:00:00Z"
    assert status["source"]["last_timestamp"] == "1838-12-26T00:00:00Z"
    assert status["scoring_available"] is True
    assert status["scoring_source"] == "live_jepa"
    assert status["scoring_checkpoint_path"].endswith("jepa_model.pt")
    assert 10 <= len(status["timeline_points"]) <= 13
    assert not any(
        point["label"] == "Decision point" for point in status["timeline_points"]
    )
    assert not any(": " in point["label"] for point in status["timeline_points"])
    assert status["evidence_events"]
    assert all(
        event["timestamp"] <= "1837-09-06T00:00:00Z"
        for event in status["evidence_events"]
    )
    subjects = " ".join(event["subject"] for event in status["evidence_events"])
    assert "President" in subjects or "Senate" in subjects or "Congress" in subjects
    assert "Abolition" in subjects or "slavery" in subjects.lower()

    chat_response = client.post(
        "/api/workspace/public-demo/chat",
        json={
            "source_id": "news_americanstories_public_world",
            "as_of": "1837-09-06",
            "topic": "banking_markets",
            "message": "What banking and credit risks were visible?",
        },
    )

    assert chat_response.status_code == 200
    chat = chat_response.json()
    assert chat["cited_event_ids"]
    assert all(
        event["timestamp"] <= "1837-09-06T00:00:00Z" for event in chat["cited_events"]
    )
    assert "Future bill passage marker" not in chat["assistant_text"]
    assert "Church notices" not in chat["assistant_text"]
    assert "not using later outcomes" in chat["assistant_text"]

    earlier_response = client.get(
        "/api/workspace/public-demo",
        params={"as_of": "1837-06-02", "topic": "banking_markets"},
    )
    assert earlier_response.status_code == 200
    earlier = earlier_response.json()
    assert earlier["as_of"] == "1837-06-02T00:00:00Z"
    assert all(
        event["timestamp"] <= "1837-06-02T00:00:00Z"
        for event in earlier["evidence_events"]
    )
    assert "Banking bill debate" not in earlier["state_summary"]


def test_public_demo_status_reports_missing_live_jepa_without_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "VEI_PUBLIC_DEMO_JEPA_CHECKPOINT",
        str(tmp_path / "missing-model.pt"),
    )
    client = TestClient(ui_api.create_ui_app(PUBLIC_DEMO_ROOT))

    status_response = client.get("/api/workspace/public-demo")

    assert status_response.status_code == 200
    status = status_response.json()
    assert status["available"] is True
    assert status["scoring_available"] is False
    assert status["scoring_source"] == "live_jepa"
    assert "will not fabricate rankings" in status["scoring_unavailable_reason"]

    response = client.post(
        "/api/workspace/public-demo/score",
        json={
            "source_id": "news_americanstories_public_world",
            "as_of": "1837-09-06",
            "topic": "all_public_record",
            "decision_title": "Public-world response",
            "candidates": [
                {"label": "Your scenario", "action": "Test a public action."}
            ],
        },
    )

    assert response.status_code == 503
    assert "will not fabricate rankings" in response.json()["detail"]


def test_public_demo_scores_custom_scenario_with_live_jepa(
    tmp_path: Path,
    monkeypatch,
) -> None:
    checkpoint = tmp_path / "model.pt"
    checkpoint.write_text("stub", encoding="utf-8")
    monkeypatch.setenv("VEI_PUBLIC_DEMO_JEPA_CHECKPOINT", str(checkpoint))
    monkeypatch.setenv("VEI_PUBLIC_DEMO_ARTIFACTS_ROOT", str(tmp_path / "runs"))
    calls: dict[str, Any] = {}

    def fake_run(*_args: Any, **kwargs: Any) -> SimpleNamespace:
        calls.update(kwargs)
        result_root = tmp_path / "runs" / "fake"
        result_root.mkdir(parents=True, exist_ok=True)
        result_path = result_root / "result.json"
        result_path.write_text(
            json.dumps(
                {
                    "candidates": [
                        {
                            "candidate_id": "candidate_1",
                            "label": "Your scenario",
                            "action": kwargs["candidates"][0].action,
                            "strategic_rank": 1,
                            "strategic_usefulness_score": 0.72,
                            "business_heads": {
                                "enterprise_risk": 0.31,
                                "commercial_position_proxy": 0.62,
                                "org_strain_proxy": 0.24,
                                "stakeholder_trust": 0.64,
                                "execution_drag": 0.28,
                            },
                            "future_state_heads": {
                                "regulatory_exposure": 0.22,
                                "liquidity_stress": 0.33,
                                "governance_response": 0.68,
                                "evidence_control": 0.61,
                                "external_confidence_pressure": 0.28,
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(
            artifacts=SimpleNamespace(result_json_path=result_path),
        )

    monkeypatch.setattr(
        public_demo_routes,
        "run_news_state_point_counterfactual",
        fake_run,
    )
    client = TestClient(ui_api.create_ui_app(PUBLIC_DEMO_ROOT))

    response = client.post(
        "/api/workspace/public-demo/score",
        json={
            "source_id": "news_americanstories_public_world",
            "as_of": "1837-06-02",
            "topic": "all_public_record",
            "decision_title": "Custom public action",
            "candidates": [
                {
                    "label": "Your scenario",
                    "action": "Open a public relief watch and publish weekly indicators.",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["as_of"] == "1837-06-02T00:00:00Z"
    assert payload["scoring_source"] == "live_jepa"
    assert payload["scoring_checkpoint_path"] == str(checkpoint)
    assert payload["scoring_artifact_path"].endswith("result.json")
    assert payload["candidates"][0]["label"] == "Your scenario"
    assert payload["candidates"][0]["source"] == "live_jepa"
    assert calls["topic"] == "all_public_record"
    assert calls["as_of"] == "1837-06-02T00:00:00Z"
    assert calls["checkpoint_path"] == checkpoint
    assert all(
        event["timestamp"] <= "1837-06-02T00:00:00Z"
        for event in payload["evidence_events"]
    )


def test_public_demo_rejects_unknown_source_and_invalid_date() -> None:
    client = TestClient(ui_api.create_ui_app(PUBLIC_DEMO_ROOT))

    bad_source = client.post(
        "/api/workspace/public-demo/chat",
        json={
            "source_id": "unknown",
            "as_of": "1837-09-06",
            "message": "What was visible?",
        },
    )
    assert bad_source.status_code == 400
    assert "unknown public demo source_id" in bad_source.json()["detail"]

    bad_date = client.post(
        "/api/workspace/public-demo/chat",
        json={
            "source_id": "news_americanstories_public_world",
            "as_of": "not-a-date",
            "message": "What was visible?",
        },
    )
    assert bad_date.status_code == 400


def test_public_demo_topic_lenses_filter_broad_world() -> None:
    client = TestClient(ui_api.create_ui_app(PUBLIC_DEMO_ROOT))

    response = client.get(
        "/api/workspace/public-demo",
        params={"as_of": "1837-09-06", "topic": "government_policy"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["topic"] == "government_policy"
    assert 10 <= len(payload["timeline_points"]) <= 13
    assert payload["evidence_events"]
    assert all(
        event["timestamp"] <= "1837-09-06T00:00:00Z"
        for event in payload["evidence_events"]
    )
    subjects = " ".join(event["subject"] for event in payload["evidence_events"])
    assert "President" in subjects or "Senate" in subjects or "Congress" in subjects

    early_lens = client.get(
        "/api/workspace/public-demo",
        params={"as_of": "1836-01-05", "topic": "international"},
    )
    assert early_lens.status_code == 200
    early_payload = early_lens.json()
    assert early_payload["available"] is True
    assert early_payload["topic"] == "international"
    assert early_payload["evidence_events"] == []
    assert "no topic-matched public evidence" in early_payload["state_summary"]


def test_public_demo_governor_status_does_not_require_company_scenario() -> None:
    client = TestClient(ui_api.create_ui_app(PUBLIC_DEMO_ROOT))

    response = client.get("/api/workspace/governor")

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "public_history"
    assert client.get("/api/workspace/historical").json() == {}
    assert client.get("/api/workspace/whatif").json()["available"] is False
