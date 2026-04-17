from __future__ import annotations

import pytest

from vei.router.core import MCPError, Router
from vei.world import get_scenario

pytestmark = pytest.mark.integration


@pytest.fixture()
def router() -> Router:
    return Router(
        seed=1234,
        artifacts_dir=None,
        scenario=get_scenario("campaign_launch_guardrail"),
    )


def test_knowledge_list_assets_rejects_invalid_kind(router: Router) -> None:
    with pytest.raises(MCPError) as exc:
        router.call_and_step("knowledge.list_assets", {"kinds": ["not-a-kind"]})

    assert exc.value.code == "invalid_args"


def test_knowledge_compose_rejects_invalid_mode(router: Router) -> None:
    with pytest.raises(MCPError) as exc:
        router.call_and_step(
            "knowledge.compose_artifact",
            {
                "subject_object_ref": "crm_deal:CRM-NSG-D1",
                "mode": "broken-mode",
            },
        )

    assert exc.value.code == "invalid_args"
