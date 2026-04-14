from __future__ import annotations

import pytest

from vei.router.core import MCPError, Router
from vei.world.scenarios import scenario_multi_channel

pytestmark = pytest.mark.integration


@pytest.fixture()
def router():
    return Router(seed=1234, artifacts_dir=None, scenario=scenario_multi_channel())


def test_okta_tools_registered(router: Router):
    search = router.search_tools("okta")
    names = {entry["name"] for entry in search["results"]}
    assert "okta.list_users" in names
    assert "okta.assign_group" in names


def test_okta_group_assignment(router: Router):
    router.call_and_step(
        "okta.assign_group", {"user_id": "USR-2001", "group_id": "GRP-procurement"}
    )
    user = router.call_and_step("okta.get_user", {"user_id": "USR-2001"})
    assert "GRP-procurement" in user["groups"]


def test_okta_reset_password_rejects_deprovisioned(router: Router):
    with pytest.raises(MCPError) as exc:
        router.call_and_step("okta.reset_password", {"user_id": "USR-3001"})
    assert exc.value.code == "okta.invalid_state"


def test_okta_pagination_suspend_and_unassign(router: Router):
    first = router.call_and_step("okta.list_users", {"limit": 1, "sort_by": "email"})
    assert first["count"] == 1
    assert first["total"] >= 1

    suspended = router.call_and_step("okta.suspend_user", {"user_id": "USR-2001"})
    assert suspended["status"] == "SUSPENDED"
    unsuspended = router.call_and_step("okta.unsuspend_user", {"user_id": "USR-2001"})
    assert unsuspended["status"] == "ACTIVE"

    apps = router.call_and_step("okta.list_applications", {"limit": 1})
    app_id = apps["applications"][0]["id"]
    router.call_and_step(
        "okta.assign_application", {"user_id": "USR-2001", "app_id": app_id}
    )
    unassigned = router.call_and_step(
        "okta.unassign_application", {"user_id": "USR-2001", "app_id": app_id}
    )
    assert unassigned["app_id"] == app_id
