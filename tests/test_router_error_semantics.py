from __future__ import annotations

import pytest

from vei.router.core import MCPError, Router

pytestmark = pytest.mark.integration


def test_mail_reply_unknown_message_raises_mcperror() -> None:
    router = Router(seed=21, artifacts_dir=None)
    with pytest.raises(MCPError) as exc:
        router.call_and_step(
            "mail.reply", {"id": "m-does-not-exist", "body_text": "ping"}
        )
    assert exc.value.code == "unknown_message"


def test_erp_get_po_unknown_id_raises_mcperror() -> None:
    router = Router(seed=22, artifacts_dir=None)
    with pytest.raises(MCPError) as exc:
        router.call_and_step("erp.get_po", {"id": "PO-404"})
    assert exc.value.code == "unknown_po"


def test_crm_get_contact_unknown_id_raises_mcperror() -> None:
    router = Router(seed=23, artifacts_dir=None)
    with pytest.raises(MCPError) as exc:
        router.call_and_step("crm.get_contact", {"id": "C-404"})
    assert exc.value.code == "unknown_contact"
