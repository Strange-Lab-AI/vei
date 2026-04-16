from __future__ import annotations

import inspect
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import vei.router.server_fastmcp as server_fastmcp
from vei.router import __main__ as router_main
from vei.router import sse as router_sse
from vei.router.api import create_router
from vei.router.errors import MCPError
from vei.twin import app as twin_app
from vei.ui import app as ui_app

pytestmark = pytest.mark.integration


class _DumpModel:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def model_dump(self, mode: str | None = None) -> dict[str, Any]:
        return dict(self.payload)


def _sample_value(name: str, annotation: Any) -> Any:
    if name == "url":
        return "https://example.com"
    if name in {"to", "email", "principal", "attendee", "owner", "organizer"}:
        return "agent@example.com"
    if name in {"channel"}:
        return "#ops"
    if name in {"currency"}:
        return "USD"
    if name in {"vendor"}:
        return "Vendor"
    if name in {"table"}:
        return "orders"
    if name in {"folder"}:
        return "INBOX"
    if name in {"tool"}:
        return "browser.read"
    if name in {"query"}:
        return "query"
    if name in {"status"}:
        return "open"
    if name in {"priority"}:
        return "P1"
    if name in {"severity"}:
        return "high"
    if name in {"sort_by"}:
        return "name"
    if name in {"sort_dir", "direction"}:
        return "asc"
    if name in {"visibility"}:
        return "internal"
    if name in {"env"}:
        return "prod"
    if name in {"match_field"}:
        return "id"
    if name in {"match_value"}:
        return "row-1"
    if name in {"cell"}:
        return "A1"
    if name in {"formula"}:
        return "=1+1"
    if name in {"thread_ts", "ts"}:
        return "1"
    if name == "flag_key":
        return "checkout_v2"
    if name.endswith("_id") or name == "id":
        return f"{name}-1"
    if name.endswith("_ms") or name in {"limit", "offset", "top_k", "rollout_pct"}:
        return 1
    if name in {"amount", "min_amount", "max_amount"}:
        return 1.0
    if name in {"row", "filters", "payload", "args"}:
        return {}
    if name in {"columns", "tags", "labels", "attendees"}:
        return []
    if name == "lines":
        return [{"item_id": "SKU-1", "desc": "Widget", "qty": 1, "unit_price": 1.0}]
    if (
        name.startswith("include_")
        or name in {"enabled", "descending", "do_not_contact"}
        or annotation is bool
    ):
        return False
    if annotation is int:
        return 1
    if annotation is float:
        return 1.0
    return "value"


def _required_args(fn: Any) -> dict[str, Any]:
    args: dict[str, Any] = {}
    for name, parameter in inspect.signature(fn).parameters.items():
        if parameter.kind in {
            inspect.Parameter.VAR_KEYWORD,
            inspect.Parameter.VAR_POSITIONAL,
        }:
            continue
        if parameter.default is not inspect._empty:
            continue
        args[name] = _sample_value(name, parameter.annotation)
    return args


def test_fastmcp_server_registers_wrappers_and_special_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VEI_PORT", "not-a-port")
    monkeypatch.setenv("FASTMCP_DEBUG", "true")
    monkeypatch.setenv("FASTMCP_DISABLE_SECURITY", "1")
    monkeypatch.setenv("VEI_ALIAS_PACKS", "xero")
    monkeypatch.setenv("VEI_CRM_ALIAS_PACKS", "salesforce")

    router = create_router(seed=11)
    router.bus.clock_ms = 123

    tool_calls: list[tuple[str, dict[str, Any]]] = []

    def fake_call_and_step(tool: str, args: dict[str, Any]) -> dict[str, Any]:
        payload = dict(args)
        tool_calls.append((tool, payload))
        return {"tool": tool, "args": payload}

    monkeypatch.setattr(router, "call_and_step", fake_call_and_step)
    monkeypatch.setattr(
        router,
        "observe",
        lambda focus_hint=None: _DumpModel({"focus": focus_hint, "ok": True}),
    )
    monkeypatch.setattr(
        router,
        "act_and_observe",
        lambda tool, args=None: {
            "tool": tool,
            "args": dict(args or {}),
            "observed": True,
        },
    )
    monkeypatch.setattr(router, "pending", lambda: {"total": 0})
    monkeypatch.setattr(
        router,
        "state_snapshot",
        lambda **kwargs: {"snapshot": kwargs},
    )
    monkeypatch.setattr(
        router,
        "help_payload",
        lambda: {"examples": [], "instructions": "demo"},
    )
    monkeypatch.setattr(
        router,
        "search_tools",
        lambda query, top_k=10: {"query": query, "top_k": top_k, "results": []},
    )
    monkeypatch.setattr(router, "tick", lambda dt_ms: {"dt_ms": dt_ms})

    class DummySession:
        def orientation(self) -> _DumpModel:
            return _DumpModel({"organization_name": "Acme"})

        def capability_graphs(self) -> _DumpModel:
            return _DumpModel(
                {
                    "branch": "main",
                    "clock_ms": 123,
                    "available_domains": ["identity_graph"],
                    "identity_graph": {"nodes": 2},
                }
            )

        def graph_plan(
            self, *, domain: str | None = None, limit: int = 12
        ) -> _DumpModel:
            return _DumpModel({"domain": domain, "limit": limit, "suggested_steps": []})

        def graph_action(self, payload: dict[str, Any]) -> _DumpModel:
            if payload.get("action") == "explode":
                raise ValueError("bad graph action")
            return _DumpModel({"received": payload})

    monkeypatch.setattr(
        server_fastmcp, "ensure_world_session", lambda _router: DummySession()
    )

    server = server_fastmcp.create_mcp_server(router)
    tool_names = set(server._tool_manager._tools)

    assert "vei.help" in tool_names
    assert "xero.create_purchase_order" in tool_names
    assert "salesforce.account.list" in tool_names
    assert "google_admin.list_oauth_apps" in tool_names
    assert server.settings.port == 3001
    assert server.settings.debug is True
    assert server.settings.transport_security is not None
    assert server.settings.transport_security.enable_dns_rebinding_protection is False

    for name in sorted(tool_names):
        if name.startswith("vei."):
            continue
        tool = server._tool_manager.get_tool(name)
        tool.fn(**_required_args(tool.fn))

    seen_tool_names = {tool for tool, _args in tool_calls}
    assert "slack.open_channel" in seen_tool_names
    assert "erp.create_po" in seen_tool_names
    assert "crm.list_companies" in seen_tool_names
    assert "google_admin.list_oauth_apps" in seen_tool_names

    assert server._tool_manager.get_tool("vei.observe").fn(focus="slack") == {
        "focus": "slack",
        "ok": True,
    }
    assert server._tool_manager.get_tool("vei.orientation").fn() == {
        "organization_name": "Acme"
    }
    assert server._tool_manager.get_tool("vei.capability_graphs").fn() == {
        "branch": "main",
        "clock_ms": 123,
        "available_domains": ["identity_graph"],
        "identity_graph": {"nodes": 2},
    }
    assert server._tool_manager.get_tool("vei.capability_graphs").fn(
        domain="identity_graph"
    ) == {
        "branch": "main",
        "clock_ms": 123,
        "domain": "identity_graph",
        "graph": {"nodes": 2},
    }
    assert server._tool_manager.get_tool("vei.capability_graphs").fn(
        domain="missing"
    ) == {
        "error": {
            "code": "unknown_domain",
            "message": "Unknown capability graph domain: missing",
        }
    }
    assert server._tool_manager.get_tool("vei.graph_plan").fn(
        domain="identity_graph", limit=3
    ) == {
        "domain": "identity_graph",
        "limit": 3,
        "suggested_steps": [],
    }
    assert server._tool_manager.get_tool("vei.graph_action").fn(
        domain="identity_graph",
        action="assign_application",
        args={"user_id": "USR-1", "app_id": "APP-1"},
    ) == {
        "received": {
            "domain": "identity_graph",
            "action": "assign_application",
            "args": {"user_id": "USR-1", "app_id": "APP-1"},
            "step_id": None,
        }
    }
    assert server._tool_manager.get_tool("vei.graph_action").fn(
        domain="identity_graph",
        action="explode",
        args={},
    ) == {
        "error": {
            "code": "invalid_graph_action",
            "message": "bad graph action",
        }
    }
    assert server._tool_manager.get_tool("vei.act_and_observe").fn(
        tool="browser.read",
        args={},
    ) == {
        "tool": "browser.read",
        "args": {},
        "observed": True,
    }
    assert server._tool_manager.get_tool("vei.call").fn(
        tool="db.query",
        args={"table": "orders"},
    ) == {
        "tool": "db.query",
        "args": {"table": "orders"},
    }
    assert server._tool_manager.get_tool("vei.tools.search").fn(
        query="slack",
        top_k=-4,
    ) == {
        "query": "slack",
        "top_k": 0,
        "results": [],
    }
    assert server._tool_manager.get_tool("vei.tick").fn(dt_ms=50) == {"dt_ms": 50}
    assert server._tool_manager.get_tool("vei.pending").fn() == {"total": 0}
    assert server._tool_manager.get_tool("vei.state").fn(
        include_state=True,
        tool_tail=5,
        include_receipts=False,
    ) == {
        "snapshot": {
            "include_state": True,
            "tool_tail": 5,
            "include_receipts": False,
        }
    }
    assert server._tool_manager.get_tool("vei.help").fn()["examples"]
    assert server._tool_manager.get_tool("vei.ping").fn() == {
        "ok": True,
        "time_ms": 123,
    }

    replacement_router = create_router(seed=99)
    replacement_router.bus.clock_ms = 456
    monkeypatch.setattr(
        server_fastmcp, "create_router", lambda **kwargs: replacement_router
    )
    assert server._tool_manager.get_tool("vei.reset").fn(seed=99) == {
        "ok": True,
        "seed": 99,
        "time_ms": 456,
    }


def test_fastmcp_error_wrappers_return_error_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    router = create_router(seed=7)

    def raising_call_and_step(tool: str, args: dict[str, Any]) -> dict[str, Any]:
        raise MCPError("bad_request", f"failed:{tool}")

    monkeypatch.setattr(router, "call_and_step", raising_call_and_step)

    server = server_fastmcp.create_mcp_server(router)
    handled: set[str] = set()

    for name, tool in server._tool_manager._tools.items():
        if name.startswith("vei.") and name != "vei.call":
            continue
        source = inspect.getsource(tool.fn)
        if (
            "except MCPError" not in source
            and "safe_call(" not in source
            and "_safe_call(" not in source
        ):
            continue
        result = tool.fn(**_required_args(tool.fn))
        assert result["error"]["code"] == "bad_request"
        handled.add(name)

    assert "slack.open_channel" in handled
    assert "xero.create_purchase_order" in handled
    assert "google_admin.list_oauth_apps" in handled
    assert "vei.call" in handled


def test_router_entrypoints_use_expected_transports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = SimpleNamespace(
        seed=7,
        artifacts_dir="./_vei_out/tests",
        scenario=None,
        host="127.0.0.1",
        port=3030,
    )
    router = object()
    runs: list[str] = []

    class DummyServer:
        def run(self, mode: str) -> None:
            runs.append(mode)

    dummy_server = DummyServer()
    monkeypatch.setattr(
        router_main.Config,
        "from_env",
        classmethod(lambda cls: cfg),
    )
    monkeypatch.setattr(
        router_sse.Config,
        "from_env",
        classmethod(lambda cls: cfg),
    )
    monkeypatch.setattr(router_main, "create_router", lambda **kwargs: router)
    monkeypatch.setattr(router_sse, "create_router", lambda **kwargs: router)
    monkeypatch.setattr(
        router_main,
        "create_mcp_server",
        lambda router, host=None, port=None: dummy_server,
    )
    monkeypatch.setattr(
        router_sse,
        "create_mcp_server",
        lambda router, host=None, port=None: dummy_server,
    )

    router_main.main()
    router_sse.main()

    assert runs == ["stdio", "sse"]


def test_ui_and_twin_app_wrappers_invoke_uvicorn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runs: list[tuple[Any, str, int, str]] = []
    monkeypatch.setitem(
        sys.modules,
        "uvicorn",
        SimpleNamespace(
            run=lambda app, host, port, log_level: runs.append(
                (app, host, port, log_level)
            )
        ),
    )
    monkeypatch.setattr(
        ui_app, "create_ui_app", lambda root, **_kw: {"kind": "ui", "root": str(root)}
    )
    monkeypatch.setattr(
        twin_app,
        "create_twin_gateway_app",
        lambda root: {"kind": "twin", "root": str(root)},
    )

    ui_app.serve_ui(tmp_path, host="0.0.0.0", port=3010)
    twin_app.serve_customer_twin(tmp_path, host="0.0.0.0", port=3020)

    assert runs == [
        ({"kind": "ui", "root": str(tmp_path)}, "0.0.0.0", 3010, "warning"),
        ({"kind": "twin", "root": str(tmp_path)}, "0.0.0.0", 3020, "warning"),
    ]
