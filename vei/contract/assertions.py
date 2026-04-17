from __future__ import annotations

from typing import Any, Iterable, List, Optional

from vei.knowledge.api import resolve_knowledge_now_ms, validate_composed_asset
from vei.scenario_engine.models import AssertionSpec

from .models import ContractSurface


def infer_assertion_source(assertion: AssertionSpec) -> ContractSurface:
    if assertion.kind.startswith("result_"):
        return "tool_result"
    if assertion.kind.startswith("observation_"):
        return "visible_observation"
    if assertion.kind.startswith("state_"):
        return "oracle_state"
    if assertion.kind == "pending_max":
        return "pending"
    if assertion.kind == "time_max_ms":
        return "time"
    return "oracle_state"


def evaluate_assertion_specs(
    *,
    assertions: Iterable[AssertionSpec],
    result: Any,
    observation: dict[str, Any],
    pending: dict[str, int],
    oracle_state: dict[str, Any],
    time_ms: int,
) -> List[str]:
    failures: List[str] = []
    for assertion in assertions:
        msg = _assertion_failure(
            assertion=assertion,
            result=result,
            observation=observation,
            pending=pending,
            oracle_state=oracle_state,
            time_ms=time_ms,
        )
        if msg:
            failures.append(msg)
    return failures


def _assertion_failure(
    *,
    assertion: AssertionSpec,
    result: Any,
    observation: dict[str, Any],
    pending: dict[str, int],
    oracle_state: dict[str, Any],
    time_ms: int,
) -> Optional[str]:
    if assertion.kind == "result_contains":
        value = _resolve_field(result, assertion.field)
        needle = assertion.contains or ""
        if needle not in str(value):
            return f"expected result field '{assertion.field}' to contain '{needle}'"
        return None

    if assertion.kind == "result_not_contains":
        value = _resolve_field(result, assertion.field)
        needle = assertion.contains or ""
        if needle in str(value):
            return (
                f"expected result field '{assertion.field}' to not contain '{needle}'"
            )
        return None

    if assertion.kind == "result_equals":
        value = _resolve_field(result, assertion.field)
        expected = assertion.equals
        if value != expected:
            return (
                f"expected result field '{assertion.field}' == {expected!r}, "
                f"got {value!r}"
            )
        return None

    if assertion.kind == "observation_contains":
        focus = assertion.focus or "summary"
        value = _resolve_field(observation, focus)
        needle = assertion.contains or ""
        if needle not in str(value):
            return f"expected observation '{focus}' to contain '{needle}'"
        return None

    if assertion.kind == "observation_not_contains":
        focus = assertion.focus or "summary"
        value = _resolve_field(observation, focus)
        needle = assertion.contains or ""
        if needle in str(value):
            return f"expected observation '{focus}' to not contain '{needle}'"
        return None

    if assertion.kind == "pending_max":
        field = assertion.field or "total"
        value = _resolve_field(pending, field)
        max_value = assertion.max_value if assertion.max_value is not None else 0
        try:
            numeric = int(value)
        except Exception:  # noqa: BLE001
            return f"pending field '{field}' is not numeric: {value}"
        if numeric > max_value:
            return f"expected pending '{field}' <= {max_value}, got {numeric}"
        return None

    if assertion.kind == "state_contains":
        value = _resolve_field(oracle_state, assertion.field)
        needle = assertion.contains or ""
        if needle not in str(value):
            return f"expected state field '{assertion.field}' to contain '{needle}'"
        return None

    if assertion.kind == "state_not_contains":
        value = _resolve_field(oracle_state, assertion.field)
        needle = assertion.contains or ""
        if needle in str(value):
            return f"expected state field '{assertion.field}' to not contain '{needle}'"
        return None

    if assertion.kind == "state_equals":
        value = _resolve_field(oracle_state, assertion.field)
        expected = assertion.equals
        if value != expected:
            return (
                f"expected state field '{assertion.field}' == {expected!r}, "
                f"got {value!r}"
            )
        return None

    if assertion.kind == "state_exists":
        value = _resolve_field(oracle_state, assertion.field)
        if value is None:
            return f"expected state field '{assertion.field}' to exist"
        return None

    if assertion.kind == "state_count_equals":
        value = _resolve_field(oracle_state, assertion.field)
        count = _resolve_count(value)
        expected = assertion.equals
        if count is None:
            return f"state field '{assertion.field}' is not countable: {value!r}"
        if count != expected:
            return (
                f"expected state field '{assertion.field}' count == {expected!r}, "
                f"got {count!r}"
            )
        return None

    if assertion.kind == "state_count_max":
        value = _resolve_field(oracle_state, assertion.field)
        count = _resolve_count(value)
        max_value = assertion.max_value if assertion.max_value is not None else 0
        if count is None:
            return f"state field '{assertion.field}' is not countable: {value!r}"
        if count > max_value:
            return (
                f"expected state field '{assertion.field}' count <= {max_value}, "
                f"got {count}"
            )
        return None

    if assertion.kind == "time_max_ms":
        max_value = assertion.max_value if assertion.max_value is not None else 0
        if time_ms > max_value:
            return f"expected workflow time <= {max_value} ms, got {time_ms} ms"
        return None

    if assertion.kind in {
        "citations_present",
        "citations_resolve",
        "sources_within_shelf_life",
        "numbers_reconcile",
        "format_matches_template",
    }:
        composed = _resolve_field(oracle_state, assertion.field)
        if not isinstance(composed, dict):
            return f"knowledge assertion field '{assertion.field}' does not resolve to a composed artifact"
        knowledge_assets = _resolve_field(oracle_state, "components.knowledge.assets")
        if not isinstance(knowledge_assets, dict):
            return "knowledge assets are missing from oracle state"
        try:
            asset = _coerce_knowledge_asset(composed)
            assets = {
                str(asset_id): _coerce_knowledge_asset(payload)
                for asset_id, payload in knowledge_assets.items()
                if isinstance(payload, dict)
            }
            knowledge_store = _resolve_field(oracle_state, "components.knowledge")
            now_ms = resolve_knowledge_now_ms(
                knowledge_store if isinstance(knowledge_store, dict) else {},
                clock_ms=int(_resolve_field(oracle_state, "clock_ms") or time_ms or 0),
            )
            validation = validate_composed_asset(
                asset,
                assets=assets,
                now_ms=now_ms,
                tolerance=float(assertion.params.get("tolerance", 0.0) or 0.0),
            )
        except Exception as exc:  # noqa: BLE001
            return (
                f"knowledge assertion failed to validate: {type(exc).__name__}: {exc}"
            )
        passed = getattr(validation, assertion.kind)
        if not passed:
            issue = validation.issues[0] if validation.issues else assertion.kind
            return f"expected {assertion.kind} for '{assertion.field}': {issue}"
        return None

    return f"unknown assertion kind: {assertion.kind}"


def _resolve_field(payload: Any, field: str | None) -> Any:
    if field is None or field == "":
        return payload
    current = payload
    for key in field.split("."):
        if isinstance(current, dict):
            current = current.get(key)
            continue
        if isinstance(current, list):
            try:
                current = current[int(key)]
            except Exception:  # noqa: BLE001
                return None
            continue
        return None
    return current


def _resolve_count(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (str, list, tuple, set, dict)):
        return len(value)
    try:
        return len(value)  # type: ignore[arg-type]
    except Exception:  # noqa: BLE001
        return None


def _coerce_knowledge_asset(value: Any) -> Any:
    from vei.knowledge.api import KnowledgeAsset

    if isinstance(value, KnowledgeAsset):
        return value
    if isinstance(value, dict):
        return KnowledgeAsset.model_validate(value)
    raise TypeError(f"unsupported knowledge asset payload: {type(value).__name__}")
