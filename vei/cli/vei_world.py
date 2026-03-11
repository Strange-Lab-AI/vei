from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

import typer

from vei.capability_graph.api import build_runtime_capability_graphs
from vei.orientation.api import build_world_orientation
from vei.world.state import StateStore
from vei.world.models import WorldState


app = typer.Typer(
    add_completion=False,
    help="Inspect VEI world snapshots, branches, and receipts.",
)


def _resolve_root(state_dir: Optional[Path]) -> Path:
    raw = Path(state_dir) if state_dir else None
    if raw is None:
        env = os.environ.get("VEI_STATE_DIR")
        if not env:
            raise typer.BadParameter("Provide --state-dir or set VEI_STATE_DIR")
        raw = Path(env)
    path = raw.expanduser().resolve()
    if not path.exists():
        raise typer.BadParameter(f"State directory does not exist: {path}")
    return path


def _branch_path(root: Path, branch: str) -> Path:
    safe = StateStore._sanitize_branch(branch)  # type: ignore[attr-defined]
    bpath = root / safe
    if not bpath.exists():
        raise typer.BadParameter(f"Branch '{branch}' not found under {root}")
    return bpath


def _load_snapshot(path: Path) -> Dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_snapshot_payload(
    root: Path, branch: str, snapshot: Optional[int]
) -> Dict[str, object]:
    store = StateStore(base_dir=root, branch=branch)
    paths = _snapshot_paths(store)
    if not paths:
        raise typer.BadParameter("No snapshots captured yet")
    if snapshot is None:
        return _load_snapshot(paths[-1])
    matching = [p for p in paths if p.stem == f"{snapshot:09d}"]
    if not matching:
        raise typer.BadParameter(f"Snapshot {snapshot} not found")
    return _load_snapshot(matching[0])


def _flatten(prefix: str, value: object, out: Dict[str, object]) -> None:
    if isinstance(value, dict):
        for key, sub in value.items():
            new_prefix = f"{prefix}.{key}" if prefix else str(key)
            _flatten(new_prefix, sub, out)
    else:
        out[prefix] = value


def _diff_snapshots(a: Dict[str, object], b: Dict[str, object]) -> Dict[str, object]:
    flat_a: Dict[str, object] = {}
    flat_b: Dict[str, object] = {}
    _flatten("", a.get("data", {}), flat_a)
    _flatten("", b.get("data", {}), flat_b)
    keys = set(flat_a) | set(flat_b)
    added: Dict[str, object] = {}
    removed: Dict[str, object] = {}
    changed: Dict[str, Dict[str, object]] = {}
    for key in sorted(keys):
        if key not in flat_a:
            added[key] = flat_b[key]
        elif key not in flat_b:
            removed[key] = flat_a[key]
        elif flat_a[key] != flat_b[key]:
            changed[key] = {"from": flat_a[key], "to": flat_b[key]}
    return {"added": added, "removed": removed, "changed": changed}


def _snapshot_paths(store: StateStore) -> List[Path]:
    return store.list_snapshot_paths()


def _load_receipts(branch_path: Path) -> List[Dict[str, object]]:
    receipts_path = branch_path / "receipts.jsonl"
    if not receipts_path.exists():
        return []
    out: List[Dict[str, object]] = []
    with receipts_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


@app.command("list")
def list_snapshots(
    state_dir: Optional[Path] = typer.Option(None, help="Root VEI state directory"),
    branch: str = typer.Option("main", help="Branch name"),
) -> None:
    """List available snapshots for a branch."""

    root = _resolve_root(state_dir)
    store = StateStore(base_dir=root, branch=branch)
    paths = _snapshot_paths(store)
    summary = []
    for path in paths:
        snap = _load_snapshot(path)
        summary.append(
            {
                "index": snap.get("index"),
                "clock_ms": snap.get("clock_ms"),
                "path": str(path),
            }
        )
    typer.echo(
        json.dumps(
            {"branch": branch, "head": store.head, "snapshots": summary},
            indent=2,
        )
    )


@app.command("show")
def show_snapshot(
    state_dir: Optional[Path] = typer.Option(None, help="Root VEI state directory"),
    branch: str = typer.Option("main", help="Branch name"),
    snapshot: Optional[int] = typer.Option(
        None,
        help="Snapshot index (default: latest)",
    ),
    include_state: bool = typer.Option(False, help="Include full state payload"),
    receipts_tail: int = typer.Option(0, help="Include last N receipts"),
) -> None:
    """Show a snapshot summary (latest by default)."""

    root = _resolve_root(state_dir)
    snap = _resolve_snapshot_payload(root, branch, snapshot)
    selected_path = (
        _branch_path(root, branch)
        / "snapshots"
        / f"{int(snap.get('index', 0)):09d}.json"
    )
    output = {
        "branch": branch,
        "path": str(selected_path),
        "index": snap.get("index"),
        "clock_ms": snap.get("clock_ms"),
    }
    if include_state:
        output["data"] = snap.get("data")
    if receipts_tail:
        receipts = _load_receipts(_branch_path(root, branch))
        output["receipts"] = receipts[-receipts_tail:]
    typer.echo(json.dumps(output, indent=2))


@app.command("graphs")
def show_capability_graphs(
    state_dir: Optional[Path] = typer.Option(None, help="Root VEI state directory"),
    branch: str = typer.Option("main", help="Branch name"),
    snapshot: Optional[int] = typer.Option(
        None, help="Snapshot index (default: latest)"
    ),
    domain: Optional[str] = typer.Option(
        None,
        help=(
            "Optional capability domain filter such as identity_graph, "
            "doc_graph, work_graph, comm_graph, or revenue_graph"
        ),
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Render runtime capability graphs from a stored snapshot."""

    root = _resolve_root(state_dir)
    snap = _resolve_snapshot_payload(root, branch, snapshot)
    state = WorldState.model_validate(snap.get("data", {}))
    graphs = build_runtime_capability_graphs(state).model_dump(mode="json")
    if domain:
        normalized = domain.strip().lower()
        if normalized not in {
            "comm_graph",
            "doc_graph",
            "work_graph",
            "identity_graph",
            "revenue_graph",
        }:
            raise typer.BadParameter(f"Unknown capability graph domain: {domain}")
        payload = {
            "branch": graphs["branch"],
            "clock_ms": graphs["clock_ms"],
            "domain": normalized,
            "graph": graphs.get(normalized),
        }
    else:
        payload = graphs
    typer.echo(json.dumps(payload, indent=indent))


@app.command("orient")
def show_orientation(
    state_dir: Optional[Path] = typer.Option(None, help="Root VEI state directory"),
    branch: str = typer.Option("main", help="Branch name"),
    snapshot: Optional[int] = typer.Option(
        None, help="Snapshot index (default: latest)"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Render an agent-facing orientation summary from a stored snapshot."""

    root = _resolve_root(state_dir)
    snap = _resolve_snapshot_payload(root, branch, snapshot)
    state = WorldState.model_validate(snap.get("data", {}))
    payload = build_world_orientation(state).model_dump(mode="json")
    typer.echo(json.dumps(payload, indent=indent))


@app.command("diff")
def diff_snapshots(
    state_dir: Optional[Path] = typer.Option(None, help="Root VEI state directory"),
    branch: str = typer.Option("main", help="Branch name"),
    snapshot_from: int = typer.Option(..., help="Snapshot index to diff from"),
    snapshot_to: int = typer.Option(..., help="Snapshot index to diff to"),
) -> None:
    """Diff two snapshots and print structural changes."""

    root = _resolve_root(state_dir)
    store = StateStore(base_dir=root, branch=branch)
    paths = {p.stem: p for p in _snapshot_paths(store)}
    key_from = f"{snapshot_from:09d}"
    key_to = f"{snapshot_to:09d}"
    if key_from not in paths or key_to not in paths:
        raise typer.BadParameter("Snapshot not found; run 'vei-world list' for indices")
    snap_from = _load_snapshot(paths[key_from])
    snap_to = _load_snapshot(paths[key_to])
    diff = _diff_snapshots(snap_from, snap_to)
    typer.echo(
        json.dumps(
            {"branch": branch, "from": snapshot_from, "to": snapshot_to, "diff": diff},
            indent=2,
        )
    )


@app.command("receipts")
def receipts_tail(
    state_dir: Optional[Path] = typer.Option(None, help="Root VEI state directory"),
    branch: str = typer.Option("main", help="Branch name"),
    tail: int = typer.Option(10, help="Number of receipts to display"),
) -> None:
    """Show the most recent receipts for a branch."""

    root = _resolve_root(state_dir)
    branch_path = _branch_path(root, branch)
    receipts = _load_receipts(branch_path)
    typer.echo(json.dumps({"branch": branch, "receipts": receipts[-tail:]}, indent=2))


if __name__ == "__main__":
    app()
