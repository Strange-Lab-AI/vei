from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, List

import typer

from vei.context.api import capture_context, hydrate_blueprint
from vei.context.models import ContextProviderConfig
from vei.knowledge.api import (
    KnowledgeComposeRequest,
    compose_artifact,
    empty_store,
    store_from_payload,
    utc_now_ms,
)
from vei.project_settings import default_model_for_provider
from vei.workspace.api import (
    compile_workspace,
    load_workspace,
    resolve_workspace_scenario,
)

app = typer.Typer(add_completion=False)

_WORKSPACE_KNOWLEDGE_DIR = ".artifacts/knowledge"
_WORKSPACE_KNOWLEDGE_SNAPSHOT = "knowledge_snapshot.json"


@app.command("ingest")
def ingest(
    provider: List[str] = typer.Option(
        ...,
        "--provider",
        "-p",
        help="Knowledge provider name (notion, linear, granola). Repeat for each provider.",
    ),
    source: List[str] = typer.Option(
        [],
        "--source",
        help="Provider export path mapping in the form provider=/path/to/export.",
    ),
    org: str = typer.Option("", "--org", help="Organization name override."),
    domain: str = typer.Option("", "--domain", help="Organization domain override."),
    workspace: str = typer.Option(
        "",
        "--workspace",
        help="Workspace root. When set, the snapshot is written into workspace artifacts.",
    ),
    output: str = typer.Option(
        "",
        "--output",
        "-o",
        help="Output knowledge snapshot path. Defaults to the workspace knowledge snapshot when --workspace is set.",
    ),
) -> None:
    """Capture offline-first knowledge exports into a deterministic knowledge snapshot."""
    workspace_root = _resolve_workspace_root(workspace)
    source_map = _parse_source_map(source)
    organization_name, organization_domain = _resolve_org(
        workspace_root,
        org=org,
        domain=domain,
    )
    configs = [
        ContextProviderConfig(
            provider=_validated_provider(name),
            base_url=source_map.get(name.strip().lower()),
        )
        for name in provider
    ]
    snapshot = capture_context(
        configs,
        organization_name=organization_name,
        organization_domain=organization_domain,
    )
    blueprint = hydrate_blueprint(
        snapshot,
        scenario_name="knowledge_capture",
        workflow_name="knowledge_capture",
    )
    graph = (
        blueprint.capability_graphs.knowledge_graph
        if blueprint.capability_graphs is not None
        else None
    )
    store = (
        store_from_payload(graph.model_dump(mode="json"))
        if graph is not None
        else empty_store()
    )
    destination = _resolve_ingest_output(
        workspace_root,
        output=output,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(store.model_dump_json(indent=2), encoding="utf-8")
    typer.echo(
        f"Wrote {len(store.assets)} knowledge assets and {len(store.edges)} links -> {destination}"
    )


@app.command("compose")
def compose(
    workspace: str = typer.Option(
        "",
        "--workspace",
        help="Workspace root to load compiled knowledge state from.",
    ),
    snapshot: str = typer.Option(
        "",
        "--snapshot",
        help="Standalone or overlay knowledge snapshot JSON path.",
    ),
    scenario_name: str = typer.Option(
        "",
        "--scenario",
        help="Workspace scenario name. Defaults to the active/default scenario.",
    ),
    target: str = typer.Option(
        "proposal",
        "--target",
        help="Artifact target: proposal, brief, or weekly_review.",
    ),
    template: str = typer.Option(
        "proposal_v1",
        "--template",
        help="Composition template id.",
    ),
    subject: str = typer.Option(
        ...,
        "--subject",
        help="Primary object ref, such as crm_deal:CRM-NSG-D1.",
    ),
    scope_ref: List[str] = typer.Option(
        [],
        "--scope-ref",
        help="Additional retrieval scope refs.",
    ),
    tag: List[str] = typer.Option([], "--tag", help="Required asset tags."),
    prompt: str = typer.Option("", "--prompt", help="Optional composition prompt."),
    mode: str = typer.Option(
        "heuristic_baseline",
        "--mode",
        help="Composition mode: heuristic_baseline or llm.",
    ),
    provider: str = typer.Option(
        "openai",
        "--provider",
        help="LLM provider when --mode llm is selected.",
    ),
    model: str = typer.Option("", "--model", help="Model override."),
    output: str = typer.Option(
        "",
        "--output",
        "-o",
        help="Composition result path. Defaults to a workspace artifact path when --workspace is set, otherwise stdout.",
    ),
    write_back: bool = typer.Option(
        True,
        "--write-back/--no-write-back",
        help="Persist the updated knowledge snapshot under the workspace knowledge artifacts directory.",
    ),
) -> None:
    """Compose a grounded artifact from workspace or snapshot knowledge state."""
    workspace_root = _resolve_workspace_root(workspace)
    overlay_path = Path(snapshot).expanduser().resolve() if snapshot else None
    store = _load_compose_store(
        workspace_root,
        scenario_name=scenario_name or None,
        snapshot_path=overlay_path,
    )
    result = compose_artifact(
        store,
        KnowledgeComposeRequest(
            target=target,  # type: ignore[arg-type]
            template_id=template,
            subject_object_ref=subject,
            scope_refs=list(scope_ref),
            tags=list(tag),
            prompt=prompt,
            mode=mode,  # type: ignore[arg-type]
            provider=provider,
            model=model or default_model_for_provider(provider),
        ),
        now_ms=utc_now_ms(),
    )
    payload = result.model_dump(mode="json")
    destination = _resolve_compose_output(
        workspace_root,
        output=output,
        subject=subject,
        target=target,
    )
    if destination is None:
        typer.echo(json.dumps(payload, indent=2))
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        typer.echo(f"Wrote composed artifact -> {destination}")

    if workspace_root is not None and write_back:
        snapshot_path = _workspace_knowledge_snapshot_path(workspace_root)
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(store.model_dump_json(indent=2), encoding="utf-8")
        typer.echo(
            f"Updated workspace knowledge snapshot -> {snapshot_path}",
            err=(destination is None),
        )


def _resolve_workspace_root(raw: str) -> Path | None:
    if not raw.strip():
        return None
    return Path(raw).expanduser().resolve()


def _validated_provider(raw: str) -> str:
    name = raw.strip().lower()
    allowed = {"notion", "linear", "granola"}
    if name not in allowed:
        raise typer.BadParameter(
            f"provider must be one of: {', '.join(sorted(allowed))}"
        )
    return name


def _parse_source_map(values: Iterable[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for raw in values:
        if "=" not in raw:
            raise typer.BadParameter(
                "each --source must look like provider=/path/to/export"
            )
        provider, path = raw.split("=", 1)
        key = provider.strip().lower()
        resolved = Path(path).expanduser().resolve()
        if not resolved.exists():
            raise typer.BadParameter(
                f"source path does not exist for {key}: {resolved}"
            )
        parsed[key] = str(resolved)
    return parsed


def _resolve_org(
    workspace_root: Path | None,
    *,
    org: str,
    domain: str,
) -> tuple[str, str]:
    if workspace_root is None:
        if not org.strip():
            raise typer.BadParameter("--org is required when --workspace is not set")
        return org.strip(), domain.strip()
    manifest = load_workspace(workspace_root)
    organization_name = org.strip() or manifest.title or manifest.name
    organization_domain = domain.strip()
    return organization_name, organization_domain


def _resolve_ingest_output(
    workspace_root: Path | None,
    *,
    output: str,
) -> Path:
    if output.strip():
        return Path(output).expanduser().resolve()
    if workspace_root is not None:
        return _workspace_knowledge_snapshot_path(workspace_root)
    return Path("knowledge_snapshot.json").expanduser().resolve()


def _resolve_compose_output(
    workspace_root: Path | None,
    *,
    output: str,
    subject: str,
    target: str,
) -> Path | None:
    if output.strip() == "-":
        return None
    if output.strip():
        return Path(output).expanduser().resolve()
    if workspace_root is None:
        return None
    slug = _slug(f"{subject}-{target}")
    return workspace_root / _WORKSPACE_KNOWLEDGE_DIR / f"{slug}.compose.json"


def _workspace_knowledge_snapshot_path(workspace_root: Path) -> Path:
    return workspace_root / _WORKSPACE_KNOWLEDGE_DIR / _WORKSPACE_KNOWLEDGE_SNAPSHOT


def _load_compose_store(
    workspace_root: Path | None,
    *,
    scenario_name: str | None,
    snapshot_path: Path | None,
) -> Any:
    store = empty_store()
    if workspace_root is not None:
        compile_workspace(workspace_root)
        manifest = load_workspace(workspace_root)
        scenario = resolve_workspace_scenario(workspace_root, manifest, scenario_name)
        scenario_seed_path = (
            workspace_root
            / manifest.compiled_root
            / scenario.name
            / "scenario_seed.json"
        )
        payload = json.loads(scenario_seed_path.read_text(encoding="utf-8"))
        store = store_from_payload(payload.get("knowledge_graph"))
        workspace_snapshot = _workspace_knowledge_snapshot_path(workspace_root)
        if workspace_snapshot.exists():
            store = _merge_store(
                store, store_from_payload(_read_json(workspace_snapshot))
            )
    if snapshot_path is not None:
        store = _merge_store(store, store_from_payload(_read_json(snapshot_path)))
    return store


def _merge_store(base: Any, overlay: Any) -> Any:
    merged = base.model_copy(deep=True)
    for asset_id, asset in overlay.assets.items():
        merged.assets[asset_id] = asset.model_copy(deep=True)
    merged.edges.extend(edge.model_copy(deep=True) for edge in overlay.edges)
    merged.events.extend(dict(event) for event in overlay.events)
    merged.asset_seq = max(int(merged.asset_seq), int(overlay.asset_seq))
    merged.edge_seq = max(int(merged.edge_seq), int(overlay.edge_seq))
    merged.metadata = {**dict(merged.metadata), **dict(overlay.metadata)}
    return merged


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _slug(value: str) -> str:
    cleaned = [
        char.lower() if char.isalnum() else "-" for char in value.strip().lower()
    ]
    slug = "".join(cleaned).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or "knowledge"
