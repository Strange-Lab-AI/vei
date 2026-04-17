from __future__ import annotations

import typer

from vei.cli._lazy import LazyCommandSpec, LazyTyperGroup


class VEILazyGroup(LazyTyperGroup):
    lazy_commands = {
        "project": LazyCommandSpec(
            module_path="vei.cli.vei_project",
            help="Manage workspace imports, sources, and project scaffolding.",
        ),
        "doctor": LazyCommandSpec(
            module_path="vei.cli.vei_doctor",
            help="Inspect local setup and surface common workspace issues.",
        ),
        "quickstart": LazyCommandSpec(
            module_path="vei.cli.vei_quickstart",
            help="Launch guided local demos and twin-backed workspaces.",
        ),
        "release": LazyCommandSpec(
            module_path="vei.cli.vei_release",
            help="Build release artifacts and nightly snapshots.",
        ),
        "run": LazyCommandSpec(
            module_path="vei.cli.vei_run",
            help="Launch and inspect workspace runs.",
        ),
        "eval": LazyCommandSpec(
            module_path="vei.cli.vei_eval",
            help="Run benchmark demos, suites, and comparison flows.",
        ),
        "inspect": LazyCommandSpec(
            module_path="vei.cli.vei_inspect",
            help="Inspect fidelity, orientation, and workspace state.",
        ),
        "twin": LazyCommandSpec(
            module_path="vei.cli.vei_twin",
            help="Build and serve customer twin environments.",
        ),
        "ui": LazyCommandSpec(
            module_path="vei.cli.vei_ui",
            help="Serve the local FastAPI studio surfaces.",
        ),
        "whatif": LazyCommandSpec(
            module_path="vei.cli.vei_whatif",
            help="Explore counterfactuals and replayable what-if episodes.",
        ),
        "world": LazyCommandSpec(
            module_path="vei.cli.vei_world",
            help="Inspect world catalogs and blueprint-backed sessions.",
        ),
        "blueprint": LazyCommandSpec(
            module_path="vei.cli.vei_blueprint",
            help="Inspect, generate, and scaffold blueprint assets.",
        ),
        "context": LazyCommandSpec(
            module_path="vei.cli.vei_context",
            help="Capture and inspect context bundles.",
        ),
        "knowledge": LazyCommandSpec(
            module_path="vei.cli.vei_knowledge",
            help="Capture knowledge assets and compose grounded business artifacts.",
        ),
        "contract": LazyCommandSpec(
            module_path="vei.cli.vei_contract",
            help="Inspect and validate workspace contracts.",
        ),
        "demo": LazyCommandSpec(
            module_path="vei.cli.vei_demo",
            help="Run lightweight scripted or LLM-driven demos.",
        ),
        "det": LazyCommandSpec(
            module_path="vei.cli.vei_det_pipeline",
            help="Run the deterministic data and evaluation pipeline.",
        ),
        "llm-test": LazyCommandSpec(
            module_path="vei.cli.vei_llm_test",
            help="Run the live LLM harness against the MCP world.",
        ),
        "report": LazyCommandSpec(
            module_path="vei.cli.vei_report",
            help="Render run and benchmark reports.",
        ),
        "smoke": LazyCommandSpec(
            module_path="vei.cli.vei_smoke",
            help="Run transport and harness smoke checks.",
        ),
        "showcase": LazyCommandSpec(
            module_path="vei.cli.vei_showcase",
            help="Run curated showcase scenarios.",
        ),
        "synthesize": LazyCommandSpec(
            module_path="vei.cli.vei_synthesize",
            help="Generate synthesis configs and runbooks.",
        ),
        "visualize": LazyCommandSpec(
            module_path="vei.cli.vei_visualize",
            help="Render visualization artifacts from runs and traces.",
        ),
    }


app = typer.Typer(
    add_completion=False,
    cls=VEILazyGroup,
    no_args_is_help=True,
    help="VEI — programmable enterprise simulation, context capture, and synthesis.",
)


@app.callback()
def main() -> None:
    """Expose the lazy top-level VEI command group."""
    return None


if __name__ == "__main__":
    app()
