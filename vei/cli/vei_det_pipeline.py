from __future__ import annotations

import json
from pathlib import Path

import typer

from vei.corpus.generator import generate_corpus
from vei.corpus.models import CorpusBundle, GeneratedWorkflowSpec
from vei.quality.filter import filter_workflow_corpus
from vei.scenario_engine.api import compile_workflow
from vei.scenario_runner.api import run_workflow

app = typer.Typer(
    add_completion=False,
    help="VEI phases 0-6 workflow/corpus pipeline commands.",
)


@app.command("compile-workflow")
def compile_workflow_cmd(
    spec: Path = typer.Option(
        ..., exists=True, readable=True, help="Workflow spec JSON"
    ),
    seed: int = typer.Option(42042, help="Seed for deterministic world compilation"),
    output: Path = typer.Option(Path("-"), help="Destination path, or '-' for stdout"),
) -> None:
    workflow = compile_workflow(spec, seed=seed)
    payload = {
        "name": workflow.spec.name,
        "steps": [step.step_id for step in workflow.steps],
        "metadata": workflow.scenario.metadata or {},
        "scenario": workflow.scenario.__dict__,
    }
    text = json.dumps(payload, indent=2)
    if output == Path("-"):
        typer.echo(text)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text, encoding="utf-8")
    typer.echo(f"compiled workflow written to {output}")


@app.command("run-workflow")
def run_workflow_cmd(
    spec: Path = typer.Option(
        ..., exists=True, readable=True, help="Workflow spec JSON"
    ),
    seed: int = typer.Option(42042, help="Seed for deterministic execution"),
    connector_mode: str = typer.Option(
        "sim", help="Connector execution mode: sim|replay|live"
    ),
    artifacts: Path = typer.Option(
        Path("_vei_out/workflow_run"), help="Artifacts output directory"
    ),
    output: Path = typer.Option(
        Path("_vei_out/workflow_run/result.json"), help="Result JSON path"
    ),
) -> None:
    workflow = compile_workflow(spec, seed=seed)
    result = run_workflow(
        workflow,
        seed=seed,
        artifacts_dir=str(artifacts),
        connector_mode=connector_mode,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result.model_dump(), indent=2), encoding="utf-8")
    typer.echo(
        f"workflow={result.workflow_name} ok={result.ok} "
        f"steps={len(result.steps)} output={output}"
    )


@app.command("generate-corpus")
def generate_corpus_cmd(
    seed: int = typer.Option(42042, help="Seed for deterministic generation"),
    environments: int = typer.Option(25, help="Number of enterprise environments"),
    scenarios_per_environment: int = typer.Option(
        20, help="Number of workflow scenarios per environment"
    ),
    output: Path = typer.Option(
        Path("_vei_out/corpus/generated_corpus.json"),
        help="Output JSON path for generated corpus bundle",
    ),
) -> None:
    bundle = generate_corpus(
        seed=seed,
        environment_count=environments,
        scenarios_per_environment=scenarios_per_environment,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(bundle.model_dump(), indent=2), encoding="utf-8")
    typer.echo(
        f"generated environments={len(bundle.environments)} "
        f"workflows={len(bundle.workflows)} -> {output}"
    )


@app.command("filter-corpus")
def filter_corpus_cmd(
    corpus: Path = typer.Option(..., exists=True, readable=True, help="Corpus JSON"),
    realism_threshold: float = typer.Option(
        0.55, help="Minimum realism score for acceptance"
    ),
    output: Path = typer.Option(
        Path("_vei_out/corpus/filter_report.json"),
        help="Output JSON report path",
    ),
) -> None:
    data = json.loads(corpus.read_text(encoding="utf-8"))
    bundle = CorpusBundle.model_validate(data)
    workflows = [
        GeneratedWorkflowSpec.model_validate(item.model_dump())
        for item in bundle.workflows
    ]
    report = filter_workflow_corpus(
        workflows,
        realism_threshold=realism_threshold,
    )
    payload = report.model_dump()
    payload["summary"] = {
        "accepted": len(report.accepted),
        "rejected": len(report.rejected),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    typer.echo(
        f"quality filter accepted={len(report.accepted)} "
        f"rejected={len(report.rejected)} -> {output}"
    )


@app.command("sample-workflow")
def sample_workflow_cmd(
    output: Path = typer.Option(
        Path("_vei_out/workflow_sample.json"),
        help="Destination for sample workflow DSL v2",
    ),
) -> None:
    sample = {
        "name": "sample-procurement-workflow",
        "objective": {
            "statement": "Collect quote and route approval for laptop purchase.",
            "success": [
                "quote requested over email",
                "approval request posted to slack",
                "ticket updated",
            ],
        },
        "world": {"catalog": "multi_channel"},
        "actors": [
            {"actor_id": "agent", "role": "procurement_operator"},
            {"actor_id": "approver", "role": "finance_manager"},
        ],
        "constraints": [
            {
                "name": "budget",
                "description": "Approval must include amount",
                "required": True,
            }
        ],
        "approvals": [{"stage": "finance", "approver": "approver", "required": True}],
        "steps": [
            {
                "step_id": "read_store",
                "description": "Read browser context",
                "tool": "browser.read",
                "args": {},
            },
            {
                "step_id": "send_quote_request",
                "description": "Email vendor for quote",
                "tool": "mail.compose",
                "args": {
                    "to": "sales@macrocompute.example",
                    "subj": "Quote request",
                    "body_text": "Please share latest quote with ETA.",
                },
                "expect": [{"kind": "result_contains", "field": "id", "contains": "m"}],
            },
            {
                "step_id": "post_approval",
                "description": "Post budget approval request",
                "tool": "slack.send_message",
                "args": {
                    "channel": "#procurement",
                    "text": "Please approve purchase budget $3200 with cited quote.",
                },
                "expect": [{"kind": "result_contains", "field": "ts", "contains": ""}],
            },
        ],
        "success_assertions": [
            {"kind": "pending_max", "field": "total", "max_value": 10}
        ],
        "tags": ["sample", "dsl-v2"],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(sample, indent=2), encoding="utf-8")
    typer.echo(f"sample workflow written to {output}")


if __name__ == "__main__":
    app()
