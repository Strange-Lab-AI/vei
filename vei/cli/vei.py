from __future__ import annotations

import typer

from vei.cli.vei_blueprint import app as blueprint_app
from vei.cli.vei_context import app as context_app
from vei.cli.vei_contract import app as contract_app
from vei.cli.vei_demo import app as demo_app
from vei.cli.vei_det_pipeline import app as det_app
from vei.cli.vei_eval import app as eval_app
from vei.cli.vei_inspect import app as inspect_app
from vei.cli.vei_llm_test import app as llm_test_app
from vei.cli.vei_project import app as project_app
from vei.cli.vei_quickstart import app as quickstart_app
from vei.cli.vei_report import app as report_app
from vei.cli.vei_run import app as run_app
from vei.cli.vei_smoke import app as smoke_app
from vei.cli.vei_synthesize import app as synthesize_app
from vei.cli.vei_twin import app as twin_app
from vei.cli.vei_ui import app as ui_app
from vei.cli.vei_visualize import app as visualize_app
from vei.cli.vei_world import app as world_app

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="VEI — programmable enterprise simulation, context capture, and synthesis.",
)

app.add_typer(project_app, name="project")
app.add_typer(quickstart_app, name="quickstart")
app.add_typer(run_app, name="run")
app.add_typer(eval_app, name="eval")
app.add_typer(inspect_app, name="inspect")
app.add_typer(twin_app, name="twin")
app.add_typer(ui_app, name="ui")
app.add_typer(world_app, name="world")
app.add_typer(blueprint_app, name="blueprint")
app.add_typer(context_app, name="context")
app.add_typer(contract_app, name="contract")
app.add_typer(demo_app, name="demo")
app.add_typer(det_app, name="det")
app.add_typer(llm_test_app, name="llm-test")
app.add_typer(report_app, name="report")
app.add_typer(smoke_app, name="smoke")
app.add_typer(synthesize_app, name="synthesize")
app.add_typer(visualize_app, name="visualize")

from vei.cli.vei_showcase import app as showcase_app
app.add_typer(showcase_app, name="showcase")


if __name__ == "__main__":
    app()
