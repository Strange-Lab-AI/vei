from __future__ import annotations

import typer

from vei.cli.vei_context import app as context_app
from vei.cli.vei_contract import app as contract_app
from vei.cli.vei_export import app as export_app
from vei.cli.vei_inspect import app as inspect_app
from vei.cli.vei_project import app as project_app
from vei.cli.vei_run import app as run_app
from vei.cli.vei_scenario import app as scenario_app
from vei.cli.vei_showcase import app as showcase_app
from vei.cli.vei_studio import app as studio_app
from vei.cli.vei_synthesize import app as synthesize_app
from vei.cli.vei_ui import app as ui_app


app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Unified VEI workspace, run, and UI workflow.",
)

app.add_typer(project_app, name="project")
app.add_typer(context_app, name="context")
app.add_typer(contract_app, name="contract")
app.add_typer(scenario_app, name="scenario")
app.add_typer(run_app, name="run")
app.add_typer(inspect_app, name="inspect")
app.add_typer(showcase_app, name="showcase")
app.add_typer(studio_app, name="studio")
app.add_typer(synthesize_app, name="synthesize")
app.add_typer(export_app, name="export")
app.add_typer(ui_app, name="ui")


if __name__ == "__main__":
    app()
