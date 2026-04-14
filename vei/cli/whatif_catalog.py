from __future__ import annotations

import typer

from vei.whatif import list_objective_packs, list_supported_scenarios

from .whatif_shared import emit_payload


def register_catalog_commands(app: typer.Typer) -> None:
    @app.command("scenarios")
    def list_scenarios_command(
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """List the currently supported what-if scenarios."""

        scenarios = list_supported_scenarios()
        if format == "markdown":
            lines = ["# What-If Scenarios", ""]
            for scenario in scenarios:
                lines.append(f"- `{scenario.scenario_id}`: {scenario.description}")
            typer.echo("\n".join(lines))
            return
        emit_payload(
            [scenario.model_dump(mode="json") for scenario in scenarios],
            format=format,
        )

    @app.command("objectives")
    def list_objectives_command(
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """List ranked what-if objective packs."""

        packs = list_objective_packs()
        if format == "markdown":
            lines = ["# Ranked What-If Objectives", ""]
            for pack in packs:
                lines.append(f"- `{pack.pack_id}`: {pack.summary}")
            typer.echo("\n".join(lines))
            return
        emit_payload([pack.model_dump(mode="json") for pack in packs], format=format)
