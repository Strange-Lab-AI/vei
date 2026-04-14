from __future__ import annotations

from importlib import import_module
from pathlib import Path

import typer

from .whatif_shared import (
    emit_payload,
    fail_if_artifact_validation_failed,
    reject_workspace_seed_snapshot,
    time_window,
)


def _whatif_api():
    return import_module("vei.whatif.api")


def _whatif_render():
    return import_module("vei.whatif.render")


def _whatif_validation():
    return import_module("vei.whatif.artifact_validation")


def register_episode_commands(app: typer.Typer) -> None:
    @app.command("explore")
    def explore_command(
        source: str = typer.Option(
            "auto",
            help="What-if source: auto | enron | mail_archive | company_history",
        ),
        source_dir: Path = typer.Option(
            ...,
            "--source-dir",
            "--rosetta-dir",
            help="Historical source directory or file",
        ),
        scenario: str | None = typer.Option(None, help="Supported scenario id"),
        prompt: str | None = typer.Option(None, help="Plain-English question"),
        date_from: str | None = typer.Option(None, help="Optional ISO start timestamp"),
        date_to: str | None = typer.Option(None, help="Optional ISO end timestamp"),
        custodian: list[str] | None = typer.Option(
            None,
            help="Optional custodian filters",
        ),
        max_events: int | None = typer.Option(None, help="Optional event cap"),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Run deterministic what-if analysis over the source history."""

        reject_workspace_seed_snapshot(source_dir)
        api = _whatif_api()
        render = _whatif_render()
        world = api.load_world(
            source=source,
            source_dir=source_dir,
            time_window=time_window(date_from, date_to),
            custodian_filter=custodian or [],
            max_events=max_events,
        )
        if scenario is None and prompt is None:
            payload = (
                render.render_world_summary(world)
                if format == "markdown"
                else world.summary.model_dump(mode="json")
            )
            emit_payload(payload, format=format)
            return

        result = api.run_whatif(world, scenario=scenario, prompt=prompt)
        payload = (
            render.render_result(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @app.command("candidates")
    def candidates_command(
        source: str = typer.Option(
            "auto",
            help="What-if source: auto | enron | mail_archive | company_history",
        ),
        source_dir: Path = typer.Option(
            ...,
            "--source-dir",
            "--rosetta-dir",
            help="Historical source directory or file",
        ),
        limit: int = typer.Option(10, help="Maximum branch candidates to return"),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Rank strong branch points from one company-history bundle."""

        reject_workspace_seed_snapshot(source_dir)
        api = _whatif_api()
        render = _whatif_render()
        world = api.load_world(source=source, source_dir=source_dir)
        result = api.list_branch_candidates(world, limit=limit)
        payload = (
            render.render_branch_candidates(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @app.command("open")
    def open_command(
        source: str = typer.Option(
            "auto",
            help="What-if source: auto | enron | mail_archive | company_history",
        ),
        source_dir: Path = typer.Option(
            ...,
            "--source-dir",
            "--rosetta-dir",
            help="Historical source directory or file",
        ),
        root: Path = typer.Option(
            ...,
            help="Workspace root for the replayable episode",
        ),
        thread_id: str | None = typer.Option(None, help="Thread to materialize"),
        event_id: str | None = typer.Option(
            None, help="Optional branch event override"
        ),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Build a strict historical workspace from one event or thread."""

        api = _whatif_api()
        render = _whatif_render()
        validation = _whatif_validation()
        world = api.load_world(source=source, source_dir=source_dir)
        materialization = api.materialize_episode(
            world,
            root=root,
            thread_id=thread_id,
            event_id=event_id,
        )
        fail_if_artifact_validation_failed(
            issues=validation.validate_saved_workspace(root),
            label="workspace artifacts",
        )
        payload = (
            render.render_episode(materialization)
            if format == "markdown"
            else materialization.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @app.command("scene")
    def scene_command(
        source: str = typer.Option(
            "auto",
            help="What-if source: auto | enron | mail_archive | company_history",
        ),
        source_dir: Path | None = typer.Option(
            None,
            "--source-dir",
            "--rosetta-dir",
            help="Historical source directory or file",
        ),
        thread_id: str | None = typer.Option(
            None, help="Thread to build the scene from"
        ),
        event_id: str | None = typer.Option(
            None, help="Optional branch event override"
        ),
        workspace_root: Path | None = typer.Option(
            None,
            "--workspace-root",
            help="Saved episode workspace root (uses saved context instead of loading a world)",
        ),
        format: str = typer.Option("markdown", help="Output format: json | markdown"),
    ) -> None:
        """Build a decision scene for a branch point from a world or a saved workspace."""

        api = _whatif_api()
        render = _whatif_render()
        if workspace_root is not None:
            scene = api.build_saved_decision_scene(workspace_root)
        else:
            if source_dir is None:
                raise typer.BadParameter(
                    "Provide --source-dir (or --workspace-root for saved episodes)"
                )
            world = api.load_world(source=source, source_dir=source_dir)
            scene = api.build_decision_scene(
                world,
                thread_id=thread_id,
                event_id=event_id,
            )
        payload = (
            render.render_decision_scene(scene)
            if format == "markdown"
            else scene.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @app.command("events")
    def events_command(
        source: str = typer.Option(
            "auto",
            help="What-if source: auto | enron | mail_archive | company_history",
        ),
        source_dir: Path = typer.Option(
            ...,
            "--source-dir",
            "--rosetta-dir",
            help="Historical source directory or file",
        ),
        actor: str | None = typer.Option(None, help="Filter by sender email fragment"),
        participant: str | None = typer.Option(
            None,
            help="Filter by any participant or recipient email fragment",
        ),
        thread_id: str | None = typer.Option(None, help="Filter by thread id"),
        event_type: str | None = typer.Option(None, help="Filter by event type"),
        query: str | None = typer.Option(
            None,
            help="Match against event id, subject, actors, and recipients",
        ),
        flagged_only: bool = typer.Option(
            False,
            help="Only return events with policy-relevant flags",
        ),
        max_events: int | None = typer.Option(None, help="Optional event cap"),
        limit: int = typer.Option(20, help="Maximum number of events to return"),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Search historical events so you can branch from an exact point in time."""

        api = _whatif_api()
        render = _whatif_render()
        world = api.load_world(
            source=source,
            source_dir=source_dir,
            max_events=max_events,
        )
        result = api.search_events(
            world,
            actor=actor,
            participant=participant,
            thread_id=thread_id,
            event_type=event_type,
            query=query,
            flagged_only=flagged_only,
            limit=limit,
        )
        payload = (
            render.render_event_search(result)
            if format == "markdown"
            else result.model_dump(mode="json")
        )
        emit_payload(payload, format=format)

    @app.command("replay")
    def replay_command(
        root: Path = typer.Option(..., help="Workspace root from `vei whatif open`"),
        tick_ms: int = typer.Option(
            0,
            help="Optional logical time to advance after scheduling the baseline future",
        ),
        seed: int = typer.Option(42042, help="Deterministic replay seed"),
        format: str = typer.Option("json", help="Output format: json | markdown"),
    ) -> None:
        """Schedule the saved historical future into the world sim."""

        api = _whatif_api()
        render = _whatif_render()
        summary = api.replay_episode_baseline(root, tick_ms=tick_ms, seed=seed)
        payload = (
            render.render_replay(summary)
            if format == "markdown"
            else summary.model_dump(mode="json")
        )
        emit_payload(payload, format=format)
