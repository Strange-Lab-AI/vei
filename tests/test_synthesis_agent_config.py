from __future__ import annotations

from pathlib import Path


from vei.synthesis.api import synthesize_agent_config
from vei.workspace.api import create_workspace_from_template
from vei.run.api import launch_workspace_run


def test_agent_config_from_run(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    manifest = launch_workspace_run(root, runner="workflow")

    config = synthesize_agent_config(root, manifest.run_id)

    assert config.system_prompt
    assert len(config.system_prompt) > 20
    assert isinstance(config.tools, list)
    assert isinstance(config.guardrails, list)
    assert isinstance(config.success_criteria, list)
    assert manifest.run_id in config.metadata.get("run_id", "")
