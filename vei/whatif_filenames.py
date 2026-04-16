from __future__ import annotations

CONTEXT_SNAPSHOT_FILE = "context_snapshot.json"
EPISODE_MANIFEST_FILE = "episode_manifest.json"
PUBLIC_CONTEXT_FILE = "whatif_public_context.json"

EXPERIMENT_RESULT_FILE = "whatif_experiment_result.json"
EXPERIMENT_OVERVIEW_FILE = "whatif_experiment_overview.md"
LLM_RESULT_FILE = "whatif_llm_result.json"
EJEPA_RESULT_FILE = "whatif_ejepa_result.json"
EJEPA_PROXY_RESULT_FILE = "whatif_ejepa_proxy_result.json"
BUSINESS_STATE_COMPARISON_FILE = "whatif_business_state_comparison.json"
BUSINESS_STATE_COMPARISON_OVERVIEW_FILE = "whatif_business_state_comparison.md"

SCRUBBED_PATH_PLACEHOLDER = "not-included-in-repo-example"
WORKSPACE_DIRECTORY = "workspace"

STUDIO_SAVED_FORECAST_FILES = (
    EJEPA_RESULT_FILE,
    EJEPA_PROXY_RESULT_FILE,
)

__all__ = [
    "BUSINESS_STATE_COMPARISON_FILE",
    "BUSINESS_STATE_COMPARISON_OVERVIEW_FILE",
    "CONTEXT_SNAPSHOT_FILE",
    "EJEPA_PROXY_RESULT_FILE",
    "EJEPA_RESULT_FILE",
    "EPISODE_MANIFEST_FILE",
    "EXPERIMENT_OVERVIEW_FILE",
    "EXPERIMENT_RESULT_FILE",
    "LLM_RESULT_FILE",
    "PUBLIC_CONTEXT_FILE",
    "SCRUBBED_PATH_PLACEHOLDER",
    "STUDIO_SAVED_FORECAST_FILES",
    "WORKSPACE_DIRECTORY",
]
