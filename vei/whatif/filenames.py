from __future__ import annotations

CONTEXT_SNAPSHOT_FILE = "context_snapshot.json"
CANONICAL_EVENTS_FILE = "canonical_events.jsonl"
CANONICAL_EVENT_INDEX_FILE = "canonical_event_index.json"
EPISODE_MANIFEST_FILE = "episode_manifest.json"
PUBLIC_CONTEXT_FILE = "whatif_public_context.json"

EXPERIMENT_RESULT_FILE = "whatif_experiment_result.json"
EXPERIMENT_OVERVIEW_FILE = "whatif_experiment_overview.md"
LLM_RESULT_FILE = "whatif_llm_result.json"
EJEPA_RESULT_FILE = "whatif_ejepa_result.json"
REFERENCE_FORECAST_FILE = "whatif_reference_result.json"
HEURISTIC_FORECAST_FILE = "whatif_heuristic_baseline_result.json"
RANKED_RESULT_FILE = "whatif_ranked_result.json"
RANKED_OVERVIEW_FILE = "whatif_ranked_overview.md"
BUSINESS_STATE_COMPARISON_FILE = "whatif_business_state_comparison.json"
BUSINESS_STATE_COMPARISON_OVERVIEW_FILE = "whatif_business_state_comparison.md"

SCRUBBED_PATH_PLACEHOLDER = "not-included-in-repo-example"
WORKSPACE_DIRECTORY = "workspace"

STUDIO_SAVED_FORECAST_FILES = (
    EJEPA_RESULT_FILE,
    REFERENCE_FORECAST_FILE,
    HEURISTIC_FORECAST_FILE,
)

__all__ = [
    "BUSINESS_STATE_COMPARISON_FILE",
    "BUSINESS_STATE_COMPARISON_OVERVIEW_FILE",
    "CANONICAL_EVENT_INDEX_FILE",
    "CANONICAL_EVENTS_FILE",
    "CONTEXT_SNAPSHOT_FILE",
    "EJEPA_RESULT_FILE",
    "EPISODE_MANIFEST_FILE",
    "EXPERIMENT_OVERVIEW_FILE",
    "EXPERIMENT_RESULT_FILE",
    "HEURISTIC_FORECAST_FILE",
    "LLM_RESULT_FILE",
    "PUBLIC_CONTEXT_FILE",
    "REFERENCE_FORECAST_FILE",
    "RANKED_OVERVIEW_FILE",
    "RANKED_RESULT_FILE",
    "SCRUBBED_PATH_PLACEHOLDER",
    "STUDIO_SAVED_FORECAST_FILES",
    "WORKSPACE_DIRECTORY",
]
