Test setup (stdio-first)

- No server to pre-start: stdio tests spawn `python -m vei.router` automatically.
- Live LLM test requires an API key in environment or `.env` (OPENAI_API_KEY).
- `make llm-live` now auto-loads `.env` when present and writes `summary.json` alongside `score.json`, `trace.jsonl`, and `llm_metrics.json`.
- Test and benchmark artifacts belong under `_vei_out/` or `.artifacts/`; they are local outputs, not committed fixtures.

Run tests:

```bash
make test
```

Live LLM test (optional):

```bash
# Ensure .env contains OPENAI_API_KEY (and optional OPENAI_BASE_URL)
make llm-live
```

Targeted slices:

```bash
# Focus on the kernel and benchmark-family surfaces
python -m pytest -q tests/test_world_session.py tests/test_benchmark_api.py tests/test_control_plane_twins.py tests/test_vei_world_cli.py

# Focus on the graph-native planning/mutation surface
python -m pytest -q tests/test_capability_graph_api.py tests/test_capability_graph_actions.py tests/test_mcp_discoverability_tools.py

# Focus on graph-native workflow execution
python -m pytest -q tests/test_workflow_runner.py tests/test_sdk_contract.py

# Focus on the workspace/run/UI product workflow
python -m pytest -q tests/test_workspace_api.py tests/test_run_api.py tests/test_vei_product_cli.py tests/test_ui_api.py

# Focus on grounded import pipeline coverage
python -m pytest -q tests/test_imports_api.py tests/test_workspace_api.py tests/test_run_api.py tests/test_ui_api.py tests/test_vei_product_cli.py

# Keep `make check` green before opening a PR
make check
```
