# Agent Onboarding

VEI is one deterministic enterprise simulation engine with several product surfaces built on top of it. The outer layer is the CLI, Studio UI, and twin gateway. Those all feed the same world session, event spine, snapshots, and scoring flow.

## Start here

- Read `README.md` for setup and operator flows.
- Read `docs/ARCHITECTURE.md` for the module map and runtime shape.
- Read `AGENTS.md` for repo policy, eval runners, and validation expectations.
- Read `Makefile` and `.agents.yml` before changing gates or CI behavior.

## Repo shape

- `vei/world/` holds the world-session kernel and replayable state.
- `vei/router/` exposes the MCP tool surface over the kernel.
- `vei/twin/` exposes the HTTP governed twin surface.
- `vei/workspace/` and `vei/run/` hold the file-backed workspace and run model.
- `vei/whatif/` handles branch-point replay, counterfactuals, and saved example bundles.
- `vei/verticals/` holds seeded business packs and overlays.
- `tests/` covers the repo. `tests/dynamics/` now also emits `_vei_out/dynamics_eval/metrics.json`.

## Rules that matter immediately

- Same seed means same world. Treat determinism as part of the product.
- Use `vei/<module>/api.py` for cross-module imports.
- Keep local secrets in `.env` only.
- Do not commit `_vei_out/`, `.artifacts/`, or ad hoc traces.
- Prefer the repo command surface over raw tool invocations.

## Commands

Fast loop:

```bash
make check
make test
```

Full validation:

```bash
make check-full
make test-full
make enron-example
make dynamics-eval
VEI_LLM_LIVE_BYPASS=1 make llm-live
make deps-audit
```

Product orientation:

```bash
vei quickstart run
vei eval benchmark --runner workflow --family security_containment
vei ui serve --root docs/examples/enron-master-agreement-public-context/workspace --host 127.0.0.1 --port 3055
```

## Eval runners

- `workflow` is the reference runner.
- `scripted` is the deterministic floor baseline.
- `bc` is the tool-frequency baseline powered by `FrequencyPolicy`.
- `llm` runs a real model through the MCP world.

## What-if artifact names

The supported forecast filenames are:

- `whatif_ejepa_result.json`
- `whatif_heuristic_baseline_result.json`

Import these constants from `vei.whatif.filenames` or `vei.whatif.api`.

## Hygiene

- `main` is protected; CI must be green to merge.
- Keep branch work on topic.
- After large what-if or eval runs, use `make clean-workspace`.
- When you touch public behavior, update the docs in the same branch.
