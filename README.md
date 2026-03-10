## Digital Enterprise Twin (VEI)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/strangeloopcanon/digital-enterprise-twin)

![VEI Virtual Office](docs/assets/virtual_office.gif)
<p align="center"><sub>Conceptual office view</sub></p>

VEI is a deterministic, MCP-native enterprise simulator for training, evaluating, and replaying agent behavior against synthetic business systems. The stable boundary is the world kernel: a `WorldSession` owns world state, event queues, snapshots, branches, replay, injection, actor state, and receipts.

## Core Primitives

VEI now exposes one coherent product shape:

- `Blueprint`: typed composition of scenario, facades, workflow, and contract
- `Scenario`: seeded enterprise world and difficulty/tool manifest
- `Facade`: typed enterprise surface grouped by capability domain
- `Contract`: success predicates, forbidden predicates, observation boundary, policy invariants, reward terms, and intervention rules
- `Run`: workflow, benchmark, demo, and suite executions over the same world kernel
- `Snapshot`: branchable world-state checkpoint with replay and receipts

The older per-app router twins are still used, but they are now wrapped as a typed facade catalog rather than presented as the product ontology by themselves.

## License

This repository is licensed under the Business Source License 1.1 in [LICENSE](LICENSE).

- Additional Use Grant: `None`
- Change Date: `2030-03-10`
- Change License: `GPL-2.0-or-later`

## Quick Start

### Install

```bash
pip install -e ".[llm,sse]"
```

### Configure `.env`

```env
OPENAI_API_KEY=sk-your-key
VEI_SEED=42042
VEI_ARTIFACTS_DIR=./_vei_out
```

### Verify the repo

```bash
make setup
make check
make test
vei-smoke --transport stdio --timeout-s 30
```

### Run a live episode

```bash
vei-llm-test \
  --provider openai \
  --model gpt-5 \
  --task "Research price, get Slack approval under budget, and email vendor for quote."
```

## What You Get

- Deterministic simulator with replayable traces
- Stable world-kernel API with snapshot, branch, restore, replay, inject, and event inspection
- Typed blueprint and facade catalog over the existing enterprise twins
- Enterprise twins for Slack, Mail, Browser, Docs, Tickets, DB, ERP/CRM, Okta-style identity, ServiceDesk, Google Admin, SIEM, Datadog, PagerDuty, feature flags, HRIS, and Jira-style issue flows
- Scenario compilation, dataset rollout, BC training, benchmark execution, and release packaging
- Reusable benchmark families for security containment, enterprise onboarding/migration, and revenue incident response

## Architecture

```text
Agent ──MCP──► VEI Router
                  └─ transport + tool dispatch
                            │
                            ▼
                      WorldSession Kernel
                  ├─ unified world state
                  ├─ snapshots / branch / replay / inject
                  ├─ actor state + receipts
                  └─ enterprise twins and control planes
```

## Use It As A Library

Install directly from GitHub:

```bash
pip install "git+https://github.com/strangeloopcanon/digital-enterprise-twin.git@main"
```

SDK embedding:

```python
from vei.sdk import create_session

session = create_session(seed=42042, scenario_name="multi_channel")
obs = session.observe()
page = session.call_tool("browser.read", {})
```

World-kernel embedding:

```python
from vei.world.api import create_world_session

world = create_world_session(seed=42042, scenario_name="multi_channel")
obs = world.observe()
snapshot = world.snapshot("before-run")
events = world.list_events()
```

Useful helpers:

- Scenario manifests: `list_scenario_manifest()`, `get_scenario_manifest(name)`
- Facade catalog: `list_facade_manifest_entries()`, `get_facade_manifest_entry(name)`
- Blueprint catalog: `list_blueprint_entries()`, `build_blueprint_for_family_entry(name)`, `build_blueprint_for_scenario_entry(name)`
- Benchmark families: `list_benchmark_family_manifest_entries()`, `get_benchmark_family_manifest_entry(name)`
- Release packaging: `build_release_version()`, `export_release_dataset(...)`, `export_release_benchmark(...)`, `run_release_nightly(...)`

## Primary Commands

```bash
make setup
make check
make test
make llm-live
make deps-audit
make all
```

If you do not have LLM credentials:

```bash
VEI_LLM_LIVE_BYPASS=1 make llm-live
```

## Supported CLI Surface

- Runtime: `vei-llm-test`, `vei-smoke`, `vei-demo`, `vei-world`
- Ontology: `vei-blueprint`
- Release/Ops: `vei-release dataset|benchmark|nightly`
- Scenarios: `vei-scenarios list|manifest|dump`
- DSL/corpus: `vei-det sample-workflow|compile-workflow|run-workflow|generate-corpus|filter-corpus`
- Policy/eval: `vei-rollout`, `vei-train`, `vei-eval`, `vei-eval-frontier`, `vei-score`
- Visualization: `vei-visualize replay|flow|dashboard|export`

## Benchmarking

Baseline run:

```bash
export VEI_ARTIFACTS_DIR=_vei_out/llmtest
VEI_SEED=42042 vei-llm-test \
  --provider openai \
  --model gpt-5 \
  --max-steps 32 \
  --task "Open product page, cite specs, post approval under $3200, email sales@macrocompute.example for a quote, wait for reply."
vei-score --artifacts-dir _vei_out/llmtest --success-mode full
```

Kernel-backed benchmark run:

```bash
vei-eval benchmark \
  --runner scripted \
  --scenario multi_channel \
  --artifacts-root _vei_out/benchmark \
  --run-id scripted_multi
```

Family-level benchmark run:

```bash
vei-eval benchmark \
  --runner workflow \
  --family security_containment \
  --artifacts-root _vei_out/benchmark \
  --run-id security_workflow
```

Explicit workflow selection for a single scenario:

```bash
vei-eval benchmark \
  --runner workflow \
  --scenario oauth_app_containment \
  --workflow-name security_containment \
  --workflow-variant internal_only_review \
  --artifacts-root _vei_out/benchmark \
  --run-id security_named_workflow
```

Scripted or LLM family runs stay on the same pipeline:

```bash
vei-eval benchmark \
  --runner scripted \
  --family security_containment \
  --artifacts-root _vei_out/benchmark \
  --run-id security_family
```

Canonical family demo flow:

```bash
vei-eval demo \
  --family security_containment \
  --artifacts-root _vei_out/demo \
  --run-id security_demo
```

That command runs the deterministic family workflow baseline plus a comparison runner, writes `leaderboard.md` / `leaderboard.csv` / `leaderboard.json`, stores inspectable world state under `_vei_out/demo/security_demo/state` for follow-up `vei-world` inspection, and records explicit `contract.json` artifacts for both the baseline and comparison paths. Contract evaluation now separates oracle state from agent-visible observation so hidden state can be graded without making the demo omniscient.

Canonical multi-family workflow suite:

```bash
vei-eval suite \
  --artifacts-root _vei_out/suite \
  --run-id nightly_suite
```

That command runs each family's primary workflow variant and writes stable `leaderboard.*` artifacts plus `suite_result.json`, which makes it a good fit for CI or nightly publishing. Each family case also writes a `contract.json` artifact so the suite has an explicit contract layer, not just score files.

Frontier batch for one model:

```bash
vei-eval-frontier run \
  --runner llm \
  --model gpt-5 \
  --scenario-set reasoning \
  --artifacts-root _vei_out/frontier_eval
```

Artifacts from batch evaluation include:

- `aggregate_results.json`
- per-scenario `benchmark_result.json`
- benchmark runs also write `blueprint.json`
- `benchmark_summary.json`
- benchmark-family runs also write `contract.json`
- demo runs also write `leaderboard.md`, `leaderboard.csv`, `leaderboard.json`, and `demo_result.json`
- suite runs also write `leaderboard.md`, `leaderboard.csv`, `leaderboard.json`, and `suite_result.json`
- family-level dimension scores such as evidence preservation, blast radius, least privilege, oversharing avoidance, deadline compliance, comms correctness, and safe rollback

Render a report from any benchmark or frontier batch:

```bash
vei-report generate \
  --root _vei_out/frontier_eval/<run-id> \
  --format markdown \
  --output LEADERBOARD.md
```

## Release Bundles

```bash
vei-release dataset \
  --input-path _vei_out/rollout.json \
  --label rollout \
  --version v20260310

vei-release benchmark \
  --benchmark-dir _vei_out/benchmark/scripted_multi \
  --label scripted-benchmark \
  --version v20260310

vei-release nightly \
  --release-root _vei_out/releases \
  --workspace-root _vei_out/nightly \
  --version nightly-20260310 \
  --environments 5 \
  --scenarios-per-environment 5 \
  --rollout-episodes 2 \
  --benchmark-scenario multi_channel
```

## Examples

- `examples/sdk_playground_min.py`
- `examples/mcp_client_stdio_min.py`
- `examples/rl_train.py`

## Docs

- `docs/ARCHITECTURE.md`
- `docs/BENCHMARKS.md`

## Contributor Notes

`bd` state is local-only under `.beads/` and should stay out of Git.
