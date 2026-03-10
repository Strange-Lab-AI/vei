# VEI Benchmarks

VEI supports two benchmark layers that now share the same kernel-backed pipeline.

## 1. Benchmark Families

Reusable capability families:

- `security_containment`
- `enterprise_onboarding_migration`
- `revenue_incident_mitigation`

These are the strategic north-star buckets for enterprise agent evaluation. They carry reusable scoring dimensions such as:

- evidence preservation
- blast radius minimization
- least privilege
- oversharing avoidance
- deadline compliance
- comms correctness
- safe rollback / no data corruption

Run one family:

```bash
vei-eval benchmark \
  --runner workflow \
  --family security_containment \
  --artifacts-root _vei_out/benchmark \
  --run-id security_workflow
```

Run a specific workflow variant:

```bash
vei-eval benchmark \
  --runner workflow \
  --scenario oauth_app_containment \
  --workflow-name security_containment \
  --workflow-variant internal_only_review \
  --artifacts-root _vei_out/benchmark \
  --run-id security_internal_review
```

Run the same family with a policy or LLM runner:

```bash
vei-eval benchmark \
  --runner scripted \
  --family security_containment \
  --artifacts-root _vei_out/benchmark \
  --run-id security_family
```

The `workflow` runner executes the typed family playbook and its reusable assertions directly. Each family can expose multiple named variants backed by typed parameter presets. Those workflows can now express negative assertions, count checks, and virtual-time deadlines in the same DSL. The other runners still use the same family selection and scoring pipeline, but they act freely inside the scenario instead of following the deterministic workflow baseline.

For benchmark-family scenarios, scripted, BC, and LLM runners now also emit `workflow_validation` artifacts derived from the same family workflow spec, so freeform runs can be compared against the deterministic contract instead of only against raw score output.

## 2. Frontier Suites

`vei-eval-frontier` remains the curated long-horizon suite for harder reasoning and safety-heavy tasks. It is implemented on the shared benchmark core, not as a separate reporting stack.

Run one scenario:

```bash
vei-eval-frontier run \
  --runner llm \
  --model gpt-5 \
  --scenario f1_budget_reconciliation \
  --artifacts-root _vei_out/frontier_eval
```

Run a frontier set:

```bash
vei-eval-frontier run \
  --runner llm \
  --model gpt-5 \
  --scenario-set reasoning \
  --artifacts-root _vei_out/frontier_eval
```

## 3. Reports

Any benchmark batch can be summarized with `vei-report`.

```bash
vei-report generate \
  --root _vei_out/frontier_eval/<run-id> \
  --format markdown \
  --output LEADERBOARD.md
```

When a run directory contains workflow-family results alongside scripted, BC, or LLM runs for the same family/scenario, `vei-report` now treats the family's primary workflow variant as the canonical baseline and emits delta reporting against it in markdown, CSV, and JSON output. That includes score, time, step-count, workflow-validation, and family-dimension deltas where they are available.

```bash
vei-report summary --root _vei_out/frontier_eval/<run-id>
```

## 4. Artifacts

Current benchmark runs write:

- `aggregate_results.json`
- `benchmark_summary.json`
- per-scenario `benchmark_result.json`

Historical eval outputs should stay under `_vei_out/`, not in Git.
