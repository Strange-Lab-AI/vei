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
- revenue impact handling
- artifact follow-through
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

For benchmark-family scenarios, scripted, BC, and LLM runners now also emit `workflow_validation` artifacts derived from the same family workflow spec, plus first-class `blueprint_asset.json`, `blueprint.json`, and `contract.json` artifacts. `blueprint_asset.json` is the authored blueprint root; `blueprint.json` is the compiled blueprint with resolved facades, state roots, workflow defaults, contract defaults, and run defaults; `contract.json` makes the success predicates, forbidden predicates, observation boundary, policy invariants, reward terms, and intervention rules explicit. Contract evaluation now treats oracle state and agent-visible observation as separate inputs, so hidden state can be graded without leaking it into the visible surface. Freeform runs are compared against that deterministic contract instead of only against raw score output.

Run the supported benchmark-family demo flow:

```bash
vei-eval demo \
  --family security_containment \
  --artifacts-root _vei_out/demo \
  --run-id security_demo
```

That command runs the family's canonical workflow baseline plus one comparison runner, writes `leaderboard.md` / `leaderboard.csv` / `leaderboard.json`, stores inspectable world state under `state/`, and emits `demo_result.json` with ready-to-run `vei-world` inspection commands plus direct paths to the baseline and comparison `contract.json` artifacts.

Run the curated complex-example showcase:

```bash
vei-eval showcase \
  --artifacts-root _vei_out/showcase \
  --run-id flagship_examples
```

That command executes three stronger enterprise examples on the same kernel-backed demo path:

- `oauth_incident_chain`
  - containment with Google Admin, SIEM, Jira, Docs, and Slack
- `acquired_seller_cutover`
  - identity migration with HRIS, Okta, Google Admin, Salesforce, Jira, Docs, and Slack
- `checkout_revenue_flightdeck`
  - mixed-stack revenue mitigation with Datadog, PagerDuty, feature flags, Spreadsheet, Docs, CRM, Tickets, and Slack

The showcase writes one `showcase_overview.md` plus per-example demo bundles, which makes it the best single command for proving that VEI can coordinate long-horizon, partially observable enterprise tasks across multiple surfaces.

The flagship mixed-stack demo is the revenue/ops primary variant:

```bash
vei-blueprint asset \
  --family revenue_incident_mitigation \
  --workflow-variant revenue_ops_flightdeck

vei-blueprint compile \
  --family revenue_incident_mitigation \
  --workflow-variant revenue_ops_flightdeck

vei-eval demo \
  --family revenue_incident_mitigation \
  --artifacts-root _vei_out/demo \
  --run-id revenue_ops_demo
```

That demo exercises Spreadsheet, Docs, CRM, feature flags, Datadog, PagerDuty, Tickets, and Slack in one contract-graded run.

The cleanest environment-builder example is the identity/access-governance wedge:

```bash
vei-blueprint examples

vei-blueprint bundle \
  --example acquired_user_cutover

vei-blueprint asset \
  --example acquired_user_cutover

vei-blueprint compile \
  --example acquired_user_cutover

vei-blueprint observe \
  --example acquired_user_cutover \
  --focus slack
```

That flow keeps the benchmark stack intact while proving that VEI can compile a typed organization bundle into a runnable world. The current built-in example exposes the full builder ladder: grounding bundle, authored blueprint asset, compiled blueprint, and live world observation. The built-in identity wedge includes HRIS employee state, Okta-style identity records, policy constraints, Google Drive sharing posture, Jira tracking, docs, Slack coordination, and CRM handoff.

Run the canonical multi-family workflow suite for CI or nightly jobs:

```bash
vei-eval suite \
  --artifacts-root _vei_out/suite \
  --run-id nightly_suite
```

That command runs each family's primary workflow variant, then writes the same stable `leaderboard.*` artifacts plus `suite_result.json` for automation-friendly publishing.

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
- benchmark runs additionally write `blueprint_asset.json`
- benchmark runs additionally write `blueprint.json`
- benchmark-family runs additionally write `contract.json`
- demo runs additionally write `leaderboard.md`, `leaderboard.csv`, `leaderboard.json`, and `demo_result.json`
- showcase runs additionally write `showcase_overview.md` and `showcase_result.json`
- suite runs additionally write `leaderboard.md`, `leaderboard.csv`, `leaderboard.json`, and `suite_result.json`

Historical eval outputs should stay under `_vei_out/`, not in Git.
