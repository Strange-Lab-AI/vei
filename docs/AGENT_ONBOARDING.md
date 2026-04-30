# Agent Onboarding

This is the one starting document for an agent working on VEI. It explains what
VEI is, how to run it, and how to use it for world-model experiments and
company skill maps.

VEI is a deterministic enterprise simulation and evaluation system. The same
event spine supports the CLI, Studio UI, twin gateway, branch-point what-if
experiments, company skill-map compilation, and learned world-model benchmarks.

The practical job is usually one of four things:

- run a deterministic enterprise scenario and score an agent
- turn real historical records into a canonical timeline
- ingest agent-activity evidence and review VEI Control reports
- compile company-specific draft skills from the normalized company bundle
- train or apply the JEPA-style world model to forecast future state and rank
  counterfactual actions

Do not treat VEI as a magic CEO oracle. Treat it as an offline benchmark and
decision-support workflow. Factual prediction is the strongest evidence.
Counterfactual rankings are hypotheses for human or expert review.

## Start Here

Read these next when needed:

- `README.md` for setup and operator flows
- `docs/ARCHITECTURE.md` for the module map and runtime shape
- `docs/ENRON_EXAMPLE.md` for the public company example
- `docs/NEWS_EXAMPLE.md` for the public news-timeline example
- `docs/WHATIF.md` for the world-model and what-if command reference
- `docs/RL_GYM.md` for the scoped contract-only RL-training plan
- `AGENTS.md` for repo policy, eval runners, and validation expectations
- `Makefile` and `.agents.yml` before changing gates or CI behavior

Use the repo command surface where possible:

```bash
make check
make test
```

Full local validation is heavier:

```bash
make check-full
make test-full
make enron-example
make dynamics-eval
VEI_LLM_LIVE_BYPASS=1 make llm-live
make deps-audit
```

## Repo Map

- `vei/world/` holds the deterministic world-session kernel and replayable state.
- `vei/router/` exposes the MCP tool surface over the kernel.
- `vei/structure/` builds the event-derived read model and truth-comparison
  helpers.
- `vei/twin/` exposes the HTTP governed twin surface.
- `vei/workspace/` and `vei/run/` hold the file-backed workspace and run model.
- `vei/skillmap/` compiles evidence-backed, replay-checked company skills from
  normalized bundles.
- `vei/ingest/agent_activity/` captures external agent behavior from JSONL
  landing zones, MCP transcripts, and OpenAI org usage/audit evidence.
- `vei/provenance/` builds Control reports: timeline, activity graph, access
  review, blast radius, policy replay, and OTel export.
- `vei/whatif/` handles branch-point replay, counterfactuals, benchmarks, and
  saved example bundles.
- `vei/verticals/` holds seeded business packs and overlays.
- `tests/` covers the repo. `tests/dynamics/` also emits
  `_vei_out/dynamics_eval/metrics.json`.

Keep cross-module calls behind typed module APIs, usually `some_module/api.py`.
Keep local secrets in `.env`. Do not commit `_vei_out/`, `.artifacts/`, private
exports, generated prompts, raw archives, or ad hoc traces.

## Basic Use

Install the learned-world-model extras when needed:

```bash
pip install -e ".[worldmodel,llm,ui,browser]"
pip install -e ".[jepa]"
```

Useful entrypoints:

```bash
vei quickstart run
vei eval benchmark --runner workflow --family security_containment
vei skillmap build --source-dir _vei_out/<tenant>/context_snapshot.json --output _vei_out/<tenant>/skill_map
vei ingest agent-activity --source agent_activity_jsonl --path ./logs --workspace _vei_out/<tenant>
vei provenance access-review --agent-id <agent-id> --workspace _vei_out/<tenant>
vei provenance evidence-pack --agent-id <agent-id> --workspace _vei_out/<tenant>
vei provenance export --format evidence-pack --workspace _vei_out/<tenant> --output _vei_out/<tenant>/evidence_pack.json
vei ui serve --root docs/examples/enron-master-agreement-public-context/workspace --host 127.0.0.1 --port 3055
```

Eval runners:

- `workflow` is the reference runner.
- `scripted` is the deterministic floor baseline.
- `bc` is the tool-frequency baseline powered by `FrequencyPolicy`.
- `llm` runs a real model through the MCP world.

Same seed means same world. Determinism is part of the product.

## World Model Mental Model

The world-model loop is:

```text
raw work or news data
-> canonical timestamped events
-> archive-derived doctrine packet: what this organization is, what is strategic,
   what is out of scope, and which archive citations support that reading
-> temporal train / validation / test split
-> JEPA learns: doctrine text + current state + action -> future heads
-> decision points are selected from pre-branch signals
-> candidate actions are generated from pre-branch context only
-> JEPA scores the candidate futures
-> CSV / Markdown results are reviewed by humans
```

The model predicts a bundle of future heads, not one universal truth number. The
single score in decision tables is a convenience score built from those
predicted heads for a balanced decision objective.

The doctrine packet is explicit, not magic. It is now generated from archive
evidence first, with citations and provenance, then carried as text in each
pre-branch contract. JEPA receives a deterministic text-hash embedding of that
packet alongside the current state and candidate action. Compact doctrine tags
still exist as debug/fallback metadata, but the main scalable path is doctrine
text conditioning, not hand-written tenant flags.

Important predicted heads include:

- evidence flow: outside spread, follow-up volume, fanout, review loops, delays,
  blame pressure, reassurance, commitment clarity
- business heads: enterprise risk, commercial position, organization strain,
  stakeholder trust, execution drag
- future-state heads: regulatory exposure, accounting-control pressure,
  liquidity stress, governance response, evidence control, external-confidence
  pressure
- objective scores: minimize risk, protect commercial position, reduce strain,
  preserve trust, maintain velocity

Current evidence from the local pooled company runs supports a careful claim:
JEPA improved factual calibration and business-head error versus the heuristic
baseline. Counterfactual rankings should still be presented as decision support,
not causal proof. The learned target is factual future heads; operator objective
weights are optional query/reporting policies, not the model's ground truth.

## Canonical Inputs

For each tenant, prefer a VEI bundle with:

- `context_snapshot.json`
- `canonical_events.jsonl`
- `canonical_event_index.json`

Source records may be email, ClickUp, Notion, docs, tickets, CRM, meeting notes,
news articles, or other time-bearing records. The key requirement is that they
can be canonicalized into dated events with enough thread or case structure to
build pre-branch state and future tails.

Check readiness before training:

```bash
python scripts/check_tenant_world_model.py --root _vei_out/<tenant>/context_snapshot.json
```

## Pooled Training

Build one pooled benchmark from all timestamp-ready tenants. Hold out the final
tail of each tenant timeline for testing.

```bash
vei whatif benchmark build-multitenant \
  --input enron=_vei_out/enron/context_snapshot.json \
  --input dispatch=_vei_out/dispatch/context_snapshot.json \
  --input newco=_vei_out/newco/context_snapshot.json \
  --artifacts-root _vei_out/world_model_multitenant_jepa \
  --label enron_dispatch_newco \
  --heldout-cases-per-tenant 4 \
  --future-horizon-events 12 \
  --max-branch-rows-per-thread 512 \
  --candidate-mode template
```

Train JEPA on earlier history:

```bash
vei whatif benchmark train \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch_newco \
  --model-id jepa_latent \
  --epochs 10 \
  --batch-size 128 \
  --train-split train \
  --train-split validation \
  --validation-split test
```

Train the heuristic baseline for comparison:

```bash
vei whatif benchmark train \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch_newco \
  --model-id heuristic_baseline \
  --train-split train \
  --train-split validation \
  --validation-split test
```

Evaluate both:

```bash
vei whatif benchmark eval \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch_newco \
  --model-id jepa_latent

vei whatif benchmark eval \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch_newco \
  --model-id heuristic_baseline
```

Report factual metrics first: AUROC when defined, Brier score, calibration
error, business-head MAE, and objective-head MAE.

## Decision Points And Counterfactuals

Use strategic state-point runs when the user wants concrete choices a manager,
editor, agency head, or analyst could have made. This is the only user-facing
counterfactual interface. Older event-selector paths have been cut over, and
news-specific helpers are internal regression/provenance tools, not the product
path.

Keep the boundary clear. JEPA learns to predict factual future heads. It should
not be described as learning a universal preference for caution, speed, upside,
trust repair, commercial acceleration, or regulatory safety. The primary report
ranks candidate actions by the learned predicted future vector: Pareto rank
first, then a convenience future score. Optional objective views can be shown as
operator/query lenses in the detailed audit file, but they are not JEPA targets
and not learned ground truth.

The repeatable order is doctrine extraction first, then state-point proposal,
then candidate generation, then JEPA scoring. The doctrine packet should be
generated from archive evidence and source docs when available. A human can
override it, but the override must be recorded in provenance.

A state point does not need to be a real email, Slack message, ticket, or
article. It can be an as-of question proposed by an LLM or a human, for example:
"as of November 2025, should this company pitch a major AI lab, run a narrow
pilot, change positioning, or hold for more evidence?" The proposal layer uses
only pre-as-of evidence; JEPA then scores each candidate action through the same
future-prediction boundary.

For every strategic state-point run, record or infer:

- decision role: CEO, product lead, editor, regulator, investor, operator, or
  analyst
- strategic relevance: why this as-of decision point was proposed from
  pre-as-of evidence
- candidate diversity policy: which action postures must be present before
  scoring
- optional objective view: only if the operator wants a named reporting lens

Do not ask the LLM only for "plausible actions." That tends to produce cautious
process options. Ask for a balanced choice set that includes defensive, active,
fast, escalated, communication, monitoring, and coordination paths where
relevant. The ranking is meaningful only after the candidate set is broad enough
to test real alternatives.

Candidate actions are currently generated by an LLM or by deterministic
templates, and the run records the source in `generation_source` and
`generation_model`. Candidate sets must include broad postures, not minor
variants: upside/exploit, narrow pilot, fast move, hold/review, escalation,
coordination, commercial reset, and trust/privacy where relevant. Human-written
counterfactuals should go through the same JEPA scoring path.

For news and history, a decision point does not need to be an existing article
or branch event. Prefer a state point:

```text
as-of date + topic/state dossier from records up to that date
-> operator or LLM proposes candidate next events/actions
-> JEPA predicts the implied future vector for each candidate
-> report Pareto/tradeoff ranks, with optional operator views if requested
```

Do not show a "current strategic top" when asking for human actions. That
anchors the operator to a prior model choice. Show the state dossier and ask for
the next event or action to test.

The general rule is:

```text
state + doctrine text + candidate action text/schema
-> JEPA-predicted future vector
-> Pareto/tradeoff report
-> optional operator objective view
```

Do not collapse that into "the model recommends X" without saying what the model
actually predicted and whether an optional operator view was applied.

```bash
vei whatif benchmark strategic-state-points \
  --input dispatch=_vei_out/datasets/dispatch_real/context_snapshot.json \
  --input powrofyou=_vei_out/datasets/powrofyou/context_snapshot.json \
  --checkpoint _vei_out/world_model_multitenant_jepa/current/model_runs/jepa_latent/model.pt \
  --artifacts-root _vei_out/world_model_strategic_state_points \
  --label current_strategic_state_points \
  --decisions-per-tenant 3 \
  --candidates-per-decision 8 \
  --proposal-mode llm \
  --proposal-model gpt-5.4
```

This writes `strategic_state_point_results.csv` and `.md`. The fields named
`why_this_decision_was_proposed` and `candidate_type` are proposal scaffolding.
The fields named `predicted_*` are JEPA outputs. The field named
`balanced_operator_score` is a fixed reporting readout over predicted heads, not
a learned JEPA preference.

Read the result in layers:

- `predicted_future_vector`, `operator_utility_heads`, `domain_risk_heads`, and
  `telemetry_heads` are the model-facing future readout.
- `latent_future_id` and the latent distance columns compare the JEPA-predicted
  branch futures when the checkpoint exposes them.
- `pareto_frontier_group`, `frontier_rank`, `operator_score_rank`, and
  `display_rank` are report ordering fields. Display rank puts frontier options
  first; operator score rank is only the fixed score order.
- `success_observable`, `failure_observable`, `time_to_signal`,
  `next_decision_trigger`, and `falsifying_evidence` are generated before
  scoring so the branch can be checked later.

The alternate user path is interactive: show the state dossier and proposed
decisions to the user, let them edit or replace the decision/action set, then
score the final actions through this same state-point path.

Strategic proposal models route through Codex by default. The default is
`gpt-5.4`, which is the newest model accepted by the current local Codex CLI.
Override `--proposal-model` to `gpt-5.5` when the installed Codex runtime
supports it. Set `VEI_STRATEGIC_PROPOSAL_BACKEND=api` only when an explicit
direct-provider API run is intended. The run must record which model generated
candidates and whether it fell back to templates.

A good candidate action is concrete enough that someone could choose it. It
should name the owner, action path, review path, communication boundary, and
follow-up trigger where relevant. It should not be a minor rewording of another
candidate.

For news timelines, use [NEWS_EXAMPLE.md](NEWS_EXAMPLE.md) as the public worked
example. Do not let the candidate set collapse into only caution,
verification, and compliance. A useful news-world run should compare defensive
actions against active strategic options:

- publish a public advisory or warning
- launch a monitoring watch with escalation triggers
- issue a market, policy, customer, or public-risk memo
- map affected actors, institutions, geography, or press networks
- coordinate with external agencies, experts, civic groups, or market actors
- choose an explicit editorial or leadership stance
- publish a narrow verified update with a correction path
- hold or defer when source risk outweighs action

Primary outputs:

- `_vei_out/<root>/<label>/strategic_state_point_results.csv`
- `_vei_out/<root>/<label>/strategic_state_point_results.md`
- `_vei_out/<root>/<label>/strategic_state_point_results.json`
- `_vei_out/<root>/<label>/strategic_state_point_proposals.json`

When reporting results, show the decision point, why it was selected, the
candidate actions, the JEPA-predicted future vector, the delta versus the
baseline action, frontier membership, score rank, concrete observables,
generation source, and leakage status.

For all runs, report the learned future vector before any optional operator lens:

- predicted risk, commercial, strain, trust, drag, external-spread, fanout, and
  related heads are JEPA outputs.
- `is_pareto_efficient`, `pareto_frontier_group`, predicted deltas versus the
  baseline action, latent distances, and tradeoff summaries are model-facing
  comparison aids.
- `balanced_operator_score`, older `balanced_ceo_score`,
  `strategic_usefulness_score`, `objective_policy_summary`, and
  `operator_policy_basis` are optional audit/reporting lenses. They are useful
  for inspecting a run, but they are not treated as learned ground-truth
  preferences.

This is intentional. Otherwise news timelines can look "safe" while mostly
recommending holds and verification, which is not the same as being useful.

## Leakage Rules

Never let the candidate generator or judge see the recorded future tail.

Required checks:

- no held-out branch event IDs leak into fit rows
- train, validation, and test splits are temporal by tenant
- candidate-generation prompts contain only pre-branch context
- generated candidates contain no future-tail event markers
- judge dossiers contain no future-tail event markers or model scores
- generated candidates record prompt hash, evidence hash, model, source, and
  `no_future_context=true`

If any leakage check fails, do not make model-performance claims from that run.

## News Timelines

VEI can also run on news timelines. See [NEWS_EXAMPLE.md](NEWS_EXAMPLE.md) for
the public example. That is an outside-in forecasting setup, not a full internal
company operating model.

For bounded historical experiments, use public-domain or permissively licensed
contiguous data, then canonicalize each article or page as a dated event with
source, title, body excerpt, date, topic, provenance, and confidence metadata.

The helper below builds a bounded PleIAs sample. PleIAs rows are OCR newspaper
pages, not clean article units, so the output is exploratory. The helper wraps
pages as synthetic mail/docs surfaces so they fit VEI's existing timeline and
branch-point machinery.

```bash
python scripts/build_news_world_model_snapshot.py \
  --output-root _vei_out/news_world_model/pleias_1935_1939_sample \
  --start-date 1935-01-01 \
  --end-date 1939-12-31 \
  --max-pages-per-day 20 \
  --max-pages-per-source-per-day 2
```

Then run the same readiness, build, train, evaluate, and strategic-state-point
commands as a company tenant. For news runs, generated actions should respond to
the public event itself. They should not propose fixing OCR, ingestion, product,
or data-pipeline wrappers unless the source event is actually about those topics.

Future news-native heads should include follow-up coverage, topic persistence,
source diversity, public-risk escalation, market or policy attention, and
correction pressure. Until those heads exist, business-head scores are useful
but imperfect proxies.

## Artifact Names

Supported what-if forecast filenames:

- `whatif_ejepa_result.json`
- `whatif_reference_result.json`
- `whatif_heuristic_baseline_result.json`

Use `whatif_reference_result.json` as the main saved forecast artifact for
repo-owned Enron bundles. Keep `whatif_heuristic_baseline_result.json` for debug
and baseline comparisons. Import constants from `vei.whatif.filenames` or
`vei.whatif.api`.

## Cleanup

After large runs, inspect before deleting:

```bash
du -sh _vei_out
make clean-workspace-dry-run
make clean-workspace-hard-dry-run
git status --short --branch
```

Use `make clean-workspace` for low-risk cache/build cleanup. It leaves `_vei_out`
runs alone. Use `make clean-workspace-hard` only when the user approves a
cutover that discards old generated VEI runs; it preserves
`_vei_out/world_model_current/`, `_vei_out/datasets/`, and
`_vei_out/llm_live/latest/`. Keep source zips or archives outside `_vei_out`
when the user asked to preserve them.

For world-model decision outputs, use one human-facing current name:
`_vei_out/world_model_current/world_model_decision_summary.csv` plus the matching
`.md`. The run-local
`_vei_out/world_model_strategic_state_points/<run_label>/` folder is provenance,
not a second canonical result. If the user needs every audit column, use
`_vei_out/world_model_current/world_model_decision_results.json`.
