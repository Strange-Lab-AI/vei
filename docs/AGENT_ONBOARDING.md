# Agent Onboarding

This is the one starting document for an agent working on VEI. It explains what
VEI is, how to run it, and how to use it for world-model experiments.

VEI is a deterministic enterprise simulation and evaluation system. The same
event spine supports the CLI, Studio UI, twin gateway, branch-point what-if
experiments, and learned world-model benchmarks.

The practical job is usually one of three things:

- run a deterministic enterprise scenario and score an agent
- turn real historical records into a canonical timeline
- train or apply the JEPA-style world model to forecast future state and rank
  counterfactual actions

Do not treat VEI as a magic CEO oracle. Treat it as an offline benchmark and
decision-support workflow. Factual prediction is the strongest evidence.
Counterfactual rankings are hypotheses for human or expert review.

## Start Here

Read these next when needed:

- `README.md` for setup and operator flows
- `docs/ARCHITECTURE.md` for the module map and runtime shape
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
-> temporal train / validation / test split
-> JEPA learns: current state + action -> future heads
-> decision points are selected from pre-branch signals
-> candidate actions are generated from pre-branch context only
-> JEPA scores the candidate futures
-> CSV / Markdown results are reviewed by humans
```

The model predicts a bundle of future heads, not one universal truth number. The
single score in decision tables is a convenience score built from those
predicted heads for a balanced decision objective.

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
not causal proof.

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
  --max-branch-rows-per-thread 24 \
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

Use critical-decision runs when the user wants concrete choices a manager,
editor, agency head, or analyst could have made.

Before ranking candidates, make the decision policy explicit. JEPA predicts
future heads; it does not know by itself whether the operator wants caution,
speed, upside, public warning, trust repair, commercial acceleration, or
regulatory safety. The objective layer defines what "good" means.

For every critical-decision run, record or infer:

- decision role: CEO, product lead, editor, regulator, investor, operator, or
  analyst
- objective policy: what this run is trying to optimize
- candidate diversity policy: which action postures must be present before
  scoring

Do not ask the LLM only for "plausible actions." That tends to produce cautious
process options. Ask for a balanced choice set that includes defensive, active,
fast, escalated, communication, monitoring, and coordination paths where
relevant. The ranking is meaningful only after this policy is clear.

Candidate actions are currently generated by an LLM or by deterministic
templates, and the run records the source in `generation_source` and
`generation_model`. The next interactive mode should use the same candidate
schema but let a person stay in the loop: show selected decision points, let the
operator choose one, accept one or more human-written counterfactual actions,
then send those actions through the same JEPA scoring path.

For news and history, a decision point does not need to be an existing article
or branch event. Prefer a state point:

```text
as-of date + topic/state dossier from records up to that date
-> operator or LLM proposes candidate next events/actions
-> JEPA scores the implied futures under the selected objective policy
```

Do not show a "current strategic top" when asking for human actions. That
anchors the operator to a prior model choice. Show the state dossier and ask for
the next event or action to test.

The general rule is:

```text
JEPA prediction + objective policy + candidate policy = ranked recommendation
```

Do not collapse that into "the model recommends X" without naming the policy.

```bash
vei whatif benchmark critical-decisions \
  --input enron=_vei_out/enron/context_snapshot.json \
  --input dispatch=_vei_out/dispatch/context_snapshot.json \
  --input newco=_vei_out/newco/context_snapshot.json \
  --source-build-root _vei_out/world_model_multitenant_jepa/enron_dispatch_newco \
  --checkpoint _vei_out/world_model_multitenant_jepa/enron_dispatch_newco/model_runs/jepa_latent/model.pt \
  --artifacts-root _vei_out/world_model_critical_decisions \
  --label enron_dispatch_newco_critical \
  --cases-per-tenant 4 \
  --candidates-per-decision 10 \
  --candidate-mode llm \
  --candidate-model gpt-5.3-codex-spark \
  --model-id jepa_latent
```

For a human-supplied news/history state point:

```bash
vei whatif benchmark news-state-point \
  --input news=_vei_out/news_world_model/americanstories/context_snapshot.json \
  --checkpoint _vei_out/news_world_model/benchmarks/current/model_runs/jepa_latent/model.pt \
  --artifacts-root _vei_out/news_world_model/state_points \
  --label banking_1837_09_06 \
  --topic banking_markets \
  --as-of 1837-09-06 \
  --candidate "New banking bill passes::A new banking bill passes; issue a public economy memo on credit, deposits, prices, employment, and merchant failures." \
  --candidate "Congress delays reform::Congress delays banking reform; publish a risk warning on credit contraction and business failures."
```

Use `label::action` for supplied candidates, or
`candidate_type::label::action` when the operator wants to lock the action
posture. The generated state event id will look like
`news_state:<topic>:<date>` and is not a historical event id.

Codex-session models route through Codex. Direct provider API models are also
allowed when configured. The run must record which model generated candidates
and whether it fell back to templates.

A good candidate action is concrete enough that someone could choose it. It
should name the owner, action path, review path, communication boundary, and
follow-up trigger where relevant. It should not be a minor rewording of another
candidate.

For news timelines, do not let the candidate set collapse into only caution,
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

- `_vei_out/<root>/<label>/critical_decision_scores.csv`
- `_vei_out/<root>/<label>/critical_decision_scores.md`
- `_vei_out/<root>/<label>/leakage_report.json`
- `_vei_out/<root>/<label>/candidate_generation_manifest.json`

When reporting results, show the decision point, why it was selected, the
candidate actions, the predicted score and tradeoffs, JEPA's top-ranked action,
generation source, and leakage status.

For news runs, report both scores and the selected objective policy:

- `balanced_ceo_score` is the raw JEPA-balanced outcome score using the current
  company-style proxy heads.
- `strategic_usefulness_score` is the selected objective-policy score. In the
  current news policy, it keeps the JEPA predictions but breaks close calls
  toward active moves such as advisories, watches, actor maps, policy memos, and
  coordination.

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

VEI can also run on news timelines. That is an outside-in forecasting setup, not
a full internal company operating model.

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

Then run the same readiness, build, train, evaluate, and critical-decision
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
