# Historical What-Ifs

VEI now supports a company-history historical what-if workflow for archive-backed datasets such as the Enron Rosetta event tables and normalized multi-source context snapshots.

Install the learned runtime when you want the repo-owned Enron examples to open with the shipped reference forecast from a fresh clone:

```bash
pip install -e ".[worldmodel,llm,ui,browser]"
```

Install the optional JEPA backend from the same clone:

```bash
pip install -e ".[jepa]"
```

The flow has five steps:

1. **Normalize** — turn raw company records into a verified `context_snapshot.json` (`vei context normalize`).
2. **Branch** — explore the whole history, pick one exact historical event as the branch point.
3. **Materialize** — build a strict historical workspace with `episode_manifest.json` and the canonical `whatif_public_context.json` sidecar.
4. **Compare** — run the baseline future against one or more counterfactual paths.
5. **Validate** — verify the saved bundle (`python scripts/validate_whatif_artifacts.py`).

Studio supports the same loop directly:

1. search the archive for a real historical moment
2. choose one event from the results
3. materialize the baseline workspace
4. run the counterfactual and inspect the saved comparison bundle
5. validate the resulting bundle

## Why this shape

VEI does not try to turn an entire historical corpus into one giant always-running simulation. That would be slower, heavier, and harder to understand in a demo.

Instead, the system uses two connected layers:

- **Whole-history analysis** for broad questions such as “what would this policy have caught?”
- **Event-level replay** for one chosen moment, where VEI can branch, replay, and compare outcomes inside a normal world workspace

This keeps the whole-history pass deterministic and cheap while still giving us a true replay environment for the interesting moment.

## What gets materialized

When VEI opens a historical episode, it builds a workspace from the selected surface:

- earlier records on that surface become the initial historical state
- the selected event and later historical records become scheduled replay events
- observed participants become identity records
- policy-relevant annotations stay attached for analysis and scoring

The important constraint is honesty:

- VEI keeps mail as mail, chat as chat, and tickets as tickets
- VEI keeps historical excerpts labeled as excerpts when the source data is truncated
- unsupported surfaces stay disabled instead of being faked

## Compare paths

There are two compare paths today:

- **LLM actor continuation**
  - bounded continuation on the affected thread or ticket
- supports mail, Slack or Teams-style chat, and Jira-style ticket comments
- derives shared case ids across those surfaces and attaches linked docs or CRM records when the bundle includes them
  - limited to the known thread participants and allowed targets
  - defaults to `gpt-5-mini` so the interactive run completes quickly and predictably
  - useful for “what would someone have said or done next?”
- **Learned backend forecast (optional, pluggable)**
  - real checkpoint-backed forecast for risk and volume deltas when the repo-local reference checkpoint or the optional JEPA runtime is available
  - trained on a deterministic local slice of related branch history around the chosen event, so the forecast stays tied to the exact decision you are changing
  - prefers the repo-local reference checkpoint, then the optional JEPA runtime, then the heuristic baseline
  - useful for “how much would this likely reduce exposure, escalation, or follow-up volume?”

The heuristic baseline is a tag-driven heuristic, not a learned model. Keep it as a debug and regression baseline. Use the repo-local reference checkpoint as the flagship learned path.

Both forecast paths now go through the shared `vei.dynamics` boundary. The what-if experiment flow calls `vei.dynamics.api.get_backend(...)` and the concrete forecast engines (JEPA subprocess, heuristic, reference checkpoint) are plugged in behind the contract via `vei.whatif.dynamics_bridge`. That means swapping or adding a forecast backend does not touch the whatif flow — it registers a new `DynamicsBackend` and updates `.agents.yml`.

On top of the forecast path, VEI now builds a shared business-state readout. That layer translates the forecast into decision language such as outside spread risk, internal handling load, execution delay, commercial position, and approval or escalation pressure. The saved workspace and the saved forecast bundle both carry that readout.

## CLI

```bash
# Whole-history analysis
vei whatif explore \
  --rosetta-dir /path/to/rosetta \
  --scenario compliance_gateway \
  --format markdown

# Search for exact branch points
vei whatif events \
  --rosetta-dir /path/to/rosetta \
  --actor vince.kaminski \
  --query "btu weekly" \
  --flagged-only \
  --format markdown

# Build a replayable episode from one exact event
vei whatif open \
  --rosetta-dir /path/to/rosetta \
  --root _vei_out/whatif/enron_case \
  --event-id evt_1234

# Replay the historical future
vei whatif replay \
  --root _vei_out/whatif/enron_case \
  --tick-ms 600000

# Run the full counterfactual experiment
vei whatif experiment \
  --rosetta-dir /path/to/rosetta \
  --artifacts-root _vei_out/whatif_experiments \
  --label master_agreement_internal_review \
  --event-id evt_1234 \
  --model gpt-5-mini \
  --forecast-backend reference \
  --counterfactual-prompt "Keep the draft inside Enron, loop in Gerald Nemec for legal review, and hold the outside send until the clean version is approved."
```

## Artifacts

`vei whatif experiment` writes a result bundle that includes:

- experiment result JSON
- experiment overview Markdown
- LLM path JSON
- forecast path JSON
- business-state comparison JSON
- business-state comparison Markdown
- the strict replay workspace used for the run

The forecast bundle is written as `whatif_ejepa_result.json` when the JEPA path runs, `whatif_reference_result.json` when the repo-local reference backend runs, and `whatif_heuristic_baseline_result.json` for the heuristic baseline.

This makes it easy to inspect the result in Studio later, compare runs, or hand the output to another tool.

Use `python scripts/validate_whatif_artifacts.py <workspace-or-bundle-path>` after you refresh a saved workspace or a repo-owned example bundle.

The saved bundle that Studio reads has one stable core shape:

- `whatif_experiment_result.json`: combined saved result and artifact pointers
- `whatif_experiment_overview.md`: short human-readable run summary
- `workspace/context_snapshot.json`: normalized company-history bundle for the saved branch
- `workspace/canonical_events.jsonl`: saved canonical event timeline for the branch
- `workspace/canonical_event_index.json`: saved index for timeline and readiness views
- `workspace/episode_manifest.json`: saved what-if workspace manifest

Optional sidecars are validated when present:

- `whatif_llm_result.json`: saved bounded continuation result
- `whatif_ejepa_result.json`, `whatif_reference_result.json`, or `whatif_heuristic_baseline_result.json`: saved forecast result
- `whatif_business_state_comparison.json` + `whatif_business_state_comparison.md`: ranked comparison payload and summary when the ranked path is saved

For Enron, VEI now ships the saved example surface plus a small checked-in Rosetta sample. The sample lives under `data/enron/rosetta/`, the full archive is an optional download fetched with `make fetch-enron-full`, the public-company fixture lives under `vei/whatif/fixtures/enron_public_context`, the curated public-record fixture lives under `vei/whatif/fixtures/enron_record_history`, and the helper docs live in [ENRON_DATASET.md](ENRON_DATASET.md) and [ROSETTA_SOURCE.md](ROSETTA_SOURCE.md). Refresh the public fixture with `python scripts/prepare_enron_public_context.py`, fetch and verify the full archive with `make fetch-enron-full` plus `python scripts/check_rosetta_archive.py`, and refresh screenshots with `python scripts/capture_enron_bundle_screenshots.py`.

The current Enron public context carries 11 dated financial checkpoints, 21 dated public news events, 986 daily stock rows, 7 credit events, and 1 FERC timeline event across 24 archived public source files. The curated public-record fixture adds dated filings, disclosures, hearing records, and exhibit-style records into the same saved canonical timeline. VEI slices those rows to the active Enron window and then to the chosen branch date before they are shown in Studio, written into the saved episode manifest, attached to the saved bundle, or added to benchmark dossiers.

The same path now works for a new company history bundle. Put the normalized historical source in `context_snapshot.json`. The capture path also writes `canonical_events.jsonl` and `canonical_event_index.json` beside that snapshot. Multi-source snapshots can now branch from mail, Slack or Teams-style chat, Jira-style ticket history, GitHub, GitLab, ClickUp, linked docs, and CRM records through the same typed what-if path. VEI derives a shared case id from that history, shows earlier cross-surface case activity in the branch scene, and carries that linked operational history plus linked document or CRM records into the saved workspace when the bundle includes them. Put a sidecar `whatif_public_context.json` in the same folder when you want dated public facts in the branch scene, the prompt, and the saved run. Put a research-pack JSON file anywhere on disk when you want a reusable set of held-out branch cases for `vei whatif pack run` or `vei whatif benchmark build`.

Use the new file-backed chronology commands when you want to inspect that tenant before you branch:

```bash
vei context timeline --root /path/to/newco/context_snapshot.json --limit 25
vei context readiness --root /path/to/newco/context_snapshot.json --format plain
python scripts/check_tenant_world_model.py --root /path/to/newco/context_snapshot.json
```

## New company onboarding

Bring a new company into the what-if system with three files:

- `context_snapshot.json` for the normalized company history
- `whatif_public_context.json` beside that archive when you want dated public-company context
- `research_pack.json` when you want reusable pack runs or held-out benchmark cases

For live onboarding, use the twin entrypoint when you want capture, canonical timeline files, workspace build, and a readiness readout in one command:

```bash
vei twin onboard \
  --root _vei_out/newco/twin \
  --org "NewCo" \
  --domain newco.example \
  --provider github \
  --provider gitlab \
  --provider clickup \
  --filter github:repo=newco/platform \
  --filter gitlab:project=newco/platform \
  --filter clickup:list_id=123456
```

For offline exports, point the same entrypoint at local archive paths:

```bash
vei twin onboard \
  --root _vei_out/newco/twin \
  --org "NewCo" \
  --domain newco.example \
  --provider gmail \
  --provider notion \
  --base-url gmail=/path/to/gmail-takeout.zip \
  --base-url notion=/path/to/notion-export.zip
```

The repo also includes a local-only helper for the real Dispatch startup archives. It auto-detects `~/Downloads/dispatch-gmail.zip` and `~/Downloads/dispatch-notion.zip`, builds a private workspace under `_vei_out/dispatch-real-example`, and writes a readiness plus timeline summary beside it:

```bash
python scripts/build_dispatch_local_example.py
```

### From a quickstart / playable workspace

If you already have a quickstart or vertical workspace (e.g. `vei quickstart run --world service_ops --governor-demo`), the company graph is in the blueprint asset, not in a `context_snapshot.json`. Project it into the canonical shape with one command:

```bash
vei whatif export --workspace _vei_out/quickstart \
  --output _vei_out/quickstart/context_snapshot.json
```

The output is a multi-source `context_snapshot.json` plus `canonical_events.jsonl` and `canonical_event_index.json`. Those three files can be passed to `vei whatif events`, `vei whatif open`, and `vei whatif experiment` via `--source company_history --source-dir <path>`, and the same sidecars now drive the Company Timeline view and the readiness commands.

The normalized company history is the event layer. Put real time-ordered activity there for mail, chat, tickets, and any other surface that can branch or replay. Put state-only sources such as documents or CRM records there too when you want them to show up as linked case context around the branch.

The source-bundle public-context sidecar is optional. If it is missing, VEI still opens the branch scene, runs the replay, and scores the counterfactual. If the sidecar is present but broken, VEI keeps loading the world and shows an empty public-context slice instead of failing the run.

The saved workspace sidecar is always written as `workspace/whatif_public_context.json`, even when the public slice is empty, so saved-workspace validation stays stable.

The history bundle still needs at least one healthy event surface. VEI ignores raw providers that captured with `status: "error"` when it decides whether a bundle is usable for branching and replay.

The repo now also carries three saved synthetic Clearwater bundles built through this same export path:

- `docs/examples/clearwater-dispatch-recovery`
- `docs/examples/clearwater-billing-dispute-reopened`
- `docs/examples/clearwater-technician-no-show`

```bash
# Run a branch experiment on a generic company history bundle
vei whatif experiment \
  --source-dir /path/to/newco/context_snapshot.json \
  --artifacts-root _vei_out/whatif_experiments \
  --label newco_internal_review \
  --event-id msg_123 \
  --forecast-backend e_jepa \
  --counterfactual-prompt "Keep the draft inside NewCo, route it through legal review, and hold the outside send until the clean version is ready."

# Run a reusable case pack from a JSON file
vei whatif pack run \
  --source-dir /path/to/newco/context_snapshot.json \
  --label newco_pack_run \
  --pack-id /path/to/newco/research_pack.json

# Build a held-out benchmark from the same file-backed case pack
vei whatif benchmark build \
  --source-dir /path/to/newco/context_snapshot.json \
  --label newco_benchmark \
  --heldout-pack-id /path/to/newco/research_pack.json
```

## Current Studio flow

The current repo-owned Enron set has eight saved bundles split into two roles.

Proof bundles:

- `docs/examples/enron-master-agreement-public-context/`
- `docs/examples/enron-pge-power-deal/`
- `docs/examples/enron-california-crisis-strategy/`
- `docs/examples/enron-baxter-press-release/`
- `docs/examples/enron-braveheart-forward/`

Narrative bundles:

- `docs/examples/enron-watkins-follow-up/`
- `docs/examples/enron-q3-disclosure-review/`
- `docs/examples/enron-skilling-resignation-materials/`

The flow gif and search screenshot below still show the interactive loop at a high level. The decision, public-context, macro, and ranking panels were refreshed from the repo-owned bundles with `python scripts/capture_enron_bundle_screenshots.py`, so a fresh clone can open the same saved branch points and inspect the same saved comparisons without a sibling checkout.

![Enron historical what-if flow](assets/enron-whatif/enron-whatif-flow.gif)

![Enron search results](assets/enron-whatif/enron-search-results.png)

![Enron decision scene with public context](assets/enron-whatif/enron-decision-scene-top.png)

The public-company panel is its own dated slice. This saved `Master Agreement` branch is on September 27, 2000, so it shows the five financial checkpoints and six public-news rows that were already public by that date. The saved Company Timeline beside it now carries 30 prior canonical events pulled from multiple source families rather than only a short mail thread.

![Enron public company context panel](assets/enron-whatif/enron-public-context.png)

The repo-owned `Master Agreement` example also carries the saved result panels for the same branch point. The single saved counterfactual keeps the same 84-event horizon, moves risk from `1.000` to `0.560`, and predicts `64` fewer outside-addressed sends.

![Enron predicted business change](assets/enron-whatif/enron-predicted-business-change.png)

The same bundle now shows a macro panel beside the email-path readout. The current Enron macro calibration stays weak, so the stock, credit, and FERC heads are clearly labeled as advisory context.

![Enron macro outcome panel](assets/enron-whatif/enron-macro-outcomes.png)

The saved ranked comparison then turns that into a real decision view for the same moment: hold for internal review ranks first at `0.209`, a narrow status note lands second at `0.208`, and a fast-turnaround push loses the containment gain at `-0.019`.

![Enron ranked business comparison](assets/enron-whatif/enron-ranked-comparison.png)

## Live Enron display in Studio

Use the repo-owned saved Enron examples directly when you want the screen to show Enron itself from a fresh clone.

```bash
vei ui serve \
  --root docs/examples/enron-master-agreement-public-context/workspace \
  --host 127.0.0.1 \
  --port 3055
```

Open `http://127.0.0.1:3055` and stay inside that workspace. This keeps the display tied to the actual Enron branch point and the actual saved result.

This repo-owned Studio path is a saved reference display first. The checkout carries the Rosetta sample under `data/enron/rosetta`, so a fresh clone can open the saved bundles and rebuild the sample-backed example surface immediately. Fetch the full archive with `make fetch-enron-full` when you want whole-history Enron search, full benchmark builds, full training runs, or archive validation.

Use the saved snapshot directly when you want a fresh-clone rerun of the same branch slice:

```bash
vei whatif experiment \
  --source mail_archive \
  --source-dir docs/examples/enron-master-agreement-public-context/workspace/context_snapshot.json \
  --artifacts-root _vei_out/enron_saved_snapshot_runs \
  --label enron_internal_review_rerun \
  --thread-id thr_e565b47423d035c9 \
  --event-id enron_bcda1b925800af8c \
  --counterfactual-prompt "Keep the draft inside Enron, ask Gerald Nemec and Sara Shackleton for review, and hold the outside send." \
  --forecast-backend reference
```

That rerun uses the saved branch workspace slice that ships in the repo. Whole-history Enron search, training, and full benchmark builds use the fetched full archive, or `VEI_WHATIF_ROSETTA_DIR` when you point VEI at a different full archive path.

The committed example bundle also carries:

- `whatif_experiment_overview.md`
- `whatif_llm_result.json`
- `whatif_reference_result.json`
- `whatif_experiment_result.json`
- `whatif_business_state_comparison.md`
- `whatif_business_state_comparison.json`
- `workspace/canonical_events.jsonl`
- `workspace/canonical_event_index.json`

Unlike a plain saved run (which may omit ranked sidecars), this repo-owned example intentionally includes ranked comparison artifacts as part of the reference story.

Those files live under `docs/examples/enron-master-agreement-public-context/` beside the saved workspace. The branch date is September 27, 2000, so the saved scene shows 5 financial checkpoints, 6 public-news items, 680 market rows, and a 30-event branch-local canonical timeline drawn from multiple source families. Use `make fetch-enron-full` when you want whole-history Enron search or a new run from the full corpus.

Refresh the repo-owned bundles, validate them, and refresh the screenshots before you report a change:

```bash
make enron-example
make enron-screens
```

## Enron business-outcome benchmark

The historical replay flow above is for one branch point and one saved comparison. The Enron benchmark is for repeated measurement across many branch points.

This benchmark answers a different question:

- given only the history before the branch point
- and one structured candidate action
- what later business-relevant email evidence becomes more or less likely

The benchmark keeps the older replay and ranked what-if flows intact. It adds a separate benchmark path for business-facing proxy outcomes:

- `enterprise_risk`
- `commercial_position_proxy`
- `org_strain_proxy`
- `stakeholder_trust`
- `execution_drag`

Those scores come from later email evidence that the archive can actually support:

- outside spread
- legal burden
- executive heat
- coordination load
- decision drag
- trust or repair language
- conflict heat
- artifact churn

All trained model families use the same boundary for this benchmark:

- pre-branch canonical event history
- structured candidate action only

The held-out Enron dossiers now include dated public financial checkpoints, public news items, and curated public-record events that were already known by the branch date. That same richer pre-branch history now feeds the training rows and the saved replay path.

Rebuilding this benchmark uses the fetched full Enron archive. The checked-in sample under `data/enron/rosetta` is for saved bundles, smoke checks, and repo-default source resolution. `VEI_WHATIF_ROSETTA_DIR` still overrides the full-data path when you want to point at a different archive.

The matched-input benchmark study now gives `jepa_latent` and `full_context_transformer` the same pre-branch event sequence, summary features, and action schema. That makes the main rerun a clean model comparison instead of a mixed input comparison.

### Benchmark commands

```bash
# Build the factual dataset and the held-out Enron case pack
vei whatif benchmark build \
  --rosetta-dir /path/to/rosetta \
  --artifacts-root _vei_out/whatif_benchmarks/branch_point_ranking_v2 \
  --label enron_business_outcome_public_context_20260412

# Train one model family
vei whatif benchmark train \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412 \
  --model-id jepa_latent

# Judge the held-out counterfactual cases from dossiers only
vei whatif benchmark judge \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412 \
  --model gpt-4.1-mini

# Evaluate the trained model against factual futures and judged rankings
vei whatif benchmark eval \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412 \
  --model-id jepa_latent \
  --judged-rankings-path _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412/judge_result.json \
  --audit-records-path /path/to/completed_audit_records.json

# Run the matched-input study across multiple models and seeds
vei whatif benchmark study \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412 \
  --label matched_input_public_context_20260412 \
  --model-id jepa_latent \
  --model-id full_context_transformer \
  --model-id treatment_transformer \
  --seed 42042 \
  --seed 42043 \
  --seed 42044 \
  --seed 42045 \
  --seed 42046 \
  --epochs 2
```

### What gets written

`vei whatif benchmark build` writes:

- factual train, validation, and test rows built from observed Enron futures
- a held-out Enron case pack
- one dossier per case and per business objective
- a judged-ranking template
- an audit template

`vei whatif benchmark judge` writes:

- `judge_result.json`
- `audit_queue.json`

`vei whatif benchmark eval` writes:

- factual forecasting metrics
- judged counterfactual ranking metrics
- audit coverage and agreement metrics
- rollout stress metrics only as a separate section

`vei whatif benchmark study` writes:

- one aggregate JSON result
- one Markdown overview
- one seeded run folder per model under `studies/<label>/runs/...`

### Multi-company world-model experiment

The pooled learned world-model path is `vei whatif benchmark build-multitenant`.
It accepts multiple normalized company-history snapshots and builds one dataset
with strict per-company time splits. A typical Enron + Dispatch + new-company
run is:

- train on earlier rows from every company
- validate on later but non-final rows for every company
- test on the final tail for every company
- generate held-out decision cases from final-tail branch points

Candidate actions are generated from the branch event and pre-branch history
only. The command writes the candidate prompt, generation model, pre-branch
evidence hash, and `no_future_context=true` metadata for every generated
candidate. It also writes a leakage report that checks train/heldout thread and
event separation and checks that generated candidate prompts and judge dossiers
do not contain recorded future-tail event markers.

The default `template` candidate mode is deterministic and CI-safe. Live LLM
generation is available as an explicit opt-in with `--candidate-mode llm`.
Ordinary API-available models use the direct API path. Codex-session models such
as `gpt-5.3-codex-spark` route through Codex instead of provider API keys.

```bash
vei whatif benchmark build-multitenant \
  --input enron=/path/to/enron/context_snapshot.json \
  --input dispatch=/path/to/dispatch/context_snapshot.json \
  --input powrofyou=/path/to/powrofyou/context_snapshot.json \
  --artifacts-root _vei_out/world_model_multitenant_jepa \
  --label enron_dispatch_powrofyou \
  --heldout-cases-per-tenant 4 \
  --future-horizon-events 12 \
  --max-branch-rows-per-thread 24 \
  --candidate-mode template

vei whatif benchmark train \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch_powrofyou \
  --model-id jepa_latent \
  --epochs 10 \
  --batch-size 128 \
  --train-split train \
  --train-split validation \
  --validation-split test

vei whatif benchmark eval \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch_powrofyou \
  --model-id jepa_latent

# Optional factual comparator under the same split
vei whatif benchmark train \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch_powrofyou \
  --model-id heuristic_baseline \
  --train-split train \
  --train-split validation \
  --validation-split test

vei whatif benchmark eval \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch_powrofyou \
  --model-id heuristic_baseline
```

Point a Dispatch what-if at the pooled checkpoint through the existing reference backend boundary:

```bash
VEI_REFERENCE_BACKEND_CHECKPOINT=_vei_out/world_model_multitenant_jepa/enron_dispatch_powrofyou/model_runs/jepa_latent/model.pt \
  vei whatif experiment \
    --source company_history \
    --source-dir /path/to/dispatch/context_snapshot.json \
    --label dispatch_reference_forecast \
    --forecast-backend reference \
    --counterfactual-prompt "Route the incident to an accountable owner, hold broad sends, and send one controlled status update."
```

Treat this as an offline artifact-backed experiment, not a production-proven universal CEO recommender. The strongest evidence remains factual held-out forecasting. Counterfactual rankings are decision-support signals until they are backed by human audit, expert review, or natural-experiment evidence.

### Critical-decision counterfactual runs

`vei whatif benchmark critical-decisions` applies the trained pooled checkpoint
to the kind of decision grid a CEO or manager can inspect. It is deliberately
separate from training:

- select critical branch points with deterministic pre-branch-only scoring
- optionally restrict the pool to an existing benchmark's `test` and `heldout` split
- generate 8-12 concrete counterfactual actions per decision
- save the candidate prompt, raw response, candidate type, generation model, and pre-branch evidence hash
- check that prompts, generated candidate text, and judge dossiers do not contain future-tail markers
- score the candidates through the normal JEPA benchmark boundary
- write `critical_decision_scores.csv` and `critical_decision_scores.md`

```bash
vei whatif benchmark critical-decisions \
  --input dispatch=/path/to/dispatch/context_snapshot.json \
  --input newco=/path/to/newco/context_snapshot.json \
  --source-build-root _vei_out/world_model_multitenant_jepa/enron_dispatch_newco \
  --checkpoint _vei_out/world_model_multitenant_jepa/enron_dispatch_newco/model_runs/jepa_latent/model.pt \
  --artifacts-root _vei_out/world_model_critical_decisions \
  --label dispatch_newco_critical \
  --cases-per-tenant 4 \
  --candidates-per-decision 10 \
  --candidate-mode template
```

The selection score is not a learned outcome label and does not use the future tail. It is a repeatable way to choose promising decision points from branch-time evidence: external scope, risk/governance terms, customer or commercial terms, product/delivery terms, coordination complexity, urgency/escalation, conflict/delay, and evidence pressure. JEPA then scores candidate actions from the pre-branch state.

For local exploratory runs, keep the current shareable MD/CSV exports under
`_vei_out/world_model_current/`. The lower-level benchmark, checkpoint, and
critical-decision folders are provenance and rerun material. Use
`make clean-workspace` for cache/build cleanup that leaves `_vei_out/` in place.
Use `make clean-workspace-hard` to prune old generated runs while preserving
`_vei_out/world_model_current/`, `_vei_out/datasets/`, and
`_vei_out/llm_live/latest/`.

### Current model state

The fresh-clone learned path is the shipped `full_context_transformer`
reference backend under `data/enron/reference_backend/`. It reports factual
next-event AUROC `0.787817`, Brier `0.332025`, and calibration ECE `0.373951`
on the held-out Enron validation split.

The latest local pooled JEPA run combined Enron, Dispatch, and a private startup
archive. It canonicalized `59,920` events, built `17,602` eligible branch rows,
trained on `14,655` train/validation rows, and tested on `2,641` held-out rows.
Against the heuristic baseline:

- external-spread calibration improved sharply: Brier `0.00135` vs `0.54745`, ECE `0.00567` vs `0.68841`
- all five business-head MAEs improved: enterprise risk, commercial position, org strain, stakeholder trust, and execution drag
- AUROC on the rare external-spread label was lower than the heuristic (`0.89231` vs `0.92389`), so the result is not "better on every metric"

The latest local critical-decision run selected 12 decisions and scored 120
candidate actions. The latest live run used structured Codex generation with
`gpt-5.3-codex-spark` and produced all 12 candidate sets through the LLM path
with no template fallback. Leakage checks passed for train/test separation and
for future-tail exclusion from prompts, generated candidates, and judge
dossiers. Those rankings are useful decision-support outputs, not causal proof
of what would definitely have happened.

### Important constraint

This benchmark stays honest about the source data. Enron email can support business proxies. It does not support true profit ground truth or true HR outcome ground truth.
