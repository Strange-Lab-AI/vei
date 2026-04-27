## VEI
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Strange-Lab-AI/vei)

VEI turns built-in scenarios or real company records into a runnable company world. You can use it to test an agent before it touches a real company, watch an outside agent through a governed twin, branch from a real historical decision and compare a different move, or draft grounded knowledge artifacts from the same company state.

The same engine powers every path: one world state, one event history, one replay model, and one CLI.

## Contents

- [Quick Start](#quick-start)
- [Pick Your Entry Point](#pick-your-entry-point)
- [How VEI Works](#how-vei-works)
- [Walk Through The Enron Case](#walk-through-the-enron-case)
- [Knowledge Authoring Demo](#knowledge-authoring-demo)
- [Bring Your Own Company History](#bring-your-own-company-history)
- [Repo Checks](#repo-checks)
- [Docs](#docs)

## Quick Start

```bash
git clone https://github.com/Strange-Lab-AI/vei.git
cd vei
make setup
vei doctor
vei quickstart run
```

`vei quickstart run` gives you a ready world to inspect:

- Studio on `http://127.0.0.1:3011`
- Twin Gateway on `http://127.0.0.1:3012`
- a seeded workspace with visible activity already in motion
- connection details in `.vei/quickstart.json`

Use these next:

```bash
vei twin status --root <workspace-root>
vei project show --root <workspace-root>
vei eval benchmark --runner workflow --family security_containment
```

What you need:

- Python `3.11`
- a local virtual environment, which `make setup` creates at `.venv`
- ports `3011` and `3012` available
- `OPENAI_API_KEY` in `.env` only when you want live LLM runs

For live planning backends, VEI supports OpenAI, Anthropic, Google, OpenRouter, and local Codex CLI depending on your local auth and provider setup.

## Pick Your Entry Point

- See the product: `vei quickstart run`
- Connect an outside agent: start with quickstart, then use the Twin Gateway URLs and token from `.vei/quickstart.json`
- Replay a real historical decision: `vei ui serve --root docs/examples/enron-master-agreement-public-context/workspace --host 127.0.0.1 --port 3055`
- Run a benchmark: `vei eval benchmark --runner workflow --family security_containment`

## How VEI Works

VEI has three top-level paths.

The runnable company path starts from a built-in world or a captured company snapshot. VEI compiles that into one deterministic world session with connected surfaces such as mail, chat, tickets, docs, CRM, identity, and knowledge assets. Agents and humans act through VEI tools and routes. VEI records what happened, scores the run, and lets you replay or branch it.

The governed twin path supports three write outcomes: allow, deny, or hold for approval. Workspace governor config can carry typed `approval_rules` for selected surfaces or tools, so approval holds live in normal workspace policy instead of one-off event payloads.

The agent-facing discovery ladder inside that world is now explicit: start with `vei.orientation`, then `vei.structure_view`, then `vei.capability_graphs`, `vei.graph_plan`, and `vei.graph_action`. `vei.structure_view` shows the event-derived read model with inferred entities, case clusters, timelines, and open ambiguities. Hidden truth comparison stays in the SDK, contract, and benchmark layers instead of the MCP tool surface.

The knowledge authoring path rides on that same world. VEI hydrates notes, transcripts, metric snapshots, SOPs, pricing sheets, and deliverables into one `knowledge_graph`, then composes proposals or briefs with citations, freshness checks, and contract scoring. The deterministic baseline runs without an API key. The bounded LLM mode uses the same recorded event spine and the same workspace/run model.

The historical what-if path starts from one normalized company history bundle. The outer layer is `context_snapshot.json`. It keeps the raw sources parallel as typed records, with provider health, timestamps, actors, cases, and linked records. VEI explores that bundle, ranks branch candidates, and picks one real decision point.

The same capture now writes a canonical timeline beside the snapshot: `canonical_events.jsonl` plus `canonical_event_index.json`. That shared spine is what the company-history loader and the Studio timeline panel read first when those files are present.

The saved what-if workspace is the inner layer. It lives under `workspace/` and is anchored by `episode_manifest.json`. That workspace contains the chosen branch event, the earlier history, the recorded future, and the saved files VEI uses to replay and compare alternate moves.

Public-company facts live beside the bundle in `whatif_public_context.json` when they are available. VEI slices those facts to what was already known by the branch date, then carries that slice into the saved workspace, the comparison run, and the business readout.

## Walk Through The Enron Case

The repo now ships the Enron what-if surface in two parts. The checkout carries a small checked-in Rosetta sample under `data/enron/rosetta/`, the public-company fixtures, the curated public-record fixture, and eight saved Studio bundles under `docs/examples/`. The full Enron archive is an optional download fetched with `make fetch-enron-full`.

The repo-owned Enron public context now carries 11 dated financial checkpoints, 21 dated public news events, 986 daily stock rows, 7 credit events, and 1 FERC timeline event across 24 archived public source files. The saved Enron workspaces now also carry a richer branch-local timeline that blends mail with dated filings, disclosures, hearings, exhibits, and market records through the same canonical ledger.

Fetch the full archive when you want full-data search, training, or archive validation:

```bash
make fetch-enron-full
```

Verify that full archive after it is fetched:

```bash
python scripts/check_rosetta_archive.py
```

Install the learned runtime when you want the saved Enron bundles to open with the shipped reference forecast from a fresh clone:

```bash
pip install -e ".[worldmodel,llm,ui,browser]"
```

Install the optional JEPA path when you want to run the second backend from the same clone:

```bash
pip install -e ".[jepa]"
```

Open it in Studio:

```bash
vei ui serve \
  --root docs/examples/enron-master-agreement-public-context/workspace \
  --host 127.0.0.1 \
  --port 3055
```

Open `http://127.0.0.1:3055`.

This saved Master Agreement workspace is still the simplest Enron walkthrough in the repo. The branch date is September 27, 2000, so the public slice only shows the facts that were already public by then: 5 financial checkpoints, 6 public news events, and 680 daily stock rows. The saved branch timeline itself now carries 30 prior canonical events from multiple source families, so the branch scene reads as a company timeline rather than a single mail thread.

![Decision scene for the Enron Master Agreement branch point, showing the branch moment, what actually happened, public company context, and recorded business state](docs/assets/enron-whatif/enron-decision-scene-top.png)

The public-company panel now comes from the repo-owned v2 fixture rather than a side checkout:

![Public company context sliced to the branch date, showing financial checkpoints and public news known before September 27, 2000](docs/assets/enron-whatif/enron-public-context.png)

The saved counterfactual keeps the draft inside Enron, asks Gerald Nemec and Sara Shackleton for review, and holds the outside send. The saved forecast keeps the same 84-event horizon, moves risk from `1.000` to `0.560`, and predicts `64` fewer outside-addressed sends.

![Predicted business change comparing the historical baseline, the LLM alternate path, and the learned forecast](docs/assets/enron-whatif/enron-predicted-business-change.png)

VEI now also shows a macro outcome panel for the saved Enron bundles. It carries short-horizon stock, credit, and FERC heads, plus the measured calibration note. The current calibration is weak, so these macro heads stay advisory beside the email-path evidence.

![Macro outcome panel for the saved Master Agreement branch, showing stock, credit, and FERC heads with the measured calibration note](docs/assets/enron-whatif/enron-macro-outcomes.png)

The ranked comparison turns the same branch into a business choice. `Hold for internal review` ranks first at `0.209`, `Send a narrow status note` ranks second at `0.208`, and `Push for fast turnaround` falls to `-0.019`.

![Ranked business comparison of three candidate moves scored against the recorded future](docs/assets/enron-whatif/enron-ranked-comparison.png)

The repo now ships eight saved Enron examples. Each one carries at least 30 prior canonical events and at least three real source families in the saved timeline.

Proof examples:

- `enron-master-agreement-public-context`: contract control with a long visible downstream tail
- `enron-pge-power-deal`: commercial judgment under counterparty deterioration
- `enron-california-crisis-strategy`: regulatory and trading pressure under a preservation order
- `enron-baxter-press-release`: public communications under executive shock
- `enron-braveheart-forward`: accounting and structure review inside a finance loop

Narrative examples:

- `enron-watkins-follow-up`: the strongest governance fork, with a thinner recorded tail
- `enron-q3-disclosure-review`: disclosure choices inside the October 2001 crisis
- `enron-skilling-resignation-materials`: executive messaging and trust under leadership change

The repo-owned Enron data chain, saved examples, and benchmark notes are
documented in [docs/ENRON_EXAMPLE.md](docs/ENRON_EXAMPLE.md).

Train the repo-local reference backend when you want the Enron path to use the learned forecast by default:

```bash
python scripts/train_reference_backend_on_enron.py
```

The shipped reference checkpoint currently reports factual next-event AUROC `0.787817`, Brier `0.332025`, and calibration ECE `0.373951` on the held-out Enron validation split. That is the honest baseline for the thicker Enron timeline that now ships in the repo.

Useful files in the Master Agreement example:

- [workspace](docs/examples/enron-master-agreement-public-context/workspace/)
- [whatif_experiment_overview.md](docs/examples/enron-master-agreement-public-context/whatif_experiment_overview.md)
- [whatif_experiment_result.json](docs/examples/enron-master-agreement-public-context/whatif_experiment_result.json)
- [whatif_business_state_comparison.md](docs/examples/enron-master-agreement-public-context/whatif_business_state_comparison.md)

Refresh the bundles and screenshots:

```bash
make enron-example
make enron-screens
```

Inspect a new company-history bundle through the same file-backed timeline path:

```bash
vei context timeline --root /path/to/context_snapshot.json --limit 25
vei context readiness --root /path/to/context_snapshot.json --format plain
python scripts/check_tenant_world_model.py --root /path/to/context_snapshot.json
```

Build a local real-company example from offline exports through the same path:

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

For a local Dispatch export stored outside the repo, run:

```bash
python scripts/build_dispatch_local_example.py
```

## Synthetic Clearwater Rig

Clearwater stays in the repo as a synthetic control-room rig. It is the right place to test the kernel, the governor flow, and replay tooling without bringing in outside company data. Use Enron when you want the flagship real-history learned path.

The repo now also ships three saved Clearwater what-if bundles so the same file-backed timeline and saved forecast flow can be checked across multiple synthetic service-ops cases:

- `clearwater-dispatch-recovery`
- `clearwater-billing-dispute-reopened`
- `clearwater-technician-no-show`

Spin up the built-in `service_ops` workspace with governor mode and run the same what-if loop:

```bash
vei quickstart run --world service_ops --governor-demo --no-serve
vei whatif export --workspace _vei_out/quickstart
vei whatif events --source company_history \
  --source-dir _vei_out/quickstart/context_snapshot.json
vei whatif experiment --source company_history \
  --source-dir _vei_out/quickstart/context_snapshot.json \
  --artifacts-root _vei_out/dispatch_whatif \
  --label dispatch_t1 \
  --thread-id "jira:JRA-CFS-10" \
  --event-id "jira:JRA-CFS-10:state" \
  --counterfactual-prompt "What if dispatch had escalated to a regional supervisor and pre-authorized after-hours premium?" \
  --mode heuristic_baseline
```

`--mode heuristic_baseline` runs without any LLM key. Add `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, or `OPENROUTER_API_KEY` to `.env` and use `--mode both` for LLM-driven counterfactual continuations alongside the forecast.

Open one of the saved synthetic bundles directly:

```bash
vei ui serve \
  --root docs/examples/clearwater-dispatch-recovery/workspace \
  --host 127.0.0.1 \
  --port 3056
```

Rebuild the tracked synthetic bundles:

```bash
make service-ops-example
```

Use the saved Clearwater bundle as the synthetic service-ops example. Use Enron
for the flagship real-history example.

## Knowledge Authoring Demo

The built-in `knowledge_authoring` family turns the Northstar Growth world into a grounded proposal-drafting workspace. It is a benchmark family built on the same Northstar pack used for campaign operations, with transcripts, pricing, delivery metrics, SOPs, and planning notes seeded into the normal VEI event spine.

Run-based authoring export uses a recorded VEI run:

```bash
vei project init \
  --root _vei_out/knowledge_authoring \
  --family knowledge_authoring \
  --overwrite

vei run start \
  --root _vei_out/knowledge_authoring \
  --runner workflow

vei synthesize training-data \
  --root _vei_out/knowledge_authoring \
  --run-id <run_id> \
  --format authoring
```

Standalone workspace compose writes a fresh artifact into the workspace knowledge snapshot. It updates workspace knowledge state for future compose calls and future runs. It does not attach itself to an existing run timeline.

```bash
vei knowledge compose \
  --workspace _vei_out/knowledge_authoring \
  --target proposal \
  --template proposal_v1 \
  --subject crm_deal:CRM-NSG-D1 \
  --mode heuristic_baseline \
  --write-back
```

`--mode heuristic_baseline` is fully deterministic and runs without any API key. `--mode llm` uses the same bounded composition path with `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GOOGLE_API_KEY` or `GEMINI_API_KEY`, or `OPENROUTER_API_KEY`. When the selected key is missing, the compose command falls back to `heuristic_baseline` and records that note in the result.

To ingest offline exports from real knowledge systems, use the new offline-first connectors. `notion`, `linear`, and `granola` read local exports only:

```bash
vei knowledge ingest \
  --provider notion \
  --provider linear \
  --provider granola \
  --source notion=/path/to/notion_export \
  --source linear=/path/to/linear_dump.json \
  --source granola=/path/to/granola_notes \
  --org "Northstar Growth" \
  --output _vei_out/knowledge_snapshot.json
```

## Bring Your Own Company History

Bring raw exports into VEI as one verified bundle before you run what-if work.

```bash
vei context normalize \
  --source-dir <raw_input_path> \
  --org "<name>" \
  --domain "<domain>" \
  --output _vei_out/<company>/context_snapshot.json

vei context verify --snapshot _vei_out/<company>/context_snapshot.json
vei context status --snapshot _vei_out/<company>/context_snapshot.json

vei whatif explore \
  --source-dir _vei_out/<company>/context_snapshot.json \
  --format markdown

vei whatif candidates \
  --source-dir _vei_out/<company>/context_snapshot.json \
  --limit 10 \
  --format markdown
```

For live multi-surface onboarding, use the twin entrypoint so capture, twin build, canonical timeline files, and the readiness readout land together:

```bash
vei twin onboard \
  --root _vei_out/<company>/twin \
  --org "<name>" \
  --domain "<domain>" \
  --provider github \
  --provider clickup \
  --filter github:repo=<org>/<repo> \
  --filter clickup:list_id=<list_id>
```

When you find a real branch point, open a saved workspace and run a comparison:

```bash
vei whatif open \
  --source-dir _vei_out/<company>/context_snapshot.json \
  --root _vei_out/<company>/whatif_case \
  --event-id <branch_event_id>

vei whatif experiment \
  --source-dir _vei_out/<company>/context_snapshot.json \
  --artifacts-root _vei_out/<company>/whatif_runs \
  --label internal_review \
  --event-id <branch_event_id> \
  --counterfactual-prompt "Keep the draft inside the company, route it through one more internal review, and hold the outside send."
```

The canonical files are:

- `context_snapshot.json` for the normalized company history bundle
- `canonical_events.jsonl` for the shared event spine
- `canonical_event_index.json` for the searchable event index with case and stitch metadata
- `episode_manifest.json` for the saved what-if workspace manifest
- `whatif_public_context.json` for the saved public-context sidecar written into every saved what-if workspace

### Learned world-model benchmarks

The learned path starts from the same canonical timeline files as the rest of
VEI. It turns emails, tickets, ClickUp items, docs, and other dated work records
into one event spine, then builds training rows that look like this:

```text
pre-branch company state + doctrine text + candidate action text/schema
-> observed future state
```

For factual rows, the candidate action is the historical branch event and the
target is what actually happened next. For decision cases, candidate actions are
created from pre-branch context only, then scored by the trained model. The
model is never supposed to see the recorded future when proposing or scoring
those candidates.

The JEPA benchmark model does not predict one magic number directly. It predicts
a bundle of factual future heads: evidence flow, business risk, stakeholder
trust, execution drag, governance pressure, and related signals. Any single
number in ranking tables is a convenience view computed after prediction, not a
learned universal preference label.

The current default readout is named `balanced_operator_score`. It is a fixed
operator lens over five predicted heads:

```text
mean(1-risk, commercial position, 1-strain, trust, 1-drag)
```

Use that score as a sorting aid, not as proof that the JEPA learned a universal
CEO preference. The more important output is the predicted future vector, its
delta versus the baseline action, and the Pareto/tradeoff set.

The current score flow is:

```text
candidate action
+ doctrine text
+ pre-branch company state
-> JEPA predicts likely future heads
-> Pareto/tradeoff report, with optional operator objective views
```

The predicted heads include:

- evidence heads such as outside spread, legal follow-up, review loops, participant fanout, delays, reassurance, blame pressure, and commitment clarity
- business heads: `enterprise_risk`, `commercial_position_proxy`, `org_strain_proxy`, `stakeholder_trust`, `execution_drag`
- future-state heads: `regulatory_exposure`, `accounting_control_pressure`, `liquidity_stress`, `governance_response`, `evidence_control`, `external_confidence_pressure`
- optional reporting views such as risk-minimization or trust-preservation, computed from predicted futures rather than trained as factual targets

The latest local pooled action-conditioned JEPA run combined the Enron Rosetta
sample, Dispatch, Powr of You, and a small AmericanStories historical-news
sample. It trained on `4,206` rows, validated on `785`, tested on `965`, and
kept `12` final held-out cases. It uses deterministic hashing encoders for
doctrine text and raw candidate action text, so changing the action text while
holding the structured action schema fixed can change the prediction. On the
held-out factual rows, external-spread Brier was `0.001085` and ECE was
`0.003312`; AUROC is not informative in that particular split because the test
label is nearly all positive. Business-head MAEs were `0.055` enterprise risk,
`0.061` commercial position, `0.037` org strain, `0.058` stakeholder trust, and
`0.099` execution drag. The honest read is: JEPA is a useful factual
future-state forecaster in this run, but counterfactual rankings remain
decision support, not causal proof.

Build a pooled benchmark from multiple company-history bundles:

```bash
vei whatif benchmark build-multitenant \
  --input enron=_vei_out/enron/context_snapshot.json \
  --input dispatch=_vei_out/dispatch/context_snapshot.json \
  --input newco=_vei_out/newco/context_snapshot.json \
  --artifacts-root _vei_out/world_model_multitenant_jepa \
  --label enron_dispatch_newco \
  --candidate-mode template
```

Train JEPA on earlier history and keep the final tail for proof:

```bash
vei whatif benchmark train \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch_newco \
  --model-id jepa_latent \
  --train-split train \
  --train-split validation \
  --validation-split test

vei whatif benchmark eval \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch_newco \
  --model-id jepa_latent
```

The builder writes a leakage report and a data provenance report beside the dataset. Generated private company artifacts stay under `_vei_out/` and are not committed.

Run the user-facing strategic state-point counterfactual pass against a trained checkpoint:

```bash
vei whatif benchmark strategic-state-points \
  --input dispatch=_vei_out/dispatch/context_snapshot.json \
  --input newco=_vei_out/newco/context_snapshot.json \
  --checkpoint _vei_out/world_model_multitenant_jepa/enron_dispatch_newco/model_runs/jepa_latent/model.pt \
  --artifacts-root _vei_out/world_model_strategic_state_points \
  --label dispatch_newco_strategy \
  --decisions-per-tenant 3 \
  --candidates-per-decision 8 \
  --proposal-mode llm \
  --proposal-model gpt-5.4
```

That command is the reproducible CEO-decision workflow: build an as-of state
dossier, propose strategic decision points and broad candidate actions from
pre-as-of evidence, save prompts/responses/evidence hashes, score every
candidate with the JEPA checkpoint, and export CSV/Markdown ranking tables.
Strategic state points are now the counterfactual product surface.

Use `--proposal-mode llm` for live LLM-generated decisions/actions. Strategic
proposal models route through Codex by default. The default is `gpt-5.4`,
because that is the newest model accepted by the current local Codex CLI.
Override `--proposal-model` to `gpt-5.5` when the installed Codex runtime
supports it. Set `VEI_STRATEGIC_PROPOSAL_BACKEND=api` only for an explicit
direct-provider API run.

## Repo Checks

```bash
make check
make test
make check-full
make test-full
make llm-live
make clean-workspace
make clean-workspace-hard-dry-run
make clean-workspace-hard
```

`make check` and `make test` are the fast local loop. `make test` skips tests marked `slow`. `make check-full` and `make test-full` match the stricter CI path with whole-repo security scans, the full slow suite, and coverage. `make llm-live` needs live keys. `make clean-workspace` clears low-risk local clutter such as repo-root `.artifacts/`, build folders, caches, and bytecode. It leaves `_vei_out/` runs alone. Use `make clean-workspace-hard-dry-run` to preview pruning old `_vei_out/` runs; `make clean-workspace-hard` keeps `_vei_out/datasets/`, `_vei_out/world_model_current/`, and `_vei_out/llm_live/latest/` when those are present.

## Docs

- [docs/AGENT_ONBOARDING.md](docs/AGENT_ONBOARDING.md) for a fast repo briefing, command checklist, and module-boundary rules
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the module map, capability domains, and knowledge layer
- [docs/WHATIF.md](docs/WHATIF.md) for the world-model and what-if command reference
- [docs/ENRON_EXAMPLE.md](docs/ENRON_EXAMPLE.md) for the repo-owned public company example
- [docs/NEWS_EXAMPLE.md](docs/NEWS_EXAMPLE.md) for the public news-timeline example
- [docs/RL_GYM.md](docs/RL_GYM.md) for the scoped RL-training plan over deterministic process contracts
- [docs/examples/clearwater-dispatch-recovery/README.md](docs/examples/clearwater-dispatch-recovery/README.md) for a repo-owned synthetic Clearwater example

## License

Business Source License 1.1. See [LICENSE](LICENSE). Change date: `2030-03-10`. Change license: `GPL-2.0-or-later`.
