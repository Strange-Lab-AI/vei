## VEI
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/strangeloopcanon/vei)

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
git clone https://github.com/strangeloopcanon/vei.git
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

## Pick Your Entry Point

- See the product: `vei quickstart run`
- Connect an outside agent: start with quickstart, then use the Twin Gateway URLs and token from `.vei/quickstart.json`
- Replay a real historical decision: `vei ui serve --root docs/examples/enron-master-agreement-public-context/workspace --host 127.0.0.1 --port 3055`
- Run a benchmark: `vei eval benchmark --runner workflow --family security_containment`

## How VEI Works

VEI has three top-level paths.

The runnable company path starts from a built-in world or a captured company snapshot. VEI compiles that into one deterministic world session with connected surfaces such as mail, chat, tickets, docs, CRM, identity, and knowledge assets. Agents and humans act through VEI tools and routes. VEI records what happened, scores the run, and lets you replay or branch it.

The knowledge authoring path rides on that same world. VEI hydrates notes, transcripts, metric snapshots, SOPs, pricing sheets, and deliverables into one `knowledge_graph`, then composes proposals or briefs with citations, freshness checks, and contract scoring. The deterministic baseline runs without an API key. The bounded LLM mode uses the same recorded event spine and the same workspace/run model.

The historical what-if path starts from one normalized company history bundle. The outer layer is `context_snapshot.json`. It keeps the raw sources parallel as typed records, with provider health, timestamps, actors, cases, and linked records. VEI explores that bundle, ranks branch candidates, and picks one real decision point.

The saved what-if workspace is the inner layer. It lives under `workspace/` and is anchored by `episode_manifest.json`. That workspace contains the chosen branch event, the earlier history, the recorded future, and the saved files VEI uses to replay and compare alternate moves.

Public-company facts live beside the bundle in `whatif_public_context.json` when they are available. VEI slices those facts to what was already known by the branch date, then carries that slice into the saved workspace, the comparison run, and the business readout.

## Walk Through The Enron Case

The repo now ships the whole Enron what-if chain under `data/enron/` and `docs/examples/`. That includes the raw mail tar, the normalized source parquet, the repo-local Rosetta parquet archive, the public-company context fixtures, and four saved Studio bundles.

The repo-owned Enron public context now carries 11 dated financial checkpoints, 21 dated public news events, 986 daily stock rows, 7 credit events, and 1 FERC timeline event across 24 archived public source files.

Verify the vendored archive before you start:

```bash
python scripts/check_rosetta_archive.py
```

Open it in Studio:

```bash
vei ui serve \
  --root docs/examples/enron-master-agreement-public-context/workspace \
  --host 127.0.0.1 \
  --port 3055
```

Open `http://127.0.0.1:3055`.

This saved Master Agreement workspace is still the simplest Enron walkthrough in the repo. The branch date is September 27, 2000, so the public slice only shows the facts that were already public by then: 5 financial checkpoints, 6 public news events, and 680 daily stock rows. Credit and FERC rows stay empty at that date because those later signals had not landed yet.

![Decision scene for the Enron Master Agreement branch point, showing the branch moment, what actually happened, public company context, and recorded business state](docs/assets/enron-whatif/enron-decision-scene-top.png)

The public-company panel now comes from the repo-owned v2 fixture rather than a side checkout:

![Public company context sliced to the branch date, showing financial checkpoints and public news known before September 27, 2000](docs/assets/enron-whatif/enron-public-context.png)

The saved counterfactual keeps the draft inside Enron, asks Gerald Nemec and Sara Shackleton for review, and holds the outside send. The saved forecast keeps the same 84-event horizon, moves risk from `1.000` to `0.560`, and predicts `64` fewer outside-addressed sends.

![Predicted business change comparing the historical baseline, the LLM alternate path, and the learned forecast](docs/assets/enron-whatif/enron-predicted-business-change.png)

VEI now also shows a macro outcome panel for the saved Enron bundles. It carries short-horizon stock, credit, and FERC heads, plus the measured calibration note. The current calibration is weak, so these macro heads stay advisory beside the email-path evidence.

![Macro outcome panel for the saved Master Agreement branch, showing stock, credit, and FERC heads with the measured calibration note](docs/assets/enron-whatif/enron-macro-outcomes.png)

The ranked comparison turns the same branch into a business choice. `Hold for internal review` ranks first at `0.209`, `Send a narrow status note` ranks second at `0.208`, and `Push for fast turnaround` falls to `-0.019`.

![Ranked business comparison of three candidate moves scored against the recorded future](docs/assets/enron-whatif/enron-ranked-comparison.png)

The repo now ships four saved Enron examples:

- `enron-master-agreement-public-context`: procedural legal review inside the wider company arc
- `enron-watkins-follow-up`: the October 30 Watkins follow-up note that restates the questions she says she raised to Lay
- `enron-california-crisis-strategy`: California trading conduct inside the refund and FERC timeline
- `enron-pge-power-deal`: commercial legal handling against counterparty deterioration

The repo-owned Enron data chain is documented in [docs/ROSETTA_SOURCE.md](docs/ROSETTA_SOURCE.md).

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

## Live Dispatch demo (Clearwater Field Services)

Spin up the built-in `service_ops` workspace with governor mode and run the same what-if loop without any external data:

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

See [docs/SERVICE_OPS_WALKTHROUGH.md](docs/SERVICE_OPS_WALKTHROUGH.md) for the full Studio walkthrough.

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
- `episode_manifest.json` for the saved what-if workspace manifest
- `whatif_public_context.json` for the saved public-context sidecar written into every saved what-if workspace

## Repo Checks

```bash
make check
make test
make check-full
make test-full
make llm-live
make clean-workspace
```

`make check` and `make test` are the fast local loop. `make test` skips tests marked `slow`. `make check-full` and `make test-full` match the stricter CI path with whole-repo security scans, the full slow suite, and coverage. `make llm-live` needs live keys. `make clean-workspace` clears local generated clutter such as `_vei_out/` runs, repo-root `.artifacts/`, build folders, caches, and bytecode. It keeps `_vei_out/datasets/` and `_vei_out/llm_live/latest/` when those are present. Use `make clean-workspace-dry-run` when you want to preview the delete list.

## Docs

- [docs/AGENT_ONBOARDING.md](docs/AGENT_ONBOARDING.md) for a fast repo briefing, command checklist, and module-boundary rules
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the module map, capability domains, and knowledge layer
- [docs/WHATIF.md](docs/WHATIF.md) for the historical replay and comparison flow
- [docs/OVERVIEW.md](docs/OVERVIEW.md) for the product framing, including the knowledge brain layer
- [docs/examples/enron-master-agreement-public-context/README.md](docs/examples/enron-master-agreement-public-context/README.md) for the repo-owned Enron example
- [docs/SERVICE_OPS_WALKTHROUGH.md](docs/SERVICE_OPS_WALKTHROUGH.md) for the Studio and control-room path

## License

Business Source License 1.1. See [LICENSE](LICENSE). Change date: `2030-03-10`. Change license: `GPL-2.0-or-later`.
