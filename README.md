## VEI
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/strangeloopcanon/vei)

VEI turns built-in scenarios or real company records into a runnable company world. You can use it to test an agent before it touches a real company, watch an outside agent through a governed twin, or branch from a real historical decision and compare a different move.

The same engine powers every path: one world state, one event history, one replay model, and one CLI.

## Contents

- [Quick Start](#quick-start)
- [Pick Your Entry Point](#pick-your-entry-point)
- [How VEI Works](#how-vei-works)
- [Walk Through The Enron Case](#walk-through-the-enron-case)
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

VEI has two top-level paths.

The runnable company path starts from a built-in world or a captured company snapshot. VEI compiles that into one deterministic world session with connected surfaces such as mail, chat, tickets, docs, CRM, and identity. Agents and humans act through VEI tools and routes. VEI records what happened, scores the run, and lets you replay or branch it.

The historical what-if path starts from one normalized company history bundle. The outer layer is `context_snapshot.json`. It keeps the raw sources parallel as typed records, with provider health, timestamps, actors, cases, and linked records. VEI explores that bundle, ranks branch candidates, and picks one real decision point.

The saved what-if workspace is the inner layer. It lives under `workspace/` and is anchored by `episode_manifest.json`. That workspace contains the chosen branch event, the earlier history, the recorded future, and the saved files VEI uses to replay and compare alternate moves.

Public-company facts live beside the bundle in `whatif_public_context.json` when they are available. VEI slices those facts to what was already known by the branch date, then carries that slice into the saved workspace, the comparison run, and the business readout.

## Walk Through The Enron Case

The repo ships a real saved Enron example under `docs/examples/enron-master-agreement-public-context/`. It combines a real Enron email branch point with a dated public-company context pack built from financial checkpoints and public news sources. This specific branch date only shows the facts that were already public on September 27, 2000. Later Enron branch dates automatically pick up the later public news items too.

Open it in Studio:

```bash
vei ui serve \
  --root docs/examples/enron-master-agreement-public-context/workspace \
  --host 127.0.0.1 \
  --port 3055
```

Open `http://127.0.0.1:3055`.

![Enron historical what-if flow](docs/assets/enron-whatif/enron-whatif-flow.gif)

![Enron decision scene with dated public context](docs/assets/enron-whatif/enron-decision-scene-top.png)

Here is the exact story this example shows:

1. VEI loads a real Enron branch point from the `Master Agreement` thread.
2. The saved world contains 6 prior messages and 84 recorded future events on that case.
3. The branch date is September 27, 2000, so the public-company slice shows the facts that were already public by that date: 2 financial checkpoints and 0 public news items.
4. The real move is Debra Perlingiere sending a draft agreement outside Enron to `kathy_gerken@cargill.com`.
5. The alternate move keeps the draft inside Enron, asks Gerald Nemec and Sara Shackleton for review, and holds the outside send.
6. The saved continuation stays internal. The saved forecast keeps the same 84-event horizon, moves risk from `1.000` to `0.983`, and predicts `29` fewer outside sends.
7. The ranked comparison turns that into a business choice: `Hold for internal review` ranks first at `0.351`, `Send a narrow status note` ranks second at `0.155`, and `Push for fast turnaround` falls to `-0.019`.

![Enron predicted business change](docs/assets/enron-whatif/enron-predicted-business-change.png)

![Enron ranked business comparison](docs/assets/enron-whatif/enron-ranked-comparison.png)

Useful files in that example:

- [workspace](docs/examples/enron-master-agreement-public-context/workspace/)
- [whatif_experiment_overview.md](docs/examples/enron-master-agreement-public-context/whatif_experiment_overview.md)
- [whatif_experiment_result.json](docs/examples/enron-master-agreement-public-context/whatif_experiment_result.json)
- [whatif_business_state_comparison.md](docs/examples/enron-master-agreement-public-context/whatif_business_state_comparison.md)

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
- `whatif_public_context.json` for optional public-company context

## Repo Checks

```bash
make check
make test
make llm-live
```

`make llm-live` needs live keys. The other two are the normal local gates.

## Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the module map and data flow
- [docs/WHATIF.md](docs/WHATIF.md) for the historical replay and comparison flow
- [docs/examples/enron-master-agreement-public-context/README.md](docs/examples/enron-master-agreement-public-context/README.md) for the repo-owned Enron example
- [docs/SERVICE_OPS_WALKTHROUGH.md](docs/SERVICE_OPS_WALKTHROUGH.md) for the Studio and control-room path

## License

Business Source License 1.1. See [LICENSE](LICENSE). Change date: `2030-03-10`. Change license: `GPL-2.0-or-later`.
