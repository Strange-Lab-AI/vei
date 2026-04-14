## VEI
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/strangeloopcanon/vei)

VEI is a programmable replica of a company's entire operational software stack. You give it a company — real or synthetic — and it builds a fully working enterprise twin with Slack, email, tickets, CRM, docs, identity, approvals, and 15+ other surfaces, all connected and all stateful. Then you run things on top of it.

**One kernel, four things you can do:**

1. **Test an agent before it touches a real company.** Run it against a deterministic company world, grade it against typed contracts, and compare different agents or policies over the same starting state.

2. **Steer outside agents while they work.** Put VEI between agents and enterprise systems. Watch what they do, hold risky writes for approval, post guidance into their tasks, and see the downstream effect across every system in one view.

3. **Branch the world and compare outcomes.** Fork from any snapshot, change the rules, replay, and compare two paths side by side — same company, different future.

4. **Export traces for training.** Turn completed runs into rollouts, demonstrations, and RL-friendly datasets.

Every mode shares the same world session, event spine, contract scoring, and replay model. The world simulation is the substrate; the modes are lenses on top.

**Start with one path:** `vei doctor` -> `vei quickstart run` -> Studio -> `vei twin status` / `vei project show` -> `vei eval benchmark`.
That path is the clearest proof that VEI is working as a company twin, not just as a pile of commands.

## Try It Now

```bash
git clone https://github.com/strangeloopcanon/vei.git
cd vei
make setup
vei doctor
vei quickstart run
```

Need a manual install path instead? Use `pip install -e ".[sse,ui]"`.

What you need before running this:

- Python `3.11`
- A local virtual environment, which `make setup` creates for you at `.venv`
- Ports `3011` and `3012` available on your machine
- `OPENAI_API_KEY` in `.env` only if you plan to run live LLM evaluations later

What happens when `vei quickstart run` starts:

- VEI creates a workspace and compiles a company world
- Studio starts on `http://127.0.0.1:3011`
- The Twin Gateway starts on `http://127.0.0.1:3012`
- A scripted baseline runs so the company already has visible activity when you arrive
- Connection details are written to `.vei/quickstart.json` inside the workspace root

What to do next once it is up:

```bash
vei twin status --root <workspace-root>
vei project show --root <workspace-root>
vei eval benchmark --runner workflow --family security_containment
```

Press Ctrl-C to stop.

Options: `--world service_ops`, `--governor-demo`, `--connector-mode live`, `--seed`, `--no-baseline`.

## Pick Your Entry Point

- **I want to see the product:** run `vei quickstart run`, open Studio, and inspect the generated company state.
- **I want to connect an outside agent:** start with quickstart, then use the Twin Gateway URLs and bearer token from `.vei/quickstart.json`.
- **I want to run a benchmark:** start a world, then run `vei eval benchmark --runner workflow --family security_containment`.
- **I want to use VEI as a Python library:** jump to [Use It As A Library](#use-it-as-a-library).

## What This Looks Like

The screenshots below come from `vei quickstart run --world service_ops --governor-demo`, so a new teammate can reproduce the same flow locally in one command. VEI is not just showing logs. It is showing the company state, the governed outside agents, and the approval story in one place.

![VEI governor demo hero](docs/assets/governor-demo/service-ops-governor-hero.png)

### Control Room

The control room keeps the outside agents, their permissions, and the company-facing connectors in the same view as the simulated company.

![VEI governor control room](docs/assets/governor-demo/service-ops-governor-control-room.png)

### Governance Feed

The governance feed shows what VEI allowed, held, and blocked, with the reason attached to the event instead of hiding it in a side log.

![VEI governor activity log](docs/assets/governor-demo/service-ops-governor-activity-log.png)

Need the live-orchestrator proof point too? Read the [Paperclip Control Room Report](docs/PAPERCLIP_CONTROL_ROOM_REPORT.md).
Need the historical replay flow? Read the [Historical What-If guide](docs/WHATIF.md).

## How It Works

VEI simulates a complete enterprise environment — every software system, every person, every process — as one deterministic, branchable world.

**Software surfaces:** Slack, Email, Browser, Docs, Spreadsheet, Tickets, CRM, ERP, Okta-style identity, ServiceDesk, Google Admin, SIEM, Datadog, PagerDuty, feature flags, HRIS, and Jira-style issues. One move in one system can trigger visible changes across all the others.

**Built-in company worlds:**
- **Clearwater Field Services** (Service Ops) — VIP outage, technician no-show, and billing dispute colliding
- **Pinnacle Analytics** (B2B SaaS) — $480K enterprise renewal at risk
- **Harbor Point Management** (Real Estate) — flagship tenant opening under pressure
- **Northstar Growth** (Marketing Agency) — campaign launch with approval and pacing risk
- **Atlas Storage Systems** (Storage/Logistics) — strategic customer quote with fragmented capacity

### The Simulation Loop

1. A `BlueprintAsset` declares the company: org structure, tool data, domain objects
2. The blueprint compiles into a `WorldSession` — a deterministic kernel that owns all state, event queues, and tool dispatch
3. A `Scenario` overlays pressure (a crisis, deadline, or fault injection)
4. A `Contract` defines success (predicates, invariants, reward terms)
5. Actions flow through MCP tools, resolve to capability-graph mutations, and produce side effects across every surface simultaneously
6. The entire run is recorded as an append-only event spine — replayable, branchable, and gradeable

**Why it's deterministic:** No LLM calls happen inside the simulation. Vendor email replies are picked from pre-written template lists using a seeded RNG. Slack approval checks use regex. CRM, tickets, and docs are pure CRUD with state machines. Scenario seed data (channels, threads, org charts, CRM records) is all static. Same seed = same world — the only variable in an eval is the agent being tested. An optional actor system can use an LLM for NPC responses, but the default backend is template-based and deterministic.

### Architecture

```text
Agent ──MCP──► VEI Router                       External Agent ──HTTP──► Twin Gateway (:3012)
                  └─ transport + tool dispatch                              ├─ Slack / Jira / Graph / SFDC compat routes
                            │                                               ├─ governor agent registry
                            ▼                                               ├─ policy profiles + approval queue
                                                                            ├─ surface / connector enforcement
                      WorldSession Kernel ◄─────────────────────────────────┘
                  ├─ unified world state
                  ├─ snapshots / branch / replay / inject
                  ├─ actor state + receipts
                  ├─ enterprise twins (15+ surfaces)
                  └─ governor runtime (agent fleet, denial tracking, event feed)
                            │
                            ▼
                      Studio UI (:3011)
                  ├─ Living Company View + mode indicator
                  ├─ Control Plane panel (agents, approvals, connector strip)
                  ├─ Mission play + sandbox forking
                  └─ Path comparison + policy replay + snapshot comparison
```

## Test Your Agent Against VEI

```
┌─────────────┐     HTTP / MCP      ┌──────────────────┐     call_tool      ┌──────────────┐
│  Your Agent │ ──────────────────► │  Twin Gateway    │ ────────────────► │  WorldSession │
│  (any lang) │ ◄────────────────── │  :3012           │ ◄──────────────── │  Kernel       │
└─────────────┘   Slack/Jira/SFDC   └──────────────────┘   state + events  └──────────────┘
                   shaped responses         │                                      │
                                            ▼                                      ▼
                                   Contract Evaluation                      Event Spine
                                   (pass/fail/score)                       (events.jsonl)
```

1. **Start VEI**: `vei quickstart run` (or `vei twin serve --root workspace`)
2. **Connect your agent** to the mock API endpoints printed on startup — Slack, Jira, MS Graph, Salesforce — using the bearer token shown
3. **Your agent takes actions** and VEI responds with coherent, stateful results
4. **VEI evaluates** against the contract (success predicates, forbidden predicates, policy invariants) and produces a scorecard
5. **Inspect results** in the Studio UI or read run artifacts (`events.jsonl`, contract evaluation, snapshots)

For MCP-native agents, connect directly: `python -m vei.router`

## Core Path

### 1. Start the company

```bash
vei quickstart run --world service_ops
```

This is the main entry point. It prepares the company, starts Studio, starts the Twin Gateway, and gives you one coherent world to inspect.

### 2. Inspect the twin and workspace

```bash
vei twin status --root <workspace-root>
vei project show --root <workspace-root>
```

Use these to confirm the company identity, scenario, surfaces, and compiled workspace all agree before you start deeper testing.

### 3. Run an evaluation

```bash
vei eval benchmark --runner workflow --family security_containment \
  --artifacts-root _vei_out/benchmark --run-id security_workflow
```

Once the company is up and the twin looks right, this is the next important proof: can an agent or policy actually complete the work and satisfy the contract?

## Advanced Paths

### Run a live LLM episode

```bash
vei llm-test run \
  --provider openai \
  --model gpt-5 \
  --task "Research price, get Slack approval under budget, and email vendor for quote."
```

### Workspace and UI flow

```bash
vei project init --root _vei_out/workspaces/harbor_point --vertical real_estate_management
vei scenario activate --root _vei_out/workspaces/harbor_point --variant vendor_no_show
vei contract activate --root _vei_out/workspaces/harbor_point --variant safety_over_speed
vei run start --root _vei_out/workspaces/harbor_point --runner workflow
vei ui serve --root _vei_out/workspaces/harbor_point
```

### Playable mission mode

```bash
vei studio play \
  --root _vei_out/playable/harbor_point \
  --world real_estate_management \
  --mission tenant_opening_conflict
```
Prepares the world, activates the mission, generates a twin-fidelity report, and serves Studio in Mission Mode.

### Customer-shaped twin

```bash
vei twin build \
  --root _vei_out/customer_twins/acme_cloud \
  --snapshot _vei_out/context/acme_snapshot.json \
  --organization-domain acme.ai

vei twin serve --root _vei_out/customer_twins/acme_cloud
```
The gateway exposes provider-shaped routes for Slack, Jira, MS Graph, and Salesforce while keeping normal VEI scoring and replay underneath.

### Twin stack with orchestrator bridge

```bash
vei twin up \
  --root _vei_out/twins/pinnacle \
  --orchestrator paperclip \
  --orchestrator-url http://127.0.0.1:3100 \
  --orchestrator-company-id company-1 \
  --orchestrator-api-key-env PAPERCLIP_API_KEY
```
Starts the customer twin gateway and Studio in the governor skin. The control room gives the operator one place to follow agent activity, sync routeable workers, send guidance, and approve or reject decisions.

### Grounded import from real enterprise data

```bash
vei project identity-demo --root _vei_out/workspaces/identity_demo --overwrite
vei ui serve --root _vei_out/workspaces/identity_demo
```
For the full step-by-step path: `vei project validate-import`, `review-import`, `scaffold-overrides`, `normalize`, `import`, `scenario generate`, `scenario activate`, `run start`.

### Benchmarking

VEI provides four runner types that form a performance ladder over the same scenario:

- **scripted** — hardcoded behavior tree. Fixed sequence of tool calls. Deterministic baseline floor; if scripted can't solve it, the scenario is probably broken.
- **workflow** — declarative step graph with post-condition assertions. The reference solution — what a correct playthrough looks like.
- **bc** (behavior cloning) — learned policy from demonstration data. Picks tools by frequency statistics. Deterministic but data-driven.
- **llm** — real LLM agent via MCP stdio. Observes the world, reasons, calls tools. Non-deterministic, requires API keys.

```bash
# Family-level benchmark
vei eval benchmark --runner workflow --family security_containment \
  --artifacts-root _vei_out/benchmark --run-id security_workflow

# Multi-family suite
vei eval suite --artifacts-root _vei_out/suite --run-id nightly_suite

# Frontier batch for one model
vei eval benchmark --runner llm --model gpt-5 --frontier \
  --scenario-set reasoning --artifacts-root _vei_out/frontier_eval
```

### Historical what-if replay

```bash
# Explore a whole historical corpus
vei whatif explore \
  --rosetta-dir /path/to/rosetta \
  --scenario compliance_gateway \
  --format markdown

# Find exact branch points by person, thread, or subject
vei whatif events \
  --rosetta-dir /path/to/rosetta \
  --actor vince.kaminski \
  --query "btu weekly" \
  --flagged-only \
  --format markdown

# Materialize one replayable branch point into a strict mail-only workspace
vei whatif open \
  --rosetta-dir /path/to/rosetta \
  --root _vei_out/whatif/enron_case \
  --event-id evt_1234

# Replay the saved historical future
vei whatif replay --root _vei_out/whatif/enron_case --tick-ms 600000

# Run the full counterfactual experiment and save artifacts
vei whatif experiment \
  --rosetta-dir /path/to/rosetta \
  --artifacts-root _vei_out/whatif_experiments \
  --label master_agreement_internal_review \
  --event-id evt_1234 \
  --model gpt-5-mini \
  --forecast-backend e_jepa \
  --ejepa-epochs 1 \
  --ejepa-batch-size 64 \
  --counterfactual-prompt "Keep the draft inside Enron, loop in Gerald Nemec for legal review, and hold the outside send until the clean version is approved."
```

This flow now runs on a normalized company history bundle instead of assuming the historical world is email-only. VEI first answers broad “what would this have touched?” questions over the full history, then lets you search for a concrete event by actor, thread, subject, or surface, and finally turns that historical event into a replayable workspace on the same surface. The workspace branches just before that event, so the event itself becomes the first thing in the baseline future or the first thing a counterfactual path can replace. The replay stays honest to the source data: mail stays mail, chat stays chat, tickets stay tickets, and every excerpt is labeled as an excerpt when the underlying source is partial.

The experiment command writes a bundle with JSON and Markdown summaries plus per-path outputs under `_vei_out/whatif_experiments`. The LLM path generates bounded follow-up emails on the selected thread. The forecast path can now use a real local E-JEPA runtime when the sibling `ARP_Jepa_exp` repo is available, and it falls back to the proxy forecaster only when that runtime is missing or errors. For exact-event forecasting, the JEPA adapter now trains on a deterministic local slice of related threads around the chosen branch point instead of trying to relearn the whole archive every time.

In Studio, the same flow is available as a search-first loop: find a real historical event, materialize it, then run the counterfactual and inspect the saved comparison.

The Enron flow now carries a packaged public-company context pack alongside the mail archive. That pack lives under `vei/whatif/fixtures/enron_public_context`, can be refreshed with `python scripts/prepare_enron_public_context.py`, and contains 7 dated financial checkpoints plus 7 dated public news events drawn from 7 archived public source files. Those public facts span December 31, 1998 through December 2, 2001. VEI filters that public context twice: once to the loaded Enron email window, and again to the chosen branch date. The saved manifest, the Studio decision scene, the LLM counterfactual prompt, and the held-out benchmark dossiers all use that same pre-branch slice.

The same path now works for a new company without adding a fresh Enron-style code path. Point `vei whatif` at a `context_snapshot.json` bundle. A multi-source context snapshot can now branch from mail, Slack or Teams-style chat, and Jira-style ticket history through the same typed what-if API. VEI also derives a shared case id across those surfaces, then carries earlier cross-surface case activity plus linked document or CRM records into the branch scene and the saved workspace when the normalized bundle has them. Add a sidecar `whatif_public_context.json` in the same folder when you have dated public facts you want VEI to carry into the branch scene and the saved run. Add a research-pack JSON file when you want reusable case sets for pack runs or benchmark builds.

![Enron historical what-if flow](docs/assets/enron-whatif/enron-whatif-flow.gif)

Later 2001 branch points now show the combined mail-plus-public-context view directly in Studio.

![Enron decision scene with public context](docs/assets/enron-whatif/enron-decision-scene-top.png)

This repo now ships a saved Enron example workspace directly under `docs/examples`, so a fresh clone can open the historical branch without first rebuilding the run locally:

```bash
vei ui serve \
  --root docs/examples/enron-master-agreement-public-context/workspace \
  --host 127.0.0.1 \
  --port 3055
```

That repo-owned bundle also includes the saved experiment overview, the LLM path result, the JEPA forecast result, and a repo-owned three-way business-state comparison under `docs/examples/enron-master-agreement-public-context/`. Use the real Rosetta archive when you want whole-history Enron search or a fresh rerun from the full corpus.

The saved `Master Agreement` example shows the full loop in one place. VEI starts from a real outside send on September 27, 2000, keeps only the Enron public-company facts that were already known on that date, and then compares alternate moves against the recorded future.

![Enron predicted business change](docs/assets/enron-whatif/enron-predicted-business-change.png)

On that saved run, VEI keeps the same 84-event horizon but predicts 29 fewer outside sends and a small risk drop from `1.000` to `0.983`. The business readout turns that into decision language: lower outside spread risk, slightly stronger commercial footing, and no large delay penalty.

![Enron ranked business comparison](docs/assets/enron-whatif/enron-ranked-comparison.png)

The ranked view shows how VEI turns that forecast into a choice. `Hold for internal review` comes out best at `0.351`, `Send a narrow status note` still helps at `0.155`, and `Push for fast turnaround` falls behind at `-0.019`.

<details>
<summary>Concrete Enron example: Debra Perlingiere -> Cargill Master Agreement</summary>

**What happened**

On September 27, 2000, Debra Perlingiere sent a real `Master Agreement` draft from Enron Legal to `kathy_gerken@cargill.com`. In VEI, we branch just before that send, so the original email becomes the first event in the historical future that can either play out as it did in history or be replaced by a counterfactual path.

**What we changed**

We asked: what if Debra had kept the draft inside Enron, asked Gerald Nemec for legal review first, and held the outside send until that review came back?

**How the two compare paths work**

The LLM path does not rewrite the whole company. It generates a bounded continuation on that one thread, limited to real thread participants and allowed recipient addresses, then delivers those messages through VEI's mail environment.

The E-JEPA path does not write emails. When the local JEPA runtime is available, VEI trains a local forecast around the selected branch point and predicts how the rest of the thread changes in structure, such as outside sharing and risk. If that runtime is unavailable, VEI falls back to the proxy forecast described above.

**What the current combined dataset shows before you branch**

Opening that branch point against the current combined dataset creates a workspace with 6 prior messages and 84 recorded future events. Because the branch date is September 27, 2000, the public-company panel shows the 1998 and 1999 annual checkpoints and no public-news items yet. Later 2001 branch points pick up both the financial and public-news columns automatically.

**What the saved repo example says about the counterfactual**

The committed example bundle shows both compare paths. The bounded LLM path keeps the draft inside Enron, loops in Gerald Nemec for legal review, and sends an internal status update instead of sending the attachment to Cargill. The JEPA forecast keeps the same 84-event horizon but predicts risk moving from `1.000` to `0.983` and the outside-send count dropping by `29`.

The same saved bundle now also carries a business-state translation layer on top of that forecast. In this example, VEI reads the forecast as lower outside spread risk, slightly stronger commercial position, slightly stronger relationship stability, and slightly higher approval or escalation pressure. The repo-owned comparison file then ranks the three branch-scene candidate moves for this same Enron branch point:

- `Hold for internal review`: strongest net improvement at `0.351`, with a much safer containment path
- `Send a narrow status note`: positive middle path at `0.155`, with lower spread risk but a smaller overall gain
- `Push for fast turnaround`: weaker result at `-0.019`, with more execution pressure and no containment gain
</details>

### Enron business-outcome benchmark

VEI also supports a separate Enron-only benchmark for the harder question: from one real business state, does a candidate action make things better or worse for the business.

This benchmark uses only the history before the chosen decision point plus a structured action description. The models then predict later email evidence that can actually be observed in the archive, such as outside spread, legal burden, executive heat, coordination load, trust signals, and execution drag. VEI turns that evidence into five business-facing proxy scores:

- `enterprise_risk`
- `commercial_position_proxy`
- `org_strain_proxy`
- `stakeholder_trust`
- `execution_drag`

The held-out Enron pack is judged separately from rollout generation. Each held-out case gets one dossier per business objective, the locked LLM judge ranks the candidate actions from the pre-branch dossier alone, and the benchmark keeps an audit queue for low-confidence or sampled cases.

Those Enron dossiers now also include the dated public-company backdrop that was already known by the branch date. The default held-out pack is `enron_business_outcome_v1`, and it currently covers 24 fixed branch points across 6 case families with 4 candidate actions per case. The models still train on the same pre-branch mail history, summary features, and structured action schema as before. The public context is informational for replay, judging, and inspection in this pass.

```bash
# Build the pre-branch Enron benchmark dataset and held-out case pack
vei whatif benchmark build \
  --rosetta-dir /path/to/rosetta \
  --artifacts-root _vei_out/whatif_benchmarks/branch_point_ranking_v2 \
  --label enron_business_outcome_public_context_20260412

# Train one model family on observed Enron futures
vei whatif benchmark train \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412 \
  --model-id jepa_latent

# Judge the held-out counterfactual cases
vei whatif benchmark judge \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412 \
  --model gpt-4.1-mini

# Evaluate one trained model against factual futures and judged rankings
vei whatif benchmark eval \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412 \
  --model-id jepa_latent \
  --judged-rankings-path _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412/judge_result.json \
  --audit-records-path /path/to/completed_audit_records.json
```

For a new company history bundle, run the same flow against the normalized snapshot and point the held-out cases at a JSON pack file:

```bash
vei whatif pack run \
  --source-dir /path/to/newco/context_snapshot.json \
  --label newco_pack_run \
  --pack-id /path/to/newco/research_pack.json

vei whatif benchmark build \
  --source-dir /path/to/newco/context_snapshot.json \
  --label newco_benchmark \
  --heldout-pack-id /path/to/newco/research_pack.json
```

The current Enron business-outcome benchmark uses 24 held-out cases with 4 candidate actions each. The clean comparison now comes from the matched-input study path, where `jepa_latent`, `full_context_transformer`, and `treatment_transformer` all see the same pre-branch history, summary features, and structured action.

The current saved build also carries the dated Enron public-company backdrop that was already known by each branch date. The held-out dossiers include those financial checkpoints and public news items, while the model-training inputs stay the same. The current study report lives under `studies/matched_input_public_context_20260412/`.

On the current 5-seed, 2-epoch matched-input rerun over the saved `enron_business_outcome_public_context_20260412` build, the held-out decision checks came out like this:

- `jepa_latent`: `80.2/120` mean, `0.668 +/- 0.012`
- `full_context_transformer`: `79.4/120` mean, `0.662 +/- 0.031`
- `treatment_transformer`: `68.2/120` mean, `0.568 +/- 0.117`

On the simpler factual task of predicting whether anything goes outside after the branch point, all three stayed tightly grouped around `0.98` AUROC: `0.981` for `jepa_latent`, `0.982` for `full_context_transformer`, and `0.980` for `treatment_transformer`. The main point is that once the model inputs are aligned, the richer Enron dossiers are in place, and the result is averaged across seeds, the JEPA-style path still leads the business decision checks while the treatment transformer remains much less stable.

## Use It As A Library

```bash
pip install "git+https://github.com/strangeloopcanon/vei.git@main"
```

```python
from vei.sdk import create_session

session = create_session(seed=42042, scenario_name="multi_channel")
obs = session.observe()
page = session.call_tool("browser.read", {})
```

```python
from vei.world.api import create_world_session, get_catalog_scenario

world = create_world_session(
    seed=42042,
    scenario=get_catalog_scenario("multi_channel"),
)
obs = world.observe()
snapshot = world.snapshot("before-run")
events = world.list_events()
```

## `.env` Configuration

```env
OPENAI_API_KEY=sk-your-key
VEI_SEED=42042
VEI_ARTIFACTS_DIR=./_vei_out
```

## Repo Validation

```bash
make setup       # lighter local install for Studio + quickstart
make setup-full  # full dev + test + llm + rl extras
make check    # format, lint, types, secrets
make test     # unit + integration
make llm-live # LLM golden scenarios (needs OPENAI_API_KEY; bypass with VEI_LLM_LIVE_BYPASS=1)
make all      # check → test → llm-live, stops on first failure
```

## CLI Surface

- **Primary path:** `vei doctor` -> `vei quickstart run` -> `vei twin status` -> `vei project show` -> `vei eval`
- **Twin and governor:** `vei twin build|serve|status|up|down|reset|finalize|sync`
- **Workspace lifecycle:** `vei project|contract|scenario|run|inspect`
- **Benchmarking:** `vei eval benchmark|demo|suite`
- **Historical what-if and Enron benchmark:** `vei whatif experiment|rank|benchmark`
- **Advanced workflows:** `vei ui serve` · `vei studio play` · `vei context capture|hydrate|diff` · `vei synthesize runbook|training-set|agent-config`
- **Expert tools:** `vei showcase` · `vei visualize` · `vei world` · `vei blueprint` · `vei rollout` · `vei train` · `vei release`

## Examples

- `examples/sdk_playground_min.py`
- `examples/mcp_client_stdio_min.py`
- `examples/rl_train.py`
- `examples/governor_client.py`

## Docs

- **[OVERVIEW.md](docs/OVERVIEW.md)** — what VEI is, who it's for, how to connect your data
- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** — module structure and data flow
- **[WHATIF.md](docs/WHATIF.md)** — historical replay, ranked what-if, and Enron benchmark flows
- **[ENRON_BUSINESS_OUTCOME_BENCHMARK.md](docs/ENRON_BUSINESS_OUTCOME_BENCHMARK.md)** — Enron benchmark setup, objectives, judge loop, and artifact layout
- **[Enron Example](docs/examples/enron-master-agreement-public-context/README.md)** — repo-owned saved Enron branch with current public-context results
- **[SERVICE_OPS_WALKTHROUGH.md](docs/SERVICE_OPS_WALKTHROUGH.md)** — visual walkthrough of the control plane

## License

Business Source License 1.1 — see [LICENSE](LICENSE). Change date: 2030-03-10. Change license: GPL-2.0-or-later.

## Workspace Hygiene

Source of truth: `vei/`, `tests/`, `docs/`, `tools/`, and top-level config. Generated folders (`_vei_out/`, `.artifacts/`, caches) are disposable — run `make clean-workspace` to prune.
