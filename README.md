## VEI
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/strangeloopcanon/vei)

VEI is a programmable replica of a company's entire operational software stack. You give it a company — real or synthetic — and it builds a fully working enterprise twin with Slack, email, tickets, CRM, docs, identity, approvals, and 15+ other surfaces, all connected and all stateful. Then you run things on top of it.

**One kernel, four things you can do:**

1. **Test an agent before it touches a real company.** Run it against a deterministic company world, grade it against typed contracts, and compare different agents or policies over the same starting state.

2. **Steer outside agents while they work.** Put VEI between agents and enterprise systems. Watch what they do, hold risky writes for approval, post guidance into their tasks, and see the downstream effect across every system in one view.

3. **Branch the world and compare outcomes.** Fork from any snapshot, change the rules, replay, and compare two paths side by side — same company, different future.

4. **Export traces for training.** Turn completed runs into rollouts, demonstrations, and RL-friendly datasets.

Every mode shares the same world session, event spine, contract scoring, and replay model. The world simulation is the substrate; the modes are lenses on top.

## Try It Now

```bash
git clone https://github.com/strangeloopcanon/vei.git
cd vei
pip install -e ".[llm,sse,ui]"
vei quickstart run
```

That creates a company world, starts Studio (`:3011`) and the Twin Gateway (`:3012`), runs a scripted baseline so you see events flowing immediately, and prints connection details. Press Ctrl-C to stop.

Options: `--world service_ops`, `--governor-demo`, `--connector-mode live`, `--seed`, `--no-baseline`.

## What This Looks Like

The screenshots below come from a real local VEI run connected to a real local Paperclip company. VEI is not just showing logs here. It is posting guidance into outside work, recording its own intervention, and showing the downstream response inside the same company view.

![VEI + Paperclip control room demo](docs/assets/paperclip-control-room/vei-paperclip-demo.gif)

[Download the MP4 version](docs/assets/paperclip-control-room/vei-paperclip-demo.mp4)

### Operator Console

The outside-workforce view: who exists, what they own, and whether VEI can steer the work.

![VEI operator console](docs/assets/paperclip-control-room/demo-frame-01-pilot-top.png)

### VEI Intervention

VEI's own action appears as `Guided task`, followed by the downstream Paperclip update. The cause-and-effect chain is visible instead of hiding VEI inside generic activity noise.

![VEI intervention in the operator feed](docs/assets/paperclip-control-room/demo-frame-02-pilot-activity.png)

### Company Control Room

The outside workforce is visible inside the company world itself, not only in a side dashboard. Tasks, approvals, interventions, and responses become part of the world state VEI can replay, compare, and branch.

![VEI company control room](docs/assets/paperclip-control-room/demo-frame-03-control-room.png)

### Intervention Story

VEI intervention first, outside response second, all inside one operating picture.

![VEI intervention story in the company view](docs/assets/paperclip-control-room/demo-frame-04-intervention-story.png)

[Full write-up: Paperclip Control Room Report](docs/PAPERCLIP_CONTROL_ROOM_REPORT.md)

## How It Works

VEI simulates a complete enterprise environment — every software system, every person, every process — as one deterministic, branchable world.

**Software surfaces:** Slack, Email, Browser, Docs, Spreadsheet, Tickets, CRM, ERP, Okta-style identity, ServiceDesk, Google Admin, SIEM, Datadog, PagerDuty, feature flags, HRIS, and Jira-style issues. One move in one system can trigger visible changes across all the others.

**Built-in company worlds:**
- **Pinnacle Analytics** (B2B SaaS) — $480K enterprise renewal at risk
- **Harbor Point Management** (Real Estate) — flagship tenant opening under pressure
- **Northstar Growth** (Marketing Agency) — campaign launch with approval and pacing risk
- **Atlas Storage Systems** (Storage/Logistics) — strategic customer quote with fragmented capacity
- **Clearwater Field Services** (Service Ops) — VIP outage, technician no-show, and billing dispute colliding

### The Simulation Loop

1. A `BlueprintAsset` declares the company: org structure, tool data, domain objects
2. The blueprint compiles into a `WorldSession` — a deterministic kernel that owns all state, event queues, and tool dispatch
3. A `Scenario` overlays pressure (a crisis, deadline, or fault injection)
4. A `Contract` defines success (predicates, invariants, reward terms)
5. Actions flow through MCP tools, resolve to capability-graph mutations, and produce side effects across every surface simultaneously
6. The entire run is recorded as an append-only event spine — replayable, branchable, and gradeable

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

## Recipes

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
make setup    # install deps + hooks
make check    # format, lint, types, secrets
make test     # unit + integration
make llm-live # LLM golden scenarios (needs OPENAI_API_KEY; bypass with VEI_LLM_LIVE_BYPASS=1)
make all      # check → test → llm-live, stops on first failure
```

## CLI Surface

- **Start here:** `vei quickstart run` · `vei ui serve` · `vei studio play`
- **Twin and governor:** `vei twin build|serve|status|up|down|reset|finalize|sync`
- **Context and synthesis:** `vei context capture|hydrate|diff` · `vei synthesize runbook|training-set|agent-config`
- **Workspace lifecycle:** `vei project|contract|scenario|run|inspect|showcase`
- **Benchmarking:** `vei eval benchmark|demo|suite|showcase` · `vei bench list|run|scorecard`
- **Expert tools:** `vei world` · `vei blueprint` · `vei visualize` · `vei rollout` · `vei train` · `vei release`

## Examples

- `examples/sdk_playground_min.py`
- `examples/mcp_client_stdio_min.py`
- `examples/rl_train.py`
- `examples/pilot_client.py`

## Docs

- **[OVERVIEW.md](docs/OVERVIEW.md)** — what VEI is, who it's for, how to connect your data
- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** — module structure and data flow
- **[SERVICE_OPS_WALKTHROUGH.md](docs/SERVICE_OPS_WALKTHROUGH.md)** — visual walkthrough of the control plane

## License

Business Source License 1.1 — see [LICENSE](LICENSE). Change date: 2030-03-10. Change license: GPL-2.0-or-later.

## Workspace Hygiene

Source of truth: `vei/`, `tests/`, `docs/`, `tools/`, and top-level config. Generated folders (`_vei_out/`, `.artifacts/`, caches) are disposable — run `make clean-workspace` to prune.

`bd` state is local-only under `.beads/` and should stay out of Git.
