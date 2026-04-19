# VEI

VEI is a programmable replica of an entire company's operational software stack. You give it a company description — or connect it to real Slack, Gmail, Jira, Teams, Notion, Linear, and meeting-note exports — and it builds a fully functioning simulated copy of that company with working Slack channels, email threads, ticket queues, CRM pipelines, document stores, identity systems, and a typed knowledge layer. An agent or a human can then operate inside it: play crisis scenarios, steer live activity through the twin gateway, train on the traces, draft grounded proposals and briefs, and synthesize operational artifacts from what happened.

It spans hundreds of Python files and tests, a single-page Studio UI, and one unified `vei` CLI for project setup, world simulation, benchmarking, release/export, and evaluation.

Use the root `README.md` for install and operator quickstart. Use this document for product framing, personas, and the end-to-end story. Use `docs/ARCHITECTURE.md` for the technical map. Use `docs/WHATIF.md` for the historical counterfactual workflow.

## One Kernel, Four Modes

VEI is best understood as **one kernel with four operating modes**, not as a pile of separate products.

- **Test / Eval** — run a fixed company world, score an agent, and see whether it actually works
- **Governor / Control** — place VEI between agents and enterprise systems, or ingest their actions, so VEI can govern, record, and replay what happened
- **Sandbox / What-if** — fork the same world, change policy or actions, compare alternate futures, and for `service_ops` replay the same starting point with a small set of named policy changes
- **Train / Data** — turn the same traces and trajectories into rollouts, demonstrations, and RL-friendly data

Those four modes share the same world session, connector layer, event spine, replay model, and contract scoring. The world simulation is the substrate for all of them. Governor mode is the special case with live edges: VEI still uses the same kernel, but some actions also flow to or from real systems.

## Knowledge Brain Layer

VEI now carries the "knowledge brain" on the same rails as the action brain. Notes, transcripts, pricing guides, metric snapshots, SOPs, and deliverables land in `knowledge_graph` instead of a parallel authoring system. The kernel stores them under `vei.world`, mutations emit typed `CanonicalEvent` records, and authored artifacts stay branchable, replayable, and contract-scored.

That keeps proposal drafting, weekly reviews, and other knowledge work inside the same deterministic spine as everything else:

- connectors and offline exports hydrate typed `KnowledgeAsset` nodes
- `knowledge.compose_artifact` retrieves grounded sources and drafts an artifact with inline citations
- structural contracts score citation presence, citation resolution, freshness, numeric reconciliation, and template shape
- the same run timeline, snapshot model, compare view, and synthesis layer work on authoring runs too

Standalone `vei knowledge compose` uses the same knowledge state and validation rules, writes the updated knowledge snapshot back into the workspace, and stays separate from any existing run timeline. Run-based authoring export still reads recorded runs.

`llm-siem` fits beside VEI, not inside it. It is a useful companion for the thinking layer of agent operations — fleet posture, LLM-call observability, and later cross-agent correlation — while VEI owns the acting layer: enterprise actions, world state, contracts, and consequences.

### Historical What-Ifs

VEI also supports a more grounded historical version of the sandbox story: take a real communication corpus, ask “what would this rule or decision have changed?”, and then drill into one moment for replay.

The first version of that flow is mail-first:

- a whole-history explorer can scan archive-backed datasets such as the Enron Rosetta event tables
- a selected historical event can then be materialized into a strict historical workspace
- that workspace replays real past messages up to just before the chosen event and schedules the chosen event plus the later historical messages as the baseline future
- only after that branch point does VEI run counterfactual continuation logic

The company-history path now writes a shared canonical timeline beside each normalized snapshot. `context_snapshot.json` stays as the portable interchange bundle. `canonical_events.jsonl` and `canonical_event_index.json` now carry the actual chronology that the company-history loader and Studio timeline panel read first.

For Enron, that mail-first branch point can also carry a packaged public-company backdrop. VEI now ships 11 dated public financial checkpoints, 21 dated public news events, 986 daily stock rows, 7 credit events, and 1 FERC timeline event from 24 archived public source files. VEI filters those rows to the loaded Enron email window, then filters them again to the branch date. The same pre-branch slice flows into the saved episode manifest, Studio decision scene, bounded LLM prompt, and benchmark dossiers.

Two compare paths exist today:

- **LLM actor continuation** — bounded multi-channel continuation on the affected thread, supporting mail, chat (Slack/Teams-style), and Jira-style ticket adapters
- **Dynamics forecast** — a pluggable forecast backend registered through `vei.dynamics.api`; VEI now prefers a repo-local reference checkpoint when one is present, then the JEPA runtime, then the heuristic baseline. The reference checkpoint is the flagship learned path. The heuristic path stays as a baseline and debug path.

## The Five Layers

### 1. Context Capture

Real enterprise data comes in. VEI captures Slack, Gmail, Microsoft Teams, Jira, Google Workspace, Okta, and offline-first knowledge exports such as Notion, Linear, and Granola. Slack, Gmail, Teams, Jira, Google, and Okta can use live APIs. The knowledge connectors read offline exports only: Notion exports, Linear dumps, and transcript files. The output is a `ContextSnapshot` — a structured record of what the company looks like right now: who's talking to whom, what tickets are open, what docs exist, what notes are current, and who has what access.

### 2. Blueprint Compilation

The snapshot gets hydrated into a `BlueprintAsset` — VEI's portable, declarative description of a company. This includes a communications graph (Slack channels, mail threads), a work graph (Jira-style tickets and workflows), a document graph, an identity graph (users, groups, app assignments, policies), a `knowledge_graph` for grounded authoring assets and edges, and optionally revenue, ops, inventory, campaign, property graphs. Five built-in vertical archetypes exist as ready-to-go blueprints:

- **Pinnacle Analytics** (B2B SaaS) — $480K renewal at risk, support escalation spirals, pricing deadlocks
- **Harbor Point Management** (Real Estate) — Tenant openings, vendor no-shows, lease revisions, double-booked units
- **Northstar Growth** (Marketing Agency) — Campaign launch guardrails, creative approvals, budget runaways
- **Atlas Storage Systems** (Storage/Logistics) — Capacity quotes, vendor dispatch gaps, fragmented inventory
- **Clearwater Field Services** (Service Operations) — VIP outage, technician no-show, and billing dispute colliding on the same account; this is the built-in synthetic rig, not the flagship real-data proof

Two authoring tools lower the barrier to creating new verticals:

- **`vei blueprint scaffold --openapi spec.yaml`** — reads an OpenAPI spec and generates a skeleton blueprint with Pydantic models, router stubs, and capability graph entries. You fill in the causal links.
- **`vei blueprint generate --prompt "..."`** — uses an LLM to draft a complete blueprint from a natural language description of a company, its tools, and a crisis scenario. Produces actors, Slack channels, mail threads, tickets, documents, causal links, and contract predicates.

Blueprints support **progressive disclosure** via per-surface fidelity levels: L1 (static canned responses), L2 (stateful key-value store, no cross-system causality), and L3 (full coherent simulation, the default). A single blueprint can mix levels — make Jira L3 while leaving Salesforce at L1.

### 3. World Simulation Engine

The blueprint compiles into a live `WorldSession` — a deterministic, branchable, replayable discrete-event simulation. The router provides 50+ MCP tools spanning every enterprise surface: `slack.send_message`, `mail.compose`, `browser.navigate`, `docs.create`, `tickets.update`, `crm.log_activity`, `okta.suspend_user`, `erp.check_inventory`, `servicedesk.resolve`, `knowledge.compose_artifact`, and many more. Time advances. Events fire. State changes propagate across surfaces. You can snapshot, branch, restore, replay, and diff any point in the world's history.

The connector layer routes each tool call through one of three adapters — simulated (default), replay (from recorded traces), or live (real API calls) — with policy gates classifying operations as READ, WRITE_SAFE, or WRITE_RISKY.

For archive-backed historical episodes, the simulation stays deliberately narrow. If the source data only supports mail and identity, VEI exposes a mail-first workspace instead of inventing unrelated surfaces. Historical message bodies are also preserved as excerpts when the source data is truncated.

### 4. Playable Missions and Evaluation

On top of the simulation sits a mission system. Each vertical has multiple crisis scenarios. A mission gives you a starting world state, a set of available moves (each triggering a sequence of tool calls), success/failure contracts, and a scorecard. You can play interactively, run a scripted baseline, or let an LLM agent play — then compare the paths. The first knowledge mission is `northstar_proposal_drafting`, which drafts a grounded client proposal from transcript, pricing, delivery, and SOP evidence on the same Northstar world used for campaign operations. `knowledge_authoring` is the benchmark family name for that mission, not a sixth vertical.

The contract system defines predicates (what must happen), invariants (what must not happen), observation boundaries (what the agent can see vs. hidden oracle state), and reward signals. The benchmark framework runs families of workflows, scores them, and supports difficulty tiers (p0-easy through pX-adversarial) plus frontier rubric scenarios (budget reconciliation, knowledge QA, cascading failures, ethical dilemmas).

A lightweight RL layer provides a Gymnasium-compatible `VEIEnv`, behavior cloning trainer, and BC policy wrapper for learning policies from demonstration traces.

The same idea extends naturally to historical what-if runs. Each branch point becomes a decision sample: one real baseline future, one counterfactual continuation, and a typed comparison result describing what changed. For knowledge work, two authored runs can now be compared in Studio with their source coverage and validation deltas shown beside the normal run comparison view.

### 5. Synthesis, Twin Gateway, and Governor Mode

Finished runs produce structured outputs:

- **Runbooks** — step-by-step operational procedures extracted from what actually happened
- **Training sets** — conversation, trajectory, and demonstration data formatted for fine-tuning
- **Authoring fine-tune rows** — proposal or brief drafting examples with retrieved sources, citations, validation, and contract results
- **Agent configs** — system prompts, tool specs, guardrails, and success criteria for deploying agents
- **Counterfactual bundles** — JSON and Markdown summaries for what-if experiments, including selected thread, baseline replay, LLM continuation, and forecast deltas

The twin gateway takes a `ContextSnapshot` plus a vertical archetype, merges them into a "customer twin" — a workspace that mirrors the customer's actual company but runs on VEI's simulation. It exposes compatibility surface specs (Slack-shaped, Jira-shaped, Graph-shaped, Salesforce-shaped routes) so the twin can be addressed through familiar API shapes.

Governor mode builds on that gateway in two parallel ways:

- **Proxy path** — for agents you control, register the agent, point it at VEI's compatibility routes, and let VEI govern and record live-shaped traffic
- **Ingest path** — for third-party or already-deployed agents, register the agent and send typed external events into the same run history

Today governor mode ships in two maturity levels:

- **Governor demo mode** — built-in agent registry plus staged timed activity over simulated worlds, especially `service_ops`, so the control-room story feels live without real credentials
- **Governor live alpha** — Slack-first live pass-through with typed policy profiles, approval holds, and connector-status reporting; unsupported surfaces still serve reads from the last synced twin snapshot, and live writes fail clearly until their adapters exist

That last point matters: VEI is authoritative for actions it directly proxies or ingests. Everything else is refreshed by capture or re-sync, not by claiming real-time convergence yet.

#### Governor runtime internals

The `vei.governor` package provides the runtime that makes governor mode work:

- **`GovernorRuntime`** — manages the agent registry, event ingest, demo autoplay, approval queue, rate limiting, and snapshot generation. Each registered agent is a `GovernorAgentSpec` with typed fields for role, team, allowed surfaces, policy profile, status, `last_action`, `denied_count`, and `throttled_count`.
- **Typed policy profiles** — every registered agent resolves to one of four built-in profiles: `observer`, `operator`, `approver`, or `admin`. Those profiles decide whether reads, safe writes, and risky writes are allowed, denied, or held for approval.
- **Governor decision pipeline** — the twin gateway evaluates registration, agent mode, allowed surfaces, policy profile, connector safety, and lightweight rate limits before it executes a governed action. The `record_only` path intentionally bypasses enforcement so passive observation agents can report telemetry without policy gating.
- **Approval queue and recent-event feed** — risky actions can pause in a pending-approval state instead of executing immediately. The runtime keeps a bounded ring buffer of recent governor events (`GovernorRecentEvent`) so the Studio control plane can show blocked, held, throttled, and executed activity without rescanning full history.
- **`GovernorRuntimeSnapshot`** — a typed snapshot of the governed fleet state including agents, resolved policy profiles, pending approvals, connector status, denial counts, throttle counts, config, and the recent event feed.

## The UI

Studio now ships one shell. It picks the visual tone automatically: historical archive workspaces open in the lighter casebook tone, while live and governor workspaces open in the darker control-room tone.

A single-page Studio interface exposes four top-level tabs plus the Connections panel:

- **Company tab** — one surface with five in-page sections: `Live Company`, `Next Move`, `Recent Changes`, `Historical Decision`, and `Knowledge`. The live section shows every surface (Slack, Mail, Docs, Tickets, CRM, and the vertical heartbeat) updating in real time as the simulation runs. The Knowledge section lists grounded assets, freshness state, and citation details for composed artifacts. When governor mode is active, a **mode indicator banner** appears and the **Control Plane panel** shows registered agents, policy badges, connector status, an approval queue, inline agent controls, and a readable activity log.
- **Crisis tab** — structured analysis of what went wrong and why it matters, with crisis description, impact assessment, and failure consequences.
- **Outcome tab** — contract evaluation, decision audit trail, path comparison, snapshot branching, cross-run snapshot comparison, the authored-artifact compare card for knowledge runs, and the `service_ops` policy replay flow.
- **Audit tab** — benchmark review queue and human scoring workflow for held-out what-if cases.
- **Connections panel** — live source status and one-click capture from the Views menu.

For Enron workspaces, the `Historical Decision` section also shows the dated public-company slice beside the branch event and recorded future when public facts were known by that date.

Run pickers for path comparison are always visible. Fork-from-here and snapshot inspection are accessible to all users, not gated behind a developer toggle.

### Governor Control Room

The operator experience now lives directly inside Studio. One command (`vei twin up`) starts the twin gateway and Studio, writes the launch details, and opens the control-room view where the operator can watch live or demo agent activity, check outcome status, and reset, finalize, or sync runs.

The same control room can bridge an outside orchestrator into VEI. For the first adapter, Paperclip workforce state is normalized into a vendor-neutral snapshot, shown beside VEI activity, and routeable workers are synced into governor registration using provider-prefixed agent IDs such as `paperclip:agent-42`.

---

## Connecting Your Own Enterprise Data

Three paths exist today, from easiest to most integrated:

### Path 1: Offline exports (no API keys needed)

If you have a Slack workspace export (JSON) or a Gmail Takeout (MBOX), you can ingest them directly:

```bash
vei context ingest-slack --export-dir ~/Downloads/slack_export --org "Acme Corp"
vei context ingest-gmail --mbox ~/Downloads/gmail_takeout.mbox --org "Acme Corp"
```

This produces a `context_snapshot.json` that can be hydrated into a blueprint and played.

### Path 2: Live API capture (set tokens, one command)

Set environment variables for each provider you want to connect:

```bash
export VEI_SLACK_TOKEN=xoxb-your-slack-bot-token
export VEI_GMAIL_TOKEN=ya29.your-gmail-oauth-token
export VEI_TEAMS_TOKEN=eyJ0your-graph-api-token
export VEI_JIRA_TOKEN=your-jira-api-token
export VEI_GOOGLE_TOKEN=ya29.your-google-workspace-token
export VEI_OKTA_TOKEN=your-okta-api-token
```

Then capture from any combination:

```bash
vei context capture \
  --provider slack \
  --provider gmail \
  --provider jira \
  --org "Acme Corp" \
  --domain acme.com \
  --output acme_snapshot.json
```

Add `--anonymize` to strip PII (emails, phones, names) from captured data before it enters the simulation. The anonymizer uses deterministic pseudonymization — the same real identity always maps to the same fake identity, preserving referential integrity across all surfaces.

### Path 3: UI Connect panel

Start the Studio UI and click "Connections" in the Views menu. The panel shows each provider's status (configured or missing). If tokens are set, click "Capture Now" to pull live data.

### From snapshot to playable world

Once you have a snapshot, hydrate it into a blueprint and build a workspace:

```bash
vei context hydrate --snapshot acme_snapshot.json --output acme_blueprint.json
vei twin build --root _vei_out/twins/acme --snapshot acme_snapshot.json --organization-domain acme.com
vei ui serve --root _vei_out/twins/acme
```

The twin builder merges your captured data with the closest vertical archetype (it picks based on which data surfaces are present) and produces a workspace you can run scenarios against, play missions in, govern live traffic through, or generate training data from.

If you want the governor experience without real credentials yet, build the same twin with staged demo activity:

```bash
vei twin build \
  --root _vei_out/twins/clearwater_demo \
  --snapshot acme_snapshot.json \
  --organization-domain clearwater.example.com \
  --archetype service_ops \
  --governor-demo

vei twin serve --root _vei_out/twins/clearwater_demo
```

If you are testing the first live-control slice, flip the connector mode instead:

```bash
vei twin build \
  --root _vei_out/twins/clearwater_live \
  --snapshot acme_snapshot.json \
  --organization-domain clearwater.example.com \
  --archetype service_ops \
  --connector-mode live
```

### What each provider captures

| Provider | What it pulls | Token type |
|----------|--------------|------------|
| Slack | Channels, messages, user profiles | Bot token (`xoxb-`) |
| Gmail | Threads, messages, labels, profile | OAuth2 bearer |
| Teams | Joined teams, channels, messages, profile | MS Graph bearer |
| Jira | Projects, issues, comments, users | API token or PAT |
| Google | Drive files, shared drives, users | OAuth2 bearer |
| Okta | Users, groups, app assignments, policies | API token |

All API calls are read-only. No data is written back. Tokens stay in your `.env` file and are never committed.

---

## Is It a World Model?

Honest answer: **not yet, but it's the training ground for one.**

A "world model" in the ML/reinforcement learning sense is a *learned* model that predicts future states given actions. You show it (state, action) pairs and it learns the transition dynamics — then it can generalize to states it hasn't seen.

VEI today is a **simulation**: a hand-authored, rule-based, deterministic engine. It encodes how enterprise systems work through explicit transition rules. Send a Slack message and the ticket queue updates because there's a rule connecting them. Suspend an OAuth app and the security case progresses because that causality is authored.

The relationship between VEI-the-simulation and a future VEI-the-world-model is like the relationship between a physics engine and a physics-from-video model:

1. **The simulation generates unlimited training data** — every run produces a full trajectory of (state, action, next-state) triples across all enterprise surfaces, with contract scores as reward signals
2. **The synthesis layer packages this data** — conversations, trajectories, and demonstrations in formats ready for fine-tuning
3. **The RL layer provides the gym interface** — a Gymnasium-compatible environment wrapper with observation/action spaces

So the path to an actual world model is: run thousands of scenarios through VEI, collect the trajectories, and train a model that learns the transition function. The simulation is the ground truth that bootstraps the learned model. Once the learned model is good enough, you could swap it in as a faster/cheaper prediction engine underneath the same API surface.

What makes VEI more than a toy simulation is the *coherence* — changing one surface genuinely affects others in plausible ways, the contracts grade whether those ripple effects happened correctly, and the whole thing is deterministic and replayable. That coherence is what makes the generated training data valuable rather than random.

---

## Do We Need to Dockerize?

Not urgently, but it would help in three specific situations:

**When it helps:**
- **Demo distribution** — `docker compose up` instead of "install Python 3.11, create a venv, install deps, set env vars, run make setup." One command to show someone the product.
- **Twin gateway as a service** — if you want to serve a customer twin that external agents talk to, it makes sense as a containerized service with its own port.
- **CI reproducibility** — pin the exact environment so tests run identically everywhere.

**When it doesn't matter:**
- For development — pip install in a venv is already fast and works
- For library use — people embedding VEI in their own code won't want Docker
- For the current user base (you + collaborators) — overhead without payoff

**Recommendation:** worth doing when there's a concrete distribution need (e.g., "I want to send someone a link and they click play"). Not blocking anything right now. When it's time, it's a straightforward Dockerfile — the app is already a single `pip install` with a FastAPI server, so containerizing it is a one-session task.

---

## Who Uses This and What Pain Are They In?

Five distinct user types, each with a different entry point into VEI:

### 1. Agent Builders

**Pain:** "My agent passes toy benchmarks but falls apart when enterprise workflows get messy — multiple systems, hidden state, competing deadlines, things that break halfway through."

**How they use VEI:** Drop in their agent via the MCP interface or the SDK. Run it against progressively harder scenarios (p0-easy through pX-adversarial). The contract system tells them exactly what the agent got right and wrong. Branch from the failure point and try a different approach. Compare paths side by side.

**Entry point:** `vei llm-test run --provider openai --model gpt-5 --task "..."`

### 2. Synthetic Data Teams

**Pain:** "We need training data for enterprise agents but can't use real customer data — compliance, privacy, volume, variety."

**How they use VEI:** Generate thousands of rollout trajectories across multiple verticals and difficulty levels. Export as conversation pairs, state-action trajectories, or demonstration sequences. Each trajectory is fully deterministic and reproducible. The synthesis layer formats everything for fine-tuning.

**Entry point:** `vei rollout procurement --episodes 1000` then `vei synthesize training-set`

### 3. Enterprise Ops / Process Teams

**Pain:** "What happens to our workflows if a key vendor drops out? If a P1 hits during a renewal? If our identity provider has a policy conflict?" There's no way to test "what if" scenarios against real operational complexity without actually breaking things.

**How they use VEI:** Connect their real Slack/Jira/Gmail data, build a twin of their company, then play crisis scenarios against it. See exactly which systems are affected, what breaks, and what the recovery path looks like — without touching production.

**Entry point:** `vei context capture --provider slack --provider jira` then `vei twin build`

### 4. Benchmark / Eval Researchers

**Pain:** "There's no standardized, graded benchmark for how well agents handle realistic enterprise work. Existing benchmarks are either too simple (single-tool, one-step) or too synthetic (no real business logic)."

**How they use VEI:** Run the built-in benchmark families (security containment, enterprise onboarding, revenue incidents) across difficulty tiers. Use the contract system for objective scoring. Publish leaderboards. The frontier scenarios test edge cases (contradictory requirements, ethical dilemmas, cascading failures) that most benchmarks ignore.

**Entry point:** `vei eval benchmark --frontier --scenario-set reasoning --model gpt-5`

**Quick benchmarking:** `vei bench list` shows all available scenarios, vertical packs, and benchmark families. `vei bench run --scenario multi_channel --runner scripted` runs a benchmark and produces a scorecard. `vei bench scorecard <dir>` renders results from a previous run.

### 5. Platform / Integration Teams

**Pain:** "We're building AI features that integrate with Slack, Jira, Salesforce, etc. Testing against real APIs is slow, expensive, flaky, and requires sandbox accounts. We need a local replica that behaves like the real thing."

**How they use VEI:** The twin gateway exposes provider-shaped API routes (Slack-compatible, Jira-compatible, Graph-compatible) backed by the simulation. Their integration code talks to VEI exactly like it would talk to the real service, but everything is local, deterministic, and inspectable.

**Entry point:** `vei twin serve --root workspace --port 3020`

### Quick-start for all personas: the Twin stack

Any of the above can skip the per-step setup by running `vei twin up`, which builds a twin, starts the gateway and Studio, and writes launch details with bearer token, curl snippets, and a sample client script. The Studio governor view shows connection details, live agent activity, outcome status, and reset/finalize controls — one command to get from zero to a working enterprise twin.

### Quickstart: `vei quickstart run`

The fastest path. One command creates a workspace from a built-in vertical, starts both the Studio UI (`:3011`) and the Twin Gateway (`:3012`), runs a scripted baseline so events are immediately flowing, and prints connection details (mock API URLs, auth token, MCP endpoint). Press Ctrl-C to stop.

### Testing your agent

The Twin Gateway exposes provider-shaped HTTP endpoints (Slack Web API, Jira REST v3, Microsoft Graph, Salesforce REST) backed by the simulation. Your agent connects with a bearer token and interacts as if talking to real services. VEI evaluates the run against the contract (success predicates, forbidden predicates, policy invariants) and produces a scorecard. Results appear in the Studio UI timeline and as run artifacts (`events.jsonl`, contract evaluation JSON, state snapshots).

For MCP-native agents, bypass the HTTP gateway entirely with `python -m vei.router`. Configure scenario, seed, and artifact paths through environment variables or `mcp.json`.

---

## What Holds It Together

The thing that makes VEI more than a collection of mock APIs is that it's **one connected system**. The simulation doesn't have isolated Slack and isolated Jira — it has a world where Slack messages, Jira tickets, email threads, CRM records, identity policies, and document state all share a common event bus and react to each other. A move in one surface creates observable side effects in others, and the contract system grades whether those side effects happened correctly.

This coherence is what makes the generated data useful for training, the benchmarks meaningful for evaluation, the "what if" scenarios credible for ops teams, and the test environment realistic for integration teams.

## Scale Snapshot

| Area | Current shape |
|------|---------------|
| Source tree | Hundreds of Python files under `vei/` plus a single-page Studio UI |
| Test coverage | Hundreds of pytest cases across kernel, CLI, UI, workspace, benchmark, and import flows |
| CLI surface | One `vei` entry point with grouped commands for project, world, benchmark, release, UI, and data workflows |
| Simulated surfaces | ~15 major enterprise surfaces (Slack, Mail, Browser, Docs, Tickets, CRM, ERP, Identity, ServiceDesk, SIEM, HRIS, PagerDuty, Feature Flags, Spreadsheet, Calendar) |
| Built-in company verticals | 5 |
| Scenario variants | ~25 |
| Difficulty tiers | 4 (p0-easy → pX-adversarial) |
| Frontier rubric scenarios | 9 |
| Context providers | 6 (Slack, Gmail, Teams, Jira, Google, Okta) |
