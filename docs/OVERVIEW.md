# VEI: What This Is

VEI is a programmable replica of an entire company's operational software stack. You give it a company description — or connect it to real Slack, Gmail, Jira, and Teams data — and it builds a fully functioning simulated copy of that company with working Slack channels, email threads, ticket queues, CRM pipelines, document stores, identity systems, and more. An agent or a human can then operate inside it: play crisis scenarios, train on the traces, and synthesize operational artifacts from what happened.

It is ~67,000 lines of Python (215 source files, 328 tests), a single-page Studio UI, and 28 CLI subcommands under one `vei` entry point.

## The Five Layers

### 1. Context Capture

Real enterprise data comes in. Six providers today: Slack, Gmail, Microsoft Teams, Jira, Google Workspace, Okta. Each makes real API calls using OAuth tokens, or ingests offline exports (Slack JSON archives, Gmail MBOX Takeout files). The output is a `ContextSnapshot` — a structured record of what the company looks like right now: who's talking to whom, what tickets are open, what docs exist, who has what access.

### 2. Blueprint Compilation

The snapshot gets hydrated into a `BlueprintAsset` — VEI's portable, declarative description of a company. This includes a communications graph (Slack channels, mail threads), a work graph (Jira-style tickets and workflows), a document graph, an identity graph (users, groups, app assignments, policies), and optionally revenue, ops, inventory, campaign, and property graphs. Four built-in vertical archetypes exist as ready-to-go blueprints:

- **Pinnacle Analytics** (B2B SaaS) — $480K renewal at risk, support escalation spirals, pricing deadlocks
- **Harbor Point Management** (Real Estate) — Tenant openings, vendor no-shows, lease revisions, double-booked units
- **Northstar Growth** (Marketing Agency) — Campaign launch guardrails, creative approvals, budget runaways
- **Atlas Storage Systems** (Storage/Logistics) — Capacity quotes, vendor dispatch gaps, fragmented inventory

### 3. World Simulation Engine

The blueprint compiles into a live `WorldSession` — a deterministic, branchable, replayable discrete-event simulation. The router provides 50+ MCP tools spanning every enterprise surface: `slack.send_message`, `mail.compose`, `browser.navigate`, `docs.create`, `tickets.update`, `crm.log_activity`, `okta.suspend_user`, `erp.check_inventory`, `servicedesk.resolve`, and many more. Time advances. Events fire. State changes propagate across surfaces. You can snapshot, branch, restore, replay, and diff any point in the world's history.

The connector layer routes each tool call through one of three adapters — simulated (default), replay (from recorded traces), or live (real API calls) — with policy gates classifying operations as READ, WRITE_SAFE, or WRITE_RISKY.

### 4. Playable Missions and Evaluation

On top of the simulation sits a mission system. Each vertical has multiple crisis scenarios. A mission gives you a starting world state, a set of available moves (each triggering a sequence of tool calls), success/failure contracts, and a scorecard. You can play interactively, run a scripted baseline, or let an LLM agent play — then compare the paths.

The contract system defines predicates (what must happen), invariants (what must not happen), observation boundaries (what the agent can see vs. hidden oracle state), and reward signals. The benchmark framework runs families of workflows, scores them, and supports difficulty tiers (p0-easy through pX-adversarial) plus frontier rubric scenarios (budget reconciliation, knowledge QA, cascading failures, ethical dilemmas).

A lightweight RL layer provides a Gymnasium-compatible `VEIEnv`, behavior cloning trainer, and BC policy wrapper for learning policies from demonstration traces.

### 5. Synthesis and Twin Gateway

Finished runs produce structured outputs:

- **Runbooks** — step-by-step operational procedures extracted from what actually happened
- **Training sets** — conversation, trajectory, and demonstration data formatted for fine-tuning
- **Agent configs** — system prompts, tool specs, guardrails, and success criteria for deploying agents

The twin gateway takes a `ContextSnapshot` plus a vertical archetype, merges them into a "customer twin" — a workspace that mirrors the customer's actual company but runs on VEI's simulation. It exposes compatibility surface specs (Slack-shaped, Jira-shaped, Graph-shaped, Salesforce-shaped routes) so the twin can be addressed through familiar API shapes.

## The UI

A single-page Studio interface with three views:

- **Company view** — "Living company" panels showing every surface (Slack, Mail, Docs, Tickets, CRM, etc.) updating in real time as the simulation runs. A cascade replay system auto-plays changes panel by panel. Changed systems are highlighted.
- **Timeline view** — A swim-lane causality grid showing how events propagate through enterprise surfaces (Slack, Email, Tickets, Docs, Approvals, Business Core) over time. Each column is a move/turn, each row is a surface. Click any event node for detailed payload inspection. Move column headers highlight to isolate causal chains. Compare mode stacks two run timelines for side-by-side analysis.
- **Connect panel** — Shows which live data sources are configured with status indicators and one-click capture.

A developer mode toggle exposes run forms, raw JSON, orientation data, capability graphs, snapshots, and timeline events.

### Pilot Console

The Pilot Console is a separate operator sidecar at `/pilot` that provides the fastest path for an outside agent (or a researcher) to connect. One command (`vei pilot up`) starts the twin gateway and Studio, writes a launch manifest with bearer token and curl snippets, and serves the Pilot Console where the operator can watch live agent activity, check outcome status, and reset or finalize runs.

![VEI Pilot Console](../docs/assets/vei_pilot_console.png)

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

### Path 3: UI Connect panel

Start the Studio UI and click "Connections" in the Views menu. The panel shows each provider's status (configured or missing). If tokens are set, click "Capture Now" to pull live data.

### From snapshot to playable world

Once you have a snapshot, hydrate it into a blueprint and build a workspace:

```bash
vei context hydrate --snapshot acme_snapshot.json --output acme_blueprint.json
vei twin build --root _vei_out/twins/acme --snapshot acme_snapshot.json --organization-domain acme.com
vei ui serve --root _vei_out/twins/acme
```

The twin builder merges your captured data with the closest vertical archetype (it picks based on which data surfaces are present) and produces a workspace you can run scenarios against, play missions in, or generate training data from.

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

**Entry point:** `vei llm-test --provider openai --model gpt-5 --task "..."`

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

**Entry point:** `vei eval-frontier run --scenario-set reasoning --model gpt-5`

### 5. Platform / Integration Teams

**Pain:** "We're building AI features that integrate with Slack, Jira, Salesforce, etc. Testing against real APIs is slow, expensive, flaky, and requires sandbox accounts. We need a local replica that behaves like the real thing."

**How they use VEI:** The twin gateway exposes provider-shaped API routes (Slack-compatible, Jira-compatible, Graph-compatible) backed by the simulation. Their integration code talks to VEI exactly like it would talk to the real service, but everything is local, deterministic, and inspectable.

**Entry point:** `vei twin serve --root workspace --port 3020`

### Quick-start for all personas: the Pilot stack

Any of the above can skip the per-step setup by running `vei pilot up`, which builds a twin, starts the gateway and Studio, and writes a manifest with bearer token, curl snippets, and a sample client script. The Pilot Console at `/pilot` shows connection details, live agent activity, outcome status, and reset/finalize controls — one command to get from zero to a working enterprise twin.

---

## What Holds It Together

The thing that makes VEI more than a collection of mock APIs is that it's **one connected system**. The simulation doesn't have isolated Slack and isolated Jira — it has a world where Slack messages, Jira tickets, email threads, CRM records, identity policies, and document state all share a common event bus and react to each other. A move in one surface creates observable side effects in others, and the contract system grades whether those side effects happened correctly.

This coherence is what makes the generated data useful for training, the benchmarks meaningful for evaluation, the "what if" scenarios credible for ops teams, and the test environment realistic for integration teams.

## Numbers

| Metric | Count |
|--------|-------|
| Python source files | 219 |
| Lines of Python | ~68,000 |
| Test files | 77 |
| Tests | 333 |
| CLI subcommands | 29 |
| Simulated enterprise surfaces | ~15 major (Slack, Mail, Browser, Docs, Tickets, CRM, ERP, Identity, ServiceDesk, SIEM, HRIS, PagerDuty, Feature Flags, Spreadsheet, Calendar) |
| Built-in company verticals | 4 |
| Scenario variants | ~25 |
| Difficulty tiers | 4 (p0-easy → pX-adversarial) |
| Frontier rubric scenarios | 9 |
| Context providers | 6 (Slack, Gmail, Teams, Jira, Google, Okta) |
| UI (app.js) | ~3,500 lines |
| UI (styles.css) | ~3,200 lines |
