# VEI Architecture

VEI is a deterministic, MCP-native enterprise simulator built around one stable boundary: `WorldSession`.

## Core Primitives

- `Blueprint`
  - authored asset compiled into scenario, facades, workflow, contract, and run defaults
- `BlueprintAsset`
  - authoring root for scenario templates, capability-graph or environment seed data, facade requirements, and workflow defaults
- `CompiledBlueprint`
  - resolved facade/state-root graph plus workflow/contract/run defaults
- `GroundingBundle`
  - typed imported org/policy/incident bundle that compiles into a `BlueprintAsset`
- `Scenario`
  - seeded enterprise world plus manifest metadata
- `Facade`
  - typed enterprise surface grouped by capability domain
- `Contract`
  - explicit success predicates, forbidden predicates, observation boundary, policy invariants, reward terms, and intervention rules
- `Run`
  - workflow, benchmark, demo, showcase, and suite executions
- `Snapshot`
  - branchable world-state checkpoint over the kernel

## Runtime Shape

```text
Agent / SDK / CLI
        │
        ▼
  Router transport layer
        │
        ▼
   WorldSession kernel
        ├─ world state
        ├─ event queue
        ├─ actor state
        ├─ receipts
        ├─ snapshots / branch / restore
        └─ replay / injection
```

The router is a transport and tool-dispatch adapter. Mutable enterprise state belongs to the kernel, not to transport wrappers.

## Stable Python Surfaces

- `vei.world.api`
  - `create_world_session`
  - `observe`
  - `call_tool`
  - `snapshot`
  - `restore`
  - `branch`
  - `replay`
  - `inject`
  - `list_events`
  - `cancel_event`
- `vei.sdk`
  - `create_session`
  - scenario/facade/blueprint/benchmark manifest helpers
  - release/export helpers
  - workflow compile/run helpers
- `vei.blueprint`
  - authored blueprint assets and compiled blueprints
  - environment-builder examples and blueprint-to-world session creation
  - typed facade catalog backed by a facade plugin contract
- `vei.grounding`
  - typed grounding bundles for imported organization, policy, and incident input
  - compilers that turn grounding bundles into `BlueprintAsset` authoring roots
- `vei.contract`
  - contract builders and evaluators

## Supported Entry Points

- `python -m vei.router`
  - stdio MCP transport
- `python -m vei.router.sse`
  - SSE MCP transport
- `vei-world`
  - snapshot/receipt inspection
- `vei-llm-test`, `vei-eval`, `vei-eval-frontier`, `vei-report`
  - evaluation and benchmarking

## Software Twins

- Collaboration: Slack, Mail
- Knowledge: Browser, Docs
- Operations: Tickets, ServiceDesk
- Identity and control plane: Okta-style identity, Google Admin, SIEM, Datadog, PagerDuty, feature flags
- Business systems: ERP, CRM, HRIS, Jira-style issues
- Office/data surfaces: Spreadsheet

## Capability Domains

VEI keeps the current router twins, but the public ontology now groups them as facades under capability domains:

- `comm_graph`
  - Slack, Mail, Calendar
- `doc_graph`
  - Browser, Docs
- `work_graph`
  - Tickets, ServiceDesk, Jira
- `identity_graph`
  - Identity, Google Admin, HRIS
- `revenue_graph`
  - CRM
- `obs_graph`
  - SIEM, Datadog, PagerDuty
- `ops_graph`
  - Feature flags, ERP
- `data_graph`
  - Database, Spreadsheet

## Design Rules

- New mutable state belongs under `vei.world`.
- Cross-module usage should go through typed `api.py` surfaces.
- All actor outputs should enter the world through typed events so snapshot/replay stays deterministic.
- New software environments should register as facade plugins before they become public blueprint/compiler surfaces.
- Prefer `GroundingBundle -> BlueprintAsset -> CompiledBlueprint -> WorldSession` as the environment-builder path.
- Prefer semantic environment building first. VM-backed or OS-level facades are future plugin substrates, not the core runtime model.
