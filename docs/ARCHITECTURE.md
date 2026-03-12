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
- `ImportPackage`
  - raw file-based intake package with source manifests, mapping profiles, redaction state, and provenance anchors
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
Workspace / CLI / UI / SDK / Agent
                │
                ▼
         Project + Run surfaces
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

## Product Workflow Layer

VEI now has a product-shaped layer above the kernel:

- `vei.workspace`
  - file-backed workspace/project model
  - blueprint asset, contract, scenario, compile records, and run registry
- `vei.run`
  - unified run manifest, normalized timeline, snapshot references, and contract summary
- `vei.ui`
  - local FastAPI + SSE playback/debug app over workspace and run APIs
  - control-room style playback surface for launch, timeline, contract, graph, and snapshot inspection
- `vei.visualization`
  - shared flow/timeline shaping for CLI and UI playback surfaces

The intended loop is:

1. create or import a workspace
2. compile or refresh runnable scenario artifacts when the workspace changes
3. launch a run
4. inspect orientation, graphs, timeline, snapshots, and diffs
5. replay, branch, or export from there through the current expert surfaces such as `vei-world`, `vei-visualize`, and release/export tooling

Imported identity workspaces now add an earlier preparation ladder:

1. validate an `ImportPackage`
2. normalize it into a `GroundingBundle`
3. compile into workspace artifacts
4. generate scenario candidates and bootstrap contracts
5. launch runs against the generated scenarios
6. inspect diagnostics and provenance in the same workspace/UI flow

## Stable Python Surfaces

- `vei.world.api`
  - `create_world_session`
  - `observe`
  - `orientation`
  - `call_tool`
  - `capability_graphs`
  - `graph_plan`
  - `graph_action`
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
- `vei.capability_graph`
  - runtime shared-domain graph views derived from live world state and builder metadata
  - central graph-native planning and mutation surface for agents
  - shared identity/doc/work/comm/revenue/data/obs/ops graph surfaces for inspection and action
- workflow runner / benchmark baselines
  - flagship workflows can now compile graph-native steps to `vei.graph_action` and resolve to concrete twins only at execution time
- `vei.orientation`
  - agent-facing summaries derived from live world state, capability graphs, and builder hints
  - visible surfaces, policy hints, key objects, suggested focuses, and next questions
- `vei.grounding`
  - typed grounding bundles for imported organization, policy, and incident input
  - compilers that turn grounding bundles into `BlueprintAsset` authoring roots
- `vei.imports`
  - canonical raw import package format for offline CSV/JSON enterprise exports
  - mapping profiles, validation reports, provenance, redaction reports, and scenario generation over normalized identity environments
- `vei.contract`
  - contract builders and evaluators
- `vei.workspace`
  - create/import/show/compile workspaces
  - scenario/contract authoring helpers, import diagnostics, provenance access, and run registry
- `vei.run`
  - launch runs from a workspace
  - canonical per-run manifest, timeline, and snapshot APIs
- `vei.ui`
  - local playback/debug server for workspace runs

## Supported Entry Points

- `python -m vei.router`
  - stdio MCP transport
  - agent-facing discoverability tools now include `vei.orientation`, `vei.capability_graphs`, `vei.graph_plan`, and `vei.graph_action`
- `python -m vei.router.sse`
  - SSE MCP transport
- `vei-world`
  - snapshot/receipt inspection plus runtime capability-graph and orientation rendering
- `vei`
  - top-level product workflow entrypoint
  - `project`, `contract`, `scenario`, `run`, `inspect`, and `ui` groups
- `vei-ui`
  - standalone alias for the local playback/debug server
  - equivalent to `vei ui serve`
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
- Prefer `ImportPackage -> GroundingBundle -> BlueprintAsset -> CompiledBlueprint -> WorldSession` when working from real or sanitized enterprise exports.
- Prefer `WorldSession -> capability_graphs() -> graph_plan() -> graph_action()` as the main agent-facing planning/mutation ladder inside a live world.
- Use `orientation()` to help agents discover the world before they begin mutating it.
- Prefer graph-native workflow steps for long-horizon playbooks when the intent is domain-level mutation rather than a specific vendor surface.
- Prefer semantic environment building first. VM-backed or OS-level facades are future plugin substrates, not the core runtime model.
- Preserve imported-vs-derived-vs-simulated provenance through normalization, workspace storage, run timelines, and UI inspection.
