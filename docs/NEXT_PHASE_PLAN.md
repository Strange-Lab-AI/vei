# Next Phase Plan

## Summary

VEI is now a real semantic environment builder:

- import or author an environment
- compile it into a runnable workspace
- generate or refine scenarios
- bootstrap or edit contracts
- run agents
- inspect graphs, snapshots, provenance, and timeline state
- watch it in a local UI

That is a real product shape.

The next moat is not "more facades." It is making VEI excellent at turning messy enterprise inputs into runnable, inspectable, contract-graded worlds, while tightening the runtime so there is one canonical execution and event spine beneath everything.

The next phase should therefore focus on two linked outcomes:

1. make `vei.run` and the event model the one true execution spine
2. make grounded import and graph-native world compilation strong enough for messy real-company data

This is the shortest path from "good internal platform" to "serious product for enterprise agent testing, training, and red-teaming."

## Simple View

At the highest level, the next phase is:

```text
messy enterprise exports
    -> validated import package
    -> normalized grounding bundle
    -> compiled workspace
    -> generated scenarios + contracts
    -> canonical runs + event stream
    -> snapshots / replay / UI / exports
```

If this phase goes well, VEI becomes much more ready for the first real customer environment without needing to reinvent the engine when those inputs arrive.

## Why This Phase Matters

Three things are true at the same time:

1. The engine is already strong.
2. The product workflow is finally coherent.
3. The main remaining risk is that too much of the runtime is still split across benchmark/run/timeline layers and too much of the grounding path still assumes relatively clean, curated inputs.

So the next phase should make VEI:

- more canonical internally
- more tolerant of real-world messiness
- more legible to agents and operators
- more ready for tenant-specific environments

## What We Should Expect To Get From Real Companies

We should plan around the kinds of inputs enterprises can realistically hand us first, not the ideal final ingestion story.

Most plausible early inputs:

- Identity exports
  - users, groups, org units, managers, roles, app assignments, SCIM mappings
- Policy/config exports
  - access policies, approval rules, break-glass rules, sharing defaults
- Document/share posture
  - owners, ACLs, external shares, oversharing findings
- HRIS/org data
  - employee records, departments, managers, status changes
- Tickets/change/incident records
  - Jira, ServiceNow, cutover/change requests, approval workflows
- Admin/audit logs
  - provisioning changes, policy decisions, admin actions
- Communications and artifacts
  - incident summaries, approval threads, runbooks, docs
- Optional business context
  - CRM ownership and customer-impact context

The early product should assume these inputs are:

- incomplete
- inconsistent
- partially redacted
- exported in ad hoc CSV/JSON shapes
- sometimes contradictory

That means the compiler layer must be built for ambiguity, provenance, and review, not only for happy-path structured data.

## North-Star User Flow

This is the user journey we should be optimizing for by the end of the phase:

1. Bring VEI a folder of sanitized exports.
2. Review what VEI understood, what it could not map, and what needs overrides.
3. Compile that into a workspace with capability graphs, contracts, and scenario candidates.
4. Choose or edit a scenario.
5. Launch workflow and agent runs against it.
6. Watch the timeline, inspect graph mutations, branch, replay, and compare outcomes.
7. Export the resulting eval, regression, or training artifacts.

If we can do that cleanly for identity/access-governance, we will have the right product foundation.

## Phase Name

**Execution Spine + Grounded Identity Compiler**

## Phase Goals

### Goal 1: One true execution and event spine

`vei.run` should become the canonical run lifecycle and event stream for:

- workflow runs
- scripted runs
- BC runs
- live LLM runs
- replay/branch flows

Everything else should adapt to it rather than inventing parallel artifact shapes.

### Goal 2: Import messy identity environments well

`ImportPackage` and grounding should evolve from "good curated fixture flow" to "good messy real-export flow."

### Goal 3: Make capability graphs more authoritative

Capability graphs should keep moving from "useful read abstraction" toward "main internal world model," with app twins treated more clearly as adapter surfaces.

### Goal 4: Keep the identity/access-governance wedge as the first fully productized motion

This wedge still gives the strongest combination of:

- crisp invariants
- high enterprise pain
- cross-tool coordination
- hidden state
- async approvals
- compliance/audit value

## Workstreams

## Workstream 1: Canonical Run Spine

### Objective

Make `vei.run` the single execution contract for the platform.

### Why

Right now, runs are good enough for product use, but the internal execution story is still partly mediated by benchmark and artifact layers. That creates unnecessary complexity for:

- live playback
- reproducibility
- branch/replay
- export
- SDK stability

### Key Changes

- Introduce one canonical `RunEvent` stream emitted during execution, not reconstructed afterward.
- Standardize event types for:
  - run started
  - observation rendered
  - graph plan issued
  - graph action requested
  - tool resolved
  - tool result
  - contract updated
  - snapshot created
  - branch created
  - replay started/completed
  - intervention injected
  - run completed/failed
- Make workflow/scripted/BC/LLM runners write through the same run lifecycle and event sink.
- Have UI timeline, `vei inspect events`, and exports all read from the same event stream.
- Move benchmark/demo helpers to adapt to the canonical run spine rather than being an alternate execution path.

### Acceptance

- One run manifest format across runner types.
- One event timeline format across runner types.
- UI live playback and post-run playback render from the same event stream.
- Replay and branch metadata are visible in the same event sequence instead of as disconnected artifacts.

## Workstream 2: Capability Graph Authority

### Objective

Make capability graphs more central to state mutation, workflow compilation, and inspection.

### Why

We already proved graph-native planning and actions are useful. The next step is to reduce the gap between graph-native control and underlying app-shaped state ownership.

### Key Changes

- Continue migrating flagship workflows to graph-native steps by default.
- Move more scenario compilation through graph/domain objects instead of direct app-state seeding.
- Add clearer graph mutation provenance:
  - requested graph intent
  - resolved adapter/tool
  - state objects affected
  - downstream receipts
- Strengthen domain-level validation for:
  - identity policies
  - doc sharing posture
  - approval completion
  - work-item state

### Acceptance

- Identity wedge workflows are predominantly graph-native.
- Scenario generation and world inspection can speak in capability-domain terms first.
- UI can show graph mutations and downstream tool realizations in one coherent story.

## Workstream 3: ImportPackage v2

### Objective

Make the import pipeline robust to messy, incomplete, partially contradictory enterprise exports.

### Why

This is the most important "ready for real customers" preparation work we can do before having live tenant data in hand.

### Key Changes

- Add richer source manifests:
  - source system
  - extraction time
  - file version
  - mapping profile
  - redaction status
  - provenance anchors
- Add more mapping profiles for:
  - Okta-like identity exports
  - Google Admin/Drive posture
  - HRIS exports
  - Jira/ServiceNow approvals and change records
  - optional CRM ownership context
- Add ambiguity handling:
  - unresolved references
  - duplicate identities
  - conflicting assignments
  - missing managers/org units
  - policy inconsistencies
- Add review-mode outputs:
  - what was mapped
  - what was inferred
  - what was dropped
  - what needs override
- Improve override scaffolding and make it safer to iterate repeatedly on the same package.

### Acceptance

- Fixture packages with intentional ambiguity produce useful diagnostics rather than brittle failures.
- Override scaffolds are enough to unblock normalization without manual code changes.
- Redaction and provenance survive review -> normalize -> workspace import.

## Workstream 4: Policy + Contract Compiler

### Objective

Turn imported policy and ACL posture into stronger tenant-shaped contract defaults.

### Why

A key part of the moat is not only compiling a world, but compiling the right success and failure conditions for that world.

### Key Changes

- Strengthen contract bootstrapping from:
  - entitlement policy
  - sharing policy
  - approval policy
  - offboarding / mover rules
  - break-glass handling
- Distinguish:
  - imported policy facts
  - inferred contract defaults
  - user-edited contract rules
- Add contract explanation surfaces:
  - why this predicate exists
  - which imported sources justified it
  - which objects it applies to

### Acceptance

- Generated contracts feel tenant-shaped rather than generic.
- UI and CLI can show the source of a bootstrapped rule.
- Missing policy context fails loudly when contract generation would otherwise be misleading.

## Workstream 5: Scenario Generation From Partial Environments

### Objective

Generate better scenario candidates from partially complete imported environments.

### Why

Real customer data will rarely arrive as a perfect, fully explained incident world. We need to synthesize strong scenario candidates from the topology and hints we do have.

### Key Changes

- Improve generated identity/access scenarios:
  - joiner/mover/leaver
  - stale entitlement cleanup
  - oversharing remediation
  - approval bottlenecks
  - break-glass cleanup
  - acquired-user cutovers
- Allow hidden-fault toggles and scenario deltas even when the import only gives topology and policy posture.
- Add scenario scoring/prioritization so the UI can show "best candidate scenarios to try first."

### Acceptance

- One imported environment can yield several plausible, runnable scenario candidates.
- Scenario generation works with partial exports, not just richly populated fixture packs.
- Activated scenarios clearly preserve which parts are imported, derived, or simulated.

## Workstream 6: Workspace / Imports / SDK Decomposition

### Objective

Reduce successful-growth complexity in the largest modules.

### Why

The repo is now conceptually coherent, but several important modules have become too large and are carrying too many responsibilities.

### Key Changes

- Split `vei.run.api` into smaller modules for:
  - launch/preflight
  - event stream
  - manifests
  - replay/branch helpers
- Split `vei.imports.api` into:
  - validation/review
  - normalization
  - overrides
  - provenance/redaction
  - scenario generation
- Split `vei.workspace.api` into:
  - project lifecycle
  - compile/import
  - scenario/contract activation
  - run registry
- Split `vei.sdk.api` into a clearer public surface for:
  - workspace
  - run
  - import
  - inspect

### Acceptance

- Public APIs stay stable while internal files get smaller and clearer.
- `mypy` and test coverage remain green during the split.

## Workstream 7: Identity Wedge Productization

### Objective

Turn the identity/access-governance wedge into the clearest commercial motion in the repo.

### Why

This is the best wedge to make VEI look like a product instead of an impressive lab project.

### Key Changes

- Add one polished end-to-end identity demo path based on imported fixture packs.
- Refine the UI to make identity-specific posture obvious:
  - entitlement drift
  - external sharing exposure
  - approvals outstanding
  - break-glass history
- Tighten wording and docs around:
  - what problem VEI solves first
  - what data it expects
  - what outputs users get

### Acceptance

- A new user can understand the identity wedge without reading deep architecture docs.
- One canonical demo shows:
  - import
  - normalization
  - generated scenarios
  - bootstrapped contract
  - workflow baseline
  - freer agent run
  - branch/replay/snapshot inspection

## Recommended Sequence

1. Canonical Run Spine
2. ImportPackage v2
3. Policy + Contract Compiler
4. Scenario Generation From Partial Environments
5. Capability Graph Authority
6. Workspace / Imports / SDK Decomposition
7. Identity Wedge Productization

This order is deliberate:

- first make runtime truth clearer
- then make imported environments stronger
- then make the wedge sharper and easier to sell

## What Not To Do Next

These are plausible ideas, but they are not the right next move:

- broad new SaaS/app connector expansion
- VM or desktop/OS-backed environments as a core milestone
- fancy cloud deployment work
- multi-tenant production control plane work
- lots of new benchmark families
- large UI flourish work without stronger runtime grounding

Those can all matter later. They are not the shortest path to the moat.

## Deliverable At The End Of This Phase

By the end of this phase, VEI should be able to do the following cleanly:

1. Ingest a messy but realistic identity export package.
2. Tell the user what it understood, what is ambiguous, and what needs overrides.
3. Compile that into a tenant-shaped workspace with provenance.
4. Generate several plausible governance scenarios.
5. Bootstrap a contract from real policy posture.
6. Launch runs through one canonical execution and event spine.
7. Show those runs live and after the fact in the same UI.
8. Branch, replay, diff, and export the results.

That is the right next plateau.

## Concrete CLI Shape We Should Be Driving Toward

We do not need to rename everything immediately, but the intended flow should feel like this:

```bash
vei project validate-import --package <import-package>
vei project review-import --package <import-package>
vei project scaffold-overrides --package <import-package> --source-id <source>
vei project normalize --package <import-package>
vei project import --root <workspace> --package <import-package>
vei scenario generate --root <workspace>
vei scenario activate --root <workspace> --scenario-name <scenario> --bootstrap-contract
vei run start --root <workspace> --runner workflow
vei inspect events --root <workspace>
vei inspect provenance --root <workspace> --object-ref <object>
vei ui serve --root <workspace>
```

The phase should make this path more canonical, more resilient, and more obviously the main product story.

## Success Criteria

We should consider this phase successful if all of these are true:

- VEI can tolerate messy import packages without brittle failure.
- The run/event spine becomes simpler and more authoritative.
- Capability graphs feel more central, not more ornamental.
- Identity/access-governance is clearly the first serious product wedge.
- The UI tells a stronger story about imported facts, simulated facts, policy, action, and outcome.
- We feel ready to accept a real company export package without needing to rethink the architecture.
