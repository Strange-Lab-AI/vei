# VEI Service Operations Pack — Control Plane and What-If Walkthrough

**Clearwater Field Services** is the built-in synthetic rig for VEI's control-room story: a VIP outage, technician no-show, and billing dispute all collide before 9 AM, while governed agents and a human operator respond through the same control surface.

This walkthrough describes the current Studio experience for the `service_ops` pack with governor mode enabled. Use Enron for the flagship real-history example. Use Clearwater when you want the built-in synthetic kernel and governor path.

1. **Control plane** — governed agent activity with visible denials, approval holds, connector status, and operator controls.
2. **What-if replay** — change a small set of service-ops policy knobs, replay from the same starting point, and compare the new outcome side by side.

### Quick start (from a fresh clone)

```bash
make setup
vei quickstart run --world service_ops --governor-demo --no-serve
```

This launches the Clearwater Field Services world with governor mode and opens Studio. No API keys required for the built-in deterministic demo.

### What-if from the Clearwater workspace

The quickstart workspace stores its company graph inside the blueprint asset, not as a `context_snapshot.json`. To branch a historical what-if from it, first export a snapshot:

```bash
# 1. Export the workspace blueprint as a context_snapshot.json
vei whatif export --workspace _vei_out/quickstart \
  --output _vei_out/quickstart/context_snapshot.json

# 2. Search events on the resulting snapshot
vei whatif events --source company_history \
  --source-dir _vei_out/quickstart/context_snapshot.json

# 3. Materialize an episode at the chosen branch event
vei whatif open --source company_history \
  --source-dir _vei_out/quickstart/context_snapshot.json \
  --event-id <chosen_event> \
  --root _vei_out/dispatch_whatif

# 4. Run the counterfactual experiment (heuristic if no LLM key, both if key present)
vei whatif experiment --source company_history \
  --source-dir _vei_out/quickstart/context_snapshot.json \
  --artifacts-root _vei_out/dispatch_whatif \
  --label dispatch_t1 \
  --thread-id "jira:JRA-CFS-10" \
  --event-id "jira:JRA-CFS-10:state" \
  --counterfactual-prompt "What if dispatch had immediately escalated to a regional supervisor and pre-authorized the after-hours premium tech?" \
  --mode heuristic_baseline
```

That export now writes `canonical_events.jsonl` and `canonical_event_index.json` beside the snapshot, so the Company Timeline view and the readiness commands can read the same file-backed history immediately.

No API keys required for `heuristic_baseline` mode. Add `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, or `OPENROUTER_API_KEY` to `.env` and use `--mode both` for LLM-driven counterfactual continuations alongside the forecast.

### Repo-owned synthetic bundles

The repo now includes three saved Clearwater bundles under `docs/examples/`:

- `clearwater-dispatch-recovery`
- `clearwater-billing-dispute-reopened`
- `clearwater-technician-no-show`

Open one directly in Studio:

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

---

## 1. Entering the World

The Studio opens on the **Company** tab with the **Live Company** section in focus. The top of the screen still centers the company identity and mission controls:

- **Company**: Clearwater Field Services
- **Crisis**: one of the built-in service-ops crisis variants
- **Success means**: an objective variant such as Protect SLA, Protect Revenue, or Protect Customer Trust

Below that header, governor-enabled runs show a persistent **mode indicator** so the operator can immediately tell they are looking at governed agent traffic rather than an unguided simulation.

---

## 2. Situation Room Strip

The situation room remains the fastest summary of business state. It shows six cells:

- **Systems** — how many major surfaces are in play and whether one is in trouble
- **Exceptions** — active operational exceptions
- **Policy** — whether the run is still on the default rules or has drifted
- **Approvals** — pending approvals visible to the operator
- **Deadline** — time or action-budget pressure
- **Risk** — overall mission risk level

This strip updates after both human moves and governor-governed agent activity, so the operator sees operational state and governance state move together.

---

## 3. Control Room — The Main Governor Story

The **Control Room** panel is the main governor surface in the Company view.

In the built-in service-ops demo, three governed agents are pre-registered:

| Agent | Profile | Main surfaces | Role in demo |
|---|---|---|---|
| Dispatch Bot | `operator` | slack, service_ops | posts updates and reroutes work |
| Billing Bot | `observer` | slack, service_ops | can read and surface context but cannot make risky changes |
| Control Lead | `approver` | slack, service_ops, jira | resolves approval holds |

The panel now does more than show raw activity:

- **Agent cards** show the agent name, role, current status, allowed surfaces, last action, blocked count, and throttled count at a glance. Each card has a policy badge. Edit controls (profile, status, surfaces, save, remove) are collapsed behind a "Configure" disclosure so the default view is a monitoring dashboard rather than a settings form.
- **Connector strip** shows Slack, Jira, Graph, and Salesforce with readable status such as simulated interactive, live interactive, live read-only, or unsupported.
- **Approval queue** shows all approvals — pending items first with a gold "held" badge, then resolved items with green "approved" or red "rejected" badges — so you can see the full approval history, not just the current queue.
- **Activity log** shows allowed actions, blocked actions, held actions, and throttled actions with relative timestamps (e.g. "2m ago"). The feed shows the 20 most recent events with a "Show all" disclosure for the rest.
- **Register Agent form** is collapsed behind a "Register governed agent" disclosure to keep the panel clean by default.
- **Inline operator controls** let the human add an agent, change profile, update allowed surfaces, and set status without leaving Studio.

This is the current control-plane moment: you can explain who is acting, what VEI allowed, what VEI blocked, and why.

---

## 4. The Governed Demo Beats

The built-in governor demo is no longer just ambient activity. It stages a clear governance story:

1. Dispatch Bot posts an allowed Slack update.
2. Dispatch Bot performs an allowed dispatch reassignment.
3. Billing Bot performs an allowed service-ops billing hold.
4. Dispatch Bot posts a follow-up Slack update.
5. Billing Bot attempts an action on an unallowed surface and is blocked.
6. A risky `service_ops.update_policy` action is held for approval.
7. Control Lead approves the held action from the control plane.
8. A follow-up action proceeds after approval.
9. A read on Jira/tickets shows broader surface visibility without pretending every surface is fully live-write capable.

These actions appear both in the Control Plane activity log and in the run timeline, so operators can see governance and business consequences in one place.

---

## 5. The Live Company Section

The rest of the Company view still shows the full business wall:

- **Slack** for dispatch and leadership coordination
- **Email** for customer and internal communication
- **Work tracker** for tickets and operational tasks
- **Documents** for runbooks and account context
- **Approvals** for business-side requests
- **Service loop** for the domain-specific service-ops state

Governor-controlled actions show up here as normal business activity. That is the key product trick: the control plane is not a side dashboard disconnected from the company simulation. It governs actions that visibly change the company world.

---

## 6. Mission Play Still Matters

The mission controls still let a human play the scenario directly:

- move cards remain categorized by availability and risk
- the score, budget, and objective progress update after each move
- risky human moves still use the explicit policy-override ceremony

That means the `service_ops` pack can still demo both halves of VEI:

- **human-in-the-loop mission play**
- **governed agent activity through governor mode**

---

## 7. Crisis and Outcome Views

The **Crisis** tab still explains the business problem and why it matters.

The **Outcome** tab is where the second major product moment now lives:

- contract evaluation and success checks
- decision audit trail
- snapshot cards with **Fork from here**
- side-by-side **Compare Paths**
- snapshot pickers for both comparison paths
- cross-run snapshot comparison grouped by domain
- **Try Different Policy** for replayable `service_ops` runs

---

## 8. What-If Replay

For `service_ops`, the Outcome view now supports a focused replay flow rather than a generic “change anything” editor.

Click **Try Different Policy** and Studio opens a compact form for exactly four supported policy knobs taken from the run's initial snapshot:

- `approval_threshold_usd`
- `vip_priority_override`
- `billing_hold_on_dispute`
- `max_auto_reschedules`

When the operator submits the form, VEI:

1. branches from the same starting snapshot
2. applies the named policy change
3. replays the baseline path against that modified world
4. opens compare mode with the original run on one side and the replayed run on the other

This is intentionally narrow and believable. The feature is saying, “change a few important business rules and see the new outcome,” not “edit the whole world.”

---

## 9. Snapshots, Forking, and Compare

Snapshot and compare behavior is broader now than the original demo:

- **Fork from here** works from any run snapshot, not just the currently active mission. A compact fork card also appears in the Company rail when snapshots exist, so the operator can branch without switching tabs.
- **Compare Paths** uses explicit run pickers and snapshot pickers for both sides. When entering compare mode, the current run and the most recent other run are auto-selected.
- **Compare snapshots** compares the selected snapshot pair rather than always using the latest snapshot. It frames the result as what changed between snapshots, which is easier to explain in business terms. The comparison area is taller (600px) so more of the changes are visible without scrolling.

This makes the sandbox story easier to explain:

- one branch can be the baseline
- one branch can be a human alternative
- one branch can be a policy replay

---

## 10. Cross-Run Snapshot Comparison

The cross-run diff is now cleaner and more business-readable:

- branch-local runtime noise is stripped out
- governor runtime internals do not dominate the diff
- field paths are humanized
- booleans are rendered as toggles
- numeric values are shown as deltas where useful

The point is not to dump raw JSON. The point is to show how two strategies produced meaningfully different company states.

---

## Demo Script

### Demo 1 — Control Plane

Use this moment to show that VEI is governing real-looking work, not just visualizing it:

1. Start `service_ops` with `--governor-demo`.
2. Show the mode banner and the situation room.
3. Point out the three governed agents and their policy badges.
4. Explain the connector strip: Slack is the live-first story, the other surfaces are still governed but may be read-only or unsupported for live writes.
5. Let the staged actions play until you can show:
   - one allowed action
   - one blocked action
   - one approval-held action
6. Approve the held action from the approval queue.

### Demo 2 — What-If Replay

Use this moment to show that VEI can replay the same company from the same start with different rules:

1. Open the Outcome tab on a completed `service_ops` run.
2. Click **Try Different Policy**.
3. Change `billing_hold_on_dispute` and `approval_threshold_usd`.
4. Replay the path.
5. Compare the original and replayed runs side by side.
6. Click **Compare snapshots** and explain how the business outcome changed.

---

## Running It

```bash
# Standard mode (no governed agents)
vei quickstart run --world service_ops

# Governor demo with staged governed agents
vei quickstart run --world service_ops --governor-demo

# Slack-first live alpha
vei quickstart run --world service_ops --connector-mode live
```
