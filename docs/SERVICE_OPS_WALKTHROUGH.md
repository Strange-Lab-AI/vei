# VEI Service Operations Pack — Control Plane and What-If Walkthrough

**Clearwater Field Services** is the strongest built-in demo for VEI's current product story: a VIP outage, technician no-show, and billing dispute all collide before 9 AM, while mirror-governed agents and a human operator respond through the same control surface.

This walkthrough describes the current Studio experience for the `service_ops` pack with mirror mode enabled. It focuses on the two demo moments the product now supports well:

1. **Control plane** — governed agent activity with visible denials, approval holds, connector status, and operator controls.
2. **What-if replay** — change a small set of service-ops policy knobs, replay from the same starting point, and compare the new outcome side by side.

---

## 1. Entering the World

The Studio opens on the **Living Company** view. The top of the screen still centers the company identity and mission controls:

- **Company**: Clearwater Field Services
- **Crisis**: one of the built-in service-ops crisis variants
- **Success means**: an objective variant such as Protect SLA, Protect Revenue, or Protect Customer Trust

Below that header, mirror-enabled runs show a persistent **mode indicator** so the operator can immediately tell they are looking at governed agent traffic rather than an unguided simulation.

![Header, controls, and mirror mode indicator](assets/service_ops/01_header_and_controls.png)

---

## 2. Situation Room Strip

The situation room remains the fastest summary of business state. It shows six cells:

- **Systems** — how many major surfaces are in play and whether one is in trouble
- **Exceptions** — active operational exceptions
- **Policy** — whether the run is still on the default rules or has drifted
- **Approvals** — pending approvals visible to the operator
- **Deadline** — time or action-budget pressure
- **Risk** — overall mission risk level

This strip updates after both human moves and mirror-governed agent activity, so the operator sees operational state and governance state move together.

![Situation room strip](assets/service_ops/02_situation_room_strip.png)

---

## 3. Control Plane — The Main Mirror Story

The **Control Plane** panel is the main mirror-mode surface in the Company view.

In the built-in service-ops demo, three mirror agents are pre-registered:

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
- **Register Agent form** is collapsed behind a "Register new agent" disclosure to keep the panel clean by default.
- **Inline operator controls** let the human add an agent, change profile, update allowed surfaces, and set status without leaving Studio.

This is the current control-plane moment: you can explain who is acting, what VEI allowed, what VEI blocked, and why.

![Control Plane panel with agent cards and activity log](assets/service_ops/03_mirror_fleet_panel.png)

---

## 4. The Governed Demo Beats

The built-in mirror demo is no longer just ambient activity. It stages a clear governance story:

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

## 5. The Living Company View

The rest of the Company view still shows the full business wall:

- **Slack** for dispatch and leadership coordination
- **Email** for customer and internal communication
- **Work tracker** for tickets and operational tasks
- **Documents** for runbooks and account context
- **Approvals** for business-side requests
- **Service loop** for the domain-specific service-ops state

Mirror-governed actions show up here as normal business activity. That is the key product trick: the control plane is not a side dashboard disconnected from the company simulation. It governs actions that visibly change the company world.

![Full company view with all surfaces](assets/service_ops/04_living_company_full.png)

---

## 6. Mission Play Still Matters

The mission controls still let a human play the scenario directly:

- move cards remain categorized by availability and risk
- the score, budget, and objective progress update after each move
- risky human moves still use the explicit policy-override ceremony

That means the `service_ops` pack can still demo both halves of VEI:

- **human-in-the-loop mission play**
- **governed agent activity through mirror mode**

![Mission control and moves](assets/service_ops/05_mission_control_moves.png)

![Policy change ceremony modal](assets/service_ops/06_policy_change_modal.png)

![After policy override](assets/service_ops/07_after_policy_override.png)

![Updated mission score](assets/service_ops/08_updated_mission_score.png)

---

## 7. Crisis and Outcome Views

The **Crisis** tab still explains the business problem and why it matters.

The **Outcome** tab is where the second major product moment now lives:

- contract evaluation and assertion results
- decision audit trail
- snapshot cards with **Fork from here**
- side-by-side **Compare Paths**
- snapshot pickers for both comparison paths
- cross-run world-state diff grouped by domain
- **Try Different Policy** for replayable `service_ops` runs

![Crisis view](assets/service_ops/09_crisis_view.png)

![Outcome view with Compare Paths and Fork buttons](assets/service_ops/10_outcome_view.png)

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
- **Diff world state** compares the selected snapshot pair rather than always using the latest snapshot. The diff container is taller (600px) so more of the diff is visible without scrolling.

This makes the sandbox story easier to explain:

- one branch can be the baseline
- one branch can be a human alternative
- one branch can be a policy replay

![Decision log and snapshots with fork buttons](assets/service_ops/11_decision_log.png)

![Outcome context cards](assets/service_ops/12_move_log.png)

![Snapshots with fork buttons and within-run diff](assets/service_ops/13_snapshots_and_fork.png)

![Path comparison with assertion diff and run pickers](assets/service_ops/14_compare_paths.png)

---

## 10. Cross-Run World-State Diff

The cross-run diff is now cleaner and more business-readable:

- branch-local runtime noise is stripped out
- mirror runtime internals do not dominate the diff
- field paths are humanized
- booleans are rendered as toggles
- numeric values are shown as deltas where useful

The point is not to dump raw JSON. The point is to show how two strategies produced meaningfully different company states.

![Cross-run world state diff grouped by domain](assets/service_ops/15_world_state_diff.png)

---

## Demo Script

### Demo 1 — Control Plane

Use this moment to show that VEI is governing real-looking work, not just visualizing it:

1. Start `service_ops` with `--mirror-demo`.
2. Show the mode banner and the situation room.
3. Point out the three mirror agents and their policy badges.
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
6. Open the world-state diff and explain how the business outcome changed.

---

## Running It

```bash
# Standard mode (no mirror agents)
vei quickstart run --world service_ops

# Mirror demo with staged governed agents
vei quickstart run --world service_ops --mirror-demo

# Slack-first live alpha
vei quickstart run --world service_ops --connector-mode live
```
