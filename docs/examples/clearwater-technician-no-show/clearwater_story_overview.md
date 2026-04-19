# VEI Story · Clearwater Field Services

VEI is the shared world kernel underneath every company world we open: the company-specific part is just the capability graph and contract, while the runtime, event spine, branching, replay, and inspection surfaces stay the same.

## Company

- World: `Service Operations`
- Company: `Clearwater Field Services`
- Briefing: Clearwater Field Services coordinates field dispatch, account handling, billing follow-through, and exception management across a high-volume service business.
- Why failure matters: If this scenario fails, Clearwater misses a VIP SLA, aggravates an open billing dispute, and turns one bad morning into churn and manual cleanup.

## Situation

- Scenario: `default`
- Scenario variant: `technician_no_show`
- Situation briefing: Billing is stable, but the assigned technician drops out and the dispatch bench has to recover the day quickly. Why this matters: Shows that the same service company can pivot into a dispatch-first mission without rebuilding the world.

## Objective

- Contract variant: `protect_customer_trust`
- Objective briefing: Keep the customer-facing truth clean: dispatch, billing, and manager escalation should all tell the same story. Why it exists: Shows the same service loop optimized around trust, coordination, and low managerial thrash.
- Presentation manifest: `/Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-technician-no-show/presentation_manifest.json`
- Presentation guide: `/Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-technician-no-show/presentation_guide.md`

## Runs

- Workflow baseline: `workflow_baseline`
- Comparison (scripted): `scripted_comparison`
- Workflow contract ok: `True`
- Comparison contract ok: `False`
- UI: `python -m vei.cli.vei ui serve --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-technician-no-show --host 127.0.0.1 --port 3011`

## Branch Story

- Base world: Clearwater Field Services coordinates field dispatch, account handling, billing follow-through, and exception management across a high-volume service business.
- Chosen situation: Billing is stable, but the assigned technician drops out and the dispatch bench has to recover the day quickly.
- Chosen objective: Keep the customer-facing truth clean: dispatch, billing, and manager escalation should all tell the same story.
- Baseline branch: Recover the route with the right backup technician
- Agent branch: Keep the work unassigned and let the SLA slip

What changed:
- The same Clearwater Field Services world stayed fixed while the `technician_no_show` situation overlay and `protect_customer_trust` objective overlay defined this run.
- The workflow baseline resolved the world in `20` events and `7` graph actions; the comparison run took `8` events and `0` graph actions.
- Both runs used the same kernel surfaces, but the alternate path touched different tools and ended with `6` contract issue(s) instead of `0`.

Why the outcome matters:
- The baseline passes because the company world, situation, and objective all stay aligned: `Protect Customer Trust` rewards the right business behavior for `Technician No-Show`.
- The comparison path fails meaningfully, which is the point of the kernel: same company, different decisions, different outcome.
- If this scenario fails, Clearwater misses a VIP SLA, aggravates an open billing dispute, and turns one bad morning into churn and manual cleanup.

## Export Preview

### RL Episode Export

One world state plus one event spine already yields state transitions, graph-native actions, contract-shaped rewards, and branch boundaries that can later become trainable RL episodes.

### Continuous Eval Export

The same company world, situation, and objective can be replayed as a baseline/comparison pair, which makes VEI naturally usable as a continuous eval harness later.

### Agent Ops Export

Playback, resolved tools, graph domains, receipts, and contract findings already form an agent-observability bundle that can later become an operations and governance surface.
