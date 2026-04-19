# VEI Story · Clearwater Field Services

VEI is the shared world kernel underneath every company world we open: the company-specific part is just the capability graph and contract, while the runtime, event spine, branching, replay, and inspection surfaces stay the same.

## Company

- World: `Service Operations`
- Company: `Clearwater Field Services`
- Briefing: Clearwater Field Services coordinates field dispatch, account handling, billing follow-through, and exception management across a high-volume service business.
- Why failure matters: If this scenario fails, Clearwater misses a VIP SLA, aggravates an open billing dispute, and turns one bad morning into churn and manual cleanup.

## Situation

- Scenario: `default`
- Scenario variant: `service_day_collision`
- Situation briefing: A VIP outage, technician no-show, and billing dispute collide on the same service account before the morning stabilizes. Why this matters: This is the flagship Clearwater field-services story and the clearest proof of a service-ops pack.

## Objective

- Contract variant: `protect_sla`
- Objective briefing: Prioritize rapid, valid dispatch recovery so the service request reaches a credible field response on time. Why it exists: This is the default Clearwater business objective and the clearest service-ops baseline.
- Presentation manifest: `/Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-dispatch-recovery/presentation_manifest.json`
- Presentation guide: `/Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-dispatch-recovery/presentation_guide.md`

## Runs

- Workflow baseline: `workflow_baseline`
- Comparison (scripted): `scripted_comparison`
- Workflow contract ok: `True`
- Comparison contract ok: `False`
- UI: `python -m vei.cli.vei ui serve --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-dispatch-recovery --host 127.0.0.1 --port 3011`

## Branch Story

- Base world: Clearwater Field Services coordinates field dispatch, account handling, billing follow-through, and exception management across a high-volume service business.
- Chosen situation: A VIP outage, technician no-show, and billing dispute collide on the same service account before the morning stabilizes.
- Chosen objective: Prioritize rapid, valid dispatch recovery so the service request reaches a credible field response on time.
- Baseline branch: Reassign dispatch, hold billing, and keep the customer whole
- Agent branch: Let the morning fragment into dispatch drift and billing mistakes

What changed:
- The same Clearwater Field Services world stayed fixed while the `service_day_collision` situation overlay and `protect_sla` objective overlay defined this run.
- The workflow baseline resolved the world in `20` events and `7` graph actions; the comparison run took `8` events and `0` graph actions.
- Both runs used the same kernel surfaces, but the alternate path touched different tools and ended with `8` contract issue(s) instead of `0`.

Why the outcome matters:
- The baseline passes because the company world, situation, and objective all stay aligned: `Protect SLA` rewards the right business behavior for `Service Day Collision`.
- The comparison path fails meaningfully, which is the point of the kernel: same company, different decisions, different outcome.
- If this scenario fails, Clearwater misses a VIP SLA, aggravates an open billing dispute, and turns one bad morning into churn and manual cleanup.

## Export Preview

### RL Episode Export

One world state plus one event spine already yields state transitions, graph-native actions, contract-shaped rewards, and branch boundaries that can later become trainable RL episodes.

### Continuous Eval Export

The same company world, situation, and objective can be replayed as a baseline/comparison pair, which makes VEI naturally usable as a continuous eval harness later.

### Agent Ops Export

Playback, resolved tools, graph domains, receipts, and contract findings already form an agent-observability bundle that can later become an operations and governance surface.
