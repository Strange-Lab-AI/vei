# VEI Story · Clearwater Field Services

VEI is the shared world kernel underneath every company world we open: the company-specific part is just the capability graph and contract, while the runtime, event spine, branching, replay, and inspection surfaces stay the same.

## Company

- World: `Service Operations`
- Company: `Clearwater Field Services`
- Briefing: Clearwater Field Services coordinates field dispatch, account handling, billing follow-through, and exception management across a high-volume service business.
- Why failure matters: If this scenario fails, Clearwater misses a VIP SLA, aggravates an open billing dispute, and turns one bad morning into churn and manual cleanup.

## Situation

- Scenario: `default`
- Scenario variant: `billing_dispute_reopened`
- Situation briefing: Dispatch has a plausible recovery path, but finance reopens the account dispute and the customer trust problem gets worse. Why this matters: Shows the same company world bending toward trust and revenue preservation instead of dispatch only.

## Objective

- Contract variant: `protect_revenue`
- Objective briefing: Prefer actions that prevent billing damage and preserve the customer relationship while the field response unfolds. Why it exists: Useful when the same service day should optimize for revenue preservation instead of raw dispatch speed.
- Presentation manifest: `/Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-billing-dispute-reopened/presentation_manifest.json`
- Presentation guide: `/Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-billing-dispute-reopened/presentation_guide.md`

## Runs

- Workflow baseline: `workflow_baseline`
- Comparison (scripted): `scripted_comparison`
- Workflow contract ok: `False`
- Comparison contract ok: `False`
- UI: `python -m vei.cli.vei ui serve --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-billing-dispute-reopened --host 127.0.0.1 --port 3011`

## Branch Story

- Base world: Clearwater Field Services coordinates field dispatch, account handling, billing follow-through, and exception management across a high-volume service business.
- Chosen situation: Dispatch has a plausible recovery path, but finance reopens the account dispute and the customer trust problem gets worse.
- Chosen objective: Prefer actions that prevent billing damage and preserve the customer relationship while the field response unfolds.
- Baseline branch: Contain finance risk before the customer relationship degrades
- Agent branch: Focus only on field completion and let billing damage keep spreading

What changed:
- The same Clearwater Field Services world stayed fixed while the `billing_dispute_reopened` situation overlay and `protect_revenue` objective overlay defined this run.
- The workflow baseline resolved the world in `20` events and `7` graph actions; the comparison run took `8` events and `0` graph actions.
- Both runs used the same kernel surfaces, but the alternate path touched different tools and ended with `7` contract issue(s) instead of `1`.

Why the outcome matters:
- The baseline passes because the company world, situation, and objective all stay aligned: `Protect Revenue` rewards the right business behavior for `Billing Dispute Reopened`.
- The comparison path fails meaningfully, which is the point of the kernel: same company, different decisions, different outcome.
- If this scenario fails, Clearwater misses a VIP SLA, aggravates an open billing dispute, and turns one bad morning into churn and manual cleanup.

## Export Preview

### RL Episode Export

One world state plus one event spine already yields state transitions, graph-native actions, contract-shaped rewards, and branch boundaries that can later become trainable RL episodes.

### Continuous Eval Export

The same company world, situation, and objective can be replayed as a baseline/comparison pair, which makes VEI naturally usable as a continuous eval harness later.

### Agent Ops Export

Playback, resolved tools, graph domains, receipts, and contract findings already form an agent-observability bundle that can later become an operations and governance surface.
