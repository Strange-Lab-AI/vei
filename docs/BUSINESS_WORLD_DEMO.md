# Business World Demo

This demo now lives inside the existing showcase system. It generates a typed bundle, a guide, and a normal `service_ops` story workspace instead of relying on a one-off script.

Run this from the repo root:

```bash
cd <repo-root>
.venv/bin/python -m vei.cli.vei showcase business-world \
  --root _vei_out/showcase \
  --run-id business_world_demo
```

That command writes:

- `_vei_out/showcase/business_world_demo/business_world_demo_manifest.json`
- `_vei_out/showcase/business_world_demo/business_world_demo_guide.md`
- `_vei_out/showcase/business_world_demo/service_ops_story/` with the standard story artifacts already used elsewhere in the repo

The flow stays the same:

- show one live business world
- show the kinds of evaluations that world enables
- show Enron as a toy historical example of branching and scoring

The product framing is practical:

> VEI is a business world model in the practical sense. It is one coherent, branchable business world where people, software systems, agents, policies, and consequences live in the same runtime.

Keep the learned-versus-sim distinction in the background unless someone asks directly.

## Sequence

### 1. Start with the live business world

Run:

```bash
cd <repo-root>
.venv/bin/python -m vei.cli.vei quickstart run --world service_ops --governor-demo
```

Open the Studio URL it prints, usually `http://127.0.0.1:3011`.

Open on the Company view and let the room see one company with active systems, governed agents, approvals, and changing business state.

Say:

> This is a world model of a business in the practical sense. It is one company world where people, software systems, agents, rules, and consequences all live in the same runtime.

Then point at the control room and business surfaces.

Say:

> The key thing is that this is not one workflow in isolation. One action changes the rest of the company.

Show one allowed action, one blocked action, and one approval-held action.

### 2. Explain the eval layer

Stay on the same world.

Say:

> Once you have a stable business world, evals stop being toy tasks. You can evaluate behavior against the business.

Use these three buckets:

- task completion: did the agent solve the business problem
- policy and risk: did it violate rules, trigger approvals, or create risk
- business outcome: what happened to customer trust, revenue protection, delays, escalation, or downstream load

Then tie it back to the repo:

> In this repo, the same run already gives us the event spine, the contract result, the branch comparison, and the artifacts for repeated eval.

### 3. End with Enron as the toy example

Do not make Enron the main product. Use it as the capstone.

Open the saved result in a second terminal window:

```bash
cd <repo-root>
.venv/bin/python -m vei.cli.vei whatif show-result \
  --root <saved-enron-result-root> \
  --format markdown
```

Say:

> Enron is just the toy historical example. It proves the same machinery can branch from a real past business decision instead of only from a synthetic company.

Then tell the story in under one minute:

> Here the same idea is applied to a real historical business moment. Debra Perlingiere sent a Master Agreement draft to Cargill. We branch just before that send, replay what actually happened, then compare it to an alternate internal-review path.

Give the concrete result:

> In the saved run, the historical path had 84 follow-up events. The alternate path kept the draft inside Enron, produced 3 internal follow-up emails, and the forecast estimated lower risk and 29 fewer outside-addressed sends.

Close by reconnecting Enron to the main product:

> So the point of Enron is not the dataset itself. The point is that the same engine can branch from a real business decision, simulate alternatives, and score the outcomes.

## Timing

- 3 minutes on the live business world
- 1.5 minutes on eval types
- 1 minute on Enron
- 30 seconds on the closing platform vision

## Opening And Closing Lines

Opening:

> Most agent demos show one tool call or one workflow. We are showing a business world. The company, the systems, the policies, and the consequences are all connected.

Closing:

> The long-term value is not one simulator or one benchmark. It is a reusable business world where you can test agents, compare strategies, branch futures, and eventually train learned models on the trajectories.

## Backup Notes

If the live `service_ops` world feels busy, stay on the Company view and talk over it. The control room is enough.

If the Enron terminal feels too dense, use these saved files:

- `<saved-enron-result-root>/../run_summary.md`
- `<saved-enron-result-root>/whatif_experiment_overview.md`

Do not lead with archive search. Do not lead with ranked reruns. Do not lead with benchmark taxonomy.
