# VEI World Briefing Guide · Clearwater Field Services

VEI is one enterprise world kernel. This world shows that the same runtime can instantiate different companies, vary the situation, vary the objective, and still produce inspectable runs, branches, and exportable artifacts.

## Why This World Exists

Start from Clearwater Field Services as a stable company world, then watch how `Billing Dispute Reopened` changes the situation, how `Protect Revenue` changes the definition of success, and how the same kernel turns both runs into playback, branching, and future RL/eval/agent-ops outputs.

## Open The World

- Open Studio on the Briefing view, then move into Living Company before you touch anything else.
- Frame the product as a world studio for enterprises, not a static workflow viewer.
- Keep the language anchored in the company world: Company, Situation, and Objective are the stable user-facing primitives.
- Use `Contain finance risk before the customer relationship degrades` versus `Focus only on field completion and let billing damage keep spreading` to explain branching.

## World Primitives

### Company

- Current value: `Clearwater Field Services`
- What it means: Start with one stable business world. The company stays fixed while the situation and the objective move around it.
- Under the hood: Workspace + blueprint + capability graphs

### Situation

- Current value: `Billing Dispute Reopened`
- What it means: Situations are overlays on the base world. They add deadline pressure, faults, and branch-worthy tradeoffs without rebuilding the company.
- Under the hood: Scenario variant overlay on the world state

### Objective

- Current value: `Protect Revenue`
- What it means: Objectives change what counts as good behavior. The same company and same situation can be judged under different business preferences.
- Under the hood: Contract variant overlay on the shared contract engine

### Run

- Current value: `workflow_baseline vs scripted_comparison`
- What it means: Runs are attempts to solve the same world under the same event spine. Workflow, scripted, and LLM agents all land in one runtime model.
- Under the hood: Canonical run/event spine + snapshots + playback

### Branch

- Current value: `Baseline branch and agent branch`
- What it means: Branching shows alternate futures from one company world. That is the most intuitive proof that this is an engine, not a static benchmark.
- Under the hood: Snapshots + branch labels + diff and replay surfaces

### Exports

- Current value: `RL / eval / agent ops preview`
- What it means: The same run artifacts later become RL episodes, continuous eval cases, and agent observability bundles.
- Under the hood: Derived artifacts from the same run and contract outputs

## Walkthrough Flow

### Step 1 · Open with the kernel thesis

- Studio view: `presentation`
- Operator action: Start on Briefing, then move into Living Company once the software wall is visible.
- Read it as: Say that VEI is one enterprise world kernel. We are about to show different companies, different situations, and different objectives on top of the same runtime.
- Proof point: The world opens with the engine, not with a single handcrafted scenario.
- Audience takeaway: This is a reusable platform layer, not a one-off workflow viewer.

### Step 2 · Choose the company world

- Studio view: `worlds`
- Operator action: Click Company and show that this world is one stable business with its own graphs and operating surfaces.
- Read it as: Introduce Clearwater Field Services in plain language, explain what the company does, and why failure in this world has real business consequences.
- Proof point: Different companies can live on the same kernel without changing the runtime model.
- Audience takeaway: The kernel is flexible enough to instantiate very different enterprise environments.

### Step 3 · Show the situation overlay

- Studio view: `situations`
- Operator action: Click Situation, highlight the active scenario variant, and call out what changed from the base world.
- Read it as: Explain that `Billing Dispute Reopened` is not a different company. It is one alternate future layered on top of the same company world.
- Proof point: Problem setup is a first-class overlay, not a rewritten environment.
- Audience takeaway: The same company can generate many meaningful simulations.

### Step 4 · Show the objective overlay

- Studio view: `objectives`
- Operator action: Click Objective and compare the active contract with the other objective variants.
- Read it as: Explain that `Protect Revenue` tells VEI what good looks like in this situation, and that different objectives can produce different preferred behavior on the same world.
- Proof point: Success criteria are separate from both the world and the situation.
- Audience takeaway: This same kernel can later support eval, policy testing, and reward shaping.

### Step 5 · Run the baseline and the agent path

- Studio view: `runs`
- Operator action: Launch or open the workflow baseline, then the comparison run, and play the timeline for a few events.
- Read it as: Point out that every action lands in one event spine with graph intent, resolved tools, and snapshots. That is what makes the world inspectable instead of magical.
- Proof point: Same runtime model for deterministic baseline and freer agent behavior.
- Audience takeaway: This is already a serious observability surface, not just a benchmark harness.

### Step 6 · Explain the branch and outcome

- Studio view: `runs`
- Operator action: Scroll to Branch + Outcome and contrast the baseline branch with the agent branch.
- Read it as: Use the branch story to explain that the company world stayed the same, but the decisions changed, so the business result changed.
- Proof point: Branching makes alternate futures legible on top of one shared world state.
- Audience takeaway: This is why VEI can later serve as a simulation engine, recovery lab, and decision-testing system.

### Step 7 · Close on the platform bridge

- Studio view: `runs`
- Operator action: Finish on Exports and tie the run outputs to RL episodes, continuous eval, and agent operations.
- Read it as: Close by saying that this world already emits the ingredients for the next products: RL transitions, eval comparisons, and agent observability.
- Proof point: The future-platform story is a direct extension of the current artifacts, not a speculative rewrite.
- Audience takeaway: The upside is a family of products built on one world kernel.

## Closing Argument

The core claim is simple: VEI already behaves like a world studio for enterprises. These worlds are different instantiations of one kernel, and the same kernel is what later becomes an RL environment, a continuous eval harness, and an agent management platform.

## Operator Commands

- `python -m vei.cli.vei project init --vertical service_ops --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-billing-dispute-reopened`
- `python -m vei.cli.vei scenario activate --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-billing-dispute-reopened --variant billing_dispute_reopened --bootstrap-contract`
- `python -m vei.cli.vei contract activate --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-billing-dispute-reopened --variant protect_revenue`
- `python -m vei.cli.vei ui serve --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/_vei_out/service_ops_story_sources/clearwater-billing-dispute-reopened --host 127.0.0.1 --port 3011`
