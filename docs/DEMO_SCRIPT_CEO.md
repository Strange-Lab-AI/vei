# CEO demo script — VEI Studio

Reference screenshots (refreshed with current Studio chrome): [vei_studio_hero.png](assets/vei_studio_hero.png), [vei_studio_outcome_tab.png](assets/vei_studio_outcome_tab.png). Regenerate with `python scripts/capture_ui_docs.py` from a dev env with Playwright browsers installed.

Short walkthrough for a **5–8 minute** conversation: company state, pressure, decisions, and proof. Use a single vertical (e.g. Pinnacle Analytics) and one crisis the audience already cares about (renewal, outage, approval bottleneck).

## Before you share your screen

1. Run Studio from a prepared workspace (`vei ui serve` or your usual command).
2. Confirm the **trust line** under the header reads as expected (simulated workspace vs control plane / import sync).
3. Optional: enable **Demo mode** (header → Views → Demo mode) if you want a calmer layout for projection.

## Beat 1 — “This is our company, under pressure” (≈1 min)

1. Point to the **company name** and the subtitle (current situation summary).
2. Open the **Crisis** tab briefly: “What went wrong and why it matters.”
3. Say in one sentence: *This is a branchable model of how our tools and people respond—not a slide deck.*

## Beat 2 — “How we define success” (≈1 min)

1. In the header, show **Crisis** and **Success criteria** dropdowns.
2. Click **Start scenario** (or **Apply crisis** in exercise workspaces).
3. Land on **Company**: call out the **situation room** strip (systems, approvals, risk) as the exec dashboard.

## Beat 3 — “The next real decision” (≈2 min)

1. Scroll to **Next move**; pick one move and narrate tradeoffs in plain language.
2. After the move, point to **what changed** (impact ribbon / surface highlights).
3. If mirror is on, mention the **Live mirror** banner: *agents here are governed the same way we’d govern them in production.*

## Beat 4 — “Did we make it better?” (≈2 min)

1. Switch to **Outcome**: scorecard and decision log.
2. If you have two runs, use **Compare paths** or **Event timeline** (Outcome tab shortcuts or Views menu).
3. Close with: *We can replay the same starting point with different policy or actions—useful for training and vendor proof.*

## If they ask “Is this our real data?”

Use the trust line: simulated workspace vs live connectors vs last import sync. Say clearly what is **live**, **captured**, or **fully simulated** for this demo.

## Parking lot (don’t open unless asked)

- **Operator Console** (`/pilot`): outside agent connection and pilot controls.
- **Connections**: enterprise capture configuration.
- **Technical detail**: playback, JSON, run tools—keep for engineers after the exec leaves.
