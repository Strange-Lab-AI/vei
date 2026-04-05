# Historical What-Ifs

VEI now supports a mail-first historical what-if workflow for archive-backed datasets such as the Enron Rosetta event tables.

The flow has four steps:

1. Explore the whole history to see what a rule or intervention would have touched.
2. Pick one exact historical event.
3. Materialize that event's thread into a strict historical workspace.
4. Compare the baseline future against one or more counterfactual paths.

Studio now supports this same loop directly:

1. search the archive for a real historical moment
2. choose one event from the results
3. materialize the baseline workspace
4. run the counterfactual and inspect the saved comparison bundle

## Why this shape

VEI does not try to turn an entire historical corpus into one giant always-running simulation. That would be slower, heavier, and harder to understand in a demo.

Instead, the system uses two connected layers:

- **Whole-history analysis** for broad questions such as “what would this policy have caught?”
- **Event-level replay** for one chosen moment, where VEI can branch, replay, and compare outcomes inside a normal world workspace

This keeps the whole-history pass deterministic and cheap while still giving us a true replay environment for the interesting moment.

## What gets materialized

When VEI opens a historical episode, it builds a mail-first workspace from the selected thread:

- messages before the selected event become the initial mail state
- the selected event and later historical messages become scheduled replay events
- observed thread participants become identity records
- policy-relevant annotations stay attached for analysis and scoring

The important constraint is honesty:

- VEI does **not** invent Slack history for archive-backed email episodes
- VEI keeps historical body excerpts labeled as excerpts when the source data is truncated
- unsupported surfaces stay disabled instead of being faked

## Compare paths

There are two compare paths today:

- **LLM actor continuation**
  - bounded email-only continuation on the affected thread
  - limited to the known thread participants and allowed recipients
  - defaults to `gpt-5-mini` so the interactive run completes quickly and predictably
  - useful for “what would someone have said or done next?”
- **E-JEPA forecast**
  - real checkpoint-backed forecast for risk and volume deltas when the local `ARP_Jepa_exp` runtime is available
  - trained on a deterministic local slice of related threads around the chosen branch point, so the forecast stays tied to the exact decision you are changing
  - falls back to the proxy forecast only when that runtime is missing or errors
  - useful for “how much would this likely reduce exposure, escalation, or follow-up volume?”

## CLI

```bash
# Whole-history analysis
vei whatif explore \
  --rosetta-dir /path/to/rosetta \
  --scenario compliance_gateway \
  --format markdown

# Search for exact branch points
vei whatif events \
  --rosetta-dir /path/to/rosetta \
  --actor vince.kaminski \
  --query "btu weekly" \
  --flagged-only \
  --format markdown

# Build a replayable episode from one exact event
vei whatif open-episode \
  --rosetta-dir /path/to/rosetta \
  --root _vei_out/whatif/enron_case \
  --event-id evt_1234

# Replay the historical future
vei whatif replay \
  --root _vei_out/whatif/enron_case \
  --tick-ms 600000

# Run the full counterfactual experiment
vei whatif experiment \
  --rosetta-dir /path/to/rosetta \
  --artifacts-root _vei_out/whatif_experiments \
  --label master_agreement_internal_review \
  --event-id evt_1234 \
  --model gpt-5-mini \
  --forecast-backend e_jepa \
  --ejepa-epochs 1 \
  --ejepa-batch-size 64 \
  --counterfactual-prompt "Keep the draft inside Enron, loop in Gerald Nemec for legal review, and hold the outside send until the clean version is approved."
```

## Artifacts

`vei whatif experiment` writes a result bundle that includes:

- experiment result JSON
- experiment overview Markdown
- LLM path JSON
- forecast path JSON
- the strict replay workspace used for the run

The forecast bundle is written as `whatif_ejepa_result.json` when the real JEPA path runs, or `whatif_ejepa_proxy_result.json` when the fallback path is used.

This makes it easy to inspect the result in Studio later, compare runs, or hand the output to another tool.
