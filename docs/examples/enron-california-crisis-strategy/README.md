# Enron California Crisis Strategy Example

This is the regulatory conduct case. It puts a preservation order, an active trading posture, and a narrow fork about halting versus continuing onto one saved branch.

## Open It In Studio

```bash
vei ui serve \
  --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/docs/examples/enron-california-crisis-strategy/workspace \
  --host 127.0.0.1 \
  --port 3055
```

Open `http://127.0.0.1:3055`.

![Saved forecast panel](../../assets/enron-whatif/california-crisis-strategy-forecast.png)

![Saved ranking panel](../../assets/enron-whatif/california-crisis-strategy-ranking.png)

## Branch Point

- Tim Belden's desk receives a preservation order tied to the California crisis while the trading strategy is still active.

## What Actually Happened

- The preservation-order thread stayed inside the active crisis loop while the desk was still deciding how far to halt or continue.

## Actions We Can Take

- **Preserve and halt**: Stop the play, preserve the record, and open the legal path.
- **Preserve and seek executive sign-off**: Preserve the record, but wait for top-level approval before acting.
- **Continue in a narrow loop**: Keep going, but keep the circle tight.
- **Continue and widen**: Keep going and broaden the loop.

## Predicted Effect On The Company

- Recorded future events after the historical branch: 4
- Current top-ranked action: Preserve and seek executive sign-off
- Short readout: Slightly lower exposure risk.
- Legal and regulatory exposure: improves (0.06 -> 0.029)
- Disclosure and stakeholder trust: improves (0.955 -> 0.977)
- Commercial damage: improves (0.04 -> 0.018)
- Internal execution drag: improves (0.026 -> 0.022)

## Why This Branch Matters

This case is useful because the fork is mechanically clear. Preserve and halt. Preserve and seek executive sign-off. Continue in a narrow loop. Or continue and widen.

It gives the proof set the cleanest legal and operational branch.

## Bundle Facts

- Saved branch scene: 30 prior events and 4 recorded future events
- Public-company slice at 2000-12-15: 6 financial checkpoints, 6 public news items, 733 market checkpoints, 0 credit checkpoints, and 0 regulatory checkpoints
- Prior timeline source families: disclosure, filing, financial, mail, market, news
- Prior timeline domains: governance, internal, obs_graph
- Bundle role: `proof`
- Saved LLM path: Pause the strategy, preserve the record, alert legal and compliance, and prepare a self-report path instead of continuing the trading play.
- Saved forecast file: `whatif_reference_result.json`

## Saved Files

- `workspace/`: saved workspace you can open in Studio
- `whatif_experiment_overview.md`: short human-readable run summary
- `whatif_experiment_result.json`: saved combined result for the example bundle
- `whatif_llm_result.json`: bounded message-path result
- `whatif_reference_result.json`: saved forecast result
- `whatif_business_state_comparison.md`: ranked comparison in business language
- `whatif_business_state_comparison.json`: structured comparison payload
- `enron_story_overview.md`: presenter-facing branch summary
- `enron_story_manifest.json`: structured demo manifest
- `enron_exports_preview.json`: export preview for timeline and forecast artifacts
- `enron_presentation_manifest.json`: presentation beat manifest
- `enron_presentation_guide.md`: operator guide for bundle demos

## Other Enron Examples

- [Enron Master Agreement Example](../enron-master-agreement-public-context/README.md)
- [Enron PG&E Power Deal Example](../enron-pge-power-deal/README.md)
- [Enron Baxter Press Release Example](../enron-baxter-press-release/README.md)
- [Enron Braveheart Forward Example](../enron-braveheart-forward/README.md)
- [Enron Watkins Follow-up Example](../enron-watkins-follow-up/README.md)
- [Enron Q3 Disclosure Review Example](../enron-q3-disclosure-review/README.md)
- [Enron Skilling Resignation Materials Example](../enron-skilling-resignation-materials/README.md)

## Refresh

```bash
python scripts/build_enron_example_bundles.py --bundle enron-california-crisis-strategy
python scripts/validate_whatif_artifacts.py docs/examples/enron-california-crisis-strategy
python scripts/capture_enron_bundle_screenshots.py --bundle enron-california-crisis-strategy
```

## Constraint

This repo now carries a small checked-in Enron Rosetta sample for the saved bundles and smoke checks. Fetch the full archive with `make fetch-enron-full` when you want full training, full benchmark builds, or full archive validation.

The macro heads in these saved bundles stay advisory context beside the email-path evidence. See [the current calibration report](../../../studies/macro_calibration_enron_v1/calibration_report.md) before making any stronger claim.
