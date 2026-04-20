# Enron Master Agreement Example

This is the default long-tail technical proof case. It keeps the visible downstream mail tail while placing the contract choice inside the richer Enron company timeline.

## Open It In Studio

```bash
vei ui serve \
  --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/docs/examples/enron-master-agreement-public-context/workspace \
  --host 127.0.0.1 \
  --port 3055
```

Open `http://127.0.0.1:3055`.

![Saved forecast panel](../../assets/enron-whatif/master-agreement-public-context-forecast.png)

![Saved ranking panel](../../assets/enron-whatif/master-agreement-public-context-ranking.png)

## Branch Point

- Debra Perlingiere is about to send the Master Agreement draft to Cargill on September 27, 2000.

## What Actually Happened

- The draft went outside quickly, then the thread widened into a long reassignment and redline tail with no visible formal signoff.

## Actions We Can Take

- **Internal legal review**: Keep ownership narrow and get legal review before any outside send.
- **Narrow status note**: Acknowledge the request without sending the draft.
- **Controlled external send**: Send outside once, with explicit limits and a tight reply loop.
- **Fast outside circulation**: Move fast and widen the loop for speed.

## Predicted Effect On The Company

- Recorded future events after the historical branch: 84
- Current top-ranked action: Internal legal review
- Short readout: Much lower outside spread risk. Trade-off: Slightly higher internal handling load.
- Legal and regulatory exposure: improves (0.71 -> 0.42)
- Disclosure and stakeholder trust: improves (0.345 -> 0.536)
- Commercial damage: improves (0.699 -> 0.49)
- Internal execution drag: worsens (0.498 -> 0.504)

## Why This Branch Matters

This is the clearest proof case because it has the largest visible recorded tail after the branch. You can point at the same decision, the actual downstream chain, and the ranked alternate moves in one view.

The company timeline around it is thicker now, so the branch reads as a company event rather than a detached contract email.

## Bundle Facts

- Saved branch scene: 30 prior events and 84 recorded future events
- Public-company slice at 2000-09-27: 5 financial checkpoints, 6 public news items, 679 market checkpoints, 0 credit checkpoints, and 0 regulatory checkpoints
- Prior timeline source families: disclosure, filing, financial, mail, market, news
- Prior timeline domains: governance, internal, obs_graph
- Bundle role: `proof`
- Saved LLM path: Keep the draft inside Enron, ask Gerald Nemec and Sara Shackleton for review, and hold the outside send.
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

- [Enron PG&E Power Deal Example](../enron-pge-power-deal/README.md)
- [Enron California Crisis Strategy Example](../enron-california-crisis-strategy/README.md)
- [Enron Baxter Press Release Example](../enron-baxter-press-release/README.md)
- [Enron Braveheart Forward Example](../enron-braveheart-forward/README.md)
- [Enron Watkins Follow-up Example](../enron-watkins-follow-up/README.md)
- [Enron Q3 Disclosure Review Example](../enron-q3-disclosure-review/README.md)
- [Enron Skilling Resignation Materials Example](../enron-skilling-resignation-materials/README.md)

## Bankruptcy Arc Timeline

See [timeline_arc.md](timeline_arc.md) for the dated public timeline and [the rendered timeline image](../../assets/enron-whatif/enron-bankruptcy-arc-timeline.png) for the visual version that places this branch beside the PG&E, California, and Watkins follow-up examples.

## Refresh

```bash
python scripts/build_enron_example_bundles.py --bundle enron-master-agreement-public-context
python scripts/validate_whatif_artifacts.py docs/examples/enron-master-agreement-public-context
python scripts/capture_enron_bundle_screenshots.py --bundle enron-master-agreement-public-context
```

## Constraint

This repo now carries a small checked-in Enron Rosetta sample for the saved bundles and smoke checks. Fetch the full archive with `make fetch-enron-full` when you want full training, full benchmark builds, or full archive validation.

The macro heads in these saved bundles stay advisory context beside the email-path evidence. See [the current calibration report](../../../studies/macro_calibration_enron_v1/calibration_report.md) before making any stronger claim.
