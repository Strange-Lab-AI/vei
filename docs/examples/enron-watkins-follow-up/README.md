# Enron Watkins Follow-up Example

This is the main narrative governance case. It is the strongest moral fork in the set, even though it has a thinner recorded downstream tail than Master Agreement.

## Open It In Studio

```bash
vei ui serve \
  --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/docs/examples/enron-watkins-follow-up/workspace \
  --host 127.0.0.1 \
  --port 3055
```

Open `http://127.0.0.1:3055`.

![Saved forecast panel](../../assets/enron-whatif/watkins-follow-up-forecast.png)

![Saved ranking panel](../../assets/enron-whatif/watkins-follow-up-ranking.png)

## Branch Point

- Sherron Watkins is writing a follow-up note that preserves her account of the questions she says she raised to Ken Lay on August 22, 2001.

## What Actually Happened

- The follow-up note became a narrow internal preserved record during the wider disclosure spiral.

## Actions We Can Take

- **Formal audit escalation**: Turn the note into a formal accounting escalation.
- **Board and legal preservation**: Preserve the record inside the board and legal path.
- **Narrow internal escalation**: Warn upward, but keep the loop narrow.
- **Suppress and monitor**: Keep the concern private and avoid formal escalation.

## Predicted Effect On The Company

- Recorded future events after the historical branch: 1
- Current top-ranked action: Suppress and monitor
- Short readout: This move stays close to the historical business path.
- Legal and regulatory exposure: stays flat (0.068 -> 0.068)
- Disclosure and stakeholder trust: stays flat (0.954 -> 0.954)
- Commercial damage: stays flat (0.031 -> 0.031)
- Internal execution drag: stays flat (0.029 -> 0.029)

## Why This Branch Matters

This is the case to use when you want the clearest human and governance story. The branch is simple to explain and the stakes are obvious.

Use it as a narrative case, not as the main technical proof case.

## Bundle Facts

- Saved branch scene: 36 prior events and 1 recorded future events
- Public-company slice at 2001-10-30: 11 financial checkpoints, 13 public news items, 944 market checkpoints, 4 credit checkpoints, and 1 regulatory checkpoints
- Prior timeline source families: credit, disclosure, filing, financial, governance, news, regulatory
- Prior timeline domains: governance, obs_graph
- Bundle role: `narrative`
- Saved LLM path: Escalate the follow-up note to Ken Lay, the audit committee, and internal legal, preserve the written record, and pause broad reassurance.
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
- [Enron California Crisis Strategy Example](../enron-california-crisis-strategy/README.md)
- [Enron Baxter Press Release Example](../enron-baxter-press-release/README.md)
- [Enron Braveheart Forward Example](../enron-braveheart-forward/README.md)
- [Enron Q3 Disclosure Review Example](../enron-q3-disclosure-review/README.md)
- [Enron Skilling Resignation Materials Example](../enron-skilling-resignation-materials/README.md)

## Refresh

```bash
python scripts/build_enron_example_bundles.py --bundle enron-watkins-follow-up
python scripts/validate_whatif_artifacts.py docs/examples/enron-watkins-follow-up
python scripts/capture_enron_bundle_screenshots.py --bundle enron-watkins-follow-up
```

## Constraint

This repo now carries a small checked-in Enron Rosetta sample for the saved bundles and smoke checks. Fetch the full archive with `make fetch-enron-full` when you want full training, full benchmark builds, or full archive validation.

The macro heads in these saved bundles stay advisory context beside the email-path evidence. See [the current calibration report](../../../studies/macro_calibration_enron_v1/calibration_report.md) before making any stronger claim.
