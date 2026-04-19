# VEI Story · Enron Corporation

VEI is one shared timeline and replay surface. This bundle shows the real-history path: one dated Enron decision, the prior branch-local chronology, the public-company side data, and the shipped learned forecast in one saved package.

## Bundle

- Title: `Enron PG&E Power Deal Example`
- Bundle slug: `enron-pge-power-deal`
- Branch point: Sara Shackleton is moving a PG&E financial power deal while the counterparty's macro-credit picture is deteriorating.
- Branch date: `1999-05-12`
- Prior events: `30`
- Recorded future events: `6`
- Source families: `disclosure, financial, market`
- Domains: `governance, obs_graph`
- Saved forecast file: `whatif_reference_result.json`
- Top ranked candidate: `Hold for credit re-check`

## Why it matters

This branch is interesting because it keeps the legal drafting work small and concrete while the counterparty story around PG&E is getting materially worse in public. The saved choices are about whether Enron slows down, restructures, or keeps pressing ahead.

The stock, credit, and bankruptcy fixtures add context around the deal date. The macro panel still stays advisory because the current calibration report is weak.

## Open it

```bash
vei ui serve \
  --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/docs/examples/enron-pge-power-deal/workspace \
  --host 127.0.0.1 \
  --port 3055
```

## Demo files

- Story manifest: `enron_story_manifest.json`
- Exports preview: `enron_exports_preview.json`
- Presentation manifest: `enron_presentation_manifest.json`
- Presentation guide: `enron_presentation_guide.md`

## Structured notes

- Story manifest role: `headline`
- Presentation beats: `7`
