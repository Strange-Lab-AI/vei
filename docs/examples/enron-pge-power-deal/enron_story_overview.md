# VEI Story · Enron Corporation

VEI is one shared timeline and replay surface. This bundle shows the real-history path: one dated Enron decision, the prior branch-local chronology, the public-company side data, and the shipped learned forecast in one saved package.

## Bundle

- Title: `Enron PG&E Power Deal Example`
- Bundle slug: `enron-pge-power-deal`
- Bundle role: `proof`
- Branch point: Sara Shackleton is moving a PG&E financial power deal while the counterparty credit picture is deteriorating.
- What actually happened: The deal thread kept moving through the legal and commercial loop while the wider PG&E situation worsened.
- Branch date: `1999-05-12`
- Prior events: `30`
- Recorded future events: `6`
- Source families: `disclosure, financial, market`
- Domains: `governance, obs_graph`
- Saved forecast file: `whatif_reference_result.json`
- Top ranked candidate: `Hold for credit re-check`

## Why it matters

This case is strong because more than one move looks plausible. The question is not only safety. The question is whether Enron should slow down, restructure, or still push the deal through.

It also gives the proof set a commercial and credit branch instead of only legal or governance branches.

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

- Story manifest role: `proof`
- Presentation beats: `7`
