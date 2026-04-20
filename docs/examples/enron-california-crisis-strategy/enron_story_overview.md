# VEI Story · Enron Corporation

VEI is one shared timeline and replay surface. This bundle shows the real-history path: one dated Enron decision, the prior branch-local chronology, the public-company side data, and the shipped learned forecast in one saved package.

## Bundle

- Title: `Enron California Crisis Strategy Example`
- Bundle slug: `enron-california-crisis-strategy`
- Bundle role: `proof`
- Branch point: Tim Belden's desk receives a preservation order tied to the California crisis while the trading strategy is still active.
- What actually happened: The preservation-order thread stayed inside the active crisis loop while the desk was still deciding how far to halt or continue.
- Branch date: `2000-12-15`
- Prior events: `30`
- Recorded future events: `4`
- Source families: `disclosure, filing, financial, mail, market, news`
- Domains: `governance, internal, obs_graph`
- Saved forecast file: `whatif_reference_result.json`
- Top ranked candidate: `Preserve and seek executive sign-off`

## Why it matters

This case is useful because the fork is mechanically clear. Preserve and halt. Preserve and seek executive sign-off. Continue in a narrow loop. Or continue and widen.

It gives the proof set the cleanest legal and operational branch.

## Open it

```bash
vei ui serve \
  --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/docs/examples/enron-california-crisis-strategy/workspace \
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
