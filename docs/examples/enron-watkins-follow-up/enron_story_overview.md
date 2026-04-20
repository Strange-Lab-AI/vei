# VEI Story · Enron Corporation

VEI is one shared timeline and replay surface. This bundle shows the real-history path: one dated Enron decision, the prior branch-local chronology, the public-company side data, and the shipped learned forecast in one saved package.

## Bundle

- Title: `Enron Watkins Follow-up Example`
- Bundle slug: `enron-watkins-follow-up`
- Bundle role: `narrative`
- Branch point: Sherron Watkins is writing a follow-up note that preserves her account of the questions she says she raised to Ken Lay on August 22, 2001.
- What actually happened: The follow-up note became a narrow internal preserved record during the wider disclosure spiral.
- Branch date: `2001-10-30`
- Prior events: `36`
- Recorded future events: `1`
- Source families: `credit, disclosure, filing, financial, governance, news, regulatory`
- Domains: `governance, obs_graph`
- Saved forecast file: `whatif_reference_result.json`
- Top ranked candidate: `Suppress and monitor`

## Why it matters

This is the case to use when you want the clearest human and governance story. The branch is simple to explain and the stakes are obvious.

Use it as a narrative case, not as the main technical proof case.

## Open it

```bash
vei ui serve \
  --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/docs/examples/enron-watkins-follow-up/workspace \
  --host 127.0.0.1 \
  --port 3055
```

## Demo files

- Story manifest: `enron_story_manifest.json`
- Exports preview: `enron_exports_preview.json`
- Presentation manifest: `enron_presentation_manifest.json`
- Presentation guide: `enron_presentation_guide.md`

## Structured notes

- Story manifest role: `narrative`
- Presentation beats: `7`
