# VEI Story · Enron Corporation

VEI is one shared timeline and replay surface. This bundle shows the real-history path: one dated Enron decision, the prior branch-local chronology, the public-company side data, and the shipped learned forecast in one saved package.

## Bundle

- Title: `Enron Skilling Resignation Materials Example`
- Bundle slug: `enron-skilling-resignation-materials`
- Bundle role: `narrative`
- Branch point: The company is drafting materials around Jeff Skilling's resignation and has to decide how candid, controlled, or aggressively reassuring the message should be.
- What actually happened: The resignation materials moved through a controlled executive communications loop.
- Branch date: `2001-08-14`
- Prior events: `32`
- Recorded future events: `1`
- Source families: `credit, disclosure, filing, financial, governance, mail, news, regulatory`
- Domains: `governance, internal, obs_graph`
- Saved forecast file: `whatif_reference_result.json`
- Top ranked candidate: `Narrow internal-only draft`

## Why it matters

This case gives the narrative set a leadership-trust branch. Readers can follow it quickly because the public meaning of the choice is clear.

It is also useful for presentation because the scene is legible even without deep accounting context.

## Open it

```bash
vei ui serve \
  --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/docs/examples/enron-skilling-resignation-materials/workspace \
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
