# VEI Story · Enron Corporation

VEI is one shared timeline and replay surface. This bundle shows the real-history path: one dated Enron decision, the prior branch-local chronology, the public-company side data, and the shipped learned forecast in one saved package.

## Bundle

- Title: `Enron Q3 Disclosure Review Example`
- Bundle slug: `enron-q3-disclosure-review`
- Bundle role: `narrative`
- Branch point: Third-quarter review material is moving while the company is deciding how much to say about the growing accounting and liquidity overhang.
- What actually happened: The review material kept moving through management, finance, and legal during the disclosure crisis.
- Branch date: `2001-10-31`
- Prior events: `36`
- Recorded future events: `3`
- Source families: `credit, disclosure, filing, financial, governance, news, regulatory`
- Domains: `governance, obs_graph`
- Saved forecast file: `whatif_reference_result.json`
- Top ranked candidate: `Management-only review before disclosure`

## Why it matters

This case is useful because two or three options can look defensible at first glance. That makes it a better narrative example for explaining why ranking the actions matters.

It also sits closer to public disclosure mechanics than Watkins does.

## Open it

```bash
vei ui serve \
  --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/docs/examples/enron-q3-disclosure-review/workspace \
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
