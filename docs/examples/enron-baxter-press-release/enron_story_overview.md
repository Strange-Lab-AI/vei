# VEI Story · Enron Corporation

VEI is one shared timeline and replay surface. This bundle shows the real-history path: one dated Enron decision, the prior branch-local chronology, the public-company side data, and the shipped learned forecast in one saved package.

## Bundle

- Title: `Enron Baxter Press Release Example`
- Bundle slug: `enron-baxter-press-release`
- Bundle role: `proof`
- Branch point: The Cliff Baxter press-release loop is active and the company has to decide how transparent, delayed, or reassuring the public message should be.
- What actually happened: The communications loop moved through a tight internal chain while the company shaped how much to say and how fast to say it.
- Branch date: `2001-05-02`
- Prior events: `30`
- Recorded future events: `6`
- Source families: `credit, disclosure, filing, financial, market, news`
- Domains: `governance, obs_graph`
- Saved forecast file: `whatif_reference_result.json`
- Top ranked candidate: `Transparent fact-based release`

## Why it matters

This is a better public-message case than Watkins for technical proof. It still has real stakes, but it also has a clearer downstream tail and a public-facing branch that readers understand quickly.

It gives the proof set a crisis-communications case that is about more than pure suppression versus escalation.

## Open it

```bash
vei ui serve \
  --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/docs/examples/enron-baxter-press-release/workspace \
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
