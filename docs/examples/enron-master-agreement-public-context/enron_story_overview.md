# VEI Story · Enron Corporation

VEI is one shared timeline and replay surface. This bundle shows the real-history path: one dated Enron decision, the prior branch-local chronology, the public-company side data, and the shipped learned forecast in one saved package.

## Bundle

- Title: `Enron Master Agreement Example`
- Bundle slug: `enron-master-agreement-public-context`
- Bundle role: `proof`
- Branch point: Debra Perlingiere is about to send the Master Agreement draft to Cargill on September 27, 2000.
- What actually happened: The draft went outside quickly, then the thread widened into a long reassignment and redline tail with no visible formal signoff.
- Branch date: `2000-09-27`
- Prior events: `30`
- Recorded future events: `84`
- Source families: `disclosure, filing, financial, mail, market, news`
- Domains: `governance, internal, obs_graph`
- Saved forecast file: `whatif_reference_result.json`
- Top ranked candidate: `Internal legal review`

## Why it matters

This is the clearest proof case because it has the largest visible recorded tail after the branch. You can point at the same decision, the actual downstream chain, and the ranked alternate moves in one view.

The company timeline around it is thicker now, so the branch reads as a company event rather than a detached contract email.

## Open it

```bash
vei ui serve \
  --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/docs/examples/enron-master-agreement-public-context/workspace \
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
