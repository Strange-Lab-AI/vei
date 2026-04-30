# News Public History Demo

This workspace is the public-history Studio demo. It uses an expanded
AmericanStories-derived 1836-1838 historical news snapshot with 1,200 selected
public records across markets, government policy, war and foreign affairs,
local civic life, slavery and abolition, labor, agriculture and weather, public
health and disasters, crime and courts, and transport infrastructure. The UI can
choose a historical cutoff, show only pre-cutoff evidence, and score candidate
public actions with a bundled JEPA checkpoint. It does not use live LLM keys and
it does not fabricate rankings when the checkpoint is unavailable.

Run it locally:

```bash
vei ui serve \
  --root docs/examples/news-public-history-demo/workspace \
  --host 127.0.0.1 \
  --port 3055
```

Export the browser-hosted static assets used by strangelab.ai:

```bash
python scripts/export_public_history_static_assets.py \
  --workspace docs/examples/news-public-history-demo/workspace \
  --output /path/to/strangelab.ai/public/public-history
```

The live JEPA ranking is exploratory decision support. It is not causal proof,
and unsupported scenarios should be treated as weakly grounded until broader
retrieval and confidence checks are added.
