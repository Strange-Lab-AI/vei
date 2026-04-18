# Enron Public Context

This fixture stores public-source material that can be joined to an Enron mail slice later.

The current packaged fixture contains:
- 11 dated financial checkpoints
- 21 dated public news events
- 24 archived public source files

The public dates currently span December 31, 1998 through March 14, 2002.

Contents:
- `raw/`: downloaded public-source HTML and PDF files.
- `package.json`: manifest describing the raw sources and normalized artifact.
- `enron_public_context_v2.json`: normalized dated financial checkpoints and public-news checkpoints.

Regenerate with:
- `python scripts/prepare_enron_public_context.py`

Integration rule:
- Read the oldest and latest email timestamps from the active Enron dataset.
- Keep only the public rows whose dates overlap that email window.
- For a concrete branch point, keep only the rows whose dates are on or before the branch timestamp.
- If the packaged fixture is missing or malformed, Enron mail loading still succeeds with an empty public-context slice.

