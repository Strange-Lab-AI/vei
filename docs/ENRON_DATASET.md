# Enron Dataset

The repo now ships the Enron what-if surface in two parts.

- The checkout carries a small Rosetta sample under `data/enron/rosetta/`.
- The full Enron archive is an optional release download fetched with `make fetch-enron-full`.

## What the checkout includes

The normal clone includes:

- the small Rosetta sample
- the release manifest at `data/enron/full_dataset_release.json`
- the public-company fixture under `vei/whatif/fixtures/enron_public_context`
- the curated public-record fixture under `vei/whatif/fixtures/enron_record_history`
- the saved Enron example bundles under `docs/examples/`

That is enough to open the saved Enron bundles, rebuild the casebook surface, run smoke checks, and use the repo-owned Studio examples from a fresh clone.

## What needs the full archive

Fetch the full archive when you want:

- whole-history Enron search
- archive validation
- reference-backend training
- macro-study rebuilds
- candidate-event mining
- full benchmark builds

Use:

```bash
make fetch-enron-full
python scripts/check_rosetta_archive.py
```

## Full archive location

The fetch command reads `data/enron/full_dataset_release.json`, downloads the release asset, verifies the checksum, and extracts it into the local cache root described in that manifest.

The current release lives at `https://github.com/Strange-Lab-AI/vei/releases/tag/enron-dataset-v1`.

Set `VEI_WHATIF_ROSETTA_DIR` when you want VEI to use a different full archive location.

## Contributor flow

Fresh clone:

```bash
make setup
```

Saved-bundle and smoke-check work:

```bash
make enron-example
make enron-screens
```

Full-data work:

```bash
make fetch-enron-full
python scripts/check_rosetta_archive.py
```

See [ROSETTA_SOURCE.md](ROSETTA_SOURCE.md) for the sample and full-data layout details.
