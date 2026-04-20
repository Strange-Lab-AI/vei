# Enron Rosetta Source

The Enron dataset now has two layers.

- `data/enron/rosetta/` is the small checked-in sample that powers the saved bundles, source resolution, smoke checks, and repo-default Studio paths.
- The full Enron archive is an optional download fetched with `make fetch-enron-full`. It is stored outside the repo checkout and described by `data/enron/full_dataset_release.json`.

## Repo sample

The checked-in sample keeps the same file names the runtime already expects:

- `enron_rosetta_events_metadata.parquet`
- `enron_rosetta_events_content.parquet`
- `enron_talk_graph_edges.parquet`
- `enron_work_graph_transitions.parquet`
- `enron_work_graph_edges.parquet`

The sample also carries:

- `enron_rosetta_dataset.json`
- `enron_rosetta_summary.md`
- schema and sample CSV sidecars

That sample is built from a fixed set of benchmark anchor cases and short local thread context. It is large enough for the case register, the saved Enron bundles, the timeline UI, and repo-default source resolution.

## Full archive

The full archive lives outside the normal checkout as one release asset. Fetch it with:

```bash
make fetch-enron-full
```

That command downloads the release asset, verifies the checksum from `data/enron/full_dataset_release.json`, extracts it into the local cache, and prints the resolved paths.

After that, these commands use the full archive:

- `python scripts/check_rosetta_archive.py`
- `python scripts/train_reference_backend_on_enron.py`
- `python scripts/build_enron_macro_outcome_table.py`
- `python scripts/find_enron_candidate_events.py`

`VEI_WHATIF_ROSETTA_DIR` still overrides the discovered full-data path when you want to point VEI at a different archive location.

## Resolution order

When VEI looks for Enron Rosetta data, it resolves paths in this order:

- the workspace manifest source directory
- `VEI_WHATIF_ROSETTA_DIR`
- the fetched full-dataset cache path
- the checked-in sample at `data/enron/rosetta/`
- a workspace-local `rosetta/` folder

That means full-data work automatically uses the fetched archive when it is present, while saved-bundle and smoke-check paths still work from a fresh clone.

## Rebuild paths

Build or refresh the checked-in sample from a full archive with:

```bash
python scripts/build_enron_rosetta_sample.py
```

Refresh the fetched full Rosetta archive itself with:

```bash
python scripts/build_enron_rosetta.py --prefer-local-source --include-content
```

Package a new full-dataset release asset with:

```bash
make package-enron-full
```

The release packager writes a checksum file beside the archive and adds a full-dataset marker into the staged Rosetta folder.
