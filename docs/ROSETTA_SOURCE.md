# Enron Rosetta Source

The repo now carries the Enron Rosetta chain directly under `data/enron/`.

## Layout

- `data/enron/raw/enron_mail_20150507.tar.gz`: vendored CMU maildir tar
- `data/enron/source/enron_rosetta_source.parquet`: normalized source cache used to build Rosetta
- `data/enron/rosetta/`: derived Rosetta event archive used by VEI

The runtime default is `data/enron/rosetta`. Set `VEI_WHATIF_ROSETTA_DIR` only when you need to override that location.

## Derived archive

The Rosetta archive includes:

- `enron_rosetta_events_metadata.parquet`
- `enron_rosetta_events_content.parquet`
- schema and summary sidecars
- sample CSV exports
- talk and work graph sidecars from the same build

`python scripts/check_rosetta_archive.py` verifies the archive, checks the required parquet columns, checks row-count parity, and confirms the fixed benchmark event ids resolve.

## Build path

The build script lives in `scripts/build_enron_rosetta.py`.

Default inputs:

- source cache: `data/enron/source/enron_rosetta_source.parquet`
- raw tar fallback: `data/enron/raw/enron_mail_20150507.tar.gz`
- output directory: `data/enron/rosetta`

Typical rebuild:

```bash
python scripts/build_enron_rosetta.py --prefer-local-source --include-content
python scripts/check_rosetta_archive.py
```

## Event ids

The Rosetta build keeps the same event id scheme used by the historical what-if system:

`enron_{sha1(message_id|timestamp|actor)[:16]}`

That stable scheme is what lets repo-owned saved bundles, benchmark seeds, and archive checks refer to the same branch events.

## Provenance

The builder was ported from the sibling `llmenron` project so this repo no longer depends on that checkout for Enron work. The upstream raw mail corpus is the public CMU Enron email dataset. Keep the raw tar, source cache, and derived archive together so the repo stays self-contained for rebuilds and for whole-history search.
