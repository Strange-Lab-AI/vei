# Enron Public Example

Enron is the repo-owned public company example. Use it when you want a
fresh-clone demonstration of historical replay, public-context slicing,
business-outcome forecasting, and saved Studio bundles.

For the general command reference, use [WHATIF.md](WHATIF.md). This file keeps
the Enron-specific data, cases, and benchmark notes in one place.

## What Ships

The normal checkout includes enough data to open and rerun the saved Enron
examples without downloading the full archive:

- a small Rosetta email sample under `data/enron/rosetta/`
- the full archive release manifest at `data/enron/full_dataset_release.json`
- public-company fixtures under `vei/whatif/fixtures/enron_public_context`
- curated public-record fixtures under `vei/whatif/fixtures/enron_record_history`
- saved Enron example bundles under `docs/examples/`
- the shipped reference backend under `data/enron/reference_backend/`

The public context currently contains 11 dated financial checkpoints, 21 dated
public news events, 24 archived public source files, 986 daily stock rows, 7
credit events, and 1 FERC timeline event. VEI slices these facts to the branch
date before showing them in Studio or adding them to benchmark dossiers.

## Full Archive

Fetch the full Enron archive when you want whole-history search, full benchmark
rebuilds, reference-backend training, macro-study rebuilds, or candidate-event
mining:

```bash
make fetch-enron-full
python scripts/check_rosetta_archive.py
```

The fetch command reads `data/enron/full_dataset_release.json`, downloads the
release asset, verifies the checksum, and extracts it into the local cache root
described by the manifest. The current release lives at
`https://github.com/Strange-Lab-AI/vei/releases/tag/enron-dataset-v1`.

`VEI_WHATIF_ROSETTA_DIR` overrides the discovered full-data path when you need
to point VEI at a different archive location.

VEI resolves Enron Rosetta data in this order:

- the workspace manifest source directory
- `VEI_WHATIF_ROSETTA_DIR`
- the fetched full-dataset cache path
- the checked-in sample at `data/enron/rosetta/`
- a workspace-local `rosetta/` folder

<!-- BEGIN GENERATED ENRON CASES -->
## Saved Examples

Start with the Master Agreement example. It is the clearest fresh-clone
walkthrough:

```bash
vei ui serve \
  --root docs/examples/enron-master-agreement-public-context/workspace \
  --host 127.0.0.1 \
  --port 3055
```

### Proof examples

- [Enron Master Agreement Example](examples/enron-master-agreement-public-context/README.md)
  - Branch point: Debra Perlingiere is about to send the Master Agreement draft to Cargill on September 27, 2000.
  - What actually happened: The draft went outside quickly, then the thread widened into a long reassignment and redline tail with no visible formal signoff.
- [Enron PG&E Power Deal Example](examples/enron-pge-power-deal/README.md)
  - Branch point: Sara Shackleton is moving a PG&E financial power deal while the counterparty credit picture is deteriorating.
  - What actually happened: The deal thread kept moving through the legal and commercial loop while the wider PG&E situation worsened.
- [Enron California Crisis Strategy Example](examples/enron-california-crisis-strategy/README.md)
  - Branch point: Tim Belden's desk receives a preservation order tied to the California crisis while the trading strategy is still active.
  - What actually happened: The preservation-order thread stayed inside the active crisis loop while the desk was still deciding how far to halt or continue.
- [Enron Baxter Press Release Example](examples/enron-baxter-press-release/README.md)
  - Branch point: The Cliff Baxter press-release loop is active and the company has to decide how transparent, delayed, or reassuring the public message should be.
  - What actually happened: The communications loop moved through a tight internal chain while the company shaped how much to say and how fast to say it.
- [Enron Braveheart Forward Example](examples/enron-braveheart-forward/README.md)
  - Branch point: The Braveheart structure is being forwarded through the valuation and review chain as the company decides whether to reopen the accounting question.
  - What actually happened: The thread kept moving through a narrow finance and legal chain tied to the larger broadband and structure story.

### Narrative examples

- [Enron Watkins Follow-up Example](examples/enron-watkins-follow-up/README.md)
- [Enron Q3 Disclosure Review Example](examples/enron-q3-disclosure-review/README.md)
- [Enron Skilling Resignation Materials Example](examples/enron-skilling-resignation-materials/README.md)
<!-- END GENERATED ENRON CASES -->

## Business-Outcome Benchmark

The Enron benchmark asks:

From one real Enron decision point, does a candidate action make the later
business state look better or worse on outcomes a company cares about?

Each model sees only the canonical event history before the branch point and a
structured action description for the candidate move. It does not get generated
rollout messages or post-branch summary fields.

The model predicts later evidence that can actually be read from the archive,
including outside-recipient spread, outside forwarding, legal follow-up burden,
review-loop burden, executive escalation, fanout, coordination load, time to
follow-up, reassurance language, disagreement markers, and attachment
recirculation.

VEI converts those evidence heads into five business-facing proxy scores:

- `enterprise_risk`
- `commercial_position_proxy`
- `org_strain_proxy`
- `stakeholder_trust`
- `execution_drag`

These are proxy outcomes. Enron email can support evidence and business proxies;
it does not support true profit ground truth or true HR outcome ground truth.

## Benchmark Commands

```bash
# Build the factual dataset and held-out Enron pack
vei whatif benchmark build \
  --rosetta-dir /path/to/full/rosetta \
  --artifacts-root _vei_out/whatif_benchmarks/branch_point_ranking_v2 \
  --label enron_business_outcome_public_context

# Train one model family
vei whatif benchmark train \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context \
  --model-id jepa_latent

# Evaluate the trained model
vei whatif benchmark eval \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context \
  --model-id jepa_latent
```

## Shipped reference backend

The current fresh-clone headline path is the shipped `full_context_transformer`
reference backend under `data/enron/reference_backend/`. A fresh clone can open
the repo-owned Enron bundles and use that checkpoint without setting an external
path.

The current shipped metrics card reports:

- factual next-event AUROC `0.787817`
- factual next-event Brier `0.332025`
- calibration ECE `0.373951`
- 7,613 train rows and 1,631 validation rows

Use those numbers as the main factual forecasting headline for the repo-owned
Enron path. They are weaker than the earlier mail-heavier checkpoint, and that
gap is the current cost of moving the shipped Enron path onto the thicker
canonical timeline.

## Refresh Paths

```bash
# Refresh saved example bundles and screenshots
make enron-example
make enron-screens

# Refresh public fixtures
python scripts/prepare_enron_public_context.py

# Build or refresh the checked-in sample from a full archive
python scripts/build_enron_rosetta_sample.py

# Refresh the full Rosetta archive itself
python scripts/build_enron_rosetta.py --prefer-local-source --include-content

# Package a new full-dataset release asset
make package-enron-full
```
