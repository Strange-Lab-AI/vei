# Enron Business-Outcome Benchmark

This benchmark measures a specific question:

From one real Enron decision point, does a candidate action make the later business state look better or worse on outcomes a company cares about.

## What goes in

Each model sees only:

- the email history before the branch point
- a structured action description for the candidate move

The benchmark does not give the model generated rollout messages or any post-branch summary fields.

The held-out Enron dossiers can include a separate public-company context section with dated financial checkpoints and public news items. VEI filters that section to facts already known by the branch date. That public context is for dossier-based judging and audit review. It does not change the model input schema or the training rows in this pass.

## Combined source data

The current Enron benchmark build joins two repo-owned inputs:

- the Enron Rosetta email archive for branch history and observed futures
- the packaged public-company context fixture under `vei/whatif/fixtures/enron_public_context`

The benchmark reads the vendored Rosetta archive under `data/enron/rosetta` by default. `VEI_WHATIF_ROSETTA_DIR` still overrides that path when you want to point at a different archive.

That public-context fixture currently contains:

- 11 dated financial checkpoints
- 21 dated public news events
- 24 archived public source files
- 986 daily stock rows
- 7 credit events
- 1 FERC timeline event

Its public dates span December 31, 1998 through December 2, 2001.

That thicker fixture gives the earlier 2000 Enron branch dates much better public-company coverage than the older pack did.

## What comes out

The model predicts later email evidence that can actually be read from the archive:

- outside-recipient spread
- outside forward count
- outside attachment spread
- legal follow-up burden
- review-loop burden
- markup-loop burden
- executive escalation and executive mention heat
- participant fanout and CC expansion
- cross-functional coordination load
- time to first follow-up
- time to thread end
- review-delay burden
- reassurance language
- apology or repair language
- commitment clarity
- blame or pressure language
- internal disagreement markers
- attachment recirculation
- version turns

VEI then turns that evidence into five business-facing proxy scores:

- `enterprise_risk`
- `commercial_position_proxy`
- `org_strain_proxy`
- `stakeholder_trust`
- `execution_drag`

These are explicitly proxy outcomes. The source data is email history, so the benchmark stays within what that data can support.

## Objective packs

The benchmark ships five business objective packs:

- `minimize_enterprise_risk`
- `protect_commercial_position`
- `reduce_org_strain`
- `preserve_stakeholder_trust`
- `maintain_execution_velocity`

Each pack scores the same business heads with a different weighting rubric.

## Held-out Enron pack

The default held-out pack is `enron_business_outcome_v1`.

It contains 31 fixed Enron branch points across these case families:

- `outside_sharing`
- `legal_contract`
- `commercial_counterparty`
- `executive_regulatory`
- `coordination_strain`
- `org_heat`
- `whistleblower`
- `market_manipulation`
- `crisis_communication`
- `accounting_disclosure`

Each held-out case has 4 candidate actions.

The current benchmark build uses:

- 31 held-out Enron cases
- 4 candidate actions per case
- 155 pairwise-style dominance checks in the headline ranking table

## Judge path

The benchmark keeps factual forecasting and counterfactual ranking separate.

Factual forecasting uses the real historical future after the observed Enron action.

Counterfactual ranking uses a locked LLM judge over case dossiers only:

- one dossier per case and per business objective
- one pairwise ranking pass over the candidate set
- one saved judged ranking
- one audit queue for low-confidence or sampled cases

Each dossier now carries the same branch-filtered Enron public-company backdrop used by the replay flow.

That means the held-out judge sees the same dated public-company slice that the Studio decision scene shows for the same branch point.

The judge does not see rollout futures.

## Model families

The benchmark currently supports five model families:

- `jepa_latent`
- `full_context_transformer`
- `ft_transformer`
- `sequence_transformer`
- `treatment_transformer`

They all predict the same later-email evidence heads and the same business proxy scores.

The matched-input comparison path uses:

- the pre-branch event sequence
- the pre-branch summary features
- the structured action schema

`jepa_latent` and `full_context_transformer` both read that full bundle. The older tabular and narrower transformer baselines stay available as reference points.

## CLI

```bash
# Build the factual dataset and held-out Enron pack
vei whatif benchmark build \
  --rosetta-dir data/enron/rosetta \
  --artifacts-root _vei_out/whatif_benchmarks/branch_point_ranking_v2 \
  --label enron_business_outcome_public_context_20260412

# Train one model family
vei whatif benchmark train \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412 \
  --model-id jepa_latent

# Judge the held-out counterfactual cases
vei whatif benchmark judge \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412 \
  --model gpt-4.1-mini

# Evaluate one trained model
vei whatif benchmark eval \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412 \
  --model-id jepa_latent \
  --judged-rankings-path _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412/judge_result.json \
  --audit-records-path /path/to/completed_audit_records.json

# Run the matched-input study across models and seeds
vei whatif benchmark study \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context_20260412 \
  --label matched_input_public_context_20260412 \
  --model-id jepa_latent \
  --model-id full_context_transformer \
  --model-id treatment_transformer \
  --seed 42042 \
  --seed 42043 \
  --seed 42044 \
  --seed 42045 \
  --seed 42046 \
  --epochs 2
```

## Artifact layout

The benchmark build writes a folder like:

```text
_vei_out/whatif_benchmarks/branch_point_ranking_v2/<label>/
  branch_point_benchmark_build.json
  heldout_cases.json
  judged_ranking_template.json
  audit_record_template.json
  dataset/
    train_rows.jsonl
    validation_rows.jsonl
    test_rows.jsonl
    heldout_rows.jsonl
    dataset_manifest.json
  dossiers/
    <case_id>/
      minimize_enterprise_risk.md
      protect_commercial_position.md
      reduce_org_strain.md
      preserve_stakeholder_trust.md
      maintain_execution_velocity.md
      *.rubric.json
```

Training writes one model folder under `model_runs/<model_id>/`.

Judging writes:

- `judge_result.json`
- `audit_queue.json`

Evaluation writes:

- `model_runs/<model_id>/eval_result.json`
- `model_runs/<model_id>/predictions.jsonl`

Study runs write:

- `studies/<label>/benchmark_study_result.json`
- `studies/<label>/benchmark_study_overview.md`
- `studies/<label>/runs/<model_id>/seed_<seed>/...`

## Current comparison result

The current headline result is the matched-input multi-seed rerun over one saved Enron public-context build that was produced from the vendored Rosetta archive. A fresh clone can read these numbers and rebuild the same benchmark without reaching outside the repo. That rerun compares the three aligned models that all read the same pre-branch contract: `jepa_latent`, `full_context_transformer`, and `treatment_transformer`.

The current saved 5-seed, 2-epoch matched-input study produced these held-out decision scores:

| Model | Mean passed checks | Total checks | Mean pass rate | Seed std |
|---|---:|---:|---:|---:|
| `jepa_latent` | `80.2` | `120` | `0.668` | `0.012` |
| `full_context_transformer` | `79.4` | `120` | `0.662` | `0.031` |
| `treatment_transformer` | `68.2` | `120` | `0.568` | `0.117` |

On the simpler factual task of predicting whether anything goes outside after the branch point, all three models stayed tightly grouped around `0.98` AUROC: `0.981` for `jepa_latent`, `0.982` for `full_context_transformer`, and `0.980` for `treatment_transformer`.

The repo now also carries macro outcome side data for stock, credit, and FERC events. Those heads are available in the replay and saved bundle path, but the current Enron calibration stays weak: stock Spearman `0.041`, credit AUROC `0.370`, and FERC AUROC `0.568`. Treat the macro heads as advisory context beside the email-path evidence until those numbers improve.

The main point is that the fair rerun keeps the JEPA-style path in front on the business decision checks even after the held-out dossiers picked up the dated Enron public-company context. The full-context transformer stays close. The treatment transformer varies much more from seed to seed.

An earlier single-run reference comparison is still useful as a historical checkpoint, but it should not be treated as the headline result because it mixed narrower and richer model inputs. Use `vei whatif benchmark study` for the clean comparison path, then look under `studies/` for the aggregate report.
