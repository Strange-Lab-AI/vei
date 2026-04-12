# Enron Business-Outcome Benchmark

This benchmark measures a specific question:

From one real Enron decision point, does a candidate action make the later business state look better or worse on outcomes a company cares about.

## What goes in

Each model sees only:

- the email history before the branch point
- a structured action description for the candidate move

The benchmark does not give the model generated rollout messages or any post-branch summary fields.

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

It contains 24 fixed Enron branch points across these case families:

- sensitive outside sharing
- legal review and contract control
- commercial counterpart handling
- executive and regulatory pressure
- internal coordination strain
- personnel and organizational heat

Each held-out case has 4 candidate actions.

The current benchmark build uses:

- 24 held-out Enron cases
- 4 candidate actions per case
- 120 pairwise-style dominance checks in the headline ranking table

## Judge path

The benchmark keeps factual forecasting and counterfactual ranking separate.

Factual forecasting uses the real historical future after the observed Enron action.

Counterfactual ranking uses a locked LLM judge over case dossiers only:

- one dossier per case and per business objective
- one pairwise ranking pass over the candidate set
- one saved judged ranking
- one audit queue for low-confidence or sampled cases

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
  --rosetta-dir /path/to/rosetta \
  --artifacts-root _vei_out/whatif_benchmarks/branch_point_ranking_v2 \
  --label enron_business_outcome_reset

# Train one model family
vei whatif benchmark train \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_reset \
  --model-id jepa_latent

# Judge the held-out counterfactual cases
vei whatif benchmark judge \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_reset \
  --model gpt-4.1-mini

# Evaluate one trained model
vei whatif benchmark eval \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_reset \
  --model-id jepa_latent \
  --judged-rankings-path _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_reset/judge_result.json \
  --audit-records-path /path/to/completed_audit_records.json

# Run the matched-input study across models and seeds
vei whatif benchmark study \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_reset \
  --label matched_input_rerun \
  --model-id jepa_latent \
  --model-id full_context_transformer \
  --model-id treatment_transformer \
  --seed 42042 \
  --seed 42043 \
  --seed 42044 \
  --epochs 2 \
  --judged-rankings-path _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_reset/judge_result.json
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

The current headline result is the matched-input multi-seed rerun over the saved Enron reset build. That rerun compares the three aligned models that all read the same pre-branch contract: `jepa_latent`, `full_context_transformer`, and `treatment_transformer`.

The current saved 5-seed, 2-epoch matched-input study produced these held-out decision scores:

| Model | Mean passed checks | Total checks | Mean pass rate | Seed std |
|---|---:|---:|---:|---:|
| `jepa_latent` | `76.6` | `120` | `0.638` | `0.022` |
| `full_context_transformer` | `75.2` | `120` | `0.627` | `0.025` |
| `treatment_transformer` | `65.6` | `120` | `0.547` | `0.108` |

On the simpler factual task of predicting whether anything goes outside after the branch point, all three models stayed tightly grouped around `0.98` AUROC.

The main point is that the fair rerun changed the ordering. Once the models use the same pre-branch inputs and the result is averaged across seeds, the JEPA-style path slightly leads the matched full-context transformer on the business decision checks, while the treatment transformer drops back and varies much more from seed to seed.

An earlier single-run reference comparison is still useful as a historical checkpoint, but it should not be treated as the headline result because it mixed narrower and richer model inputs. Use `vei whatif benchmark study` for the clean comparison path and look under `studies/` for the aggregate report.
