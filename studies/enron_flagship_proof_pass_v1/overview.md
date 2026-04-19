# Enron Flagship Proof Pass

This proof pass checks the shipped `full_context_transformer` reference backend against judged rankings on the four flagship Enron cases:

- `master_agreement`
- `watkins_followup_questions`
- `california_crisis_order`
- `pg_e_power_deal`

The judge run uses the branch-filtered case summary, short history preview, public context slice, and candidate actions for each business objective.

## Results

- Judged rankings: `20`
- Judge top-1 agreement: `0.15`
- Judge pairwise accuracy: `0.667`
- Judge Kendall tau: `0.333`
- Dominance checks: `66/155` (`0.426`)
- Factual next-event AUROC: `0.787822`
- Factual next-event Brier: `0.332025`
- Calibration ECE: `0.373951`

## Read

The shipped reference backend carries real factual signal on the thicker Enron timeline, and it reaches better-than-chance pairwise agreement on the flagship judged rankings.

The flagship ranking numbers are still moderate rather than strong. They support a "promising but not yet definitive" claim for the counterfactual ranking path.

## Rebuild

Run:

```bash
set -a; source .env; set +a
.venv/bin/python scripts/run_enron_flagship_proof_pass.py
```

That command writes the live judged artifacts under `_vei_out/enron_flagship_proof_pass/`.
