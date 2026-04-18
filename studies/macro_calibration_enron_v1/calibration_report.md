# Enron Macro Calibration Report

This study checks whether the tracked email-path risk proxy moves with the repo-owned stock, credit, and FERC outcome timelines.

## Dataset

- Total rows: 62
- Held-out benchmark rows: 31
- Sampled factual rows: 31
- Predictor: `proxy_risk_score` from the saved historical email-path replay

## Results

- Stock return (5d) Spearman: 0.041
- Credit action (30d) AUROC: 0.37
- Credit action (30d) Brier: 0.516
- FERC action (180d) AUROC: 0.568
- FERC action (180d) Brier: 0.425

## Read

The current email-path proxy scores stay weak or mixed, so the stronger bankruptcy-mechanism claim should stay narrow.

These numbers still measure an email-path proxy against macro outcomes. They do not turn the archive into direct market foresight.
