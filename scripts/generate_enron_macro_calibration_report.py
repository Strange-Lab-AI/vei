from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

DEFAULT_ROWS_PATH = Path("data/enron/macro_outcome_rows.jsonl")
DEFAULT_REPORT_ROOT = Path("studies/macro_calibration_enron_v1")


def _load_rows(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        rows.append(json.loads(text))
    return rows


def _float_values(rows: Iterable[dict[str, object]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(key)
        if isinstance(value, (int, float)):
            values.append(float(value))
    return values


def _paired_values(
    rows: Iterable[dict[str, object]],
    *,
    predictor_key: str,
    outcome_key: str,
) -> tuple[list[float], list[float]]:
    predictors: list[float] = []
    outcomes: list[float] = []
    for row in rows:
        predictor = row.get(predictor_key)
        outcome = row.get(outcome_key)
        if not isinstance(predictor, (int, float)):
            continue
        if not isinstance(outcome, (int, float)):
            continue
        predictors.append(float(predictor))
        outcomes.append(float(outcome))
    return predictors, outcomes


def _rank(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    cursor = 0
    while cursor < len(indexed):
        next_cursor = cursor + 1
        while (
            next_cursor < len(indexed)
            and indexed[next_cursor][1] == indexed[cursor][1]
        ):
            next_cursor += 1
        average_rank = (cursor + next_cursor - 1) / 2 + 1
        for original_index, _ in indexed[cursor:next_cursor]:
            ranks[original_index] = average_rank
        cursor = next_cursor
    return ranks


def _pearson(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    mean_left = sum(left) / len(left)
    mean_right = sum(right) / len(right)
    numerator = sum(
        (lval - mean_left) * (rval - mean_right)
        for lval, rval in zip(left, right)
    )
    left_var = sum((value - mean_left) ** 2 for value in left)
    right_var = sum((value - mean_right) ** 2 for value in right)
    if left_var == 0 or right_var == 0:
        return None
    return numerator / ((left_var ** 0.5) * (right_var ** 0.5))


def _spearman(left: list[float], right: list[float]) -> float | None:
    return _pearson(_rank(left), _rank(right))


def _auroc(labels: list[float], scores: list[float]) -> float | None:
    positives = [score for label, score in zip(labels, scores) if label >= 0.5]
    negatives = [score for label, score in zip(labels, scores) if label < 0.5]
    total_pairs = len(positives) * len(negatives)
    if total_pairs == 0:
        return None
    wins = 0.0
    for positive in positives:
        for negative in negatives:
            if positive > negative:
                wins += 1.0
                continue
            if positive == negative:
                wins += 0.5
    return wins / total_pairs


def _brier(labels: list[float], scores: list[float]) -> float | None:
    if not labels:
        return None
    return sum((score - label) ** 2 for label, score in zip(labels, scores)) / len(
        labels
    )


def _rounded(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 3)


def _report_lines(metrics: dict[str, float | int | None]) -> list[str]:
    row_count = int(metrics.get("row_count") or 0)
    heldout_count = int(metrics.get("heldout_count") or 0)
    sample_count = int(metrics.get("sample_count") or 0)
    stock_spearman = metrics.get("stock_spearman")
    credit_auroc = metrics.get("credit_auroc")
    credit_brier = metrics.get("credit_brier")
    ferc_auroc = metrics.get("ferc_auroc")
    ferc_brier = metrics.get("ferc_brier")
    supported = bool(
        isinstance(stock_spearman, (int, float))
        and isinstance(credit_auroc, (int, float))
        and isinstance(ferc_auroc, (int, float))
        and credit_auroc >= 0.6
    )
    claim_line = (
        "The current email-path proxy scores show enough movement against stock and credit outcomes to support a careful macro warning claim."
        if supported
        else "The current email-path proxy scores stay weak or mixed, so the stronger bankruptcy-mechanism claim should stay narrow."
    )
    return [
        "# Enron Macro Calibration Report",
        "",
        "This study checks whether the tracked email-path risk proxy moves with the repo-owned stock, credit, and FERC outcome timelines.",
        "",
        "## Dataset",
        "",
        f"- Total rows: {row_count}",
        f"- Held-out benchmark rows: {heldout_count}",
        f"- Sampled factual rows: {sample_count}",
        "- Predictor: `proxy_risk_score` from the saved historical email-path replay",
        "",
        "## Results",
        "",
        f"- Stock return (5d) Spearman: {stock_spearman}",
        f"- Credit action (30d) AUROC: {credit_auroc}",
        f"- Credit action (30d) Brier: {credit_brier}",
        f"- FERC action (180d) AUROC: {ferc_auroc}",
        f"- FERC action (180d) Brier: {ferc_brier}",
        "",
        "## Read",
        "",
        claim_line,
        "",
        "These numbers still measure an email-path proxy against macro outcomes. They do not turn the archive into direct market foresight.",
        "",
    ]


def generate_report(
    *,
    rows_path: Path = DEFAULT_ROWS_PATH,
    report_root: Path = DEFAULT_REPORT_ROOT,
) -> tuple[Path, Path]:
    rows = _load_rows(rows_path)
    stock_predictors, stock_outcomes = _paired_values(
        rows,
        predictor_key="proxy_risk_score",
        outcome_key="stock_return_5d",
    )
    credit_predictors, credit_outcomes = _paired_values(
        rows,
        predictor_key="proxy_risk_score",
        outcome_key="credit_action_30d",
    )
    ferc_predictors, ferc_outcomes = _paired_values(
        rows,
        predictor_key="proxy_risk_score",
        outcome_key="ferc_action_180d",
    )
    metrics = {
        "row_count": len(rows),
        "heldout_count": sum(1 for row in rows if row.get("split") == "heldout"),
        "sample_count": sum(1 for row in rows if row.get("split") == "sample"),
        "stock_spearman": _rounded(_spearman(stock_predictors, stock_outcomes)),
        "credit_auroc": _rounded(_auroc(credit_outcomes, credit_predictors)),
        "credit_brier": _rounded(_brier(credit_outcomes, credit_predictors)),
        "ferc_auroc": _rounded(_auroc(ferc_outcomes, ferc_predictors)),
        "ferc_brier": _rounded(_brier(ferc_outcomes, ferc_predictors)),
    }
    report_root.mkdir(parents=True, exist_ok=True)
    metrics_path = report_root / "metrics.json"
    report_path = report_root / "calibration_report.md"
    metrics_path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    report_path.write_text(
        "\n".join(_report_lines(metrics)),
        encoding="utf-8",
    )
    return metrics_path, report_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate the tracked Enron macro calibration report."
    )
    parser.add_argument(
        "--rows",
        type=Path,
        default=DEFAULT_ROWS_PATH,
        help="Path to the tracked macro outcome rows JSONL file.",
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=DEFAULT_REPORT_ROOT,
        help="Directory that will receive metrics.json and calibration_report.md.",
    )
    args = parser.parse_args()
    metrics_path, report_path = generate_report(
        rows_path=args.rows.resolve(),
        report_root=args.report_root.resolve(),
    )
    print(f"wrote: {metrics_path}")
    print(f"wrote: {report_path}")


if __name__ == "__main__":
    main()
