from __future__ import annotations

import re
from pathlib import Path


def _metrics_card_value(label: str) -> str:
    text = Path("data/enron/reference_backend/metrics_card.md").read_text(
        encoding="utf-8"
    )
    pattern = rf"- {re.escape(label)}: `([^`]+)`"
    match = re.search(pattern, text)
    assert match is not None
    return match.group(1)


def test_enron_benchmark_doc_tracks_shipped_reference_metrics() -> None:
    text = Path("docs/ENRON_BUSINESS_OUTCOME_BENCHMARK.md").read_text(encoding="utf-8")

    assert "## Shipped reference backend" in text
    assert "`full_context_transformer`" in text
    assert f"`{_metrics_card_value('Factual next-event AUROC')}`" in text
    assert f"`{_metrics_card_value('Factual next-event Brier')}`" in text


def test_docs_show_clean_optional_jepa_install_path() -> None:
    paths = [
        Path("README.md"),
        Path("docs/WHATIF.md"),
        Path("docs/AGENT_ONBOARDING.md"),
    ]
    for path in paths:
        text = path.read_text(encoding="utf-8")
        assert 'pip install -e ".[jepa]"' in text


def test_evals_doc_positions_clearwater_as_smoke_path() -> None:
    text = Path("docs/EVALS.md").read_text(encoding="utf-8")

    assert "flagship learned path" in text
    assert "kernel and workflow smoke tests" in text
