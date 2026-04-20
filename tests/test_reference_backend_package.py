from __future__ import annotations

from pathlib import Path


def test_reference_backend_directory_contains_only_shipped_files() -> None:
    root = Path("data/enron/reference_backend")
    shipped = {
        "eval_result.json",
        "metadata.json",
        "metrics_card.md",
        "model.pt",
        "train_result.json",
    }

    assert root.exists()
    files = {path.name for path in root.iterdir() if path.is_file()}
    assert files == shipped


def test_reference_backend_shipped_files_do_not_embed_local_paths() -> None:
    root = Path("data/enron/reference_backend")

    for filename in ("train_result.json", "eval_result.json", "metrics_card.md"):
        text = (root / filename).read_text(encoding="utf-8")
        assert "/Users/" not in text
