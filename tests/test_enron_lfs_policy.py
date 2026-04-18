from __future__ import annotations

from pathlib import Path


def test_enron_large_files_stay_under_lfs_patterns() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    gitattributes = (repo_root / ".gitattributes").read_text(encoding="utf-8")

    assert (
        "data/enron/raw/*.tar.gz filter=lfs diff=lfs merge=lfs -text" in gitattributes
    )
    assert (
        "data/enron/source/*.parquet filter=lfs diff=lfs merge=lfs -text"
        in gitattributes
    )
    assert (
        "data/enron/rosetta/*.parquet filter=lfs diff=lfs merge=lfs -text"
        in gitattributes
    )
