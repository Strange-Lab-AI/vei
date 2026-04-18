# Contributing

## Setup

Use Python 3.11 and the repo-managed virtual environment.

```bash
make setup-full
```

The repo reads local secrets from `.env`. Keep that file untracked.

## Daily loop

Run the fast loop while you work:

```bash
make check
make test
```

Run the full repo gates before you open a PR:

```bash
make check-full
make test-full
make dynamics-eval
VEI_LLM_LIVE_BYPASS=1 make llm-live
make deps-audit
```

Use `make enron-example` after what-if artifact changes. Use `make clean-workspace` before committing if `_vei_out/` or `.artifacts/` picked up throwaway output.

## Module boundary rule

Code in one module calls another module through `vei/<module>/api.py`.

- Use `from vei.<module>.api import ...` for cross-module imports.
- Keep data-shape imports behind the same public surface.
- Treat `vei.data`, `vei.llm`, `vei.policy`, and `vei.behavior` as shared utility modules.

Run this check directly when you touch module boundaries:

```bash
python scripts/check_import_boundaries.py --strict --max-violations 0
```

## Tests and artifacts

- Pytest configuration lives in `pyproject.toml`.
- Repo-owned generated artifacts belong in `docs/examples/...` only when they are deliberate fixtures.
- Scratch output belongs in `_vei_out/` or `.artifacts/`.
- `_vei_out/`, `.artifacts/`, and `**/trace.jsonl` are gitignored.

## Branches and PRs

- Branch from `main`.
- Keep changes focused.
- Use Conventional Commit messages.
- Include the commands you ran and the result in the PR description.
- Link related issues or benchmark context when it exists.

## Review standard

Before asking for review, make sure the branch is in the new steady state:

- no removed shim or backend names remain
- the strict boundary check passes
- test, security, and dynamics gates pass
- docs match the code paths you changed
