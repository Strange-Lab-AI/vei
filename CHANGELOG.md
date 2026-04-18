# Changelog

## Unreleased

### Changed

- Strengthened the repo-owned Enron dataset path with four saved bundles, richer public-company fixtures, repo-local Rosetta data, and macro outcome side data.
- Cleaned up the Watkins saved example so it now points at the follow-up note that is actually present in the archive, and refreshed the Enron bundle docs and screenshots around that corpus-backed branch.

## 0.3.0 - 2026-04-17

### Added

- `docs/AGENT_ONBOARDING.md` with a repo-specific briefing for fresh coding agents.
- `vei/rl/api.py` as the supported public RL surface for `FrequencyPolicy` and `run_policy`.
- Strict pytest configuration in `pyproject.toml`, xdist-based parallel test runs, a committed detect-secrets baseline, semgrep in `check-full`, and a threshold-based dynamics metrics validator wired into `make dynamics-eval`.

### Changed

- `vei.whatif.filenames` is now the canonical home for what-if artifact filename constants, and those constants are also re-exported from `vei.whatif.api`.
- Cross-module model and error imports now go through each module’s `api.py`, and the strict boundary scan is part of the enforced local and CI check path.
- The repo command surface now matches the gates described in `AGENTS.md` more closely: strict marker/config pytest checks, blocking detect-secrets with a committed baseline, advisory semgrep in baseline mode, and blocking semgrep in production mode.

### Removed

- `vei.rl.policy_bc.BCPPolicy`.
- The `e_jepa_proxy` what-if backend name, CLI/UI parsing paths, and saved-artifact compatibility handling.
- `whatif_ejepa_proxy_result.json` and `EJEPA_PROXY_RESULT_FILE`.
- `pytest.ini` in favor of a single pytest configuration in `pyproject.toml`.

### Migration

- Replace `from vei.rl.policy_bc import BCPPolicy` with `from vei.rl.api import FrequencyPolicy`.
- Replace any `e_jepa_proxy` backend selection with `heuristic_baseline`.
- Replace any code that imported `vei.whatif_filenames` with `vei.whatif.filenames` or `vei.whatif.api`.
- Expect only `whatif_ejepa_result.json` or `whatif_heuristic_baseline_result.json` forecast artifacts in saved what-if bundles.

## 2026-03-10

- Added a first-class `WorldSession` kernel and promoted `vei.world.api` as the stable platform boundary for full-world observe, snapshot, restore, branch, replay, inject, and event controls.
- Reworked the benchmark/eval stack onto the kernel, including reusable benchmark families for security containment, onboarding/migration, and revenue incident response plus reusable enterprise scoring dimensions.
- Added deterministic enterprise/control-plane twins for Google Admin, SIEM, Datadog, PagerDuty, feature flags, HRIS, and Jira-style issue workflows.
- Added release/export tooling (`vei release`) with benchmark/dataset manifests and a nightly GitHub Actions workflow.
- Consolidated the repo around `vei world`, deleted compatibility-only CLIs and legacy transport wrappers, and pruned committed eval artifacts plus historical planning docs.
- Licensed the repository under Business Source License 1.1 and removed tracked local workflow state from version control.

## 0.2.0a1 - 2026-02-06

Mini-alpha stabilization for external embedding.

- Added SDK runtime hooks via `SessionHook` and `EnterpriseSession.register_hook`.
- Added typed scenario manifest APIs (`list_scenario_manifest`, `get_scenario_manifest`).
- Added CLI manifest output in `vei scenarios manifest`.
- Added external-consumer example: `examples/sdk_playground_min.py`.
- Added git dependency smoke script for CI: `tools/git_dependency_smoke.sh`.
- Added an SDK surface guide for external consumers.
