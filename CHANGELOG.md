# Changelog

## Unreleased - 2026-03-10

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
