# Changelog

## Unreleased - 2026-04-16

Architecture rework landed in five phases. Two frozen contracts, a real in-repo learned-dynamics module, a four-layer ingest substrate, and an honesty pass across docs and names.

### Added

- `vei.events` — frozen `CanonicalEvent` v1 envelope (`schema_version`, `event_id`, `tenant_id`, `case_id`, `ts_ms`, `domain`, `kind`, `actor_ref`, `participants`, `object_refs`, `internal_external`, `provenance`, `text_handle`, `policy_tags`, `hash`). Per-domain `StateDelta` shapes version independently via `delta_schema_version`. `CanonicalEvent` is the single source of truth on the event spine; `StateStore`, run timelines, and connector receipts are derived views.
- `vei.events.governance` — first-class governance event kinds on the same spine: `governance.approval.{requested,granted,denied,expired}`, `governance.hold.{applied,released}`, `governance.surface.denied`, `governance.connector.safety_state_changed`, `governance.receipt.recorded`.
- `vei.events.legacy` — adapter that reads legacy `events.jsonl` and emits v1 envelopes, plus `scripts/migrate_events_to_canonical.py`.
- `vei.dynamics` — in-repo learned-dynamics subsystem with a frozen `DynamicsBackend` protocol and registry. Backends: `null`, `heuristic_baseline` (renamed from the former `e_jepa_proxy`), `reference` (loads a trained `benchmark_bridge` checkpoint and runs the in-repo PyTorch predictor), `external_subprocess` (optional adapter for external runtimes such as `ARP_Jepa_exp`).
- `vei.dynamics.feed` — canonical training-data feed (`canonical_feed.py`, `tenant_manifest.py`, `redaction.py`). Structured fields by default; text bodies gated per tenant opt-in.
- `vei.dynamics.eval` — held-out-company evaluation harness with separated factual and counterfactual reporting tables.
- `vei.dynamics.checkpoints` — narrow on-disk checkpoint record (backend id/version, feed schema version, determinism manifest, provenance, content hash).
- `vei.ingest` — four-layer ingest substrate: `RawLog`, `Normalizer`, `CaseResolver`, `Materializer`, `SessionMaterializer` protocols plus `IngestPipeline` orchestrator.
- `vei.ingest` — local offline defaults (`JsonlRawLog`, `DuckDBMaterializer`, `DuckDBSessionMaterializer`) and live defaults (`PostgresRawLog`, `PostgresMaterializer`, `PostgresSessionMaterializer`). Postgres cursor resumes by `(created_at, record_id)`; older cursor values still parse for backward compatibility.
- `vei.ingest.cases.DefaultCaseResolver` — stamps the resolved `case_id` onto the `CanonicalEvent` before the Materializer sees it, so stored history can be reopened as a real active case.
- `WorldSession.from_session_materializer(sm, case_id)` — opt-in slice constructor that replays case slices through real surfaces (mail, Slack, docs, tickets, calendar, tools) and advances the replay clock.
- Required `dynamics-eval` Makefile target folded into `make all`.
- `.agents.yml` gains `dynamics:` and `ingest:` config sections with feature flags (`dynamics.enabled`, `dynamics.default_backend`, `ingest.profile: offline | live`).

### Changed

- `vei.whatif.experiment` now drives forecasts through `vei.dynamics.api.get_backend(...)`. The existing JEPA and proxy engines are plugged in behind the contract via `vei.whatif.dynamics_bridge`.
- Docs honesty pass in `docs/ARCHITECTURE.md`, `docs/WHATIF.md`, and `docs/EVALS.md`: new "What Is and Isn't Learned" and "Causal Identification" sections; factual and counterfactual metrics reported in separated tables.
- The built-in backends `null`, `heuristic_baseline`, and `reference` are now auto-registered on import (and restored after any `reset_registry()` call) so test setup cannot leave the registry empty.

### Renamed (with one-release deprecation shims)

- `e_jepa_proxy` forecast backend → `heuristic_baseline`. Saved bundles and CLI inputs with the old name continue to parse and canonicalise to the new name.
- `vei.rl.policy_bc.BCPPolicy` → `vei.rl.policy_frequency.FrequencyPolicy`. The old module re-exports the new class and emits a `DeprecationWarning`.

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
