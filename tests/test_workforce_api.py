from __future__ import annotations

from vei.orchestrators.api import (
    OrchestratorActivityItem,
    OrchestratorAgent,
    OrchestratorApproval,
    OrchestratorSnapshot,
    OrchestratorSummary,
    OrchestratorSyncCapabilities,
    OrchestratorSyncHealth,
    OrchestratorTask,
)
from vei.workforce.api import (
    WorkforceCommandRecord,
    append_workforce_command,
    build_workforce_state,
    workforce_state_fingerprint,
)


def test_build_workforce_state_summarizes_vendor_neutral_snapshot() -> None:
    state = build_workforce_state(
        snapshot=OrchestratorSnapshot(
            provider="paperclip",
            company_id="company-1",
            fetched_at="2026-04-03T07:00:00+00:00",
            summary=OrchestratorSummary(
                provider="paperclip",
                company_id="company-1",
                company_name="VEI Service Ops Lab",
            ),
            capabilities=OrchestratorSyncCapabilities(
                can_pause_agents=True,
                can_resume_agents=True,
                can_comment_on_tasks=True,
                can_manage_approvals=True,
                routeable_surfaces=["slack", "service_ops"],
            ),
            agents=[
                OrchestratorAgent(
                    provider="paperclip",
                    agent_id="paperclip:ceo",
                    external_agent_id="ceo",
                    name="CEO",
                    role="chief executive officer",
                    status="running",
                    integration_mode="proxy",
                    allowed_surfaces=["slack"],
                    task_ids=["paperclip:issue-1"],
                ),
                OrchestratorAgent(
                    provider="vendorx",
                    agent_id="vendorx:watcher",
                    external_agent_id="watcher",
                    name="Watcher",
                    role="observer",
                    status="paused",
                    integration_mode="observe",
                ),
            ],
            tasks=[
                OrchestratorTask(
                    provider="paperclip",
                    task_id="paperclip:issue-1",
                    external_task_id="issue-1",
                    title="Protect the customer",
                    status="in_progress",
                )
            ],
            approvals=[
                OrchestratorApproval(
                    provider="paperclip",
                    approval_id="paperclip:approval-1",
                    external_approval_id="approval-1",
                    approval_type="hire_agent",
                    status="pending",
                )
            ],
            recent_activity=[
                OrchestratorActivityItem(
                    provider="paperclip",
                    label="CEO opened an approval",
                    created_at="2026-04-03T07:10:00+00:00",
                )
            ],
        ),
        sync=OrchestratorSyncHealth(
            provider="paperclip",
            status="healthy",
            last_success_at="2026-04-03T07:10:00+00:00",
        ),
    )

    assert state.summary.provider == "paperclip"
    assert state.summary.company_name == "VEI Service Ops Lab"
    assert state.summary.sync_status == "healthy"
    assert state.summary.observed_agent_count == 2
    assert state.summary.governable_agent_count == 1
    assert state.summary.steerable_agent_count == 2
    assert state.summary.active_agent_count == 1
    assert state.summary.task_count == 1
    assert state.summary.pending_approval_count == 1
    assert state.summary.routeable_surface_count == 2
    assert state.summary.latest_activity_at == "2026-04-03T07:10:00+00:00"


def test_workforce_state_fingerprint_ignores_clock_noise_but_not_real_changes() -> None:
    baseline = build_workforce_state(
        snapshot=OrchestratorSnapshot(
            provider="paperclip",
            company_id="company-1",
            fetched_at="2026-04-03T07:00:00+00:00",
            summary=OrchestratorSummary(
                provider="paperclip",
                company_id="company-1",
                company_name="VEI Service Ops Lab",
            ),
            agents=[
                OrchestratorAgent(
                    provider="paperclip",
                    agent_id="paperclip:ceo",
                    external_agent_id="ceo",
                    name="CEO",
                )
            ],
        ),
        sync=OrchestratorSyncHealth(
            provider="paperclip",
            status="healthy",
            last_attempt_at="2026-04-03T07:00:00+00:00",
            last_success_at="2026-04-03T07:00:00+00:00",
        ),
    )
    same_meaning = build_workforce_state(
        snapshot=baseline.snapshot.model_copy(
            update={"fetched_at": "2026-04-03T07:30:00+00:00"}
        ),
        sync=baseline.sync.model_copy(
            update={
                "last_attempt_at": "2026-04-03T07:30:00+00:00",
                "last_success_at": "2026-04-03T07:30:00+00:00",
            }
        ),
    )

    changed = append_workforce_command(
        baseline,
        WorkforceCommandRecord(
            provider="paperclip",
            action="pause",
            created_at="2026-04-03T07:40:00+00:00",
            agent_id="paperclip:ceo",
        ),
    )

    assert workforce_state_fingerprint(baseline) == workforce_state_fingerprint(
        same_meaning
    )
    assert workforce_state_fingerprint(changed) != workforce_state_fingerprint(baseline)
