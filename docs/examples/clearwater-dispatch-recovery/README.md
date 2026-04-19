# Clearwater Dispatch Recovery

Synthetic Clearwater what-if bundle built from the repo-owned service-ops story workspace.

- Scenario variant: `service_day_collision`
- Contract variant: `protect_sla`
- Branch thread: `tickets:JRA-CFS-10`
- Saved forecast file: `whatif_reference_result.json`
- Prior canonical events in the saved timeline: `44`
- Source families in the saved timeline: `google, jira, mail_archive, slack`

## Branch
Synthetic Clearwater branch on the VIP outage command thread after the dispatch recovery work has started.

- Branch subject: VIP outage command thread
- Branch event id: `history_1b8c4c0ac118f774`
- Recorded future events: `1`

## Saved forecast
- Learned backend: `reference`
- Forecast summary: reference forecast completed.

## Saved ranked comparison
- Top candidate: Stabilize the full service loop
- Top business-state summary: This move stays close to the historical business path.

## Open in Studio
```bash
vei ui serve --root /Users/rohit/Documents/Workspace/Coding/digital-enterprise-twin/docs/examples/clearwater-dispatch-recovery/workspace --host 127.0.0.1 --port 3056
```

## Bundle files
- `workspace/context_snapshot.json`: saved workspace seed
- `workspace/canonical_events.jsonl`: saved canonical timeline
- `workspace/canonical_event_index.json`: saved searchable timeline index
- `whatif_experiment_overview.md`: saved what-if summary
- `whatif_reference_result.json`: saved learned forecast
- `whatif_business_state_comparison.md`: saved candidate comparison
- `clearwater_story_overview.md`: source synthetic story walkthrough
