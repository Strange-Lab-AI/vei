# Paperclip Control Room Report

Date: 2026-04-03

## What this run proved

VEI now treats outside agent work as part of the company world instead of as a sidecar feed.

In the live local Paperclip run, VEI could:

- show the outside team, work items, approvals, notes, and recent activity in both the Company view and the Operator Console
- store that outside-workforce state inside the VEI world so it appears in company snapshots, timelines, and the work graph
- send guidance back into Paperclip from VEI
- make a board decision in VEI and push that decision back into Paperclip
- record VEI's own control actions alongside Paperclip's follow-on work so the cause-and-effect chain is visible

## Real setup used for the proof run

- Paperclip URL: `http://127.0.0.1:3100`
- VEI Studio URL: `http://127.0.0.1:3511`
- VEI Operator Console URL: `http://127.0.0.1:3511/pilot`
- Paperclip company: `VEI Service Ops Lab`
- VEI company world: `Pinnacle Analytics`
- Crisis lens: `Enterprise Renewal at Risk`

This run used a real local Paperclip instance, not the earlier fake compatibility service.

## What VEI could see

At the time of the final proof capture, VEI showed:

- 2 Paperclip agents
- 4 Paperclip tasks
- 1 completed approval trail for the engineer hire
- recent activity including comments, document creation, approval events, and status changes
- VEI-issued control actions in the same activity story

The Company view now also surfaces this as a live Control Room inside the world model instead of hiding it in a separate setup screen.

## What VEI actually did

### 1. VEI sent guidance into a Paperclip task

From the Company view Control Room, VEI posted this instruction on `VEI-1`:

> Keep the hire blocked until the board can see a first-week execution plan for VEI-2 and VEI-3, then resubmit if needed.

VEI recorded that action as a control command, and Paperclip showed it as a new task comment.

On the refreshed visual pass, VEI also posted a follow-up board note on `VEI-4` from the Operator Console:

> VEI board note: turn the completed feature work into a one-paragraph operator proof with user impact, test evidence, and the next safest move.

That refreshed run now shows the full sequence in the Operator Console activity feed:

- VEI records `Guided task`
- Paperclip records the new `Issue Comment Added`
- the CEO heartbeat is visible right after the VEI note

### 2. VEI made the board decision from inside VEI

From the same VEI control surface, VEI approved the hire request with this decision note:

> Approved from VEI. Execute VEI-2 first, post a checkpoint when the workspace is ready, and do not start VEI-4 until the board sees the product brief from VEI-3.

VEI recorded that approval as a control command, and Paperclip changed the approval state to approved.

## What happened afterward inside Paperclip

After VEI's guidance and approval, the outside team responded inside Paperclip:

- the CEO added a new comment on `VEI-1` summarizing the first-week execution plans
- the CEO created a `First-Week Execution Plan` document for `VEI-2`
- the CEO created a `First-Week Execution Plan` document for `VEI-3`
- the hire approval moved to approved
- Paperclip queued the requester wakeup for the now-approved hire
- the task state on `VEI-1` updated again after the VEI decision

This is the core proof point: VEI was not only watching the outside system. VEI changed the outside workflow, and the outside workflow reacted in a visible way.

## Why this is better than the old mirror framing

The new flow makes the dashboard meaningful because it answers the real operating questions in one place:

- who is working
- what they are working on
- what the board is being asked to decide
- what VEI has told the team
- what changed in the company because of those decisions

That is much closer to a true control room than the older mirror-only framing.

## World-model impact

The outside workforce is now stored as company state inside VEI. In practice that means:

- the workforce snapshot is part of the world session
- control actions become timeline events
- the work graph can include outside agents, tasks, approvals, and VEI control actions
- Studio can show the workforce state directly from the world

This turns the outside team into part of the world model instead of a separate dashboard-only integration.

## Current limit

This proof run used Paperclip agents in `observe` mode, with VEI steering them through comments, pause or resume controls, and approval decisions.

That means this run demonstrates strong visibility and real steering, but not full before-the-fact gating of every low-level action. The architecture now supports vendor-neutral `proxy`, `ingest`, and `observe` modes so we can extend that later without reshaping the core again.

## Local proof artifacts

Use these tracked repo assets for the visual proof:

- Refreshed Company Control Room full capture:
  - `docs/assets/paperclip-control-room/demo-frame-03-control-room.png`
- Refreshed Operator Console full capture:
  - `docs/assets/paperclip-control-room/demo-frame-01-pilot-top.png`
- VEI intervention in the operator activity stream:
  - `docs/assets/paperclip-control-room/demo-frame-02-pilot-activity.png`
- VEI intervention story in the company view:
  - `docs/assets/paperclip-control-room/demo-frame-04-intervention-story.png`
- Demo GIF:
  - `docs/assets/paperclip-control-room/vei-paperclip-demo.gif`
- Demo MP4:
  - `docs/assets/paperclip-control-room/vei-paperclip-demo.mp4`

## Bottom line

VEI now works as a real governance and operating console for a live Paperclip workspace. It can see the outside work, store it as part of the world, guide it, decide on it, and show the downstream result in one company-level view.
