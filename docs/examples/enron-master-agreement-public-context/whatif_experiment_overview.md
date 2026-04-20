# master_agreement_saved_bundle_20260419

Thread: `thr_e565b47423d035c9`
Case: `thread:thr_e565b47423d035c9`
Surface: mail
Branch event: `enron_bcda1b925800af8c`
Changed actor: `debra.perlingiere@enron.com`
Historical event type: assignment
Historical subject: Master Agreement
Prompt: Keep the draft inside Enron, ask Gerald Nemec and Sara Shackleton for review, and hold the outside send.

## Historical Event
- Timestamp: 2000-09-27T13:42:00Z
- To: kathy_gerken@cargill.com
- Forward: no
- Escalation: no
- Attachment: yes

## Baseline
- Scheduled historical future events: 84
- Delivered historical future events: 84
- Baseline forecast risk score: 1.0
- First baseline events:
  - `enron_bcda1b925800af8c` assignment from `debra.perlingiere@enron.com`: Master Agreement
  - `enron_dfc83644a0a54f2d` assignment from `debra.perlingiere@enron.com`: Master Agreement
  - `enron_8bfbb287dfcdabad` assignment from `debra.perlingiere@enron.com`: Master Agreement

## LLM Actor
- Status: ok
- Summary: Debra Perlingiere withholds the draft Master Firm Purchase/Sale Agreement from external distribution and routes it internally for review by Gerald Nemec and Sara Shackleton, asking both to hold any external send to Cargill until internal comments are resolved.
- Delivered actions: 2
- Inbox count: 4
- `mail` `debra.perlingiere@enron.com` -> `gerald.nemec@enron.com` after 60000 ms: Master Agreement — internal review (do not send outside)
- `mail` `debra.perlingiere@enron.com` -> `sara.shackleton@enron.com` after 120000 ms: Master Agreement — internal review (do not send outside)

## Forecast
- Status: ok
- Backend: reference
- Summary: reference forecast completed.
- Baseline risk: 1.0
- Predicted risk: 1.0
- External-send delta: 64
- Escalation delta: 0

## Business State Change
- Summary: Slightly lower internal handling load. Trade-off: Slightly higher outside spread risk.
- Confidence: medium
- Net effect score: 0.001
- Slightly lower internal handling load.
- Slightly lower approval and escalation pressure.
- Slightly higher outside spread risk.
- Execution delay stays close to the historical path.
- The thread carries slightly more exposure.
- Internal handling looks lighter.
- Execution pace stays close to the historical path.

## Macro Outcomes
- Stock return (5d): 0.0007 -> 0.0407 (delta 0.04)
- Credit action (30d): 0.0 -> 0.0 (delta 0.0)
- FERC action (180d): 0.0 -> 0.0 (delta 0.0)