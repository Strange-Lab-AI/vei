# master_agreement_saved_bundle_20260417

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
- Summary: Instead of sending the draft Master Firm Purchase/Sale Agreement to Cargill, Debra retains the draft inside Enron and requests internal review by Gerald Nemec and Sara Shackleton, instructing that no external transmission be made until internal comments are consolidated.
- Delivered actions: 2
- Inbox count: 4
- `mail` `debra.perlingiere@enron.com` -> `gerald.nemec@enron.com` after 120000 ms: Master Agreement — draft for internal review (hold external send)
- `mail` `debra.perlingiere@enron.com` -> `sara.shackleton@enron.com` after 240000 ms: Master Agreement — draft for internal legal/credit review (hold send)

## Forecast
- Status: ok
- Backend: heuristic_baseline
- Summary: Predicted risk moves down by 0.440, with escalation delta 0 and external-send delta -64.
- Baseline risk: 1.0
- Predicted risk: 0.56
- External-send delta: -64
- Escalation delta: 0

## Business State Change
- Summary: Much lower outside spread risk. Trade-off: Slightly higher internal handling load.
- Confidence: medium
- Net effect score: 0.209
- Much lower outside spread risk.
- Much stronger commercial position.
- Much stronger relationship stability.
- Slightly higher internal handling load.
- The thread looks much safer to contain.
- Internal handling looks heavier.
- Near-term execution looks faster.

## Macro Outcomes
- Stock return (5d): 0.0007 -> 0.0407 (delta 0.04)
- Credit action (30d): 0.0 -> 0.0 (delta 0.0)
- FERC action (180d): 0.0 -> 0.0 (delta 0.0)