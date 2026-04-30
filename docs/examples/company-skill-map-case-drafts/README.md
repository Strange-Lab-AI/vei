# Company Skill Map Case Drafts

These are the concrete draft skills I would create from the three available
company archives. They are intentionally company-specific. They should remain
`draft` and `shadow` until a human owner reviews them and the replay layer has
scored them against historical or counterfactual state points.

Sources checked:

- Enron: `_vei_out/datasets/enron_rosetta_sample/context_snapshot.json`
- Dispatch: `_vei_out/datasets/dispatch_real/context_snapshot.json`
- Powr of You: `_vei_out/datasets/powrofyou/context_snapshot.json`
- Strategic state points:
  `_vei_out/world_model_strategic_state_points/enron_dispatch_powr_news_frontier_gpt54_statepoints_20260427/proposal_responses/`

## Enron Corporation

Bundle coverage: mail archive only, 31 threads, 343 messages, from
1998-11-05 to 2001-10-31.

### `skill:enron-california-legal-hold-governance`

- Title: California legal-hold governance gate
- Status: `draft`
- Mode: `shadow`
- Domain: `legal_risk_governance`
- Trigger: A California-linked commercial action, regulatory-sensitive message,
  or market-risk update appears after a legal hold, Attorney General request, or
  preservation instruction.
- Goal: Treat California exposure as a governance-controlled matter, not an
  ordinary commercial thread.
- Steps:
  1. Read the cited California thread and extract the preservation obligation,
     affected systems, affected business groups, and deadline.
  2. Draft a legal-hold status pack listing owners, systems, record classes,
     open commercial decisions, and unresolved risk questions.
  3. Require legal, records, and market-risk approval before any new
     California-linked structured transaction or external regulatory message.
  4. Draft a controlled counterparty or regulator message only from approved
     facts and route it for legal approval before sending.
- Allowed actions: read source threads, draft internal status packs, draft
  approval checklists, propose owner assignments.
- Blocked actions: delete or alter records, move evidence outside approved
  repositories, send unreviewed external messages, approve new California
  exposure without legal and risk sign-off.
- Evidence refs:
  - `mail_archive:Thr Af4757447Fa4B384` California Attorney General preservation
    order thread.
  - `state_point:California Governance Posture` saved strategic state point.
- Replay probe: If this skill is applied at the California preservation-order
  branch point, later activity should show fewer fragmented regulatory/legal
  decisions and more preserved, owner-tagged, approval-gated communications.

### `skill:enron-master-agreement-risk-discipline`

- Title: Master agreement risk discipline
- Status: `draft`
- Mode: `shadow`
- Domain: `contracting_credit_legal`
- Trigger: A master agreement, ISDA, power deal, swap agreement, credit term,
  arbitration question, or unresolved redline appears in legal/commercial mail.
- Goal: Convert bilateral contract work from ad hoc inbox negotiation into a
  controlled legal, credit, and commercial decision lane.
- Steps:
  1. Pull the active agreement thread, open issues, current draft owner, and
     latest counterparty position.
  2. Classify each unresolved issue as credit, arbitration, evidence/version
     control, commercial economics, or legal form.
  3. Draft fallback positions for credit and arbitration using prior approved
     positions, then route unresolved issues to a joint legal-credit-commercial
     forum.
  4. Record all drafts, approvals, and concessions in one versioned tracker
     before the next external send.
- Allowed actions: summarize agreement state, update the controlled tracker,
  draft fallback-position memos, prepare internal approvals.
- Blocked actions: send a redline externally without version control, concede
  credit or arbitration terms without approval, continue parallel inbox-only
  negotiations.
- Evidence refs:
  - `mail_archive:Thr 0E578Eb0C195B5C3` ETOL interest-rate swap and master ISDA
    rush thread.
  - `mail_archive:Thr 820F7864Cf391784` master swap agreement thread.
  - `mail_archive:Thr 5C00Adfa02940001` arbitration scorecard thread.
  - `state_point:Counterparty Contracting Discipline` saved strategic state
    point.
- Replay probe: If the skill is applied to active master-agreement work, the
  replay should see contract decisions move toward fewer open-loop forwards,
  clearer issue ownership, and a smaller set of unresolved credit/arbitration
  decisions.

### `skill:enron-sensitive-decision-archive`

- Title: Sensitive decision archive and same-day review
- Status: `draft`
- Mode: `shadow`
- Domain: `executive_operating_model`
- Trigger: A legal, regulatory, accounting, market-risk, or external
  communication issue spans multiple Enron groups and has unclear decision
  rights.
- Goal: Create one governed decision record before sensitive action leaves the
  company or changes market exposure.
- Steps:
  1. Identify all involved groups, accountable executives, affected systems, and
     existing legal or accounting constraints.
  2. Create a single decision record with the question, owner, deadline,
     evidence bundle, reviewers, and non-negotiable stop rules.
  3. Require same-day legal and market-risk review for threshold-sensitive
     outbound messages or exposure changes.
  4. Summarize the approved decision back to the affected operators and archive
     the communication chain with versioned sign-off.
- Allowed actions: create read-only decision records, draft internal summaries,
  propose review routing, flag missing decision rights.
- Blocked actions: make irreversible exposure changes, send external messages,
  or split decision evidence across disconnected inbox threads without
  approval.
- Evidence refs:
  - `mail_archive:Thr 44E3Ef3A1390591A` NERC urgent review thread.
  - `mail_archive:Thr 23255C500305185D` third-quarter review, Andersen,
    investigation, bankruptcy, and lender-distribution thread.
  - `mail_archive:Thr Af4757447Fa4B384` California preservation thread.
  - `state_point:Operating Model For Sensitive Decisions` saved strategic state
    point.
- Replay probe: If the skill is applied to a sensitive-decision branch point,
  later evidence should show explicit owner/reviewer records instead of
  disconnected legal, market-risk, and commercial threads.

## Dispatch

Bundle coverage: Gmail and Notion, 1,447 mail threads, 2,757 messages, 229
Notion pages, from 2024-01-08 to 2026-04-13.

### `skill:dispatch-pilot-scope-gate`

- Title: Pilot scope gate for Songtradr/Acorns-style deals
- Status: `draft`
- Mode: `shadow`
- Domain: `customer_pilot_sales`
- Trigger: A prospect conversation reaches pilot, PoC, SOW, service agreement,
  security document, or recurring scheduling stage.
- Goal: Turn a promising customer thread into a bounded pilot with explicit
  scope, review sequence, owner, acceptance criteria, and next date.
- Steps:
  1. Pull the full customer thread, related calendar changes, attached security
     or agreement material, and any Notion account notes.
  2. Draft a one-page pilot brief: problem, buyer, pilot scope, success
     criteria, required integrations, security path, owner, dates, and price or
     commercial assumption.
  3. Ask the prospect for the exact internal review sequence, named blockers,
     and next decision owner before sending more custom material.
  4. Park the thread if the buyer cannot confirm scope, owner, timeline, or
     decision process.
- Allowed actions: draft pilot briefs, summarize review blockers, prepare
  founder follow-up, log stage and next action.
- Blocked actions: expand custom diligence without scoped pilot terms, share
  sensitive docs outside the approved packet, treat vague interest as pipeline.
- Evidence refs:
  - `gmail:Updated invitation: The Dispatch <> Songtradr : Pilot`
  - `gmail:Fwd: The Dispatch Pilot Program`
  - `gmail:Fwd: PoC SOW draft`
  - `state_point:Decide Whether Songtradr Becomes The Lead Wedge`
- Replay probe: Applied to the Songtradr branch, the skill should produce a
  clearer pilot plan or an earlier stop decision, not a longer custom-diligence
  loop.

### `skill:dispatch-external-materials-control`

- Title: Approved external-materials packet
- Status: `draft`
- Mode: `shadow`
- Domain: `trust_security_sales_ops`
- Trigger: Dispatch is about to send or revise a service agreement, security
  document, pilot material, pricing promise, or partner/customer claim.
- Goal: Keep external selling fast while controlling what claims, documents,
  versions, and security representations leave the company.
- Steps:
  1. Collect the latest agreement, security, pilot, pricing, and product-claim
     material referenced in the thread.
  2. Compare the proposed send against the approved minimal packet and flag
     anything new, sensitive, stale, or unsupported by product reality.
  3. Draft a short send/no-send recommendation with missing approvals and the
     smallest acceptable document set.
  4. Log what was shared, with recipient organization, date, version, and claim
     summary.
- Allowed actions: read materials, produce a send checklist, draft an approved
  packet, log external sharing.
- Blocked actions: send unversioned security docs, invent claims, widen
  diligence before the prospect has committed to a concrete review path.
- Evidence refs:
  - `gmail:Fwd: The Dispatch Pilot Program`
  - `gmail:Acorns VSA`
  - `state_point:Decide Whether To Harden The Selling Motion Before Scaling Outreach`
- Replay probe: Applied before external diligence sends, later evidence should
  show fewer unsupported claims and a cleaner link between customer stage and
  material shared.

### `skill:dispatch-warm-intro-filter`

- Title: Warm-intro commercial-signal filter
- Status: `draft`
- Mode: `shadow`
- Domain: `founder_gtm`
- Trigger: An investor, connector, collaborator, or friendly contact offers an
  intro, meeting, collaboration, or generic advice thread.
- Goal: Accept only warm-intro work that creates buyer access, pilot conversion,
  product learning, or credible distribution.
- Steps:
  1. Classify the thread as buyer, operator, investor, collaborator, hiring,
     vendor, or generic networking.
  2. Require one concrete commercial learning objective and one dated next
     action before founder time is allocated.
  3. Draft either a crisp buyer-facing CTA or a polite park response.
  4. Add accepted threads to the weekly deal cadence with stage, owner, next
     action, and success/failure condition.
- Allowed actions: classify threads, draft replies, update pipeline cadence,
  suggest no-op or park decisions.
- Blocked actions: create meetings with no buyer/operator signal, let investor
  intros crowd out active customer pilots, count generic networking as demand.
- Evidence refs:
  - `gmail:Intro: Jon@Dispatch <> Tm@Essence VC`
  - `gmail:Introductions - Mo, Aaron`
  - `state_point:Choose Between Warm-Intro Momentum And Offer Discipline`
- Replay probe: Applied to warm-intro periods, the skill should reduce meetings
  without clear buyer/pilot outcomes and preserve high-signal customer threads.

### `skill:dispatch-founder-calendar-deal-cadence`

- Title: Founder calendar to deal-cadence gate
- Status: `draft`
- Mode: `shadow`
- Domain: `founder_operating_cadence`
- Trigger: A new external meeting, follow-up, investor call, customer call, or
  collaboration thread requests founder calendar time.
- Goal: Make every external meeting map to pilot, buyer access, product
  learning, credible distribution, fundraising, or a deliberate no.
- Steps:
  1. Read the meeting thread and any linked Notion pipeline/fundraising/sales
     notes.
  2. Assign a meeting purpose, expected output, owner, and next-action date.
  3. Reject or defer meetings that do not map to the current founder priorities.
  4. Produce a weekly pipeline digest showing pilot-stage threads, blocked
     security/legal items, and founder-time leakage.
- Allowed actions: tag calendar items, draft weekly digests, suggest deferrals,
  prepare founder follow-up notes.
- Blocked actions: auto-accept meetings, create founder tasks without a
  strategic category, mix investor networking with buyer pipeline.
- Evidence refs:
  - `notion:Sales`
  - `notion:Fundraising`
  - `gmail:The Dispatch <> Acorns PoC`
  - `state_point:Decide Whether To Harden The Selling Motion Before Scaling Outreach`
- Replay probe: Applied to September 2024 selling activity, the skill should
  make founder-time allocation explainable against current strategic priorities.

## Powr of You

Bundle coverage: Gmail only, 10,025 threads, 12,000 messages, from 2021-12-02
to 2026-04-18.

### `skill:powrofyou-enterprise-data-feed-gate`

- Title: Enterprise data-feed gate for Vi.co-style inbound
- Status: `draft`
- Mode: `shadow`
- Domain: `enterprise_data_commercialization`
- Trigger: A buyer asks for raw feeds, identifier-linked behavior data, MAID,
  hashed email, sample schema, scale, market coverage, or supply mechanics.
- Goal: Convert serious enterprise data-feed inbound into a bounded,
  privacy-reviewed pilot without over-promising supply or permitted use.
- Steps:
  1. Extract the buyer ask: geography, identifiers, data categories, scale,
     delivery method, sample requirement, use case, and deadline.
  2. Run an internal capability and supply check before any sample, including
     provenance, consent basis, permitted use, identifier policy, and available
     volume.
  3. Draft a scoped pilot offer with one market, one identifier type, one sample
     schema, acceptance criteria, and explicit constraints.
  4. Escalate to a named founder owner for commercial, data, and legal approval
     before replying.
- Allowed actions: summarize buyer requirements, draft internal readiness
  memo, draft bounded pilot terms, request approval.
- Blocked actions: send raw samples before provenance review, imply unrestricted
  behavioral surveillance, promise scale or markets not evidenced in the archive.
- Evidence refs:
  - `gmail:Re: Vi.co data opportunity`
  - `state_point:Should Powr of You turn the Vi.co inbound into a flagship enterprise deal?`
- Replay probe: Applied to the Vi.co branch, the skill should produce either a
  bounded enterprise pilot or a controlled no-go, not an unbounded data-supply
  commitment.

### `skill:powrofyou-partner-pipeline-triage`

- Title: Partner pipeline triage from noisy inbound
- Status: `draft`
- Mode: `shadow`
- Domain: `partnerships_gtm`
- Trigger: A partnership, research buyer, panel/supply, insights, vendor, or
  collaboration inbound arrives.
- Goal: Separate commercially real partner demand from vendor noise and generic
  networking.
- Steps:
  1. Classify the inbound as buyer demand, supply-side platform, research
     partner, vendor, recruiter, newsletter, or low-fit networking.
  2. Score commercial reality, product fit, execution burden, privacy risk, and
     evidence of repeatable demand.
  3. Draft either a qualification reply, partner-pilot brief, founder gate memo,
     or decline/park response.
  4. Keep one shared partner brief so Shruti, Keshav, and operators use the same
     offer, boundaries, and disallowed claims.
- Allowed actions: classify inbound, draft qualification replies, create partner
  briefs, escalate founder go/no-go.
- Blocked actions: treat newsletters or vendor pitches as pipeline, send vague
  partner claims, pursue multiple partnership stories with conflicting offers.
- Evidence refs:
  - `gmail:Re: Directing the question to Shruti`
  - `gmail:RE: MetrixLab Intro`
  - `gmail:Access new segment-level data in supply APIs`
  - `state_point:Should Powr of You build a real partner pipeline from the recent inbound, or cut most of it?`
- Replay probe: Applied to recent inbound, the skill should rank the Vi.co and
  real partner threads above vendor/recruiting/newsletter noise.

### `skill:powrofyou-claim-discipline-and-positioning`

- Title: Evidence-bound public positioning and claim discipline
- Status: `draft`
- Mode: `shadow`
- Domain: `marketing_trust_governance`
- Trigger: A landing page, deck, outbound email, partner reply, or founder note
  makes claims about behavioral data, privacy-led research, consent, supply,
  measurement, or identifier-linked products.
- Goal: Keep market positioning sharp but constrained by documented capability,
  provenance, and trust boundaries.
- Steps:
  1. Compare the proposed claim against cited customer/partner demand and known
     capability evidence.
  2. Mark each claim as supported, needs qualification, disallowed, or requires
     governance review.
  3. Draft one canonical narrative and one disallowed-claims list for current
     external use.
  4. Require founder approval before public language implies identifier-linked
     behavioral-data products at unrestricted scale.
- Allowed actions: draft claim review, edit narrative, maintain disallowed
  claims, request governance approval.
- Blocked actions: publish broad unsupported claims, imply unrestricted data
  access, change category positioning from one-off inbound without review.
- Evidence refs:
  - `gmail:Re: Vi.co data opportunity`
  - `gmail:We are reviewing your achievements`
  - `state_point:Should Powr of You change its public story now, or keep the positioning narrow and evidence-bound?`
- Replay probe: Applied to positioning work after the Vi.co signal, the skill
  should sharpen messaging around real demand while preserving privacy and
  capability constraints.

### `skill:powrofyou-hiring-vendor-noise-separation`

- Title: Hiring and vendor noise separation
- Status: `draft`
- Mode: `shadow`
- Domain: `ops_signal_triage`
- Trigger: The inbox contains recruiting, Wellfound, developer test task,
  vendor outreach, SaaS update, newsletter, or platform notification traffic
  alongside customer/partner messages.
- Goal: Prevent high-volume operational noise from contaminating customer,
  partner, and market-signal judgment.
- Steps:
  1. Classify threads into customer/partner, recruiting, vendor, platform,
     finance/admin, newsletter, and personal/noise buckets.
  2. Route recruiting and vendor items into their own queue with lightweight
     owner, deadline, and response template.
  3. Preserve customer/partner signal for founder review and exclude low-fit
     noise from skill-map generation.
  4. Report weekly counts and the top commercial-signal threads separately from
     operational inbox volume.
- Allowed actions: classify and summarize threads, draft routing decisions,
  prepare weekly signal/noise reports.
- Blocked actions: infer company strategy from recruiter/vendor volume, mix
  hiring pipeline with customer demand, discard unreviewed customer/partner
  signal.
- Evidence refs:
  - `gmail:Re: Powrofyou_QA_Wellfound_Immediate`
  - `gmail:Re: Applying for Python SDE Role at py-insights`
  - `gmail:Re: Developer Test Task`
  - `gmail:Data Analyst Application`
- Replay probe: Applied to the 12,000-message archive, the skill should keep
  Vi.co, MetrixLab, Cint/Lucid, and real partner demand visible while reducing
  false strategic signal from recruiting and vendor threads.
