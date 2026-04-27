# News Public Example

News timelines are the public outside-in example. Use them when you want to test
whether VEI can build a dated world model from public articles, propose
state-level decision points, generate counterfactual actions, and score the
predicted future with the same JEPA path used for company data.

For the general command reference, use [WHATIF.md](WHATIF.md). This file keeps
the news-specific setup, decision-point shape, and limits in one place.

## What This Is

The news setup is not a full internal operating model. It does not see private
emails, tickets, customer records, or meeting notes. It sees dated public
articles or newspaper pages and turns them into canonical timeline events.

That makes it useful for questions like:

- as of a historical date, what public risks were visible?
- what next public event or policy choice should we test?
- what happens if a banking bill passes, a reform stalls, or an emergency credit
  support action is announced?
- which candidate action creates better predicted future risk, trust, drag,
  commercial, or public-confidence tradeoffs?

## Data Sources

The builder supports two historical public-news sources:

- AmericanStories: article-level extractions from Chronicling America.
- PleIAs US-PD-Newspapers: OCR newspaper pages from Chronicling America.

AmericanStories is usually the cleaner starting point because rows are
article-level. PleIAs is lower-friction and broad, but rows are OCR pages and can
carry headers, tables, page-number noise, and multi-column artifacts.

## Build A Bounded News Snapshot

Example:

```bash
python scripts/build_news_world_model_snapshot.py \
  --dataset americanstories \
  --output-root _vei_out/datasets/news_americanstories_1836_1838 \
  --start-date 1836-01-01 \
  --end-date 1838-12-31 \
  --max-pages-per-day 30 \
  --max-pages-per-source-per-day 3
```

For a PleIAs page-level sample:

```bash
python scripts/build_news_world_model_snapshot.py \
  --dataset pleias \
  --output-root _vei_out/news_world_model/pleias_1935_1939_sample \
  --start-date 1935-01-01 \
  --end-date 1939-12-31 \
  --max-pages-per-day 20 \
  --max-pages-per-source-per-day 2
```

The output is a normal VEI context snapshot, so it can be pooled with company
tenants in the multi-tenant world-model benchmark.

## Decision Point Shape

For news, a decision point does not need to be an existing article. It should be
an as-of state question:

```text
Date: 1837-09-06
State known so far:
- Panic of 1837 ongoing
- specie suspensions reported
- bank-credit stress visible
- commercial failures and unemployment pressure visible
- political debate over Treasury/banking policy active

Candidate next event/action:
- a new banking bill passes
- Treasury adopts Independent Treasury policy
- state banks suspend or resume specie
- Congress delays banking reform
- emergency credit support is announced
```

This is the same strategic state-point interface used for companies. The LLM or
a human proposes decision points and candidate actions from pre-as-of evidence
only; JEPA scores the predicted future vector.

## Run Strategic State Points

```bash
vei whatif benchmark strategic-state-points \
  --input news=_vei_out/datasets/news_americanstories_1836_1838/context_snapshot.json \
  --checkpoint _vei_out/world_model_multitenant_jepa/enron_dispatch_powr_news_fuller_cap512_h12_20260427/model_runs/jepa_latent/model.pt \
  --artifacts-root _vei_out/world_model_strategic_state_points \
  --label news_banking_statepoints \
  --as-of news=1837-09-06 \
  --decisions-per-tenant 3 \
  --candidates-per-decision 8 \
  --proposal-mode llm \
  --proposal-model gpt-5.4
```

Strategic proposal models route through Codex by default. The current default is
`gpt-5.4`; override `--proposal-model` when a newer Codex-supported model is
available. Set `VEI_STRATEGIC_PROPOSAL_BACKEND=api` only when an explicit
direct-provider API run is intended.

## Current Local Result

The latest local four-group run includes Enron, Dispatch, Powr of You, and a
small AmericanStories news sample. The human-facing current output is:

```text
_vei_out/world_model_current/world_model_decision_summary.csv
```

The news rows in that file are a worked application example. They are not
committed source data.

## Limits

The current model still uses generic business and future-state heads:

- risk
- commercial position
- organizational strain
- stakeholder trust
- execution drag
- regulatory exposure
- liquidity stress
- governance response
- evidence control
- external-confidence pressure

Those heads are workable but imperfect for news. Future news-native heads should
include follow-up coverage, topic persistence, source diversity, public-risk
escalation, market or policy attention, and correction pressure.

Until those heads exist, treat news results as exploratory decision support, not
historical causal proof.
