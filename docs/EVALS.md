# Evaluation and Calibration

VEI evaluation has three layers. Each layer answers a different question and produces a different kind of artifact. The layers are designed to work together but can run independently.

Use `docs/ENRON_BUSINESS_OUTCOME_BENCHMARK.md` for the specific Enron benchmark setup. This document covers the evaluation framework itself.

## What Is and Isn't Learned

VEI today is a deterministic enterprise simulator, governed twin, replay
platform, and learned forecasting workbench. It is not a finished universal CEO
recommender. The reference backend (`vei.dynamics.backends.reference`) is a real
PyTorch model trained on canonical event sequences. The heuristic baseline is a
tag-driven heuristic, not a learned model. The repo-owned Enron benchmark is the
shipped flagship learned path; the multi-tenant benchmark is the pooled
world-model experiment. Clearwater workflow families stay in the repo as
kernel and workflow smoke tests. See `docs/ARCHITECTURE.md` for the full
breakdown.

The world-model benchmark turns each company archive into timestamped events,
then learns rows of the form:

```text
state up to time T + action at time T -> future state after T
```

The shipped Enron reference checkpoint currently reports factual next-event
AUROC `0.787817`, Brier `0.332025`, and calibration ECE `0.373951`. The latest
local pooled JEPA run over Enron, Dispatch, and a private startup archive built
`59,920` canonical events and `17,602` eligible branch rows. It was much better
calibrated than the heuristic baseline on external spread (Brier `0.003` vs
`0.547`, ECE `0.002` vs `0.688`) and improved all five business-head MAEs. Its
external-spread AUROC was lower than the heuristic, so the claim is deliberately
narrow: factual calibration and business-head regression improved, while
counterfactual rankings remain decision support.

## Layer 1: Factual forecast metrics

The cheapest layer. No humans, no LLM calls. The model predicts what happened after the branch point, and VEI compares the prediction against the observed historical future.

Metrics:

- **AUROC** on the binary question (did information spread externally?)
- **Brier score** on the same binary (are predicted probabilities calibrated?)
- **Expected calibration error** (10-bin ECE, confirming that "70% confident" means 70% of the time)
- **Evidence head MAE** per signal (outside spread, legal burden, executive heat, etc.)
- **Business objective MAE** per objective pack

This layer tells you whether the model can predict things that actually happened. It cannot tell you anything about counterfactuals, because the counterfactual future was never observed.

## Layer 2: LLM judge

A structured LLM evaluation of counterfactual candidate rankings.

### What the judge sees

Each case produces one **dossier** per business objective. The dossier contains:

- case title and summary
- the objective question, criteria, and decision rule from the rubric
- branch event details (sender, recipients, subject, excerpt)
- pre-branch thread history
- branch-filtered public-company context when the source is Enron and dated public facts were already known
- candidate decisions with action tags, review path, coordination breadth, and outside sharing posture

The judge does **not** see rollout futures, model predictions, or any post-branch information.

### How the judge works

The judge performs **pairwise comparisons** over the candidate set (for 4 candidates, 6 pairs). For each pair it returns a preferred candidate, confidence, evidence references, and rationale. VEI aggregates the pairwise wins into a total ordering.

The call uses `temperature=0.0` and `json_mode=true`. Default model is
`gpt-4.1-mini`. Codex-session models are not called through provider API keys;
when a Codex-only model is being tested, run that test through Codex itself.

### Uncertainty detection

A judgment is flagged as uncertain when:

- self-reported confidence is below 0.65
- the judge failed to cover all expected pairs
- any candidate is missing from the final ordering

Uncertain cases always enter the audit queue. An additional ~25% of confident cases are deterministically sampled for audit.

### Judge metrics

VEI compares the model's predicted candidate ranking against the judge's ranking:

- **Top-1 agreement** (did they agree on the best candidate?)
- **Pairwise accuracy** (across all pairs, how often do they agree on which is better?)
- **Kendall tau** (rank correlation, -1 to +1)

### Rubrics

Five business objective rubrics ship with the benchmark:

| Pack ID | Question |
|---|---|
| `minimize_enterprise_risk` | Which candidate best reduces enterprise risk? |
| `protect_commercial_position` | Which candidate best protects commercial position? |
| `reduce_org_strain` | Which candidate best reduces internal coordination strain? |
| `preserve_stakeholder_trust` | Which candidate best preserves trust with affected people? |
| `maintain_execution_velocity` | Which candidate best keeps the business moving? |

Each rubric includes 3 criteria and a decision rule. See `vei/whatif/benchmark_business.py` for the full rubric definitions.

## Layer 3: Human audit and panel

The human layer serves two purposes: calibrating the automated evaluators and producing independent training data.

### Audit queue

The audit queue is auto-generated from the judge step. It contains cases where:

- the LLM judge flagged uncertainty (low confidence or incomplete comparisons)
- the case was deterministically sampled for spot-check (~25% of confident cases)

Each audit record targets one (case, objective) pair.

### Human audit workflow

The audit workflow is **blind-then-reveal** to avoid anchoring the human on the LLM judge's reasoning.

**Phase 1: Blind ranking**

The auditor sees only the case dossier — the same information the LLM judge saw:

- case context, branch event, pre-branch history
- objective question, criteria, and decision rule
- candidate decisions with structured action descriptions

The auditor does **not** see the judge's pairwise comparisons, ordering, or confidence.

The auditor performs pairwise comparisons over the candidate set (same shape as the judge) and produces:

- per-pair preference with brief rationale
- per-pair confidence
- a reviewed final candidate ordering
- overall confidence and optional notes

**Phase 2: Reveal and comparison**

After the auditor submits their ranking, the UI reveals:

- the LLM judge's pairwise comparisons and ordering for the same case
- per-pair agreement/disagreement highlights
- the overall ordering comparison

This lets the auditor reflect on divergences without having been influenced by the judge's reasoning.

### Human panel

A separate input path for broader human evaluation. Panel members rank candidates independently for any (case, objective) pair, not just those in the audit queue. Panel members can abstain.

The panel uses the same blind-then-reveal pattern.

### Metrics

**Audit summary**: agreement rate between human auditors and the LLM judge (exact ordering match, pairwise accuracy, Kendall tau).

**Panel summary**: agreement between the model's predicted ranking and human panel rankings (top-1 agreement, pairwise accuracy, Kendall tau).

These metrics close the calibration triangle:

- factual metrics: model vs. observed reality
- judge summary: model vs. LLM judge
- audit summary: LLM judge vs. human
- panel summary: model vs. human

### Human rankings as training data

Human audit and panel submissions are structurally identical to LLM judge outputs: per-pair preferences with rationale, confidence, and a final ordering. This makes them directly usable as training signal:

- **Preference fine-tuning** for the JEPA model's ranking head (pairwise preference loss over candidate actions, grounded in business judgment rather than factual evidence regression alone)
- **Reward model training** for the RL path (human preference pairs over counterfactual actions are the standard input for reward modeling)
- **Judge calibration** (systematic human-judge disagreements on specific objective types can update rubric phrasing or few-shot the judge)

Human audit records are written to `completed_audit_records.json` alongside the judge artifacts. The judge output (`judge_result.json`) stays immutable.

## Artifact layout

After a full benchmark cycle (build, train, judge, audit, eval), the artifact tree looks like:

```text
<benchmark_root>/
  branch_point_benchmark_build.json
  heldout_cases.json
  judged_ranking_template.json
  audit_record_template.json
  judge_result.json              # LLM judge output (immutable)
  audit_queue.json               # auto-generated from judge step
  completed_audit_records.json   # human audit submissions
  dataset/
    train_rows.jsonl
    validation_rows.jsonl
    test_rows.jsonl
    heldout_rows.jsonl
  dossiers/
    <case_id>/
      <objective_pack_id>.md
      <objective_pack_id>.rubric.json
  model_runs/
    <model_id>/
      eval_result.json
      predictions.jsonl
```

## Studio audit UI

The Studio UI exposes the human audit workflow at the `/audit` view and on the Audit tab.

### Queue view

Shows all pending audit records with case title, objective, and judge confidence. Cases flagged as uncertain are highlighted.

### Audit view

For one (case, objective) pair:

1. Displays the full dossier (same text the LLM judge saw)
2. Presents pairwise comparison cards for each candidate pair
3. The auditor picks a preferred candidate per pair, optionally adds rationale and confidence
4. The auditor reviews the final ordering before submit
5. Submitting appends a completed record to `completed_audit_records.json`
6. After submission, reveals the judge's comparisons with agree/disagree highlights

### CLI

```bash
# Serve the audit UI pointed at a benchmark root
vei ui serve \
  --root <benchmark_root> \
  --host 127.0.0.1 \
  --port 3055
```

The audit routes require a benchmark build with a completed judge step. If `judge_result.json` is not present, the audit view shows an empty state.

## Causal Identification

Current counterfactual benchmark numbers are **ranked by rubric**, not causally estimated. The Enron benchmark scores candidate actions by comparing predicted business proxies against later email evidence. This is a useful ranking signal but not a causal claim.

To support a genuine causal claim, VEI would need:

- Quasi-experimental datasets from real policy changes (reviewer additions, recipient removals, routing changes)
- Held-out-company tests where the model is trained on companies A and B, then evaluated on company C
- Matched natural experiments where the same situation played out differently due to an observable policy difference

Until those datasets exist, factual forecasting metrics and counterfactual rankings should be reported in **separate tables** and interpreted accordingly.
