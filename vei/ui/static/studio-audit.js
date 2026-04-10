// ---------------------------------------------------------------------------
// studio-audit.js — Human audit UI for LLM judge benchmark rankings
// Blind-then-reveal: auditor ranks candidates without seeing judge reasoning,
// then sees the comparison after submission.
// ---------------------------------------------------------------------------

let auditQueue = null;
let auditActiveItem = null;
let auditPairwise = {};  // key: "left|right" -> preferred_candidate_id
let auditPairwiseRationale = {};  // key: "left|right" -> rationale string
let auditPairwiseConfidence = {};  // key: "left|right" -> confidence float
let auditFinalOrder = [];
let auditFinalOrderTouched = false;
let auditReviewerId = "";
let auditOverallConfidence = "0.7";
let auditNotes = "";
let auditRevealData = null;

async function loadAuditQueue() {
  auditActiveItem = null;
  auditRevealData = null;
  _resetPairwiseState();
  try {
    auditQueue = await getJson("/api/workspace/whatif/audit");
  } catch (error) {
    auditQueue = { items: [], error: error?.message || String(error) };
  }
  renderAuditStudio();
}

function _resetPairwiseState() {
  auditPairwise = {};
  auditPairwiseRationale = {};
  auditPairwiseConfidence = {};
  auditFinalOrder = [];
  auditFinalOrderTouched = false;
  auditReviewerId = "";
  auditOverallConfidence = "0.7";
  auditNotes = "";
}

function _candidatePairs(candidates) {
  const pairs = [];
  for (let i = 0; i < candidates.length; i++) {
    for (let j = i + 1; j < candidates.length; j++) {
      pairs.push([candidates[i], candidates[j]]);
    }
  }
  return pairs;
}

function _aggregatePairwiseRank(candidates, pairwise) {
  const wins = {};
  for (const c of candidates) wins[c.candidate_id] = 0;
  for (const [key, preferred] of Object.entries(pairwise)) {
    if (preferred && wins[preferred] !== undefined) wins[preferred]++;
  }
  return [...candidates].sort(
    (a, b) => (wins[b.candidate_id] || 0) - (wins[a.candidate_id] || 0)
  );
}

function _suggestedFinalOrder(candidates) {
  return _aggregatePairwiseRank(candidates, auditPairwise).map(
    (candidate) => candidate.candidate_id
  );
}

function _syncFinalOrder(candidates) {
  const candidateIds = candidates.map((candidate) => candidate.candidate_id);
  const suggestedOrder = _suggestedFinalOrder(candidates);
  if (!auditFinalOrderTouched || auditFinalOrder.length !== candidateIds.length) {
    auditFinalOrder = suggestedOrder;
    return auditFinalOrder;
  }

  const normalized = auditFinalOrder.filter((candidateId) =>
    candidateIds.includes(candidateId)
  );
  while (normalized.length < candidateIds.length) {
    normalized.push("");
  }
  auditFinalOrder = normalized.slice(0, candidateIds.length);
  return auditFinalOrder;
}

function _hasValidFinalOrder(candidates) {
  const candidateIds = candidates.map((candidate) => candidate.candidate_id);
  return (
    auditFinalOrder.length === candidateIds.length
    && auditFinalOrder.every((candidateId) => candidateIds.includes(candidateId))
    && new Set(auditFinalOrder).size === candidateIds.length
  );
}

function renderAuditStudio() {
  const queueNode = document.getElementById("audit-queue");
  const workspaceNode = document.getElementById("audit-workspace");
  const revealNode = document.getElementById("audit-reveal");
  if (!queueNode || !workspaceNode || !revealNode) return;

  // ---- Queue view ----
  if (!auditQueue || auditQueue.error) {
    queueNode.innerHTML = `
      <div class="whatif-empty">
        <strong>${auditQueue?.error ? "Could not load audit queue" : "Loading audit queue..."}</strong>
        ${auditQueue?.error ? `<span>${escapeHtml(auditQueue.error)}</span>` : ""}
      </div>
    `;
    workspaceNode.innerHTML = "";
    revealNode.innerHTML = "";
    return;
  }

  const items = auditQueue.items || [];
  if (!items.length) {
    queueNode.innerHTML = `<div class="whatif-empty">No audit items found. Run <code>vei whatif benchmark judge</code> to generate the audit queue.</div>`;
    workspaceNode.innerHTML = "";
    revealNode.innerHTML = "";
    return;
  }

  const pending = items.filter((item) => item.status === "pending");
  const completed = items.filter((item) => item.status === "completed");

  queueNode.innerHTML = `
    <div class="audit-queue-header">
      <span><strong>${items.length}</strong> audit items: <strong>${pending.length}</strong> pending, <strong>${completed.length}</strong> completed</span>
    </div>
    <div class="audit-queue-list">
      ${items
        .map(
          (item, index) => `
        <button type="button" class="audit-queue-item ${item.status === "completed" ? "is-completed" : ""} ${item.judge_uncertainty_flag ? "is-uncertain" : ""}"
                data-audit-index="${index}">
          <span class="audit-queue-title">${escapeHtml(item.case_title || item.case_id)}</span>
          <span class="audit-queue-meta">${escapeHtml(item.objective_pack_id.replace(/_/g, " "))}</span>
          <span class="audit-queue-status">${item.status === "completed" ? "done" : item.judge_uncertainty_flag ? "uncertain" : "pending"}</span>
        </button>
      `
        )
        .join("")}
    </div>
  `;

  queueNode.querySelectorAll("[data-audit-index]").forEach((node) => {
    node.addEventListener("click", () => {
      const index = Number(node.getAttribute("data-audit-index"));
      const item = items[index];
      if (!item) return;
      auditActiveItem = item;
      auditRevealData = null;
      _resetPairwiseState();
      renderAuditStudio();
    });
  });

  // ---- Workspace: blind ranking ----
  if (!auditActiveItem) {
    workspaceNode.innerHTML = `<div class="whatif-empty">Select an audit item from the queue above.</div>`;
    revealNode.innerHTML = "";
    return;
  }

  if (auditRevealData) {
    workspaceNode.innerHTML = "";
    _renderReveal(revealNode);
    return;
  }

  const item = auditActiveItem;
  const candidates = item.candidates || [];
  const pairs = _candidatePairs(candidates);
  const allPairsAnswered = pairs.every(
    ([left, right]) => auditPairwise[`${left.candidate_id}|${right.candidate_id}`]
  );
  if (allPairsAnswered) {
    _syncFinalOrder(candidates);
  }
  const finalOrderValid = allPairsAnswered && _hasValidFinalOrder(candidates);

  workspaceNode.innerHTML = `
    <div class="audit-dossier">
      <div class="audit-dossier-header">
        <strong>${escapeHtml(item.case_title || item.case_id)}</strong>
        <span>${escapeHtml(item.objective_pack_id.replace(/_/g, " "))}</span>
      </div>
      <pre class="audit-dossier-text">${escapeHtml(item.dossier_text || "No dossier available.")}</pre>
    </div>
    <div class="audit-pairs">
      <h3>Pairwise Comparisons</h3>
      <p class="metric-detail">For each pair, choose the candidate that better achieves the objective. Add a brief rationale if you can.</p>
      ${pairs
        .map(([left, right]) => {
          const key = `${left.candidate_id}|${right.candidate_id}`;
          const selected = auditPairwise[key] || "";
          return `
          <div class="audit-pair-card" data-pair-key="${escapeHtml(key)}">
            <div class="audit-pair-options">
              <button type="button"
                      class="audit-pair-btn ${selected === left.candidate_id ? "is-selected" : ""}"
                      data-pair-key="${escapeHtml(key)}"
                      data-pick="${escapeHtml(left.candidate_id)}">
                <strong>${escapeHtml(left.label)}</strong>
                <span>${escapeHtml(left.prompt)}</span>
              </button>
              <span class="audit-pair-vs">vs</span>
              <button type="button"
                      class="audit-pair-btn ${selected === right.candidate_id ? "is-selected" : ""}"
                      data-pair-key="${escapeHtml(key)}"
                      data-pick="${escapeHtml(right.candidate_id)}">
                <strong>${escapeHtml(right.label)}</strong>
                <span>${escapeHtml(right.prompt)}</span>
              </button>
            </div>
            <label class="audit-rationale-field">
              <span>Rationale (optional)</span>
              <input type="text" class="audit-rationale-input"
                     data-pair-key="${escapeHtml(key)}"
                     value="${escapeHtml(auditPairwiseRationale[key] || "")}"
                     placeholder="Brief reason for your choice" />
            </label>
            <label class="audit-confidence-field">
              <span>Confidence (0-1)</span>
              <input type="number" class="audit-confidence-input"
                     data-pair-key="${escapeHtml(key)}"
                     min="0" max="1" step="0.05"
                     value="${escapeHtml(auditPairwiseConfidence[key] ?? "")}"
                     placeholder="0.70" />
            </label>
          </div>
        `;
        })
        .join("")}
    </div>
    ${allPairsAnswered ? `
      <div class="audit-final-order">
        <h3>Final ranking</h3>
        <p class="metric-detail">Review the final ordering before you submit. You can adjust it directly if your pairwise choices left a tie.</p>
        <div class="audit-final-order-list">
          ${candidates
            .map((candidate, index) => `
              <label class="audit-final-order-row">
                <span>Rank ${index + 1}</span>
                <select class="audit-final-order-select" data-final-rank="${index}">
                  ${candidates
                    .map((option) => `
                      <option value="${escapeHtml(option.candidate_id)}" ${auditFinalOrder[index] === option.candidate_id ? "selected" : ""}>
                        ${escapeHtml(option.label)}
                      </option>
                    `)
                    .join("")}
                </select>
              </label>
            `)
            .join("")}
        </div>
      </div>
    ` : ""}
    <div class="audit-submit-area">
      <label class="whatif-field">
        <span>Your name / ID (optional)</span>
        <input type="text" id="audit-reviewer-id" value="${escapeHtml(auditReviewerId)}" placeholder="reviewer" />
      </label>
      <label class="whatif-field">
        <span>Overall confidence (0-1)</span>
        <input type="number" id="audit-confidence" min="0" max="1" step="0.05" value="${escapeHtml(auditOverallConfidence)}" />
      </label>
      <label class="whatif-field whatif-field-wide">
        <span>Notes (optional)</span>
        <input type="text" id="audit-notes" value="${escapeHtml(auditNotes)}" placeholder="Any additional notes" />
      </label>
      <div class="whatif-actions">
        <button type="button" id="audit-submit-btn" ${finalOrderValid ? "" : "disabled"}>
          Submit ranking
        </button>
      </div>
      ${!allPairsAnswered ? `<p class="metric-detail">Complete all pairwise comparisons to unlock the final ranking.</p>` : ""}
      ${allPairsAnswered && !finalOrderValid ? `<p class="metric-detail">Choose each candidate once in the final ranking.</p>` : ""}
    </div>
  `;

  // Wire up pair selection
  workspaceNode.querySelectorAll(".audit-pair-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const key = btn.getAttribute("data-pair-key");
      const pick = btn.getAttribute("data-pick");
      auditPairwise[key] = pick;
      renderAuditStudio();
    });
  });

  // Wire up rationale inputs
  workspaceNode.querySelectorAll(".audit-rationale-input").forEach((input) => {
    input.addEventListener("input", () => {
      const key = input.getAttribute("data-pair-key");
      auditPairwiseRationale[key] = input.value;
    });
  });

  workspaceNode.querySelectorAll(".audit-confidence-input").forEach((input) => {
    input.addEventListener("input", () => {
      const key = input.getAttribute("data-pair-key");
      const value = input.value.trim();
      auditPairwiseConfidence[key] = value === "" ? "" : Number(value);
    });
  });

  workspaceNode.querySelectorAll(".audit-final-order-select").forEach((select) => {
    select.addEventListener("change", () => {
      const rank = Number(select.getAttribute("data-final-rank"));
      const nextOrder = [...auditFinalOrder];
      nextOrder[rank] = select.value;
      auditFinalOrder = nextOrder;
      auditFinalOrderTouched = true;
      renderAuditStudio();
    });
  });

  document.getElementById("audit-reviewer-id")?.addEventListener("input", (event) => {
    auditReviewerId = event.target.value;
  });
  document.getElementById("audit-confidence")?.addEventListener("input", (event) => {
    auditOverallConfidence = event.target.value;
  });
  document.getElementById("audit-notes")?.addEventListener("input", (event) => {
    auditNotes = event.target.value;
  });

  // Wire up submit
  const submitBtn = document.getElementById("audit-submit-btn");
  if (submitBtn && finalOrderValid) {
    submitBtn.addEventListener("click", () => submitAudit());
  }

  revealNode.innerHTML = "";
}

async function submitAudit() {
  const item = auditActiveItem;
  if (!item) return;

  const candidates = item.candidates || [];
  if (!_hasValidFinalOrder(candidates)) {
    auditRevealData = { error: "Choose each candidate once in the final ranking." };
    renderAuditStudio();
    return;
  }
  const pairs = _candidatePairs(candidates);

  const pairwiseComparisons = pairs.map(([left, right]) => {
    const key = `${left.candidate_id}|${right.candidate_id}`;
    const pairwiseConfidence = auditPairwiseConfidence[key];
    return {
      left_candidate_id: left.candidate_id,
      right_candidate_id: right.candidate_id,
      preferred_candidate_id: auditPairwise[key] || "",
      confidence:
        pairwiseConfidence === "" || pairwiseConfidence === undefined
          ? null
          : Number(pairwiseConfidence),
      evidence_references: [],
      rationale: auditPairwiseRationale[key] || "",
    };
  });

  const orderedIds = [...auditFinalOrder];

  const reviewerId = auditReviewerId.trim();
  const confidenceValue = auditOverallConfidence.trim();
  const confidence =
    confidenceValue === "" || !Number.isFinite(Number(confidenceValue))
      ? null
      : Number(confidenceValue);
  const notes = auditNotes.trim();

  const submitBtn = document.getElementById("audit-submit-btn");
  if (submitBtn) {
    submitBtn.disabled = true;
    submitBtn.textContent = "Submitting...";
  }

  try {
    const result = await getJson(
      `/api/workspace/whatif/audit/${encodeURIComponent(item.case_id)}/${encodeURIComponent(item.objective_pack_id)}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          reviewer_id: reviewerId,
          ordered_candidate_ids: orderedIds,
          pairwise_comparisons: pairwiseComparisons,
          confidence,
          notes,
        }),
      }
    );
    auditRevealData = result;

    // Update the item status in the local queue
    if (auditQueue?.items) {
      const idx = auditQueue.items.findIndex(
        (i) =>
          i.case_id === item.case_id &&
          i.objective_pack_id === item.objective_pack_id
      );
      if (idx >= 0) auditQueue.items[idx].status = "completed";
    }
  } catch (error) {
    auditRevealData = { error: error?.message || String(error) };
  }
  renderAuditStudio();
}

function _renderReveal(revealNode) {
  if (!auditRevealData || !auditActiveItem) {
    revealNode.innerHTML = "";
    return;
  }

  if (auditRevealData.error) {
    revealNode.innerHTML = `
      <div class="whatif-empty">
        <strong>Submission failed</strong>
        <span>${escapeHtml(auditRevealData.error)}</span>
      </div>
    `;
    return;
  }

  const submitted = auditRevealData.submitted || {};
  const judgeRanking = auditRevealData.judge_ranking;
  const agreement = auditRevealData.agreement_with_judge;
  const item = auditActiveItem;
  const candidates = item.candidates || [];
  const candidateById = {};
  for (const c of candidates) candidateById[c.candidate_id] = c;

  const humanOrder = submitted.ordered_candidate_ids || [];
  const judgeOrder = judgeRanking?.ordered_candidate_ids || [];

  let comparisonHtml = "";
  if (judgeRanking) {
    const judgePairwise = judgeRanking.pairwise_comparisons || [];
    const humanPairwise = submitted.pairwise_comparisons || [];
    const humanPairMap = {};
    for (const comp of humanPairwise) {
      const pairKey = [comp.left_candidate_id, comp.right_candidate_id]
        .sort()
        .join("|");
      humanPairMap[pairKey] = comp.preferred_candidate_id;
    }

    comparisonHtml = `
      <div class="audit-reveal-pairwise">
        <h3>Pairwise comparison</h3>
        ${judgePairwise
          .map((jp) => {
            const key = [jp.left_candidate_id, jp.right_candidate_id]
              .sort()
              .join("|");
            const humanPick = humanPairMap[key] || "";
            const judgePick = jp.preferred_candidate_id || "";
            const agree = humanPick === judgePick;
            const leftLabel =
              candidateById[jp.left_candidate_id]?.label ||
              jp.left_candidate_id;
            const rightLabel =
              candidateById[jp.right_candidate_id]?.label ||
              jp.right_candidate_id;
            const humanLabel =
              candidateById[humanPick]?.label || humanPick || "?";
            const judgeLabel =
              candidateById[judgePick]?.label || judgePick || "?";
            return `
              <div class="audit-reveal-pair ${agree ? "is-agree" : "is-disagree"}">
                <span class="audit-reveal-pair-matchup">${escapeHtml(leftLabel)} vs ${escapeHtml(rightLabel)}</span>
                <span>You: <strong>${escapeHtml(humanLabel)}</strong></span>
                <span>Judge: <strong>${escapeHtml(judgeLabel)}</strong></span>
                <span class="audit-reveal-pair-verdict">${agree ? "agree" : "disagree"}</span>
                ${jp.rationale ? `<span class="audit-reveal-pair-rationale">Judge rationale: ${escapeHtml(jp.rationale)}</span>` : ""}
              </div>
            `;
          })
          .join("")}
      </div>
    `;
  }

  revealNode.innerHTML = `
    <div class="audit-reveal-header">
      <h3>Results</h3>
      <div class="audit-reveal-agreement ${agreement === true ? "is-agree" : agreement === false ? "is-disagree" : ""}">
        ${agreement === true ? "Full agreement with judge" : agreement === false ? "Ordering differs from judge" : "No judge ranking available"}
      </div>
    </div>
    <div class="audit-reveal-rankings">
      <div class="audit-reveal-column">
        <h4>Your ranking</h4>
        <ol>
          ${humanOrder.map((id) => `<li>${escapeHtml(candidateById[id]?.label || id)}</li>`).join("")}
        </ol>
      </div>
      ${
        judgeOrder.length
          ? `
        <div class="audit-reveal-column">
          <h4>Judge ranking</h4>
          <ol>
            ${judgeOrder.map((id) => `<li>${escapeHtml(candidateById[id]?.label || id)}</li>`).join("")}
          </ol>
        </div>
      `
          : ""
      }
    </div>
    ${comparisonHtml}
    <div class="whatif-actions" style="margin-top: 1rem">
      <button type="button" id="audit-back-btn">Back to queue</button>
    </div>
  `;

  const backBtn = document.getElementById("audit-back-btn");
  if (backBtn) {
    backBtn.addEventListener("click", () => {
      auditActiveItem = null;
      auditRevealData = null;
      _resetPairwiseState();
      renderAuditStudio();
    });
  }
}

window.renderAuditStudio = renderAuditStudio;
window.loadAuditQueue = loadAuditQueue;
