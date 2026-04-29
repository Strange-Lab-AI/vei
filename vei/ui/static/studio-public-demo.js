function publicDemoSourceId() {
  return state.publicDemoStatus?.source?.source_id || "news_americanstories_public_world";
}

function publicDemoTopic() {
  return document.getElementById("public-demo-topic-select")?.value || "all_public_record";
}

function publicDemoAsOf() {
  return document.getElementById("public-demo-asof-input")?.value || "1837-09-06";
}

const PUBLIC_DEMO_STOPWORDS = new Set([
  "about",
  "after",
  "again",
  "against",
  "because",
  "before",
  "between",
  "could",
  "decided",
  "does",
  "from",
  "happens",
  "have",
  "into",
  "over",
  "public",
  "should",
  "take",
  "that",
  "their",
  "there",
  "this",
  "through",
  "what",
  "when",
  "where",
  "will",
  "with",
  "would",
]);

function bindPublicDemoControls() {
  document.getElementById("public-demo-chat-btn")?.addEventListener("click", () => {
    void askPublicDemo();
  });
  document.getElementById("public-demo-score-btn")?.addEventListener("click", () => {
    void scorePublicDemoActions();
  });
  document.getElementById("public-demo-action-input")?.addEventListener("input", () => {
    state.publicDemoScore = null;
    renderPublicDemoScore();
  });
  document.getElementById("public-demo-timeline-range")?.addEventListener("input", (event) => {
    updatePublicDemoTimelinePreview(Number(event.target.value));
  });
  document.getElementById("public-demo-timeline-range")?.addEventListener("change", (event) => {
    const timelineRange = state.publicDemoTimelineRange;
    if (timelineRange) {
      void refreshPublicDemoState(publicDemoDateFromOffset(timelineRange.start, Number(event.target.value)));
    }
  });
  document.getElementById("public-demo-timeline-points")?.addEventListener("click", (event) => {
    const button = event.target.closest("[data-public-demo-timeline-index]");
    if (button) {
      void selectPublicDemoTimelineIndex(Number(button.dataset.publicDemoTimelineIndex));
    }
  });
  document.getElementById("public-demo-asof-input")?.addEventListener("change", () => {
    state.publicDemoScore = null;
    renderPublicDemo();
  });
  document.getElementById("public-demo-topic-select")?.addEventListener("change", () => {
    void refreshPublicDemoState(publicDemoAsOf());
  });
}

function renderPublicDemo() {
  const status = state.publicDemoStatus || {};
  const statusNode = document.getElementById("public-demo-status");
  const titleNode = document.getElementById("public-demo-title");
  const summaryNode = document.getElementById("public-demo-summary");
  const cutoffNode = document.getElementById("public-demo-cutoff");
  const evidenceNode = document.getElementById("public-demo-evidence-list");
  const candidatesNode = document.getElementById("public-demo-candidates-input");

  if (!statusNode || !summaryNode || !cutoffNode || !evidenceNode) {
    return;
  }

  if (!status.available) {
    summaryNode.textContent = "No public history source is configured for this workspace.";
    cutoffNode.textContent = "Public history unavailable";
    statusNode.innerHTML = `
      <div class="whatif-empty">
        ${escapeHtml(status.unavailable_reason || status.error || "Public history is unavailable.")}
      </div>
    `;
    evidenceNode.innerHTML = `<div class="whatif-empty">Load a workspace with a public-demo context snapshot.</div>`;
    updatePublicDemoScoreButton();
    renderPublicDemoChat();
    renderPublicDemoScore();
    return;
  }

  const source = status.source || {};
  if (status.as_of) {
    const asOfInput = document.getElementById("public-demo-asof-input");
    if (asOfInput) {
      asOfInput.value = status.as_of.slice(0, 10);
    }
  }
  if (titleNode) {
    titleNode.textContent = `Ask the public world as of ${(status.as_of || "1837-09-06").slice(0, 10)}`;
  }
  summaryNode.textContent = source.summary || status.state_summary || "";
  cutoffNode.textContent = status.historical_cutoff || "Only pre-cutoff evidence is visible.";
  statusNode.innerHTML = `
    <div class="public-demo-source">
      <strong>${escapeHtml(source.title || "Public History")}</strong>
      <span>${escapeHtml(status.state_summary || "")}</span>
    </div>
  `;
  updatePublicDemoScoreButton();
  renderPublicDemoTimeline(status);
  renderPublicDemoEvidence(status.evidence_events || []);
  const candidates = status.suggested_candidate_actions || [];
  state.publicDemoSuggestedActions = candidates;
  if (candidatesNode && !candidatesNode.value.trim()) {
    candidatesNode.value = candidates
      .map((candidate) => `${candidate.label} | ${candidate.action}`)
      .join("\n");
  }
  renderPublicDemoChat();
  renderPublicDemoScore();
}

function renderPublicDemoTimeline(status) {
  const points = status.timeline_points || [];
  const range = document.getElementById("public-demo-timeline-range");
  const list = document.getElementById("public-demo-timeline-points");
  const summary = document.getElementById("public-demo-known-summary");
  if (!range || !list || !summary) {
    return;
  }
  state.publicDemoTimelinePoints = points;
  if (!points.length) {
    list.innerHTML = "";
    summary.innerHTML = "";
    return;
  }
  const timelineRange = publicDemoTimelineRange(status, points);
  state.publicDemoTimelineRange = timelineRange;
  const selectedIndex = publicDemoSelectedTimelineIndex(points, status.as_of);
  range.max = String(Math.max(0, timelineRange.dayCount));
  range.value = String(publicDemoDayOffset(timelineRange.start, status.as_of));
  updatePublicDemoTimelinePreview(Number(range.value));
  list.innerHTML = points
    .map((point, index) => {
      const selected = index === selectedIndex;
      return `
        <button
          type="button"
          class="public-demo-timeline-point ${selected ? "is-selected" : ""}"
          data-public-demo-timeline-index="${index}"
        >
          <span>${escapeHtml((point.timestamp || "").slice(5, 10))}</span>
          <strong>${escapeHtml(point.label || "Public record")}</strong>
          <small>${escapeHtml(String(point.visible_event_count || 0))} records visible</small>
        </button>
      `;
    })
    .join("");
  summary.innerHTML = `
    <strong>Known by ${escapeHtml((status.as_of || "").slice(0, 10))}</strong>
    <p>${escapeHtml(status.state_summary || "")}</p>
  `;
}

function publicDemoSelectedTimelineIndex(points, asOf) {
  const selectedDay = (asOf || "").slice(0, 10);
  const exactIndex = points.findIndex((point) => (point.timestamp || "").slice(0, 10) === selectedDay);
  if (exactIndex >= 0) {
    return exactIndex;
  }
  const priorIndex = points.findLastIndex((point) => (point.timestamp || "").slice(0, 10) <= selectedDay);
  return priorIndex >= 0 ? priorIndex : 0;
}

function publicDemoTimelineRange(status, points) {
  const source = status.source || {};
  const start = (
    source.first_timestamp ||
    points[0]?.timestamp ||
    status.as_of ||
    "1837-09-06"
  ).slice(0, 10);
  const end = (
    source.last_timestamp ||
    points[points.length - 1]?.timestamp ||
    status.as_of ||
    start
  ).slice(0, 10);
  return {
    start,
    end,
    dayCount: Math.max(0, publicDemoDayOffset(start, end)),
  };
}

function publicDemoDateMs(value) {
  return Date.parse(`${(value || "").slice(0, 10)}T00:00:00Z`);
}

function publicDemoDayOffset(start, value) {
  const startMs = publicDemoDateMs(start);
  const valueMs = publicDemoDateMs(value);
  if (!Number.isFinite(startMs) || !Number.isFinite(valueMs)) {
    return 0;
  }
  return Math.max(0, Math.round((valueMs - startMs) / 86400000));
}

function publicDemoDateFromOffset(start, offset) {
  const startMs = publicDemoDateMs(start);
  if (!Number.isFinite(startMs)) {
    return "1837-09-06";
  }
  return new Date(startMs + Number(offset || 0) * 86400000).toISOString().slice(0, 10);
}

function updatePublicDemoTimelinePreview(index) {
  const output = document.getElementById("public-demo-timeline-date");
  const timelineRange = state.publicDemoTimelineRange;
  if (output && timelineRange) {
    output.textContent = publicDemoDateFromOffset(timelineRange.start, index);
  }
}

async function selectPublicDemoTimelineIndex(index) {
  const points = state.publicDemoTimelinePoints || [];
  const point = points[index];
  if (!point) {
    return;
  }
  await refreshPublicDemoState((point.timestamp || "").slice(0, 10));
}

async function refreshPublicDemoState(asOf) {
  const params = new URLSearchParams({
    as_of: asOf,
    topic: publicDemoTopic(),
  });
  state.publicDemoStatus = await getJson(`/api/workspace/public-demo?${params.toString()}`);
  state.publicDemoChat = [];
  state.publicDemoScore = null;
  renderPublicDemo();
}

function renderPublicDemoEvidence(events) {
  const evidenceNode = document.getElementById("public-demo-evidence-list");
  if (!evidenceNode) {
    return;
  }
  if (!events.length) {
    evidenceNode.innerHTML = `<div class="whatif-empty">No evidence events are visible for this cutoff.</div>`;
    return;
  }
  evidenceNode.innerHTML = events
    .map(
      (event) => `
        <div class="public-demo-evidence-item" data-event-id="${escapeHtml(event.event_id)}">
          <span class="whatif-result-meta">${escapeHtml((event.timestamp || "").slice(0, 10))} · ${escapeHtml(event.surface || "source")}</span>
          <strong>${escapeHtml(event.subject || event.event_id)}</strong>
          <span>${escapeHtml(event.snippet || "")}</span>
        </div>
      `,
    )
    .join("");
}

function renderPublicDemoChat() {
  const chatNode = document.getElementById("public-demo-chat-log");
  if (!chatNode) {
    return;
  }
  const messages = state.publicDemoChat || [];
  if (!messages.length) {
    chatNode.innerHTML = `
      <div class="public-demo-message is-assistant">
        <span>VEI</span>
        <p>I can answer from the public news visible by the cutoff, then help test a public action against the saved historical forecast.</p>
      </div>
    `;
    return;
  }
  chatNode.innerHTML = messages
    .map(
      (message) => `
        <div class="public-demo-message is-${escapeHtml(message.role)}">
          <span>${escapeHtml(message.role === "user" ? "You" : "VEI")}</span>
          <p>${escapeHtml(message.text)}</p>
          ${
            Array.isArray(message.citations) && message.citations.length
              ? `<div class="public-demo-citations">${message.citations
                  .map((event) => `<code>${escapeHtml((event.timestamp || "").slice(0, 10))} ${escapeHtml(event.subject || event.event_id)}</code>`)
                  .join("")}</div>`
              : ""
          }
        </div>
      `,
    )
    .join("");
  chatNode.scrollTop = chatNode.scrollHeight;
}

async function askPublicDemo() {
  const input = document.getElementById("public-demo-message-input");
  const button = document.getElementById("public-demo-chat-btn");
  const message = input?.value?.trim() || "";
  if (!message) {
    return;
  }
  state.publicDemoChat = [...(state.publicDemoChat || []), { role: "user", text: message }];
  state.publicDemoPending = true;
  if (button) {
    button.disabled = true;
    button.textContent = "Asking";
  }
  renderPublicDemoChat();
  try {
    const response = await getJson("/api/workspace/public-demo/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source_id: publicDemoSourceId(),
        as_of: publicDemoAsOf(),
        topic: publicDemoTopic(),
        message,
      }),
    });
    state.publicDemoChat = [
      ...(state.publicDemoChat || []),
      {
        role: "assistant",
        text: response.assistant_text || "",
        citations: response.cited_events || [],
      },
    ];
    if (response.suggested_candidate_actions?.length) {
      const candidatesNode = document.getElementById("public-demo-candidates-input");
      if (candidatesNode && !candidatesNode.value.trim()) {
        candidatesNode.value = response.suggested_candidate_actions
          .map((candidate) => `${candidate.label} | ${candidate.action}`)
          .join("\n");
      }
    }
  } catch (error) {
    state.publicDemoChat = [
      ...(state.publicDemoChat || []),
      { role: "assistant", text: `Public history chat failed: ${error?.message || error}` },
    ];
  } finally {
    state.publicDemoPending = false;
    if (button) {
      button.disabled = false;
      button.textContent = "Ask";
    }
    renderPublicDemoChat();
  }
}

function publicDemoCandidatesFromTextarea() {
  const actionInput = document.getElementById("public-demo-action-input");
  const selectedAction = actionInput?.value?.trim() || "";
  const selectedLabel = "Your scenario";
  const raw = document.getElementById("public-demo-candidates-input")?.value || "";
  const parsedCandidates = raw
    .split("\n")
    .map((line, index) => {
      const trimmed = line.trim();
      if (!trimmed) {
        return null;
      }
      const parts = trimmed.split("|");
      if (parts.length >= 2) {
        return {
          label: parts[0].trim() || `Candidate ${index + 1}`,
          action: parts.slice(1).join("|").trim(),
        };
      }
      return { label: `Candidate ${index + 1}`, action: trimmed };
    })
    .filter(Boolean);
  if (!selectedAction) {
    return [];
  }
  const alternatives = parsedCandidates.filter(
    (candidate) => candidate.action !== selectedAction && candidate.label !== selectedLabel,
  );
  return [{ label: selectedLabel, action: selectedAction }, ...alternatives].slice(0, 4);
}

async function scorePublicDemoActions() {
  const button = document.getElementById("public-demo-score-btn");
  const status = state.publicDemoStatus || {};
  if (status.scoring_available === false) {
    state.publicDemoScore = {
      error:
        status.scoring_unavailable_reason ||
        "Live model inference is unavailable. No ranking was produced.",
      candidates: [],
    };
    renderPublicDemoScore();
    return;
  }
  const decisionTitle =
    document.getElementById("public-demo-decision-input")?.value?.trim() ||
    "Public-world response";
  const candidates = publicDemoCandidatesFromTextarea();
  if (!candidates.length) {
    state.publicDemoScore = { error: "Type a scenario to test.", candidates: [] };
    renderPublicDemoScore();
    return;
  }
  if (button) {
    button.disabled = true;
    button.textContent = "Testing";
  }
  try {
    state.publicDemoScore = await getJson("/api/workspace/public-demo/score", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source_id: publicDemoSourceId(),
        as_of: publicDemoAsOf(),
        topic: publicDemoTopic(),
        decision_title: decisionTitle,
        candidates,
      }),
    });
  } catch (error) {
    state.publicDemoScore = { error: error?.message || String(error), candidates: [] };
  } finally {
    if (button) {
      button.disabled = state.publicDemoStatus?.scoring_available === false;
      button.textContent = "Test action";
    }
    renderPublicDemoScore();
  }
}

function renderPublicDemoScore() {
  const node = document.getElementById("public-demo-score-result");
  if (!node) {
    return;
  }
  const score = state.publicDemoScore;
  if (!score) {
    const status = state.publicDemoStatus || {};
    if (status.scoring_available === false) {
      node.innerHTML = `<div class="whatif-empty">${escapeHtml(status.scoring_unavailable_reason || "Live model inference is unavailable. No ranking was produced.")}</div>`;
      return;
    }
    node.innerHTML = `<div class="whatif-empty">Test an action with the live model checkpoint.</div>`;
    return;
  }
  if (score.error) {
    node.innerHTML = `<div class="whatif-empty">${escapeHtml(score.error)}</div>`;
    return;
  }
  const candidates = score.candidates || [];
  const displayCandidates = publicDemoDisplayCandidates(candidates);
  node.innerHTML = `
    <div class="public-demo-score-head">
      <strong>Action test result</strong>
      <span>${escapeHtml(publicDemoSourceLabel(score.scoring_source || "saved result"))}</span>
    </div>
    <div class="public-demo-score-list">
      ${displayCandidates
        .map((candidate) => {
          const isCustom = candidate.label === "Your scenario";
          const metrics = publicDemoScoreMetrics(candidate);
          const fit = isCustom ? publicDemoEvidenceFit(candidate.action || "") : null;
          return `
            <article class="public-demo-score-item ${isCustom ? "is-custom" : ""} ${candidate.rank === 1 ? "is-lead" : ""}">
              <div class="public-demo-score-rank">${escapeHtml(candidate.rank)}</div>
              <div>
                <strong>${escapeHtml(candidate.label)}</strong>
                ${isCustom ? `<span class="public-demo-rank-note">ranked ${escapeHtml(candidate.rank)} of ${escapeHtml(candidates.length)}</span>` : ""}
                ${fit ? publicDemoEvidenceFitMarkup(fit) : ""}
                <dl class="public-demo-score-metrics">
                  ${metrics
                    .map(
                      (metric) => `
                        <div>
                          <dt>${escapeHtml(metric.label)}</dt>
                          <dd>${escapeHtml(metric.value)}</dd>
                        </div>
                      `
                    )
                    .join("")}
                </dl>
              </div>
            </article>
          `;
        })
        .join("")}
    </div>
    <p class="metric-detail">Evidence-grounded decision support from public records, not causal proof.</p>
  `;
}

function publicDemoDisplayCandidates(candidates) {
  return [...(candidates || [])].sort((left, right) => {
    if (left.label === "Your scenario" && right.label !== "Your scenario") {
      return -1;
    }
    if (right.label === "Your scenario" && left.label !== "Your scenario") {
      return 1;
    }
    return Number(left.rank || 0) - Number(right.rank || 0);
  });
}

function publicDemoScoreMetrics(candidate) {
  const business = candidate.predicted_business_heads || {};
  const future = candidate.predicted_future_heads || {};
  return [
    { label: "score", value: publicDemoFixed(candidate.score) },
    { label: "risk", value: publicDemoFixed(business.enterprise_risk) },
    { label: "trust", value: publicDemoFixed(business.stakeholder_trust) },
    { label: "drag", value: publicDemoFixed(business.execution_drag) },
    { label: "liquidity", value: publicDemoFixed(future.liquidity_stress) },
  ];
}

function publicDemoFixed(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number.toFixed(3) : "--";
}

function publicDemoSourceLabel(value) {
  return String(value || "")
    .replace("live_jepa", "live model inference")
    .replaceAll("_", " ");
}

function publicDemoEvidenceFit(action) {
  const actionTokens = publicDemoKeywordTokens(action);
  if (!actionTokens.length) {
    return {
      label: "not enough text",
      matchedCount: 0,
      tokenCount: 0,
      matchedTokens: [],
    };
  }
  const status = state.publicDemoStatus || {};
  const evidenceText = [
    status.state_summary || "",
    ...(status.evidence_events || []).map(
      (event) => `${event.subject || ""} ${event.snippet || ""}`,
    ),
  ].join(" ");
  const visibleTokens = new Set(publicDemoKeywordTokens(evidenceText));
  const matchedTokens = actionTokens.filter((token) => visibleTokens.has(token));
  const ratio = matchedTokens.length / actionTokens.length;
  let label = "weak";
  if (ratio >= 0.55) {
    label = "strong";
  } else if (ratio >= 0.25) {
    label = "partial";
  }
  return {
    label,
    matchedCount: matchedTokens.length,
    tokenCount: actionTokens.length,
    matchedTokens,
  };
}

function publicDemoEvidenceFitMarkup(fit) {
  const matched = fit.matchedTokens.slice(0, 5).join(", ");
  const matchedText = matched ? ` · visible terms: ${escapeHtml(matched)}` : "";
  const label =
    fit.label === "weak"
      ? "weak evidence fit; treat as out-of-domain"
      : `${fit.label} evidence fit`;
  return `<p class="public-demo-evidence-fit is-${escapeHtml(fit.label)}">${escapeHtml(label)} · ${escapeHtml(fit.matchedCount)} / ${escapeHtml(fit.tokenCount)} scenario terms found in visible evidence${matchedText}</p>`;
}

function publicDemoKeywordTokens(text) {
  return [
    ...new Set(
      String(text || "")
        .toLowerCase()
        .replace(/[^a-z0-9]/g, " ")
        .split(/\s+/)
        .filter((token) => token.length > 3 && !PUBLIC_DEMO_STOPWORDS.has(token)),
    ),
  ].slice(0, 80);
}

function updatePublicDemoScoreButton() {
  const button = document.getElementById("public-demo-score-btn");
  const status = state.publicDemoStatus || {};
  if (!button) {
    return;
  }
  if (status.scoring_available === false) {
    button.disabled = true;
    button.textContent = "Model unavailable";
    button.title =
      status.scoring_unavailable_reason ||
      "Live model checkpoint is not configured.";
    return;
  }
  button.disabled = false;
  button.textContent = "Test action";
  button.title = "Run this scenario through live model inference.";
}
