function whatIfSelectedEventPayload() {
  return state.whatIfSelectedEvent?.event || null;
}

function whatIfRecipients(event) {
  const recipients = Array.isArray(event?.to_recipients)
    ? event.to_recipients.filter(Boolean)
    : [];
  if (recipients.length) return recipients.join(", ");
  return event?.target_id || "";
}

function whatIfDefaultLabel(event) {
  const subject = event?.subject || "counterfactual";
  return `${subject} alternate path`;
}

function whatIfDefaultPrompt(event) {
  if (!event) return "";
  const actor = event.actor_id || "someone";
  const subject = event.subject || "this thread";
  return `What if ${actor} had handled "${subject}" differently at this point?`;
}

function whatIfDefaultCandidates(event) {
  if (!event) return "";
  const subject = event.subject || "this thread";
  return [
    `Keep "${subject}" internal, add legal review, and pause the outside send.`,
    `Send a short status update immediately, clarify the owner, and keep the next step moving.`,
    `Acknowledge the issue, ask for confirmation, and limit follow-up to one clear owner.`,
  ].join("\n");
}

function whatIfObjectivePacks() {
  return Array.isArray(state.whatIfStatus?.objective_packs)
    ? state.whatIfStatus.objective_packs
    : [];
}

function renderWhatIfStudio() {
  const statusNode = document.getElementById("whatif-status");
  const resultsNode = document.getElementById("whatif-results");
  const selectionNode = document.getElementById("whatif-selection");
  const openNode = document.getElementById("whatif-open-result");
  const resultNode = document.getElementById("whatif-experiment-result");
  const objectiveSelect = document.getElementById("whatif-objective-select");
  if (!statusNode || !resultsNode || !selectionNode || !openNode || !resultNode) {
    return;
  }

  const status = state.whatIfStatus || { available: false };
  if (!status.available) {
    statusNode.innerHTML = `
      <div class="whatif-empty">
        <strong>Enron archive not configured for this workspace.</strong>
        <span>Set <code>VEI_WHATIF_ROSETTA_DIR</code> or keep the sibling Rosetta repo next to this workspace.</span>
      </div>
    `;
    resultsNode.innerHTML = "";
    selectionNode.innerHTML = "";
    openNode.innerHTML = "";
    resultNode.innerHTML = "";
    return;
  }

  statusNode.innerHTML = `
    <div class="whatif-status-pill">
      <strong>Archive ready</strong>
      <span>${escapeHtml(status.rosetta_dir || "Enron Rosetta source")}</span>
    </div>
  `;
  if (objectiveSelect) {
    const packs = whatIfObjectivePacks();
    const currentValue = objectiveSelect.value || "contain_exposure";
    objectiveSelect.innerHTML = packs.length
      ? packs
          .map(
            (pack) =>
              `<option value="${escapeHtml(pack.pack_id)}">${escapeHtml(pack.title)}</option>`
          )
          .join("")
      : `<option value="contain_exposure">Contain Exposure</option>`;
    objectiveSelect.value = packs.some((pack) => pack.pack_id === currentValue)
      ? currentValue
      : "contain_exposure";
  }

  const searchResult = state.whatIfSearchResult;
  if (searchResult?.error) {
    resultsNode.innerHTML = `
      <div class="whatif-empty">
        <strong>Search failed.</strong>
        <span>${escapeHtml(searchResult.error)}</span>
      </div>
    `;
  } else if (
    !searchResult ||
    !Array.isArray(searchResult.matches) ||
    !searchResult.matches.length
  ) {
    resultsNode.innerHTML = `<div class="whatif-empty">Search for a person, thread, or decision moment to begin.</div>`;
  } else {
    resultsNode.innerHTML = `
      <div class="whatif-result-list">
        ${searchResult.matches.map((match, index) => {
          const event = match.event || {};
          const reasons = Array.isArray(match.reason_labels) ? match.reason_labels.join(", ") : "";
          return `
            <button type="button" class="whatif-result-item" data-whatif-index="${index}">
              <span class="whatif-result-title">${escapeHtml(event.subject || event.thread_id || event.event_id)}</span>
              <span class="whatif-result-meta">${escapeHtml(event.timestamp || "")} · ${escapeHtml(event.actor_id || "")}</span>
              <span class="whatif-result-meta">${escapeHtml(whatIfRecipients(event) || "")}</span>
              <span class="whatif-result-caption">${escapeHtml(reasons || "historical match")}</span>
            </button>
          `;
        }).join("")}
      </div>
    `;
    resultsNode.querySelectorAll("[data-whatif-index]").forEach((node) => {
      node.addEventListener("click", () => {
        const index = Number(node.getAttribute("data-whatif-index"));
        const match = state.whatIfSearchResult?.matches?.[index];
        if (!match) return;
        state.whatIfSelectedEvent = match;
        state.whatIfOpenResult = null;
        state.whatIfExperimentResult = null;
        state.whatIfRankedResult = null;
        const event = match.event || {};
        const labelInput = document.getElementById("whatif-label-input");
        const promptInput = document.getElementById("whatif-prompt-input");
        const candidatesInput = document.getElementById("whatif-candidates-input");
        if (labelInput && !labelInput.value.trim()) {
          labelInput.value = whatIfDefaultLabel(event);
        }
        if (promptInput && !promptInput.value.trim()) {
          promptInput.value = whatIfDefaultPrompt(event);
        }
        if (candidatesInput && !candidatesInput.value.trim()) {
          candidatesInput.value = whatIfDefaultCandidates(event);
        }
        renderWhatIfStudio();
      });
    });
  }

  const selected = whatIfSelectedEventPayload();
  if (!selected) {
    selectionNode.innerHTML = `<div class="whatif-empty">Choose one of the matching events to inspect the exact branch point.</div>`;
  } else {
    selectionNode.innerHTML = `
      <div class="whatif-selection-card">
        <div class="whatif-selection-head">
          <strong>${escapeHtml(selected.subject || selected.thread_id || selected.event_id)}</strong>
          <span>${escapeHtml(selected.timestamp || "")}</span>
        </div>
        <div class="whatif-selection-grid">
          <span><strong>Actor</strong> ${escapeHtml(selected.actor_id || "")}</span>
          <span><strong>Recipient</strong> ${escapeHtml(whatIfRecipients(selected) || "")}</span>
          <span><strong>Thread</strong> ${escapeHtml(selected.thread_id || "")}</span>
          <span><strong>Event</strong> ${escapeHtml(selected.event_type || "")}</span>
        </div>
        ${selected.snippet ? `<pre class="whatif-snippet">${escapeHtml(selected.snippet)}</pre>` : `<p class="metric-detail">No body excerpt available for this event.</p>`}
      </div>
    `;
  }

  const openResult = state.whatIfOpenResult;
  if (openResult?.error) {
    openNode.innerHTML = `
      <div class="whatif-open-card">
        <strong>Could not materialize the baseline</strong>
        <span>${escapeHtml(openResult.error)}</span>
      </div>
    `;
  } else if (!openResult) {
    openNode.innerHTML = "";
  } else {
    const materialization = openResult.materialization || {};
    openNode.innerHTML = `
      <div class="whatif-open-card">
        <strong>Baseline materialized</strong>
        <span>${escapeHtml(openResult.episode_root || "")}</span>
        <span>${escapeHtml(materialization.history_message_count || 0)} prior messages · ${escapeHtml(materialization.future_event_count || 0)} future events</span>
      </div>
    `;
  }

  const ranked = state.whatIfRankedResult;
  if (ranked) {
    if (ranked.error) {
      resultNode.innerHTML = `
        <div class="whatif-summary-card">
          <p class="eyebrow">Ranked what-if failed</p>
          <strong>${escapeHtml(ranked.label || "Ranked historical what-if")}</strong>
          <span>${escapeHtml(ranked.error)}</span>
        </div>
      `;
      return;
    }
    const objective = ranked.objective_pack || {};
    const candidates = Array.isArray(ranked.candidates) ? ranked.candidates : [];
    resultNode.innerHTML = `
      <div class="whatif-summary-card is-recommended">
        <p class="eyebrow">Recommended option</p>
        <strong>${escapeHtml(ranked.recommended_candidate_label || "No recommendation")}</strong>
        <span>${escapeHtml(objective.title || "Objective")}</span>
      </div>
      <div class="whatif-ranked-list">
        ${candidates
          .map((candidate) => {
            const score = candidate.outcome_score?.overall_score ?? "n/a";
            const shadowScore = candidate.shadow?.outcome_score?.overall_score;
            return `
              <div class="whatif-summary-card ${candidate.rank === 1 ? "is-recommended" : ""}">
                <p class="eyebrow">Rank ${escapeHtml(candidate.rank || "n/a")}</p>
                <strong>${escapeHtml(candidate.intervention?.label || "")}</strong>
                <span>${escapeHtml(candidate.reason || "")}</span>
                <span>Score ${escapeHtml(score)} across ${escapeHtml(candidate.rollout_count || 0)} rollouts</span>
                <span>Exposure ${escapeHtml(candidate.average_outcome_signals?.exposure_risk ?? "n/a")} · Delay ${escapeHtml(candidate.average_outcome_signals?.delay_risk ?? "n/a")} · Relationship ${escapeHtml(candidate.average_outcome_signals?.relationship_protection ?? "n/a")}</span>
                ${shadowScore != null ? `<span>Shadow ${escapeHtml(candidate.shadow?.backend || "forecast")} ${escapeHtml(shadowScore)}</span>` : ""}
              </div>
            `;
          })
          .join("")}
      </div>
      <div class="whatif-artifacts">
        <span><strong>Saved result</strong> ${escapeHtml(ranked.artifacts?.result_json_path || "")}</span>
        <span><strong>Saved summary</strong> ${escapeHtml(ranked.artifacts?.overview_markdown_path || "")}</span>
      </div>
    `;
    return;
  }

  const experiment = state.whatIfExperimentResult;
  if (!experiment) {
    resultNode.innerHTML = "";
    return;
  }
  if (experiment.error) {
    resultNode.innerHTML = `
      <div class="whatif-summary-card">
        <p class="eyebrow">Counterfactual run failed</p>
        <strong>${escapeHtml(experiment.label || "Historical what-if")}</strong>
        <span>${escapeHtml(experiment.error)}</span>
      </div>
    `;
    return;
  }
  const branch = experiment.materialization?.branch_event || {};
  const baseline = experiment.baseline || {};
  const llm = experiment.llm_result || null;
  const forecast = experiment.forecast_result || null;
  const branchRecipients = whatIfRecipients(branch);
  const baselineExternalCount = baseline.forecast?.future_external_event_count ?? 0;
  resultNode.innerHTML = `
    <div class="whatif-summary-grid">
      <div class="whatif-summary-card">
        <p class="eyebrow">Historical baseline</p>
        <strong>${escapeHtml(branch.subject || branch.thread_id || branch.event_id || experiment.label)}</strong>
        <span>${escapeHtml(branch.actor_id || "")} -> ${escapeHtml(branchRecipients || "")}</span>
        <span>${escapeHtml(baseline.delivered_event_count || 0)} delivered baseline events</span>
        <span>${escapeHtml(baselineExternalCount)} outside-addressed events in the historical path</span>
      </div>
      <div class="whatif-summary-card">
        <p class="eyebrow">LLM alternate path</p>
        <strong>${escapeHtml(llm?.status || "not run")}</strong>
        <span>${escapeHtml(llm?.summary || "No LLM branch for this run.")}</span>
        <span>${escapeHtml(llm?.delivered_event_count || 0)} alternate messages delivered</span>
      </div>
      <div class="whatif-summary-card">
        <p class="eyebrow">Learned forecast</p>
        <strong>${escapeHtml(forecast?.backend || "not run")}</strong>
        <span>${escapeHtml(forecast?.summary || "No forecast for this run.")}</span>
        <span>Risk ${escapeHtml(forecast?.baseline?.risk_score ?? "n/a")} -> ${escapeHtml(forecast?.predicted?.risk_score ?? "n/a")}</span>
      </div>
    </div>
    <div class="whatif-artifacts">
      <span><strong>Saved result</strong> ${escapeHtml(experiment.artifacts?.result_json_path || "")}</span>
      <span><strong>Saved summary</strong> ${escapeHtml(experiment.artifacts?.overview_markdown_path || "")}</span>
    </div>
  `;
}

async function searchWhatIfEvents() {
  const query = document.getElementById("whatif-query-input")?.value?.trim() || "";
  const limit = Number(document.getElementById("whatif-limit-input")?.value || 6);
  if (!query) {
    state.whatIfSearchResult = null;
    state.whatIfSelectedEvent = null;
    renderWhatIfStudio();
    return;
  }
  const statusNode = document.getElementById("whatif-status");
  if (statusNode) {
    statusNode.innerHTML = `<div class="whatif-status-pill"><strong>Searching</strong><span>${escapeHtml(query)}</span></div>`;
  }
  try {
    const result = await getJson("/api/workspace/whatif/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source: "enron",
        query,
        limit,
      }),
    });
    state.whatIfSearchResult = result;
    state.whatIfSelectedEvent = null;
    state.whatIfOpenResult = null;
    state.whatIfExperimentResult = null;
    state.whatIfRankedResult = null;
  } catch (error) {
    state.whatIfSearchResult = {
      matches: [],
      error: error?.message || String(error),
    };
  }
  renderWhatIfStudio();
}

async function materializeWhatIfEpisode() {
  const event = whatIfSelectedEventPayload();
  if (!event) return;
  const label = document.getElementById("whatif-label-input")?.value?.trim() || whatIfDefaultLabel(event);
  try {
    state.whatIfOpenResult = await getJson("/api/workspace/whatif/open", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source: "enron",
        event_id: event.event_id,
        thread_id: event.thread_id,
        label,
      }),
    });
  } catch (error) {
    state.whatIfOpenResult = {
      materialization: null,
      episode_root: "",
      error: error?.message || String(error),
    };
  }
  renderWhatIfStudio();
}

async function runWhatIfExperimentFromUI() {
  const event = whatIfSelectedEventPayload();
  if (!event) return;
  const label = document.getElementById("whatif-label-input")?.value?.trim() || whatIfDefaultLabel(event);
  const prompt = document.getElementById("whatif-prompt-input")?.value?.trim() || "";
  if (!prompt) return;
  const statusNode = document.getElementById("whatif-status");
  if (statusNode) {
    statusNode.innerHTML = `<div class="whatif-status-pill"><strong>Running</strong><span>${escapeHtml(label)}</span></div>`;
  }
  try {
    state.whatIfRankedResult = null;
    state.whatIfExperimentResult = await getJson("/api/workspace/whatif/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source: "enron",
        event_id: event.event_id,
        thread_id: event.thread_id,
        label,
        prompt,
        mode: "both",
      }),
    });
  } catch (error) {
    state.whatIfExperimentResult = {
      label,
      artifacts: {},
      error: error?.message || String(error),
    };
  }
  renderWhatIfStudio();
  document.getElementById("whatif-experiment-result")?.scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
}

async function runRankedWhatIfFromUI() {
  const event = whatIfSelectedEventPayload();
  if (!event) return;
  const label = document.getElementById("whatif-label-input")?.value?.trim() || whatIfDefaultLabel(event);
  const objectivePackId = document.getElementById("whatif-objective-select")?.value || "contain_exposure";
  const rolloutCount = Number(document.getElementById("whatif-rollout-count-input")?.value || 4);
  const rawCandidates = document.getElementById("whatif-candidates-input")?.value || "";
  const candidates = rawCandidates
    .split("\n")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((prompt, index) => ({
      label: `Option ${index + 1}`,
      prompt,
    }));
  if (!candidates.length) return;
  const statusNode = document.getElementById("whatif-status");
  if (statusNode) {
    statusNode.innerHTML = `<div class="whatif-status-pill"><strong>Ranking</strong><span>${escapeHtml(label)}</span></div>`;
  }
  try {
    state.whatIfExperimentResult = null;
    state.whatIfRankedResult = await getJson("/api/workspace/whatif/rank", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source: "enron",
        event_id: event.event_id,
        thread_id: event.thread_id,
        label,
        objective_pack_id: objectivePackId,
        rollout_count: rolloutCount,
        candidates,
      }),
    });
  } catch (error) {
    state.whatIfRankedResult = {
      label,
      artifacts: {},
      error: error?.message || String(error),
    };
  }
  renderWhatIfStudio();
  document.getElementById("whatif-experiment-result")?.scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
}

window.renderWhatIfStudio = renderWhatIfStudio;
window.searchWhatIfEvents = searchWhatIfEvents;
window.materializeWhatIfEpisode = materializeWhatIfEpisode;
window.runWhatIfExperimentFromUI = runWhatIfExperimentFromUI;
window.runRankedWhatIfFromUI = runRankedWhatIfFromUI;
