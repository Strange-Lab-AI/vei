const WHATIF_CUSTOM_MOVE_LABEL = "Your move";

function whatIfSelectedEventPayload() {
  return state.whatIfSelectedEvent?.event || state.whatIfScene?.branch_event || null;
}

function whatIfCurrentScene() {
  if (!state.whatIfScene || state.whatIfScene.error) {
    return null;
  }
  return state.whatIfScene;
}

function whatIfRecipients(event) {
  const recipients = Array.isArray(event?.to_recipients)
    ? event.to_recipients.filter(Boolean)
    : [];
  if (recipients.length) {
    return recipients.join(", ");
  }
  return event?.target_id || "";
}

function whatIfDefaultLabel(event) {
  const subject = event?.subject || "counterfactual";
  return `${subject} alternate path`;
}

function whatIfDefaultPrompt(event) {
  if (!event) {
    return "";
  }
  const actor = event.actor_id || "someone";
  const subject = event.subject || "this thread";
  return `What if ${actor} had handled "${subject}" differently at this point?`;
}

function whatIfObjectivePacks() {
  return Array.isArray(state.whatIfStatus?.objective_packs)
    ? state.whatIfStatus.objective_packs
    : [];
}

function whatIfSourceId() {
  return state.whatIfStatus?.source || "auto";
}

function whatIfSourceLabel() {
  const source = whatIfSourceId();
  if (source === "mail_archive") {
    return "Historical mail archive";
  }
  if (source === "enron") {
    return "Enron Rosetta archive";
  }
  return "Historical archive";
}

function whatIfCustomMovePrompt() {
  return String(state.whatIfCustomMovePrompt || "").trim();
}

function whatIfDefaultSceneOption() {
  const scene = whatIfCurrentScene();
  const options = Array.isArray(scene?.candidate_options) ? scene.candidate_options : [];
  return options[0] || null;
}

function whatIfCustomOption() {
  const prompt = whatIfCustomMovePrompt();
  if (!prompt) {
    return null;
  }
  return {
    label: WHATIF_CUSTOM_MOVE_LABEL,
    prompt,
    summary: prompt,
  };
}

function whatIfSelectedOption() {
  if (state.whatIfChosenOptionLabel === WHATIF_CUSTOM_MOVE_LABEL) {
    return whatIfCustomOption() || whatIfDefaultSceneOption();
  }
  const scene = whatIfCurrentScene();
  const options = Array.isArray(scene?.candidate_options) ? scene.candidate_options : [];
  return options.find((option) => option.label === state.whatIfChosenOptionLabel) || options[0] || null;
}

function whatIfSuggestedLabel(event, option) {
  if (!event) {
    return option?.label || "counterfactual";
  }
  const subject = event.subject || event.thread_id || "counterfactual";
  if (!option?.label) {
    return `${subject} alternate path`;
  }
  return `${subject} - ${option.label}`;
}

function serializeWhatIfCandidates(candidates) {
  return candidates
    .filter((candidate) => candidate?.prompt)
    .map((candidate) => `${candidate.label || "Option"} | ${candidate.prompt}`)
    .join("\n");
}

function parseWhatIfCandidates(rawValue) {
  return String(rawValue || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line, index) => {
      const separatorIndex = line.indexOf("|");
      if (separatorIndex === -1) {
        return {
          label: `Option ${index + 1}`,
          prompt: line,
        };
      }
      const label = line.slice(0, separatorIndex).trim() || `Option ${index + 1}`;
      const prompt = line.slice(separatorIndex + 1).trim();
      return {
        label,
        prompt,
      };
    })
    .filter((candidate) => candidate.prompt);
}

function whatIfRankCandidates() {
  const candidatesInput = document.getElementById("whatif-candidates-input");
  const parsed = parseWhatIfCandidates(candidatesInput?.value || "");
  const scene = whatIfCurrentScene();
  let candidates = parsed;
  if (!candidates.length) {
    candidates = Array.isArray(scene?.candidate_options)
      ? scene.candidate_options.map((option) => ({
          label: option.label,
          prompt: option.prompt,
        }))
      : [];
  }
  const selectedOption = whatIfSelectedOption();
  if (!selectedOption?.prompt) {
    return candidates;
  }
  if (candidates.some((candidate) => candidate.label === selectedOption.label)) {
    return candidates;
  }
  return [
    {
      label: selectedOption.label,
      prompt: selectedOption.prompt,
    },
    ...candidates,
  ];
}

function syncWhatIfCandidateInputs() {
  const candidatesInput = document.getElementById("whatif-candidates-input");
  if (!candidatesInput) {
    return;
  }
  candidatesInput.value = serializeWhatIfCandidates(whatIfRankCandidates());
}

function setWhatIfAdvancedInputs(event, option) {
  const labelInput = document.getElementById("whatif-label-input");
  const promptInput = document.getElementById("whatif-prompt-input");
  if (labelInput) {
    labelInput.value = whatIfSuggestedLabel(event, option);
  }
  if (promptInput) {
    promptInput.value = option?.prompt || whatIfDefaultPrompt(event);
  }
}

function applyWhatIfScene(scene) {
  state.whatIfScene = scene;
  state.whatIfSceneLoading = false;
  state.whatIfCustomMovePrompt = "";
  state.whatIfChosenOptionLabel = scene?.candidate_options?.[0]?.label || "";
  const event = scene?.branch_event || whatIfSelectedEventPayload();
  setWhatIfAdvancedInputs(event, whatIfSelectedOption());
  syncWhatIfCandidateInputs();
}

function renderWhatIfHistoryEvents(events, { current = false } = {}) {
  if (!Array.isArray(events) || !events.length) {
    return `<div class="whatif-empty">No events available for this part of the thread.</div>`;
  }
  return `
    <div class="whatif-event-list">
      ${events
        .map((event) => {
          const meta = [
            event.timestamp || "",
            event.actor_id || "",
            whatIfRecipients(event) || "",
          ]
            .filter(Boolean)
            .join(" · ");
          return `
            <div class="whatif-event-card ${current ? "is-current" : ""}">
              <strong>${escapeHtml(event.subject || event.thread_id || event.event_id)}</strong>
              <span class="whatif-result-meta">${escapeHtml(meta)}</span>
              <span class="whatif-result-caption">${escapeHtml(event.event_type || "")}</span>
              ${
                event.snippet
                  ? `<p class="whatif-event-snippet">${escapeHtml(event.snippet)}</p>`
                  : ""
              }
            </div>
          `;
        })
        .join("")}
    </div>
  `;
}

function syncWhatIfSelectionAfterCustomEdit() {
  if (state.whatIfChosenOptionLabel !== WHATIF_CUSTOM_MOVE_LABEL) {
    return;
  }
  if (!whatIfCustomMovePrompt()) {
    state.whatIfChosenOptionLabel = whatIfDefaultSceneOption()?.label || "";
    setWhatIfAdvancedInputs(whatIfSelectedEventPayload(), whatIfSelectedOption());
    syncWhatIfCandidateInputs();
    renderWhatIfStudio();
    return;
  }
  setWhatIfAdvancedInputs(whatIfSelectedEventPayload(), whatIfSelectedOption());
  syncWhatIfCandidateInputs();
}

function useWhatIfCustomMove() {
  if (!whatIfCustomMovePrompt()) {
    return;
  }
  state.whatIfChosenOptionLabel = WHATIF_CUSTOM_MOVE_LABEL;
  setWhatIfAdvancedInputs(whatIfSelectedEventPayload(), whatIfSelectedOption());
  syncWhatIfCandidateInputs();
  renderWhatIfStudio();
}

function renderWhatIfScene(scene) {
  const selectedOption = whatIfSelectedOption();
  const customPrompt = state.whatIfCustomMovePrompt || "";
  const customSelected = state.whatIfChosenOptionLabel === WHATIF_CUSTOM_MOVE_LABEL;
  return `
    <div class="whatif-scene-shell">
      <div class="whatif-scene-hero">
        <div class="whatif-scene-copy">
          <p class="eyebrow">Decision Scene</p>
          <strong>${escapeHtml(scene.thread_subject || scene.branch_event.subject || scene.thread_id)}</strong>
          <span>${escapeHtml(scene.branch_summary || "")}</span>
          <span class="metric-detail">${escapeHtml(scene.decision_question || "")}</span>
        </div>
        <div class="whatif-chip-row">
          <span class="whatif-chip">${escapeHtml(scene.organization_name || "Company")}</span>
          <span class="whatif-chip">${escapeHtml(scene.history_message_count || 0)} prior messages</span>
          <span class="whatif-chip">${escapeHtml(scene.future_event_count || 0)} recorded future events</span>
        </div>
      </div>
      <div class="whatif-scene-grid">
        <div class="whatif-scene-panel">
          <p class="eyebrow">Branch Moment</p>
          <strong>${escapeHtml(scene.branch_event.subject || scene.branch_event.event_id)}</strong>
          <span class="whatif-result-meta">${escapeHtml(scene.branch_event.timestamp || "")} · ${escapeHtml(scene.branch_event.actor_id || "")}</span>
          <span class="whatif-result-caption">${escapeHtml(whatIfRecipients(scene.branch_event) || "")}</span>
          ${
            scene.branch_event.snippet
              ? `<p class="whatif-event-snippet">${escapeHtml(scene.branch_event.snippet)}</p>`
              : ""
          }
        </div>
        <div class="whatif-scene-panel">
          <p class="eyebrow">What Actually Happened</p>
          <strong>${escapeHtml(scene.historical_action_summary || "")}</strong>
          <span>${escapeHtml(scene.historical_outcome_summary || "")}</span>
          <span class="metric-detail">${escapeHtml(scene.stakes_summary || "")}</span>
        </div>
      </div>
      <div class="whatif-scene-panel whatif-thread-panel">
        <div class="whatif-thread-head">
          <div>
            <p class="eyebrow">Thread So Far</p>
            <strong>Read the lead-up before you choose a move</strong>
          </div>
          ${
            scene.content_notice
              ? `<span class="metric-detail">${escapeHtml(scene.content_notice)}</span>`
              : ""
          }
        </div>
        ${renderWhatIfHistoryEvents(scene.history_preview || [])}
      </div>
      <div class="whatif-scene-panel">
        <div class="whatif-thread-head">
          <div>
            <p class="eyebrow">Recorded Future</p>
            <strong>These are the first observed follow-ups after the branch point</strong>
          </div>
        </div>
        ${renderWhatIfHistoryEvents(scene.historical_future_preview || [])}
      </div>
      <div class="whatif-scene-panel">
        <div class="whatif-thread-head">
          <div>
            <p class="eyebrow">Choose A Move</p>
            <strong>Pick one action, then score it against the others</strong>
          </div>
          ${
            selectedOption
              ? `<span class="whatif-chip is-selected">Selected: ${escapeHtml(selectedOption.label)}</span>`
              : ""
          }
        </div>
        <div class="whatif-option-grid">
          ${(scene.candidate_options || [])
            .map((option) => {
              const isSelected = option.label === state.whatIfChosenOptionLabel;
              return `
                <button
                  type="button"
                  class="whatif-option-card ${isSelected ? "is-selected" : ""}"
                  data-whatif-option-label="${escapeHtml(option.label)}"
                >
                  <span class="whatif-option-badge">${isSelected ? "Your pick" : "Candidate"}</span>
                  <strong class="whatif-option-title">${escapeHtml(option.label)}</strong>
                  <span class="whatif-option-summary">${escapeHtml(option.summary || option.prompt)}</span>
                </button>
              `;
            })
            .join("")}
        </div>
        <div class="whatif-custom-move ${customSelected ? "is-selected" : ""}">
          <div class="whatif-thread-head">
            <div>
              <p class="eyebrow">Or Write Your Own Move</p>
              <strong>Type the next step you want to test</strong>
            </div>
            ${
              customSelected
                ? `<span class="whatif-chip is-selected">Selected: ${escapeHtml(WHATIF_CUSTOM_MOVE_LABEL)}</span>`
                : ""
            }
          </div>
          <label class="whatif-field whatif-field-wide">
            <span>Describe the action in plain English</span>
            <textarea
              id="whatif-custom-move-input"
              rows="4"
              placeholder="Keep the draft inside Enron, ask Gerald Nemec for review, and hold the outside send until legal clears it."
            >${escapeHtml(customPrompt)}</textarea>
          </label>
          <div class="whatif-custom-actions">
            <button type="button" id="whatif-custom-move-btn">Use my move</button>
            <span class="metric-detail">VEI will score your typed move against the anchor moves for this same branch point.</span>
          </div>
        </div>
      </div>
    </div>
  `;
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
        <strong>Historical archive not configured for this workspace.</strong>
        <span>Set <code>VEI_WHATIF_SOURCE_DIR</code> to a mail archive or context snapshot, or set <code>VEI_WHATIF_ROSETTA_DIR</code> for the Enron Rosetta source.</span>
      </div>
    `;
    resultsNode.innerHTML = "";
    selectionNode.innerHTML = "";
    openNode.innerHTML = "";
    resultNode.innerHTML = "";
    return;
  }

  const statusLabel = state.whatIfSceneLoading ? "Loading scene" : "Archive ready";
  const statusDetail = state.whatIfSceneLoading
    ? state.whatIfSelectedEvent?.event?.subject || "Historical decision"
    : status.source_dir || whatIfSourceLabel();
  statusNode.innerHTML = `
    <div class="whatif-status-pill">
      <strong>${escapeHtml(statusLabel)}</strong>
      <span>${escapeHtml(statusDetail)}</span>
    </div>
  `;

  if (objectiveSelect) {
    const packs = whatIfObjectivePacks();
    const currentValue = objectiveSelect.value || "contain_exposure";
    objectiveSelect.innerHTML = packs.length
      ? packs
          .map(
            (pack) =>
              `<option value="${escapeHtml(pack.pack_id)}">${escapeHtml(pack.title)}</option>`,
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
        ${searchResult.matches
          .map((match, index) => {
            const event = match.event || {};
            const reasons = Array.isArray(match.reason_labels)
              ? match.reason_labels.join(", ")
              : "";
            const isSelected = state.whatIfSelectedEvent?.event?.event_id === event.event_id;
            return `
              <button type="button" class="whatif-result-item ${isSelected ? "is-selected" : ""}" data-whatif-index="${index}">
                <span class="whatif-result-title">${escapeHtml(event.subject || event.thread_id || event.event_id)}</span>
                <span class="whatif-result-meta">${escapeHtml(event.timestamp || "")} · ${escapeHtml(event.actor_id || "")}</span>
                <span class="whatif-result-meta">${escapeHtml(whatIfRecipients(event) || "")}</span>
                <span class="whatif-result-caption">${escapeHtml(reasons || "historical match")}</span>
              </button>
            `;
          })
          .join("")}
      </div>
    `;
    resultsNode.querySelectorAll("[data-whatif-index]").forEach((node) => {
      node.addEventListener("click", () => {
        const index = Number(node.getAttribute("data-whatif-index"));
        const match = state.whatIfSearchResult?.matches?.[index];
        if (!match?.event) {
          return;
        }
        state.whatIfSelectedEvent = match;
        state.whatIfOpenResult = null;
        state.whatIfExperimentResult = null;
        state.whatIfRankedResult = null;
        void loadWhatIfDecisionScene({
          eventId: match.event.event_id,
          threadId: match.event.thread_id,
        });
      });
    });
  }

  if (state.whatIfSceneLoading) {
    selectionNode.innerHTML = `
      <div class="whatif-empty">
        <strong>Loading the decision scene.</strong>
        <span>Pulling the thread history, the branch point, and the observed future.</span>
      </div>
    `;
  } else if (state.whatIfScene?.error) {
    selectionNode.innerHTML = `
      <div class="whatif-empty">
        <strong>Could not load this decision scene.</strong>
        <span>${escapeHtml(state.whatIfScene.error)}</span>
      </div>
    `;
  } else if (whatIfCurrentScene()) {
    selectionNode.innerHTML = renderWhatIfScene(whatIfCurrentScene());
    selectionNode
      .querySelectorAll("[data-whatif-option-label]")
      .forEach((button) => {
        button.addEventListener("click", () => {
          const label = button.getAttribute("data-whatif-option-label") || "";
          state.whatIfChosenOptionLabel = label;
          setWhatIfAdvancedInputs(whatIfSelectedEventPayload(), whatIfSelectedOption());
          syncWhatIfCandidateInputs();
          renderWhatIfStudio();
        });
      });
    const customInput = document.getElementById("whatif-custom-move-input");
    if (customInput) {
      customInput.addEventListener("input", () => {
        state.whatIfCustomMovePrompt = customInput.value;
        syncWhatIfSelectionAfterCustomEdit();
      });
    }
    document
      .getElementById("whatif-custom-move-btn")
      ?.addEventListener("click", useWhatIfCustomMove);
  } else if (!whatIfSelectedEventPayload()) {
    selectionNode.innerHTML = `<div class="whatif-empty">Choose one of the matching events to inspect the exact branch point.</div>`;
  } else {
    const selected = whatIfSelectedEventPayload();
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
      </div>
    `;
  }

  const openResult = state.whatIfOpenResult;
  if (openResult?.error) {
    openNode.innerHTML = `
      <div class="whatif-open-card">
        <strong>Could not materialize the baseline.</strong>
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
          <p class="eyebrow">Ranked decision failed</p>
          <strong>${escapeHtml(ranked.label || "Historical decision")}</strong>
          <span>${escapeHtml(ranked.error)}</span>
        </div>
      `;
      return;
    }
    const objective = ranked.objective_pack || {};
    const candidates = Array.isArray(ranked.candidates) ? ranked.candidates : [];
    const chosenLabel = state.whatIfChosenOptionLabel || "";
    const chosenCandidate = candidates.find(
      (candidate) => candidate.intervention?.label === chosenLabel,
    );
    const scene = whatIfCurrentScene();
    resultNode.innerHTML = `
      <div class="whatif-summary-grid">
        ${
          chosenCandidate
            ? `
              <div class="whatif-summary-card is-picked">
                <p class="eyebrow">Your Pick</p>
                <strong>${escapeHtml(chosenCandidate.intervention?.label || chosenLabel)}</strong>
                <span>Rank ${escapeHtml(chosenCandidate.rank || "n/a")} of ${escapeHtml(candidates.length || 0)}</span>
                <span>Score ${escapeHtml(chosenCandidate.outcome_score?.overall_score ?? "n/a")} for ${escapeHtml(objective.title || "this objective")}</span>
              </div>
            `
            : ""
        }
        <div class="whatif-summary-card is-recommended">
          <p class="eyebrow">Recommended Move</p>
          <strong>${escapeHtml(ranked.recommended_candidate_label || "No recommendation")}</strong>
          <span>${escapeHtml(objective.title || "Objective")}</span>
        </div>
        ${
          scene
            ? `
              <div class="whatif-summary-card">
                <p class="eyebrow">Historical Path</p>
                <strong>${escapeHtml(scene.historical_action_summary || "")}</strong>
                <span>${escapeHtml(scene.historical_outcome_summary || "")}</span>
              </div>
            `
            : ""
        }
      </div>
      <div class="whatif-ranked-list">
        ${candidates
          .map((candidate) => {
            const score = candidate.outcome_score?.overall_score ?? "n/a";
            const shadowScore = candidate.shadow?.outcome_score?.overall_score;
            const isRecommended =
              candidate.intervention?.label === ranked.recommended_candidate_label;
            const isPicked = candidate.intervention?.label === chosenLabel;
            return `
              <div class="whatif-summary-card ${isRecommended ? "is-recommended" : ""} ${isPicked ? "is-picked" : ""}">
                <div class="whatif-summary-flags">
                  <p class="eyebrow">Rank ${escapeHtml(candidate.rank || "n/a")}</p>
                  ${isPicked ? `<span class="whatif-chip is-selected">Your pick</span>` : ""}
                  ${isRecommended ? `<span class="whatif-chip">VEI pick</span>` : ""}
                </div>
                <strong>${escapeHtml(candidate.intervention?.label || "")}</strong>
                <span>${escapeHtml(candidate.reason || "")}</span>
                <span>Score ${escapeHtml(score)} across ${escapeHtml(candidate.rollout_count || 0)} rollouts</span>
                <span>Exposure ${escapeHtml(candidate.average_outcome_signals?.exposure_risk ?? "n/a")} · Delay ${escapeHtml(candidate.average_outcome_signals?.delay_risk ?? "n/a")} · Relationship ${escapeHtml(candidate.average_outcome_signals?.relationship_protection ?? "n/a")}</span>
                ${
                  shadowScore != null
                    ? `<span>Shadow ${escapeHtml(candidate.shadow?.backend || "forecast")} ${escapeHtml(shadowScore)}</span>`
                    : ""
                }
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
        <strong>${escapeHtml(experiment.label || "Historical decision")}</strong>
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
    state.whatIfScene = null;
    state.whatIfChosenOptionLabel = "";
    state.whatIfCustomMovePrompt = "";
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
        source: whatIfSourceId(),
        query,
        limit,
      }),
    });
    state.whatIfSearchResult = result;
    state.whatIfSelectedEvent = null;
    state.whatIfScene = null;
    state.whatIfChosenOptionLabel = "";
    state.whatIfCustomMovePrompt = "";
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

async function loadWhatIfDecisionScene({ eventId = null, threadId = null } = {}) {
  if (!eventId && !threadId) {
    return;
  }
  state.whatIfSceneLoading = true;
  state.whatIfScene = null;
  renderWhatIfStudio();
  try {
    const scene = await getJson("/api/workspace/whatif/scene", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source: whatIfSourceId(),
        event_id: eventId,
        thread_id: threadId,
      }),
    });
    applyWhatIfScene(scene);
  } catch (error) {
    state.whatIfSceneLoading = false;
    state.whatIfScene = {
      error: error?.message || String(error),
    };
  }
  renderWhatIfStudio();
}

async function materializeWhatIfEpisode() {
  const event = whatIfSelectedEventPayload();
  if (!event) {
    return;
  }
  const label =
    document.getElementById("whatif-label-input")?.value?.trim() ||
    whatIfSuggestedLabel(event, whatIfSelectedOption());
  try {
    state.whatIfOpenResult = await getJson("/api/workspace/whatif/open", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source: whatIfSourceId(),
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
  if (!event) {
    return;
  }
  const label =
    document.getElementById("whatif-label-input")?.value?.trim() ||
    whatIfSuggestedLabel(event, whatIfSelectedOption());
  const prompt =
    document.getElementById("whatif-prompt-input")?.value?.trim() ||
    whatIfSelectedOption()?.prompt ||
    "";
  if (!prompt) {
    return;
  }
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
        source: whatIfSourceId(),
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
  if (!event) {
    return;
  }
  const selectedOption = whatIfSelectedOption();
  const label =
    document.getElementById("whatif-label-input")?.value?.trim() ||
    whatIfSuggestedLabel(event, selectedOption);
  const objectivePackId =
    document.getElementById("whatif-objective-select")?.value || "contain_exposure";
  const rolloutCount = Number(
    document.getElementById("whatif-rollout-count-input")?.value || 4,
  );
  const candidates = whatIfRankCandidates();
  if (!candidates.length) {
    return;
  }
  const statusNode = document.getElementById("whatif-status");
  if (statusNode) {
    statusNode.innerHTML = `<div class="whatif-status-pill"><strong>Scoring</strong><span>${escapeHtml(label)}</span></div>`;
  }
  try {
    state.whatIfExperimentResult = null;
    state.whatIfRankedResult = await getJson("/api/workspace/whatif/rank", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source: whatIfSourceId(),
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

async function primeWhatIfSceneFromHistoricalWorkspace() {
  const historical = state.historicalWorkspace;
  if (!state.whatIfStatus?.available || !historical?.branch_event_id) {
    return;
  }
  if (state.whatIfSceneLoading || state.whatIfScene || state.whatIfSelectedEvent) {
    return;
  }
  state.whatIfSelectedEvent = {
    event: historical.branch_event,
    reason_labels: ["saved historical branch"],
  };
  await loadWhatIfDecisionScene({
    eventId: historical.branch_event_id,
    threadId: historical.thread_id,
  });
}

window.renderWhatIfStudio = renderWhatIfStudio;
window.searchWhatIfEvents = searchWhatIfEvents;
window.loadWhatIfDecisionScene = loadWhatIfDecisionScene;
window.materializeWhatIfEpisode = materializeWhatIfEpisode;
window.runWhatIfExperimentFromUI = runWhatIfExperimentFromUI;
window.runRankedWhatIfFromUI = runRankedWhatIfFromUI;
window.primeWhatIfSceneFromHistoricalWorkspace = primeWhatIfSceneFromHistoricalWorkspace;
