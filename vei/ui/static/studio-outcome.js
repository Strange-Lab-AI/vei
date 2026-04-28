function serviceOpsReplayAvailable() {
  return Boolean(
    state.missionState?.mission?.vertical_name === "service_ops"
      && state.missionState?.run_id
  );
}

function renderOutcomeActions() {
  const replayButton = document.getElementById("outcome-policy-btn");
  if (!replayButton) return;
  replayButton.style.display = serviceOpsReplayAvailable() ? "" : "none";
}

function renderRunHeader() {
  const run = state.activeRun;
  const title = el("active-run-title");
  const badges = el("active-run-badges");
  if (!title || !badges) return;
  if (!run) {
    title.textContent = "No run selected";
    badges.innerHTML = "";
    return;
  }
  title.textContent = displayScenarioVariantTitle(run.scenario_name, run.run_id || "Run");
  badges.innerHTML = [
    badge(displayRunnerTitle(run.runner), null),
    badge(displayStatusTitle(run.status), null, statusClass(run.status)),
    badge("Contract", displayContractHealth(run.contract?.ok), statusClass(run.contract?.ok)),
    badge("Run ID", run.run_id),
    run.branch ? badge("Branch", displayBranchTitle(run.branch)) : "",
  ].join("");
}

function renderRunSummary() {
  const run = state.activeRun;
  const contract = state.activeRunContract;
  if (!run) {
    return;
  }
  const panel = document.getElementById("run-outcome-summary");
  const branchPanel = document.getElementById("branch-outcome-panel");
  const successPassed = run.contract?.success_assertions_passed || 0;
  const successTotal = run.contract?.success_assertion_count || 0;
  const issueCount = run.contract?.issue_count || contract?.dynamic_validation?.issues?.length || 0;
  const policyFails = contract?.policy_invariants_failed || 0;
  const graphEvents = state.timeline.filter((item) => item.graph_intent || item.graph_action_ref);
  const graphDomains = uniqueStrings(graphEvents.map((item) => item.graph_domain).filter(Boolean));
  const resolvedTools = uniqueStrings(graphEvents.map((item) => item.resolved_tool).filter(Boolean));
  const story = state.story || {};
  const outcome = story.outcome || null;
  const whatIfBranches = Array.isArray(story.branch_labels) && story.branch_labels.length
    ? story.branch_labels
    : state.scenarioPreview?.scenario?.metadata?.builder_environment?.what_if_branches || [];
  const outcomeTitle = run.contract?.ok
    ? "This path is holding the company together."
    : run.contract?.ok === false
      ? "This path still leaves the company exposed."
      : "This path is still unfolding.";
  const outcomeBody = inWorldCopy(
    outcome?.why_it_matters?.[0],
    run.contract?.ok
      ? "The moves so far are reducing the business risk tied to this crisis."
      : "The moves so far have not fully repaired the business risk tied to this crisis."
  );
  const changedTitle = inWorldCopy(
    outcome?.what_changed?.[0],
    run.contract?.ok
      ? "The run is starting to clear the operational blockers."
      : "The run changed the company, but not enough to make the crisis safe yet."
  );
  const changedDetail = inWorldCopy(
    outcome?.what_changed?.[1],
    `${issueCount} issue${issueCount === 1 ? "" : "s"} remain open across ${graphDomains.length || 1} active domain${graphDomains.length === 1 ? "" : "s"}.`
  );
  panel.innerHTML = `
    <div class="score-strip">
      ${scorePill("Contract", displayContractHealth(run.contract?.ok), displayContractVariantTitle(run.contract?.contract_name || "", "Workspace contract"))}
      ${scorePill("Success checks", `${successPassed}/${successTotal}`)}
      ${scorePill("Issues", String(issueCount))}
      ${scorePill("Policy overrides", String(policyFails))}
      ${scorePill("Run events", compactNumber(state.timeline.length))}
    </div>
    <div class="briefing-grid">
      <div class="story-card accent-card story-span-2">
        <p class="eyebrow">Did this help?</p>
        <h3>${escapeHtml(outcomeTitle)}</h3>
        <p class="metric-detail">${escapeHtml(outcomeBody)}</p>
        <div class="detail-grid">
          ${detailTile("System changes", compactNumber(graphEvents.length))}
          ${detailTile("Snapshots", compactNumber(state.snapshots.length))}
          ${detailTile("Domains", compactNumber(graphDomains.length))}
          ${detailTile("Elapsed time", formatMs(run.metrics?.time_ms || 0))}
        </div>
        <div class="chip-row">${graphDomains.map((item) => chip(formatDomainTitle(item))).join("")}</div>
      </div>
      <div class="story-card">
        <p class="eyebrow">What changed</p>
        <h3>${escapeHtml(changedTitle)}</h3>
        <p class="metric-detail">${escapeHtml(changedDetail)}</p>
        ${(() => {
          const riskyPlayed = (state.missionState?.executed_moves || []).filter((m) => m.payload?.availability === "risky");
          return riskyPlayed.length
            ? `<div class="decision-policy-alert" style="margin-top:8px">
                <span class="policy-modal-badge">Policy Override</span>
                <span>${riskyPlayed.length} policy override${riskyPlayed.length === 1 ? "" : "s"}: ${riskyPlayed.map((m) => escapeHtml(m.title)).join(", ")}</span>
              </div>`
            : "";
        })()}
        <div class="chip-row">${resolvedTools.slice(0, 5).map((item) => chip(humanize(item))).join("")}</div>
      </div>
      ${
        whatIfBranches.length
          ? `<div class="story-card">
              <p class="eyebrow">What-if paths</p>
              <h3>Alternate paths</h3>
              <div class="chip-row">${whatIfBranches.map((item) => chip(displayBranchTitle(item))).join("")}</div>
              <p class="metric-detail">These are alternate futures that begin from the same company state.</p>
            </div>`
          : ""
      }
    </div>
  `;
  if (branchPanel) {
    const branchChangeLines = (outcome?.what_changed || [])
      .map((item) => inWorldCopy(item, ""))
      .filter(Boolean);
    const branchReasonLines = (outcome?.why_it_matters || [])
      .map((item) => inWorldCopy(item, ""))
      .filter(Boolean);
    branchPanel.innerHTML = outcome
      ? `
        <div class="story-card accent-card story-span-2">
          <p class="eyebrow">Base world</p>
          <h3>${escapeHtml(story.manifest?.company_name || state.workspace?.manifest?.title || "Company world")}</h3>
          <p class="metric-detail">${escapeHtml(outcome.base_world || "")}</p>
        </div>
        <div class="story-card">
          <p class="eyebrow">Chosen situation</p>
          <p class="metric-detail">${escapeHtml(outcome.chosen_situation || "")}</p>
        </div>
        <div class="story-card">
          <p class="eyebrow">Chosen objective</p>
          <p class="metric-detail">${escapeHtml(outcome.chosen_objective || "")}</p>
        </div>
        <div class="story-card">
          <p class="eyebrow">Baseline branch</p>
          <p class="metric-detail">${escapeHtml(outcome.baseline_branch || "Baseline path")}</p>
        </div>
        <div class="story-card">
          <p class="eyebrow">Agent branch</p>
          <p class="metric-detail">${escapeHtml(outcome.comparison_branch || "Agent path")}</p>
        </div>
        <div class="story-card story-span-2">
          <p class="eyebrow">What changed</p>
          <div class="stack">${(branchChangeLines.length ? branchChangeLines : [changedTitle, changedDetail]).map((item) => `<p class="metric-detail">${escapeHtml(item)}</p>`).join("")}</div>
        </div>
        <div class="story-card story-span-2">
          <p class="eyebrow">Why the result was good or bad</p>
          <div class="stack">${(branchReasonLines.length ? branchReasonLines : [outcomeBody]).map((item) => `<p class="metric-detail">${escapeHtml(item)}</p>`).join("")}</div>
        </div>
      `
      : `
        <div class="story-card story-span-2">
          <p class="eyebrow">Branch story</p>
          <p class="metric-detail">Open a baseline path and a comparison path to see how the same company can end in different states.</p>
        </div>
      `;
  }
  renderDecisionLog();
  renderExportsPanel();
  renderJson("run-panel", run);
  renderJson("contract-panel", contract || { note: "No run contract evaluation yet." });
}

function renderDecisionLog() {
  const el = document.getElementById("decision-log");
  if (!el) return;

  const ms = state.missionState;
  const executedMoves = ms?.executed_moves || [];
  const riskyMoves = executedMoves.filter((m) => m.payload?.availability === "risky");

  if (!executedMoves.length) {
    el.innerHTML = "";
    return;
  }

  const policyChanged = riskyMoves.length > 0;
  let html = `<div class="decision-log-section">`;
  html += `<p class="eyebrow">Decision audit trail</p>`;

  if (policyChanged) {
    html += `<div class="decision-policy-alert">
      <span class="policy-modal-badge">Policy Override</span>
      <span>Policy was modified at move ${riskyMoves.map((m) => String(executedMoves.indexOf(m) + 1)).join(", ")}</span>
    </div>`;
  }

  html += `<div class="decision-log-entries">`;
  executedMoves.forEach((move, idx) => {
    const isRisky = move.payload?.availability === "risky";
    html += `
      <div class="decision-entry ${isRisky ? "decision-entry-risky" : ""}">
        <span class="decision-entry-idx">${idx + 1}</span>
        <div class="decision-entry-body">
          <strong>${escapeHtml(move.title)}</strong>
          ${isRisky ? `<span class="decision-risky-tag">policy override</span>` : ""}
          <p class="metric-detail">${escapeHtml(move.branch_label || "")}</p>
        </div>
      </div>`;
  });
  html += `</div></div>`;
  el.innerHTML = html;
}

function renderExportsPanel() {
  const panel = document.getElementById("exports-panel");
  if (!panel) {
    return;
  }
  const exportsPreview =
    Array.isArray(state.missionState?.exports) && state.missionState.exports.length
      ? state.missionState.exports
      : Array.isArray(state.exportsPreview)
        ? state.exportsPreview
        : [];
  panel.innerHTML = exportsPreview.length
    ? exportsPreview
        .map(
          (item) => `
            <div class="story-card">
              <p class="eyebrow">${escapeHtml(item.title || humanize(item.name) || "Export")}</p>
              <h3>${escapeHtml(item.title || humanize(item.name) || "Export")}</h3>
              <p class="metric-detail">${escapeHtml(item.summary || "")}</p>
              <div class="chip-row">
                ${Object.entries(item.payload || {})
                  .slice(0, 4)
                  .map(([key, value]) => chip(`${key}=${Array.isArray(value) ? value.length : value}`))
                  .join("")}
              </div>
            </div>
          `
        )
        .join("")
    : `
      <div class="story-card story-span-2">
        <p class="eyebrow">Exports</p>
        <p class="metric-detail">Story exports appear here once the workspace has a narrative bundle or a baseline/comparison pair to summarize.</p>
      </div>
    `;
}

function positionForEvent(event, timeline) {
  if (!timeline.length) {
    return 0;
  }
  const maxTime = Math.max(...timeline.map((item) => Number(item.time_ms || 0)), 1);
  const time = Number(event.time_ms || 0);
  return maxTime === 0 ? 0 : Math.max(0, Math.min(100, (time / maxTime) * 100));
}

function renderPlaybackStage() {
  const timeline = state.timeline;
  const panel = document.getElementById("playback-stage");
  const feed = document.getElementById("timeline-feed");
  const slider = document.getElementById("timeline-slider");
  const selected = timeline[state.selectedEventIndex] || null;

  slider.max = String(Math.max(0, timeline.length - 1));
  slider.value = String(Math.max(0, state.selectedEventIndex));
  document.getElementById("playback-counter").textContent = timeline.length
    ? `${state.selectedEventIndex + 1} / ${timeline.length}`
    : "0 / 0";
  document.getElementById("playback-time").textContent = selected
    ? `t=${formatMs(selected.time_ms)}`
    : "t=0ms";

  if (!timeline.length) {
    panel.innerHTML = `<div class="stack-card"><h3>No events yet</h3><p class="metric-detail">Playback appears after you <strong>start a scenario</strong> and a run records tool calls. Open <strong>Run tools</strong> (technical detail) to launch a run, or pick an existing run from run history.</p></div>`;
    feed.innerHTML = "";
    return;
  }

  const grouped = new Map();
  for (const event of timeline) {
    const channel = event.channel || "Misc";
    if (!grouped.has(channel)) {
      grouped.set(channel, []);
    }
    grouped.get(channel).push(event);
  }

  const laneOrder = Array.from(grouped.keys()).sort((a, b) => {
    const known = Object.keys(CHANNEL_COLORS);
    return (known.indexOf(a) === -1 ? 999 : known.indexOf(a)) - (known.indexOf(b) === -1 ? 999 : known.indexOf(b));
  });
  const playhead = selected ? positionForEvent(selected, timeline) : 0;

  panel.innerHTML = laneOrder
    .map((channel) => {
      const items = grouped.get(channel) || [];
      return `
        <div class="lane">
          <div class="lane-label">${escapeHtml(channel)}</div>
          <div class="lane-track">
            <div class="lane-playhead" style="left: ${playhead}%"></div>
            ${items
              .map((event) => {
                const index = timeline.findIndex((item) => item.index === event.index);
                const color = CHANNEL_COLORS[channel] || CHANNEL_COLORS.Misc;
                return `
                  <button
                    type="button"
                    class="lane-event ${index === state.selectedEventIndex ? "active" : ""}"
                    data-index="${index}"
                    style="left: ${positionForEvent(event, timeline)}%; --event-color: ${color};"
                    title="${escapeHtml(event.label)}"
                  ></button>
                `;
              })
              .join("")}
          </div>
        </div>
      `;
    })
    .join("");

  panel.querySelectorAll(".lane-event").forEach((node) => {
    node.addEventListener("click", () => {
      setSelectedEvent(Number(node.dataset.index));
    });
  });

  feed.innerHTML = timeline
    .map((event, index) => {
      const metaBits = [
        event.kind,
        event.channel,
        `t=${formatMs(event.time_ms)}`,
        event.resolved_tool ? `resolved=${event.resolved_tool}` : null,
        event.graph_intent ? `graph=${event.graph_intent}` : null,
        !event.graph_intent && event.graph_action_ref ? `graph=${event.graph_action_ref}` : null,
      ].filter(Boolean);
      return `
        <div class="timeline-card ${index === state.selectedEventIndex ? "active" : ""}" data-index="${index}">
          <strong>${escapeHtml(`${event.index}. ${event.label}`)}</strong>
          <div class="timeline-meta">
            ${metaBits.map((bit) => chip(bit)).join("")}
          </div>
        </div>
      `;
    })
    .join("");

  feed.querySelectorAll(".timeline-card").forEach((node) => {
    node.addEventListener("click", () => {
      setSelectedEvent(Number(node.dataset.index));
    });
  });
}

function renderEventDetail() {
  const event = state.timeline[state.selectedEventIndex];
  const summary = document.getElementById("event-detail-summary");
  if (!event) {
    summary.innerHTML = "";
    renderJson("event-panel", { note: "Select a timeline event to inspect." });
    return;
  }
  summary.innerHTML = `
    <div class="detail-grid">
      ${detailTile("Label", event.label)}
      ${detailTile("Kind", event.kind)}
      ${detailTile("Channel", event.channel)}
      ${detailTile("Time", formatMs(event.time_ms))}
      ${detailTile("Tool", event.tool || "n/a")}
      ${detailTile("Resolved", event.resolved_tool || event.graph_action_ref || "n/a")}
      ${detailTile("Graph intent", event.graph_intent || "n/a")}
      ${detailTile("Graph domain", event.graph_domain || "n/a")}
    </div>
    <div class="chip-row">
      ${(event.object_refs || []).map((item) => `<button type="button" class="ghost-button provenance-chip" data-object-ref="${escapeHtml(item)}">${escapeHtml(item)}</button>`).join("")}
    </div>
  `;
  renderJson("event-panel", event);
  summary.querySelectorAll(".provenance-chip").forEach((node) => {
    node.addEventListener("click", () => {
      state.selectedObjectRef = node.dataset.objectRef;
      renderImportSummary();
    });
  });
}

function renderOrientation() {
  const orientation = state.orientation;
  const panel = document.getElementById("orientation-summary");
  const questions = document.getElementById("next-questions");
  if (!orientation) {
    panel.innerHTML = "";
    questions.innerHTML = "";
    return;
  }
  panel.innerHTML = `
    <div class="stack-card">
      <h3>World summary</h3>
      <p class="metric-detail">${escapeHtml(orientation.summary || "No summary available.")}</p>
      <div class="chip-row">${(orientation.available_surfaces || []).map((item) => chip(item)).join("")}</div>
    </div>
    <div class="stack-card">
      <h3>Suggested focuses</h3>
      <div class="chip-row">${(orientation.suggested_focuses || []).map((item) => chip(item)).join("")}</div>
    </div>
    <div class="stack-card">
      <h3>Key objects</h3>
      <div class="stack">${(orientation.key_objects || []).slice(0, 6).map(keyObjectCard).join("")}</div>
    </div>
  `;
  questions.innerHTML = (orientation.next_questions || [])
    .map((question) => `<div class="question-item">${escapeHtml(question)}</div>`)
    .join("");

  renderJson("orientation-panel", orientation);
}

function renderGraphs() {
  const graphs = state.graphs;
  const panel = document.getElementById("graphs-panel");
  if (!graphs) {
    panel.innerHTML = "";
    return;
  }

  const domains = (graphs.available_domains || []).filter((item) => graphs[item]);
  panel.innerHTML = domains
    .map((domain) => {
      const stats = summarizeGraph(graphs[domain]);
      const headline = graphs[domain]?.scenario_brief || graphs[domain]?.organization_name || "";
      return `
        <div class="graph-card">
          <h3>${escapeHtml(formatDomainTitle(domain))}</h3>
          ${headline ? `<p class="metric-detail">${escapeHtml(headline)}</p>` : ""}
          <div class="graph-stats">
            ${stats.map(([label, value]) => `<span><strong>${escapeHtml(label)}</strong><em>${escapeHtml(String(value))}</em></span>`).join("")}
          </div>
          <div class="chip-row">${chip(domain)}</div>
        </div>
      `;
    })
    .join("");

  renderJson("graphs-raw-panel", graphs);
}

function renderKnowledge() {
  const knowledge = state.knowledgeStore;
  const panel = document.getElementById("knowledge-panel");
  const viewer = document.getElementById("knowledge-citation-viewer");
  if (!panel || !viewer) {
    return;
  }
  const assets = Object.values(knowledge?.assets || {});
  if (!assets.length) {
    panel.innerHTML = "";
    viewer.innerHTML = "";
    return;
  }

  const ordered = assets
    .filter((item) => item && typeof item === "object")
    .sort((left, right) => {
      const leftComposed = left.composition ? 1 : 0;
      const rightComposed = right.composition ? 1 : 0;
      if (leftComposed !== rightComposed) {
        return rightComposed - leftComposed;
      }
      const leftTime = Number(left.metadata?.captured_at_ms || 0);
      const rightTime = Number(right.metadata?.captured_at_ms || 0);
      return rightTime - leftTime;
    });

  panel.innerHTML = ordered
    .slice(0, 8)
    .map((asset) => `
      <button class="graph-card knowledge-card" data-asset-id="${escapeHtml(asset.asset_id)}">
        <h3>${escapeHtml(asset.title || asset.asset_id)}</h3>
        <p class="metric-detail">${escapeHtml(asset.summary || asset.body || "")}</p>
        <div class="chip-row">
          ${chip(asset.kind || "knowledge")}
          ${chip(asset.status || "active", statusClass(asset.status || "active"))}
          ${(asset.tags || []).slice(0, 2).map((tag) => chip(tag)).join("")}
          ${asset.composition ? chip("composed", "ok") : ""}
        </div>
      </button>
    `)
    .join("");

  const selected = ordered.find((asset) => asset.composition) || ordered[0];
  viewer.innerHTML = renderKnowledgeDetail(selected);
  panel.querySelectorAll("[data-asset-id]").forEach((node) => {
    node.addEventListener("click", () => {
      const assetId = node.getAttribute("data-asset-id");
      const asset = ordered.find((item) => item.asset_id === assetId);
      viewer.innerHTML = renderKnowledgeDetail(asset || selected);
    });
  });
}

function renderKnowledgeDetail(asset) {
  if (!asset) {
    return "";
  }
  const composition = asset.composition || null;
  if (!composition) {
    return `
      <div class="detail-grid">
        ${detailTile("Asset", asset.asset_id || "")}
        ${detailTile("Kind", asset.kind || "")}
        ${detailTile("Status", asset.status || "")}
      </div>
      <p class="metric-detail">${escapeHtml(asset.body || "")}</p>
    `;
  }
  return `
    <div class="detail-grid">
      ${detailTile("Asset", asset.asset_id || "")}
      ${detailTile("Target", composition.target || "")}
      ${detailTile("Template", composition.template_id || "")}
      ${detailTile("Validation", (composition.validation?.issues || []).length ? "issues" : "ok")}
    </div>
    <p class="metric-detail">${escapeHtml(asset.summary || "")}</p>
    <div class="chip-row">
      ${(composition.sections || []).map((section) => chip(section)).join("")}
    </div>
    <div class="run-grid">
      ${(composition.citation_spans || []).map((span) => `
        <div class="graph-card">
          <strong>${escapeHtml(span.marker || span.asset_id || "")}</strong>
          <p class="metric-detail">${escapeHtml(span.section || "citation")}</p>
          <p class="metric-detail">${escapeHtml(span.quote || "")}</p>
        </div>
      `).join("")}
    </div>
  `;
}

async function updateDiff() {
  const runId = state.activeRunId;
  const from = document.getElementById("snapshot-from-select").value;
  const to = document.getElementById("snapshot-to-select").value;
  if (!runId || !from || !to) {
    document.getElementById("diff-summary").innerHTML = "";
    renderJson("diff-panel", { note: "Need at least two snapshots to diff." });
    return;
  }

  state.selectedSnapshotFrom = Number(from);
  state.selectedSnapshotTo = Number(to);

  const diff = await getJson(
    `/api/runs/${runId}/diff?snapshot_from=${state.selectedSnapshotFrom}&snapshot_to=${state.selectedSnapshotTo}`
  );
  const changedCount = Object.keys(diff.changed || {}).length;
  const addedCount = Object.keys(diff.added || {}).length;
  const removedCount = Object.keys(diff.removed || {}).length;
  document.getElementById("diff-summary").innerHTML = `
    <div class="detail-grid">
      ${detailTile("Changed", String(changedCount))}
      ${detailTile("Added", String(addedCount))}
      ${detailTile("Removed", String(removedCount))}
      ${detailTile("Range", `${diff.from} → ${diff.to}`)}
    </div>
  `;
  renderJson("diff-panel", diff);
}

function renderSnapshots() {
  const snapshots = state.snapshots;
  const rail = document.getElementById("snapshots-panel");
  const fromSelect = document.getElementById("snapshot-from-select");
  const toSelect = document.getElementById("snapshot-to-select");
  const forkRunId = currentSnapshotForkRunId();
  rail.innerHTML = "";
  fromSelect.innerHTML = "";
  toSelect.innerHTML = "";

  if (state.snapshotForkError) {
    rail.insertAdjacentHTML(
      "beforeend",
      `<p class="metric-detail">${escapeHtml(state.snapshotForkError)}</p>`
    );
  }
  if (!forkRunId) {
    rail.insertAdjacentHTML(
      "beforeend",
      `<p class="metric-detail">Forking is available when snapshots exist.</p>`
    );
  }

  for (const snapshot of snapshots) {
    const card = document.createElement("div");
    const active =
      snapshot.snapshot_id === state.selectedSnapshotFrom ||
      snapshot.snapshot_id === state.selectedSnapshotTo;
    card.className = `snapshot-item ${active ? "active" : ""}`;
    card.innerHTML = `
      <strong>${escapeHtml(snapshot.label || `snapshot ${snapshot.snapshot_id}`)}</strong>
      <p class="metric-detail">${escapeHtml(snapshot.branch || "branch")} · ${formatMs(snapshot.time_ms)}</p>
      <div class="chip-row">
        ${chip(`id=${snapshot.snapshot_id}`)}
        ${chip(`t=${formatMs(snapshot.time_ms)}`)}
      </div>
      ${
        forkRunId
          ? `<button class="fork-from-btn" data-snapshot-id="${snapshot.snapshot_id}">Fork from here</button>`
          : ""
      }
    `;
    rail.appendChild(card);
    card.querySelector(".fork-from-btn")?.addEventListener("click", async (e) => {
      e.stopPropagation();
      const snapId = parseInt(e.target.dataset.snapshotId, 10);
      try {
        const result = await requestMissionBranch(forkRunId, snapId);
        if (result?.run_id) {
          await activateMissionBranch(result);
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        state.snapshotForkError = `Fork failed: ${message}`;
        renderSnapshots();
      }
    });

    for (const select of [fromSelect, toSelect]) {
      const option = document.createElement("option");
      option.value = String(snapshot.snapshot_id);
      option.textContent = `${snapshot.snapshot_id} · ${snapshot.label || "snapshot"}`;
      select.appendChild(option);
    }
  }

  const availableIds = new Set(snapshots.map((item) => item.snapshot_id));
  if (!availableIds.has(state.selectedSnapshotFrom)) {
    state.selectedSnapshotFrom = null;
  }
  if (!availableIds.has(state.selectedSnapshotTo)) {
    state.selectedSnapshotTo = null;
  }

  if (snapshots.length >= 2) {
    if (state.selectedSnapshotFrom === null) {
      state.selectedSnapshotFrom = snapshots[0].snapshot_id;
    }
    if (state.selectedSnapshotTo === null) {
      state.selectedSnapshotTo = snapshots[snapshots.length - 1].snapshot_id;
    }
    fromSelect.value = String(state.selectedSnapshotFrom);
    toSelect.value = String(state.selectedSnapshotTo);
    void updateDiff();
  } else {
    state.selectedSnapshotFrom = null;
    state.selectedSnapshotTo = null;
    document.getElementById("diff-summary").innerHTML = "";
    renderJson("diff-panel", { note: "Need at least two snapshots to diff." });
  }
}

function setSelectedEvent(index) {
  if (!state.timeline.length) {
    state.selectedEventIndex = 0;
  } else {
    state.selectedEventIndex = Math.max(0, Math.min(index, state.timeline.length - 1));
  }
  renderPlaybackStage();
  renderEventDetail();
}

function stopPlayback() {
  if (state.playbackTimer) {
    window.clearInterval(state.playbackTimer);
    state.playbackTimer = null;
  }
  document.getElementById("playback-toggle").textContent = "Play";
}

function startPlayback() {
  if (!state.timeline.length) {
    return;
  }
  stopPlayback();
  document.getElementById("playback-toggle").textContent = "Pause";
  state.playbackTimer = window.setInterval(() => {
    if (state.selectedEventIndex >= state.timeline.length - 1) {
      stopPlayback();
      return;
    }
    setSelectedEvent(state.selectedEventIndex + 1);
  }, 900);
}

function togglePlayback() {
  if (state.playbackTimer) {
    stopPlayback();
  } else {
    startPlayback();
  }
}

async function loadWorkspace() {
  const previousHistoricalFocusKey = state.historicalAutoFocusKey || "";
  const [workspace, storyArtifacts, playableArtifacts, scenarios, importSummary, identityFlow, importSources, importNormalization, importReview, generatedImportScenarios, provenanceIndex, governorWorkspace, historicalWorkspace, whatIfStatus, publicDemoStatus] = await Promise.all([
    getJson("/api/workspace"),
    fetchStoryArtifacts(),
    fetchPlayableArtifacts(),
    getJson("/api/scenarios"),
    getJson("/api/imports/summary").catch(() => ({})),
    getJson("/api/identity/flow").catch(() => ({})),
    getJson("/api/imports/sources").catch(() => ({ sources: [], syncs: [] })),
    getJson("/api/imports/normalization").catch(() => ({})),
    getJson("/api/imports/review").catch(() => ({})),
    getJson("/api/imports/scenarios").catch(() => []),
    getJson("/api/imports/provenance").catch(() => []),
    getJson("/api/workspace/governor").catch(() => ({})),
    getJson("/api/workspace/historical").catch(() => ({})),
    getJson("/api/workspace/whatif").catch((error) => ({
      available: false,
      unavailable_reason: "fetch_error",
      error: error?.message || String(error),
    })),
    getJson("/api/workspace/public-demo").catch((error) => ({
      available: false,
      unavailable_reason: "fetch_error",
      error: error?.message || String(error),
    })),
  ]);
  state.workspace = workspace;
  applyGovernorWorkspaceStatus(governorWorkspace);
  state.historicalWorkspace = nonEmptyPayload(historicalWorkspace);
  state.story = nonEmptyPayload(storyArtifacts.story);
  state.exportsPreview = storyArtifacts.exportsPreview;
  state.presentation =
    nonEmptyPayload(storyArtifacts.presentation) ||
    nonEmptyPayload(storyArtifacts.story?.presentation) ||
    null;
  state.playableBundle = nonEmptyPayload(playableArtifacts.playableBundle);
  state.missions = Array.isArray(playableArtifacts.missions)
    ? playableArtifacts.missions
    : [];
  state.missionState = nonEmptyPayload(playableArtifacts.missionState);
  state.fidelityReport = nonEmptyPayload(playableArtifacts.fidelityReport);
  state.scenarios = scenarios;
  state.importSummary = importSummary;
  state.identityFlow = identityFlow;
  state.importSources = importSources;
  state.importNormalization = importNormalization;
  state.importReview = importReview;
  state.generatedImportScenarios = generatedImportScenarios;
  state.provenanceIndex = provenanceIndex;
  state.whatIfStatus = whatIfStatus;
  state.publicDemoStatus = publicDemoStatus;
  const evalProviderInput = document.getElementById("eval-provider-input");
  if (evalProviderInput && state.whatIfStatus?.default_provider) {
    const currentValue = evalProviderInput.value?.trim() || "";
    if (!currentValue || currentValue === "openai") {
      evalProviderInput.value = state.whatIfStatus.default_provider;
    }
  }
  renderWorkspaceHero();
  renderImportSummary();
  renderExportsPanel();
  renderScenarioSelector();
  renderMissionSelector();
  renderMissionSummary();
  renderMissionPlay();
  renderFidelityPanel();
  if (typeof renderWhatIfStudio === "function") {
    renderWhatIfStudio();
  }
  if (typeof renderPublicDemo === "function") {
    renderPublicDemo();
  }
  if (typeof primeWhatIfSceneFromHistoricalWorkspace === "function") {
    void primeWhatIfSceneFromHistoricalWorkspace();
  }
  if (scenarios.length > 0) {
    const activeName = workspace?.manifest?.active_scenario || scenarios[0].name;
    document.getElementById("scenario-select").value = activeName;
    await loadScenario(activeName);
  }
  const historicalFocusKey = state.historicalWorkspace?.branch_event_id || "";
  if (!historicalFocusKey) {
    state.historicalAutoFocusKey = "";
  } else if (historicalFocusKey !== previousHistoricalFocusKey) {
    state.historicalAutoFocusKey = historicalFocusKey;
    jumpToCompanySection("company-historical", { behavior: "auto" });
  }
}

async function loadScenario(name) {
  const [preview, contract] = await Promise.all([
    getJson(`/api/scenarios/${name}/preview`),
    getJson(`/api/scenarios/${name}/contract`),
  ]);
  state.scenarioPreview = preview;
  state.scenarioContract = contract;
  renderWorkspaceHero();
  renderMissionSelector();
  renderMissionSummary();
  renderMissionPlay();
  renderLivingCompanyView();
  renderScenarioBriefing();
}

async function activateScenarioVariant(name) {
  if (!name) {
    return;
  }
  await getJson("/api/scenarios/activate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ variant: name, bootstrap_contract: true }),
  });
  await loadWorkspace();
  await loadRuns();
}

async function activateContractVariant(name) {
  if (!name) {
    return;
  }
  await getJson("/api/contract-variants/activate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ variant: name }),
  });
  await loadWorkspace();
  await loadRuns();
}

async function activateMission(name, objectiveVariant = null) {
  if (!name) {
    return;
  }
  const status = document.getElementById("mission-form-status");
  status.textContent = `Loading scenario ${name}\u2026`;
  state.lastMoveImpact = null;
  try {
    await getJson("/api/missions/activate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        mission_name: name,
        objective_variant: objectiveVariant || undefined,
      }),
    });
    await loadWorkspace();
    status.textContent = `Scenario ${name} is ready.`;
    setStudioView("crisis");
  } catch (error) {
    status.textContent = `Could not activate scenario: ${error?.message || error}`;
  }
}

async function startMission() {
  const missionSelect = document.getElementById("mission-select");
  const objectiveSelect = document.getElementById("objective-select");
  const status = document.getElementById("mission-form-status");
  const missionName = missionSelect?.value;
  const objectiveVariant = objectiveSelect?.value || null;
  if (hasExerciseMode()) {
    status.textContent = "Applying crisis\u2026";
    state.lastMoveImpact = null;
    try {
      await getJson("/api/workspace/governor/exercise/activate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          scenario_variant: missionName || undefined,
          contract_variant: objectiveVariant || undefined,
        }),
      });
      await loadWorkspace();
      await loadRuns();
      status.textContent = "Company pressure updated.";
      setStudioView("company");
    } catch (error) {
      status.textContent = `Could not update situation: ${error?.message || error}`;
    }
    return;
  }
  status.textContent = "Starting scenario\u2026";
  state.lastMoveImpact = null;
  try {
    const payload = await getJson("/api/missions/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        mission_name: missionName || undefined,
        objective_variant: objectiveVariant || undefined,
      }),
    });
    state.missionState = payload;
    await loadRuns({ selectActiveRun: false });
    if (payload.run_id) {
      await selectRun(payload.run_id, { previousSurfaceState: null });
    }
    state.missionState = payload;
    renderMissionPlay();
    status.textContent = `Scenario is live: ${payload.mission?.title || payload.run_id}.`;
    setStudioView("company");
  } catch (error) {
    status.textContent = `Could not start scenario: ${error?.message || error}`;
  }
}

function showPolicyChangeModal(move) {
  let existing = document.getElementById("policy-change-modal");
  if (existing) existing.remove();

  const graphAction = move.graph_action || {};
  const args = graphAction.args || {};
  const currentPolicy = {};
  const proposedPolicy = {};

  const ss = state.surfaceState;
  if (ss && Array.isArray(ss.panels)) {
    const svcPanel = ss.panels.find((p) => p.kind === "vertical_heartbeat" || p.surface === "service_ops");
    if (svcPanel && svcPanel.policy) {
      Object.assign(currentPolicy, svcPanel.policy);
    }
  }

  Object.entries(args).forEach(([key, value]) => {
    if (key !== "note") {
      currentPolicy[key] = currentPolicy[key] !== undefined ? currentPolicy[key] : "—";
      proposedPolicy[key] = value;
    }
  });

  const policyRows = Object.keys({ ...currentPolicy, ...proposedPolicy })
    .filter((k) => proposedPolicy[k] !== undefined)
    .map((key) => {
      const cur = currentPolicy[key] !== undefined ? String(currentPolicy[key]) : "—";
      const next = String(proposedPolicy[key]);
      const changed = cur !== next;
      return `<tr class="${changed ? "policy-row-changed" : ""}">
        <td>${escapeHtml(key.replace(/_/g, " "))}</td>
        <td>${escapeHtml(cur)}</td>
        <td>${changed ? escapeHtml(next) : "—"}</td>
      </tr>`;
    })
    .join("");

  const modal = document.createElement("div");
  modal.id = "policy-change-modal";
  modal.className = "policy-modal-overlay";
  modal.innerHTML = `
    <div class="policy-modal">
      <div class="policy-modal-header">
        <span class="policy-modal-badge">Policy Override</span>
        <h3>${escapeHtml(move.title)}</h3>
      </div>
      <p class="policy-modal-consequence">${escapeHtml(move.consequence_preview || move.summary || "")}</p>
      <table class="policy-diff-table">
        <thead><tr><th>Policy</th><th>Current</th><th>Proposed</th></tr></thead>
        <tbody>${policyRows}</tbody>
      </table>
      <div class="policy-modal-actions">
        <button type="button" class="ghost-button" id="policy-modal-cancel">Cancel</button>
        <button type="button" class="ghost-button policy-modal-confirm" id="policy-modal-confirm">Confirm override</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  document.getElementById("policy-modal-cancel").addEventListener("click", () => modal.remove());
  modal.addEventListener("click", (e) => { if (e.target === modal) modal.remove(); });
  document.getElementById("policy-modal-confirm").addEventListener("click", () => {
    modal.remove();
    void applyMissionMove(move.move_id);
  });
}

async function onTryDifferentPolicyClick() {
  if (!state.missionState?.run_id) return;
  try {
    const bundle = await getJson(`/api/runs/${encodeURIComponent(state.missionState.run_id)}/policy-knobs`);
    showPolicyReplayModal(bundle);
  } catch (error) {
    const status = document.getElementById("mission-form-status");
    if (status) status.textContent = `Policy replay failed: ${error}`;
  }
}

function showPolicyReplayModal(bundle) {
  let existing = document.getElementById("policy-replay-modal");
  if (existing) existing.remove();
  const knobs = Array.isArray(bundle?.knobs) ? bundle.knobs : [];
  if (!knobs.length) return;

  const rows = knobs.map((knob) => {
    const inputId = `policy-replay-${knob.field}`;
    if (knob.value_type === "boolean") {
      return `
        <label class="policy-replay-row" for="${escapeHtml(inputId)}">
          <span class="policy-replay-copy"><strong>${escapeHtml(knob.label)}</strong><em>${escapeHtml(knob.description || "")}</em></span>
          <select id="${escapeHtml(inputId)}" data-field="${escapeHtml(knob.field)}" data-value-type="${escapeHtml(knob.value_type)}" class="compare-run-picker">
            <option value="true" ${knob.value ? "selected" : ""}>On</option>
            <option value="false" ${!knob.value ? "selected" : ""}>Off</option>
          </select>
        </label>
      `;
    }
    return `
      <label class="policy-replay-row" for="${escapeHtml(inputId)}">
        <span class="policy-replay-copy"><strong>${escapeHtml(knob.label)}</strong><em>${escapeHtml(knob.description || "")}</em></span>
        <input id="${escapeHtml(inputId)}" class="policy-replay-input" data-field="${escapeHtml(knob.field)}" data-value-type="${escapeHtml(knob.value_type)}" value="${escapeHtml(knob.value)}" />
      </label>
    `;
  }).join("");

  const modal = document.createElement("div");
  modal.id = "policy-replay-modal";
  modal.className = "policy-modal-overlay";
  modal.innerHTML = `
    <div class="policy-modal policy-replay-modal">
      <div class="policy-modal-header">
        <span class="policy-modal-badge">What-If Replay</span>
        <h3>Try different policy</h3>
      </div>
      <p class="policy-modal-consequence">Change the named service-ops policy knobs, replay from the same starting point, and compare the new outcome side by side.</p>
      <div class="policy-replay-form">${rows}</div>
      <div class="policy-modal-actions">
        <button type="button" class="ghost-button" id="policy-replay-cancel">Cancel</button>
        <button type="button" class="ghost-button policy-modal-confirm" id="policy-replay-confirm">Replay path</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
  document.getElementById("policy-replay-cancel").addEventListener("click", () => modal.remove());
  modal.addEventListener("click", (event) => {
    if (event.target === modal) modal.remove();
  });
  document.getElementById("policy-replay-confirm").addEventListener("click", async () => {
    const status = document.getElementById("mission-form-status");
    const policyDelta = {};
    modal.querySelectorAll("[data-field]").forEach((node) => {
      const field = node.dataset.field;
      const valueType = node.dataset.valueType;
      let value = node.value;
      if (valueType === "boolean") value = value === "true";
      if (valueType === "integer") value = Number.parseInt(value, 10);
      if (valueType === "number") value = Number.parseFloat(value);
      policyDelta[field] = value;
    });
    modal.remove();
    if (status) status.textContent = "Replaying the path with updated policy\u2026";
    try {
      const replay = await governorPost(
        `/api/runs/${encodeURIComponent(state.missionState.run_id)}/replay-with-policy`,
        { policy_delta: policyDelta }
      );
      await loadRuns({ selectActiveRun: false });
      const runs = state.runs || [];
      const original = runs.find((run) => run.run_id === replay.source_run_id) || { run_id: replay.source_run_id };
      const replayed = runs.find((run) => run.run_id === replay.replay_run_id) || { run_id: replay.replay_run_id };
      await loadCompareRunData(original, replayed);
      state.compareSnapshotA = replay.source_snapshot_id || state.compareSnapshotA;
      state.compareSnapshotB = replay.replay_snapshot_id || state.compareSnapshotB;
      state.compareMode = true;
      if (!state.timelineMode) toggleTimelineMode();
      renderTimelineView();
      setStudioView("outcome");
      if (status) status.textContent = "Replay complete. Compare the original path against the new policy run.";
    } catch (error) {
      if (status) status.textContent = `Policy replay failed: ${error}`;
    }
  });
}

async function applyMissionMove(moveId) {
  if (!moveId || !state.missionState?.run_id) {
    return;
  }
  const previousSurfaceState = state.surfaceState;
  const status = document.getElementById("mission-form-status");
  const moveSpec = (state.missionState.available_moves || []).find((m) => m.move_id === moveId);
  const moveTitle = moveSpec?.title || moveId;
  status.textContent = `Playing ${moveTitle}\u2026`;
  try {
    const payload = await getJson(
      `/api/missions/${state.missionState.run_id}/moves/${encodeURIComponent(moveId)}`,
      {
        method: "POST",
      }
    );
    state.missionState = payload;
    await loadRuns({ selectActiveRun: false });

    const oldSurface = previousSurfaceState;
    await refreshActiveRun(payload.run_id, { previousSurfaceState: oldSurface });
    state.missionState = payload;

    const diff = diffSurfaceState(oldSurface, state.surfaceState);
    const impact = buildMoveImpact(moveSpec, diff);
    const impactPanels = impact.systems || [];
    state.lastMoveImpact = impactPanels.length ? impact : null;

    if (!diff.panels.length && impactPanels.length) {
      setSurfaceHighlights({ panels: impactPanels, refs: impact.refs || [] });
    }

    if (impactPanels.length > 0) {
      if (!state.timelineMode) {
        state.cascadeActive = true;
        renderSurfaceWall();
        await playCascade(impactPanels, impact.refs || []);
      }
    }
    renderLivingCompanyView();
    renderMissionPlay();

    status.textContent = payload.status === "completed"
      ? "Run completed. Review the Outcome tab or branch from a snapshot."
      : impact.summary
        ? `${moveTitle} \u2014 ${impact.summary}`
        : `${moveTitle} \u2014 ${impactPanels.length} system${impactPanels.length === 1 ? "" : "s"} hit.`;
  } catch (error) {
    status.textContent = `Move failed: ${error?.message || error}`;
  }
}

async function branchMission() {
  if (!state.missionState?.run_id) {
    return;
  }
  const status = document.getElementById("mission-form-status");
  status.textContent = "Creating branch...";
  state.lastMoveImpact = null;
  try {
    const payload = await requestMissionBranch(state.missionState.run_id);
    await activateMissionBranch(payload);
    status.textContent = `Branch ${payload.branch_name} is live.`;
    setStudioView("outcome");
  } catch (error) {
    status.textContent = `Could not create branch: ${error?.message || error}`;
  }
}

async function finishMission() {
  if (!state.missionState?.run_id) {
    return;
  }
  const status = document.getElementById("mission-form-status");
  status.textContent = "Ending run\u2026";
  state.lastMoveImpact = null;
  try {
    const payload = await getJson(`/api/missions/${state.missionState.run_id}/finish`, {
      method: "POST",
    });
    state.missionState = payload;
    await loadRuns({ selectActiveRun: false });
    await selectRun(payload.run_id, { previousSurfaceState: state.surfaceState });
    state.missionState = payload;
    status.textContent = payload.scorecard?.mission_success
      ? "Run ended successfully."
      : "Run ended with remaining risk.";
    setStudioView("outcome");
  } catch (error) {
    status.textContent = `Could not end run: ${error?.message || error}`;
  }
}

async function loadRuns({ selectActiveRun = true } = {}) {
  state.runs = await getJson("/api/runs");
  await refreshStoryArtifacts();
  await refreshPlayableArtifacts();
  renderWorkspaceMetrics();
  renderRuns();
  if (!selectActiveRun) {
    return;
  }
  if (state.missionState?.run_id) {
    state.activeRunId = state.missionState.run_id;
  }
  if (!state.activeRunId && state.runs.length > 0) {
    await selectRun(state.runs[0].run_id);
    return;
  }
  if (state.activeRunId) {
    await selectRun(state.activeRunId);
  }
}

async function refreshActiveRun(
  runId,
  { connectStream = false, previousSurfaceState = null, preserveSurfaceHighlights = false } = {}
) {
  const generation = ++state.refreshGeneration;
  const [run, timeline, orientation, graphs, knowledgeStore, snapshots, contract, surfaces, governorWorkspace] = await Promise.all([
    getJson(`/api/runs/${runId}`),
    getJson(`/api/runs/${runId}/timeline`),
    getJson(`/api/runs/${runId}/orientation`),
    getJson(`/api/runs/${runId}/graphs`),
    getJson(`/api/runs/${runId}/knowledge`).catch(() => null),
    getJson(`/api/runs/${runId}/snapshots`),
    getJson(`/api/runs/${runId}/contract`),
    getJson(`/api/runs/${runId}/surfaces`).catch(() => null),
    getJson("/api/workspace/governor").catch(() => null),
  ]);

  if (generation !== state.refreshGeneration) return;

  state.activeRun = run;
  state.timeline = timeline;
  state.orientation = orientation;
  state.graphs = graphs;
  state.knowledgeStore = knowledgeStore;
  state.surfaceState = surfaces;
  setSurfaceHighlights(diffSurfaceState(previousSurfaceState, surfaces), {
    preserveExisting: preserveSurfaceHighlights,
  });
  state.snapshots = snapshots;
  state.activeRunContract = contract;
  applyGovernorWorkspaceStatus(governorWorkspace);
  await refreshStoryArtifacts();
  await refreshPlayableArtifacts();
  if (state.selectedEventIndex >= timeline.length) {
    state.selectedEventIndex = Math.max(0, timeline.length - 1);
  }

  renderRunHeader();
  renderRunSummary();
  renderLivingCompanyView();
  renderPlaybackStage();
  renderEventDetail();
  renderOrientation();
  renderGraphs();
  renderKnowledge();
  renderSnapshots();
  renderImportSummary();

  if (connectStream) {
    connectRunStream(runId);
  }
}
