const CHANNEL_COLORS = {
  Plan: "#7b5bff",
  Slack: "#36c5f0",
  Mail: "#ffb454",
  Browser: "#9b7bff",
  Docs: "#1aa88d",
  Tickets: "#ff6d5e",
  CRM: "#1e6cf2",
  World: "#7d8794",
  Help: "#ff5caa",
  Misc: "#94a3b8",
};

const state = {
  workspace: null,
  scenarios: [],
  scenarioPreview: null,
  scenarioContract: null,
  importSummary: null,
  importNormalization: null,
  generatedImportScenarios: [],
  provenanceIndex: [],
  selectedObjectRef: null,
  runs: [],
  activeRunId: null,
  activeRun: null,
  activeRunContract: null,
  timeline: [],
  orientation: null,
  graphs: null,
  snapshots: [],
  selectedEventIndex: 0,
  selectedSnapshotFrom: null,
  selectedSnapshotTo: null,
  playbackTimer: null,
  eventSource: null,
};

async function getJson(path, options = {}) {
  const response = await fetch(path, options);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return await response.json();
}

function renderJson(id, payload) {
  document.getElementById(id).textContent = JSON.stringify(payload, null, 2);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function compactNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "0";
  }
  return Intl.NumberFormat(undefined, { notation: "compact" }).format(Number(value));
}

function formatMs(value) {
  const ms = Number(value || 0);
  if (ms < 1000) {
    return `${ms}ms`;
  }
  if (ms < 60000) {
    return `${(ms / 1000).toFixed(1)}s`;
  }
  return `${(ms / 60000).toFixed(1)}m`;
}

function statusClass(value) {
  const normalized = String(value || "").toLowerCase();
  if (["ok", "true", "passed", "success"].includes(normalized)) {
    return "ok";
  }
  if (["error", "failed", "false"].includes(normalized)) {
    return "error";
  }
  if (["running", "queued"].includes(normalized)) {
    return "running";
  }
  return "";
}

function badge(label, value, className = "") {
  return `<span class="badge ${className}">${escapeHtml(label)}${value ? ` · ${escapeHtml(value)}` : ""}</span>`;
}

function chip(value, className = "") {
  return `<span class="chip ${className}">${escapeHtml(value)}</span>`;
}

function metricTile(label, value, detail = "") {
  return `
    <div class="metric-tile">
      <span class="metric-label">${escapeHtml(label)}</span>
      <span class="metric-value">${escapeHtml(value)}</span>
      ${detail ? `<span class="metric-detail">${escapeHtml(detail)}</span>` : ""}
    </div>
  `;
}

function scorePill(label, value, detail = "") {
  return `
    <div class="score-pill">
      <span class="metric-label">${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
      ${detail ? `<span class="metric-detail">${escapeHtml(detail)}</span>` : ""}
    </div>
  `;
}

function detailTile(label, value) {
  return `
    <div class="detail-tile">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(value)}</span>
    </div>
  `;
}

function keyObjectCard(item) {
  return `
    <div class="stack-card">
      <h3>${escapeHtml(item.title || item.object_id || item.kind)}</h3>
      <div class="chip-row">
        ${chip(item.domain || "domain")}
        ${chip(item.kind || "object")}
        ${chip(item.status || "status")}
      </div>
      ${item.reason ? `<p class="metric-detail">${escapeHtml(item.reason)}</p>` : ""}
    </div>
  `;
}

function summarizeGraph(graph) {
  const stats = [];
  for (const [key, value] of Object.entries(graph || {})) {
    if (Array.isArray(value)) {
      stats.push([key, value.length]);
    } else if (value && typeof value === "object") {
      stats.push([key, Object.keys(value).length]);
    } else if (value !== null && value !== undefined) {
      stats.push([key, value]);
    }
  }
  return stats.slice(0, 6);
}

function renderWorkspaceMetrics() {
  const workspace = state.workspace;
  const panel = document.getElementById("workspace-metrics");
  if (!workspace) {
    panel.innerHTML = "";
    return;
  }
  const manifest = workspace.manifest || {};
  const latestRun = state.runs[0];
  panel.innerHTML = [
    metricTile("Workspace", manifest.title || manifest.name || "Workspace", manifest.source_kind || "template"),
    metricTile("Scenarios", String((manifest.scenarios || []).length), `active: ${manifest.active_scenario || "default"}`),
    metricTile("Runs", String(workspace.run_count || 0), latestRun ? `latest: ${latestRun.run_id}` : "no runs yet"),
    metricTile("Contracts", String((workspace.compiled_scenarios || []).length), "compiled"),
  ].join("");
}

function renderWorkspaceHero() {
  const workspace = state.workspace;
  if (!workspace) {
    return;
  }
  const manifest = workspace.manifest || {};
  document.getElementById("workspace-subtitle").textContent =
    `${manifest.description || "Workspace ready."} ${workspace.run_count ? `This workspace has ${workspace.run_count} recorded run${workspace.run_count === 1 ? "" : "s"}.` : "Launch a run to start building a playback history."}`;
  renderWorkspaceMetrics();
  renderJson("workspace-panel", workspace);
}

function renderImportSummary() {
  const summary = state.importSummary;
  const normalization = state.importNormalization;
  const panel = document.getElementById("imports-summary");
  const generatedPanel = document.getElementById("generated-scenarios");
  const provenancePanel = document.getElementById("provenance-detail");
  const hasImportPackage =
    summary &&
    normalization &&
    Object.keys(summary).length > 0 &&
    Object.keys(normalization).length > 0 &&
    (summary.package_name || normalization.package_name);
  if (!hasImportPackage) {
    panel.innerHTML = `<div class="metric-tile"><span class="metric-label">Import</span><span class="metric-value">No import package</span></div>`;
    generatedPanel.innerHTML = "";
    provenancePanel.innerHTML = "";
    renderJson("imports-panel", {});
    renderJson("provenance-panel", []);
    return;
  }

  panel.innerHTML = [
    metricTile("Package", summary.package_name || "import", `${summary.source_count || 0} sources`),
    metricTile("Issues", String(summary.issue_count || 0), `${summary.warning_count || 0} warnings · ${summary.error_count || 0} errors`),
    metricTile("Provenance", String(summary.provenance_count || 0), `generated scenarios: ${summary.generated_scenario_count || 0}`),
    metricTile(
      "Origins",
      `${summary.origin_counts?.imported || 0}/${summary.origin_counts?.derived || 0}/${summary.origin_counts?.simulated || 0}`,
      "imported / derived / simulated"
    ),
  ].join("");

  generatedPanel.innerHTML = state.generatedImportScenarios
    .map(
      (scenario) => `
        <div class="run-item">
          <div class="chip-row">
            ${chip(scenario.workflow_name)}
            ${chip(scenario.workflow_variant || "default")}
          </div>
          <h3>${escapeHtml(scenario.title)}</h3>
          <p class="metric-detail">${escapeHtml(scenario.description)}</p>
          <div class="chip-row">${(scenario.tags || []).slice(0, 4).map((item) => chip(item)).join("")}</div>
        </div>
      `
    )
    .join("");

  const selectedRefs = state.timeline[state.selectedEventIndex]?.object_refs || [];
  const selectedRecord = state.provenanceIndex.find((item) => item.object_ref === state.selectedObjectRef) || state.provenanceIndex.find((item) => selectedRefs.includes(item.object_ref));
  if (selectedRecord) {
    provenancePanel.innerHTML = `
      <div class="detail-grid">
        ${detailTile("Object", selectedRecord.object_ref)}
        ${detailTile("Origin", selectedRecord.origin)}
        ${detailTile("Source", selectedRecord.source_system || selectedRecord.source_id || "derived")}
        ${detailTile("Label", selectedRecord.label || "object")}
      </div>
      <div class="chip-row">${(selectedRecord.lineage || []).map((item) => chip(item)).join("")}</div>
    `;
    renderJson("provenance-panel", [selectedRecord]);
  } else {
    provenancePanel.innerHTML = `
      <div class="stack-card">
        <h3>Provenance drilldown</h3>
        <p class="metric-detail">Select a run event with object references to inspect imported, derived, or simulated lineage.</p>
      </div>
    `;
    renderJson("provenance-panel", state.provenanceIndex);
  }

  renderJson("imports-panel", normalization);
}

function renderScenarioSelector() {
  const select = document.getElementById("scenario-select");
  select.innerHTML = "";
  for (const scenario of state.scenarios) {
    const option = document.createElement("option");
    option.value = scenario.name;
    option.textContent = scenario.title;
    select.appendChild(option);
  }
}

function renderScenarioBriefing() {
  const preview = state.scenarioPreview;
  const contract = state.scenarioContract;
  const panel = document.getElementById("scenario-briefing");
  if (!preview || !contract) {
    panel.innerHTML = `<div class="metric-tile"><span class="metric-label">Scenario</span><span class="metric-value">Loading</span></div>`;
    return;
  }

  const scenario = preview.scenario || {};
  const compiled = preview.compiled_blueprint || {};
  const facadeLabels = (compiled.facades || [])
    .map((item) => {
      if (typeof item === "string") {
        return item;
      }
      if (item && typeof item === "object") {
        return item.name || item.facade_name || item.domain || item.label || "facade";
      }
      return "facade";
    })
    .slice(0, 4)
    .join(", ");
  const contractSummary = [
    metricTile("Scenario", scenario.title || scenario.name || "Scenario", scenario.scenario_name || ""),
    metricTile("Workflow", compiled.workflow_name || contract.workflow_name || "n/a", compiled.workflow_variant || "default"),
    metricTile("Facades", String((compiled.facades || []).length), facadeLabels),
    metricTile("Focus", scenario.inspection_focus || "summary", (scenario.tags || []).join(", ")),
  ].join("");
  panel.innerHTML = contractSummary;

  const successCount = Array.isArray(contract.success_predicates)
    ? contract.success_predicates.length
    : Number(contract.success_predicate_count || 0);
  const forbiddenCount = Array.isArray(contract.forbidden_predicates)
    ? contract.forbidden_predicates.length
    : Number(contract.forbidden_predicate_count || 0);
  const invariants = Array.isArray(contract.policy_invariants)
    ? contract.policy_invariants.length
    : Number(contract.policy_invariant_count || 0);
  document.getElementById("contract-scorecard").innerHTML = `
    <div class="score-strip">
      ${scorePill("Success predicates", String(successCount))}
      ${scorePill("Forbidden predicates", String(forbiddenCount))}
      ${scorePill("Policy invariants", String(invariants))}
      ${scorePill("Allowed tools", String(contract.observation_boundary?.allowed_tools?.length || contract.metadata?.observation_boundary?.allowed_tools?.length || 0))}
    </div>
  `;

  renderJson("scenario-panel", preview);
}

function renderRuns() {
  const panel = document.getElementById("runs-panel");
  panel.innerHTML = "";
  for (const run of state.runs) {
    const card = document.createElement("div");
    card.className = `run-item ${state.activeRunId === run.run_id ? "active" : ""}`;
    card.innerHTML = `
      <div class="chip-row">
        ${chip(run.runner)}
        ${chip(run.status, statusClass(run.status))}
        ${run.success === null ? "" : chip(`success=${run.success}`, statusClass(run.success))}
      </div>
      <h3>${escapeHtml(run.run_id)}</h3>
      <p class="metric-detail">${escapeHtml(run.scenario_name)} · ${escapeHtml(run.workflow_variant || run.workflow_name || "no workflow")}</p>
      <div class="detail-grid">
        ${detailTile("Steps", compactNumber(run.diagnostics?.workflow_step_count || run.metrics?.actions || 0))}
        ${detailTile("Contract", run.contract?.ok === null ? "pending" : run.contract?.ok ? "pass" : "fail")}
      </div>
    `;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "ghost-button";
    button.textContent = "Open";
    button.onclick = () => selectRun(run.run_id);
    card.appendChild(button);
    panel.appendChild(card);
  }
}

function renderRunHeader() {
  const run = state.activeRun;
  const title = document.getElementById("active-run-title");
  const badges = document.getElementById("active-run-badges");
  if (!run) {
    title.textContent = "No run selected";
    badges.innerHTML = "";
    return;
  }
  title.textContent = `${run.run_id} · ${run.scenario_name}`;
  badges.innerHTML = [
    badge(run.runner, null),
    badge(run.status, null, statusClass(run.status)),
    badge(run.contract?.ok ? "contract pass" : run.contract?.ok === false ? "contract fail" : "contract pending", null, statusClass(run.contract?.ok)),
    run.branch ? badge("branch", run.branch) : "",
  ].join("");
}

function renderRunSummary() {
  const run = state.activeRun;
  const contract = state.activeRunContract;
  if (!run) {
    return;
  }
  const panel = document.getElementById("contract-scorecard");
  const successPassed = run.contract?.success_assertions_passed || 0;
  const successTotal = run.contract?.success_assertion_count || 0;
  const issueCount = run.contract?.issue_count || contract?.dynamic_validation?.issues?.length || 0;
  const policyFails = contract?.policy_invariants_failed || 0;
  panel.innerHTML = `
    <div class="score-strip">
      ${scorePill("Contract", run.contract?.ok === null ? "pending" : run.contract?.ok ? "pass" : "fail", run.contract?.contract_name || "workspace contract")}
      ${scorePill("Assertions", `${successPassed}/${successTotal}`)}
      ${scorePill("Issues", String(issueCount))}
      ${scorePill("Policy failures", String(policyFails))}
      ${scorePill("Latency p95", formatMs(run.metrics?.latency_p95_ms || 0))}
      ${scorePill("Virtual time", formatMs(run.metrics?.time_ms || 0))}
    </div>
  `;
  renderJson("run-panel", run);
  renderJson("contract-panel", contract || { note: "No run contract evaluation yet." });
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
    panel.innerHTML = `<div class="stack-card"><h3>No events yet</h3><p class="metric-detail">Launch or open a run to get a playback trace.</p></div>`;
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
        event.graph_action_ref ? `graph=${event.graph_action_ref}` : null,
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
      return `
        <div class="graph-card">
          <h3>${escapeHtml(domain)}</h3>
          <div class="graph-stats">
            ${stats.map(([label, value]) => `<span><strong>${escapeHtml(label)}</strong><em>${escapeHtml(String(value))}</em></span>`).join("")}
          </div>
        </div>
      `;
    })
    .join("");

  renderJson("graphs-raw-panel", graphs);
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
  rail.innerHTML = "";
  fromSelect.innerHTML = "";
  toSelect.innerHTML = "";

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
    `;
    rail.appendChild(card);

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
  const [workspace, scenarios, importSummary, importNormalization, generatedImportScenarios, provenanceIndex] = await Promise.all([
    getJson("/api/workspace"),
    getJson("/api/scenarios"),
    getJson("/api/imports/summary").catch(() => ({})),
    getJson("/api/imports/normalization").catch(() => ({})),
    getJson("/api/imports/scenarios").catch(() => []),
    getJson("/api/imports/provenance").catch(() => []),
  ]);
  state.workspace = workspace;
  state.scenarios = scenarios;
  state.importSummary = importSummary;
  state.importNormalization = importNormalization;
  state.generatedImportScenarios = generatedImportScenarios;
  state.provenanceIndex = provenanceIndex;
  renderWorkspaceHero();
  renderImportSummary();
  renderScenarioSelector();
  if (scenarios.length > 0) {
    await loadScenario(scenarios[0].name);
  }
}

async function loadScenario(name) {
  const [preview, contract] = await Promise.all([
    getJson(`/api/scenarios/${name}/preview`),
    getJson(`/api/scenarios/${name}/contract`),
  ]);
  state.scenarioPreview = preview;
  state.scenarioContract = contract;
  renderScenarioBriefing();
}

async function loadRuns() {
  state.runs = await getJson("/api/runs");
  renderWorkspaceMetrics();
  renderRuns();
  if (!state.activeRunId && state.runs.length > 0) {
    await selectRun(state.runs[0].run_id);
  }
}

async function refreshActiveRun(runId, { connectStream = false } = {}) {
  const [run, timeline, orientation, graphs, snapshots, contract] = await Promise.all([
    getJson(`/api/runs/${runId}`),
    getJson(`/api/runs/${runId}/timeline`),
    getJson(`/api/runs/${runId}/orientation`),
    getJson(`/api/runs/${runId}/graphs`),
    getJson(`/api/runs/${runId}/snapshots`),
    getJson(`/api/runs/${runId}/contract`),
  ]);

  state.activeRun = run;
  state.timeline = timeline;
  state.orientation = orientation;
  state.graphs = graphs;
  state.snapshots = snapshots;
  state.activeRunContract = contract;
  if (state.selectedEventIndex >= timeline.length) {
    state.selectedEventIndex = Math.max(0, timeline.length - 1);
  }

  renderRunHeader();
  renderRunSummary();
  renderPlaybackStage();
  renderEventDetail();
  renderOrientation();
  renderGraphs();
  renderSnapshots();
  renderImportSummary();

  if (connectStream) {
    connectRunStream(runId);
  }
}

function connectRunStream(runId) {
  if (state.eventSource) {
    state.eventSource.close();
  }
  state.eventSource = new EventSource(`/api/runs/${runId}/stream`);
  state.eventSource.onmessage = () => {
    void refreshActiveRun(runId);
  };
  state.eventSource.onerror = () => {
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
  };
}

async function selectRun(runId) {
  state.activeRunId = runId;
  renderRuns();
  await refreshActiveRun(runId, { connectStream: true });
}

async function startRun(event) {
  event.preventDefault();
  const runner = document.getElementById("runner-select").value;
  const payload = {
    scenario_name: document.getElementById("scenario-select").value,
    runner,
    provider: document.getElementById("provider-input").value || null,
    model: document.getElementById("model-input").value || null,
    task: document.getElementById("task-input").value || null,
    max_steps: Number(document.getElementById("max-steps-input").value || 12),
  };
  const status = document.getElementById("run-form-status");
  status.textContent = "Starting run...";
  try {
    const created = await getJson("/api/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    status.textContent = `Run ${created.run_id} is launching.`;
    await new Promise((resolve) => window.setTimeout(resolve, 400));
    await loadRuns();
    if (created.run_id) {
      await selectRun(created.run_id);
    }
  } catch (error) {
    status.textContent = `Run launch failed: ${error}`;
  }
}

function bindControls() {
  document.getElementById("run-form").addEventListener("submit", startRun);
  document.getElementById("scenario-select").addEventListener("change", (event) => {
    void loadScenario(event.target.value);
  });
  document.getElementById("timeline-slider").addEventListener("input", (event) => {
    stopPlayback();
    setSelectedEvent(Number(event.target.value));
  });
  document.getElementById("playback-toggle").addEventListener("click", togglePlayback);
  document.getElementById("playback-prev").addEventListener("click", () => {
    stopPlayback();
    setSelectedEvent(state.selectedEventIndex - 1);
  });
  document.getElementById("playback-next").addEventListener("click", () => {
    stopPlayback();
    setSelectedEvent(state.selectedEventIndex + 1);
  });
  document.getElementById("snapshot-from-select").addEventListener("change", () => {
    void updateDiff();
  });
  document.getElementById("snapshot-to-select").addEventListener("change", () => {
    void updateDiff();
  });
}

bindControls();

loadWorkspace()
  .then(loadRuns)
  .catch((error) => {
    renderJson("workspace-panel", { error: String(error) });
    renderJson("run-panel", { error: String(error) });
    document.getElementById("workspace-subtitle").textContent = `Workspace load failed: ${error}`;
  });
