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

const GRAPH_TITLES = {
  comm_graph: "Communications",
  doc_graph: "Documents",
  work_graph: "Workflows",
  identity_graph: "Identity",
  revenue_graph: "Revenue",
  data_graph: "Data",
  obs_graph: "Observability",
  ops_graph: "Operations",
  property_graph: "Property",
  campaign_graph: "Campaign",
  inventory_graph: "Inventory",
};

const state = {
  workspace: null,
  story: null,
  exercise: null,
  presentation: null,
  playableBundle: null,
  missions: [],
  missionState: null,
  fidelityReport: null,
  exportsPreview: [],
  scenarios: [],
  scenarioPreview: null,
  scenarioContract: null,
  importSummary: null,
  identityFlow: null,
  importSources: null,
  importNormalization: null,
  importReview: null,
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
  surfaceState: null,
  surfaceHighlights: { panels: [], refs: [] },
  surfaceHighlightExpiresAt: 0,
  surfaceHighlightTimer: null,
  lastMoveImpact: null,
  snapshots: [],
  snapshotForkError: null,
  selectedEventIndex: 0,
  selectedSnapshotFrom: null,
  selectedSnapshotTo: null,
  studioView: "company",
  developerMode: false,
  cinemaMode: false,
  timelineMode: false,
  compareMode: false,
  compareRunA: null,
  compareRunB: null,
  compareTimelineA: [],
  compareTimelineB: [],
  compareMissionA: null,
  compareMissionB: null,
  cinemaAutoAdvance: false,
  cinemaAutoTimer: null,
  playbackTimer: null,
  eventSource: null,
  cascadeActive: false,
  cascadeAbort: null,
  refreshGeneration: 0,
  sseDebounceTimer: null,
};

async function getJson(path, options = {}) {
  const response = await fetch(path, options);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return await response.json();
}

async function fetchStoryArtifacts() {
  const [story, exportsPreview, presentation] = await Promise.all([
    getJson("/api/story").catch(() => null),
    getJson("/api/exports-preview").catch(() => []),
    getJson("/api/presentation").catch(() => null),
  ]);
  return { story, exportsPreview, presentation };
}

async function fetchExerciseArtifacts() {
  const exercise = await getJson("/api/exercise").catch(() => null);
  return { exercise };
}

async function fetchPlayableArtifacts() {
  const [playableBundle, missions, missionState, fidelityReport] = await Promise.all([
    getJson("/api/playable").catch(() => null),
    getJson("/api/missions").catch(() => []),
    getJson("/api/missions/state").catch(() => null),
    getJson("/api/fidelity").catch(() => null),
  ]);
  return { playableBundle, missions, missionState, fidelityReport };
}

function el(id) {
  return document.getElementById(id);
}

function currentSnapshotForkRunId() {
  const missionRunId = state.missionState?.run_id || null;
  if (!missionRunId || state.activeRunId !== missionRunId) {
    return null;
  }
  return missionRunId;
}

async function requestMissionBranch(runId, snapshotId = null) {
  const body = snapshotId === null ? {} : { snapshot_id: snapshotId };
  return await getJson(`/api/missions/${encodeURIComponent(runId)}/branch`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

async function activateMissionBranch(payload) {
  const previousSurfaceState = state.surfaceState;
  state.snapshotForkError = null;
  state.missionState = payload;
  await loadRuns({ selectActiveRun: false });
  await selectRun(payload.run_id, { previousSurfaceState });
  state.missionState = payload;
}

function renderJson(id, payload) {
  const node = el(id);
  if (node) node.textContent = JSON.stringify(payload, null, 2);
}

function nonEmptyPayload(payload) {
  return payload && typeof payload === "object" && Object.keys(payload).length
    ? payload
    : null;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function activeMission() {
  return state.missionState?.mission || state.playableBundle?.mission || state.missions[0] || null;
}

function hasExerciseMode() {
  return Boolean(state.exercise?.manifest);
}

function activeScenarioVariant() {
  const variants = Array.isArray(state.scenarioPreview?.available_scenario_variants)
    ? state.scenarioPreview.available_scenario_variants
    : [];
  return variants.find((item) => item.name === state.scenarioPreview?.active_scenario_variant) || null;
}

function activeContractVariant() {
  const variants = Array.isArray(state.scenarioPreview?.available_contract_variants)
    ? state.scenarioPreview.available_contract_variants
    : [];
  return variants.find((item) => item.name === state.scenarioPreview?.active_contract_variant) || null;
}

function currentCrisisTitle() {
  if (hasExerciseMode()) {
    const scenarioVariant = activeScenarioVariant();
    if (scenarioVariant?.title) {
      return scenarioVariant.title;
    }
    if (state.exercise?.manifest?.crisis_name) {
      return state.exercise.manifest.crisis_name;
    }
  }
  const mission = activeMission();
  if (mission?.title) {
    return mission.title;
  }
  const scenarioVariant = activeScenarioVariant();
  if (scenarioVariant?.title) {
    return scenarioVariant.title;
  }
  return state.scenarioPreview?.scenario?.title || "Current crisis";
}

function currentCrisisSummary() {
  if (hasExerciseMode()) {
    const scenarioVariant = activeScenarioVariant();
    if (scenarioVariant?.description) {
      return scenarioVariant.description;
    }
    if (state.scenarioPreview?.scenario?.description) {
      return state.scenarioPreview.scenario.description;
    }
  }
  const mission = activeMission();
  if (mission?.briefing) {
    return mission.briefing;
  }
  const scenarioVariant = activeScenarioVariant();
  if (scenarioVariant?.description) {
    return scenarioVariant.description;
  }
  if (state.scenarioPreview?.scenario?.description) {
    return state.scenarioPreview.scenario.description;
  }
  return state.story?.company_briefing
    || state.workspace?.manifest?.description
    || "Choose a crisis and enter the world.";
}

function currentFailureImpact() {
  if (hasExerciseMode()) {
    const scenarioVariant = activeScenarioVariant();
    if (Array.isArray(scenarioVariant?.change_summary) && scenarioVariant.change_summary.length) {
      return scenarioVariant.change_summary.join(" · ");
    }
  }
  const mission = activeMission();
  if (mission?.failure_impact) {
    return mission.failure_impact;
  }
  const scenarioVariant = activeScenarioVariant();
  if (Array.isArray(scenarioVariant?.change_summary) && scenarioVariant.change_summary.length) {
    return scenarioVariant.change_summary.join(" · ");
  }
  return state.story?.failure_impact || "";
}

function currentObjectiveSummary() {
  if (hasExerciseMode()) {
    const contractVariant = activeContractVariant();
    if (contractVariant?.objective_summary) {
      return contractVariant.objective_summary;
    }
    if (contractVariant?.description) {
      return contractVariant.description;
    }
  }
  const contractVariant = activeContractVariant();
  if (contractVariant?.objective_summary) {
    return contractVariant.objective_summary;
  }
  if (contractVariant?.description) {
    return contractVariant.description;
  }
  return state.story?.objective_briefing
    || "The active target defines what counts as success, what must be avoided, and how tradeoffs are judged.";
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
  if (["ok", "true", "passed", "success", "approved", "ready", "fresh"].includes(normalized)) {
    return "ok";
  }
  if (["error", "failed", "false", "critical", "blocked"].includes(normalized)) {
    return "error";
  }
  if (
    [
      "running",
      "queued",
      "warning",
      "attention",
      "open",
      "in_progress",
      "review",
      "pending",
      "pending_approval",
      "scheduled",
      "draft",
      "stale",
    ].includes(normalized)
  ) {
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

function formatDomainTitle(domain) {
  return GRAPH_TITLES[domain] || domain.replaceAll("_", " ");
}

const SURFACE_TITLES = {
  slack: "Slack",
  mail: "Email",
  tickets: "Work Tracker",
  docs: "Docs",
  approvals: "Approvals",
  vertical_heartbeat: "Business Core",
};
const SURFACE_HIGHLIGHT_MS = 2600;

const DOMAIN_TO_SURFACE = {
  comm_graph: "slack",
  doc_graph: "docs",
  work_graph: "tickets",
  identity_graph: "approvals",
  property_graph: "vertical_heartbeat",
  revenue_graph: "vertical_heartbeat",
  campaign_graph: "vertical_heartbeat",
  inventory_graph: "vertical_heartbeat",
  ops_graph: "tickets",
};

function guessMoveTargets(move) {
  const targets = new Set();
  const domain = move.graph_action?.domain;
  if (domain && DOMAIN_TO_SURFACE[domain]) targets.add(DOMAIN_TO_SURFACE[domain]);
  const preview = (move.consequence_preview || "").toLowerCase();
  if (/slack|message|channel|comm/.test(preview)) targets.add("slack");
  if (/mail|email/.test(preview)) targets.add("mail");
  if (/ticket|issue|jira|comment/.test(preview)) targets.add("tickets");
  if (/doc|document|checklist|artifact/.test(preview)) targets.add("docs");
  if (/approv|request|vendor/.test(preview)) targets.add("approvals");
  if (/lease|unit|property|work.?order|revenue|campaign/.test(preview)) targets.add("vertical_heartbeat");
  if (!targets.size && domain) targets.add("vertical_heartbeat");
  return [...targets];
}

function formatSurfaceTitle(surface) {
  return SURFACE_TITLES[surface] || formatDomainTitle(surface);
}

function summarizeMoveValue(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value.slice(0, 60);
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  try {
    return JSON.stringify(value).slice(0, 60);
  } catch (error) {
    return String(value).slice(0, 60);
  }
}

function uniqueStrings(values) {
  return [...new Set((values || []).filter((item) => typeof item === "string" && item))];
}

function inWorldCopy(value, fallback) {
  const text = String(value || "").trim();
  if (!text) {
    return fallback;
  }
  if (/(kernel|demo|world pack|static workflow|proof|same company|same world)/i.test(text)) {
    return fallback;
  }
  return text;
}

function latestExecutedMove() {
  const executed = state.missionState?.executed_moves || [];
  return executed.length ? executed[executed.length - 1] : null;
}

function closeToolbarMenus() {
  document.querySelectorAll(".toolbar-menu[open]").forEach((node) => {
    node.removeAttribute("open");
  });
}

function clearSurfaceHighlightTimer() {
  if (!state.surfaceHighlightTimer) {
    return;
  }
  window.clearTimeout(state.surfaceHighlightTimer);
  state.surfaceHighlightTimer = null;
}

function setSurfaceHighlights(highlights, { preserveExisting = false } = {}) {
  const nextPanels = uniqueStrings(highlights?.panels || []);
  const nextRefs = uniqueStrings(highlights?.refs || []);
  const hasHighlights = nextPanels.length > 0 || nextRefs.length > 0;

  if (!hasHighlights) {
    if (preserveExisting && Date.now() < state.surfaceHighlightExpiresAt) {
      return;
    }
    clearSurfaceHighlightTimer();
    state.surfaceHighlights = { panels: [], refs: [] };
    state.surfaceHighlightExpiresAt = 0;
    return;
  }

  clearSurfaceHighlightTimer();
  state.surfaceHighlights = { panels: nextPanels, refs: nextRefs };
  state.surfaceHighlightExpiresAt = Date.now() + SURFACE_HIGHLIGHT_MS;
  state.surfaceHighlightTimer = window.setTimeout(() => {
    state.surfaceHighlights = { panels: [], refs: [] };
    state.surfaceHighlightExpiresAt = 0;
    state.surfaceHighlightTimer = null;
    renderLivingCompanyView();
  }, SURFACE_HIGHLIGHT_MS);
}

function normalizeStudioView(view) {
  const normalized = String(view || "").toLowerCase();
  const ALIASES = {
    play: "company",
    worlds: "company",
    situations: "crisis",
    missions: "crisis",
    objectives: "crisis",
    results: "outcome",
    runs: "outcome",
    exports: "outcome",
  };
  return ALIASES[normalized] || normalized || "company";
}

function setStudioView(view) {
  state.studioView = normalizeStudioView(view);
  document.querySelectorAll("main [data-studio-view]").forEach((node) => {
    node.classList.toggle("hidden-panel", node.dataset.studioView !== state.studioView);
  });
  document.querySelectorAll(".studio-nav-button").forEach((node) => {
    node.classList.toggle("active", node.dataset.studioView === state.studioView);
  });
}

function toggleDeveloperMode() {
  state.developerMode = !state.developerMode;
  document.body.classList.toggle("developer-mode", state.developerMode);
  closeToolbarMenus();
  document.getElementById("developer-toggle").textContent = state.developerMode
    ? "Hide Systems Detail"
    : "Systems Detail";
}

function jumpToStudioView(view) {
  const normalized = normalizeStudioView(view);
  setStudioView(normalized);
  const target = document.querySelector(`[data-studio-view="${normalized}"]`);
  if (target) {
    target.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

function renderPresentationPanel() {
  const panel = document.getElementById("presentation-panel");
  const beatsPanel = document.getElementById("presentation-beats");
  const presentation = state.presentation || state.story?.presentation || null;
  const story = state.story || {};
  if (!panel || !beatsPanel) {
    return;
  }
  if (!presentation) {
    panel.innerHTML = `
      <div class="story-card story-span-2">
        <p class="eyebrow">Briefing</p>
        <p class="metric-detail">Load this world's briefing to get the full walkthrough.</p>
      </div>
    `;
    beatsPanel.innerHTML = "";
    return;
  }

  const primitives = Array.isArray(presentation.primitives) ? presentation.primitives : [];
  const setup = Array.isArray(presentation.presenter_setup) ? presentation.presenter_setup : [];
  const commands = Array.isArray(presentation.operator_commands) ? presentation.operator_commands : [];

  panel.innerHTML = `
    <div class="story-card accent-card story-span-2">
      <p class="eyebrow">What stays fixed</p>
      <h3>The company is stable. The pressure changes.</h3>
      <p class="metric-detail">${escapeHtml(presentation.opening_hook || story.kernel_thesis || "VEI is one reusable world kernel for enterprises.")}</p>
    </div>
    <div class="story-card">
      <p class="eyebrow">Why this world exists</p>
      <p class="metric-detail">${escapeHtml(presentation.demo_goal || "Start with one stable company world, then vary the situation and the objective on top of the same runtime.")}</p>
    </div>
    <div class="story-card">
      <p class="eyebrow">Open the world</p>
      <div class="stack">
        ${setup.map((item) => `<p class="metric-detail">${escapeHtml(item)}</p>`).join("")}
      </div>
    </div>
    <div class="story-card story-span-2">
      <p class="eyebrow">World primitives</p>
      <div class="briefing-grid">
        ${primitives
          .map(
            (item) => `
              <div class="stack-card">
                <h3>${escapeHtml(item.title)}</h3>
                <div class="chip-row">
                  ${chip(item.current_value || item.name)}
                  ${chip(item.kernel_mapping || "kernel")}
                </div>
                <p class="metric-detail">${escapeHtml(item.summary || "")}</p>
              </div>
            `
          )
          .join("")}
      </div>
    </div>
    <div class="story-card story-span-2">
      <p class="eyebrow">Launch commands</p>
      <div class="stack">
        ${commands.map((item) => `<pre class="code-panel">${escapeHtml(item)}</pre>`).join("")}
      </div>
    </div>
  `;

  beatsPanel.innerHTML = (presentation.beats || [])
    .map(
      (beat) => `
        <div class="presentation-step">
          <div class="presentation-step-number">${escapeHtml(String(beat.step))}</div>
          <div class="presentation-step-body">
            <div class="chip-row">
              ${chip(`view:${normalizeStudioView(beat.studio_view)}`)}
              ${chip(beat.title)}
            </div>
            <h3>${escapeHtml(beat.title)}</h3>
            <p class="metric-detail"><strong>Do:</strong> ${escapeHtml(beat.operator_action || "")}</p>
            <p class="metric-detail"><strong>Read it as:</strong> ${escapeHtml(beat.presenter_note || "")}</p>
            <p class="metric-detail"><strong>Shows:</strong> ${escapeHtml(beat.proof_point || "")}</p>
            <p class="metric-detail"><strong>Leaves behind:</strong> ${escapeHtml(beat.audience_takeaway || "")}</p>
            <button type="button" class="ghost-button presentation-jump" data-jump-view="${escapeHtml(normalizeStudioView(beat.studio_view || "presentation"))}">Jump to this beat</button>
          </div>
        </div>
      `
    )
    .join("");

  beatsPanel.querySelectorAll(".presentation-jump").forEach((node) => {
    node.addEventListener("click", () => {
      jumpToStudioView(node.dataset.jumpView || "presentation");
    });
  });
}

function renderStudioShell() {
  setStudioView(state.studioView);
  renderPresentationPanel();
}

async function refreshStoryArtifacts() {
  const payload = await fetchStoryArtifacts();
  state.story = nonEmptyPayload(payload.story);
  state.exportsPreview = payload.exportsPreview;
  state.presentation =
    nonEmptyPayload(payload.presentation) ||
    nonEmptyPayload(payload.story?.presentation) ||
    null;
  renderStudioShell();
  renderWorldsPanel();
  renderExportsPanel();
}

async function refreshPlayableArtifacts() {
  const payload = await fetchPlayableArtifacts();
  state.playableBundle = nonEmptyPayload(payload.playableBundle);
  state.missions = Array.isArray(payload.missions) ? payload.missions : [];
  const fetched = nonEmptyPayload(payload.missionState);
  const current = state.missionState;
  const currentHasMoves = current && Array.isArray(current.available_moves) && current.available_moves.length > 0;
  const fetchedHasMoves = fetched && Array.isArray(fetched.available_moves) && fetched.available_moves.length > 0;
  if (!currentHasMoves || fetchedHasMoves) {
    state.missionState = fetched;
  }
  state.fidelityReport = nonEmptyPayload(payload.fidelityReport);
  renderMissionSelector();
  renderMissionSummary();
  renderMissionPlay();
  renderFidelityPanel();
  renderLivingCompanyView();
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
  const missionState = state.missionState;
  const systemCount = state.surfaceState?.panels?.length
    || (state.story?.manifest?.key_surfaces || []).length
    || 0;
  panel.innerHTML = [
    chip(manifest.title || manifest.name || "Workspace"),
    chip(`${systemCount} system${systemCount === 1 ? "" : "s"}`),
    chip(`${workspace.run_count || 0} path${workspace.run_count === 1 ? "" : "s"}`),
    chip(missionState?.status || (latestRun ? latestRun.status : "ready"), statusClass(missionState?.status || latestRun?.status)),
  ].join("");
}

function renderWorkspaceHero() {
  const workspace = state.workspace;
  if (!workspace) {
    return;
  }
  const manifest = workspace.manifest || {};
  const story = state.story || {};
  const title = document.getElementById("workspace-title");
  const subtitle = document.getElementById("workspace-subtitle");
  const companyName = story.manifest?.company_name || manifest.title || "Workspace";
  const missionLine = currentCrisisSummary();
  if (title) {
    title.textContent = companyName;
  }
  subtitle.classList.remove("loading-pulse");
  subtitle.textContent = missionLine;
  renderWorkspaceMetrics();
  renderStudioShell();
  renderWorldsPanel();
  renderJson("workspace-panel", workspace);
}

function renderWorldsPanel() {
  const panel = document.getElementById("world-menu-panel");
  const label = document.getElementById("world-menu-label");
  const story = state.story;
  const workspace = state.workspace;
  if (!workspace) {
    return;
  }
  const manifest = workspace.manifest || {};
  const availableWorlds = Array.isArray(story?.available_worlds) ? story.available_worlds : [];
  const currentWorldName = story?.manifest?.name || manifest.source_ref || "";
  const companyName = story?.manifest?.company_name || manifest.title || manifest.name || "Workspace";
  if (label) {
    label.textContent = companyName;
  }
  if (!panel) {
    return;
  }
  panel.innerHTML = availableWorlds
    .map(
      (item) => `
        <div class="world-menu-item ${item.name === currentWorldName ? "world-menu-item-current" : ""}">
          <div class="world-menu-head">
            <strong>${escapeHtml(item.company_name)}</strong>
            ${item.name === currentWorldName ? chip("current", "ok") : chip("included")}
          </div>
          <p class="metric-detail">${escapeHtml(item.company_briefing || item.description || "")}</p>
          <div class="chip-row">
            ${(item.key_surfaces || []).slice(0, 3).map((surface) => chip(formatDomainTitle(surface))).join("")}
          </div>
        </div>
      `
    )
    .join("");
}

function renderMissionSelector() {
  const missionSelect = document.getElementById("mission-select");
  const objectiveSelect = document.getElementById("objective-select");
  const startButton = document.getElementById("start-mission-button");
  if (!missionSelect || !objectiveSelect) {
    return;
  }
  if (hasExerciseMode()) {
    const scenarioVariants = Array.isArray(state.scenarioPreview?.available_scenario_variants)
      ? state.scenarioPreview.available_scenario_variants
      : [];
    missionSelect.innerHTML = scenarioVariants
      .map(
        (item) => `
          <option value="${escapeHtml(item.name || "")}" ${
            item.name === state.scenarioPreview?.active_scenario_variant ? "selected" : ""
          }>${escapeHtml(item.title || item.name || "Scenario")}</option>
        `
      )
      .join("");
    const contractVariants = Array.isArray(state.scenarioPreview?.available_contract_variants)
      ? state.scenarioPreview.available_contract_variants
      : [];
    objectiveSelect.innerHTML = contractVariants
      .map(
        (item) => `
          <option value="${escapeHtml(item.name || "")}" ${
            item.name === state.scenarioPreview?.active_contract_variant ? "selected" : ""
          }>${escapeHtml(item.title || item.name || "Objective")}</option>
        `
      )
      .join("");
    if (startButton) {
      startButton.textContent = "Apply Crisis";
    }
    return;
  }
  const currentMission =
    state.missionState?.mission ||
    state.playableBundle?.mission ||
    state.missions[0] ||
    null;
  const selectedMissionName = currentMission?.mission_name || "";
  missionSelect.innerHTML = state.missions
    .map(
      (item) => `
        <option value="${escapeHtml(item.mission_name)}" ${
          item.mission_name === selectedMissionName ? "selected" : ""
        }>${escapeHtml(item.title)}</option>
      `
    )
    .join("");
  const supportedObjectives = Array.isArray(currentMission?.supported_objectives)
    ? currentMission.supported_objectives
    : [];
  const contractVariants = Array.isArray(state.scenarioPreview?.available_contract_variants)
    ? state.scenarioPreview.available_contract_variants
    : [];
  const selectedObjective =
    state.missionState?.objective_variant ||
    currentMission?.default_objective ||
    supportedObjectives[0] ||
    "";
  objectiveSelect.innerHTML = supportedObjectives
    .map((name) => {
      const detail = contractVariants.find((item) => item.name === name);
      const label = detail?.title || name;
      return `<option value="${escapeHtml(name)}" ${
        name === selectedObjective ? "selected" : ""
      }>${escapeHtml(label)}</option>`;
    })
    .join("");
  if (startButton) {
    startButton.textContent = "Enter World";
  }
}

function renderMissionSummary() {
  const briefing = document.getElementById("mission-briefing");
  const catalog = document.getElementById("mission-catalog");
  if (!briefing) {
    return;
  }
  const currentMission = activeMission();
  const scenarioVariant = activeScenarioVariant();
  if (hasExerciseMode() || !currentMission) {
    const crisisTitle = currentCrisisTitle();
    const crisisSummary = currentCrisisSummary();
    const failureImpact = currentFailureImpact();
    briefing.innerHTML = `
      <div class="story-card accent-card story-span-2 crisis-hero-card">
        <p class="eyebrow">Current crisis</p>
        <h3>${escapeHtml(crisisTitle)}</h3>
        <p class="metric-detail">${escapeHtml(crisisSummary)}</p>
        <div class="chip-row">
          ${chip(state.scenarioPreview?.active_scenario_variant || "default")}
          ${chip(state.scenarioPreview?.active_contract_variant || "default objective")}
        </div>
      </div>
      <div class="story-card">
        <p class="eyebrow">Why this matters</p>
        <p class="metric-detail">${escapeHtml(inWorldCopy(
          scenarioVariant?.rationale,
          "This is the pressure point most likely to decide whether the company stabilizes or scrambles."
        ))}</p>
      </div>
      ${failureImpact ? `
        <div class="story-card">
          <p class="eyebrow">Failure impact</p>
          <p class="metric-detail">${escapeHtml(failureImpact)}</p>
        </div>
      ` : ""}
    `;
    if (catalog) {
      const scenarioVariants = Array.isArray(state.scenarioPreview?.available_scenario_variants)
        ? state.scenarioPreview.available_scenario_variants
        : [];
      catalog.innerHTML = scenarioVariants
        .map(
          (item) => `
            <div class="run-item ${item.name === state.scenarioPreview?.active_scenario_variant ? "active" : ""}">
              <div class="chip-row">
                ${chip(item.name || "scenario")}
                ${item.name === state.scenarioPreview?.active_scenario_variant ? chip("active", "ok") : ""}
              </div>
              <h3>${escapeHtml(item.title || item.name || "Scenario")}</h3>
              <p class="metric-detail">${escapeHtml(item.description || "")}</p>
              <button type="button" class="ghost-button activate-scenario-variant-button" data-variant-name="${escapeHtml(item.name || "")}">Switch crisis</button>
            </div>
          `
        )
        .join("");
      catalog.querySelectorAll(".activate-scenario-variant-button").forEach((node) => {
        node.addEventListener("click", () => {
          void activateScenarioVariant(node.dataset.variantName || "");
        });
      });
    }
    return;
  }
  briefing.innerHTML = `
    <div class="story-card accent-card story-span-2 crisis-hero-card">
      <p class="eyebrow">Current crisis</p>
      <h3>${escapeHtml(currentMission.title)}</h3>
      <p class="metric-detail">${escapeHtml(currentMission.briefing || "")}</p>
      <div class="chip-row">
        ${chip(currentMission.primary_domain || "world")}
        ${chip(`${(currentMission.supported_objectives || []).length || 1} success mode${(currentMission.supported_objectives || []).length === 1 ? "" : "s"}`)}
      </div>
    </div>
    <div class="story-card">
      <p class="eyebrow">Why this matters</p>
      <p class="metric-detail">${escapeHtml(inWorldCopy(
        currentMission.why_it_matters,
        "This is the pressure point most likely to decide whether the company stabilizes or scrambles."
      ))}</p>
    </div>
    <div class="story-card">
      <p class="eyebrow">Failure impact</p>
      <p class="metric-detail">${escapeHtml(currentMission.failure_impact || "")}</p>
    </div>
  `;
  if (!catalog) {
    return;
  }
  catalog.innerHTML = state.missions
    .map(
      (item) => `
        <div class="run-item ${item.mission_name === currentMission.mission_name ? "active" : ""}">
          <div class="chip-row">
            ${chip(item.mission_name)}
            ${item.hero ? chip("primary", "ok") : chip("included")}
          </div>
          <h3>${escapeHtml(item.title)}</h3>
          <p class="metric-detail">${escapeHtml(item.briefing || "")}</p>
          <p class="metric-detail">${escapeHtml(item.why_it_matters || "")}</p>
          <div class="chip-row">
            ${(item.supported_objectives || []).map((objective) => chip(objective)).join("")}
          </div>
          <button type="button" class="ghost-button activate-mission-button" data-mission-name="${escapeHtml(item.mission_name)}">Activate mission</button>
        </div>
      `
    )
    .join("");
  catalog.querySelectorAll(".activate-mission-button").forEach((node) => {
    node.addEventListener("click", () => {
      const objective = document.getElementById("objective-select")?.value || null;
      void activateMission(node.dataset.missionName, objective);
    });
  });
}

// ---------------------------------------------------------------------------
// Cascade replay: stagger surface highlight reveals after a move
// ---------------------------------------------------------------------------
const CASCADE_STAGGER_MS = 180;
const CASCADE_HOLD_MS = 1400;

function abortCascade() {
  if (state.cascadeAbort) {
    state.cascadeAbort();
    state.cascadeAbort = null;
  }
  state.cascadeActive = false;
}

function playCascade(changedPanels, changedRefs) {
  abortCascade();
  if (!changedPanels.length) return Promise.resolve();
  state.cascadeActive = true;
  const bar = document.getElementById("cascade-progress");
  const label = document.getElementById("cascade-label");
  if (bar) bar.style.width = "0%";
  if (label) label.textContent = `Propagating across ${changedPanels.length} system${changedPanels.length === 1 ? "" : "s"}\u2026`;
  const progressEl = document.getElementById("cascade-bar");
  if (progressEl) progressEl.classList.add("cascade-bar-visible");

  return new Promise((resolve) => {
    let step = 0;
    let cancelled = false;
    state.cascadeAbort = () => { cancelled = true; resolve(); };

    function revealNext() {
      if (cancelled || step >= changedPanels.length) {
        state.cascadeActive = false;
        if (progressEl) progressEl.classList.remove("cascade-bar-visible");
        if (label) label.textContent = "";
        resolve();
        return;
      }
      const surface = changedPanels[step];
      const panelEl = document.querySelector(`.surface-panel[data-surface="${surface}"]`);
      if (panelEl) {
        panelEl.classList.remove("cascade-pending");
        panelEl.classList.add("cascade-reveal");
        const toast = panelEl.querySelector(".cascade-toast");
        if (toast) toast.classList.add("cascade-toast-visible");
      }
      step++;
      const pct = Math.round((step / changedPanels.length) * 100);
      if (bar) bar.style.width = `${pct}%`;
      if (label) label.textContent = `${step} of ${changedPanels.length} systems updated`;
      window.setTimeout(revealNext, CASCADE_STAGGER_MS);
    }

    window.setTimeout(revealNext, 200);
  });
}

// ---------------------------------------------------------------------------
// Cinema mode: full-screen presentation layout
// ---------------------------------------------------------------------------
function toggleCinemaMode() {
  closeToolbarMenus();
  state.cinemaMode = !state.cinemaMode;
  if (state.cinemaMode && state.timelineMode) {
    state.timelineMode = false;
    document.body.classList.remove("timeline-mode");
    const tBtn = document.getElementById("timeline-toggle");
    if (tBtn) tBtn.textContent = "Timeline";
    const section = document.getElementById("timeline-section");
    if (section) section.style.display = "none";
  }
  document.body.classList.toggle("cinema-mode", state.cinemaMode);
  const btn = document.getElementById("cinema-toggle");
  if (btn) btn.textContent = state.cinemaMode ? "Exit Presentation" : "Present";
  if (!state.cinemaMode) {
    stopCinemaAutoAdvance();
  } else {
    setStudioView("company");
    const hasMoves = (state.missionState?.available_moves || []).some(
      (m) => !m.executed && m.availability !== "blocked"
    );
    if (hasMoves) {
      state.cinemaAutoAdvance = true;
      window.setTimeout(cinemaAutoStep, 1200);
    }
  }
  renderCinemaNarrative();
}

function renderCinemaNarrative() {
  const container = document.getElementById("cinema-narrative");
  if (!container) return;
  if (!state.cinemaMode) { container.innerHTML = ""; return; }
  const ms = state.missionState;
  const mission = ms?.mission || state.playableBundle?.mission || null;
  const moveCount = (ms?.executed_moves || []).length;
  const lastMove = moveCount ? ms.executed_moves[moveCount - 1] : null;
  const score = ms?.scorecard || {};
  const systemCount = (state.surfaceState?.panels || []).length;

  let narrativeLine = "";
  if (ms?.status === "completed") {
    narrativeLine = score.mission_success
      ? "Mission resolved successfully."
      : "Mission closed with remaining exposure.";
  } else if (lastMove) {
    const tool = lastMove.resolved_tool || "";
    const refs = (lastMove.object_refs || []).slice(0, 3).join(", ");
    narrativeLine = `Move ${moveCount}: ${lastMove.title}`;
    if (tool) narrativeLine += ` \u2192 ${tool}`;
    if (refs) narrativeLine += ` \u2192 ${refs}`;
  } else if (mission) {
    narrativeLine = mission.briefing || mission.description || "Entering the world\u2026";
  }

  const scorePct = Math.min(100, Math.max(0, score.overall_score || 0));
  const remaining = score.action_budget_remaining ?? "?";

  container.innerHTML = `
    <div class="cinema-narrative-bar">
      <div class="cinema-narrative-text">${escapeHtml(narrativeLine)}</div>
      <div class="cinema-narrative-meta">
        <span class="cinema-stat">${moveCount} move${moveCount === 1 ? "" : "s"}</span>
        <span class="cinema-stat">${systemCount} systems</span>
        <span class="cinema-stat">Score ${scorePct}</span>
        <span class="cinema-stat">Budget ${remaining}</span>
      </div>
      <button type="button" id="cinema-auto-toggle" class="ghost-button cinema-auto-btn">
        ${state.cinemaAutoAdvance ? "Pause" : "Auto-play"}
      </button>
    </div>
  `;
  const autoBtn = document.getElementById("cinema-auto-toggle");
  if (autoBtn) autoBtn.addEventListener("click", toggleCinemaAutoAdvance);
}

function toggleCinemaAutoAdvance() {
  state.cinemaAutoAdvance = !state.cinemaAutoAdvance;
  if (state.cinemaAutoAdvance) {
    cinemaAutoStep();
  } else {
    stopCinemaAutoAdvance();
  }
  if (state.timelineMode) {
    renderTimelineView();
  } else {
    renderCinemaNarrative();
  }
}

function stopCinemaAutoAdvance() {
  state.cinemaAutoAdvance = false;
  if (state.cinemaAutoTimer) {
    window.clearTimeout(state.cinemaAutoTimer);
    state.cinemaAutoTimer = null;
  }
}

function cinemaAutoStep() {
  if (!state.cinemaAutoAdvance || !state.missionState) return;
  const nextMove = (state.missionState.available_moves || []).find(
    (m) => !m.executed && m.availability !== "blocked"
  );
  if (!nextMove) {
    stopCinemaAutoAdvance();
    renderCinemaNarrative();
    return;
  }
  void applyMissionMove(nextMove.move_id).then(() => {
    if (!state.cinemaAutoAdvance) return;
    state.cinemaAutoTimer = window.setTimeout(cinemaAutoStep, 2800);
  });
}

// ---------------------------------------------------------------------------
// Timeline / causality view
// ---------------------------------------------------------------------------
const SURFACE_ACCENT = {
  slack: "#36c5f0",
  mail: "#ffb454",
  tickets: "#ff6d5e",
  docs: "#1aa88d",
  approvals: "#9b7bff",
  vertical_heartbeat: "#1e6cf2",
};

const TOOL_TO_SURFACE = {
  "slack.post_message": "slack", "slack.list_channels": "slack", "slack.read_channel": "slack",
  "mail.send": "mail", "mail.list": "mail", "mail.read_thread": "mail",
  "tickets.create": "tickets", "tickets.update": "tickets", "tickets.list": "tickets",
  "tickets.add_comment": "tickets",
  "docs.update": "docs", "docs.create": "docs", "docs.list": "docs",
  "servicedesk.create_request": "approvals", "servicedesk.update_request_approval": "approvals",
  "servicedesk.list_requests": "approvals",
};

function toolToSurface(tool) {
  if (!tool) return null;
  if (TOOL_TO_SURFACE[tool]) return TOOL_TO_SURFACE[tool];
  const prefix = tool.split(".")[0];
  const map = { slack: "slack", mail: "mail", tickets: "tickets", docs: "docs", servicedesk: "approvals" };
  return map[prefix] || "vertical_heartbeat";
}

function timelineSurfaceHits(timeline) {
  const hits = {};
  for (const ev of timeline) {
    if (ev.kind !== "trace_call" && ev.kind !== "workflow_step") continue;
    const surface = toolToSurface(ev.tool || ev.resolved_tool);
    if (surface) hits[surface] = (hits[surface] || 0) + 1;
  }
  return hits;
}

function toggleTimelineMode() {
  closeToolbarMenus();
  state.timelineMode = !state.timelineMode;
  if (state.timelineMode && state.cinemaMode) {
    state.cinemaMode = false;
    document.body.classList.remove("cinema-mode");
    const cinBtn = document.getElementById("cinema-toggle");
    if (cinBtn) cinBtn.textContent = "Present";
    stopCinemaAutoAdvance();
  }
  document.body.classList.toggle("timeline-mode", state.timelineMode);
  const btn = document.getElementById("timeline-toggle");
  if (btn) btn.textContent = state.timelineMode ? "Exit Timeline" : "Timeline";
  const section = document.getElementById("timeline-section");
  if (section) section.style.display = state.timelineMode ? "" : "none";
  if (state.timelineMode) {
    setStudioView("company");
    renderTimelineView();
  }
}

// ---------------------------------------------------------------------------
// Timeline rendering
// ---------------------------------------------------------------------------

function groupTimelineByMove(timeline, executedMoves) {
  const moves = executedMoves || [];
  const columns = [];
  let evIdx = 0;
  for (let mi = 0; mi < moves.length; mi++) {
    const col = { move: moves[mi], turnNumber: mi + 1, events: [] };
    const nextMs = mi + 1 < moves.length ? moves[mi + 1].time_ms : Infinity;
    while (evIdx < timeline.length && timeline[evIdx].time_ms < nextMs) {
      const ev = timeline[evIdx];
      if (ev.kind === "trace_call" || ev.kind === "workflow_step") col.events.push(ev);
      evIdx++;
    }
    columns.push(col);
  }
  if (evIdx < timeline.length) {
    const tail = { move: null, turnNumber: moves.length + 1, events: [] };
    while (evIdx < timeline.length) {
      const ev = timeline[evIdx];
      if (ev.kind === "trace_call" || ev.kind === "workflow_step") tail.events.push(ev);
      evIdx++;
    }
    if (tail.events.length) columns.push(tail);
  }
  if (!columns.length && timeline.length) {
    const col = { move: null, turnNumber: 1, events: [] };
    for (const ev of timeline) {
      if (ev.kind === "trace_call" || ev.kind === "workflow_step") col.events.push(ev);
    }
    columns.push(col);
  }
  return columns;
}

function discoverSurfaces(columns) {
  const seen = new Set();
  const ordered = [];
  for (const col of columns) {
    for (const ev of col.events) {
      const s = toolToSurface(ev.tool || ev.resolved_tool);
      if (s && !seen.has(s)) { seen.add(s); ordered.push(s); }
    }
  }
  return ordered;
}

function surfaceLabel(s) {
  const labels = { slack: "Slack", mail: "Email", tickets: "Tickets", docs: "Docs", approvals: "Approvals", vertical_heartbeat: "Business Core" };
  return labels[s] || s;
}

function renderTimelineView() {
  const section = document.getElementById("timeline-section");
  if (!section) return;
  const timeline = state.timeline || [];
  const ms = state.missionState;
  const moves = ms?.executed_moves || [];
  const columns = groupTimelineByMove(timeline, moves);
  const surfaces = discoverSurfaces(columns);
  if (!surfaces.length) {
    const panels = state.surfaceState?.panels || [];
    panels.forEach((p) => { if (!surfaces.includes(p.surface)) surfaces.push(p.surface); });
  }
  if (!surfaces.length) surfaces.push("slack", "mail", "tickets", "docs", "approvals", "vertical_heartbeat");
  const score = ms?.scorecard || {};
  const completed = ms?.status === "completed";
  const mission = ms?.mission || state.playableBundle?.mission || null;
  const companyName = state.story?.manifest?.company_name || state.workspace?.manifest?.title || "";

  let html = "";
  html += `<div class="tl-status-bar">`;
  html += `<span class="tl-company">${escapeHtml(companyName)}</span>`;
  if (mission) html += `<span class="tl-crisis">${escapeHtml(mission.title || "")}</span>`;
  html += `<span class="tl-stat">Score <strong>${score.overall_score ?? "\u2014"}</strong></span>`;
  html += `<span class="tl-stat">Moves <strong>${moves.length}</strong></span>`;
  html += `<span class="tl-stat">Risk <strong>${score.business_risk || "\u2014"}</strong></span>`;
  if (completed) {
    const cls = score.mission_success ? "tl-result-ok" : "tl-result-fail";
    const label = score.mission_success ? "Mission Complete" : "Exposure Remains";
    html += `<span class="tl-result ${cls}">${label} &mdash; ${score.success_assertions_passed || 0}/${score.success_assertions_total || 0} assertions</span>`;
  }
  html += `</div>`;

  if (state.compareMode) {
    html += renderCompareTimelines();
    section.innerHTML = html;
    const pickerA = document.getElementById("compare-picker-a");
    const pickerB = document.getElementById("compare-picker-b");
    if (pickerA && pickerB && state.compareRunA && state.compareRunB) {
      pickerA.value = state.compareRunA.run_id;
      pickerB.value = state.compareRunB.run_id;
      pickerA.addEventListener("change", onCompareRunPickerChange);
      pickerB.addEventListener("change", onCompareRunPickerChange);
    }
    document.getElementById("compare-diff-btn")?.addEventListener("click", onCompareDiffClick);
    return;
  }

  html += `<div class="tl-grid" style="--tl-cols:${Math.max(columns.length, 1)}">`;
  html += `<div class="tl-lane-labels">`;
  html += `<div class="tl-corner"></div>`;
  for (const s of surfaces) {
    html += `<div class="tl-lane-label" data-surface="${s}"><span class="tl-lane-dot" style="background:${SURFACE_ACCENT[s] || "#888"}"></span>${escapeHtml(surfaceLabel(s))}</div>`;
  }
  html += `</div>`;

  for (const col of columns) {
    const moveTitle = col.move?.title || `Turn ${col.turnNumber}`;
    html += `<div class="tl-column" data-turn="${col.turnNumber}">`;
    html += `<div class="tl-col-header" title="${escapeHtml(moveTitle)}">${escapeHtml(moveTitle)}</div>`;
    for (const s of surfaces) {
      const eventsInLane = col.events.filter((ev) => toolToSurface(ev.tool || ev.resolved_tool) === s);
      html += `<div class="tl-cell" data-surface="${s}" data-turn="${col.turnNumber}">`;
      for (const ev of eventsInLane) {
        const toolName = ev.resolved_tool || ev.tool || ev.label || "";
        const summary = ev.payload?.observation
          || ev.payload?.result?.summary
          || ev.label
          || "";
        const truncSummary = summary.length > 100 ? summary.slice(0, 100) + "\u2026" : summary;
        html += `<div class="tl-node" style="border-color:${SURFACE_ACCENT[s] || "#888"}" data-event-index="${ev.index}" title="${escapeHtml(truncSummary)}">`;
        html += `<span class="tl-node-tool">${escapeHtml(toolName.split(".").pop())}</span>`;
        if (ev.object_refs?.length) html += `<span class="tl-node-refs">${ev.object_refs.slice(0, 2).map(escapeHtml).join(", ")}</span>`;
        html += `</div>`;
      }
      if (!eventsInLane.length) html += `<div class="tl-cell-empty"></div>`;
      html += `</div>`;
    }
    html += `</div>`;
  }
  html += `</div>`;

  html += renderCausalArrowsSvg(columns, surfaces);

  section.innerHTML = html;

  section.querySelectorAll(".tl-node").forEach((node) => {
    node.addEventListener("click", () => {
      const idx = Number(node.dataset.eventIndex);
      showTimelineDetail(idx);
    });
  });
  section.querySelectorAll(".tl-col-header").forEach((hdr) => {
    hdr.addEventListener("click", () => {
      const col = hdr.closest(".tl-column");
      const turn = col?.dataset.turn;
      section.querySelectorAll(".tl-column").forEach((c) => c.classList.toggle("tl-col-dim", c.dataset.turn !== turn));
      if (hdr.classList.contains("tl-col-active")) {
        section.querySelectorAll(".tl-column").forEach((c) => c.classList.remove("tl-col-dim"));
        section.querySelectorAll(".tl-col-header").forEach((h) => h.classList.remove("tl-col-active"));
      } else {
        section.querySelectorAll(".tl-col-header").forEach((h) => h.classList.remove("tl-col-active"));
        hdr.classList.add("tl-col-active");
      }
    });
  });
}

function renderCausalArrowsSvg(columns, surfaces) {
  let arrows = "";
  for (const col of columns) {
    const surfacesHit = [];
    for (const ev of col.events) {
      const s = toolToSurface(ev.tool || ev.resolved_tool);
      if (s && !surfacesHit.includes(s)) surfacesHit.push(s);
    }
    for (let i = 0; i < surfacesHit.length - 1; i++) {
      const fromIdx = surfaces.indexOf(surfacesHit[i]);
      const toIdx = surfaces.indexOf(surfacesHit[i + 1]);
      if (fromIdx < 0 || toIdx < 0) continue;
      const colIdx = columns.indexOf(col);
      const x = colIdx;
      arrows += `<line class="tl-arrow" data-from="${surfacesHit[i]}" data-to="${surfacesHit[i + 1]}" data-col="${colIdx}" x1="${x}" y1="${fromIdx}" x2="${x}" y2="${toIdx}" />`;
    }
  }
  if (!arrows) return "";
  return `<svg class="tl-arrows-svg" data-cols="${columns.length}" data-lanes="${surfaces.length}">${arrows}</svg>`;
}

function showTimelineDetail(eventIndex) {
  const timeline = state.timeline || [];
  const ev = timeline.find((e) => e.index === eventIndex);
  if (!ev) return;
  let existing = document.getElementById("tl-detail-panel");
  if (!existing) {
    existing = document.createElement("div");
    existing.id = "tl-detail-panel";
    existing.className = "tl-detail-panel";
    document.getElementById("timeline-section")?.appendChild(existing);
  }
  const surface = toolToSurface(ev.tool || ev.resolved_tool);
  const observation = ev.payload?.observation || ev.payload?.result?.observation || "";
  const result = ev.payload?.result || {};
  const summary = typeof result === "object" ? (result.summary || result.message || "") : String(result);
  existing.innerHTML = `
    <div class="tl-detail-header">
      <span class="tl-detail-surface" style="border-color:${SURFACE_ACCENT[surface] || "#888"}">${escapeHtml(surfaceLabel(surface || ""))}</span>
      <span class="tl-detail-tool">${escapeHtml(ev.resolved_tool || ev.tool || "")}</span>
      <button type="button" class="tl-detail-close">\u2715</button>
    </div>
    <div class="tl-detail-label">${escapeHtml(ev.label)}</div>
    ${ev.object_refs?.length ? `<div class="tl-detail-refs">Refs: ${ev.object_refs.map(escapeHtml).join(", ")}</div>` : ""}
    ${observation ? `<div class="tl-detail-obs"><strong>Observation</strong><p>${escapeHtml(observation)}</p></div>` : ""}
    ${summary ? `<div class="tl-detail-summary"><strong>Result</strong><p>${escapeHtml(summary)}</p></div>` : ""}
    <div class="tl-detail-meta">Kind: ${ev.kind} &middot; Channel: ${ev.channel} &middot; Time: ${ev.time_ms}ms</div>
  `;
  existing.style.display = "";
  existing.querySelector(".tl-detail-close")?.addEventListener("click", () => { existing.style.display = "none"; });
}

async function loadCompareRunData(runA, runB) {
  const [tlA, tlB, contractA, contractB, missionA, missionB] = await Promise.all([
    getJson(`/api/runs/${runA.run_id}/timeline`),
    getJson(`/api/runs/${runB.run_id}/timeline`),
    getJson(`/api/runs/${runA.run_id}/contract`).catch(() => null),
    getJson(`/api/runs/${runB.run_id}/contract`).catch(() => null),
    getJson(`/api/missions/state?run_id=${encodeURIComponent(runA.run_id)}`).catch(() => null),
    getJson(`/api/missions/state?run_id=${encodeURIComponent(runB.run_id)}`).catch(() => null),
  ]);
  state.compareRunA = runA;
  state.compareRunB = runB;
  state.compareTimelineA = tlA;
  state.compareTimelineB = tlB;
  state.compareContractA = contractA;
  state.compareContractB = contractB;
  state.compareMissionA = nonEmptyPayload(missionA);
  state.compareMissionB = nonEmptyPayload(missionB);
}

async function toggleCompareMode() {
  closeToolbarMenus();
  state.compareMode = !state.compareMode;
  const btn = document.getElementById("compare-toggle");
  if (!state.compareMode) {
    if (btn) btn.textContent = "Compare";
    state.compareRunA = null;
    state.compareRunB = null;
    state.compareTimelineA = [];
    state.compareTimelineB = [];
    state.compareMissionA = null;
    state.compareMissionB = null;
    if (state.timelineMode) renderTimelineView();
    return;
  }
  if (btn) btn.textContent = "Exit Compare";
  if (!state.timelineMode) toggleTimelineMode();
  const runs = state.runs || [];
  if (runs.length < 2) {
    renderTimelineView();
    return;
  }
  await loadCompareRunData(runs[0], runs[1]);
  renderTimelineView();
}

async function onCompareRunPickerChange() {
  const selA = document.getElementById("compare-picker-a");
  const selB = document.getElementById("compare-picker-b");
  if (!selA || !selB) return;
  const runs = state.runs || [];
  const runA = runs.find((r) => r.run_id === selA.value);
  const runB = runs.find((r) => r.run_id === selB.value);
  if (!runA || !runB || runA.run_id === runB.run_id) return;
  await loadCompareRunData(runA, runB);
  renderTimelineView();
}

async function onCompareDiffClick() {
  const container = document.getElementById("compare-diff-result");
  if (!container) return;
  const runA = state.compareRunA;
  const runB = state.compareRunB;
  if (!runA || !runB) {
    container.innerHTML = `<p class="metric-detail">Select two runs to diff.</p>`;
    return;
  }
  container.innerHTML = `<p class="metric-detail">Loading diff...</p>`;
  try {
    const snapsA = runA.snapshots || [];
    const snapsB = runB.snapshots || [];
    const snapA = snapsA.length ? snapsA[snapsA.length - 1].snapshot_id : null;
    const snapB = snapsB.length ? snapsB[snapsB.length - 1].snapshot_id : null;
    if (snapA == null || snapB == null) {
      const [fetchedA, fetchedB] = await Promise.all([
        getJson(`/api/runs/${runA.run_id}/snapshots`).catch(() => []),
        getJson(`/api/runs/${runB.run_id}/snapshots`).catch(() => []),
      ]);
      const lastA = Array.isArray(fetchedA) && fetchedA.length ? fetchedA[fetchedA.length - 1].snapshot_id : null;
      const lastB = Array.isArray(fetchedB) && fetchedB.length ? fetchedB[fetchedB.length - 1].snapshot_id : null;
      if (lastA == null || lastB == null) {
        container.innerHTML = `<p class="metric-detail">One or both runs have no snapshots.</p>`;
        return;
      }
      const diff = await getJson(`/api/runs/diff-cross?run_a=${encodeURIComponent(runA.run_id)}&snap_a=${lastA}&run_b=${encodeURIComponent(runB.run_id)}&snap_b=${lastB}`);
      renderCrossRunDiff(container, diff, runA, runB);
      return;
    }
    const diff = await getJson(`/api/runs/diff-cross?run_a=${encodeURIComponent(runA.run_id)}&snap_a=${snapA}&run_b=${encodeURIComponent(runB.run_id)}&snap_b=${snapB}`);
    renderCrossRunDiff(container, diff, runA, runB);
  } catch (err) {
    container.innerHTML = `<p class="metric-detail">Diff failed: ${escapeHtml(String(err))}</p>`;
  }
}

function renderCrossRunDiff(container, diff, runA, runB) {
  const changedCount = Object.keys(diff.changed || {}).length;
  const addedCount = Object.keys(diff.added || {}).length;
  const removedCount = Object.keys(diff.removed || {}).length;
  const nameA = runA.runner || runA.run_id?.split("_").slice(0, 2).join("_") || "A";
  const nameB = runB.runner || runB.run_id?.split("_").slice(0, 2).join("_") || "B";
  let html = `<div class="compare-diff-header"><strong>World State Diff</strong> <span class="metric-detail">${escapeHtml(nameA)} vs ${escapeHtml(nameB)}</span></div>`;
  html += `<div class="detail-grid">`;
  html += detailTile("Changed", String(changedCount));
  html += detailTile("Added", String(addedCount));
  html += detailTile("Removed", String(removedCount));
  html += `</div>`;
  if (changedCount + addedCount + removedCount === 0) {
    html += `<p class="metric-detail">World states are identical.</p>`;
  } else {
    const entries = [];
    for (const [key, val] of Object.entries(diff.changed || {})) {
      entries.push(`<div class="diff-entry diff-changed"><span class="diff-key">${escapeHtml(key)}</span><span class="diff-val">${escapeHtml(String(val.from))} → ${escapeHtml(String(val.to))}</span></div>`);
    }
    for (const [key, val] of Object.entries(diff.added || {})) {
      entries.push(`<div class="diff-entry diff-added"><span class="diff-key">+ ${escapeHtml(key)}</span><span class="diff-val">${escapeHtml(String(val))}</span></div>`);
    }
    for (const [key, val] of Object.entries(diff.removed || {})) {
      entries.push(`<div class="diff-entry diff-removed"><span class="diff-key">- ${escapeHtml(key)}</span><span class="diff-val">${escapeHtml(String(val))}</span></div>`);
    }
    html += `<div class="diff-entries">${entries.slice(0, 50).join("")}</div>`;
    if (entries.length > 50) html += `<p class="metric-detail">${entries.length - 50} more entries...</p>`;
  }
  container.innerHTML = html;
}

function renderCompareTimelines() {
  const a = state.compareRunA;
  const b = state.compareRunB;
  if (!a || !b) return `<div class="tl-compare-empty">Need at least 2 recorded paths to compare.</div>`;
  const nameA = a.runner || a.run_id?.split("_").slice(0, 2).join("_") || "Path A";
  const nameB = b.runner || b.run_id?.split("_").slice(0, 2).join("_") || "Path B";
  const hitsA = timelineSurfaceHits(state.compareTimelineA);
  const hitsB = timelineSurfaceHits(state.compareTimelineB);
  const allSurfaces = [...new Set([...Object.keys(hitsA), ...Object.keys(hitsB)])];
  const stepsA = Object.values(hitsA).reduce((s, v) => s + v, 0);
  const stepsB = Object.values(hitsB).reduce((s, v) => s + v, 0);

  const cA = state.compareContractA;
  const cB = state.compareContractB;
  const mA = state.compareMissionA;
  const mB = state.compareMissionB;
  const branchLabels =
    mA?.mission?.branch_labels
    || mB?.mission?.branch_labels
    || state.missionState?.mission?.branch_labels
    || [];

  let html = `<div class="compare-narrative">`;

  if (branchLabels.length >= 2 || (cA && cB)) {
    html += `<div class="compare-narrative-header"><h3>Path Comparison</h3></div>`;

    if (branchLabels.length >= 2) {
      html += `<div class="compare-branches">`;
      html += `<div class="compare-branch compare-branch-a"><span class="compare-branch-dot dot-a"></span><span>${escapeHtml(branchLabels[0])}</span></div>`;
      html += `<div class="compare-branch compare-branch-b"><span class="compare-branch-dot dot-b"></span><span>${escapeHtml(branchLabels[1])}</span></div>`;
      html += `</div>`;
    }

    if ((mA && mB) || (cA && cB)) {
      const scorecardA = mA?.scorecard || null;
      const scorecardB = mB?.scorecard || null;
      const objectiveA = mA?.objective_variant || "";
      const objectiveB = mB?.objective_variant || "";
      const overallScoreA = scorecardA?.overall_score;
      const overallScoreB = scorecardB?.overall_score;
      const assertionsA = cA ? `${cA.success_predicates_passed || 0}/${cA.success_predicate_count || 0}` : "—";
      const assertionsB = cB ? `${cB.success_predicates_passed || 0}/${cB.success_predicate_count || 0}` : "—";
      const okA = scorecardA?.mission_success ?? cA?.ok ?? false;
      const okB = scorecardB?.mission_success ?? cB?.ok ?? false;
      const delta = (typeof overallScoreA === "number" && typeof overallScoreB === "number")
        ? overallScoreA - overallScoreB
        : (cA?.success_predicates_passed || 0) - (cB?.success_predicates_passed || 0);
      const deltaLabel = (typeof overallScoreA === "number" && typeof overallScoreB === "number")
        ? `${delta > 0 ? "+" : ""}${delta} score`
        : `${delta > 0 ? "+" : ""}${delta} assertions`;
      const scoreLabelA = typeof overallScoreA === "number" ? String(overallScoreA) : assertionsA;
      const scoreLabelB = typeof overallScoreB === "number" ? String(overallScoreB) : assertionsB;
      const scoreCaptionA = typeof overallScoreA === "number" ? "overall score" : "assertions";
      const scoreCaptionB = typeof overallScoreB === "number" ? "overall score" : "assertions";

      html += `<div class="compare-scores">`;
      html += `<div class="compare-score-cell ${okA ? "compare-pass" : "compare-fail"}">
        <span class="compare-score-label">${escapeHtml(nameA)}</span>
        <span class="compare-score-value">${escapeHtml(scoreLabelA)}</span>
        <span class="compare-score-caption">${escapeHtml(scoreCaptionA)}</span>
        ${objectiveA ? `<span class="compare-score-objective">${escapeHtml(objectiveA)}</span>` : ""}
        ${cA ? `<span class="compare-score-assertions">${escapeHtml(assertionsA)} assertions</span>` : ""}
        <span class="compare-score-verdict">${okA ? "contract passed" : "contract failed"}</span>
      </div>`;
      html += `<div class="compare-score-delta ${delta > 0 ? "delta-pos" : delta < 0 ? "delta-neg" : ""}">
        ${escapeHtml(deltaLabel)}
      </div>`;
      html += `<div class="compare-score-cell ${okB ? "compare-pass" : "compare-fail"}">
        <span class="compare-score-label">${escapeHtml(nameB)}</span>
        <span class="compare-score-value">${escapeHtml(scoreLabelB)}</span>
        <span class="compare-score-caption">${escapeHtml(scoreCaptionB)}</span>
        ${objectiveB ? `<span class="compare-score-objective">${escapeHtml(objectiveB)}</span>` : ""}
        ${cB ? `<span class="compare-score-assertions">${escapeHtml(assertionsB)} assertions</span>` : ""}
        <span class="compare-score-verdict">${okB ? "contract passed" : "contract failed"}</span>
      </div>`;
      html += `</div>`;

      const issuesA = [...(cA?.dynamic_validation?.issues || []), ...(cA?.static_validation?.issues || [])];
      const issuesB = [...(cB?.dynamic_validation?.issues || []), ...(cB?.static_validation?.issues || [])];
      const failedInA = new Set(issuesA.map((i) => i.predicate_name).filter(Boolean));
      const failedInB = new Set(issuesB.map((i) => i.predicate_name).filter(Boolean));
      const onlyFailedInA = [...failedInA].filter((n) => !failedInB.has(n));
      const onlyFailedInB = [...failedInB].filter((n) => !failedInA.has(n));

      if (onlyFailedInA.length || onlyFailedInB.length) {
        html += `<div class="compare-divergence">`;
        html += `<p class="eyebrow">Key divergence</p>`;
        if (onlyFailedInA.length) {
          html += `<div class="compare-div-group"><span class="compare-div-label">${escapeHtml(nameA)} missed:</span>`;
          html += onlyFailedInA.map((n) => {
            const issue = issuesA.find((i) => i.predicate_name === n);
            return `<span class="compare-div-item">${escapeHtml(issue?.message || n)}</span>`;
          }).join("");
          html += `</div>`;
        }
        if (onlyFailedInB.length) {
          html += `<div class="compare-div-group"><span class="compare-div-label">${escapeHtml(nameB)} missed:</span>`;
          html += onlyFailedInB.map((n) => {
            const issue = issuesB.find((i) => i.predicate_name === n);
            return `<span class="compare-div-item">${escapeHtml(issue?.message || n)}</span>`;
          }).join("");
          html += `</div>`;
        }
        html += `</div>`;
      }
    }
  }
  html += `</div>`;

  const allRuns = state.runs || [];
  const runOptions = allRuns.map((r) => {
    const label = r.runner || r.run_id?.split("_").slice(0, 2).join("_") || r.run_id;
    const status = r.status ? ` [${r.status}]` : "";
    return `<option value="${escapeHtml(r.run_id)}">${escapeHtml(label)}${escapeHtml(status)}</option>`;
  }).join("");

  html += `<div class="tl-compare-header">`;
  if (allRuns.length > 2) {
    html += `<select id="compare-picker-a" class="compare-run-picker">${runOptions}</select>`;
    html += `<span class="tl-compare-vs">vs</span>`;
    html += `<select id="compare-picker-b" class="compare-run-picker">${runOptions}</select>`;
  } else {
    html += `<span class="tl-compare-pill tl-pill-a">${escapeHtml(nameA)} (${stepsA} steps)</span>`;
    html += `<span class="tl-compare-vs">vs</span>`;
    html += `<span class="tl-compare-pill tl-pill-b">${escapeHtml(nameB)} (${stepsB} steps)</span>`;
  }
  html += `<button id="compare-diff-btn" class="compare-diff-btn">Diff world state</button>`;
  html += `</div>`;
  html += `<div id="compare-diff-result" class="compare-diff-result"></div>`;
  html += `<div class="tl-compare-grid">`;
  html += `<div class="tl-compare-labels"><div class="tl-corner"></div>`;
  for (const s of allSurfaces) {
    html += `<div class="tl-lane-label"><span class="tl-lane-dot" style="background:${SURFACE_ACCENT[s] || "#888"}"></span>${escapeHtml(surfaceLabel(s))}</div>`;
  }
  html += `</div>`;
  html += `<div class="tl-compare-col tl-compare-a"><div class="tl-col-header">${escapeHtml(nameA)}</div>`;
  for (const s of allSurfaces) {
    html += `<div class="tl-cell"><div class="tl-compare-bar-fill" style="width:${Math.min(100, (hitsA[s] || 0) * 20)}%;background:${SURFACE_ACCENT[s] || "#888"}"></div><span>${hitsA[s] || 0}</span></div>`;
  }
  html += `</div>`;
  html += `<div class="tl-compare-col tl-compare-b"><div class="tl-col-header">${escapeHtml(nameB)}</div>`;
  for (const s of allSurfaces) {
    html += `<div class="tl-cell"><div class="tl-compare-bar-fill" style="width:${Math.min(100, (hitsB[s] || 0) * 20)}%;background:${SURFACE_ACCENT[s] || "#888"}"></div><span>${hitsB[s] || 0}</span></div>`;
  }
  html += `</div></div>`;
  return html;
}

function renderLivingCompanyView() {
  if (state.timelineMode) {
    renderTimelineView();
    return;
  }
  renderLivingCompanyContext();
  renderSituationRoom();
  renderMirrorFleetPanel();
  renderLivingCompanyImpact();
  renderSurfaceWall();
  renderLivingCompanyRail();
  updateContextHint();
  if (state.cinemaMode) renderCinemaNarrative();
}

function renderLivingCompanyContext() {
  const panel = document.getElementById("living-company-context");
  if (!panel) return;
  const story = state.story || {};
  const companyName = story.manifest?.company_name || state.workspace?.manifest?.title || "";
  const briefing = currentCrisisSummary();
  const crisisTitle = currentCrisisTitle();
  const failureImpact = currentFailureImpact();
  if (!companyName) {
    panel.innerHTML = "";
    return;
  }
  const crisisLine = crisisTitle
    ? `<strong>${escapeHtml(crisisTitle)}</strong>: ${escapeHtml(briefing)}`
    : "";
  panel.innerHTML = `
    <div class="context-strip">
      <div class="context-strip-company">
        <strong>${escapeHtml(companyName)}</strong> &mdash; ${escapeHtml(briefing)}
      </div>
      ${crisisLine ? `<div class="context-strip-crisis">${crisisLine}</div>` : ""}
      ${failureImpact ? `<div class="context-strip-stakes">${escapeHtml(failureImpact)}</div>` : ""}
    </div>
  `;
}

function renderLivingCompanyImpact() {
  const panel = document.getElementById("living-company-impact");
  if (!panel) {
    return;
  }
  const impact = state.lastMoveImpact;
  if (!impact || !impact.systems?.length) {
    panel.innerHTML = "";
    panel.classList.remove("impact-visible");
    return;
  }
  panel.classList.add("impact-visible");
  panel.innerHTML = `
    <div class="impact-ribbon">
      <div class="impact-ribbon-copy">
        <span class="metric-label">Last move</span>
        <strong>${escapeHtml(impact.title || "Move applied")}</strong>
        <p class="metric-detail">${escapeHtml(impact.summary || "The company state shifted after the last move.")}</p>
      </div>
      <div class="impact-ribbon-meta">
        <span class="impact-ribbon-label">Systems hit</span>
        <div class="chip-row">
          ${impact.systems.map((item) => chip(formatSurfaceTitle(item), "ok")).join("")}
        </div>
        ${
          impact.items?.length
            ? `<div class="chip-row">${impact.items.slice(0, 3).map((item) => chip(item.title)).join("")}</div>`
            : impact.refs?.length
              ? `<div class="chip-row">${impact.refs.slice(0, 4).map((item) => chip(item)).join("")}</div>`
              : ""
        }
      </div>
    </div>
  `;
}

function updateContextHint() {
  const hint = document.getElementById("shell-context-hint");
  if (!hint) {
    return;
  }
  const ms = state.missionState;
  if (ms?.status === "completed") {
    hint.textContent = "Mission complete \u2014 branch the outcome or start a new crisis";
  } else if (ms?.run_id) {
    const moveCount = (ms.executed_moves || []).length;
    hint.textContent = moveCount
      ? `${moveCount} move${moveCount === 1 ? "" : "s"} played \u2014 pick the next action or finish`
      : "You\u2019re in the world \u2014 play your first move below";
  } else if (state.missions.length) {
    hint.textContent = "Pick a crisis above, then watch every system react";
  } else {
    hint.textContent = "Loading company world\u2026";
  }
}

function renderSituationRoom() {
  const el = document.getElementById("situation-room-strip");
  if (!el) return;
  const ss = state.surfaceState;
  const ms = state.missionState;
  if (!ss || !Array.isArray(ss.panels) || !ss.panels.length) {
    el.innerHTML = "";
    return;
  }

  let attentionCount = 0;
  let warningCount = 0;
  let criticalCount = 0;
  let totalSystems = ss.panels.length;
  ss.panels.forEach((p) => {
    const s = (p.status || "").toLowerCase();
    if (s === "critical") criticalCount++;
    else if (s === "warning") warningCount++;
    else if (s === "attention") attentionCount++;
  });

  const servicePanel = ss.panels.find((p) => p.kind === "vertical_heartbeat" || p.surface === "service_ops");
  let exceptionCount = 0;
  let highSeverity = false;
  let disputeAmount = "";
  let pendingApprovals = 0;
  if (servicePanel && Array.isArray(servicePanel.items)) {
    servicePanel.items.forEach((item) => {
      const badges = Array.isArray(item.badges) ? item.badges : [];
      const isException = badges.some((b) => b === "high" || b === "mitigated" || b === "open" || b === "resolved");
      const isBilling = (item.title || "").toLowerCase().includes("billing");
      const isApproval = false;
      if (isException && !isBilling) { exceptionCount++; if (badges.includes("high")) highSeverity = true; }
      if (isBilling && badges.includes("open")) disputeAmount = "at risk";
    });
  }

  const approvalPanel = ss.panels.find((p) => p.surface === "approvals");
  if (approvalPanel && Array.isArray(approvalPanel.items)) {
    pendingApprovals = approvalPanel.items.filter((item) => {
      const s = (item.status || "").toLowerCase();
      return s === "pending_approval" || s === "in_progress" || s === "review";
    }).length;
  }

  const sc = ms?.scorecard;
  const policyStatus = sc?.policy_correctness || "unknown";
  const deadlineStatus = sc?.deadline_pressure || "unknown";
  const riskLevel = sc?.business_risk || "unknown";

  const healthClass = criticalCount > 0 ? "sit-danger" : warningCount > 0 ? "sit-warn" : "sit-ok";
  const healthLabel = criticalCount > 0 ? `${criticalCount} critical` : warningCount > 0 ? `${warningCount} warning` : "all nominal";

  const exClass = exceptionCount > 0 && highSeverity ? "sit-danger" : exceptionCount > 0 ? "sit-warn" : "sit-ok";
  const policyClass = policyStatus === "drifting" ? "sit-danger" : policyStatus === "sound" ? "sit-ok" : "";
  const deadlineClass = deadlineStatus === "critical" ? "sit-danger" : deadlineStatus === "compressed" ? "sit-warn" : "sit-ok";
  const approvalClass = pendingApprovals > 0 ? "sit-warn" : "sit-ok";
  const riskClass = riskLevel === "high" ? "sit-danger" : riskLevel === "moderate" ? "sit-warn" : "sit-ok";

  el.innerHTML = `
    <div class="sit-room-cell ${healthClass}">
      <span class="sit-label">Systems</span>
      <span class="sit-value">${totalSystems}</span>
      <span class="sit-detail">${healthLabel}</span>
    </div>
    <div class="sit-room-cell ${exClass}">
      <span class="sit-label">Exceptions</span>
      <span class="sit-value">${exceptionCount}</span>
      <span class="sit-detail">${highSeverity ? "high severity" : exceptionCount ? "active" : "clear"}</span>
    </div>
    <div class="sit-room-cell ${policyClass}">
      <span class="sit-label">Policy</span>
      <span class="sit-value">${policyStatus}</span>
      <span class="sit-detail">${policyStatus === "sound" ? "no overrides" : policyStatus === "drifting" ? "rules changed" : ""}</span>
    </div>
    <div class="sit-room-cell ${approvalClass}">
      <span class="sit-label">Approvals</span>
      <span class="sit-value">${pendingApprovals}</span>
      <span class="sit-detail">${pendingApprovals ? "pending" : "clear"}</span>
    </div>
    <div class="sit-room-cell ${deadlineClass}">
      <span class="sit-label">Deadline</span>
      <span class="sit-value">${deadlineStatus}</span>
      <span class="sit-detail">${sc ? `budget: ${sc.action_budget_remaining}` : ""}</span>
    </div>
    <div class="sit-room-cell ${riskClass}">
      <span class="sit-label">Risk</span>
      <span class="sit-value">${riskLevel}</span>
      <span class="sit-detail">${sc ? `score: ${sc.overall_score}` : ""}</span>
    </div>
  `;
}

function renderMirrorFleetPanel() {
  const el = document.getElementById("mirror-fleet-strip");
  if (!el) return;
  const mirror = state.mirrorStatus;
  if (!mirror || !mirror.config) {
    el.innerHTML = "";
    return;
  }
  const agents = Array.isArray(mirror.agents) ? mirror.agents : [];
  if (!agents.length && !mirror.config.demo_mode) {
    el.innerHTML = "";
    return;
  }

  const mode = mirror.config.connector_mode === "live"
    ? "live"
    : (mirror.config.demo_mode ? "demo" : "sim");
  const badgeClass = mode === "live"
    ? "fleet-badge-live"
    : (mode === "demo" ? "fleet-badge-demo" : "fleet-badge-sim");
  const eventCount = mirror.event_count || 0;
  const pending = mirror.pending_demo_steps || 0;
  const autoplay = mirror.autoplay_running ? "autoplay" : "manual";

  const agentCards = agents.map((agent) => {
    const statusClass = `agent-status-${agent.status || "registered"}`;
    const role = agent.role ? agent.role.replace(/_/g, " ") : agent.mode || "agent";
    const surfaces = Array.isArray(agent.allowed_surfaces) ? agent.allowed_surfaces.join(", ") : "";
    const lastAction = agent.last_action ? `<span class="agent-last-action">${escapeHtml(agent.last_action)}</span>` : "";
    const deniedBadge = (agent.denied_count || 0) > 0
      ? `<span class="agent-denied-badge">${agent.denied_count} denied</span>`
      : "";
    return `
      <div class="mirror-agent-card">
        <span class="agent-name">${escapeHtml(agent.name || agent.agent_id)}${deniedBadge}</span>
        <span class="agent-role">${escapeHtml(role)}${surfaces ? " · " + escapeHtml(surfaces) : ""}</span>
        <span class="agent-status ${statusClass}">${agent.status || "registered"}</span>
        ${lastAction}
      </div>
    `;
  }).join("");

  const recentEvents = Array.isArray(mirror.recent_events) ? mirror.recent_events : [];
  let eventFeedHtml = "";
  if (recentEvents.length > 0) {
    const feedItems = recentEvents.slice(-10).reverse().map((evt) => {
      const denied = evt.handled_by === "denied";
      const cls = denied ? "mirror-feed-item mirror-feed-denied" : "mirror-feed-item";
      const label = evt.label || evt.tool || "event";
      return `<div class="${cls}"><span class="feed-agent">${escapeHtml(evt.agent_id)}</span><span class="feed-label">${escapeHtml(label)}</span>${denied ? '<span class="feed-denied-tag">denied</span>' : ""}</div>`;
    }).join("");
    eventFeedHtml = `
      <details class="mirror-event-feed">
        <summary class="mirror-feed-toggle">Mirror Activity (${recentEvents.length})</summary>
        <div class="mirror-feed-list">${feedItems}</div>
      </details>
    `;
  }

  el.innerHTML = `
    <div class="mirror-fleet-header">
      <span class="fleet-label">Agent Fleet</span>
      <span class="fleet-badge ${badgeClass}">${mode} mode</span>
    </div>
    ${agentCards}
    <div class="mirror-fleet-stats">
      <span class="stat-label">Events</span>
      <span class="stat-value">${eventCount}</span>
      <span class="stat-detail">${pending ? pending + " queued" : autoplay}</span>
    </div>
    ${eventFeedHtml}
  `;
}

function renderSurfaceWall() {
  const panel = document.getElementById("living-company-surface-wall");
  if (!panel) {
    return;
  }
  const surfaceState = state.surfaceState;
  if (!surfaceState || !Array.isArray(surfaceState.panels) || !surfaceState.panels.length) {
    const loadingRun = state.missionState?.run_id || state.activeRunId;
    panel.innerHTML = `
      <div class="surface-placeholder">
        <p class="eyebrow">Living Company</p>
        <h3>${loadingRun ? "Loading company systems" : "Enter a world to see its tools"}</h3>
        <p class="metric-detail">${
          loadingRun
            ? "Loading the latest company state so the tools can appear here."
            : "Slack, email, tickets, docs, approvals, and the vertical business system will appear here once a run is active."
        }</p>
      </div>
    `;
    return;
  }

  const impact = state.lastMoveImpact;
  const highlightPanels = Array.isArray(state.surfaceHighlights?.panels) && state.surfaceHighlights.panels.length
    ? state.surfaceHighlights.panels
    : impact?.systems || [];
  const highlightRefs = Array.isArray(state.surfaceHighlights?.refs) && state.surfaceHighlights.refs.length
    ? state.surfaceHighlights.refs
    : (impact?.items || []).map((item) => item.ref).filter(Boolean);
  const changedPanels = new Set(highlightPanels);
  const changedRefs = new Set(highlightRefs);
  const isCascade = state.cascadeActive && changedPanels.size > 0;

  panel.innerHTML = surfaceState.panels
    .map((surfacePanel) => {
      const changed = changedPanels.has(surfacePanel.surface);
      const pendingClass = isCascade && changed ? "cascade-pending" : "";
      const changedClass = !isCascade && changed ? "surface-changed" : "";
      const changedItemCount = changed
        ? (surfacePanel.items || []).filter((it) => it.highlight_ref && changedRefs.has(it.highlight_ref)).length
        : 0;
      const toastText = changed && changedItemCount
        ? `${changedItemCount} item${changedItemCount === 1 ? "" : "s"} changed`
        : changed ? "Updated" : "";
      return `
        <article
          class="surface-panel surface-panel-${escapeHtml(surfacePanel.kind)} ${pendingClass} ${changedClass}"
          data-surface="${escapeHtml(surfacePanel.surface)}"
          ${surfacePanel.accent ? `style="--panel-accent:${escapeHtml(surfacePanel.accent)}"` : ""}
        >
          ${toastText ? `<div class="cascade-toast">${escapeHtml(toastText)}</div>` : ""}
          <header class="surface-panel-header">
            <div>
              <p class="eyebrow">${escapeHtml(formatSurfaceTitle(surfacePanel.surface))}</p>
              <h3>${escapeHtml(surfacePanel.title)}</h3>
            </div>
            <div class="surface-panel-meta">
              ${surfacePanel.status ? chip(surfacePanel.status, statusClass(surfacePanel.status)) : ""}
              ${changed ? `<span class="surface-updated-tag">updated</span>` : ""}
            </div>
          </header>
          ${surfacePanel.headline ? `<p class="surface-headline">${escapeHtml(surfacePanel.headline)}</p>` : ""}
          <div class="surface-items">
            ${(surfacePanel.items || [])
              .map((item) => renderSurfaceItem(surfacePanel, item, changedRefs))
              .join("")}
          </div>
        </article>
      `;
    })
    .join("");
}

function renderSurfaceItem(surfacePanel, item, changedRefs) {
  const changed = item.highlight_ref && changedRefs.has(item.highlight_ref);
  const badges = Array.isArray(item.badges) ? item.badges : [];
  return `
    <div class="surface-item ${changed ? "surface-item-changed" : ""}">
      <div class="surface-item-topline">
        <strong>${escapeHtml(item.title || item.item_id)}</strong>
        ${item.status ? `<span class="surface-item-status ${statusClass(item.status)}">${escapeHtml(item.status)}</span>` : ""}
      </div>
      ${item.subtitle ? `<div class="surface-item-subtitle">${escapeHtml(item.subtitle)}</div>` : ""}
      ${item.body ? `<p class="surface-item-body">${escapeHtml(item.body)}</p>` : ""}
      ${badges.length ? `<div class="chip-row">${badges.map((badgeValue) => chip(badgeValue)).join("")}</div>` : ""}
    </div>
  `;
}

function renderLivingCompanyRail() {
  const panel = document.getElementById("living-company-rail");
  if (!panel) {
    return;
  }
  const missionState = state.missionState;
  const mission = activeMission();
  const objective = missionState?.objective_variant
    || document.getElementById("objective-select")?.value
    || mission?.default_objective
    || state.scenarioPreview?.active_contract_variant
    || "default";
  const recommendedMove = (missionState?.available_moves || []).find(
    (move) => move.availability === "recommended" && !move.executed
  ) || (missionState?.available_moves || []).find((move) => !move.executed) || null;
  const blockedMove = (missionState?.available_moves || []).find(
    (move) => move.availability === "blocked" && !move.executed
  ) || null;
  const latestToolEvent = [...(state.timeline || [])].reverse().find(
    (event) => event.resolved_tool || event.graph_intent
  );
  const impact = state.lastMoveImpact;
  const surfaceState = state.surfaceState;

  panel.innerHTML = `
    <div class="story-card accent-card">
      <p class="eyebrow">Current tension</p>
      <h3>${escapeHtml(surfaceState?.company_name || state.story?.manifest?.company_name || state.workspace?.manifest?.title || "Company")}</h3>
      <p class="metric-detail">${escapeHtml(currentCrisisSummary())}</p>
    </div>
    <div class="story-card">
      <p class="eyebrow">Situation</p>
      <h3>${escapeHtml(currentCrisisTitle())}</h3>
      <div class="detail-grid">
        ${detailTile("Success means", objective)}
        ${detailTile("Branch", state.activeRun?.branch || missionState?.branch_name || "base")}
      </div>
      ${currentFailureImpact() ? `<p class="metric-detail">${escapeHtml(currentFailureImpact())}</p>` : ""}
    </div>
    ${
      blockedMove
        ? `
          <div class="story-card">
            <p class="eyebrow">What is blocked</p>
            <h3>${escapeHtml(blockedMove.title)}</h3>
            <p class="metric-detail">${escapeHtml(blockedMove.blocked_reason || "This path is blocked until another system changes.")}</p>
          </div>
        `
        : ""
    }
    ${
      recommendedMove
        ? `
          <div class="story-card">
            <p class="eyebrow">Next move</p>
            <h3>${escapeHtml(recommendedMove.title)}</h3>
            <p class="metric-detail">${escapeHtml(recommendedMove.consequence_preview || recommendedMove.summary || "")}</p>
          </div>
        `
        : ""
    }
    ${
      impact
        ? `
          <div class="story-card">
            <p class="eyebrow">What changed</p>
            <h3>${escapeHtml(impact.title || "Last move")}</h3>
            <p class="metric-detail">${escapeHtml(impact.summary || "The company state changed after the last move.")}</p>
            <div class="chip-row">${(impact.systems || []).map((item) => chip(formatSurfaceTitle(item), "ok")).join("")}</div>
            ${impact.items?.length ? `<div class="chip-row">${impact.items.slice(0, 2).map((item) => chip(item.title)).join("")}</div>` : ""}
          </div>
        `
        : ""
    }
    ${
      latestToolEvent
        ? `
          <div class="story-card">
            <p class="eyebrow">Latest tool</p>
            <h3>${escapeHtml(latestToolEvent.resolved_tool || "waiting")}</h3>
            <p class="metric-detail">${escapeHtml(latestToolEvent.summary || "The latest action is recorded in the run trail.")}</p>
          </div>
        `
        : ""
    }
  `;
}

function diffSurfaceState(before, after) {
  if (!before || !after) {
    return { panels: [], refs: [], items: [] };
  }
  const beforePanels = new Map((before.panels || []).map((panel) => [panel.surface, panel]));
  const afterPanels = new Map((after.panels || []).map((panel) => [panel.surface, panel]));
  const changedPanels = [];
  const changedRefs = [];
  const changedItems = [];

  for (const [surface, panel] of afterPanels.entries()) {
    const previous = beforePanels.get(surface);
    const currentSignature = JSON.stringify(normalizeSurfacePanel(panel));
    const previousSignature = previous ? JSON.stringify(normalizeSurfacePanel(previous)) : "";
    if (currentSignature === previousSignature) {
      continue;
    }
    changedPanels.push(surface);
    const previousItems = new Map(
      ((previous && previous.items) || []).map((item) => [item.item_id, JSON.stringify(item)])
    );
    let highlighted = false;
    for (const item of panel.items || []) {
      const signature = JSON.stringify(item);
      if (previousItems.get(item.item_id) !== signature) {
        if (item.highlight_ref) {
          changedRefs.push(item.highlight_ref);
          highlighted = true;
        }
        changedItems.push({
          surface,
          ref: item.highlight_ref || "",
          title: item.title || item.item_id || formatSurfaceTitle(surface),
        });
      }
    }
    if (!highlighted) {
      const fallback = (panel.items || []).find((item) => item.highlight_ref);
      if (fallback) {
        changedRefs.push(fallback.highlight_ref);
        changedItems.push({
          surface,
          ref: fallback.highlight_ref,
          title: fallback.title || fallback.item_id || formatSurfaceTitle(surface),
        });
      }
    }
  }

  return {
    panels: changedPanels,
    refs: [...new Set(changedRefs)],
    items: changedItems.slice(0, 6),
  };
}

function normalizeSurfacePanel(panel) {
  return {
    surface: panel.surface,
    headline: panel.headline,
    status: panel.status,
    items: (panel.items || []).map((item) => ({
      item_id: item.item_id,
      title: item.title,
      subtitle: item.subtitle,
      body: item.body,
      status: item.status,
      badges: item.badges,
    })),
  };
}

function buildMoveImpact(moveSpec, diff) {
  const executedMove = latestExecutedMove();
  const systems = uniqueStrings(
    (diff?.panels || []).length
      ? diff.panels
      : guessMoveTargets(moveSpec || executedMove || {})
  );
  const items = Array.isArray(diff?.items)
    ? diff.items.filter((item) => item && item.title)
    : [];
  const refs = uniqueStrings([
    ...(diff?.refs || []),
    ...((executedMove?.object_refs || []).slice(0, 6)),
  ]);
  const summary =
    moveSpec?.consequence_preview ||
    executedMove?.payload?.observation?.summary ||
    executedMove?.payload?.scenario_brief ||
    executedMove?.summary ||
    moveSpec?.summary ||
    buildImpactSummary(systems, items);
  return {
    title: executedMove?.title || moveSpec?.title || "Move applied",
    summary,
    systems,
    refs,
    items: items.slice(0, 3),
    tool: executedMove?.resolved_tool || "",
  };
}

function buildImpactSummary(systems, items) {
  const firstItem = items[0];
  if (firstItem?.title) {
    return `${formatSurfaceTitle(firstItem.surface)} now shows ${firstItem.title}.`;
  }
  if (systems.length === 1) {
    return `${formatSurfaceTitle(systems[0])} reacted to the last move.`;
  }
  if (systems.length > 1) {
    return `${systems.map((item) => formatSurfaceTitle(item)).join(", ")} reacted to the last move.`;
  }
  return "The company state shifted after the last move.";
}

function renderMissionPlay() {
  const panel = document.getElementById("mission-moves-panel");
  const scorecard = document.getElementById("mission-scorecard");
  const status = document.getElementById("mission-form-status");
  if (!panel || !scorecard || !status) {
    return;
  }
  const missionState = state.missionState;
  if (!missionState) {
    state.lastMoveImpact = null;
    scorecard.innerHTML = `
      <div class="story-card story-span-2">
        <p class="eyebrow">Play</p>
        <p class="metric-detail">${
          hasExerciseMode()
            ? "Apply a crisis above, then connect an outside agent or use the operator console to watch the company respond."
            : "Choose a situation and enter the world to begin making moves inside the company."
        }</p>
      </div>
    `;
    panel.innerHTML = "";
    renderJson("mission-state-panel", {});
    return;
  }
  const score = missionState.scorecard || {};
  const budgetTotal = (score.move_count || 0) + (score.action_budget_remaining || 0);
  const budgetPct = budgetTotal > 0 ? Math.round(((score.action_budget_remaining || 0) / budgetTotal) * 100) : 100;
  const scorePct = Math.min(100, Math.max(0, score.overall_score || 0));
  const pressureClass = score.deadline_pressure === "critical" ? "pressure-critical" : score.deadline_pressure === "compressed" ? "pressure-compressed" : "";
  const riskClass = score.business_risk === "high" ? "pressure-critical" : score.business_risk === "moderate" ? "pressure-compressed" : "";
  scorecard.innerHTML = `
    <div class="score-strip">
      <div class="score-pill">
        <span class="metric-label">Score</span>
        <strong>${scorePct}</strong>
        <div class="score-bar"><div class="score-bar-fill ${scorePct >= 70 ? "bar-ok" : scorePct >= 40 ? "bar-warn" : "bar-danger"}" style="width:${scorePct}%"></div></div>
      </div>
      ${scorePill("Mission", score.mission_success === null ? "pending" : score.mission_success ? "pass" : "in play")}
      <div class="score-pill ${pressureClass}">
        <span class="metric-label">Budget left</span>
        <strong>${score.action_budget_remaining || 0}</strong>
        <div class="score-bar"><div class="score-bar-fill ${budgetPct >= 50 ? "bar-ok" : budgetPct >= 25 ? "bar-warn" : "bar-danger"}" style="width:${budgetPct}%"></div></div>
      </div>
      <div class="score-pill ${riskClass}">
        <span class="metric-label">Risk</span>
        <strong>${escapeHtml(score.business_risk || "moderate")}</strong>
      </div>
      <div class="score-pill ${pressureClass}">
        <span class="metric-label">Deadline</span>
        <strong>${escapeHtml(score.deadline_pressure || "stable")}</strong>
      </div>
      ${scorePill("Policy", score.policy_correctness || "sound")}
    </div>
    <div class="briefing-grid">
      <div class="story-card accent-card">
        <p class="eyebrow">Mission health</p>
        <h3>${escapeHtml(score.summary || "Mission active.")}</h3>
        <div class="detail-grid">
          ${detailTile("Moves used", String(score.move_count || 0))}
          ${detailTile("Assertions", `${score.success_assertions_passed || 0}/${score.success_assertions_total || 0}`)}
          ${detailTile("Issues", String(score.contract_issue_count || 0))}
          ${detailTile("Run id", missionState.run_id)}
        </div>
      </div>
      <div class="story-card">
        <p class="eyebrow">Branch labels</p>
        <div class="chip-row">${(missionState.mission?.branch_labels || []).map((item) => chip(item)).join("")}</div>
      </div>
    </div>
  `;
  panel.innerHTML = (missionState.available_moves || [])
    .map(
      (move) => `
        <div class="run-item ${move.availability === "recommended" ? "active" : ""}">
          <div class="chip-row">
            ${chip(move.availability, move.availability === "blocked" ? "chip-error" : move.availability === "risky" ? "chip-risky" : "chip-ok")}
            ${move.executed ? chip("used") : ""}
          </div>
          <h3>${escapeHtml(move.title)}</h3>
          <p class="metric-detail">${escapeHtml(move.summary || "")}</p>
          <p class="metric-detail">${escapeHtml(move.consequence_preview || "")}</p>
          ${move.blocked_reason ? `<p class="metric-detail">${escapeHtml(move.blocked_reason)}</p>` : ""}
          <button
            type="button"
            class="ghost-button play-move-button"
            data-move-id="${escapeHtml(move.move_id)}"
            ${move.availability === "blocked" ? "disabled" : ""}
          >${move.availability === "risky" ? "Take risky move" : "Play move"}</button>
        </div>
      `
    )
    .join("");
  panel.querySelectorAll(".play-move-button").forEach((node) => {
    node.addEventListener("click", () => {
      const moveId = node.dataset.moveId;
      const move = (state.missionState?.available_moves || []).find((m) => m.move_id === moveId);
      if (move && move.availability === "risky") {
        showPolicyChangeModal(move);
      } else {
        void applyMissionMove(moveId);
      }
    });
  });
  status.textContent =
    missionState.status === "completed"
      ? "Mission finished. Branch it, inspect the outcome, or switch to a new situation."
      : "Play a move, branch the situation, or finish the run.";
  renderMoveLog();
  renderJson("mission-state-panel", missionState);
  renderLivingCompanyView();
}

function renderMoveLog() {
  const panel = document.getElementById("move-log-panel");
  if (!panel) {
    return;
  }
  const missionState = state.missionState;
  const executed = missionState?.executed_moves || [];
  if (!executed.length) {
    panel.innerHTML = `<p class="metric-detail">No moves played yet. Each move you take will appear here with the tool that fired, the objects it touched, and what the world looked like afterward.</p>`;
    return;
  }
  panel.innerHTML = executed
    .map((move, index) => {
      const intentParts = (move.graph_intent || "").split(".", 1);
      const domain = intentParts[0] || "";
      const refs = (move.object_refs || []).slice(0, 5);
      const obs = move.payload?.observation || {};
      const obsSummary = obs.summary || obs.scenario_brief || "";
      const result = move.payload?.result || {};
      const resultKeys = Object.keys(result).slice(0, 4);
      const isRisky = move.payload?.availability === "risky";
      return `
        <div class="move-log-entry ${isRisky ? "decision-entry-risky" : ""}">
          <div class="move-log-number">${index + 1}</div>
          <div class="move-log-body">
            <div class="chip-row">
              ${move.resolved_tool ? chip(move.resolved_tool, "ok") : ""}
              ${domain ? chip(formatDomainTitle(domain)) : ""}
              ${move.time_ms ? chip(`t=${formatMs(move.time_ms)}`) : ""}
              ${isRisky ? chip("policy override", "chip-risky") : ""}
            </div>
            <h3>${escapeHtml(move.title)}</h3>
            <p class="metric-detail">${escapeHtml(move.summary || "")}</p>
            ${refs.length ? `<div class="chip-row">${refs.map((ref) => chip(ref)).join("")}</div>` : ""}
            ${resultKeys.length ? `<div class="move-log-result"><strong>Result:</strong> ${resultKeys.map((key) => `${escapeHtml(key)}=${escapeHtml(summarizeMoveValue(result[key]))}`).join(" · ")}</div>` : ""}
            ${obsSummary ? `<div class="move-log-observation">${escapeHtml(String(obsSummary).slice(0, 200))}</div>` : ""}
          </div>
        </div>
      `;
    })
    .join("");
}

function renderFidelityPanel() {
  const panel = document.getElementById("fidelity-panel");
  if (!panel) {
    return;
  }
  const report = state.fidelityReport;
  if (!report || !Array.isArray(report.cases) || !report.cases.length) {
    panel.innerHTML = `
      <div class="story-card story-span-2">
        <p class="eyebrow">System checks</p>
        <p class="metric-detail">System health checks appear here once a fidelity report is available.</p>
      </div>
    `;
    renderJson("fidelity-raw-panel", {});
    return;
  }
  panel.innerHTML = `
    <div class="story-card accent-card story-span-2">
      <p class="eyebrow">Report summary</p>
      <h3>${escapeHtml(report.company_name || "Company")} · ${chip(report.status, statusClass(report.status))}</h3>
      <p class="metric-detail">${escapeHtml(report.summary || "System checks ran against the key tools this company depends on.")}</p>
    </div>
  ` + report.cases
    .map(
      (item) => `
        <div class="story-card">
          <p class="eyebrow">${escapeHtml(item.surface)}</p>
          <h3>${escapeHtml(item.title)}</h3>
          <p class="metric-detail">${escapeHtml(item.boundary_contract || "")}</p>
          ${item.why_it_matters ? `<p class="metric-detail"><em>${escapeHtml(item.why_it_matters)}</em></p>` : ""}
          <div class="chip-row">
            ${chip(item.status, statusClass(item.status))}
            ${item.resolved_tool ? chip(item.resolved_tool) : ""}
          </div>
          <div class="stack">
            ${(item.checks || [])
              .map(
                (check) => `
                  <p class="metric-detail"><strong>${escapeHtml(check.name)}</strong>: ${escapeHtml(check.summary)}</p>
                `
              )
              .join("")}
          </div>
        </div>
      `
    )
    .join("");
  renderJson("fidelity-raw-panel", report);
}

function renderImportSummary() {
  const summary = state.importSummary;
  const identityFlow = state.identityFlow;
  const normalization = state.importNormalization;
  const review = state.importReview;
  const reconciliation = normalization?.identity_reconciliation || review?.normalization_report?.identity_reconciliation;
  const panel = document.getElementById("imports-summary");
  const generatedPanel = document.getElementById("generated-scenarios");
  const reviewPanel = document.getElementById("import-review-grid");
  const provenancePanel = document.getElementById("provenance-detail");
  const sourceRegistry = Array.isArray(state.importSources?.sources) ? state.importSources.sources : [];
  const sourceSyncs = Array.isArray(state.importSources?.syncs) ? state.importSources.syncs : [];
  const hasImportPackage =
    summary &&
    normalization &&
    Object.keys(summary).length > 0 &&
    Object.keys(normalization).length > 0 &&
    (summary.package_name || normalization.package_name);
  if (!hasImportPackage) {
    panel.innerHTML = `<div class="metric-tile"><span class="metric-label">Import</span><span class="metric-value">No import package</span></div>`;
    generatedPanel.innerHTML = "";
    reviewPanel.innerHTML = "";
    provenancePanel.innerHTML = "";
    renderJson("imports-panel", {});
    renderJson("provenance-panel", []);
    return;
  }

  panel.innerHTML = [
    metricTile("Package", summary.package_name || "import", `${summary.source_count || 0} sources`),
    metricTile("Connected", String(summary.connected_source_count || 0), `${summary.source_sync_count || 0} syncs`),
    metricTile("Issues", String(summary.issue_count || 0), `${summary.warning_count || 0} warnings · ${summary.error_count || 0} errors`),
    metricTile("Provenance", String(summary.provenance_count || 0), `generated scenarios: ${summary.generated_scenario_count || 0}`),
    metricTile("Overrides", String((review?.source_overrides || []).length), `${(review?.normalization_report?.source_summaries || []).filter((item) => item.override_applied).length} sources adapted`),
    metricTile(
      "Origins",
      `${summary.origin_counts?.imported || 0}/${summary.origin_counts?.derived || 0}/${summary.origin_counts?.simulated || 0}`,
      "imported / derived / simulated"
    ),
    reconciliation
      ? metricTile(
          "Reconciliation",
          `${compactNumber(reconciliation.resolved_count || 0)}/${compactNumber(reconciliation.subject_count || 0)}`,
          `${compactNumber(reconciliation.ambiguous_count || 0)} ambiguous · ${compactNumber(reconciliation.unmatched_count || 0)} unmatched`
        )
      : "",
  ].join("");

  generatedPanel.innerHTML = state.generatedImportScenarios
    .map(
      (scenario) => `
        <div class="run-item">
          <div class="chip-row">
            ${chip(scenario.workflow_name)}
            ${chip(scenario.workflow_variant || "default")}
            ${scenario.metadata?.priority ? chip(`priority:${scenario.metadata.priority}`, statusClass(scenario.metadata.priority === "high" ? "ok" : "")) : ""}
          </div>
          <h3>${escapeHtml(scenario.title)}</h3>
          <p class="metric-detail">${escapeHtml(scenario.description)}</p>
          <div class="chip-row">${(scenario.tags || []).slice(0, 4).map((item) => chip(item)).join("")}</div>
          ${(scenario.metadata?.generation_reasons || []).length ? `<p class="metric-detail">${escapeHtml(scenario.metadata.generation_reasons[0])}</p>` : ""}
          <button type="button" class="ghost-button activate-scenario" data-scenario-name="${escapeHtml(scenario.name)}">Make active</button>
        </div>
      `
    )
    .join("");
  generatedPanel.querySelectorAll(".activate-scenario").forEach((node) => {
    node.addEventListener("click", () => {
      void activateScenario(node.dataset.scenarioName);
    });
  });

  const connectorCards = sourceRegistry.map(
    (item) => `
      <div class="stack-card">
        <h3>${escapeHtml(item.source_id)}</h3>
        <div class="chip-row">
          ${chip(item.connector)}
          ${chip(item.connector_mode || "live")}
          ${chip("connected", "ok")}
        </div>
        <div class="detail-grid">
          ${detailTile("Config", item.config_path || "n/a")}
          ${detailTile("Updated", item.updated_at || "n/a")}
        </div>
        <p class="metric-detail">Connector-backed source synced into the same import package pipeline as file inputs.</p>
      </div>
    `
  );
  const syncCards = sourceSyncs.slice(0, 3).map(
    (item) => `
      <div class="stack-card">
        <h3>${escapeHtml(item.source_id)}</h3>
        <div class="chip-row">
          ${chip(item.connector)}
          ${chip(item.status || "ok", statusClass(item.status))}
        </div>
        <div class="detail-grid">
          ${detailTile("Users", compactNumber(item.record_counts?.users || 0))}
          ${detailTile("Groups", compactNumber(item.record_counts?.groups || 0))}
          ${detailTile("Apps", compactNumber(item.record_counts?.applications || 0))}
          ${detailTile("Synced", item.synced_at || "n/a")}
        </div>
        <p class="metric-detail">${escapeHtml(item.package_path || "No package path recorded.")}</p>
      </div>
    `
  );
  const sourceCards = (review?.normalization_report?.source_summaries || []).map(
    (item) => `
      <div class="stack-card">
        <h3>${escapeHtml(item.source_id)}</h3>
        <div class="chip-row">
          ${chip(item.source_system)}
          ${chip(item.mapping_profile)}
          ${item.override_applied ? chip("override applied", "ok") : ""}
        </div>
        <div class="detail-grid">
          ${detailTile("Loaded", compactNumber(item.loaded_record_count || 0))}
          ${detailTile("Normalized", compactNumber(item.normalized_record_count || 0))}
          ${detailTile("Dropped", compactNumber(item.dropped_record_count || 0))}
          ${detailTile("Issues", compactNumber(item.issue_count || 0))}
        </div>
        ${item.unknown_fields?.length ? `<p class="metric-detail">Unknown fields: ${escapeHtml(item.unknown_fields.join(", "))}</p>` : `<p class="metric-detail">All observed fields mapped cleanly.</p>`}
      </div>
    `
  );
  const issueCards = (review?.normalization_report?.issues || []).slice(0, 6).map(
    (item) => `
      <div class="stack-card">
        <h3>${escapeHtml(item.code)}</h3>
        <p class="metric-detail">${escapeHtml(item.message)}</p>
        <div class="chip-row">
          ${chip(item.severity || "warning", statusClass(item.severity))}
          ${item.source_id ? chip(item.source_id) : ""}
          ${item.field ? chip(item.field) : ""}
        </div>
      </div>
    `
  );
  const reconciliationCards = (reconciliation?.links || []).slice(0, 6).map(
    (item) => `
      <div class="stack-card">
        <h3>${escapeHtml(item.principal_label)}</h3>
        <div class="chip-row">
          ${chip(item.principal_type)}
          ${chip(item.status || "resolved", statusClass(item.status === "resolved" ? "ok" : item.status === "external" ? "warning" : "error"))}
        </div>
        <p class="metric-detail">${escapeHtml(item.reason || "No reconciliation reason recorded.")}</p>
        <p class="metric-detail">${escapeHtml((item.matched_refs || item.candidate_refs || []).join(", ") || "No candidate refs")}</p>
      </div>
    `
  );
  const flowCards = identityFlow && identityFlow.active_scenario
    ? [
        `
          <div class="stack-card">
            <h3>Identity wedge</h3>
            <div class="chip-row">
              ${chip(identityFlow.active_scenario)}
              ${identityFlow.selected_candidate_family ? chip(identityFlow.selected_candidate_family) : ""}
              ${chip(`${identityFlow.generated_scenario_count || 0} scenarios`, "ok")}
            </div>
            <div class="detail-grid">
              ${detailTile("Package", identityFlow.package_name || "n/a")}
              ${detailTile("Runs", compactNumber((identityFlow.run_ids || []).length))}
              ${detailTile("Imported", compactNumber(identityFlow.origin_counts?.imported || 0))}
              ${detailTile("Derived", compactNumber(identityFlow.origin_counts?.derived || 0))}
            </div>
            <p class="metric-detail">${escapeHtml((identityFlow.recommended_next_steps || [])[0] || "Identity workspace ready for scenario preview and run launch.")}</p>
          </div>
        `,
      ]
    : [];
  reviewPanel.innerHTML = [
    ...flowCards,
    ...connectorCards,
    ...syncCards,
    ...sourceCards,
    ...reconciliationCards,
    ...issueCards,
  ].join("");

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
  const select = el("scenario-select");
  if (!select) return;
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
    if (panel) {
      panel.innerHTML = `<div class="metric-tile"><span class="metric-label">Scenario</span><span class="metric-value">Loading</span></div>`;
    }
    return;
  }

  const scenario = preview.scenario || {};
  const compiled = preview.compiled_blueprint || {};
  const metadata = scenario.metadata || {};
  const builderEnvironment = metadata.builder_environment || {};
  const whatIfBranches = Array.isArray(builderEnvironment.what_if_branches)
    ? builderEnvironment.what_if_branches
    : [];
  const scenarioVariants = Array.isArray(preview.available_scenario_variants)
    ? preview.available_scenario_variants
    : [];
  const contractVariants = Array.isArray(preview.available_contract_variants)
    ? preview.available_contract_variants
    : [];
  const activeScenarioVariant = scenarioVariants.find(
    (item) => item.name === preview.active_scenario_variant
  );
  const activeContractVariant = contractVariants.find(
    (item) => item.name === preview.active_contract_variant
  );
  const verticalName = builderEnvironment.vertical || metadata.vertical || "";
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
  if (!panel) {
    renderObjectiveBriefing(contractVariants, activeContractVariant, contract);
    return;
  }

  panel.innerHTML = `
    <div class="story-card story-span-2">
      <p class="eyebrow">What changed</p>
      <h3>${escapeHtml(activeScenarioVariant?.title || scenario.title || "Current situation")}</h3>
      <p class="metric-detail">${escapeHtml(activeScenarioVariant?.description || scenario.description || state.story?.situation_briefing || "")}</p>
      <div class="chip-row">
        ${chip(activeScenarioVariant?.name || preview.active_scenario_variant || "default")}
        ${chip(compiled.workflow_name || contract.workflow_name || "workflow")}
        ${chip(String((compiled.facades || []).length) + " surfaces")}
      </div>
    </div>
      <div class="story-card">
        <p class="eyebrow">Why it matters</p>
      <p class="metric-detail">${escapeHtml(inWorldCopy(
        activeScenarioVariant?.rationale,
        "This variation shifts the pressure without changing the underlying company."
      ))}</p>
      </div>
    <div class="story-card">
      <p class="eyebrow">From the base company</p>
      <p class="metric-detail">${escapeHtml((activeScenarioVariant?.change_summary || ["The base company stays fixed while the situation overlay changes deadlines, faults, or object state."]).join(" · "))}</p>
    </div>
    ${
      whatIfBranches.length
        ? `<div class="story-card">
            <p class="eyebrow">What-if paths</p>
            <div class="chip-row">${whatIfBranches.map((item) => chip(item)).join("")}</div>
            <p class="metric-detail">Each path begins from the same company and pressure, then diverges from there.</p>
          </div>`
        : ""
    }
    <div class="story-card">
      <p class="eyebrow">Surface coverage</p>
      <p class="metric-detail">${escapeHtml(facadeLabels || verticalName || "Core company surfaces are active for this situation.")}</p>
    </div>
  `;

  renderJson("scenario-panel", preview);
  renderObjectiveBriefing(contractVariants, activeContractVariant, contract);
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

function renderObjectiveBriefing(contractVariants = [], activeContractVariant = null, contract = null) {
  const targetContract = contract || state.scenarioContract;
  const panel = document.getElementById("objective-scorecard");
  if (!panel || !targetContract) {
    return;
  }
  const successCount = Array.isArray(targetContract.success_predicates)
    ? targetContract.success_predicates.length
    : Number(targetContract.success_predicate_count || 0);
  const forbiddenCount = Array.isArray(targetContract.forbidden_predicates)
    ? targetContract.forbidden_predicates.length
    : Number(targetContract.forbidden_predicate_count || 0);
  const invariants = Array.isArray(targetContract.policy_invariants)
    ? targetContract.policy_invariants.length
    : Number(targetContract.policy_invariant_count || 0);
  const ruleProvenance = Array.isArray(targetContract.metadata?.rule_provenance)
    ? targetContract.metadata.rule_provenance
    : [];
  const importedRuleCount = ruleProvenance.filter((item) => item.origin === "imported").length;
  const availableVariants = Array.isArray(contractVariants) && contractVariants.length
    ? contractVariants
    : Array.isArray(state.scenarioPreview?.available_contract_variants)
      ? state.scenarioPreview.available_contract_variants
      : [];
  const selectedVariant = activeContractVariant || availableVariants.find(
    (item) => item.name === state.scenarioPreview?.active_contract_variant
  );
  panel.innerHTML = `
    <div class="briefing-grid">
      <div class="story-card accent-card story-span-2">
        <p class="eyebrow">Success means</p>
        <h3>${escapeHtml(selectedVariant?.title || state.scenarioPreview?.active_contract_variant || "Default objective")}</h3>
        <p class="metric-detail">${escapeHtml(currentObjectiveSummary())}</p>
        <div class="chip-row">
          ${chip(`${successCount} success checks`)}
          ${chip(`${forbiddenCount} failure checks`)}
          ${chip(`${invariants} policy guardrails`)}
          ${chip(`${importedRuleCount} imported rules`)}
        </div>
      </div>
      <div class="story-card">
        <p class="eyebrow">Why this target</p>
        <p class="metric-detail">${escapeHtml(inWorldCopy(
          selectedVariant?.rationale,
          "This target says what good looks like while the company is under pressure."
        ))}</p>
      </div>
      <div class="story-card">
        <p class="eyebrow">Guardrails</p>
        <p class="metric-detail">${escapeHtml(state.story?.manifest?.objective_focus || "This target keeps business risk and policy drift visible while the run is in motion.")}</p>
      </div>
      ${
        availableVariants.length
          ? `<div class="story-card story-span-2">
              <p class="eyebrow">Other ways to judge this crisis</p>
              <div class="stack compact-stack">
                ${availableVariants
                  .map(
                    (item) => `
                      <div class="run-item">
                        <div class="chip-row">
                          ${chip(item.name)}
                          ${item.active ? chip("active", "ok") : ""}
                        </div>
                        <strong>${escapeHtml(item.title)}</strong>
                        <p class="metric-detail">${escapeHtml(item.objective_summary || item.description || "")}</p>
                        <button type="button" class="ghost-button activate-contract-variant" data-variant-name="${escapeHtml(item.name)}">Activate objective</button>
                      </div>
                    `
                  )
                  .join("")}
              </div>
            </div>`
          : ""
      }
    </div>
  `;
  document.querySelectorAll(".activate-contract-variant").forEach((node) => {
    node.addEventListener("click", () => {
      void activateContractVariant(node.dataset.variantName);
    });
  });
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
      ${scorePill("Contract", run.contract?.ok === null ? "pending" : run.contract?.ok ? "pass" : "fail", run.contract?.contract_name || "workspace contract")}
      ${scorePill("Assertions", `${successPassed}/${successTotal}`)}
      ${scorePill("Issues", String(issueCount))}
      ${scorePill("Policy failures", String(policyFails))}
      ${scorePill("Run events", compactNumber(state.timeline.length))}
    </div>
    <div class="briefing-grid">
      <div class="story-card accent-card story-span-2">
        <p class="eyebrow">Did this help?</p>
        <h3>${escapeHtml(outcomeTitle)}</h3>
        <p class="metric-detail">${escapeHtml(outcomeBody)}</p>
        <div class="detail-grid">
          ${detailTile("Graph actions", compactNumber(graphEvents.length))}
          ${detailTile("Snapshots", compactNumber(state.snapshots.length))}
          ${detailTile("Domains", compactNumber(graphDomains.length))}
          ${detailTile("Virtual time", formatMs(run.metrics?.time_ms || 0))}
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
        <div class="chip-row">${resolvedTools.slice(0, 5).map((item) => chip(item)).join("")}</div>
      </div>
      ${
        whatIfBranches.length
          ? `<div class="story-card">
              <p class="eyebrow">What-if paths</p>
              <h3>Branch labels</h3>
              <div class="chip-row">${whatIfBranches.map((item) => chip(item)).join("")}</div>
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
              <p class="eyebrow">${escapeHtml(item.title || item.name)}</p>
              <h3>${escapeHtml(item.name)}</h3>
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
      `<p class="metric-detail">Forking is only available on the current playable mission run.</p>`
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
  const [workspace, storyArtifacts, exerciseArtifacts, playableArtifacts, scenarios, importSummary, identityFlow, importSources, importNormalization, importReview, generatedImportScenarios, provenanceIndex, mirrorStatus] = await Promise.all([
    getJson("/api/workspace"),
    fetchStoryArtifacts(),
    fetchExerciseArtifacts(),
    fetchPlayableArtifacts(),
    getJson("/api/scenarios"),
    getJson("/api/imports/summary").catch(() => ({})),
    getJson("/api/identity/flow").catch(() => ({})),
    getJson("/api/imports/sources").catch(() => ({ sources: [], syncs: [] })),
    getJson("/api/imports/normalization").catch(() => ({})),
    getJson("/api/imports/review").catch(() => ({})),
    getJson("/api/imports/scenarios").catch(() => []),
    getJson("/api/imports/provenance").catch(() => []),
    getJson("/api/workspace/mirror").catch(() => ({})),
  ]);
  state.workspace = workspace;
  state.mirrorStatus = mirrorStatus;
  state.story = nonEmptyPayload(storyArtifacts.story);
  state.exercise = nonEmptyPayload(exerciseArtifacts.exercise);
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
  renderWorkspaceHero();
  renderImportSummary();
  renderExportsPanel();
  renderScenarioSelector();
  renderMissionSelector();
  renderMissionSummary();
  renderMissionPlay();
  renderFidelityPanel();
  if (scenarios.length > 0) {
    const activeName = workspace?.manifest?.active_scenario || scenarios[0].name;
    document.getElementById("scenario-select").value = activeName;
    await loadScenario(activeName);
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
  status.textContent = `Activating mission ${name}...`;
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
    status.textContent = `Mission ${name} is ready.`;
    setStudioView("crisis");
  } catch (error) {
    status.textContent = `Mission activation failed: ${error}`;
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
      await getJson("/api/exercise/activate", {
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
      status.textContent = `Crisis update failed: ${error}`;
    }
    return;
  }
  status.textContent = "Entering world\u2026";
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
    status.textContent = `Mission ${payload.mission?.title || payload.run_id} is live.`;
    setStudioView("company");
  } catch (error) {
    status.textContent = `Mission start failed: ${error}`;
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
      ? "Mission completed. Inspect the results or branch the run."
      : impact.summary
        ? `${moveTitle} \u2014 ${impact.summary}`
        : `${moveTitle} \u2014 ${impactPanels.length} system${impactPanels.length === 1 ? "" : "s"} hit.`;
  } catch (error) {
    status.textContent = `Move failed: ${error}`;
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
    status.textContent = `Branch failed: ${error}`;
  }
}

async function finishMission() {
  if (!state.missionState?.run_id) {
    return;
  }
  const status = document.getElementById("mission-form-status");
  status.textContent = "Finishing mission...";
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
      ? "Mission finished cleanly."
      : "Mission finished with remaining risk.";
    setStudioView("outcome");
  } catch (error) {
    status.textContent = `Finish failed: ${error}`;
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
  const [run, timeline, orientation, graphs, snapshots, contract, surfaces, mirrorStatus] = await Promise.all([
    getJson(`/api/runs/${runId}`),
    getJson(`/api/runs/${runId}/timeline`),
    getJson(`/api/runs/${runId}/orientation`),
    getJson(`/api/runs/${runId}/graphs`),
    getJson(`/api/runs/${runId}/snapshots`),
    getJson(`/api/runs/${runId}/contract`),
    getJson(`/api/runs/${runId}/surfaces`).catch(() => null),
    getJson("/api/workspace/mirror").catch(() => null),
  ]);

  if (generation !== state.refreshGeneration) return;

  state.activeRun = run;
  state.timeline = timeline;
  state.orientation = orientation;
  state.graphs = graphs;
  state.surfaceState = surfaces;
  setSurfaceHighlights(diffSurfaceState(previousSurfaceState, surfaces), {
    preserveExisting: preserveSurfaceHighlights,
  });
  state.snapshots = snapshots;
  state.activeRunContract = contract;
  state.mirrorStatus = nonEmptyPayload(mirrorStatus);
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
    if (state.sseDebounceTimer) clearTimeout(state.sseDebounceTimer);
    state.sseDebounceTimer = setTimeout(() => {
      state.sseDebounceTimer = null;
      void refreshActiveRun(runId, { preserveSurfaceHighlights: true });
    }, 150);
  };
  state.eventSource.onerror = () => {
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
  };
}

async function selectRun(runId, options = {}) {
  if (state.activeRunId !== runId) {
    state.snapshotForkError = null;
  }
  state.activeRunId = runId;
  renderRuns();
  await refreshActiveRun(runId, { connectStream: true, ...options });
}

async function startRun(event) {
  event.preventDefault();
  const runner = document.getElementById("runner-select").value;
  const payload = {
    scenario_name: document.getElementById("scenario-select").value,
    runner,
    provider: document.getElementById("provider-input").value || null,
    model: document.getElementById("model-input").value || null,
    bc_model: document.getElementById("bc-model-input").value || null,
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

async function activateScenario(name) {
  const status = document.getElementById("run-form-status");
  status.textContent = `Activating scenario ${name}...`;
  try {
    await getJson("/api/scenarios/activate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scenario_name: name, bootstrap_contract: true }),
    });
    await loadWorkspace();
    status.textContent = `Scenario ${name} is now active.`;
  } catch (error) {
    status.textContent = `Scenario activation failed: ${error}`;
  }
}

// ---------------------------------------------------------------------------
// Connect panel
// ---------------------------------------------------------------------------

const PROVIDER_ICONS = {
  slack: "\u{1F4AC}",
  google: "\u{1F4E7}",
  jira: "\u{1F4CB}",
  okta: "\u{1F512}",
  gmail: "\u2709\uFE0F",
  teams: "\u{1F465}",
};

const PROVIDER_LABELS = {
  slack: "Slack",
  google: "Google Workspace",
  jira: "Jira",
  okta: "Okta",
  gmail: "Gmail",
  teams: "Microsoft Teams",
};

function toggleConnectPanel() {
  closeToolbarMenus();
  const panel = document.getElementById("connect-panel");
  const isVisible = panel.style.display !== "none";
  panel.style.display = isVisible ? "none" : "block";
  if (!isVisible) loadConnectStatus();
}

async function loadConnectStatus() {
  const container = document.getElementById("connect-providers");
  const statusBar = document.getElementById("connect-status-bar");
  try {
    const res = await fetch("/api/context/status");
    const data = await res.json();
    const providers = data.providers || [];
    container.innerHTML = providers.map((p) => {
      const icon = PROVIDER_ICONS[p.provider] || "\u26A1";
      const label = PROVIDER_LABELS[p.provider] || p.provider;
      const statusCls = p.configured ? "connect-configured" : "connect-missing";
      const statusText = p.configured ? "Connected" : "Not configured";
      const envHint = p.configured ? "" : `<span class="connect-env-hint">Set ${escapeHtml(p.env_var)}</span>`;
      const captureBtn = p.configured
        ? `<button type="button" class="ghost-button connect-capture-btn" data-provider="${escapeHtml(p.provider)}">Capture Now</button>`
        : "";
      return `
        <div class="connect-provider-row ${statusCls}">
          <span class="connect-icon">${icon}</span>
          <div class="connect-info">
            <span class="connect-label">${escapeHtml(label)}</span>
            <span class="connect-status-text">${statusText}</span>
            ${envHint}
          </div>
          ${captureBtn}
        </div>
      `;
    }).join("");

    container.querySelectorAll(".connect-capture-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        void captureProvider(btn.dataset.provider);
      });
    });

    const configured = providers.filter((p) => p.configured).length;
    statusBar.textContent = `${configured} of ${providers.length} sources configured`;
  } catch (err) {
    container.innerHTML = `<p class="connect-error">Failed to load provider status</p>`;
    statusBar.textContent = "";
  }
}

async function captureProvider(providerName) {
  const statusBar = document.getElementById("connect-status-bar");
  statusBar.textContent = `Capturing from ${PROVIDER_LABELS[providerName] || providerName}...`;
  try {
    const res = await fetch("/api/context/capture", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ providers: [providerName] }),
    });
    const data = await res.json();
    if (!res.ok) {
      statusBar.textContent = `Error: ${data.detail || "capture failed"}`;
      return;
    }
    const sources = data.sources || [];
    const summary = sources.map((s) =>
      `${s.provider}: ${Object.entries(s.record_counts || {}).map(([k,v]) => `${v} ${k}`).join(", ")}`
    ).join("; ");
    statusBar.textContent = `Captured: ${summary}`;
    await loadConnectStatus();
  } catch (err) {
    statusBar.textContent = `Capture failed: ${err.message || err}`;
  }
}

function bindControls() {
  document.getElementById("run-form").addEventListener("submit", startRun);
  document.getElementById("start-mission-button").addEventListener("click", () => {
    void startMission();
  });
  document.getElementById("branch-mission-button").addEventListener("click", () => {
    void branchMission();
  });
  document.getElementById("finish-mission-button").addEventListener("click", () => {
    void finishMission();
  });
  document.getElementById("mission-select").addEventListener("change", (event) => {
    const objective = document.getElementById("objective-select").value || null;
    void activateMission(event.target.value, objective);
  });
  document.getElementById("objective-select").addEventListener("change", async (event) => {
    const selected = event.target.value;
    if (selected) {
      await activateContractVariant(selected);
    }
  });
  document.querySelectorAll(".studio-nav-button").forEach((node) => {
    node.addEventListener("click", () => {
      setStudioView(node.dataset.studioView || "company");
    });
  });
  document.getElementById("developer-toggle").addEventListener("click", toggleDeveloperMode);
  document.getElementById("cinema-toggle").addEventListener("click", toggleCinemaMode);
  document.getElementById("timeline-toggle").addEventListener("click", toggleTimelineMode);
  document.getElementById("compare-toggle").addEventListener("click", toggleCompareMode);
  document.getElementById("connect-toggle").addEventListener("click", toggleConnectPanel);
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
