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
  snapshots: [],
  selectedEventIndex: 0,
  selectedSnapshotFrom: null,
  selectedSnapshotTo: null,
  studioView: "company",
  developerMode: false,
  cinemaMode: false,
  boardMode: false,
  boardEventLog: [],
  compareMode: false,
  compareRunA: null,
  compareRunB: null,
  compareTimelineA: [],
  compareTimelineB: [],
  cinemaAutoAdvance: false,
  cinemaAutoTimer: null,
  playbackTimer: null,
  eventSource: null,
  cascadeActive: false,
  cascadeAbort: null,
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

async function fetchPlayableArtifacts() {
  const [playableBundle, missions, missionState, fidelityReport] = await Promise.all([
    getJson("/api/playable").catch(() => null),
    getJson("/api/missions").catch(() => []),
    getJson("/api/missions/state").catch(() => null),
    getJson("/api/fidelity").catch(() => null),
  ]);
  return { playableBundle, missions, missionState, fidelityReport };
}

function renderJson(id, payload) {
  document.getElementById(id).textContent = JSON.stringify(payload, null, 2);
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
  document.getElementById("developer-toggle").textContent = state.developerMode
    ? "Hide Engine"
    : "Show Engine";
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
  const panel = document.getElementById("kernel-thesis-panel");
  const story = state.story || {};
  const presentation = state.presentation || story.presentation || {};
  const selectedWorld = story.manifest?.company_name || state.workspace?.manifest?.title || "Workspace";
  const companyBriefing =
    story.company_briefing ||
    state.workspace?.manifest?.description ||
    "A stable company world with live tools, shared business state, and pressure building across the work.";
  panel.innerHTML = `
    <div class="story-card accent-card">
      <p class="eyebrow">What this is</p>
      <h3>A full enterprise, running as software.</h3>
      <p class="metric-detail">Every tool, every person, every process \u2014 simulated end\u2011to\u2011end. Make one move and watch the ripple hit Slack, email, tickets, docs, and the business core at the same time.</p>
    </div>
    <div class="story-card">
      <p class="eyebrow">Current Company</p>
      <h3>${escapeHtml(selectedWorld)}</h3>
      <p class="metric-detail">${escapeHtml(companyBriefing)}</p>
    </div>
    <div class="story-card">
      <p class="eyebrow">How it works</p>
      <p class="metric-detail">${escapeHtml(presentation.demo_goal || "Pick a crisis. Define what success looks like. Then play moves and watch the company change \u2014 or fork reality and compare two futures side by side.")}</p>
    </div>
  `;
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
  panel.innerHTML = [
    metricTile("Workspace", manifest.title || manifest.name || "Workspace", manifest.source_kind || "template"),
    metricTile("Situations", String((manifest.scenarios || []).length), `active: ${manifest.active_scenario || "default"}`),
    metricTile("Paths", String(workspace.run_count || 0), latestRun ? `latest: ${latestRun.run_id}` : "none yet"),
    metricTile("Objectives", String((workspace.compiled_scenarios || []).length), "compiled"),
  ].join("");
}

function renderWorkspaceHero() {
  const workspace = state.workspace;
  if (!workspace) {
    return;
  }
  const manifest = workspace.manifest || {};
  const story = state.story || {};
  const subtitle = document.getElementById("workspace-subtitle");
  subtitle.classList.remove("loading-pulse");
  const companyName = story.manifest?.company_name || manifest.title || "Workspace";
  const crisis = story.manifest?.scenario_name?.replace(/_/g, " ") || "";
  const pathCount = workspace.run_count || 0;
  subtitle.textContent = pathCount
    ? `${companyName}${crisis ? ` \u2014 ${crisis}` : ""}. ${pathCount} recorded path${pathCount === 1 ? "" : "s"}.`
    : `${companyName}${crisis ? ` \u2014 ${crisis}` : ""}. Enter the world to begin.`;
  renderWorkspaceMetrics();
  renderStudioShell();
  renderWorldsPanel();
  renderJson("workspace-panel", workspace);
}

function renderWorldsPanel() {
  const panel = document.getElementById("worlds-panel");
  const story = state.story;
  const workspace = state.workspace;
  if (!panel || !workspace) {
    return;
  }
  const manifest = workspace.manifest || {};
  const availableWorlds = Array.isArray(story?.available_worlds) ? story.available_worlds : [];
  const currentWorldName = story?.manifest?.name || manifest.source_ref || "";
  const keySurfaces = Array.isArray(story?.manifest?.key_surfaces)
    ? story.manifest.key_surfaces
    : [];
  panel.innerHTML = `
    <div class="story-card accent-card story-span-2">
      <p class="eyebrow">Company</p>
      <h3>${escapeHtml(story?.manifest?.company_name || manifest.title || manifest.name || "Workspace")}</h3>
      <p class="metric-detail">${escapeHtml(story?.company_briefing || manifest.description || "This workspace is one stable company environment with shared tools and business state.")}</p>
      <div class="chip-row">${keySurfaces.map((item) => chip(formatDomainTitle(item))).join("")}</div>
    </div>
    <div class="story-card">
      <p class="eyebrow">Why failure matters</p>
      <p class="metric-detail">${escapeHtml(story?.failure_impact || "This scenario matters because operational drift has business consequences, not just engine consequences.")}</p>
    </div>
    <div class="story-card">
      <p class="eyebrow">Objective focus</p>
      <p class="metric-detail">${escapeHtml(story?.manifest?.objective_focus || story?.objective_briefing || "The current objective tells VEI what good looks like in this world.")}</p>
    </div>
    ${availableWorlds
      .map(
        (item) => `
          <div class="story-card ${item.name === currentWorldName ? "story-current" : ""}">
            <p class="eyebrow">${item.title}</p>
            <h3>${escapeHtml(item.company_name)}</h3>
            <p class="metric-detail">${escapeHtml(item.company_briefing || item.description || "")}</p>
            <div class="chip-row">
              ${item.name === currentWorldName ? chip("current company", "ok") : chip("another company")}
              ${(item.key_surfaces || []).slice(0, 3).map((surface) => chip(formatDomainTitle(surface))).join("")}
            </div>
          </div>
        `
      )
      .join("")}
  `;
}

function renderMissionSelector() {
  const missionSelect = document.getElementById("mission-select");
  const objectiveSelect = document.getElementById("objective-select");
  const summary = document.getElementById("mission-launch-summary");
  if (!missionSelect || !objectiveSelect || !summary) {
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
  summary.innerHTML = currentMission
    ? `
      <div class="detail-grid">
        ${detailTile("World", state.workspace?.manifest?.title || state.playableBundle?.world_name || "workspace")}
        ${detailTile("Mission", currentMission.title || currentMission.mission_name)}
        ${detailTile("Objective", selectedObjective || "default")}
        ${detailTile("State", state.missionState?.status || "ready")}
      </div>
    `
    : `<p class="metric-detail">No company world is loaded yet.</p>`;
}

function renderMissionSummary() {
  const briefing = document.getElementById("mission-briefing");
  const catalog = document.getElementById("mission-catalog");
  if (!briefing || !catalog) {
    return;
  }
  const missionState = state.missionState;
  const currentMission =
    missionState?.mission ||
    state.playableBundle?.mission ||
    state.missions[0] ||
    null;
  if (!currentMission) {
    briefing.innerHTML = `
      <div class="story-card story-span-2">
        <p class="eyebrow">Mission</p>
        <p class="metric-detail">Prepare the company world to explore a situation inside it.</p>
      </div>
    `;
    catalog.innerHTML = "";
    return;
  }
  briefing.innerHTML = `
    <div class="story-card accent-card story-span-2">
      <p class="eyebrow">Current mission</p>
      <h3>${escapeHtml(currentMission.title)}</h3>
      <p class="metric-detail">${escapeHtml(currentMission.briefing || "")}</p>
      <div class="chip-row">
        ${chip(currentMission.hero ? "primary company" : "included company", currentMission.hero ? "ok" : "")}
        ${chip(currentMission.primary_domain || "world")}
        ${(currentMission.branch_labels || []).map((item) => chip(item)).join("")}
      </div>
    </div>
    <div class="story-card">
      <p class="eyebrow">Why this matters</p>
      <p class="metric-detail">${escapeHtml(currentMission.why_it_matters || "")}</p>
    </div>
    <div class="story-card">
      <p class="eyebrow">Failure impact</p>
      <p class="metric-detail">${escapeHtml(currentMission.failure_impact || "")}</p>
    </div>
  `;
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
  state.cinemaMode = !state.cinemaMode;
  if (state.cinemaMode && state.boardMode) {
    state.boardMode = false;
    document.body.classList.remove("board-mode");
    const bBtn = document.getElementById("board-toggle");
    if (bBtn) bBtn.textContent = "Board";
    const section = document.getElementById("board-game-section");
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
  if (state.boardMode) {
    renderBoardStatusBar();
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
// Board game view
// ---------------------------------------------------------------------------
const HEX_ICONS = {
  slack: "\u{1F4AC}",
  mail: "\u{1F4E7}",
  tickets: "\u{1F3AB}",
  docs: "\u{1F4C4}",
  approvals: "\u2705",
  vertical_heartbeat: "\u{1F4C8}",
};

const HEX_ACCENT = {
  slack: "#36c5f0",
  mail: "#ffb454",
  tickets: "#ff6d5e",
  docs: "#1aa88d",
  approvals: "#9b7bff",
  vertical_heartbeat: "#1e6cf2",
};

function toggleBoardMode() {
  state.boardMode = !state.boardMode;
  if (state.boardMode && state.cinemaMode) {
    state.cinemaMode = false;
    document.body.classList.remove("cinema-mode");
    const cinBtn = document.getElementById("cinema-toggle");
    if (cinBtn) cinBtn.textContent = "Present";
    stopCinemaAutoAdvance();
  }
  document.body.classList.toggle("board-mode", state.boardMode);
  const btn = document.getElementById("board-toggle");
  if (btn) btn.textContent = state.boardMode ? "Exit Board" : "Board";
  const section = document.getElementById("board-game-section");
  if (section) section.style.display = state.boardMode ? "" : "none";
  if (state.boardMode) {
    setStudioView("company");
    clearFlowLines();
    renderBoardGame();
    const hasMoves = (state.missionState?.available_moves || []).some(
      (m) => !m.executed && m.availability !== "blocked"
    );
    if (hasMoves) {
      state.cinemaAutoAdvance = true;
      window.setTimeout(cinemaAutoStep, 1400);
    }
  } else {
    stopCinemaAutoAdvance();
    dismissBoardOverlay();
  }
}

function renderBoardGame() {
  renderBoardStatusBar();
  renderHexGrid();
  renderBoardActionCards();
  renderBoardEventTicker();
  if (state.missionState?.status === "completed") renderBoardVictory();
}

function renderBoardStatusBar() {
  const bar = document.getElementById("board-status-bar");
  if (!bar) return;
  const ms = state.missionState;
  const mission = ms?.mission || state.playableBundle?.mission || null;
  const score = ms?.scorecard || {};
  const moveCount = (ms?.executed_moves || []).length;
  const budget = score.action_budget_remaining ?? "?";
  const totalBudget = moveCount + (typeof budget === "number" ? budget : 0);
  const company = state.surfaceState?.company_name
    || state.workspace?.manifest?.title || "Company";
  const scorePct = Math.min(100, Math.max(0, score.overall_score || 0));
  const lastMove = moveCount ? ms.executed_moves[moveCount - 1] : null;

  let narrative = "";
  if (ms?.status === "completed") {
    narrative = score.mission_success ? "Mission complete \u2014 all systems green." : "Mission closed with exposure.";
  } else if (lastMove) {
    narrative = `Turn ${moveCount}: ${lastMove.title}`;
  } else if (mission) {
    narrative = mission.briefing || mission.description || "";
  }

  bar.innerHTML = `
    <div class="board-bar-left">
      <span class="board-company">${escapeHtml(company)}</span>
      <span class="board-narrative">${escapeHtml(narrative)}</span>
    </div>
    <div class="board-bar-right">
      <div class="board-stat">
        <span class="board-stat-label">Turn</span>
        <span class="board-stat-value">${moveCount} / ${totalBudget || "?"}</span>
      </div>
      <div class="board-stat">
        <span class="board-stat-label">Score</span>
        <div class="board-score-gauge">
          <div class="board-score-fill" style="width:${scorePct}%"></div>
          <span class="board-score-num">${scorePct}</span>
        </div>
      </div>
      <div class="board-stat">
        <span class="board-stat-label">Risk</span>
        <span class="board-stat-value board-risk-${score.business_risk || "unknown"}">${score.business_risk || "\u2014"}</span>
      </div>
      <button type="button" id="board-auto-toggle" class="ghost-button board-auto-btn">
        ${state.cinemaAutoAdvance ? "\u23F8 Pause" : "\u25B6 Auto"}
      </button>
    </div>
  `;
  const autoBtn = document.getElementById("board-auto-toggle");
  if (autoBtn) autoBtn.addEventListener("click", toggleCinemaAutoAdvance);
}

function renderHexGrid() {
  const grid = document.getElementById("board-hex-grid");
  if (!grid) return;
  const panels = state.surfaceState?.panels || [];
  const changedSet = new Set(state.surfaceHighlights?.panels || []);

  if (!panels.length) {
    grid.innerHTML = `<div class="board-hex-empty">Enter a world to see the board</div>`;
    return;
  }

  grid.innerHTML = panels.map((p, i) => {
    const icon = HEX_ICONS[p.surface] || "\u{1F3E2}";
    const title = formatSurfaceTitle(p.surface);
    const count = (p.items || []).length;
    const stat = p.headline || `${count} item${count === 1 ? "" : "s"}`;
    const changed = changedSet.has(p.surface);
    const statusCls = `hex-status-${p.status || "ok"}`;
    const pulseCls = changed ? "hex-pulse" : "";
    return `
      <div class="board-hex ${statusCls} ${pulseCls}" data-surface="${escapeHtml(p.surface)}" style="--hex-i:${i}" role="button" tabindex="0">
        <div class="hex-shape">
          <div class="hex-content">
            <span class="hex-icon">${icon}</span>
            <span class="hex-title">${escapeHtml(title)}</span>
            <span class="hex-stat">${escapeHtml(stat)}</span>
            ${changed ? '<span class="hex-changed-badge">UPDATED</span>' : ""}
          </div>
        </div>
      </div>
    `;
  }).join("");

  grid.querySelectorAll(".board-hex").forEach((hex) => {
    hex.addEventListener("click", () => {
      toggleHexDetail(hex.dataset.surface);
    });
  });
}

function toggleHexDetail(surface) {
  const existing = document.getElementById("hex-detail-panel");
  if (existing) {
    if (existing.dataset.surface === surface) { existing.remove(); return; }
    existing.remove();
  }
  const panel = state.surfaceState?.panels?.find((p) => p.surface === surface);
  if (!panel) return;
  const accent = HEX_ACCENT[surface] || "#7b5bff";
  const items = (panel.items || []).slice(0, 6);
  const el = document.createElement("div");
  el.id = "hex-detail-panel";
  el.className = "hex-detail-panel";
  el.dataset.surface = surface;
  el.style.setProperty("--hex-accent", accent);
  el.innerHTML = `
    <div class="hex-detail-header">
      <span class="hex-detail-icon">${HEX_ICONS[surface] || "\u{1F3E2}"}</span>
      <div>
        <strong>${escapeHtml(formatSurfaceTitle(surface))}</strong>
        <span class="hex-detail-status">${escapeHtml(panel.status || "ok")}</span>
      </div>
      <button type="button" class="hex-detail-close">\u2715</button>
    </div>
    <p class="hex-detail-headline">${escapeHtml(panel.headline || "")}</p>
    <div class="hex-detail-items">
      ${items.map((item) => `
        <div class="hex-detail-item">
          <strong>${escapeHtml(item.title)}</strong>
          ${item.subtitle ? `<span class="hex-detail-sub">${escapeHtml(item.subtitle)}</span>` : ""}
          ${item.body ? `<p>${escapeHtml(item.body).slice(0, 120)}</p>` : ""}
        </div>
      `).join("")}
      ${(panel.items || []).length > 6 ? `<p class="hex-detail-more">+${(panel.items || []).length - 6} more</p>` : ""}
    </div>
  `;
  el.querySelector(".hex-detail-close").addEventListener("click", () => el.remove());
  const area = document.querySelector(".board-hex-area");
  if (area) area.appendChild(el);
}

function renderBoardActionCards() {
  const panel = document.getElementById("board-action-cards");
  if (!panel) return;
  const ms = state.missionState;
  if (!ms || !Array.isArray(ms.available_moves) || !ms.available_moves.length) {
    panel.innerHTML = `<div class="board-cards-empty">
      <p class="eyebrow">Actions</p>
      <p>Enter a world to get your hand of cards.</p>
    </div>`;
    return;
  }

  panel.innerHTML = `
    <p class="eyebrow board-cards-title">Action Cards</p>
    ${ms.available_moves.map((move) => {
      const used = move.executed;
      const blocked = move.availability === "blocked";
      const risky = move.availability === "risky";
      const recommended = move.availability === "recommended";
      const availClass = risky ? "card-risky" : recommended ? "card-recommended" : "card-available";
      const stateClass = used ? "card-used" : blocked ? "card-blocked" : "";
      const targets = guessMoveTargets(move);
      const targetDots = targets.map((s) =>
        `<span class="card-target-dot" title="${escapeHtml(formatSurfaceTitle(s))}" style="background:${HEX_ACCENT[s] || "#7b5bff"}">${HEX_ICONS[s] || "\u25CF"}</span>`
      ).join("");
      return `
        <div class="board-card ${availClass} ${stateClass}" data-move-id="${escapeHtml(move.move_id)}">
          <div class="board-card-top">
            <div class="board-card-badge">${used ? "played" : escapeHtml(move.availability)}</div>
            ${targetDots ? `<div class="card-target-row">${targetDots}</div>` : ""}
          </div>
          <h4 class="board-card-title">${escapeHtml(move.title)}</h4>
          ${!used && !blocked
            ? `<button type="button" class="board-card-play" data-move-id="${escapeHtml(move.move_id)}">
                ${risky ? "Risk it" : "Play"}
              </button>`
            : ""}
        </div>
      `;
    }).join("")}
  `;

  panel.querySelectorAll(".board-card-play").forEach((btn) => {
    btn.addEventListener("click", () => {
      void applyMissionMove(btn.dataset.moveId);
    });
  });
}

function renderBoardEventTicker() {
  const ticker = document.getElementById("board-event-ticker");
  if (!ticker) return;
  const log = state.boardEventLog;
  if (!log.length) {
    ticker.innerHTML = `<span class="ticker-empty">Waiting for first move\u2026</span>`;
    return;
  }
  ticker.innerHTML = log.map((entry) =>
    `<span class="ticker-entry"><strong>Turn ${entry.turn}:</strong> ${escapeHtml(entry.title)} \u2192 ${entry.systems.map((s) => escapeHtml(formatSurfaceTitle(s))).join(", ")}</span>`
  ).join('<span class="ticker-sep">\u00b7</span>');
  ticker.scrollLeft = ticker.scrollWidth;
}

function recordBoardEvent(moveTitle, changedPanels) {
  const turn = (state.missionState?.executed_moves || []).length;
  state.boardEventLog.push({ turn, title: moveTitle, systems: changedPanels });
}

function animateHexRipple(changedPanels) {
  if (!state.boardMode || !changedPanels.length) return;
  changedPanels.forEach((surface) => {
    const hex = document.querySelector(`.board-hex[data-surface="${surface}"]`);
    if (!hex) return;
    hex.classList.remove("hex-ripple");
    void hex.offsetWidth;
    hex.classList.add("hex-ripple");
    window.setTimeout(() => hex.classList.remove("hex-ripple"), 1200);
  });
}

function fireParticleTrail(cardEl, changedPanels) {
  if (!state.boardMode || !cardEl || !changedPanels.length) return;
  const container = document.getElementById("board-particles");
  if (!container) return;
  const boardRect = container.getBoundingClientRect();
  const cardRect = cardEl.getBoundingClientRect();
  const startX = cardRect.left - boardRect.left;
  const startY = cardRect.top + cardRect.height / 2 - boardRect.top;

  changedPanels.forEach((surface, idx) => {
    const hex = document.querySelector(`.board-hex[data-surface="${surface}"]`);
    if (!hex) return;
    const hexRect = hex.getBoundingClientRect();
    const endX = hexRect.left + hexRect.width / 2 - boardRect.left;
    const endY = hexRect.top + hexRect.height / 2 - boardRect.top;
    const dot = document.createElement("div");
    dot.className = "board-particle";
    dot.style.setProperty("--px-start", `${startX}px`);
    dot.style.setProperty("--py-start", `${startY}px`);
    dot.style.setProperty("--px-end", `${endX}px`);
    dot.style.setProperty("--py-end", `${endY}px`);
    dot.style.setProperty("--p-color", HEX_ACCENT[surface] || "#7b5bff");
    dot.style.animationDelay = `${idx * 120}ms`;
    container.appendChild(dot);
    window.setTimeout(() => dot.remove(), 1000);
  });
}

function drawFlowLines(changedPanels) {
  const svg = document.getElementById("board-flow-svg");
  if (!svg || !changedPanels.length) return;
  const area = svg.parentElement;
  if (!area) return;
  const areaRect = area.getBoundingClientRect();

  function hexCenter(surface) {
    const hex = document.querySelector(`.board-hex[data-surface="${surface}"]`);
    if (!hex) return null;
    const r = hex.getBoundingClientRect();
    return { x: r.left + r.width / 2 - areaRect.left, y: r.top + r.height / 2 - areaRect.top };
  }

  const pairs = [];
  for (let i = 0; i < changedPanels.length; i++) {
    for (let j = i + 1; j < changedPanels.length; j++) {
      pairs.push([changedPanels[i], changedPanels[j]]);
    }
  }

  pairs.forEach(([a, b]) => {
    const pa = hexCenter(a);
    const pb = hexCenter(b);
    if (!pa || !pb) return;
    const existingKey = `flow-${[a, b].sort().join("-")}`;
    if (svg.querySelector(`[data-flow-key="${existingKey}"]`)) {
      const el = svg.querySelector(`[data-flow-key="${existingKey}"]`);
      el.classList.add("flow-line-active");
      window.setTimeout(() => el.classList.remove("flow-line-active"), 2000);
      return;
    }
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", pa.x);
    line.setAttribute("y1", pa.y);
    line.setAttribute("x2", pb.x);
    line.setAttribute("y2", pb.y);
    line.setAttribute("stroke", HEX_ACCENT[a] || "#7b5bff");
    line.setAttribute("stroke-dasharray", "6 4");
    line.setAttribute("data-flow-key", existingKey);
    line.classList.add("flow-line", "flow-line-active");
    svg.appendChild(line);
    window.setTimeout(() => line.classList.remove("flow-line-active"), 2000);
  });
}

function clearFlowLines() {
  const svg = document.getElementById("board-flow-svg");
  if (svg) svg.innerHTML = "";
}

function renderBoardVictory() {
  if (document.getElementById("board-victory-overlay")) return;
  const ms = state.missionState;
  const score = ms?.scorecard || {};
  const mission = ms?.mission || {};
  const success = score.mission_success;
  const overlay = document.createElement("div");
  overlay.id = "board-victory-overlay";
  overlay.className = `board-victory-overlay ${success ? "victory-success" : "victory-exposure"}`;
  overlay.innerHTML = `
    <div class="victory-card">
      <div class="victory-icon">${success ? "\u{1F3C6}" : "\u26A0\uFE0F"}</div>
      <h2 class="victory-title">${success ? "Mission Complete" : "Mission Closed"}</h2>
      <p class="victory-subtitle">${success ? "All objectives met. Every system is green." : "Exposure remains. Some objectives were not met."}</p>
      <div class="victory-stats">
        <div class="victory-stat">
          <span class="victory-stat-num">${score.overall_score || 0}</span>
          <span class="victory-stat-label">Score</span>
        </div>
        <div class="victory-stat">
          <span class="victory-stat-num">${score.move_count || 0}</span>
          <span class="victory-stat-label">Moves</span>
        </div>
        <div class="victory-stat">
          <span class="victory-stat-num">${score.success_assertions_passed || 0}/${score.success_assertions_total || 0}</span>
          <span class="victory-stat-label">Assertions</span>
        </div>
        <div class="victory-stat">
          <span class="victory-stat-num">${score.business_risk || "\u2014"}</span>
          <span class="victory-stat-label">Final Risk</span>
        </div>
      </div>
      <button type="button" class="victory-dismiss">Continue</button>
    </div>
  `;
  overlay.querySelector(".victory-dismiss").addEventListener("click", dismissBoardOverlay);
  overlay.addEventListener("click", (e) => { if (e.target === overlay) dismissBoardOverlay(); });
  document.querySelector(".board-game-section")?.appendChild(overlay);
}

function dismissBoardOverlay() {
  document.getElementById("board-victory-overlay")?.remove();
}

// ---------------------------------------------------------------------------
// Compare mode — overlay two run timelines on the board
// ---------------------------------------------------------------------------
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

async function toggleCompareMode() {
  state.compareMode = !state.compareMode;
  const bar = document.getElementById("board-compare-bar");
  const btn = document.getElementById("compare-toggle");
  if (!state.compareMode) {
    if (bar) bar.style.display = "none";
    if (btn) btn.textContent = "Compare";
    state.compareRunA = null;
    state.compareRunB = null;
    state.compareTimelineA = [];
    state.compareTimelineB = [];
    if (state.boardMode) renderBoardGame();
    return;
  }
  if (btn) btn.textContent = "Exit Compare";
  if (!state.boardMode) toggleBoardMode();

  const runs = state.runs || [];
  if (runs.length < 2) {
    if (bar) { bar.style.display = "flex"; bar.innerHTML = "<span>Need at least 2 recorded paths to compare.</span>"; }
    return;
  }
  state.compareRunA = runs[0];
  state.compareRunB = runs[1];
  const [tlA, tlB] = await Promise.all([
    getJson(`/api/runs/${runs[0].run_id}/timeline`),
    getJson(`/api/runs/${runs[1].run_id}/timeline`),
  ]);
  state.compareTimelineA = tlA;
  state.compareTimelineB = tlB;
  renderCompareBar();
  renderCompareHexOverlay();
}

function renderCompareBar() {
  const bar = document.getElementById("board-compare-bar");
  if (!bar) return;
  bar.style.display = "flex";
  const a = state.compareRunA;
  const b = state.compareRunB;
  if (!a || !b) { bar.innerHTML = ""; return; }

  const nameA = a.runner || a.run_id?.split("_").slice(0, 2).join("_") || "Path A";
  const nameB = b.runner || b.run_id?.split("_").slice(0, 2).join("_") || "Path B";
  const stepsA = state.compareTimelineA.filter((e) => e.kind === "trace_call" || e.kind === "workflow_step").length;
  const stepsB = state.compareTimelineB.filter((e) => e.kind === "trace_call" || e.kind === "workflow_step").length;

  bar.innerHTML = `
    <span class="compare-pill compare-pill-a"><span class="compare-dot"></span>${escapeHtml(nameA)} (${stepsA} steps)</span>
    <span class="compare-pill compare-pill-b"><span class="compare-dot"></span>${escapeHtml(nameB)} (${stepsB} steps)</span>
    <div class="compare-score-row">
      <div class="compare-score"><span class="compare-score-val" style="color:#5ba0ff">${a.final_score ?? "?"}</span><span class="compare-score-label">${escapeHtml(nameA)}</span></div>
      <span style="color:rgba(244,248,251,0.2);font-size:1.2rem;align-self:center">vs</span>
      <div class="compare-score"><span class="compare-score-val" style="color:#ffb454">${b.final_score ?? "?"}</span><span class="compare-score-label">${escapeHtml(nameB)}</span></div>
    </div>
  `;
}

function renderCompareHexOverlay() {
  const hitsA = timelineSurfaceHits(state.compareTimelineA);
  const hitsB = timelineSurfaceHits(state.compareTimelineB);
  const allSurfaces = new Set([...Object.keys(hitsA), ...Object.keys(hitsB)]);

  document.querySelectorAll(".board-hex").forEach((hex) => {
    hex.classList.remove("hex-ring-a", "hex-ring-b");
    const s = hex.dataset.surface;
    if (hitsA[s]) hex.classList.add("hex-ring-a");
    if (hitsB[s]) hex.classList.add("hex-ring-b");

    const existingStat = hex.querySelector(".hex-compare-stat");
    if (existingStat) existingStat.remove();
    if (hitsA[s] || hitsB[s]) {
      const stat = document.createElement("div");
      stat.className = "hex-compare-stat";
      stat.innerHTML = `<span style="color:#5ba0ff">${hitsA[s] || 0}</span> / <span style="color:#ffb454">${hitsB[s] || 0}</span>`;
      hex.querySelector(".hex-content")?.appendChild(stat);
    }
  });

  clearFlowLines();
  drawCompareFlowLines(state.compareTimelineA, "#5ba0ff");
  drawCompareFlowLines(state.compareTimelineB, "#ffb454");
}

function drawCompareFlowLines(timeline, color) {
  const svg = document.getElementById("board-flow-svg");
  if (!svg) return;
  const area = svg.parentElement;
  if (!area) return;
  const areaRect = area.getBoundingClientRect();

  function hexCenter(surface) {
    const hex = document.querySelector(`.board-hex[data-surface="${surface}"]`);
    if (!hex) return null;
    const r = hex.getBoundingClientRect();
    return { x: r.left + r.width / 2 - areaRect.left, y: r.top + r.height / 2 - areaRect.top };
  }

  const surfaces = [];
  for (const ev of timeline) {
    if (ev.kind !== "trace_call" && ev.kind !== "workflow_step") continue;
    const s = toolToSurface(ev.tool || ev.resolved_tool);
    if (s && !surfaces.includes(s)) surfaces.push(s);
  }
  for (let i = 0; i < surfaces.length - 1; i++) {
    const pa = hexCenter(surfaces[i]);
    const pb = hexCenter(surfaces[i + 1]);
    if (!pa || !pb) continue;
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", pa.x);
    line.setAttribute("y1", pa.y);
    line.setAttribute("x2", pb.x);
    line.setAttribute("y2", pb.y);
    line.setAttribute("stroke", color);
    line.setAttribute("stroke-dasharray", "6 4");
    line.classList.add("flow-line");
    line.style.animation = "none";
    line.style.opacity = "0.4";
    svg.appendChild(line);
  }
}

function renderLivingCompanyView() {
  if (state.boardMode) {
    renderBoardGame();
    return;
  }
  renderLivingCompanyContext();
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
  const briefing = story.company_briefing || state.workspace?.manifest?.description || "";
  const mission = state.missionState?.mission || state.playableBundle?.mission || state.missions[0] || null;
  const failureImpact = story.failure_impact || mission?.failure_impact || "";
  if (!companyName) {
    panel.innerHTML = "";
    return;
  }
  const crisisLine = mission
    ? `<strong>${escapeHtml(mission.title)}</strong>: ${escapeHtml(mission.briefing || mission.description || "")}`
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

  const changedPanels = new Set(state.surfaceHighlights?.panels || []);
  const changedRefs = new Set(state.surfaceHighlights?.refs || []);
  const isCascade = state.cascadeActive && changedPanels.size > 0;

  const changedStrip = changedPanels.size > 0
    ? `<div class="changed-systems-strip">
        <span class="changed-systems-label">Changed:</span>
        ${[...changedPanels].map((s) =>
          `<span class="changed-system-chip">${escapeHtml(formatSurfaceTitle(s))}</span>`
        ).join("")}
      </div>`
    : "";

  panel.innerHTML = changedStrip + surfaceState.panels
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
  const mission = missionState?.mission || state.playableBundle?.mission || state.missions[0] || null;
  const objective = missionState?.objective_variant
    || document.getElementById("objective-select")?.value
    || mission?.default_objective
    || "default";
  const recommendedMove = (missionState?.available_moves || []).find(
    (move) => move.availability === "recommended" && !move.executed
  ) || (missionState?.available_moves || []).find((move) => !move.executed) || null;
  const latestToolEvent = [...(state.timeline || [])].reverse().find(
    (event) => event.resolved_tool || event.graph_intent
  );
  const changedCount = (state.surfaceHighlights?.panels || []).length;
  const surfaceState = state.surfaceState;

  const moveCount = (missionState?.executed_moves || []).length;
  const systemCount = (surfaceState?.panels || []).length;

  panel.innerHTML = `
    <div class="story-card accent-card">
      <p class="eyebrow">Current tension</p>
      <h3>${escapeHtml(surfaceState?.company_name || state.story?.manifest?.company_name || state.workspace?.manifest?.title || "Company")}</h3>
      <p class="metric-detail">${escapeHtml(mission?.briefing || state.story?.company_briefing || "Choose a crisis above to bring the company under pressure.")}</p>
    </div>
    <div class="story-card">
      <p class="eyebrow">Situation</p>
      <h3>${escapeHtml(mission?.title || "No crisis selected")}</h3>
      <div class="detail-grid">
        ${detailTile("Success means", objective)}
        ${detailTile("Branch", state.activeRun?.branch || missionState?.branch_name || "base")}
      </div>
      ${mission?.failure_impact ? `<p class="metric-detail">${escapeHtml(mission.failure_impact)}</p>` : ""}
    </div>
    ${
      recommendedMove
        ? `
          <div class="story-card">
            <p class="eyebrow">Recommended move</p>
            <h3>${escapeHtml(recommendedMove.title)}</h3>
            <p class="metric-detail">${escapeHtml(recommendedMove.consequence_preview || recommendedMove.summary || "")}</p>
          </div>
        `
        : ""
    }
    <div class="story-card">
      <p class="eyebrow">Pulse</p>
      <div class="detail-grid">
        ${detailTile("Moves", String(moveCount))}
        ${detailTile("Changed", String(changedCount))}
        ${detailTile("Systems", String(systemCount))}
        ${detailTile("Last tool", latestToolEvent?.resolved_tool || "waiting")}
      </div>
    </div>
  `;
}

function diffSurfaceState(before, after) {
  if (!before || !after) {
    return { panels: [], refs: [] };
  }
  const beforePanels = new Map((before.panels || []).map((panel) => [panel.surface, panel]));
  const afterPanels = new Map((after.panels || []).map((panel) => [panel.surface, panel]));
  const changedPanels = [];
  const changedRefs = [];

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
    for (const item of panel.items || []) {
      const signature = JSON.stringify(item);
      if (previousItems.get(item.item_id) !== signature && item.highlight_ref) {
        changedRefs.push(item.highlight_ref);
      }
    }
  }

  return {
    panels: changedPanels,
    refs: [...new Set(changedRefs)],
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

function renderMissionPlay() {
  const panel = document.getElementById("mission-moves-panel");
  const scorecard = document.getElementById("mission-scorecard");
  const status = document.getElementById("mission-form-status");
  if (!panel || !scorecard || !status) {
    return;
  }
  const missionState = state.missionState;
  if (!missionState) {
    scorecard.innerHTML = `
      <div class="story-card story-span-2">
        <p class="eyebrow">Play</p>
        <p class="metric-detail">Choose a situation and enter the world to begin making moves inside the company.</p>
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
            ${chip(move.availability, statusClass(move.availability === "blocked" ? "error" : move.availability === "risky" ? "running" : "ok"))}
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
      void applyMissionMove(node.dataset.moveId);
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
      return `
        <div class="move-log-entry">
          <div class="move-log-number">${index + 1}</div>
          <div class="move-log-body">
            <div class="chip-row">
              ${move.resolved_tool ? chip(move.resolved_tool, "ok") : ""}
              ${domain ? chip(formatDomainTitle(domain)) : ""}
              ${move.time_ms ? chip(`t=${formatMs(move.time_ms)}`) : ""}
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
  const storyPanel = document.getElementById("situation-story-panel");
  if (!preview || !contract) {
    panel.innerHTML = `<div class="metric-tile"><span class="metric-label">Scenario</span><span class="metric-value">Loading</span></div>`;
    storyPanel.innerHTML = "";
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
  const contractSummary = [
    metricTile("Scenario", scenario.title || scenario.name || "Scenario", scenario.scenario_name || ""),
    metricTile("Workflow", compiled.workflow_name || contract.workflow_name || "n/a", compiled.workflow_variant || "default"),
    metricTile("Scenario variant", activeScenarioVariant?.title || preview.active_scenario_variant || "default", activeScenarioVariant?.name || ""),
    metricTile("Contract variant", activeContractVariant?.title || preview.active_contract_variant || "default", activeContractVariant?.name || ""),
    metricTile("Facades", String((compiled.facades || []).length), facadeLabels),
    metricTile("Focus", scenario.inspection_focus || "summary", (scenario.tags || []).join(", ")),
  ].join("");
  panel.innerHTML = contractSummary;

  storyPanel.innerHTML = `
    <div class="story-card accent-card story-span-2">
      <p class="eyebrow">Situation briefing</p>
      <h3>${escapeHtml(activeScenarioVariant?.title || scenario.title || "Current situation")}</h3>
      <p class="metric-detail">${escapeHtml(state.story?.situation_briefing || activeScenarioVariant?.description || scenario.description || "")}</p>
      <div class="chip-row">
        ${chip(activeScenarioVariant?.name || preview.active_scenario_variant || "default")}
        ${(activeScenarioVariant?.branch_labels || whatIfBranches).map((item) => chip(item)).join("")}
      </div>
    </div>
    <div class="story-card">
      <p class="eyebrow">Why this variant exists</p>
      <p class="metric-detail">${escapeHtml(activeScenarioVariant?.rationale || "This situation is one alternate future on top of the same company world.")}</p>
    </div>
    <div class="story-card">
      <p class="eyebrow">What changed from base world</p>
      <p class="metric-detail">${escapeHtml((activeScenarioVariant?.change_summary || ["The base company stays fixed while the situation overlay changes deadlines, faults, or object state."]).join(" · "))}</p>
    </div>
    ${
      whatIfBranches.length
        ? `<div class="story-card story-span-2">
            <p class="eyebrow">What-if paths</p>
            <div class="chip-row">${whatIfBranches.map((item) => chip(item)).join("")}</div>
            <p class="metric-detail">These are alternate futures for the same company, not separate worlds.</p>
          </div>`
        : ""
    }
    ${
      scenarioVariants.length
        ? `<div class="story-card story-span-2">
            <p class="eyebrow">Available situations</p>
            <div class="stack">
              ${scenarioVariants
                .map(
                  (item) => `
                    <div class="run-item">
                      <div class="chip-row">
                        ${chip(item.name)}
                        ${item.active ? chip("active", "ok") : ""}
                      </div>
                      <strong>${escapeHtml(item.title)}</strong>
                      <p class="metric-detail">${escapeHtml(item.rationale || item.description || "")}</p>
                      <p class="metric-detail">${escapeHtml((item.change_summary || []).join(" · "))}</p>
                      <button type="button" class="ghost-button activate-scenario-variant" data-variant-name="${escapeHtml(item.name)}">Activate situation</button>
                    </div>
                  `
                )
                .join("")}
            </div>
          </div>`
        : ""
    }
  `;

  document.querySelectorAll(".activate-scenario-variant").forEach((node) => {
    node.addEventListener("click", () => {
      void activateScenarioVariant(node.dataset.variantName);
    });
  });

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
    <div class="score-strip">
      ${scorePill("Success predicates", String(successCount))}
      ${scorePill("Forbidden predicates", String(forbiddenCount))}
      ${scorePill("Policy invariants", String(invariants))}
      ${scorePill("Imported rules", String(importedRuleCount))}
      ${scorePill("Allowed tools", String(targetContract.observation_boundary?.allowed_tools?.length || targetContract.metadata?.observation_boundary?.allowed_tools?.length || 0))}
    </div>
    <div class="briefing-grid">
      <div class="story-card accent-card story-span-2">
        <p class="eyebrow">Objective briefing</p>
        <h3>${escapeHtml(selectedVariant?.title || state.scenarioPreview?.active_contract_variant || "Default objective")}</h3>
        <p class="metric-detail">${escapeHtml(state.story?.objective_briefing || selectedVariant?.objective_summary || selectedVariant?.description || "The active contract defines what counts as success, what must be avoided, and how tradeoffs are judged.")}</p>
      </div>
      <div class="story-card">
        <p class="eyebrow">Why this objective exists</p>
        <p class="metric-detail">${escapeHtml(selectedVariant?.rationale || "The same company world can be evaluated under different executive or operational preferences.")}</p>
      </div>
      <div class="story-card">
        <p class="eyebrow">World invariant</p>
        <p class="metric-detail">${escapeHtml(state.story?.manifest?.objective_focus || "The contract keeps the company world honest by making business consequences explicit.")}</p>
      </div>
      ${
        availableVariants.length
          ? `<div class="story-card story-span-2">
              <p class="eyebrow">Available objectives</p>
              <div class="stack">
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
  const whatIfBranches = Array.isArray(story.branch_labels) && story.branch_labels.length
    ? story.branch_labels
    : state.scenarioPreview?.scenario?.metadata?.builder_environment?.what_if_branches || [];
  panel.innerHTML = `
    <div class="score-strip">
      ${scorePill("Contract", run.contract?.ok === null ? "pending" : run.contract?.ok ? "pass" : "fail", run.contract?.contract_name || "workspace contract")}
      ${scorePill("Assertions", `${successPassed}/${successTotal}`)}
      ${scorePill("Issues", String(issueCount))}
      ${scorePill("Policy failures", String(policyFails))}
      ${scorePill("Latency p95", formatMs(run.metrics?.latency_p95_ms || 0))}
      ${scorePill("Virtual time", formatMs(run.metrics?.time_ms || 0))}
    </div>
    <div class="briefing-grid">
      <div class="story-card accent-card">
        <p class="eyebrow">Outcome</p>
        <h3>${run.contract?.ok ? "Good branch" : run.contract?.ok === false ? "Broken branch" : "Outcome pending"}</h3>
        <p class="metric-detail">This path came from the same company state, but it produced a different business outcome.</p>
        <div class="detail-grid">
          ${detailTile("Run events", compactNumber(state.timeline.length))}
          ${detailTile("Graph actions", compactNumber(graphEvents.length))}
          ${detailTile("Snapshots", compactNumber(state.snapshots.length))}
          ${detailTile("Domains", compactNumber(graphDomains.length))}
        </div>
        <div class="chip-row">${graphDomains.map((item) => chip(formatDomainTitle(item))).join("")}</div>
      </div>
      <div class="story-card">
        <p class="eyebrow">Recorded trail</p>
        <h3>One path, full receipts</h3>
        <div class="chip-row">
          ${chip("timeline")}
          ${chip("comparisons")}
          ${chip("receipts")}
        </div>
        <p class="metric-detail">Every path records state, decisions, receipts, and tool activity in one place, so you can compare what happened and why.</p>
        <div class="chip-row">${resolvedTools.slice(0, 5).map((item) => chip(item)).join("")}</div>
      </div>
      ${
        whatIfBranches.length
          ? `<div class="story-card">
              <p class="eyebrow">What-if paths</p>
              <h3>Branch labels</h3>
              <div class="chip-row">${whatIfBranches.map((item) => chip(item)).join("")}</div>
              <p class="metric-detail">These branch ideas are alternate futures that begin from the same company state.</p>
            </div>`
          : ""
      }
    </div>
  `;
  if (branchPanel) {
    const outcome = story.outcome || null;
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
          <div class="stack">${(outcome.what_changed || []).map((item) => `<p class="metric-detail">${escapeHtml(item)}</p>`).join("")}</div>
        </div>
        <div class="story-card story-span-2">
          <p class="eyebrow">Why the result was good or bad</p>
          <div class="stack">${(outcome.why_it_matters || []).map((item) => `<p class="metric-detail">${escapeHtml(item)}</p>`).join("")}</div>
        </div>
      `
      : `
        <div class="story-card story-span-2">
          <p class="eyebrow">Branch story</p>
          <p class="metric-detail">Open a baseline path and a comparison path to see how the same company can end in different states.</p>
        </div>
      `;
  }
  renderExportsPanel();
  renderJson("run-panel", run);
  renderJson("contract-panel", contract || { note: "No run contract evaluation yet." });
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
  const [workspace, storyArtifacts, playableArtifacts, scenarios, importSummary, identityFlow, importSources, importNormalization, importReview, generatedImportScenarios, provenanceIndex] = await Promise.all([
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
  ]);
  state.workspace = workspace;
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
  status.textContent = "Entering world\u2026";
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
    state.missionState = payload;
    if (payload.run_id) {
      await selectRun(payload.run_id, { previousSurfaceState: null });
    }
    state.missionState = payload;
    state.boardEventLog = [];
    renderMissionPlay();
    status.textContent = `Mission ${payload.mission?.title || payload.run_id} is live.`;
    setStudioView("company");
  } catch (error) {
    status.textContent = `Mission start failed: ${error}`;
  }
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
    state.missionState = payload;

    const oldSurface = previousSurfaceState;
    await refreshActiveRun(payload.run_id, { previousSurfaceState: oldSurface });
    state.missionState = payload;

    const diff = diffSurfaceState(oldSurface, state.surfaceState);
    if (diff.panels.length > 0) {
      if (state.boardMode) {
        const playedCard = document.querySelector(`.board-card[data-move-id="${moveId}"]`);
        fireParticleTrail(playedCard, diff.panels);
        recordBoardEvent(moveTitle, diff.panels);
        animateHexRipple(diff.panels);
        drawFlowLines(diff.panels);
      } else {
        state.cascadeActive = true;
        renderSurfaceWall();
        await playCascade(diff.panels, diff.refs);
      }
    }
    renderLivingCompanyView();
    renderMissionPlay();

    status.textContent = payload.status === "completed"
      ? "Mission completed. Inspect the results or branch the run."
      : `${moveTitle} \u2014 ${diff.panels.length} system${diff.panels.length === 1 ? "" : "s"} affected.`;
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
  try {
    const payload = await getJson(`/api/missions/${state.missionState.run_id}/branch`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    state.missionState = payload;
    await loadRuns({ selectActiveRun: false });
    state.missionState = payload;
    await selectRun(payload.run_id, { previousSurfaceState: state.surfaceState });
    state.missionState = payload;
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
  try {
    const payload = await getJson(`/api/missions/${state.missionState.run_id}/finish`, {
      method: "POST",
    });
    state.missionState = payload;
    await loadRuns({ selectActiveRun: false });
    state.missionState = payload;
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
  const [run, timeline, orientation, graphs, snapshots, contract, surfaces] = await Promise.all([
    getJson(`/api/runs/${runId}`),
    getJson(`/api/runs/${runId}/timeline`),
    getJson(`/api/runs/${runId}/orientation`),
    getJson(`/api/runs/${runId}/graphs`),
    getJson(`/api/runs/${runId}/snapshots`),
    getJson(`/api/runs/${runId}/contract`),
    getJson(`/api/runs/${runId}/surfaces`).catch(() => null),
  ]);

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
    void refreshActiveRun(runId, { preserveSurfaceHighlights: true });
  };
  state.eventSource.onerror = () => {
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
  };
}

async function selectRun(runId, options = {}) {
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
  document.getElementById("board-toggle").addEventListener("click", toggleBoardMode);
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
