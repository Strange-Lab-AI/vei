window.VEIStudio = window.VEIStudio || {};
const studio = window.VEIStudio;

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
  knowledge_graph: "Knowledge",
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
  governorWorkspace: null,
  presentation: null,
  playableBundle: null,
  missions: [],
  missionState: null,
  fidelityReport: null,
  exportsPreview: [],
  workforceStatus: null,
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
  historicalWorkspace: null,
  publicDemoStatus: null,
  publicDemoChat: [],
  publicDemoScore: null,
  publicDemoPending: false,
  whatIfStatus: null,
  whatIfSearchPending: false,
  whatIfOpenPending: false,
  whatIfRunPending: false,
  whatIfRankPending: false,
  whatIfSearchResult: null,
  whatIfSelectedEvent: null,
  whatIfScene: null,
  whatIfSceneLoading: false,
  whatIfChosenOptionLabel: "",
  whatIfCustomMovePrompt: "",
  whatIfOpenResult: null,
  whatIfExperimentResult: null,
  whatIfRankedResult: null,
  selectedObjectRef: null,
  runs: [],
  activeRunId: null,
  activeRun: null,
  activeRunContract: null,
  timeline: [],
  orientation: null,
  graphs: null,
  knowledgeStore: null,
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
  studioView: "public-history",
  activeCompanySection: "company-overview",
  historicalAutoFocusKey: "",
  developerMode: false,
  cinemaMode: false,
  timelineMode: false,
  compareMode: false,
  compareRunA: null,
  compareRunB: null,
  compareSnapshotA: null,
  compareSnapshotB: null,
  compareTimelineA: [],
  compareTimelineB: [],
  compareMissionA: null,
  compareMissionB: null,
  compareKnowledgeA: null,
  compareKnowledgeB: null,
  visualTone: "control-room",
  cinemaAutoAdvance: false,
  cinemaAutoTimer: null,
  playbackTimer: null,
  eventSource: null,
  cascadeActive: false,
  cascadeAbort: null,
  refreshGeneration: 0,
  sseDebounceTimer: null,
};

studio.state = state;

async function getJson(path, options = {}) {
  const response = await fetch(path, options);
  if (!response.ok) {
    let detail = `Request failed: ${response.status}`;
    try {
      const payload = await response.json();
      if (payload?.detail) {
        detail = formatErrorDetail(payload.detail);
      }
    } catch {
      try {
        const text = await response.text();
        if (text) {
          detail = text;
        }
      } catch {}
    }
    throw new Error(detail);
  }
  return await response.json();
}

function formatErrorDetail(detail) {
  if (typeof detail === "string") {
    return detail;
  }
  if (Array.isArray(detail)) {
    const parts = detail
      .map((item) => formatErrorDetail(item))
      .filter((item) => Boolean(item));
    return parts.join("; ") || "Request failed";
  }
  if (detail && typeof detail === "object") {
    if (typeof detail.msg === "string" && detail.loc) {
      const location = Array.isArray(detail.loc)
        ? detail.loc.join(".")
        : String(detail.loc);
      return location ? `${location}: ${detail.msg}` : detail.msg;
    }
    if (typeof detail.detail === "string") {
      return detail.detail;
    }
    const parts = Object.entries(detail)
      .map(([key, value]) => {
        const rendered = formatErrorDetail(value);
        if (!rendered) {
          return "";
        }
        return `${key}: ${rendered}`;
      })
      .filter((item) => Boolean(item));
    return parts.join("; ") || JSON.stringify(detail);
  }
  if (detail === null || detail === undefined) {
    return "Request failed";
  }
  return String(detail);
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
  const governorWorkspace = await getJson("/api/workspace/governor").catch(() => null);
  return {
    exercise: governorWorkspace?.exercise || null,
    governorWorkspace,
  };
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

studio.getJson = getJson;
studio.formatErrorDetail = formatErrorDetail;
studio.fetchStoryArtifacts = fetchStoryArtifacts;
studio.fetchExerciseArtifacts = fetchExerciseArtifacts;
studio.fetchPlayableArtifacts = fetchPlayableArtifacts;

function el(id) {
  return document.getElementById(id);
}

function currentSnapshotForkRunId() {
  if (!state.activeRun?.run_id) {
    return null;
  }
  return state.activeRun.run_id;
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

async function governorPost(path, payload) {
  return await getJson(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
}

async function governorPatch(path, payload) {
  return await getJson(path, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
}

async function governorDelete(path) {
  return await getJson(path, { method: "DELETE" });
}

async function refreshMirrorStatus() {
  const governorWorkspace = await getJson("/api/workspace/governor").catch(() => null);
  applyGovernorWorkspaceStatus(governorWorkspace);
  renderLivingCompanyView();
  renderTrustStrip();
}

async function ensureRunSnapshots(run) {
  if (!run) return [];
  if (Array.isArray(run.snapshots) && run.snapshots.length) {
    return run.snapshots;
  }
  const snapshots = await getJson(`/api/runs/${run.run_id}/snapshots`).catch(() => []);
  run.snapshots = Array.isArray(snapshots) ? snapshots : [];
  return run.snapshots;
}

async function refreshAfterMirrorMutation() {
  await refreshMirrorStatus();
  if (state.activeRunId) {
    const previousSurfaceState = state.surfaceState;
    await refreshActiveRun(state.activeRunId, {
      previousSurfaceState,
      preserveSurfaceHighlights: true,
    }).catch(() => {});
  }
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

function applyGovernorWorkspaceStatus(payload) {
  const workspaceStatus = nonEmptyPayload(payload);
  state.governorWorkspace = workspaceStatus;
  state.governorStatus = nonEmptyPayload(workspaceStatus?.governor);
  state.workforceStatus = nonEmptyPayload(workspaceStatus?.workforce);
  state.exercise = nonEmptyPayload(workspaceStatus?.exercise);
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

function hasHistoricalWorkspace() {
  return Boolean(state.historicalWorkspace?.branch_event_id);
}

function hasExerciseMode() {
  if (hasHistoricalWorkspace()) {
    return false;
  }
  return Boolean(state.exercise?.manifest);
}

function inferStudioTone() {
  if (hasHistoricalWorkspace()) {
    return "casebook";
  }
  return "control-room";
}

function applyStudioTone(tone) {
  const resolvedTone = tone === "casebook" ? "casebook" : "control-room";
  state.visualTone = resolvedTone;
  document.body.dataset.veiTone = resolvedTone;
}

function syncStudioTone() {
  applyStudioTone(inferStudioTone());
}

function historicalBranchSummary() {
  const historical = state.historicalWorkspace;
  if (!historical) {
    return "";
  }
  const branch = historical.branch_event || {};
  const actor = branch.actor_id || "the recorded sender";
  const target = branch.target_id || "the recorded recipient";
  const subject = historical.thread_subject || branch.subject || "historical thread";
  const company = historical.organization_name || "this company";
  return `Historical replay for ${company} from ${actor} to ${target} on "${subject}".`;
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

const RUNNER_TITLES = {
  workflow: "Workflow baseline",
  scripted: "Scripted baseline",
  external: "Outside agent",
  llm: "Model-driven path",
  bc: "Learned policy",
  human: "Human path",
};

function humanize(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return "";
  }
  return text
    .replace(/\./g, " ")
    .replace(/[_-]+/g, " ")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function scenarioVariantDetail(name) {
  const variants = Array.isArray(state.scenarioPreview?.available_scenario_variants)
    ? state.scenarioPreview.available_scenario_variants
    : [];
  return variants.find((item) => item.name === name) || null;
}

function contractVariantDetail(name) {
  const variants = Array.isArray(state.scenarioPreview?.available_contract_variants)
    ? state.scenarioPreview.available_contract_variants
    : [];
  return variants.find((item) => item.name === name) || null;
}

function displayScenarioVariantTitle(name, fallback = "Scenario") {
  const detail = scenarioVariantDetail(name);
  if (detail?.title) {
    return detail.title;
  }
  return humanize(name || fallback);
}

function displayContractVariantTitle(name, fallback = "Success mode") {
  const detail = contractVariantDetail(name);
  if (detail?.title) {
    return detail.title;
  }
  return humanize(name || fallback);
}

function displayWorkflowTitle(name, fallback = "Workflow") {
  return humanize(name || fallback);
}

function displayBranchTitle(name, fallback = "Base") {
  const text = String(name || fallback).trim();
  const match = text.match(/^(.*?)(?:_\d{8}_\d{6}(?:_\d+)?(?:_[a-z0-9]+)?)$/i);
  if (match?.[1]) {
    return humanize(match[1]);
  }
  return humanize(text || fallback);
}

function displayRunnerTitle(name, fallback = "Run") {
  const normalized = String(name || "").trim().toLowerCase();
  if (RUNNER_TITLES[normalized]) {
    return RUNNER_TITLES[normalized];
  }
  return humanize(name || fallback);
}

function displayStatusTitle(name, fallback = "") {
  return humanize(name || fallback);
}

function displayContractHealth(ok) {
  if (ok === null || ok === undefined) {
    return "Pending";
  }
  return ok ? "Healthy" : "At risk";
}

function currentCrisisTitle() {
  if (hasHistoricalWorkspace()) {
    return state.historicalWorkspace?.thread_subject || "Historical replay";
  }
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
  if (hasHistoricalWorkspace()) {
    return `${historicalBranchSummary()} Branch just before the saved event and compare alternate paths.`;
  }
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
  if (hasHistoricalWorkspace()) {
    const historical = state.historicalWorkspace;
    if (!historical) {
      return "";
    }
    return `${historical.history_message_count || 0} events before the branch point and ${historical.future_event_count || 0} recorded future events after it.`;
  }
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
  if (hasHistoricalWorkspace()) {
    return "Inspect the saved branch point, replay the historical future, and compare alternate paths against the recorded thread.";
  }
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
  return GRAPH_TITLES[domain] || humanize(domain);
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
    history: "public-history",
    public: "public-history",
    "public history": "public-history",
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

const STUDIO_VIEW_HELPER_TEXT = {
  "public-history": "Ask the historical news world what was visible, then test public actions.",
  company: "Company state, missions, and historical what-if are all in this view.",
  crisis: "Review the active situation, constraints, and objective before choosing a move.",
  outcome: "Inspect run outcomes, compare paths, and verify effects across systems.",
  audit: "Audit model-ranked decisions and log reviewer judgments.",
};

const COMPANY_SECTIONS = {
  "company-overview": "company-overview",
  "company-mission": "company-mission",
  "company-recent": "company-recent",
  "company-historical": "company-historical",
};

function setActiveCompanySection(sectionId) {
  const normalized = COMPANY_SECTIONS[sectionId] || "company-overview";
  state.activeCompanySection = normalized;
  document.querySelectorAll(".company-subnav-button").forEach((node) => {
    node.classList.toggle("active", node.dataset.companyTarget === normalized);
  });
}

function updateStudioViewHelper() {
  const helper = document.getElementById("studio-view-helper");
  if (!helper) {
    return;
  }
  helper.textContent = STUDIO_VIEW_HELPER_TEXT[state.studioView] || STUDIO_VIEW_HELPER_TEXT.company;
}

function setStudioView(view) {
  state.studioView = normalizeStudioView(view);
  document.querySelectorAll("main [data-studio-view]").forEach((node) => {
    node.classList.toggle("hidden-panel", node.dataset.studioView !== state.studioView);
  });
  document.querySelectorAll(".studio-nav-button").forEach((node) => {
    node.classList.toggle("active", node.dataset.studioView === state.studioView);
  });
  if (state.studioView === "audit" && typeof loadAuditQueue === "function") {
    loadAuditQueue();
  }
  const companySubnav = document.getElementById("company-subnav");
  if (companySubnav) {
    companySubnav.hidden = state.studioView !== "company";
  }
  updateStudioViewHelper();
}

function toggleDeveloperMode() {
  state.developerMode = !state.developerMode;
  document.body.classList.toggle("developer-mode", state.developerMode);
  closeToolbarMenus();
  document.getElementById("developer-toggle").textContent = state.developerMode
    ? "Hide technical detail"
    : "Technical detail";
}

function jumpToStudioView(view) {
  const normalized = normalizeStudioView(view);
  setStudioView(normalized);
  const target = document.querySelector(`[data-studio-view="${normalized}"]`);
  if (target) {
    target.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

function jumpToCompanySection(sectionId, { behavior = "smooth", forceCompanyView = true } = {}) {
  const normalized = COMPANY_SECTIONS[sectionId] || "company-overview";
  setActiveCompanySection(normalized);
  if (forceCompanyView) {
    setStudioView("company");
  }
  const target = document.getElementById(normalized);
  if (target) {
    target.scrollIntoView({ behavior, block: "start" });
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
  const isPublicHistory = manifest.metadata?.ui_mode === "public_history";
  if (isPublicHistory) {
    const publicDemo = state.publicDemoStatus || {};
    const evidenceCount = Array.isArray(publicDemo.evidence_events)
      ? publicDemo.evidence_events.length
      : 0;
    const actionCount = Array.isArray(publicDemo.suggested_candidate_actions)
      ? publicDemo.suggested_candidate_actions.length
      : 0;
    panel.innerHTML = [
      chip(manifest.title || manifest.name || "Public History"),
      chip(`${evidenceCount} visible event${evidenceCount === 1 ? "" : "s"}`),
      chip(`${actionCount} candidate action${actionCount === 1 ? "" : "s"}`),
      chip(publicDemo.available ? "Saved bundle" : "Unavailable", publicDemo.available ? "ok" : "warn"),
    ].join("");
    return;
  }
  const latestRun = state.runs[0];
  const missionState = state.missionState;
  const systemCount = state.surfaceState?.panels?.length
    || (state.story?.manifest?.key_surfaces || []).length
    || 0;
  panel.innerHTML = [
    chip(manifest.title || manifest.name || "Workspace"),
    chip(`${systemCount} system${systemCount === 1 ? "" : "s"}`),
    chip(`${workspace.run_count || 0} path${workspace.run_count === 1 ? "" : "s"}`),
    chip(
      displayStatusTitle(missionState?.status || (latestRun ? latestRun.status : "ready"), "Ready"),
      statusClass(missionState?.status || latestRun?.status)
    ),
  ].join("");
}

function renderWorkspaceHero() {
  const workspace = state.workspace;
  if (!workspace) {
    return;
  }
  syncStudioTone();
  const manifest = workspace.manifest || {};
  const isPublicHistory = manifest.metadata?.ui_mode === "public_history";
  document.body.classList.toggle("public-history-mode", isPublicHistory);
  if (isPublicHistory) {
    document.title = "VEI Public History";
  }
  const story = state.story || {};
  const title = document.getElementById("workspace-title");
  const subtitle = document.getElementById("workspace-subtitle");
  const topbarEyebrow = document.querySelector(".brand-copy .eyebrow");
  const historical = state.historicalWorkspace;
  const companyName = historical?.organization_name || manifest.title || story.manifest?.company_name || "Workspace";
  const missionLine = currentCrisisSummary();
  if (topbarEyebrow) {
    topbarEyebrow.textContent = isPublicHistory ? "Public History" : "Company";
  }
  if (title) {
    title.textContent = companyName;
  }
  if (subtitle) {
    subtitle.classList.remove("loading-pulse");
    subtitle.textContent = missionLine;
  }
  const status = document.getElementById("mission-form-status");
  if (status && hasHistoricalWorkspace()) {
    status.textContent = "Inspect the saved branch point, then run the historical what-if below.";
  } else if (status && isPublicHistory) {
    status.textContent = "Ask the public record, then test a move.";
  }
  const hint = document.getElementById("shell-context-hint");
  if (hint && hasHistoricalWorkspace() && !state.missionState?.run_id) {
    hint.textContent = "Inspect the historical branch and compare alternate paths";
  } else if (hint && isPublicHistory) {
    hint.textContent = "Ask the historical news world what was visible, then test public actions.";
  }
  renderWorkspaceMetrics();
  renderStudioShell();
  renderWorldsPanel();
  renderTrustStrip();
  renderJson("workspace-panel", workspace);
}

function renderTrustStrip() {
  const el = document.getElementById("workspace-trust-strip");
  if (!el) {
    return;
  }
  const manifest = state.workspace?.manifest || {};
  if (manifest.metadata?.ui_mode === "public_history") {
    el.textContent = "Public news snapshot: historical cutoff enforced";
    return;
  }
  const parts = [];
  const mirror = state.governorStatus;
  if (mirror && mirror.config && typeof mirror.config === "object") {
    const cfg = mirror.config;
    if (cfg.demo_mode) {
      parts.push("Control plane: governor demo (staged activity)");
    } else if (cfg.connector_mode === "live") {
      parts.push("Control plane: live connectors on governed paths");
    } else {
      parts.push("Control plane: simulated connectors (no live writes)");
    }
  } else {
    parts.push("Company state: simulated workspace");
  }
  const syncs = Array.isArray(state.importSources?.syncs) ? state.importSources.syncs : [];
  const okSyncs = syncs.filter((s) => s && s.status === "ok");
  if (okSyncs.length) {
    const latest = [...okSyncs].sort((a, b) =>
      String(b.synced_at || "").localeCompare(String(a.synced_at || ""))
    )[0];
    if (latest?.synced_at) {
      parts.push(`Last import sync: ${latest.synced_at}`);
    }
  }
  el.textContent = parts.join(" \u00b7 ");
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
  const missionLabel = document.getElementById("mission-field-label");
  const objectiveLabel = document.getElementById("objective-field-label");
  if (!missionSelect || !objectiveSelect) {
    return;
  }
  if (hasHistoricalWorkspace()) {
    const historical = state.historicalWorkspace;
    const threadSubject = historical?.thread_subject || "Historical thread";
    missionSelect.innerHTML = `<option value="${escapeHtml(historical?.thread_id || "historical")}">${escapeHtml(threadSubject)}</option>`;
    objectiveSelect.innerHTML = '<option value="historical_replay">Historical replay</option>';
    missionSelect.disabled = true;
    objectiveSelect.disabled = true;
    if (missionLabel) {
      missionLabel.textContent = "Historical thread";
    }
    if (objectiveLabel) {
      objectiveLabel.textContent = "Replay mode";
    }
    if (startButton) {
      startButton.textContent = "Saved branch";
      startButton.disabled = true;
    }
    return;
  }
  missionSelect.disabled = false;
  objectiveSelect.disabled = false;
  if (missionLabel) {
    missionLabel.textContent = "Crisis";
  }
  if (objectiveLabel) {
    objectiveLabel.textContent = "Success criteria";
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
      startButton.textContent = "Update situation";
      startButton.disabled = false;
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
    startButton.textContent = "Start";
    startButton.disabled = false;
  }
}

function renderMissionSummary() {
  const briefing = document.getElementById("mission-briefing");
  const catalog = document.getElementById("mission-catalog");
  if (!briefing) {
    return;
  }
  if (hasHistoricalWorkspace()) {
    const historical = state.historicalWorkspace;
    const branch = historical?.branch_event || {};
    briefing.innerHTML = `
      <div class="story-card accent-card story-span-2 crisis-hero-card">
        <p class="eyebrow">Historical replay</p>
        <h3>${escapeHtml(currentCrisisTitle())}</h3>
        <p class="metric-detail">${escapeHtml(currentCrisisSummary())}</p>
        <div class="chip-row">
          ${chip(`${historical?.history_message_count || 0} prior events`)}
          ${chip(`${historical?.future_event_count || 0} future events`)}
          ${chip(branch.event_type || "branch event")}
        </div>
      </div>
      <div class="story-card">
        <p class="eyebrow">Branch event</p>
        <p class="metric-detail">${escapeHtml(branch.actor_id || "Recorded sender")} → ${escapeHtml(branch.target_id || "Recorded recipient")}</p>
        <p class="metric-detail">${escapeHtml(branch.timestamp || historical?.branch_timestamp || "")}</p>
      </div>
      <div class="story-card">
        <p class="eyebrow">Historical scope</p>
        <p class="metric-detail">${escapeHtml(currentFailureImpact())}</p>
      </div>
    `;
    if (catalog) {
      catalog.innerHTML = "";
    }
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
          ${chip(displayScenarioVariantTitle(state.scenarioPreview?.active_scenario_variant, "Current situation"))}
          ${chip(displayContractVariantTitle(state.scenarioPreview?.active_contract_variant, "Default objective"))}
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
                ${chip(displayScenarioVariantTitle(item.name, item.title || "Scenario"))}
                ${item.name === state.scenarioPreview?.active_scenario_variant ? chip("active", "ok") : ""}
              </div>
              <h3>${escapeHtml(item.title || item.name || "Scenario")}</h3>
              <p class="metric-detail">${escapeHtml(item.description || "")}</p>
              <button type="button" class="ghost-button activate-scenario-variant-button" data-variant-name="${escapeHtml(item.name || "")}">Switch to this</button>
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
        ${chip(formatDomainTitle(currentMission.primary_domain || "world"))}
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
            ${chip(humanize(item.mission_name))}
            ${item.hero ? chip("primary", "ok") : chip("included")}
          </div>
          <h3>${escapeHtml(item.title)}</h3>
          <p class="metric-detail">${escapeHtml(item.briefing || "")}</p>
          <p class="metric-detail">${escapeHtml(item.why_it_matters || "")}</p>
          <div class="chip-row">
            ${(item.supported_objectives || []).map((objective) => chip(displayContractVariantTitle(objective, objective))).join("")}
          </div>
          <button type="button" class="ghost-button activate-mission-button" data-mission-name="${escapeHtml(item.mission_name)}">Switch to this</button>
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
    if (tBtn) tBtn.textContent = "Event timeline";
    const section = document.getElementById("timeline-section");
    if (section) section.style.display = "none";
  }
  document.body.classList.toggle("cinema-mode", state.cinemaMode);
  const btn = document.getElementById("cinema-toggle");
  if (btn) btn.textContent = state.cinemaMode ? "Exit demo mode" : "Demo mode";
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

  const mirrorEvents = state.governorStatus?.recent_events || [];
  const latestMirrorEvent = mirrorEvents.length ? mirrorEvents[mirrorEvents.length - 1] : null;

  let narrativeLine = "";
  if (latestMirrorEvent && state.governorStatus?.autoplay_running) {
    const lbl = latestMirrorEvent.label || latestMirrorEvent.tool || "event";
    const handledBy = latestMirrorEvent.handled_by || "";
    narrativeLine = `${lbl}`;
    if (handledBy === "denied") narrativeLine += " \u2192 blocked";
    else if (handledBy === "pending_approval") narrativeLine += " \u2192 held for approval";
    else if (handledBy) narrativeLine += ` \u2192 ${handledBy}`;
  } else if (ms?.status === "completed") {
    narrativeLine = score.mission_success
      ? "Scenario resolved successfully."
      : "Scenario closed with remaining exposure.";
  } else if (lastMove) {
    const tool = lastMove.resolved_tool || "";
    const refs = (lastMove.object_refs || []).slice(0, 3).join(", ");
    narrativeLine = `Move ${moveCount}: ${lastMove.title}`;
    if (tool) narrativeLine += ` \u2192 ${tool}`;
    if (refs) narrativeLine += ` \u2192 ${refs}`;
  } else if (mission) {
    narrativeLine = mission.briefing || mission.description || "Starting scenario\u2026";
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
    if (cinBtn) cinBtn.textContent = "Demo mode";
    stopCinemaAutoAdvance();
  }
  document.body.classList.toggle("timeline-mode", state.timelineMode);
  const btn = document.getElementById("timeline-toggle");
  if (btn) btn.textContent = state.timelineMode ? "Close event timeline" : "Event timeline";
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
  return formatSurfaceTitle(s);
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
  html += `<span class="tl-stat">Risk <strong>${escapeHtml(humanize(score.business_risk || "\u2014"))}</strong></span>`;
  if (completed) {
    const cls = score.mission_success ? "tl-result-ok" : "tl-result-fail";
    const label = score.mission_success ? "Resolved" : "Unresolved";
    html += `<span class="tl-result ${cls}">${label} &mdash; ${score.success_assertions_passed || 0}/${score.success_assertions_total || 0} success checks</span>`;
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
    const snapA = document.getElementById("compare-snapshot-a");
    const snapB = document.getElementById("compare-snapshot-b");
    if (snapA && state.compareSnapshotA != null) {
      snapA.value = String(state.compareSnapshotA);
      snapA.addEventListener("change", onCompareSnapshotPickerChange);
    }
    if (snapB && state.compareSnapshotB != null) {
      snapB.value = String(state.compareSnapshotB);
      snapB.addEventListener("change", onCompareSnapshotPickerChange);
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
  const [tlA, tlB, contractA, contractB, missionA, missionB, snapshotsA, snapshotsB, knowledgeA, knowledgeB] = await Promise.all([
    getJson(`/api/runs/${runA.run_id}/timeline`),
    getJson(`/api/runs/${runB.run_id}/timeline`),
    getJson(`/api/runs/${runA.run_id}/contract`).catch(() => null),
    getJson(`/api/runs/${runB.run_id}/contract`).catch(() => null),
    getJson(`/api/missions/state?run_id=${encodeURIComponent(runA.run_id)}`).catch(() => null),
    getJson(`/api/missions/state?run_id=${encodeURIComponent(runB.run_id)}`).catch(() => null),
    ensureRunSnapshots(runA),
    ensureRunSnapshots(runB),
    getJson(`/api/runs/${runA.run_id}/knowledge`).catch(() => null),
    getJson(`/api/runs/${runB.run_id}/knowledge`).catch(() => null),
  ]);
  state.compareRunA = runA;
  state.compareRunB = runB;
  state.compareSnapshotA = snapshotsA.length ? snapshotsA[snapshotsA.length - 1].snapshot_id : null;
  state.compareSnapshotB = snapshotsB.length ? snapshotsB[snapshotsB.length - 1].snapshot_id : null;
  state.compareTimelineA = tlA;
  state.compareTimelineB = tlB;
  state.compareContractA = contractA;
  state.compareContractB = contractB;
  state.compareMissionA = nonEmptyPayload(missionA);
  state.compareMissionB = nonEmptyPayload(missionB);
  state.compareKnowledgeA = nonEmptyPayload(knowledgeA);
  state.compareKnowledgeB = nonEmptyPayload(knowledgeB);
}

async function toggleCompareMode() {
  closeToolbarMenus();
  state.compareMode = !state.compareMode;
  const btn = document.getElementById("compare-toggle");
  if (!state.compareMode) {
    if (btn) btn.textContent = "Compare paths";
    state.compareRunA = null;
    state.compareRunB = null;
    state.compareSnapshotA = null;
    state.compareSnapshotB = null;
    state.compareTimelineA = [];
    state.compareTimelineB = [];
    state.compareMissionA = null;
    state.compareMissionB = null;
    state.compareKnowledgeA = null;
    state.compareKnowledgeB = null;
    if (state.timelineMode) renderTimelineView();
    return;
  }
  if (btn) btn.textContent = "Close comparison";
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
  const snapASelect = document.getElementById("compare-snapshot-a");
  const snapBSelect = document.getElementById("compare-snapshot-b");
  if (!selA || !selB) return;
  const runs = state.runs || [];
  const runA = runs.find((r) => r.run_id === selA.value);
  const runB = runs.find((r) => r.run_id === selB.value);
  if (!runA || !runB || runA.run_id === runB.run_id) return;
  await loadCompareRunData(runA, runB);
  if (snapASelect && state.compareSnapshotA != null) snapASelect.value = String(state.compareSnapshotA);
  if (snapBSelect && state.compareSnapshotB != null) snapBSelect.value = String(state.compareSnapshotB);
  renderTimelineView();
}
