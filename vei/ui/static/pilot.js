const state = {
  pilot: null,
  exercise: null,
  dataset: null,
  mode: "exercise",
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

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

function currentExerciseTitle() {
  const manifest = state.exercise?.manifest;
  const variant = manifest?.scenario_variant || manifest?.crisis_name || state.pilot?.manifest?.crisis_name || "";
  const catalogMatch = (manifest?.catalog || []).find((item) => item.scenario_variant === manifest?.scenario_variant);
  return catalogMatch?.crisis_name || humanize(variant) || "Current exercise";
}

function maskToken(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return "Unavailable";
  }
  if (text.length <= 8) {
    return "Available";
  }
  return `${text.slice(0, 4)}...${text.slice(-4)}`;
}

async function getJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

async function loadJsonOrNull(url, options) {
  try {
    const payload = await getJson(url, options);
    if (!payload || (typeof payload === "object" && !Array.isArray(payload) && !Object.keys(payload).length)) {
      return null;
    }
    return payload;
  } catch {
    return null;
  }
}

function metricTile(label, value, detail = "") {
  return `
    <article class="metric-card">
      <p class="eyebrow">${escapeHtml(label)}</p>
      <strong>${escapeHtml(value)}</strong>
      ${detail ? `<p class="metric-detail">${escapeHtml(detail)}</p>` : ""}
    </article>
  `;
}

function renderModePanels() {
  document.querySelectorAll("[data-operator-mode]").forEach((button) => {
    button.classList.toggle("active", button.dataset.operatorMode === state.mode);
  });
  document.querySelectorAll("[data-operator-mode-panel]").forEach((panel) => {
    panel.hidden = panel.dataset.operatorModePanel !== state.mode;
  });
}

function renderBanner() {
  const banner = document.getElementById("pilot-status-banner");
  const payload = state.pilot;
  if (!banner) {
    return;
  }
  banner.classList.remove("pilot-status-live", "pilot-status-waiting");
  if (!payload?.manifest) {
    banner.textContent = "This workspace is not set up for external agents yet.";
    return;
  }
  if (payload.services_ready) {
    banner.classList.add("pilot-status-live");
    banner.textContent = "All systems are live. Connect an outside agent, compare it against the baselines, and watch the company respond.";
    return;
  }
  banner.classList.add("pilot-status-waiting");
  banner.textContent = "The files exist, but the gateway or Studio is not running right now.";
}

function renderHeader() {
  const pilot = state.pilot;
  const manifest = pilot?.manifest;
  const title = manifest
    ? `${manifest.organization_name} — ${currentExerciseTitle()}`
    : "Workspace not configured";
  const summary = manifest
    ? "Studio is the company view. This console connects agents, compares paths, and builds datasets."
    : "This workspace has not been set up for external agents yet. Ask the team to configure it before running an exercise here.";
  document.getElementById("pilot-company-title").textContent = title;
  document.getElementById("pilot-summary").textContent = summary;
  const studioLink = document.getElementById("pilot-open-studio");
  const gatewayLink = document.getElementById("pilot-open-gateway");
  if (studioLink) {
    studioLink.href = manifest?.studio_url || "/";
  }
  if (gatewayLink) {
    if (!manifest) {
      gatewayLink.hidden = true;
    } else {
      gatewayLink.hidden = false;
      const base = String(manifest.gateway_url || "").replace(/\/$/, "");
      const statusUrl = manifest.gateway_status_url || (base ? `${base}/healthz` : "");
      const target = statusUrl || base;
      gatewayLink.href = target || "#";
      gatewayLink.title = target
        ? "Open gateway health or status (new tab)"
        : "Gateway URL missing from manifest";
      if (!target) {
        gatewayLink.setAttribute("aria-disabled", "true");
      } else {
        gatewayLink.removeAttribute("aria-disabled");
      }
    }
  }
}

function renderLaunch() {
  const manifest = state.pilot?.manifest;
  const launchGrid = document.getElementById("pilot-launch-grid");
  const surfaceList = document.getElementById("pilot-surface-list");
  const snippetsPanel = document.getElementById("pilot-snippets");
  if (!launchGrid || !surfaceList || !snippetsPanel) {
    return;
  }
  if (!manifest) {
    launchGrid.innerHTML = metricTile("Workspace", "Not configured", "Set up this workspace before connecting outside agents.");
    surfaceList.innerHTML = "";
    snippetsPanel.innerHTML = "";
    return;
  }
  const activeAgents = state.pilot?.active_agents || [];
  const surfaces = manifest.supported_surfaces || [];
  const snippets = manifest.snippets || [];
  const statusValue = state.pilot?.services_ready ? "Live" : "Waiting";
  const statusDetail = state.pilot?.services_ready
    ? "Studio, gateway, and the exercise controls are ready."
    : "The launch details exist, but the stack is not fully running right now.";
  launchGrid.innerHTML = [
    metricTile("Status", statusValue, statusDetail),
    metricTile("Agents seen", String(activeAgents.length), activeAgents.length ? "Names are shown in the activity stream" : "No external agent has connected yet"),
    metricTile("Recommended first exercise", currentExerciseTitle(), manifest.recommended_first_exercise || "Start small and stay customer-safe."),
    metricTile("Supported surfaces", String(surfaces.length), surfaces.length ? "These systems are available to the outside agent." : "No surfaces are registered for this workspace."),
  ].join("");
  surfaceList.innerHTML = surfaces
    .map((surface) => `<span class="badge">${escapeHtml(surface.title)} · ${escapeHtml(surface.base_path)}</span>`)
    .join("");
  snippetsPanel.innerHTML = `
    <details class="pilot-connection-details">
      <summary>Connection details</summary>
      <div class="pilot-connection-grid">
        ${metricTile("Studio URL", manifest.studio_url, "Company-side view")}
        ${metricTile("Operator Console URL", manifest.pilot_console_url, "Operator sidecar")}
        ${metricTile("Gateway URL", manifest.gateway_url, "Outside-agent entrypoint")}
        ${metricTile("Bearer token", maskToken(manifest.bearer_token), "Hidden by default until you open the local snippets below")}
      </div>
      ${
        snippets.length
          ? `<div class="pilot-snippets">
              ${snippets
                .map((snippet) => `
                  <article class="pilot-snippet-card">
                    <div class="pilot-snippet-head">
                      <strong>${escapeHtml(snippet.title)}</strong>
                      <div class="chip-row">
                        <span class="badge">${escapeHtml(humanize(snippet.name))}</span>
                        <span class="badge">${escapeHtml(snippet.language || "bash")}</span>
                      </div>
                    </div>
                    <pre>${escapeHtml(snippet.content)}</pre>
                  </article>
                `)
                .join("")}
            </div>`
          : ""
      }
    </details>
  `;
}

function renderExercise() {
  const overview = document.getElementById("exercise-overview-grid");
  const criteria = document.getElementById("exercise-criteria");
  const comparison = document.getElementById("exercise-comparison");
  const compatibility = document.getElementById("exercise-compatibility");
  const catalog = document.getElementById("exercise-catalog");
  const payload = state.exercise;
  if (!overview || !criteria || !comparison || !compatibility || !catalog) {
    return;
  }
  if (!payload?.manifest) {
    overview.innerHTML = metricTile("Exercise", "Not configured", "Run `vei exercise up` to create a scored exercise with comparison paths.");
    criteria.innerHTML = "";
    comparison.innerHTML = "";
    compatibility.innerHTML = "";
    catalog.innerHTML = "";
    return;
  }

  const manifest = payload.manifest;
  overview.innerHTML = [
    metricTile("Company", manifest.company_name, humanize(manifest.archetype)),
    metricTile("Current crisis", currentExerciseTitle(), humanize(manifest.scenario_variant)),
    metricTile("Success lens", humanize(manifest.contract_variant), "The success lens that decides whether this path helped"),
    metricTile("First move", manifest.recommended_first_move, "Start by reading the company before you act"),
  ].join("");

  criteria.innerHTML = `
    <article class="pilot-note-card">
      <p class="eyebrow">Success criteria</p>
      <ul class="pilot-bullets">
        ${(manifest.success_criteria || []).map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
      </ul>
    </article>
  `;

  comparison.innerHTML = (payload.comparison || [])
    .map((row) => `
      <article class="pilot-activity-card">
        <div class="pilot-activity-head">
          <strong>${escapeHtml(row.label)}</strong>
          <span class="badge">${escapeHtml(humanize(row.status || "missing"))}</span>
        </div>
        <p class="metric-detail">${escapeHtml(row.summary || "No summary yet.")}</p>
        <div class="chip-row">
          ${row.run_id ? `<span class="badge">Run ${escapeHtml(row.run_id)}</span>` : ""}
          <span class="badge">Actions ${escapeHtml(row.action_count || 0)}</span>
          <span class="badge">Issues ${escapeHtml(row.issue_count || 0)}</span>
          ${row.contract_ok === true ? `<span class="badge">Contract healthy</span>` : ""}
          ${row.contract_ok === false ? `<span class="badge">Contract at risk</span>` : ""}
        </div>
      </article>
    `)
    .join("");
  if (!payload.comparison?.length) {
    comparison.innerHTML = `<article class="pilot-note-card"><p class="metric-detail">No comparison runs recorded yet. Connect an agent, complete a path, and refresh to see baseline vs candidate side by side.</p></article>`;
  }

  compatibility.innerHTML = manifest.supported_api_subset
    .map((surface) => `
      <article class="pilot-note-card">
        <div class="pilot-activity-head">
          <strong>${escapeHtml(surface.title)}</strong>
          <span class="badge">${escapeHtml(surface.base_path)}</span>
        </div>
        <ul class="pilot-bullets">
          ${surface.endpoints.map((endpoint) => `<li><strong>${escapeHtml(endpoint.method)} ${escapeHtml(endpoint.path)}</strong> — ${escapeHtml(endpoint.description)}</li>`).join("")}
        </ul>
      </article>
    `)
    .join("");

  catalog.innerHTML = manifest.catalog
    .map((item) => `
      <article class="pilot-catalog-card ${item.active ? "pilot-catalog-card-active" : ""}">
        <div class="pilot-activity-head">
          <strong>${escapeHtml(item.crisis_name)}</strong>
          ${item.active ? '<span class="badge">Active</span>' : `<button type="button" class="ghost-button pilot-catalog-button" data-scenario-variant="${escapeHtml(item.scenario_variant)}" data-contract-variant="${escapeHtml(item.contract_variant || "")}">Switch to this</button>`}
        </div>
        <p class="metric-detail">${escapeHtml(item.summary)}</p>
        <p class="metric-detail">${escapeHtml(item.objective_summary)}</p>
      </article>
    `)
    .join("");
}

function renderDataset() {
  const summary = document.getElementById("dataset-summary-grid");
  const splits = document.getElementById("dataset-splits");
  const exportsPanel = document.getElementById("dataset-exports");
  const payload = state.dataset;
  if (!summary || !splits || !exportsPanel) {
    return;
  }
  if (!payload) {
    summary.innerHTML = metricTile("Dataset", "Not built", "Run `vei dataset build` to generate a matrix of environments and export a clean corpus.");
    splits.innerHTML = "";
    exportsPanel.innerHTML = "";
    return;
  }
  const formats = [...new Set((payload.exports || []).map((item) => item.format))];
  summary.innerHTML = [
    metricTile("Environments", String(payload.environment_count || 0), "Distinct twin variants"),
    metricTile("Runs", String(payload.run_count || 0), "Completed paths included in this bundle"),
    metricTile("Formats", formats.join(", ") || "none", "Conversation, trajectory, and demonstration exports"),
    metricTile("Success rate", String(payload.reward_summary?.success_rate ?? 0), `Contract healthy rate ${payload.reward_summary?.contract_ok_rate ?? 0}`),
  ].join("");
  splits.innerHTML = (payload.splits || [])
    .map((split) => `
      <article class="pilot-note-card">
        <div class="pilot-activity-head">
          <strong>${escapeHtml(split.split)}</strong>
          <span class="badge">${escapeHtml(split.run_count)} runs</span>
        </div>
        <p class="metric-detail">${escapeHtml(split.example_count)} examples</p>
      </article>
    `)
    .join("");
  exportsPanel.innerHTML = (payload.exports || [])
    .map((item) => `
      <article class="pilot-activity-card">
        <div class="pilot-activity-head">
          <strong>${escapeHtml(item.format)} · ${escapeHtml(item.split)}</strong>
          <span class="badge">${escapeHtml(item.example_count)} examples</span>
        </div>
        <p class="metric-detail">${escapeHtml(item.path)}</p>
      </article>
    `)
    .join("");
}

function renderActivity() {
  const panel = document.getElementById("pilot-activity-list");
  const activity = state.pilot?.activity || [];
  if (!panel) {
    return;
  }
  if (!activity.length) {
    panel.innerHTML = `<p class="metric-detail">No outside-agent actions yet. Use the sample client or connect your own agent to start the run.</p>`;
    return;
  }
  panel.innerHTML = activity
    .map((item) => {
      const actor = [item.agent_role, item.agent_name].filter(Boolean).join(" / ") || item.agent_source || "Unattributed client";
      return `
        <article class="pilot-activity-card">
          <div class="pilot-activity-head">
            <strong>${escapeHtml(item.label)}</strong>
            <span class="badge">${escapeHtml(item.channel)}</span>
          </div>
          <p class="metric-detail">${escapeHtml(actor)}</p>
          <p class="metric-detail">${escapeHtml(item.tool || "No resolved tool recorded")}</p>
          ${(item.object_refs || []).length
            ? `<div class="chip-row">${item.object_refs.map((ref) => `<span class="badge">${escapeHtml(ref)}</span>`).join("")}</div>`
            : ""}
        </article>
      `;
    })
    .join("");
}

function renderOutcome() {
  const pilot = state.pilot;
  const outcome = pilot?.outcome;
  const panel = document.getElementById("pilot-outcome-grid");
  if (!panel) {
    return;
  }
  if (!outcome) {
    panel.innerHTML = metricTile(
      "Outcome",
      "Not ready yet",
      "Outcome metrics appear after the gateway has handled traffic and recorded a scored window. Connect an agent, run a few actions, then refresh this page."
    );
    return;
  }
  panel.innerHTML = [
    metricTile("Twin status", pilot.twin_status || "stopped", outcome.summary),
    metricTile("Requests", String(pilot.request_count || 0), "External requests handled"),
    metricTile("Contract", outcome.contract_ok === true ? "healthy" : outcome.contract_ok === false ? "at risk" : "pending", `${outcome.issue_count || 0} open issues`),
    metricTile("Active pressure", outcome.current_tension || "No live pressure summary", outcome.latest_tool || "No latest tool yet"),
  ].join("");
  if ((outcome.affected_surfaces || []).length) {
    panel.innerHTML += `
      <article class="pilot-outcome-callout">
        <p class="eyebrow">Surfaces under pressure</p>
        <div class="chip-row">
          ${outcome.affected_surfaces.map((item) => `<span class="badge">${escapeHtml(item)}</span>`).join("")}
        </div>
      </article>
    `;
  }
}

function renderControls() {
  const manifest = state.pilot?.manifest;
  const actionStatus = document.getElementById("pilot-action-status");
  const resetButton = document.getElementById("pilot-reset-button");
  const finalizeButton = document.getElementById("pilot-finalize-button");
  const disabled = !manifest;
  resetButton.disabled = disabled;
  finalizeButton.disabled = disabled;
  if (!manifest && actionStatus) {
    actionStatus.textContent = "Run `vei exercise up` or `vei pilot up` to enable reset and finalize controls.";
  }
}

function renderAll() {
  renderModePanels();
  renderBanner();
  renderHeader();
  renderLaunch();
  renderExercise();
  renderDataset();
  renderActivity();
  renderOutcome();
  renderControls();
}

async function loadOperatorStatus() {
  const [pilot, exercise, dataset] = await Promise.all([
    loadJsonOrNull("/api/pilot"),
    loadJsonOrNull("/api/exercise"),
    loadJsonOrNull("/api/dataset"),
  ]);
  state.pilot = pilot;
  state.exercise = exercise;
  state.dataset = dataset;
  renderAll();
}

async function runPilotAction(path, successMessage) {
  const status = document.getElementById("pilot-action-status");
  status.textContent = "Working…";
  try {
    state.pilot = await getJson(path, { method: "POST" });
    state.exercise = await loadJsonOrNull("/api/exercise");
    status.textContent = successMessage;
    renderAll();
  } catch (error) {
    status.textContent = error instanceof Error ? error.message : "Request failed";
  }
}

async function activateExercise(scenarioVariant, contractVariant) {
  const status = document.getElementById("pilot-action-status");
  status.textContent = "Switching the active exercise…";
  try {
    state.exercise = await getJson("/api/exercise/activate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        scenario_variant: scenarioVariant,
        contract_variant: contractVariant || null,
      }),
    });
    state.pilot = await loadJsonOrNull("/api/pilot");
    status.textContent = "Exercise switched. The baselines were refreshed for the new crisis.";
    renderAll();
  } catch (error) {
    status.textContent = error instanceof Error ? error.message : "Switch failed";
  }
}

document.querySelectorAll("[data-operator-mode]").forEach((button) => {
  button.addEventListener("click", () => {
    state.mode = button.dataset.operatorMode || "exercise";
    renderModePanels();
  });
});

document.getElementById("pilot-reset-button").addEventListener("click", () => {
  runPilotAction("/api/pilot/reset", "Twin reset complete. The gateway is back at baseline.");
});

document.getElementById("pilot-finalize-button").addEventListener("click", () => {
  runPilotAction("/api/pilot/finalize", "Current run finalized.");
});

document.body.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const button = target.closest(".pilot-catalog-button");
  if (!(button instanceof HTMLElement)) {
    return;
  }
  const scenarioVariant = button.dataset.scenarioVariant || "";
  const contractVariant = button.dataset.contractVariant || "";
  if (!scenarioVariant) {
    return;
  }
  activateExercise(scenarioVariant, contractVariant);
});

loadOperatorStatus();
setInterval(loadOperatorStatus, 5000);
