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
    status.textContent = `Run launch failed: ${error?.message || error}`;
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
    status.textContent = `Scenario activation failed: ${error?.message || error}`;
  }
}

// ---------------------------------------------------------------------------
// Connect panel
// ---------------------------------------------------------------------------

/** Neutral markers (avoid emoji in enterprise procurement contexts). */
const PROVIDER_ICONS = {
  slack: "\u25CF",
  google: "\u25CF",
  jira: "\u25CF",
  okta: "\u25CF",
  gmail: "\u25CF",
  teams: "\u25CF",
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
      const icon = PROVIDER_ICONS[p.provider] || "\u25CF";
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
  document.getElementById("outcome-compare-btn")?.addEventListener("click", toggleCompareMode);
  document.getElementById("outcome-timeline-btn")?.addEventListener("click", async () => {
    if (state.compareMode) {
      await toggleCompareMode();
    }
    if (!state.timelineMode) {
      toggleTimelineMode();
    }
    setStudioView("company");
    renderTimelineView();
    document.getElementById("timeline-section")?.scrollIntoView({ behavior: "smooth", block: "start" });
  });
  document.getElementById("outcome-policy-btn")?.addEventListener("click", () => {
    void onTryDifferentPolicyClick();
  });
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
    const msg = error?.message || String(error);
    document.getElementById("workspace-subtitle").textContent =
      `Could not load workspace. Check the server is running and the path is a valid VEI workspace. (${msg})`;
    document.getElementById("workspace-subtitle").classList.remove("loading-pulse");
  });
