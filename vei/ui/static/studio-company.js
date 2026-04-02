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
  const mirror = state.mirrorStatus;
  const mirrorActive = mirror && mirror.config && (Array.isArray(mirror.agents) ? mirror.agents.length > 0 : false || mirror.config.demo_mode);
  const modeBanner = mirrorActive
    ? `<div class="context-mode-banner"><span class="context-mode-dot"></span>Live mirror &mdash; agents run through the control plane</div>`
    : "";

  panel.innerHTML = `
    ${modeBanner}
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
    hint.textContent = "Run complete \u2014 review outcome or start a new situation";
  } else if (ms?.run_id) {
    const moveCount = (ms.executed_moves || []).length;
    hint.textContent = moveCount
      ? `${moveCount} move${moveCount === 1 ? "" : "s"} played \u2014 pick the next action or end the run`
      : "You\u2019re in the scenario \u2014 play your first move below";
  } else if (state.missions.length) {
    hint.textContent = "Pick a situation above, then watch every system react";
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
  const pending = Array.isArray(mirror.pending_approvals)
    ? mirror.pending_approvals.filter((item) => item.status === "pending").length
    : 0;
  const profileOptions = Array.isArray(mirror.policy_profiles) ? mirror.policy_profiles : [];
  const approvers = agents.filter((agent) => agent.resolved_policy_profile?.can_approve);

  const connectorStrip = (Array.isArray(mirror.connector_status) ? mirror.connector_status : []).map((item) => {
    const statusClass = `connector-${item.availability || "healthy"} connector-${item.write_capability || "interactive"}`;
    const _connectorLabels = { slack: "Slack", jira: "Jira", graph: "Graph", salesforce: "Salesforce", mail: "Mail", service_ops: "Service Ops" };
    const label = _connectorLabels[item.surface] || (item.surface ? item.surface.charAt(0).toUpperCase() + item.surface.slice(1) : "Unknown");
    const detail = `${item.source_mode} ${String(item.write_capability || "").replace("_", " ")}`;
    return `
      <div class="connector-pill ${statusClass}" title="${escapeHtml(item.reason || detail)}">
        <span class="connector-dot"></span>
        <span class="connector-name">${label}</span>
        <span class="connector-detail">${escapeHtml(detail)}</span>
      </div>
    `;
  }).join("");

  const agentCards = agents.map((agent) => {
    const statusClass = `agent-status-${agent.status || "registered"}`;
    const role = agent.role ? agent.role.replace(/_/g, " ") : agent.mode || "agent";
    const surfaces = Array.isArray(agent.allowed_surfaces) ? agent.allowed_surfaces.join(", ") : "";
    const denied = agent.denied_count || 0;
    const throttled = agent.throttled_count || 0;
    const profileBadge = agent.resolved_policy_profile
      ? `<span class="agent-policy-badge">${escapeHtml(agent.resolved_policy_profile.label)}</span>`
      : "";
    const profileSelect = profileOptions.map((profile) =>
      `<option value="${escapeHtml(profile.profile_id)}" ${profile.profile_id === agent.policy_profile_id ? "selected" : ""}>${escapeHtml(profile.label)}</option>`
    ).join("");
    return `
      <div class="mirror-agent-card${denied > 0 ? " mirror-agent-has-denials" : ""}">
        <div class="agent-card-top">
          <span class="agent-name">${escapeHtml(agent.name || agent.agent_id)}</span>
          ${profileBadge}
        </div>
        <span class="agent-role">${escapeHtml(role)}</span>
        <span class="agent-status ${statusClass}">${agent.status || "registered"}</span>
        <div class="agent-metrics">
          <span>${denied} blocked</span>
          <span>${throttled} throttled</span>
          ${surfaces ? `<span>${escapeHtml(surfaces)}</span>` : ""}
        </div>
        ${agent.last_action ? `<span class="agent-last-action">${escapeHtml(agent.last_action)}</span>` : ""}
        <details class="agent-card-edit-disclosure">
          <summary>Configure</summary>
          <div class="agent-edit-grid">
            <label><span>Profile</span><select data-agent-profile="${escapeHtml(agent.agent_id)}">${profileSelect}</select></label>
            <label><span>Status</span><select data-agent-status="${escapeHtml(agent.agent_id)}">
              <option value="registered" ${agent.status === "registered" ? "selected" : ""}>registered</option>
              <option value="active" ${agent.status === "active" ? "selected" : ""}>active</option>
              <option value="idle" ${agent.status === "idle" ? "selected" : ""}>idle</option>
              <option value="error" ${agent.status === "error" ? "selected" : ""}>error</option>
            </select></label>
            <label class="agent-edit-surfaces"><span>Surfaces</span><input data-agent-surfaces="${escapeHtml(agent.agent_id)}" value="${escapeHtml(surfaces)}" /></label>
          </div>
          <div class="agent-btn-row">
            <button type="button" class="ghost-button agent-save-btn" data-agent-save="${escapeHtml(agent.agent_id)}">Save agent</button>
            <button type="button" class="ghost-button agent-remove-btn" data-agent-remove="${escapeHtml(agent.agent_id)}">Remove</button>
          </div>
        </details>
      </div>
    `;
  }).join("");

  const allApprovals = mirror.pending_approvals || [];
  const pendingApprovals = allApprovals.filter((item) => item.status === "pending");
  const resolvedApprovals = allApprovals.filter((item) => item.status !== "pending");
  const sortedApprovals = [...pendingApprovals, ...resolvedApprovals];

  const approvalQueue = sortedApprovals.length
    ? sortedApprovals.map((item) => {
          const isPending = item.status === "pending";
          const badgeMap = { approved: "approval-badge-approved", rejected: "approval-badge-rejected" };
          const badgeClass = badgeMap[item.status] || "approval-badge-held";
          const badgeLabel = item.status === "pending" ? "held" : item.status;
          const resolverOptions = approvers.map((agent) =>
            `<option value="${escapeHtml(agent.agent_id)}">${escapeHtml(agent.name || agent.agent_id)}</option>`
          ).join("");
          return `
            <div class="approval-row${isPending ? "" : " approval-resolved"}">
              <div class="approval-copy">
                <strong>${escapeHtml(item.resolved_tool)}</strong>
                <span>${escapeHtml(item.agent_id)} · ${escapeHtml(item.surface)} · ${escapeHtml(item.reason || "approval required")}</span>
              </div>
              <span class="approval-badge ${badgeClass}">${badgeLabel}</span>
              ${isPending ? `<div class="approval-actions">
                <select data-approval-resolver="${escapeHtml(item.approval_id)}">${resolverOptions}</select>
                <button type="button" class="ghost-button approval-approve-btn" data-approval-approve="${escapeHtml(item.approval_id)}">Approve</button>
                <button type="button" class="ghost-button approval-reject-btn" data-approval-reject="${escapeHtml(item.approval_id)}">Reject</button>
              </div>` : ""}
            </div>
          `;
        }).join("")
    : `<p class="metric-detail">No approvals yet.</p>`;

  const recentEvents = Array.isArray(mirror.recent_events) ? mirror.recent_events : [];
  const _feedTimeAgo = (ts) => {
    if (!ts) return "";
    const diff = Math.max(0, Math.floor((Date.now() - new Date(ts).getTime()) / 1000));
    if (diff < 60) return `${diff}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    return `${Math.floor(diff / 3600)}h ago`;
  };
  const _renderFeedItem = (evt) => {
    const denied = evt.handled_by === "denied";
    const pendingApproval = evt.handled_by === "pending_approval";
    const cls = denied
      ? "mirror-feed-item mirror-feed-denied"
      : pendingApproval
        ? "mirror-feed-item mirror-feed-pending"
        : "mirror-feed-item";
    const label = evt.label || evt.tool || "event";
    const reason = evt.reason
      || ({
        "mirror.surface_denied": "Surface not allowed",
        "mirror.profile_denied": "Policy tier blocked the action",
        "mirror.approval_required": "Held for approval",
        "mirror.rate_limited": "Rate limited",
        "mirror.connector_degraded": "Connector unavailable",
        "mirror.unsupported_live_write": "Live write not supported",
      }[evt.reason_code] || evt.reason_code || "");
    const handledTag = pendingApproval
      ? '<span class="feed-pending-tag">held</span>'
      : denied
        ? '<span class="feed-denied-tag">blocked</span>'
        : `<span class="feed-handled-tag">${escapeHtml(evt.handled_by || "ok")}</span>`;
    const ts = _feedTimeAgo(evt.timestamp);
    return `<div class="${cls}"><span class="feed-agent">${escapeHtml(evt.agent_id)}</span><span class="feed-label">${escapeHtml(label)}</span>${handledTag}${ts ? `<span class="feed-ts">${ts}</span>` : ""}${reason ? `<span class="feed-reason">${escapeHtml(reason)}</span>` : ""}</div>`;
  };
  const FEED_CAP = 20;
  const reversed = recentEvents.slice().reverse();
  const eventFeedHtml = reversed.length
    ? (() => {
        const visible = reversed.slice(0, FEED_CAP).map(_renderFeedItem).join("");
        if (reversed.length > FEED_CAP) {
          const rest = reversed.slice(FEED_CAP).map(_renderFeedItem).join("");
          return visible + `<details class="feed-show-all"><summary>${reversed.length - FEED_CAP} older events</summary>${rest}</details>`;
        }
        return visible;
      })()
    : `<p class="metric-detail">No governed actions yet.</p>`;

  const addAgentForm = `
    <details class="mirror-agent-form-disclosure">
      <summary class="mirror-feed-header">Register new agent</summary>
      <div class="mirror-agent-form">
        <div class="agent-edit-grid">
          <label><span>Agent ID</span><input id="mirror-new-agent-id" placeholder="control-lead" /></label>
          <label><span>Name</span><input id="mirror-new-agent-name" placeholder="Control Lead" /></label>
          <label><span>Mode</span><select id="mirror-new-agent-mode">
            <option value="proxy">proxy</option>
            <option value="ingest">ingest</option>
            <option value="demo">demo</option>
          </select></label>
          <label><span>Profile</span><select id="mirror-new-agent-profile">${profileOptions.map((profile) => `<option value="${escapeHtml(profile.profile_id)}">${escapeHtml(profile.label)}</option>`).join("")}</select></label>
          <label class="agent-edit-surfaces"><span>Surfaces</span><input id="mirror-new-agent-surfaces" placeholder="slack, service_ops" /></label>
        </div>
        <button type="button" class="ghost-button" id="mirror-register-agent-btn">Add agent</button>
      </div>
    </details>
  `;

  el.innerHTML = `
    <div class="mirror-fleet-header">
      <span class="fleet-label">Control Plane</span>
      <span class="fleet-badge ${badgeClass}">${mode}</span>
      <span class="fleet-stat">${eventCount} events · ${pending} pending approvals</span>
    </div>
    <div class="mirror-connector-strip">${connectorStrip}</div>
    ${addAgentForm}
    <div class="mirror-fleet-body">
      <div class="mirror-agents-grid">${agentCards}</div>
      <div class="mirror-event-feed">
        <div class="mirror-feed-header">Approval Queue</div>
        <div class="approval-queue">${approvalQueue}</div>
        <div class="mirror-feed-header">Activity Log</div>
        <div class="mirror-feed-list">${eventFeedHtml}</div>
      </div>
    </div>
  `;

  document.getElementById("mirror-register-agent-btn")?.addEventListener("click", async () => {
    const agentId = document.getElementById("mirror-new-agent-id")?.value?.trim();
    const name = document.getElementById("mirror-new-agent-name")?.value?.trim();
    const modeValue = document.getElementById("mirror-new-agent-mode")?.value || "ingest";
    const profileValue = document.getElementById("mirror-new-agent-profile")?.value || "operator";
    const surfacesRaw = document.getElementById("mirror-new-agent-surfaces")?.value || "";
    if (!agentId || !name) return;
    await mirrorPost("/api/workspace/mirror/agents", {
      agent_id: agentId,
      name,
      mode: modeValue,
      policy_profile_id: profileValue,
      allowed_surfaces: surfacesRaw.split(",").map((item) => item.trim()).filter(Boolean),
    }).catch(() => null);
    await refreshAfterMirrorMutation();
  });

  el.querySelectorAll("[data-agent-save]").forEach((node) => {
    node.addEventListener("click", async () => {
      const agentId = node.dataset.agentSave;
      const profile = el.querySelector(`[data-agent-profile="${agentId}"]`)?.value;
      const status = el.querySelector(`[data-agent-status="${agentId}"]`)?.value;
      const surfaces = el.querySelector(`[data-agent-surfaces="${agentId}"]`)?.value || "";
      await mirrorPatch(`/api/workspace/mirror/agents/${encodeURIComponent(agentId)}`, {
        policy_profile_id: profile,
        status,
        allowed_surfaces: surfaces.split(",").map((item) => item.trim()).filter(Boolean),
      }).catch(() => null);
      await refreshAfterMirrorMutation();
    });
  });

  el.querySelectorAll("[data-agent-remove]").forEach((node) => {
    node.addEventListener("click", async () => {
      const agentId = node.dataset.agentRemove;
      if (!agentId) return;
      await mirrorDelete(`/api/workspace/mirror/agents/${encodeURIComponent(agentId)}`).catch(() => null);
      await refreshAfterMirrorMutation();
    });
  });

  el.querySelectorAll("[data-approval-approve]").forEach((node) => {
    node.addEventListener("click", async () => {
      const approvalId = node.dataset.approvalApprove;
      const resolver = el.querySelector(`[data-approval-resolver="${approvalId}"]`)?.value;
      if (!resolver) return;
      await mirrorPost(`/api/workspace/mirror/approvals/${encodeURIComponent(approvalId)}/approve`, {
        resolver_agent_id: resolver,
      }).catch(() => null);
      await refreshAfterMirrorMutation();
    });
  });

  el.querySelectorAll("[data-approval-reject]").forEach((node) => {
    node.addEventListener("click", async () => {
      const approvalId = node.dataset.approvalReject;
      const resolver = el.querySelector(`[data-approval-resolver="${approvalId}"]`)?.value;
      if (!resolver) return;
      await mirrorPost(`/api/workspace/mirror/approvals/${encodeURIComponent(approvalId)}/reject`, {
        resolver_agent_id: resolver,
      }).catch(() => null);
      await refreshAfterMirrorMutation();
    });
  });
}

function renderSurfaceWall() {
  const panel = document.getElementById("living-company-surface-wall");
  if (!panel) {
    return;
  }
  const surfaceState = state.surfaceState;
  if (!surfaceState || !Array.isArray(surfaceState.panels) || !surfaceState.panels.length) {
    const loadingRun = state.missionState?.run_id || state.activeRunId;
    const companyName = state.workspace?.manifest?.title
      || state.story?.manifest?.company_name
      || "Your company";
    panel.innerHTML = `
      <div class="surface-placeholder">
        <p class="eyebrow">Living Company</p>
        <h3>${loadingRun ? "Loading company systems" : `${escapeHtml(companyName)} is ready`}</h3>
        <p class="metric-detail">${
          loadingRun
            ? "Loading the latest company state so the tools can appear here."
            : "Enter the world to see Slack, email, tickets, and the ops loop come alive."
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
      <p class="eyebrow">Active pressure</p>
      <h3>${escapeHtml(surfaceState?.company_name || state.story?.manifest?.company_name || state.workspace?.manifest?.title || "Company")}</h3>
      <p class="metric-detail">${escapeHtml(currentCrisisSummary())}</p>
    </div>
    <div class="story-card">
      <p class="eyebrow">Situation</p>
      <h3>${escapeHtml(currentCrisisTitle())}</h3>
      <div class="detail-grid">
        ${detailTile("Success criteria", objective)}
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
    ${
      state.snapshots?.length
        ? `
          <div class="story-card fork-rail-card">
            <p class="eyebrow">World snapshot</p>
            <h3>${escapeHtml(state.snapshots[state.snapshots.length - 1]?.label || `Snapshot #${state.snapshots.length}`)}</h3>
            <p class="metric-detail">${state.snapshots.length} snapshot${state.snapshots.length === 1 ? "" : "s"} captured</p>
            ${currentSnapshotForkRunId() ? `<button type="button" class="ghost-button rail-fork-btn" data-fork-from-rail="1">Fork from here</button>` : ""}
          </div>
        `
        : ""
    }
  `;

  panel.querySelector("[data-fork-from-rail]")?.addEventListener("click", () => {
    void branchMission();
  });
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
    renderOutcomeActions();
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
  renderOutcomeActions();
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

