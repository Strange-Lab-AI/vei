async function loadControlSurface() {
  const status = document.getElementById("control-status");
  const agents = document.getElementById("control-agents");
  const accessReview = document.getElementById("control-access-review");
  const timeline = document.getElementById("control-timeline");
  const blastRadius = document.getElementById("control-blast-radius");
  const policyReplay = document.getElementById("control-policy-replay");
  const evidencePack = document.getElementById("control-evidence-pack");
  if (!status || !agents || !accessReview || !timeline || !blastRadius || !policyReplay || !evidencePack) return;
  status.innerHTML = `<p class="metric-detail">Loading Control evidence...</p>`;
  try {
    const data = await getJson("/api/workspace/provenance/control");
    status.innerHTML = renderControlMetrics(data);
    agents.innerHTML = renderAgents(data.agents || []);
    accessReview.innerHTML = renderAccessReview(data.access_review);
    timeline.innerHTML = renderTimeline(data);
    blastRadius.innerHTML = renderBlastRadius(data.blast_radius);
    policyReplay.innerHTML = renderPolicyReplay();
    evidencePack.innerHTML = renderEvidencePack(data.evidence_pack);
    bindControlActions();
  } catch (error) {
    status.innerHTML = `<p class="connect-error">Control load failed: ${escapeHtml(error?.message || error)}</p>`;
  }
}

function renderControlMetrics(data) {
  return `
    <div class="metric-tile"><strong>${escapeHtml(data.event_count || 0)}</strong><span>canonical events</span></div>
    <div class="metric-tile"><strong>${escapeHtml(data.ingest?.batch_count || 0)}</strong><span>ingest batches</span></div>
    <div class="metric-tile"><strong>${escapeHtml((data.agents || []).length)}</strong><span>agents</span></div>
    <div class="metric-tile"><strong>${escapeHtml(data.graph?.edge_count || 0)}</strong><span>evidence edges</span></div>
  `;
}

function renderAgents(items) {
  if (!items.length) return `<p class="metric-detail">No agent evidence yet.</p>`;
  return items.slice(0, 20).map((agent) => `
    <button type="button" class="control-row control-action" data-control-agent="${escapeHtml(agent.agent_id)}">
      <span>${escapeHtml(agent.display_name || agent.agent_id)}</span>
      <strong>${escapeHtml(agent.event_count || 0)} event(s)</strong>
      <small>${escapeHtml((agent.tools_used || []).slice(0, 3).join(", ") || "no tools observed")}</small>
    </button>
  `).join("");
}

function renderAccessReview(review) {
  if (!review) return `<p class="metric-detail">Select an agent to review access.</p>`;
  const warnings = renderWarnings(review.warnings || []);
  const observed = (review.observed_access || []).slice(0, 12).map(renderAccessItem).join("");
  const configured = (review.configured_access || []).slice(0, 8).map(renderAccessItem).join("");
  const unused = (review.recommended_revocations || []).slice(0, 6).map(renderAccessItem).join("");
  return `
    ${warnings}
    <div class="control-row"><span>Agent</span><strong>${escapeHtml(review.agent_id)}</strong></div>
    <div class="control-row"><span>Observed</span>${observed || "<small>No observed access.</small>"}</div>
    <div class="control-row"><span>Configured</span>${configured || "<small>No configured access found.</small>"}</div>
    <div class="control-row"><span>Recommended revocations</span>${unused || "<small>No unused configured access detected.</small>"}</div>
  `;
}

function renderTimeline(data) {
  const warnings = renderWarnings((data.warnings || []).slice(0, 4));
  const rows = (data.timeline || []).map((item) => `
    <button type="button" class="control-row control-action" data-control-event="${escapeHtml(item.event_id)}">
      <span>${escapeHtml(item.kind)}</span>
      <strong>${escapeHtml(item.actor_id || item.source_id || "unknown")}</strong>
      <code>${escapeHtml(item.event_id)}</code>
    </button>
  `).join("");
  return warnings + (rows || `<p class="metric-detail">No provenance events yet.</p>`);
}

function renderBlastRadius(report) {
  if (!report) return `<p class="metric-detail">Select an event to inspect blast radius.</p>`;
  const warnings = renderWarnings(report.unknowns || []);
  return `
    ${warnings}
    <div class="control-row"><span>Anchor</span><code>${escapeHtml(report.anchor_event_id)}</code></div>
    <div class="control-row"><span>Reached evidence</span><strong>${escapeHtml((report.observed || []).length || (report.reached_nodes || []).length)}</strong></div>
    <div class="control-row"><span>Read objects</span><small>${escapeHtml((report.read_objects || []).join(", ") || "none")}</small></div>
    <div class="control-row"><span>Written objects</span><small>${escapeHtml((report.written_objects || []).join(", ") || "none")}</small></div>
  `;
}

function renderPolicyReplay() {
  return `
    <textarea id="control-policy-input" class="control-policy-input" rows="5">{"name":"control-ui-preview","deny_event_kinds":[],"hold_tools":[]}</textarea>
    <button id="control-policy-replay-button" type="button" class="ghost-button">Replay Policy</button>
    <div id="control-policy-replay-result" class="control-list"><p class="metric-detail">Replay a policy against the current evidence spine.</p></div>
  `;
}

function renderEvidencePack(pack) {
  if (!pack) return `<p class="metric-detail">No evidence pack yet.</p>`;
  return `
    ${renderWarnings(pack.warnings || [])}
    <div class="control-row"><span>Timeline events</span><strong>${escapeHtml(pack.timeline?.event_count || 0)}</strong></div>
    <div class="control-row"><span>Agents</span><strong>${escapeHtml((pack.agents || []).length)}</strong></div>
    <div class="control-row"><span>Access reviews</span><strong>${escapeHtml((pack.access_reviews || []).length)}</strong></div>
  `;
}

function renderAccessItem(item) {
  return `<small>${escapeHtml(item.kind)}: ${escapeHtml(item.label || item.id)}</small>`;
}

function renderWarnings(warnings) {
  return (warnings || []).slice(0, 5).map((warning) =>
    `<div class="control-warning">${escapeHtml(warning)}</div>`
  ).join("");
}

function bindControlActions() {
  document.querySelectorAll("[data-control-agent]").forEach((button) => {
    button.addEventListener("click", async () => {
      const panel = document.getElementById("control-access-review");
      const agentId = button.getAttribute("data-control-agent");
      if (!panel || !agentId) return;
      const review = await getJson(`/api/workspace/provenance/agents/${encodeURIComponent(agentId)}/access-review`);
      panel.innerHTML = renderAccessReview(review);
    });
  });
  document.querySelectorAll("[data-control-event]").forEach((button) => {
    button.addEventListener("click", async () => {
      const panel = document.getElementById("control-blast-radius");
      const eventId = button.getAttribute("data-control-event");
      if (!panel || !eventId) return;
      const report = await getJson(`/api/workspace/provenance/events/${encodeURIComponent(eventId)}/blast-radius`);
      panel.innerHTML = renderBlastRadius(report);
    });
  });
  const replayButton = document.getElementById("control-policy-replay-button");
  if (replayButton) {
    replayButton.addEventListener("click", async () => {
      const input = document.getElementById("control-policy-input");
      const result = document.getElementById("control-policy-replay-result");
      if (!input || !result) return;
      try {
        const policy = JSON.parse(input.value || "{}");
        const report = await postJson("/api/workspace/provenance/policy-replay", { policy });
        result.innerHTML = `
          ${renderWarnings(report.warnings || [])}
          <div class="control-row"><span>Replay hits</span><strong>${escapeHtml(report.hit_count || 0)}</strong></div>
          ${(report.hits || []).slice(0, 8).map((hit) => `<div class="control-row"><span>${escapeHtml(hit.replay_decision)}</span><code>${escapeHtml(hit.event_id)}</code><small>${escapeHtml(hit.reason)}</small></div>`).join("")}
        `;
      } catch (error) {
        result.innerHTML = `<p class="connect-error">Policy replay failed: ${escapeHtml(error?.message || error)}</p>`;
      }
    });
  }
}

async function postJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

document.addEventListener("DOMContentLoaded", () => {
  const btn = document.getElementById("control-refresh-button");
  if (btn) btn.addEventListener("click", () => void loadControlSurface());
});
