async function loadControlSurface() {
  const status = document.getElementById("control-status");
  const timeline = document.getElementById("control-timeline");
  const graph = document.getElementById("control-graph");
  if (!status || !timeline || !graph) return;
  status.innerHTML = `<p class="metric-detail">Loading Control evidence...</p>`;
  try {
    const data = await getJson("/api/workspace/provenance/control");
    status.innerHTML = `
      <div class="metric-tile"><strong>${escapeHtml(data.event_count || 0)}</strong><span>canonical events</span></div>
      <div class="metric-tile"><strong>${escapeHtml(data.ingest?.batch_count || 0)}</strong><span>ingest batches</span></div>
      <div class="metric-tile"><strong>${escapeHtml(data.graph?.node_count || 0)}</strong><span>activity nodes</span></div>
      <div class="metric-tile"><strong>${escapeHtml(data.graph?.edge_count || 0)}</strong><span>activity edges</span></div>
    `;
    const warnings = (data.warnings || []).slice(0, 4).map((warning) =>
      `<div class="control-warning">${escapeHtml(warning)}</div>`
    ).join("");
    const rows = (data.timeline || []).map((item) => `
      <div class="control-row">
        <span>${escapeHtml(item.kind)}</span>
        <strong>${escapeHtml(item.actor_id || item.source_id || "unknown")}</strong>
        <code>${escapeHtml(item.event_id)}</code>
      </div>
    `).join("");
    timeline.innerHTML = warnings + (rows || `<p class="metric-detail">No provenance events yet.</p>`);
    graph.innerHTML = (data.graph?.nodes || []).slice(0, 30).map((node) => `
      <div class="control-row">
        <span>${escapeHtml(node.kind)}</span>
        <strong>${escapeHtml(node.label || node.id)}</strong>
        <small>${escapeHtml((node.event_ids || []).length)} event(s)</small>
      </div>
    `).join("") || `<p class="metric-detail">No activity graph yet.</p>`;
  } catch (error) {
    status.innerHTML = `<p class="connect-error">Control load failed: ${escapeHtml(error?.message || error)}</p>`;
  }
}

document.addEventListener("DOMContentLoaded", () => {
  const btn = document.getElementById("control-refresh-button");
  if (btn) btn.addEventListener("click", () => void loadControlSurface());
});
