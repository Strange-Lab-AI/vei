function knowledgeAssetTimestamp(asset) {
  const metadata = asset?.metadata;
  if (!metadata || typeof metadata !== "object") return 0;
  return Number(metadata.composed_at_ms || metadata.captured_at_ms || 0) || 0;
}

function onCompareSnapshotPickerChange() {
  const snapASelect = document.getElementById("compare-snapshot-a");
  const snapBSelect = document.getElementById("compare-snapshot-b");
  state.compareSnapshotA = snapASelect?.value ? Number(snapASelect.value) : null;
  state.compareSnapshotB = snapBSelect?.value ? Number(snapBSelect.value) : null;
}

function latestComposedKnowledgeAsset(store) {
  const assets = store?.assets;
  if (!assets || typeof assets !== "object") return null;
  const composed = Object.values(assets)
    .filter((item) => item && typeof item === "object" && item.composition)
    .sort((a, b) => {
      const timeDelta = knowledgeAssetTimestamp(a) - knowledgeAssetTimestamp(b);
      if (timeDelta !== 0) return timeDelta;
      return String(a.asset_id || "").localeCompare(String(b.asset_id || ""));
    });
  return composed.length ? composed[composed.length - 1] : null;
}

function knowledgeValidationSummary(asset) {
  const validation = asset?.composition?.validation || {};
  const checks = [
    ["Citations", validation.citations_present && validation.citations_resolve],
    ["Freshness", validation.sources_within_shelf_life],
    ["Numbers", validation.numbers_reconcile],
    ["Format", validation.format_matches_template],
  ];
  return checks.map(([label, ok]) => `
    <span class="compare-div-item">${escapeHtml(label)}: ${ok ? "ok" : "needs work"}</span>
  `).join("");
}

function renderKnowledgeCompareCard(name, asset) {
  if (!asset) {
    return `
      <div class="story-card">
        <p class="eyebrow">${escapeHtml(name)}</p>
        <h3>No composed artifact</h3>
        <p class="metric-detail">This run did not leave a knowledge artifact in the latest snapshot.</p>
      </div>
    `;
  }
  const citations = asset.composition?.citation_spans || [];
  const sections = asset.composition?.sections || [];
  return `
    <div class="story-card">
      <p class="eyebrow">${escapeHtml(name)}</p>
      <h3>${escapeHtml(asset.title || "Authored artifact")}</h3>
      <p class="metric-detail">${escapeHtml(asset.summary || "")}</p>
      <div class="detail-grid">
        ${detailTile("Kind", escapeHtml(asset.kind || "artifact"))}
        ${detailTile("Sections", String(sections.length))}
        ${detailTile("Citations", String(citations.length))}
        ${detailTile("Claims", String((asset.composition?.claims || []).length))}
      </div>
      <div class="chip-row">${knowledgeValidationSummary(asset)}</div>
      <p class="metric-detail">${escapeHtml((asset.body || "").slice(0, 260))}${(asset.body || "").length > 260 ? "..." : ""}</p>
    </div>
  `;
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
  container.innerHTML = `<p class="metric-detail">Loading snapshot differences...</p>`;
  try {
    const [snapsA, snapsB] = await Promise.all([ensureRunSnapshots(runA), ensureRunSnapshots(runB)]);
    const snapA = state.compareSnapshotA ?? (snapsA.length ? snapsA[snapsA.length - 1].snapshot_id : null);
    const snapB = state.compareSnapshotB ?? (snapsB.length ? snapsB[snapsB.length - 1].snapshot_id : null);
    if (snapA == null || snapB == null) {
      container.innerHTML = `<p class="metric-detail">One or both runs have no snapshots.</p>`;
      return;
    }
    const diff = await getJson(`/api/runs/diff-cross?run_a=${encodeURIComponent(runA.run_id)}&snap_a=${snapA}&run_b=${encodeURIComponent(runB.run_id)}&snap_b=${snapB}`);
    renderCrossRunDiff(container, diff, runA, runB);
  } catch (err) {
    container.innerHTML = `<p class="metric-detail">Diff failed: ${escapeHtml(String(err))}</p>`;
  }
}

function _humanizeKey(key) {
  const parts = key
    .replaceAll("components.", "")
    .replaceAll("audit_state.state.", "")
    .split(".")
    .map((part) => humanize(part));
  const last = parts[parts.length - 1];
  const context = parts.length > 1 ? parts.slice(0, -1).join(" > ") : "";
  const readable = humanize(last);
  return { readable, context };
}

function _groupDiffEntries(entries) {
  const groups = {};
  for (const entry of entries) {
    const prefix = entry.key.split(".").slice(0, 3).join(".");
    if (!groups[prefix]) groups[prefix] = [];
    groups[prefix].push(entry);
  }
  return groups;
}

function _formatDiffValue(value) {
  if (typeof value === "boolean") return value ? "On" : "Off";
  if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(2);
  if (value === null || value === undefined || value === "") return "\u2014";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function renderCrossRunDiff(container, diff, runA, runB) {
  const changedCount = Object.keys(diff.changed || {}).length;
  const addedCount = Object.keys(diff.added || {}).length;
  const removedCount = Object.keys(diff.removed || {}).length;
  const nameA = displayRunnerTitle(runA.runner || runA.run_id?.split("_").slice(0, 2).join("_"), "Path A");
  const nameB = displayRunnerTitle(runB.runner || runB.run_id?.split("_").slice(0, 2).join("_"), "Path B");
  let html = `<div class="compare-diff-header"><strong>What changed between snapshots</strong> <span class="metric-detail">${escapeHtml(nameA)} vs ${escapeHtml(nameB)}</span></div>`;
  html += `<div class="detail-grid">`;
  html += detailTile("Changed", String(changedCount));
  html += detailTile("Added", String(addedCount));
  html += detailTile("Removed", String(removedCount));
  html += `</div>`;
  if (changedCount + addedCount + removedCount === 0) {
    html += `<p class="metric-detail">World states are identical.</p>`;
  } else {
    const allEntries = [];
    for (const [key, val] of Object.entries(diff.changed || {})) {
      allEntries.push({ key, type: "changed", from: val.from, to: val.to });
    }
    for (const [key, val] of Object.entries(diff.added || {})) {
      allEntries.push({ key, type: "added", value: val });
    }
    for (const [key, val] of Object.entries(diff.removed || {})) {
      allEntries.push({ key, type: "removed", value: val });
    }
    const groups = _groupDiffEntries(allEntries);
    const groupKeys = Object.keys(groups).sort();
    html += `<p class="metric-detail">${changedCount} changed, ${addedCount} added, and ${removedCount} removed across ${groupKeys.length} system${groupKeys.length === 1 ? "" : "s"}.</p>`;
    html += `<div class="diff-groups">`;
    for (const groupKey of groupKeys) {
      const items = groups[groupKey];
      const groupLabel = groupKey
        .replaceAll("components.", "")
        .split(".")
        .map((part) => humanize(part))
        .join(" > ");
      html += `<div class="diff-group">`;
      html += `<div class="diff-group-label">${escapeHtml(groupLabel)}</div>`;
      for (const item of items.slice(0, 40)) {
        const h = _humanizeKey(item.key);
        const cls = `diff-entry diff-${item.type}`;
        if (item.type === "changed") {
          const numDelta = (typeof item.from === "number" && typeof item.to === "number")
            ? (() => { const d = item.to - item.from; return d > 0 ? ` (+${_formatDiffValue(d)})` : d < 0 ? ` (${_formatDiffValue(d)})` : ""; })()
            : "";
          html += `<div class="${cls}"><span class="diff-key" title="${escapeHtml(item.key)}">${escapeHtml(h.readable)}</span><span class="diff-val"><span class="diff-from">${escapeHtml(_formatDiffValue(item.from))}</span> <span class="diff-arrow">&rarr;</span> <span class="diff-to">${escapeHtml(_formatDiffValue(item.to))}</span>${numDelta ? `<span class="diff-delta">${escapeHtml(numDelta)}</span>` : ""}</span></div>`;
        } else {
          const prefix = item.type === "added" ? "+" : "-";
          html += `<div class="${cls}"><span class="diff-key" title="${escapeHtml(item.key)}">${prefix} ${escapeHtml(h.readable)}</span><span class="diff-val">${escapeHtml(_formatDiffValue(item.value))}</span></div>`;
        }
        if (h.context) {
          html += `<div class="diff-context">${escapeHtml(h.context)}</div>`;
        }
      }
      if (items.length > 40) html += `<p class="metric-detail">${items.length - 40} more in this group...</p>`;
      html += `</div>`;
    }
    html += `</div>`;
  }
  container.innerHTML = html;
}

function renderCompareTimelines() {
  const a = state.compareRunA;
  const b = state.compareRunB;
  if (!a || !b) return `<div class="tl-compare-empty">Need at least 2 recorded paths to compare.</div>`;
  const nameA = displayRunnerTitle(a.runner || a.run_id?.split("_").slice(0, 2).join("_"), "Path A");
  const nameB = displayRunnerTitle(b.runner || b.run_id?.split("_").slice(0, 2).join("_"), "Path B");
  const hitsA = timelineSurfaceHits(state.compareTimelineA);
  const hitsB = timelineSurfaceHits(state.compareTimelineB);
  const allSurfaces = [...new Set([...Object.keys(hitsA), ...Object.keys(hitsB)])];
  const stepsA = Object.values(hitsA).reduce((s, v) => s + v, 0);
  const stepsB = Object.values(hitsB).reduce((s, v) => s + v, 0);

  const cA = state.compareContractA;
  const cB = state.compareContractB;
  const mA = state.compareMissionA;
  const mB = state.compareMissionB;
  const knowledgeA = latestComposedKnowledgeAsset(state.compareKnowledgeA);
  const knowledgeB = latestComposedKnowledgeAsset(state.compareKnowledgeB);
  const branchLabels =
    mA?.mission?.branch_labels
    || mB?.mission?.branch_labels
    || state.missionState?.mission?.branch_labels
    || [];

  let html = `<div class="compare-narrative">`;

  if (branchLabels.length >= 2 || (cA && cB)) {
    html += `<div class="compare-narrative-header"><h3>How these paths differ</h3></div>`;

    if (branchLabels.length >= 2) {
      html += `<div class="compare-branches">`;
      html += `<div class="compare-branch compare-branch-a"><span class="compare-branch-dot dot-a"></span><span>${escapeHtml(displayBranchTitle(branchLabels[0]))}</span></div>`;
      html += `<div class="compare-branch compare-branch-b"><span class="compare-branch-dot dot-b"></span><span>${escapeHtml(displayBranchTitle(branchLabels[1]))}</span></div>`;
      html += `</div>`;
    }

    if ((mA && mB) || (cA && cB)) {
      const scorecardA = mA?.scorecard || null;
      const scorecardB = mB?.scorecard || null;
      const objectiveA = displayContractVariantTitle(mA?.objective_variant || "", "");
      const objectiveB = displayContractVariantTitle(mB?.objective_variant || "", "");
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
        : `${delta > 0 ? "+" : ""}${delta} success checks`;
      const scoreLabelA = typeof overallScoreA === "number" ? String(overallScoreA) : assertionsA;
      const scoreLabelB = typeof overallScoreB === "number" ? String(overallScoreB) : assertionsB;
      const scoreCaptionA = typeof overallScoreA === "number" ? "overall score" : "success checks";
      const scoreCaptionB = typeof overallScoreB === "number" ? "overall score" : "success checks";

      html += `<div class="compare-scores">`;
      html += `<div class="compare-score-cell ${okA ? "compare-pass" : "compare-fail"}">
        <span class="compare-score-label">${escapeHtml(nameA)}</span>
        <span class="compare-score-value">${escapeHtml(scoreLabelA)}</span>
        <span class="compare-score-caption">${escapeHtml(scoreCaptionA)}</span>
        ${objectiveA ? `<span class="compare-score-objective">${escapeHtml(objectiveA)}</span>` : ""}
        ${cA ? `<span class="compare-score-assertions">${escapeHtml(assertionsA)} success checks</span>` : ""}
        <span class="compare-score-verdict">${okA ? "On target" : "Needs more work"}</span>
      </div>`;
      html += `<div class="compare-score-delta ${delta > 0 ? "delta-pos" : delta < 0 ? "delta-neg" : ""}">
        ${escapeHtml(deltaLabel)}
      </div>`;
      html += `<div class="compare-score-cell ${okB ? "compare-pass" : "compare-fail"}">
        <span class="compare-score-label">${escapeHtml(nameB)}</span>
        <span class="compare-score-value">${escapeHtml(scoreLabelB)}</span>
        <span class="compare-score-caption">${escapeHtml(scoreCaptionB)}</span>
        ${objectiveB ? `<span class="compare-score-objective">${escapeHtml(objectiveB)}</span>` : ""}
        ${cB ? `<span class="compare-score-assertions">${escapeHtml(assertionsB)} success checks</span>` : ""}
        <span class="compare-score-verdict">${okB ? "On target" : "Needs more work"}</span>
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
        html += `<p class="eyebrow">Where they separate</p>`;
        if (onlyFailedInA.length) {
          html += `<div class="compare-div-group"><span class="compare-div-label">${escapeHtml(nameA)} still needs:</span>`;
          html += onlyFailedInA.map((n) => {
            const issue = issuesA.find((i) => i.predicate_name === n);
            return `<span class="compare-div-item">${escapeHtml(issue?.message || humanize(n))}</span>`;
          }).join("");
          html += `</div>`;
        }
        if (onlyFailedInB.length) {
          html += `<div class="compare-div-group"><span class="compare-div-label">${escapeHtml(nameB)} still needs:</span>`;
          html += onlyFailedInB.map((n) => {
            const issue = issuesB.find((i) => i.predicate_name === n);
            return `<span class="compare-div-item">${escapeHtml(issue?.message || humanize(n))}</span>`;
          }).join("");
          html += `</div>`;
        }
        html += `</div>`;
      }
    }
  }
  if (knowledgeA || knowledgeB) {
    const citationDelta =
      ((knowledgeA?.composition?.citation_spans || []).length)
      - ((knowledgeB?.composition?.citation_spans || []).length);
    html += `<div class="compare-divergence">`;
    html += `<p class="eyebrow">Authored artifact compare</p>`;
    html += `<div class="compare-scores">`;
    html += renderKnowledgeCompareCard(nameA, knowledgeA);
    html += `<div class="compare-score-delta ${citationDelta > 0 ? "delta-pos" : citationDelta < 0 ? "delta-neg" : ""}">`;
    html += `${citationDelta > 0 ? "+" : ""}${citationDelta} citations`;
    html += `</div>`;
    html += renderKnowledgeCompareCard(nameB, knowledgeB);
    html += `</div>`;
    if (knowledgeA && knowledgeB) {
      const sourcesA = new Set((knowledgeA.composition?.citation_spans || []).map((item) => item.asset_id).filter(Boolean));
      const sourcesB = new Set((knowledgeB.composition?.citation_spans || []).map((item) => item.asset_id).filter(Boolean));
      const onlyA = [...sourcesA].filter((item) => !sourcesB.has(item));
      const onlyB = [...sourcesB].filter((item) => !sourcesA.has(item));
      if (onlyA.length || onlyB.length) {
        html += `<div class="compare-div-group"><span class="compare-div-label">${escapeHtml(nameA)} unique sources:</span>${onlyA.map((item) => `<span class="compare-div-item">${escapeHtml(item)}</span>`).join("") || `<span class="compare-div-item">None</span>`}</div>`;
        html += `<div class="compare-div-group"><span class="compare-div-label">${escapeHtml(nameB)} unique sources:</span>${onlyB.map((item) => `<span class="compare-div-item">${escapeHtml(item)}</span>`).join("") || `<span class="compare-div-item">None</span>`}</div>`;
      }
    }
    html += `</div>`;
  }
  html += `</div>`;

  const allRuns = state.runs || [];
  const runOptions = allRuns.map((r) => {
    const label = displayRunnerTitle(r.runner || r.run_id?.split("_").slice(0, 2).join("_"), r.run_id);
    const status = r.status ? ` [${displayStatusTitle(r.status)}]` : "";
    return `<option value="${escapeHtml(r.run_id)}">${escapeHtml(label)}${escapeHtml(status)}</option>`;
  }).join("");
  const snapshotOptionsA = (a.snapshots || []).map((snapshot) =>
    `<option value="${snapshot.snapshot_id}">${snapshot.snapshot_id} · ${escapeHtml(snapshot.label || "snapshot")}</option>`
  ).join("");
  const snapshotOptionsB = (b.snapshots || []).map((snapshot) =>
    `<option value="${snapshot.snapshot_id}">${snapshot.snapshot_id} · ${escapeHtml(snapshot.label || "snapshot")}</option>`
  ).join("");

  html += `<div class="tl-compare-header">`;
  html += `<div class="compare-picker-stack"><select id="compare-picker-a" class="compare-run-picker">${runOptions}</select><select id="compare-snapshot-a" class="compare-run-picker compare-snapshot-picker">${snapshotOptionsA}</select></div>`;
  html += `<span class="tl-compare-vs">vs</span>`;
  html += `<div class="compare-picker-stack"><select id="compare-picker-b" class="compare-run-picker">${runOptions}</select><select id="compare-snapshot-b" class="compare-run-picker compare-snapshot-picker">${snapshotOptionsB}</select></div>`;
  html += `<button id="compare-diff-btn" class="compare-diff-btn">Compare snapshots</button>`;
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
