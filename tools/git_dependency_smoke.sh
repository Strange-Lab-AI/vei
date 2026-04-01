#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${1:-$(pwd)}"
PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

TMP_DIR=""
cleanup() {
  if [ -n "${TMP_DIR}" ]; then
    rm -rf "${TMP_DIR}"
  fi
}
trap cleanup EXIT

# In CI we already bootstrap .venv during `make setup`; reuse it for speed/stability.
# Local fallback creates an isolated venv when .venv is not present.
if [ ! -x "${PYTHON_BIN}" ]; then
  TMP_DIR="$(mktemp -d)"
  python3.11 -m venv "${TMP_DIR}/venv"
  PYTHON_BIN="${TMP_DIR}/venv/bin/python"
fi

"${PYTHON_BIN}" -m pip install --upgrade pip setuptools wheel >/dev/null
echo "[git-smoke] validating git install path"
"${PYTHON_BIN}" -m pip install --disable-pip-version-check --quiet --no-deps "git+file://${REPO_ROOT}" >/dev/null
echo "[git-smoke] importing VEI and opening a lightweight session"

"${PYTHON_BIN}" - <<'PY'
from vei.sdk import create_session, get_scenario_manifest

manifest = get_scenario_manifest("multi_channel")
assert manifest.name == "multi_channel"

session = create_session(seed=42042, scenario_name="multi_channel")
obs = session.observe()
assert isinstance(obs.get("action_menu"), list)
assert isinstance(obs.get("summary"), str)

print("git dependency smoke passed")
PY
