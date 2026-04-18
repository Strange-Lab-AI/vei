#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${1:-$(pwd)}"
REPO_PYTHON="${REPO_ROOT}/.venv/bin/python"
BASE_PYTHON="${PYTHON:-python3.11}"
PYTHON_BIN=""
CLONE_ROOT=""

TMP_DIR=""
cleanup() {
  if [ -n "${TMP_DIR}" ]; then
    rm -rf "${TMP_DIR}"
  fi
}
trap cleanup EXIT

run_with_timeout() {
  local seconds="$1"
  shift

  if command -v timeout >/dev/null 2>&1; then
    timeout "${seconds}" "$@"
    return
  fi

  "$@"
}

if ! command -v "${BASE_PYTHON}" >/dev/null 2>&1; then
  BASE_PYTHON="python3"
fi

TMP_DIR="$(mktemp -d)"
CLONE_ROOT="${TMP_DIR}/clone"

echo "[git-smoke] cloning repository into isolated checkout"
# The smoke test only needs an importable checkout, so keep LFS files as pointers.
GIT_LFS_SKIP_SMUDGE=1 \
run_with_timeout 60 git clone --quiet --depth 1 "file://${REPO_ROOT}" "${CLONE_ROOT}" || {
  status=$?
  if [ "${status}" -eq 124 ]; then
    echo "[git-smoke] timed out while cloning repository" >&2
  fi
  exit "${status}"
}

if [ -x "${REPO_PYTHON}" ]; then
  echo "[git-smoke] reusing setup environment for dependency layer"
  PYTHON_BIN="${REPO_PYTHON}"
else
  echo "[git-smoke] creating isolated environment"
  "${BASE_PYTHON}" -m venv "${TMP_DIR}/venv"
  PYTHON_BIN="${TMP_DIR}/venv/bin/python"

  "${PYTHON_BIN}" -m pip install --upgrade pip setuptools wheel >/dev/null
  echo "[git-smoke] installing cloned checkout into isolated environment"
  run_with_timeout 600 \
    "${PYTHON_BIN}" -m pip install --disable-pip-version-check --quiet "${CLONE_ROOT}" \
    >/dev/null || {
      status=$?
      if [ "${status}" -eq 124 ]; then
        echo "[git-smoke] timed out while installing cloned checkout" >&2
      fi
      exit "${status}"
    }
fi

echo "[git-smoke] importing VEI from cloned checkout"

cat <<'PY' > "${CLONE_ROOT}/smoke_check.py"
from vei.sdk import create_session, get_scenario_manifest

manifest = get_scenario_manifest("multi_channel")
assert manifest.name == "multi_channel"

session = create_session(seed=42042, scenario_name="multi_channel")
obs = session.observe()
assert isinstance(obs.get("action_menu"), list)
assert isinstance(obs.get("summary"), str)

print("git dependency smoke passed")
PY

(cd "${CLONE_ROOT}" && run_with_timeout 120 "${PYTHON_BIN}" "./smoke_check.py") || {
  status=$?
  if [ "${status}" -eq 124 ]; then
    echo "[git-smoke] timed out while importing VEI in smoke session" >&2
  fi
  exit "${status}"
}
