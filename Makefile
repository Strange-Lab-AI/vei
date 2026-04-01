SHELL := /bin/bash
PYTHON ?= python3.11
VENV ?= .venv
MODE ?= $(or $(AGENT_MODE),baseline)
AGENTS_FILE := .agents.yml
SETUP_STAMP := $(VENV)/.setup-complete
VENV_BIN := $(VENV)/bin
SETUP_EXTRAS := dev,llm,sse,ui,test,rl
COVERAGE_FAIL_UNDER ?= $(or $(shell awk 'BEGIN { section = 0 } $$1 == "coverage:" { section = 1; next } section && $$1 == "global:" { print int($$2 * 100); exit }' $(AGENTS_FILE) 2>/dev/null),80)
PIPAPI_PYTHON := $(abspath $(VENV_BIN)/python)

.PHONY: setup bootstrap check test llm-live deps-audit all clean clean-workspace

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)

$(SETUP_STAMP): $(VENV)/bin/activate pyproject.toml
	. $(VENV)/bin/activate && \
		pip install --upgrade pip setuptools wheel && \
		pip install -e ".[$(SETUP_EXTRAS)]"
	@if [ -f .pre-commit-config.yaml ]; then \
		$(VENV_BIN)/pre-commit install --install-hooks || \
			echo "Skipping pre-commit install; hooks are managed elsewhere."; \
	fi
	@touch $(SETUP_STAMP)

setup bootstrap: $(SETUP_STAMP)
	@echo "Virtual environment ready at $(VENV)"

check: $(SETUP_STAMP)
	$(VENV_BIN)/python -m black --check vei tests
	$(VENV_BIN)/python -m ruff check vei tests
	$(VENV_BIN)/python scripts/run_mypy_targets.py
	$(VENV_BIN)/python -m bandit -q -r vei -ll
	@mkdir -p .artifacts
	$(VENV_BIN)/detect-secrets scan $$(git ls-files) > .artifacts/detect-secrets.json
	@echo "--- import boundary check ---"
	$(VENV_BIN)/python scripts/check_import_boundaries.py
	@if [ -f .secrets.baseline ]; then \
		$(VENV_BIN)/detect-secrets-hook --baseline .secrets.baseline $$(git ls-files); \
	else \
		echo "No .secrets.baseline found; detect-secrets check is advisory-only."; \
	fi

test: $(SETUP_STAMP)
	VEI_RUN_LLM_SMOKE=0 $(VENV_BIN)/python -m pytest --cov=vei --cov-report=term-missing --cov-fail-under=$(COVERAGE_FAIL_UNDER)

llm-live: $(SETUP_STAMP)
	@if [ -f .env ]; then \
		set -a; . ./.env; set +a; \
	fi; \
	if [ -n "$$VEI_LLM_LIVE_BYPASS" ]; then \
		echo "VEI_LLM_LIVE_BYPASS=1 set; skipping llm-live checks."; \
	elif [ -z "$$OPENAI_API_KEY" ]; then \
		echo "OPENAI_API_KEY not set; cannot run llm-live target. Set the key or export VEI_LLM_LIVE_BYPASS=1 to skip in CI."; \
		exit 4; \
	else \
		ART="$${VEI_LLM_ARTIFACTS_DIR:-_vei_out/llm_live/latest}"; \
		mkdir -p "$$ART"; \
		rm -f "$$ART/trace.jsonl" "$$ART/score.json" "$$ART/transcript.json" "$$ART/llm_transcript.jsonl" "$$ART/connector_receipts.jsonl"; \
		VEI_SCENARIO=$${VEI_SCENARIO:-multi_channel} \
				$(VENV_BIN)/vei llm-test run --provider openai --model $${VEI_LLM_MODEL:-gpt-5} --max-steps $${VEI_LLM_MAX_STEPS:-18} --step-timeout-s $${VEI_LLM_STEP_TIMEOUT_S:-180} --episode-timeout-s $${VEI_LLM_EPISODE_TIMEOUT_S:-900} --score-success-mode $${VEI_LLM_SUCCESS_MODE:-full} --require-success --no-print-transcript --artifacts "$$ART" --task "$${VEI_LLM_TASK:-Run full procurement workflow: cite source, post Slack approval with budget amount, email vendor and parse price+ETA reply, log quote in Docs, update ticket, and log CRM activity.}" && \
				$(VENV_BIN)/python -c 'import json,sys;from pathlib import Path;art=Path(sys.argv[1]);score=json.loads((art/"score.json").read_text(encoding="utf-8"));trace=art/"trace.jsonl";records=[json.loads(line) for line in trace.read_text(encoding="utf-8").splitlines() if line.strip()] if trace.exists() else [];times=[int(r.get("time_ms",0)) for r in records if r.get("type")=="call"];lat=[max(0,b-a) for a,b in zip(times,times[1:])];p95=sorted(lat)[int(0.95*(len(lat)-1))] if lat else 0;passed=1 if bool(score.get("success")) else 0;failed=0 if passed else 1;actions=int(score.get("costs",{}).get("actions",len(times)));print(f"llm-live metrics: pass={passed} fail={failed} cost_usd=unknown p95_latency_ms={p95} actions={actions}")' "$$ART"; \
	fi

deps-audit: $(SETUP_STAMP)
	@if [ "$(MODE)" = "production" ]; then \
		VIRTUAL_ENV=$(abspath $(VENV)) PIPAPI_PYTHON_LOCATION=$(PIPAPI_PYTHON) $(VENV_BIN)/pip-audit --skip-editable; \
	else \
		VIRTUAL_ENV=$(abspath $(VENV)) PIPAPI_PYTHON_LOCATION=$(PIPAPI_PYTHON) $(VENV_BIN)/pip-audit --skip-editable || true; \
	fi

all: check test llm-live deps-audit

clean-workspace:
	rm -rf .artifacts .coverage .coverage.* .mypy_cache .pytest_cache ".pytest_cache 2" .ruff_cache vei.egg-info pyvei.egg-info
	find . -name '.DS_Store' -delete
	@mkdir -p _vei_out/demo _vei_out/llm_live _vei_out/datasets
	find _vei_out -mindepth 1 -maxdepth 1 ! -name demo ! -name llm_live ! -name datasets -exec rm -rf {} +
	find _vei_out/demo -mindepth 1 -maxdepth 1 ! -name security_blueprint_demo -exec rm -rf {} +
	find _vei_out/llm_live -mindepth 1 -maxdepth 1 ! -name latest -exec rm -rf {} +

clean:
	rm -rf $(VENV) $(SETUP_STAMP)
