SHELL := /bin/bash
PYTHON ?= python3.11
VENV ?= .venv
MODE ?= $(or $(AGENT_MODE),baseline)
AGENTS_FILE := .agents.yml
SETUP_STAMP := $(VENV)/.setup-complete
SETUP_FULL_STAMP := $(VENV)/.setup-full-complete
VENV_BIN := $(VENV)/bin
SETUP_EXTRAS := dev,sse,ui
SETUP_FULL_EXTRAS := dev,llm,sse,ui,test,rl,browser
COVERAGE_FAIL_UNDER ?= $(or $(shell awk 'BEGIN { section = 0 } $$1 == "coverage:" { section = 1; next } section && $$1 == "global:" { print int($$2 * 100); exit }' $(AGENTS_FILE) 2>/dev/null),80)
PIPAPI_PYTHON := $(abspath $(VENV_BIN)/python)

.PHONY: setup bootstrap setup-full check check-full test test-full dynamics-eval llm-live deps-audit enron-example enron-screens all clean clean-workspace clean-workspace-dry-run

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)

$(SETUP_STAMP): $(VENV)/bin/activate pyproject.toml
	. $(VENV)/bin/activate && \
		pip install --upgrade pip "setuptools<82" wheel && \
		pip install -e ".[$(SETUP_EXTRAS)]"
	@if [ -f .pre-commit-config.yaml ]; then \
		$(VENV_BIN)/pre-commit install --install-hooks || \
			echo "Skipping pre-commit install; hooks are managed elsewhere."; \
	fi
	@touch $(SETUP_STAMP)

setup bootstrap: $(SETUP_STAMP)
	@echo "Virtual environment ready at $(VENV)"

$(SETUP_FULL_STAMP): $(SETUP_STAMP) pyproject.toml
	. $(VENV)/bin/activate && \
		pip install -e ".[$(SETUP_FULL_EXTRAS)]"
	@touch $(SETUP_FULL_STAMP)

setup-full: $(SETUP_FULL_STAMP)
	@echo "Full development environment ready at $(VENV)"

check: $(SETUP_FULL_STAMP)
	$(VENV_BIN)/python -m black --check vei tests
	$(VENV_BIN)/python -m ruff check vei tests
	$(VENV_BIN)/python scripts/run_mypy_targets.py
	@echo "--- import boundary check ---"
	$(VENV_BIN)/python scripts/check_import_boundaries.py --strict --max-violations 0
	$(VENV_BIN)/python scripts/run_local_security_checks.py

check-full: check
	$(VENV_BIN)/python -m bandit -q -r vei -ll
	@if [ "$(MODE)" = "production" ]; then \
		$(VENV_BIN)/semgrep --config p/python --config p/security-audit --config .semgrep.yml --error vei scripts; \
	else \
		$(VENV_BIN)/semgrep --config p/python --config p/security-audit --config .semgrep.yml --error vei scripts || true; \
	fi
	@mkdir -p .artifacts
	@DETECT_SECRETS_FILES="$$(git ls-files --cached --others --exclude-standard | grep -v '^\.secrets\.baseline$$' || true)"; \
	$(VENV_BIN)/detect-secrets scan $$DETECT_SECRETS_FILES > .artifacts/detect-secrets.json
	@if [ -f .secrets.baseline ]; then \
		DETECT_SECRETS_FILES="$$(git ls-files --cached --others --exclude-standard | grep -v '^\.secrets\.baseline$$' || true)"; \
		$(VENV_BIN)/detect-secrets-hook --baseline .secrets.baseline $$DETECT_SECRETS_FILES; \
	else \
		echo "No .secrets.baseline found; detect-secrets check is advisory-only."; \
	fi

test: $(SETUP_FULL_STAMP)
	VEI_RUN_LLM_SMOKE=0 $(VENV_BIN)/python -m pytest -q -m "not slow" -n auto --maxfail=1

test-full: $(SETUP_FULL_STAMP)
	VEI_RUN_LLM_SMOKE=0 $(VENV_BIN)/python -m pytest --cov=vei --cov-report=term-missing --cov-fail-under=$(COVERAGE_FAIL_UNDER) -n auto

llm-live: $(SETUP_FULL_STAMP)
	@if [ -f .env ]; then \
		set -a; . ./.env; set +a; \
	fi; \
	if [ -n "$$VEI_LLM_LIVE_BYPASS" ]; then \
		echo "VEI_LLM_LIVE_BYPASS=1 set; skipping llm-live checks."; \
	else \
		DEFAULTS="$$( $(VENV_BIN)/python scripts/resolve_llm_live_defaults.py )"; \
		eval "$$DEFAULTS"; \
		if [ -z "$$LLM_KEY_ENV" ]; then \
			echo "Unsupported llm-live provider '$$LLM_PROVIDER'."; \
			exit 4; \
		fi; \
		if [ -z "$${!LLM_KEY_ENV}" ]; then \
			echo "$$LLM_KEY_ENV not set; cannot run llm-live target. Set the key or export VEI_LLM_LIVE_BYPASS=1 to skip in CI."; \
			exit 4; \
		fi; \
		ART="$${VEI_LLM_ARTIFACTS_DIR:-_vei_out/llm_live/latest}"; \
		mkdir -p "$$ART"; \
		rm -f "$$ART/trace.jsonl" "$$ART/score.json" "$$ART/summary.json" "$$ART/transcript.json" "$$ART/llm_metrics.json" "$$ART/llm_transcript.jsonl" "$$ART/connector_receipts.jsonl"; \
		VEI_SCENARIO=$${VEI_SCENARIO:-multi_channel} \
				$(VENV_BIN)/vei llm-test run --provider "$$LLM_PROVIDER" --model "$$LLM_MODEL" --max-steps $${VEI_LLM_MAX_STEPS:-18} --step-timeout-s $${VEI_LLM_STEP_TIMEOUT_S:-180} --episode-timeout-s $${VEI_LLM_EPISODE_TIMEOUT_S:-900} --score-success-mode $${VEI_LLM_SUCCESS_MODE:-full} --require-success --no-print-transcript --artifacts "$$ART" --task "$${VEI_LLM_TASK:-Run full procurement workflow: cite source, post Slack approval with budget amount, email vendor and parse price+ETA reply, log quote in Docs, update ticket, and log CRM activity.}"; \
		status=$$?; \
		if [ "$$status" -ne 0 ]; then \
			exit "$$status"; \
		fi; \
		$(VENV_BIN)/python scripts/validate_llm_live_metrics.py --artifacts "$$ART"; \
	fi

deps-audit: $(SETUP_FULL_STAMP)
	@if [ "$(MODE)" = "production" ]; then \
		VIRTUAL_ENV=$(abspath $(VENV)) PIPAPI_PYTHON_LOCATION=$(PIPAPI_PYTHON) $(VENV_BIN)/pip-audit --skip-editable; \
	else \
		VIRTUAL_ENV=$(abspath $(VENV)) PIPAPI_PYTHON_LOCATION=$(PIPAPI_PYTHON) $(VENV_BIN)/pip-audit --skip-editable || true; \
	fi

dynamics-eval: $(SETUP_FULL_STAMP)
	@echo "--- dynamics evaluation ---"
	$(VENV_BIN)/python -m pytest tests/dynamics/ -v --tb=short
	$(VENV_BIN)/python scripts/validate_dynamics_metrics.py --metrics _vei_out/dynamics_eval/metrics.json
	@echo "Dynamics evaluation passed."

enron-example: $(SETUP_FULL_STAMP)
	$(VENV_BIN)/python scripts/build_enron_example_bundles.py
	$(VENV_BIN)/python scripts/validate_enron_example_bundles.py

enron-screens: $(SETUP_FULL_STAMP)
	$(VENV_BIN)/python -m playwright install chromium
	$(VENV_BIN)/python scripts/capture_enron_bundle_screenshots.py

all: check-full test-full dynamics-eval llm-live deps-audit

clean-workspace:
	$(PYTHON) scripts/clean_workspace.py

clean-workspace-dry-run:
	$(PYTHON) scripts/clean_workspace.py --dry-run

clean:
	rm -rf $(VENV) $(SETUP_STAMP) $(SETUP_FULL_STAMP)
