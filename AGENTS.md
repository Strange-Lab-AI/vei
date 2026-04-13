AGENTS.md

Drop‑In Rules for Autonomous Coding Agents
> Constitution + lean appendices. Modes: set `AGENT_MODE=baseline` for speed or `AGENT_MODE=production` for rigor. Agents must obey the selected mode.

---

## TL;DR (operator card)
1) Set `AGENT_MODE`.
2) Run the interface targets (Appendix A): `setup` → `all`.  
3) On failure, apply the **triage in Appendix B** (do not loop blindly).  
4) Open PR with tests (LLM live if applicable).

---

## 0) Core Principles
- Ship minimal, correct, succinct code.
- All changes via PR. No direct pushes to `main` unless explicitly requested.
- Handle failures using Appendix B (retries, cost ceilings, infrastructure vs. code).

- All the code that’s part of our architecture goes into the one monolith we have.
- The monolith is composed of separate modules (modules which all run together in the same process).
- Modules cannot call each other, except through specific interfaces (for our Python monolith, we put those in a file called some_module/api.py, so other modules can do from some_module.api import some_function, SomeClass and call things that way.
- All of these interfaces are statically typed. What the functions accept and what they return are statically typed, with types usually being Pydantic classes (no passing a bunch of opaque dicts!).
- The above is enforced via automated checks on CI and in the git pre-commit stage. More on this later in the doc.
---

## 1) Development Cycle
1. Plan and record assumptions.  
2. Implement the smallest viable slice.  
3. Run the interface contract (Appendix A).  
4. Open PR: “simplest that works,” test evidence (incl. LLM live if relevant), risks, rollback.  
5. If blocked, split scope. Log deferrals as issues.
6. When user requests to update github, create a PR, use the CLI to merge it, sync ev'thing, get back to main and then confirm to the user. Do not stop in the middle, do the WHOLE THING.
7. Treat backend-specific caveats as part of Interface Contract notes and LLM live test goldens.

---

## 2) Tiered Quality Gates

| Topic | **Baseline** (default) | **Production** (real users + longevity) |
|---|---|---|
| Format | Black (Py); Prettier (JS/TS) | Same |
| Lint | Ruff defaults (Py); ESLint v9 flat (JS/TS) | Expanded rules; exceptions documented |
| Types | mypy (Py) / `tsc` (TS) not strict | `mypy --strict`; TS `strict`; no lingering ignores |
| Coverage | Global ≥80%; PRs don’t reduce coverage | Global ≥90%; changed lines ≥90%; mutation tests on critical modules (scheduled) |
| LLM live tests | **Mandatory if any LLM logic exists**; golden scenarios in CI (staging keys) | Add faithfulness/relevance evals, OWASP LLM Top‑10 probes, and SLO gates |
| SAST | Bandit (advisory); minimal Semgrep (advisory) | Bandit + Semgrep blocking on high severity |
| Supply chain | `pip‑audit` report (non‑blocking) | `pip‑audit` blocking; SBOM artifact |
| Releases | Conventional Commits; manual version bump | Semantic release + changelog from commits |

- Style: PEP 8, 4-space indents, type hints where practical.
- Names: modules/functions `snake_case`; classes `CapWords`.
- Lint: if present, run `flake8`/`pylint`; format with Black.

---

## 3) Environments
- **Python:** Always `venv`. If missing, create one, install `requirements*.txt`, and log the action. Never rely on global Python.  
- **Node/TS (if present):** `npm ci` or `pnpm i --frozen-lockfile`.  
- **OS:** Linux runner/container by default. Log any OS‑specific choice.

---

## 4) Tooling (names only)
- **Format/Lint:** Black, Ruff (Py). ESLint v9 + Prettier (JS/TS).  
- **Types:** mypy (Py), `tsc` (TS).  
- **Tests:** pytest + pytest‑cov. Property‑based tests where logic is stateful, numeric, or parser‑like.  
- **Secrets:** detect‑secrets (pre‑commit).  
- **Security:** Bandit, Semgrep.  
- **Deps audit:** pip‑audit.  
- **Repo ops:** platform CLI (`gh` for GitHub, `glab` for GitLab) or direct API.  
- **Commit format enforcement:** commitlint or equivalent hook.

> The **Interface Contract** standardizes commands. See **Appendix A**.

---

## 5) LLM‑Specific Rules (if any LLM use exists)
- **Live tests are non‑negotiable.** Treat as integration tests. Run in CI with **staging credentials**.  
- **Output shape:** choose **JSON vs free‑text** for the product surface. If structured, define JSON Schema or Pydantic and validate. If free‑text, assert minimal invariants to reduce brittleness.  
- **Safety:** add prompt‑injection and insecure‑output probes. Production aligns with **OWASP LLM Top‑10**.  
- **Faithfulness:** for RAG/tool‑use, add citation checks and hallucination guards; gate deploys on eval SLOs in production.  
- **Providers (illustrative; verify availability at run time):** OpenAI **current flagship** (e.g., GPT‑5 when available), Anthropic **Claude Sonnet 4.5 / latest**, Google **Gemini 2.5 Pro / latest**, xAI **Grok‑4 / latest** (e.g., via OpenRouter). Optional glue: Simon Willison’s **`llm`** library.  
- **Determinism:** lock provider+model+version for goldens; swapping models requires re‑baselining (Appendix B).

---

## 6) Local vs CI
- **Local:** fast gates only (format, lint, types, quick secrets/security).  
- **CI:** lint, types, tests with coverage, **LLM live tests**, SAST, dep audit (+ SBOM in production). Annotate PRs.

---

## 7) Git and Release Hygiene
- Branches: `feat/*`, `fix/*`, `chore/*` off `main`.  
- Commits: **Conventional Commits**; use `BREAKING CHANGE:` footer when needed; enforce via commitlint/hook.  
- Versioning: **SemVer**. Production automates versioning and changelogs from commits.
- Messages: short, imperative. 
- PRs: include summary, rationale, exact commands to reproduce if useful.
- Link related issues. Keep diffs focused. Update README or docstrings when behaviour changes.

---

## 8) Observability and Runtime Safety
- Structured logs (JSON Lines). Include timestamp, level, logger, message, and context. No f‑strings in log calls; pass fields.  
- Feature flags for risky changes; default off.  
- No PII in logs. Redact model I/O when user data may appear.

---

## 9) PR Checklist
- [ ] Format/lint clean.  
- [ ] Types pass at tier strictness.  
- [ ] Tests added/updated; coverage meets thresholds.  
- [ ] **LLM live tests present and green** (or N/A with reason).  
- [ ] Secrets/SAST clean; dep audit reviewed (blocking in production).  
- [ ] Conventional Commits enforced.  
- [ ] Docs updated if behaviour or API changed.  
- [ ] If rollback plan claimed, **include a `@pytest.mark.rollback` test**.

---

## 10) What’s intentionally excluded
- No giant config dumps. Agents synthesise only what’s necessary and prefer org templates when available.  
- No repository boilerplate here; cold‑start rules are in **Appendix F**.

---

## 11) Minimal human inputs
- Set `AGENT_MODE`.  
- Provide staging keys and allowed model providers.  
- Indicate if JS/TS is in scope; default to Python‑only.

---

# Appendices

## Appendix A — Interface Contract (required)
Expose a standard command surface (Makefile/justfile or package.json scripts). Targets are **mutually independent**; `all` orchestrates and **halts on first failure**. Do not rerun passing subsystems. Cache artifacts where possible.

Required targets:
- `setup` / `bootstrap`: create envs; install app deps and dev tooling; install/refresh hooks (pre‑commit, commitlint).  
- `check`: format check, lint, types (mode‑appropriate), quick Bandit, detect‑secrets on staged changes.  
- `test`: unit/integration tests; enforce coverage thresholds per mode; no live network unless explicitly marked.  
- `llm-live` (if LLM code exists): run golden scenarios against staging keys; assert schema/invariants; output pass/fail counts, **cost**, and **p95 latency**.  
- `deps-audit`: dependency audit; advisory in Baseline, blocking in Production; emit SBOM in Production.  
- `all`: `check` → `test` (+ `llm-live` if applicable) → (`deps-audit` in Production). Stop at first failing step.  
- `release` (Production): semantic release + changelog + tag.

**Standard exit codes (used by CI):**  
`0` pass; `1` test or gate failure; `2` **cost ceiling exceeded**; `3` **infrastructure failure** (provider outage/rate‑limit exhaustion); `4` **threshold/config missing**.

Agents MUST call these targets, not raw tools.

---

## Appendix B — LLM evaluations: determinism, retries, and cost

To use the latest APIs when coding LLM based applications, you can look up https://github.com/strangeloopcanon/llm-api-hub to figure out where to find the latest documentation for each LLM, along with your other searches. Note that Responses API does not take temperature as a parameter.

**Determinism**
- Lock provider+model+version for goldens; treat swaps as breaking and re‑baseline.  
- Freeze sampling for goldens (`temperature=0`, `top‑p=1`). Record model ID and provider version.

**Retries and flakiness**
- Classify failures:  
  *Transient* (HTTP 5xx, 429 rate limits, timeouts) → retry with jittered backoff, up to **3 attempts**.  
  *Deterministic/code* (schema mismatch, safety violation, eval rubric fail) → fail fast; no retries.  
  If 429 persists across 3 attempts or appears with high concurrency, treat as code/config bug and fix call pacing.
- **Retry budget:** max **9 total LLM calls per job** across retries. If exhausted only by transients, exit **code 3** and post “infrastructure failure”.

**Cost governance**
- CI cost ceilings per run: Baseline ≤ **$3**; Production ≤ **$10**.  
- Instrument `llm-live` to track cumulative cost (provider API or token‑based estimate). If ceiling is hit mid‑run, exit **code 2** and log *cost budget exceeded*. CI treats code 2 as **advisory/non‑blocking in Baseline**, **blocking in Production**.

**Eval size**
- Baseline: goldens.  
- Production: spanning success, safety, edge cases; refresh periodically.

**Acceptance**
- Single‑provider path: 100% of goldens pass.  
- Multi‑provider path: maintain **separate** golden sets per provider; each must meet its pass target (≥90% if non‑deterministic evaluation). Model swaps require re‑baselining.

---

## Appendix C — Security, data classes, and supply chain
**Data classes**
- **Secrets:** credentials, tokens, keys. It will and should ALWAYS be in .env file locally, that is the source of truth. Use it, add to it, edit it, as needed. Never commit secrets. Use `.env` locally; keep out of VCS.
- **PII:** identifiers (names, emails, phones, addresses, IDs).  
- **Sensitive context:** business data not public.

**Enforcement**
- Pre‑commit: detect‑secrets baseline; block on new secrets.  
- Logging: redact PII and secrets; include a test/fixture asserting no emails/SSNs/API keys appear in captured logs.  
- SAST: Bandit always; Semgrep advisory → blocking in Production.  
- Deps: `pip‑audit` advisory → blocking in Production; SBOM artifact (Production).  
- Least privilege for tokens and CI; rotate on leaks.

- Large model downloads happen on first run. Document cache locations and quotas.
- Default backend should include MLX via mlx-genkit and PyTorch, to test on this computer. Guard changes behind flags to avoid breaking.

---

## Appendix D — Determinism & flakiness (non‑LLM)
- Pin dependency versions; record toolchain versions.  
- Fixed seeds and timezone; stub clocks and network.  
- No network in unit tests; integration tests use local fakes unless explicitly marked.  
- Use ephemeral resources; clean up on failure.  
- Do not re‑execute passing gates within the same job.

---

## Appendix E — Escalation triggers to `production`
Switch to `AGENT_MODE=production` when any apply:
- PII or regulated data handled.  
- External users > **50** or paying users > **1**.  
- Declared SLOs for uptime or correctness exist.  
- Monthly LLM or infra spend > **$100**.  
- Privileged third‑party integration (write access to user data) is enabled.

**Ceremony:** record the decision (date, triggering condition(s), owner name/email) and open a tracking issue tagged `AGENT_MODE=production`.

---

## Appendix F — Cold start (empty repo rules)
- **Interface contract** (Appendix A) via a **Makefile** (preferred for Python). `justfile` is acceptable. For Node‑only repos, package.json scripts are acceptable.  
- **Python:** `pyproject.toml` with Black, Ruff, mypy, pytest, coverage; `requirements*.txt`; `.gitignore`; pre‑commit + commitlint enabled.  
- **LLM projects:** `tests_llm_live/` with goldens and schema validation.  
- **CI:** a single workflow that runs `setup` → `all` (and `release` on tags in Production).  

---

## Appendix G — Repo config (`.agents.yml`)
Keep thresholds and knobs in a single YAML at repo root:

```yaml
mode: baseline | production
coverage:
  global: 0.80  # 0.90 in production
  changed_lines: 0.90  # production only
llm:
  cost_ceiling_usd: 3   # 10 in production
  latency_p95_ms: 3000
  provider: openai|anthropic|google|xai|openrouter
  model: "<provider model id>"
  temperature: 0
  top_p: 1
  retry_attempts: 3
  max_calls_per_job: 9
security:
  sast_block_high: false  # true in production
deps:
  audit_block: false      # true in production
mutation_tests:
  schedule: weekly        # production only
````

Agents must read this file to configure gates. Missing keys default to the values in Section 2 and Appendix B.

---

## Appendix H — Repo quickstart and conventions

### Project Structure & Modules
- `vei/`: core package (router, SSE server, CLI). Key modules: `vei/router/*`, `vei/cli/*`, `vei/world/*`.
- `tests/`: pytest suite (stdio-first transport tests, scoring, scenarios).
- `examples/`: minimal clients and router loops.
- `_vei_out/` and `.artifacts/`: run artifacts, traces, logs (gitignored).
- `pyproject.toml`: package, extras, and console scripts.

### How the Simulation Works
The world kernel (`vei/world/session.py` + `vei/router/core.py`) is fully deterministic. No LLM calls happen inside the simulation — all tool responses are computed from rules, templates, and a seeded RNG:
- **Mail**: vendor replies are picked from pre-written template lists using `bus.rng.randint()`.
- **Slack**: budget approvals are checked via regex; derailment events fire based on `rng.next_float() < derail_prob`.
- **CRM, Tickets, Docs**: pure CRUD with validation and state machines.
- **Seed data**: vertical packs (`vei/verticals/packs_*.py`) are static data structures — pre-written channels, threads, tickets, org charts.
- **Actors**: the world session can attach an optional actor-response backend for NPC events. The default behavior stays deterministic and template-driven unless a separate backend is attached.

Same seed = same world. The only variable in an eval is the agent being tested.

### Eval System and Runner Types
The eval system (`vei eval`) runs agents through scenarios and scores them. Four runner types provide a performance ladder:
- **scripted** — hardcoded behavior tree (`vei/behavior/policy.py`). Fixed sequence of tool calls. Deterministic baseline floor.
- **workflow** — declarative step graph with assertions (`vei/benchmark/workflows.py`). Each step names a tool call and post-conditions. The reference solution.
- **bc** (behavior cloning) — learned policy trained from demonstration data (`vei/rl/policy_bc.py`). Selects tools by frequency statistics. Deterministic but data-driven.
- **llm** — real LLM agent via MCP stdio (`vei/cli/vei_llm_test.py`). Subprocess connects to the world, fetches tools, and runs an observe→reason→act loop. Non-deterministic, requires API keys.

Key eval commands:
- `vei eval benchmark --runner workflow --family security_containment` — run a benchmark family
- `vei eval demo --family security_containment` — side-by-side baseline vs comparison runner
- `vei eval suite` — run all families with workflow baseline
- `vei eval showcase` — curated complex multi-system examples
- `vei eval-frontier run --model gpt-5 --scenario-set reasoning` — hard frontier scenarios with multi-dimensional scoring

Scoring is multi-layered: raw task success → policy compliance → domain-specific dimensions (evidence preservation, blast radius, comms correctness, etc.) → composite score with 0.7 threshold for frontier scenarios. Optional LLM-as-judge for communication quality.

### Fidelity and Calibration
- **Fidelity checks** (`vei inspect fidelity`): validate that each twin adapter behaves like a real service — writes return success and show up in state, invalid operations fail explicitly, history is preserved.
- **Scenario variants**: the ablation mechanism. Each vertical ships variants that inject faults (vendor no-show, deadline compression, capacity fragmentation) onto the same base world. Running the same agent across variants tests sensitivity.
- **Quality filter** (`vei/quality/`): scores generated workflows on realism and structural novelty before including them in benchmarks.
- **Contracts**: separate oracle state from agent-visible observation, so scoring measures what the agent actually saw.

### Build, Test, and Dev Commands
- Install (with extras): `pip install -e ".[llm,browser,sse]"`
- Repo validation: `make check && make test`
- Run tests: `pytest -q`
- Transport smoke (no API key): `vei smoke run --transport stdio --timeout-s 30`
- LLM evaluation: `vei llm-test run --provider openai --model gpt-5 --artifacts ./_vei_out/llm_eval`
- World inspection: `vei world list --state-dir ./_vei_out/state`
- Start SSE server (optional): `VEI_SEED=42042 python -m vei.router.sse`
- Dataset/rollout pipeline:
  - `vei pack slack|mail|tickets|docs` to turn exports into canonical datasets.
  - `vei rollout procurement --episodes 5 --seed 42042 --output ./_vei_out/rollout.json` for scripted traces.
  - `vei train bc --dataset ./_vei_out/rollout.json --output ./_vei_out/bc_policy.json` to learn a BC policy.
  - `vei eval benchmark --runner scripted --scenario multi_channel --artifacts-root ./_vei_out/benchmark` or `vei eval benchmark --runner bc --bc-model ... --scenario multi_channel --artifacts-root ./_vei_out/benchmark` to produce trace + score artifacts.
  - `vei llm-test run --dataset ./_vei_out/rollout.json --artifacts ./_vei_out/llm_eval` for LLM evals in replay context.
- Rich scenarios: set `VEI_SCENARIO=multi_channel` for docs/tickets/mail coverage during manual or LLM-driven evaluations.


### Security & Config Tips
- Artifacts: `_vei_out/`, `**/artifacts/**`, and `**/trace.jsonl` are gitignored—do not add them to commits.
- Local MCP client config: `mcp.json` defines stdio transport (`python -m vei.router`).

### Architecture Overview
- Router exposes MCP tools: `slack.*`, `mail.*`, `browser.*`, `vei.*`.
- Two transports: stdio (default for dev/CI) and SSE (`vei.router.sse`). Keep new tools deterministic and replay‑friendly.
