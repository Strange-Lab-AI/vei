# Contributing

## Local setup

```bash
make setup
make all
```

## Before opening a PR

```bash
make check
make test
make llm-live      # requires OPENAI_API_KEY, or set VEI_LLM_LIVE_BYPASS=1
make deps-audit
```
