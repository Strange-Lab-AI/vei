"""vei.ingest — four-layer ingest substrate.

Layers: RawLog -> Normalizer -> CaseResolver -> Materializer.
SessionMaterializer provides the lazy-hydration boundary that WorldSession
consumes.

Live default is Postgres.  Offline / replay default is DuckDB + JSONL.
"""
