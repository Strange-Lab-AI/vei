"""vei.events — canonical event spine for VEI.

CanonicalEvent is the single source of truth.  StateStore, run timelines,
and connector receipts are derived views.  Governance events share the same
spine.  Raw provider payloads in the ingest RawLog are pre-canonical.
"""
