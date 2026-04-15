from __future__ import annotations

import json
import logging

from pydantic import ValidationError

from vei.context.api import ContextSnapshot

from .corpus import _load_history_snapshot
from .models import WhatIfWorld

logger = logging.getLogger(__name__)


def source_snapshot_for_world(world: WhatIfWorld) -> ContextSnapshot | None:
    if world.source not in {"mail_archive", "company_history"}:
        return None
    try:
        return _load_history_snapshot(world.source_dir)
    except (OSError, json.JSONDecodeError, ValueError, ValidationError) as exc:
        logger.warning(
            "whatif source snapshot load failed for %s (%s)",
            world.source,
            type(exc).__name__,
            extra={
                "source": "episode",
                "provider": world.source,
                "file_path": str(world.source_dir),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return None


__all__ = ["source_snapshot_for_world"]
