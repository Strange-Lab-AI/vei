from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class OrientationObject(BaseModel):
    domain: str
    kind: str
    object_id: str
    title: Optional[str] = None
    status: Optional[str] = None
    reason: Optional[str] = None


class OrientationPolicyHint(BaseModel):
    policy_id: str
    title: str
    summary: str


class WorldOrientation(BaseModel):
    scenario_name: str
    scenario_template_name: Optional[str] = None
    organization_name: Optional[str] = None
    organization_domain: Optional[str] = None
    timezone: Optional[str] = None
    builder_mode: Optional[str] = None
    available_domains: List[str] = Field(default_factory=list)
    available_surfaces: List[str] = Field(default_factory=list)
    active_policies: List[OrientationPolicyHint] = Field(default_factory=list)
    key_objects: List[OrientationObject] = Field(default_factory=list)
    suggested_focuses: List[str] = Field(default_factory=list)
    next_questions: List[str] = Field(default_factory=list)
    summary: str


__all__ = [
    "OrientationObject",
    "OrientationPolicyHint",
    "WorldOrientation",
]
