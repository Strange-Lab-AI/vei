from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from pydantic import BaseModel, Field

from vei.world import (
    IdentityApplicationSeed,
    IdentityGroupSeed,
    IdentityUserSeed,
)

UserStatus = str


class IdentityUser(BaseModel):
    """Typed identity user similar to Okta's user profile."""

    user_id: str
    email: str
    login: str
    first_name: str
    last_name: str
    display_name: Optional[str] = None
    status: UserStatus = "ACTIVE"
    department: Optional[str] = None
    title: Optional[str] = None
    manager: Optional[str] = None
    groups: List[str] = Field(default_factory=list)
    applications: List[str] = Field(default_factory=list)
    factors: List[str] = Field(default_factory=list)
    last_login_ms: Optional[int] = None

    def summary(self) -> Dict[str, object]:
        return {
            "id": self.user_id,
            "email": self.email,
            "display_name": self.display_name or f"{self.first_name} {self.last_name}",
            "status": self.status,
        }

    def detail(self) -> Dict[str, object]:
        return self.model_dump()


class IdentityGroup(BaseModel):
    group_id: str
    name: str
    description: Optional[str] = None
    members: List[str] = Field(default_factory=list)

    def summary(self) -> Dict[str, object]:
        return {
            "id": self.group_id,
            "name": self.name,
            "member_count": len(self.members),
        }


class IdentityApplication(BaseModel):
    app_id: str
    label: str
    status: str = "ACTIVE"
    description: Optional[str] = None
    sign_on_mode: str = "SAML_2_0"
    assignments: List[str] = Field(default_factory=list)

    def summary(self) -> Dict[str, object]:
        return {
            "id": self.app_id,
            "label": self.label,
            "status": self.status,
            "sign_on_mode": self.sign_on_mode,
            "assignments": len(self.assignments),
        }


def users_from_seeds(seeds: Iterable[IdentityUserSeed]) -> Dict[str, IdentityUser]:
    users: Dict[str, IdentityUser] = {}
    for seed in seeds:
        user = IdentityUser(
            user_id=seed.user_id,
            email=seed.email,
            login=seed.login or seed.email,
            first_name=seed.first_name,
            last_name=seed.last_name,
            display_name=seed.display_name,
            status=seed.status or "ACTIVE",
            department=seed.department,
            title=seed.title,
            manager=seed.manager,
            groups=list(seed.groups or []),
            applications=list(seed.applications or []),
            factors=list(seed.factors or []),
            last_login_ms=seed.last_login_ms,
        )
        users[user.user_id] = user
    return users


def groups_from_seeds(seeds: Iterable[IdentityGroupSeed]) -> Dict[str, IdentityGroup]:
    groups: Dict[str, IdentityGroup] = {}
    for seed in seeds:
        group = IdentityGroup(
            group_id=seed.group_id,
            name=seed.name,
            description=seed.description,
            members=list(seed.members or []),
        )
        groups[group.group_id] = group
    return groups


def apps_from_seeds(
    seeds: Iterable[IdentityApplicationSeed],
) -> Dict[str, IdentityApplication]:
    apps: Dict[str, IdentityApplication] = {}
    for seed in seeds:
        app = IdentityApplication(
            app_id=seed.app_id,
            label=seed.label,
            status=seed.status or "ACTIVE",
            description=seed.description,
            sign_on_mode=seed.sign_on_mode or "SAML_2_0",
            assignments=list(seed.assignments or []),
        )
        apps[app.app_id] = app
    return apps
