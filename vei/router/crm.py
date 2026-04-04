from __future__ import annotations

from typing import Any, Dict, List, Optional

from vei.world.api import Scenario
from ._pagination import (
    sortable,
    page_rows,
)
from .errors import MCPError


class CrmSim:
    """Minimal deterministic CRM twin.

    Scope (v0):
    - Contacts: create/get/list
    - Companies: create/get/list
    - Associations: associate contact<->company
    - Deals: create/get/list/update_stage
    - Activities: log note/email (for SLA checks)

    Consent: if a contact has do_not_contact=True, activity logging with kind 'email_outreach'
    returns an error when error_rate triggers or policy is violated.
    """

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200
    _STAGE_BY_KEY = {
        "new": "New",
        "prospecting": "Prospecting",
        "qualification": "Qualification",
        "proposal": "Proposal",
        "negotiation": "Negotiation",
        "closed won": "Closed Won",
        "closed lost": "Closed Lost",
        "closed_won": "Closed Won",
        "closed_lost": "Closed Lost",
    }

    def __init__(self, bus, scenario: Optional[Scenario] = None):  # noqa: ANN001
        import os

        self.bus = bus
        seed = (scenario.crm or {}) if scenario else {}
        self.contacts: Dict[str, Dict[str, Any]] = {
            str(contact["id"]): dict(contact)
            for contact in seed.get("contacts", [])
            if isinstance(contact, dict) and "id" in contact
        }
        self.companies: Dict[str, Dict[str, Any]] = {
            str(company["id"]): dict(company)
            for company in seed.get("companies", [])
            if isinstance(company, dict) and "id" in company
        }
        self.deals: Dict[str, Dict[str, Any]] = {
            str(deal["id"]): dict(deal)
            for deal in seed.get("deals", [])
            if isinstance(deal, dict) and "id" in deal
        }
        self.activities: List[Dict[str, Any]] = [
            dict(activity)
            for activity in seed.get("activities", [])
            if isinstance(activity, dict)
        ]
        self._c_seq = _next_seq(self.contacts, prefix="C-")
        self._co_seq = _next_seq(self.companies, prefix="CO-")
        self._d_seq = _next_seq(self.deals, prefix="D-")
        self._a_seq = max(len(self.activities) + 1, 1)
        try:
            self.error_rate = float(os.environ.get("VEI_CRM_ERROR_RATE", "0"))
        except Exception:
            self.error_rate = 0.0

    # Contacts
    def create_contact(
        self,
        email: str,
        first_name: str | None = None,
        last_name: str | None = None,
        do_not_contact: bool = False,
    ) -> Dict[str, Any]:
        if any(
            str(contact.get("email", "")).lower() == email.lower()
            for contact in self.contacts.values()
        ):
            raise MCPError(
                "conflict.contact_exists", f"Contact already exists: {email}"
            )
        cid = f"C-{self._c_seq}"
        self._c_seq += 1
        self.contacts[cid] = {
            "id": cid,
            "email": email,
            "first_name": first_name or "",
            "last_name": last_name or "",
            "do_not_contact": bool(do_not_contact),
            "company_id": None,
            "created_ms": self.bus.clock_ms,
        }
        return {"id": cid}

    def get_contact(self, id: str) -> Dict[str, Any]:
        c = self.contacts.get(id)
        if not c:
            raise MCPError("unknown_contact", f"Unknown contact: {id}")
        return c

    def list_contacts(
        self,
        query: str | None = None,
        company_id: str | None = None,
        do_not_contact: bool | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_by: str = "created_ms",
        sort_dir: str = "asc",
    ) -> List[Dict[str, Any]] | Dict[str, Any]:
        rows = list(self.contacts.values())
        needle = (query or "").strip().lower()
        if needle:
            rows = [
                row
                for row in rows
                if needle in str(row.get("email", "")).lower()
                or needle in str(row.get("first_name", "")).lower()
                or needle in str(row.get("last_name", "")).lower()
            ]
        if company_id:
            rows = [row for row in rows if str(row.get("company_id")) == company_id]
        if do_not_contact is not None:
            rows = [
                row
                for row in rows
                if bool(row.get("do_not_contact")) == bool(do_not_contact)
            ]

        sort_field = (
            sort_by if sort_by in {"created_ms", "email", "last_name"} else "created_ms"
        )
        rows.sort(
            key=lambda row: sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )

        is_legacy = (
            query is None
            and company_id is None
            and do_not_contact is None
            and limit is None
            and cursor is None
            and sort_by == "created_ms"
            and sort_dir == "asc"
        )
        if is_legacy:
            return rows
        return page_rows(rows, limit=limit, cursor=cursor, key="contacts")

    # Companies
    def create_company(self, name: str, domain: str | None = None) -> Dict[str, Any]:
        domain_value = (domain or "").strip().lower()
        if domain_value and any(
            str(company.get("domain", "")).strip().lower() == domain_value
            for company in self.companies.values()
        ):
            raise MCPError(
                "conflict.company_exists", f"Company already exists: {domain}"
            )
        coid = f"CO-{self._co_seq}"
        self._co_seq += 1
        self.companies[coid] = {
            "id": coid,
            "name": name,
            "domain": domain or "",
            "created_ms": self.bus.clock_ms,
        }
        return {"id": coid}

    def get_company(self, id: str) -> Dict[str, Any]:
        co = self.companies.get(id)
        if not co:
            raise MCPError("unknown_company", f"Unknown company: {id}")
        return co

    def list_companies(
        self,
        query: str | None = None,
        domain: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_by: str = "name",
        sort_dir: str = "asc",
    ) -> List[Dict[str, Any]] | Dict[str, Any]:
        rows = list(self.companies.values())
        needle = (query or "").strip().lower()
        if needle:
            rows = [
                row
                for row in rows
                if needle in str(row.get("name", "")).lower()
                or needle in str(row.get("domain", "")).lower()
            ]
        if domain:
            wanted_domain = domain.strip().lower()
            rows = [
                row
                for row in rows
                if str(row.get("domain", "")).strip().lower() == wanted_domain
            ]
        sort_field = sort_by if sort_by in {"name", "domain", "created_ms"} else "name"
        rows.sort(
            key=lambda row: sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )

        is_legacy = (
            query is None
            and domain is None
            and limit is None
            and cursor is None
            and sort_by == "name"
            and sort_dir == "asc"
        )
        if is_legacy:
            return rows
        return page_rows(rows, limit=limit, cursor=cursor, key="companies")

    # Associations
    def associate_contact_company(
        self, contact_id: str, company_id: str
    ) -> Dict[str, Any]:
        c = self.contacts.get(contact_id)
        if not c:
            raise MCPError("unknown_contact", f"Unknown contact: {contact_id}")
        if company_id not in self.companies:
            raise MCPError("unknown_company", f"Unknown company: {company_id}")
        c["company_id"] = company_id
        return {"ok": True}

    # Deals
    def create_deal(
        self,
        name: str,
        amount: float,
        stage: str = "New",
        contact_id: str | None = None,
        company_id: str | None = None,
        close_date: str | None = None,
    ) -> Dict[str, Any]:
        stage_name = self._normalize_stage(stage)
        if contact_id and contact_id not in self.contacts:
            raise MCPError("unknown_contact", f"Unknown contact: {contact_id}")
        if company_id and company_id not in self.companies:
            raise MCPError("unknown_company", f"Unknown company: {company_id}")
        did = f"D-{self._d_seq}"
        self._d_seq += 1
        self.deals[did] = {
            "id": did,
            "name": name,
            "amount": float(amount),
            "stage": stage_name,
            "contact_id": contact_id,
            "company_id": company_id,
            "close_date": close_date,
            "created_ms": self.bus.clock_ms,
            "updated_ms": self.bus.clock_ms,
            "stage_history": [{"stage": stage_name, "time_ms": self.bus.clock_ms}],
        }
        return {"id": did}

    def get_deal(self, id: str) -> Dict[str, Any]:
        d = self.deals.get(id)
        if not d:
            raise MCPError("unknown_deal", f"Unknown deal: {id}")
        return d

    def list_deals(
        self,
        stage: str | None = None,
        company_id: str | None = None,
        min_amount: float | None = None,
        max_amount: float | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_by: str = "updated_ms",
        sort_dir: str = "desc",
    ) -> List[Dict[str, Any]] | Dict[str, Any]:
        rows = list(self.deals.values())
        if stage:
            wanted_stage = self._normalize_stage(stage)
            rows = [row for row in rows if str(row.get("stage")) == wanted_stage]
        if company_id:
            rows = [row for row in rows if str(row.get("company_id")) == company_id]
        if min_amount is not None:
            rows = [row for row in rows if float(row.get("amount", 0.0)) >= min_amount]
        if max_amount is not None:
            rows = [row for row in rows if float(row.get("amount", 0.0)) <= max_amount]
        sort_field = (
            sort_by
            if sort_by in {"updated_ms", "created_ms", "amount", "stage"}
            else "updated_ms"
        )
        rows.sort(
            key=lambda row: sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )

        is_legacy = (
            stage is None
            and company_id is None
            and min_amount is None
            and max_amount is None
            and limit is None
            and cursor is None
            and sort_by == "updated_ms"
            and sort_dir == "desc"
        )
        if is_legacy:
            return rows
        return page_rows(rows, limit=limit, cursor=cursor, key="deals")

    def update_deal_stage(self, id: str, stage: str) -> Dict[str, Any]:
        d = self.deals.get(id)
        if not d:
            raise MCPError("unknown_deal", f"Unknown deal: {id}")
        next_stage = self._normalize_stage(stage)
        current = str(d.get("stage", "New"))
        if current in {"Closed Won", "Closed Lost"} and next_stage != current:
            raise MCPError(
                "invalid_stage_transition",
                f"Cannot move closed deal from {current} to {next_stage}",
            )
        d["stage"] = next_stage
        d["updated_ms"] = self.bus.clock_ms
        history = list(d.get("stage_history", []))
        history.append({"stage": next_stage, "time_ms": self.bus.clock_ms})
        d["stage_history"] = history
        return {"ok": True, "stage": next_stage}

    def reassign_deal_owner(self, id: str, owner: str) -> Dict[str, Any]:
        d = self.deals.get(id)
        if not d:
            raise MCPError("unknown_deal", f"Unknown deal: {id}")
        d["owner"] = owner
        d["updated_ms"] = self.bus.clock_ms
        history = list(d.get("owner_history", []))
        history.append({"owner": owner, "time_ms": self.bus.clock_ms})
        d["owner_history"] = history
        return {"ok": True, "owner": owner}

    # Activities
    def log_activity(
        self,
        kind: str,
        contact_id: str | None = None,
        deal_id: str | None = None,
        note: str | None = None,
    ) -> Dict[str, Any]:
        allowed_kinds = {
            "note",
            "email_outreach",
            "call",
            "meeting",
            "task",
            "system_event",
        }
        if kind not in allowed_kinds:
            raise MCPError(
                "invalid_activity_kind", f"Unsupported activity kind: {kind}"
            )
        if contact_id and contact_id not in self.contacts:
            raise MCPError("unknown_contact", f"Unknown contact: {contact_id}")
        if deal_id and deal_id not in self.deals:
            raise MCPError("unknown_deal", f"Unknown deal: {deal_id}")
        # Policy: if outreach to a DNC contact, sometimes error depending on error_rate
        if kind == "email_outreach" and contact_id:
            c = self.contacts.get(contact_id)
            if c and c.get("do_not_contact"):
                if self.error_rate > 0 and self.bus.rng.next_float() < self.error_rate:
                    raise MCPError(
                        "consent_violation", "Contact is marked do-not-contact."
                    )
        activity_id = f"A-{self._a_seq}"
        self._a_seq += 1
        rec = {
            "id": activity_id,
            "time_ms": self.bus.clock_ms,
            "kind": kind,
            "contact_id": contact_id,
            "deal_id": deal_id,
            "note": note or "",
        }
        self.activities.append(rec)
        return {"ok": True, "id": activity_id}

    def _normalize_stage(self, stage: str) -> str:
        normalized = self._STAGE_BY_KEY.get(stage.strip().lower())
        if not normalized:
            raise MCPError("invalid_stage", f"Unsupported stage: {stage}")
        return normalized


def _next_seq(rows: Dict[str, Dict[str, Any]], *, prefix: str) -> int:
    seq = 1
    for key in rows:
        if not key.startswith(prefix):
            continue
        try:
            seq = max(seq, int(key[len(prefix) :]) + 1)
        except ValueError:
            continue
    return seq
