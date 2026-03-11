from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class Participant:
    """Metadata describing an actor in the scenario (humans, systems)."""

    participant_id: str
    name: Optional[str] = None
    role: Optional[str] = None
    email: Optional[str] = None
    slack: Optional[str] = None


@dataclass
class Document:
    """Synthetic document stored in the docs twin."""

    doc_id: str
    title: str
    body: str
    tags: Optional[List[str]] = None


@dataclass
class CalendarEvent:
    """Calendar entry that can be exposed via calendar tools."""

    event_id: str
    title: str
    start_ms: int
    end_ms: int
    attendees: Optional[List[str]] = None
    location: Optional[str] = None
    description: Optional[str] = None


@dataclass
class Ticket:
    """Ticket/work item tracked by the tickets twin."""

    ticket_id: str
    title: str
    status: str
    assignee: Optional[str] = None
    description: Optional[str] = None
    history: Optional[List[Dict[str, Any]]] = None


@dataclass
class SpreadsheetSheet:
    """Tabular worksheet stored in the spreadsheet facade."""

    sheet_id: str
    title: str
    columns: Optional[List[str]] = None
    rows: Optional[List[Dict[str, Any]]] = None
    cells: Optional[Dict[str, Any]] = None
    formulas: Optional[Dict[str, str]] = None
    tables: Optional[List[Dict[str, Any]]] = None
    filters: Optional[List[Dict[str, Any]]] = None
    sorts: Optional[List[Dict[str, Any]]] = None


@dataclass
class SpreadsheetWorkbook:
    """Workbook stored in the spreadsheet facade."""

    workbook_id: str
    title: str
    owner: Optional[str] = None
    shared_with: Optional[List[str]] = None
    permissions: Optional[Dict[str, str]] = None
    sheets: Optional[List[SpreadsheetSheet]] = None


@dataclass
class ServiceDeskIncident:
    """Incident tracked by the ServiceDesk twin."""

    incident_id: str
    title: str
    status: str
    priority: Optional[str] = None
    assignee: Optional[str] = None
    description: Optional[str] = None
    history: Optional[List[Dict[str, Any]]] = None
    comments: Optional[List[Dict[str, Any]]] = None


@dataclass
class ServiceDeskRequest:
    """Access/service request tracked by the ServiceDesk twin."""

    request_id: str
    title: str
    status: str
    requester: Optional[str] = None
    description: Optional[str] = None
    approvals: Optional[List[Dict[str, Any]]] = None
    history: Optional[List[Dict[str, Any]]] = None
    comments: Optional[List[Dict[str, Any]]] = None


@dataclass
class IdentityUserSeed:
    """Seed data for an identity/Okta twin."""

    user_id: str
    email: str
    first_name: str
    last_name: str
    login: Optional[str] = None
    display_name: Optional[str] = None
    status: Optional[str] = "ACTIVE"
    department: Optional[str] = None
    title: Optional[str] = None
    manager: Optional[str] = None
    groups: Optional[List[str]] = None
    applications: Optional[List[str]] = None
    factors: Optional[List[str]] = None
    last_login_ms: Optional[int] = None


@dataclass
class IdentityGroupSeed:
    group_id: str
    name: str
    description: Optional[str] = None
    members: Optional[List[str]] = None


@dataclass
class IdentityApplicationSeed:
    app_id: str
    label: str
    status: Optional[str] = "ACTIVE"
    description: Optional[str] = None
    sign_on_mode: Optional[str] = "SAML_2_0"
    assignments: Optional[List[str]] = None


@dataclass
class Scenario:
    # Slack configuration
    budget_cap_usd: Optional[int] = None
    derail_prob: Optional[float] = None
    slack_initial_message: Optional[str] = None
    slack_channels: Optional[Dict[str, Dict[str, Any]]] = None

    # Mail configuration
    vendor_reply_variants: Optional[List[str]] = None

    # Browser configuration
    browser_nodes: Optional[Dict[str, dict]] = None

    # Optional pre-scheduled events (e.g., derailments)
    derail_events: Optional[List[Dict[str, Any]]] = None

    # Additional synthetic world assets
    participants: Optional[List[Participant]] = None
    documents: Optional[Dict[str, Document]] = None
    spreadsheets: Optional[Dict[str, SpreadsheetWorkbook]] = None
    calendar_events: Optional[List[CalendarEvent]] = None
    tickets: Optional[Dict[str, Ticket]] = None
    database_tables: Optional[Dict[str, List[Dict[str, Any]]]] = None
    crm: Optional[Dict[str, Any]] = None
    triggers: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None
    identity_users: Optional[Dict[str, IdentityUserSeed]] = None
    identity_groups: Optional[Dict[str, IdentityGroupSeed]] = None
    identity_applications: Optional[Dict[str, IdentityApplicationSeed]] = None
    service_incidents: Optional[Dict[str, ServiceDeskIncident]] = None
    service_requests: Optional[Dict[str, ServiceDeskRequest]] = None

    # Enterprise control-plane / admin surfaces
    google_admin: Optional[Dict[str, Any]] = None
    siem: Optional[Dict[str, Any]] = None
    datadog: Optional[Dict[str, Any]] = None
    pagerduty: Optional[Dict[str, Any]] = None
    feature_flags: Optional[Dict[str, Any]] = None
    hris: Optional[Dict[str, Any]] = None
