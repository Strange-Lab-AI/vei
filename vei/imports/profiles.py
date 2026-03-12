from __future__ import annotations

from vei.imports.models import MappingProfileSpec


_PROFILES = {
    "okta_users_v1": MappingProfileSpec(
        name="okta_users_v1",
        source_system="okta",
        file_type="csv",
        expected_fields=[
            "user_id",
            "email",
            "login",
            "first_name",
            "last_name",
            "status",
            "department",
            "title",
            "manager",
            "org_unit",
            "group_ids",
            "application_ids",
            "last_login_ms",
        ],
        required_fields=["user_id", "email", "first_name", "last_name", "status"],
        list_fields=["group_ids", "application_ids"],
        int_fields=["last_login_ms"],
    ),
    "okta_groups_v1": MappingProfileSpec(
        name="okta_groups_v1",
        source_system="okta",
        file_type="csv",
        expected_fields=["group_id", "name", "description", "members"],
        required_fields=["group_id", "name"],
        list_fields=["members"],
    ),
    "okta_apps_v1": MappingProfileSpec(
        name="okta_apps_v1",
        source_system="okta",
        file_type="csv",
        expected_fields=[
            "app_id",
            "label",
            "status",
            "description",
            "sign_on_mode",
            "assignments",
        ],
        required_fields=["app_id", "label", "status"],
        list_fields=["assignments"],
    ),
    "google_drive_shares_v1": MappingProfileSpec(
        name="google_drive_shares_v1",
        source_system="google_drive",
        file_type="csv",
        expected_fields=[
            "doc_id",
            "title",
            "owner",
            "visibility",
            "classification",
            "shared_with",
        ],
        required_fields=["doc_id", "title", "owner", "visibility"],
        list_fields=["shared_with"],
    ),
    "hris_employees_v1": MappingProfileSpec(
        name="hris_employees_v1",
        source_system="hris",
        file_type="csv",
        expected_fields=[
            "employee_id",
            "email",
            "display_name",
            "department",
            "manager",
            "status",
            "cohort",
            "identity_conflict",
            "onboarded",
            "org_unit",
            "notes",
        ],
        required_fields=[
            "employee_id",
            "email",
            "display_name",
            "department",
            "manager",
            "status",
        ],
        list_fields=["notes"],
        bool_fields=["identity_conflict", "onboarded"],
    ),
    "jira_issues_v1": MappingProfileSpec(
        name="jira_issues_v1",
        source_system="jira",
        file_type="json",
        root_list_key="issues",
        expected_fields=[
            "id",
            "title",
            "status",
            "assignee",
            "description",
            "comments",
        ],
        required_fields=["id", "title", "status"],
        list_fields=["comments"],
    ),
    "approval_requests_v1": MappingProfileSpec(
        name="approval_requests_v1",
        source_system="servicedesk",
        file_type="json",
        root_list_key="requests",
        expected_fields=[
            "request_id",
            "title",
            "status",
            "requester",
            "description",
            "approvals",
        ],
        required_fields=["request_id", "title", "status"],
        list_fields=["approvals"],
    ),
    "identity_policies_v1": MappingProfileSpec(
        name="identity_policies_v1",
        source_system="policy",
        file_type="json",
        root_list_key="policies",
        expected_fields=[
            "policy_id",
            "title",
            "allowed_application_ids",
            "forbidden_share_domains",
            "required_approval_stages",
            "deadline_max_ms",
            "notes",
            "break_glass_requires_followup",
        ],
        required_fields=["policy_id", "title"],
        list_fields=[
            "allowed_application_ids",
            "forbidden_share_domains",
            "required_approval_stages",
            "notes",
        ],
        bool_fields=["break_glass_requires_followup"],
        int_fields=["deadline_max_ms"],
    ),
    "audit_events_v1": MappingProfileSpec(
        name="audit_events_v1",
        source_system="audit",
        file_type="json",
        root_list_key="events",
        expected_fields=[
            "event_id",
            "ts",
            "event_type",
            "user_email",
            "application_id",
            "details",
        ],
        required_fields=["event_id", "event_type"],
    ),
    "crm_deals_v1": MappingProfileSpec(
        name="crm_deals_v1",
        source_system="crm",
        file_type="json",
        root_list_key="deals",
        expected_fields=[
            "id",
            "name",
            "amount",
            "stage",
            "owner",
            "contact_id",
            "company_id",
        ],
        required_fields=["id", "name", "amount", "stage", "owner"],
    ),
}


def get_mapping_profile(name: str) -> MappingProfileSpec:
    key = name.strip().lower()
    if key not in _PROFILES:
        raise KeyError(f"unknown mapping profile: {name}")
    return _PROFILES[key]


def list_mapping_profiles() -> list[MappingProfileSpec]:
    return [profile.model_copy(deep=True) for profile in _PROFILES.values()]


__all__ = ["get_mapping_profile", "list_mapping_profiles"]
