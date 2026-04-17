from __future__ import annotations

from dataclasses import dataclass

from .models import (
    ContextSnapshot,
    CrmSourceData,
    GmailSourceData,
    GoogleSourceData,
    GranolaSourceData,
    JiraSourceData,
    LinearSourceData,
    MailArchiveSourceData,
    NotionSourceData,
    OktaSourceData,
    SlackSourceData,
    TeamsSourceData,
    source_payload,
)


@dataclass(frozen=True)
class HydrateSourceInputs:
    slack_data: SlackSourceData | None
    jira_data: JiraSourceData | None
    google_data: GoogleSourceData | None
    okta_data: OktaSourceData | None
    gmail_data: GmailSourceData | None
    mail_archive_data: MailArchiveSourceData | None
    teams_data: TeamsSourceData | None
    crm_data: CrmSourceData | None
    salesforce_data: CrmSourceData | None
    notion_data: NotionSourceData | None
    linear_data: LinearSourceData | None
    granola_data: GranolaSourceData | None
    providers: list[str]


def build_hydrate_source_inputs(snapshot: ContextSnapshot) -> HydrateSourceInputs:
    slack_source = snapshot.source_for("slack")
    jira_source = snapshot.source_for("jira")
    google_source = snapshot.source_for("google")
    okta_source = snapshot.source_for("okta")
    gmail_source = snapshot.source_for("gmail")
    mail_archive_source = snapshot.source_for("mail_archive")
    teams_source = snapshot.source_for("teams")
    crm_source = snapshot.source_for("crm")
    salesforce_source = snapshot.source_for("salesforce")
    notion_source = snapshot.source_for("notion")
    linear_source = snapshot.source_for("linear")
    granola_source = snapshot.source_for("granola")

    return HydrateSourceInputs(
        slack_data=source_payload(slack_source, SlackSourceData),
        jira_data=source_payload(jira_source, JiraSourceData),
        google_data=source_payload(google_source, GoogleSourceData),
        okta_data=source_payload(okta_source, OktaSourceData),
        gmail_data=source_payload(gmail_source, GmailSourceData),
        mail_archive_data=source_payload(mail_archive_source, MailArchiveSourceData),
        teams_data=source_payload(teams_source, TeamsSourceData),
        crm_data=source_payload(crm_source, CrmSourceData),
        salesforce_data=source_payload(salesforce_source, CrmSourceData),
        notion_data=source_payload(notion_source, NotionSourceData),
        linear_data=source_payload(linear_source, LinearSourceData),
        granola_data=source_payload(granola_source, GranolaSourceData),
        providers=[source.provider for source in snapshot.sources],
    )


__all__ = ["HydrateSourceInputs", "build_hydrate_source_inputs"]
