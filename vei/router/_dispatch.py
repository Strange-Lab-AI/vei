from __future__ import annotations

from typing import Any

GUARDED_PREFIXES = {"erp.": "ERP", "crm.": "CRM"}


def build_dispatch_table(router: Any) -> dict[str, Any]:
    table: dict[str, Any] = {
        "slack.list_channels": lambda a: router.slack.list_channels(),
        "slack.open_channel": lambda a: router.slack.open_channel(**a),
        "slack.send_message": lambda a: router.slack.send_message(**a),
        "slack.react": lambda a: router.slack.react(**a),
        "slack.fetch_thread": lambda a: router.slack.fetch_thread(**a),
        "mail.list": lambda a: router.mail.list(**a),
        "mail.open": lambda a: router.mail.open(**a),
        "mail.compose": lambda a: router.mail.compose(**a),
        "mail.reply": lambda a: router.mail.reply(**a),
        "browser.open": lambda a: router.browser.open(**a),
        "browser.find": lambda a: router.browser.find(**a),
        "browser.click": lambda a: router.browser.click(**a),
        "browser.type": lambda a: router.browser.type(**a),
        "browser.submit": lambda a: router.browser.submit(**a),
        "browser.read": lambda a: router.browser.read(),
        "browser.back": lambda a: router.browser.back(),
        "docs.list": lambda a: router.docs.list(**a),
        "docs.read": lambda a: router.docs.read(**a),
        "docs.search": lambda a: router.docs.search(**a),
        "docs.create": lambda a: router.docs.create(**a),
        "docs.update": lambda a: router.docs.update(**a),
        "calendar.list_events": lambda a: router.calendar.list_events(**a),
        "calendar.create_event": lambda a: router.calendar.create_event(**a),
        "calendar.accept": lambda a: router.calendar.accept(**a),
        "calendar.decline": lambda a: router.calendar.decline(**a),
        "calendar.update_event": lambda a: router.calendar.update_event(**a),
        "calendar.cancel_event": lambda a: router.calendar.cancel_event(**a),
        "tickets.list": lambda a: router.tickets.list(**a),
        "tickets.get": lambda a: router.tickets.get(**a),
        "tickets.create": lambda a: router.tickets.create(**a),
        "tickets.update": lambda a: router.tickets.update(**a),
        "tickets.transition": lambda a: router.tickets.transition(**a),
        "tickets.add_comment": lambda a: router.tickets.add_comment(**a),
    }
    if getattr(router, "erp", None):
        erp = router.erp
        table.update(
            {
                "erp.create_po": lambda a: erp.create_po(**a),
                "erp.get_po": lambda a: erp.get_po(**a),
                "erp.list_pos": lambda a: erp.list_pos(**a),
                "erp.receive_goods": lambda a: erp.receive_goods(**a),
                "erp.submit_invoice": lambda a: erp.submit_invoice(**a),
                "erp.get_invoice": lambda a: erp.get_invoice(**a),
                "erp.list_invoices": lambda a: erp.list_invoices(**a),
                "erp.match_three_way": lambda a: erp.match_three_way(**a),
                "erp.post_payment": lambda a: erp.post_payment(**a),
            }
        )
    if getattr(router, "crm", None):
        crm = router.crm
        table.update(
            {
                "crm.create_contact": lambda a: crm.create_contact(**a),
                "crm.get_contact": lambda a: crm.get_contact(**a),
                "crm.list_contacts": lambda a: crm.list_contacts(**a),
                "crm.create_company": lambda a: crm.create_company(**a),
                "crm.get_company": lambda a: crm.get_company(**a),
                "crm.list_companies": lambda a: crm.list_companies(**a),
                "crm.associate_contact_company": lambda a: crm.associate_contact_company(
                    **a
                ),
                "crm.create_deal": lambda a: crm.create_deal(**a),
                "crm.get_deal": lambda a: crm.get_deal(**a),
                "crm.list_deals": lambda a: crm.list_deals(**a),
                "crm.update_deal_stage": lambda a: crm.update_deal_stage(**a),
                "crm.reassign_deal_owner": lambda a: crm.reassign_deal_owner(**a),
                "crm.log_activity": lambda a: crm.log_activity(**a),
            }
        )
    return table
