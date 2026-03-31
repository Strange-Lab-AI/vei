from __future__ import annotations

from vei.blueprint.models import (
    BlueprintApprovalAsset,
    BlueprintAsset,
    BlueprintBillingCaseAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintCommGraphAsset,
    BlueprintDispatchAppointmentAsset,
    BlueprintDocGraphAsset,
    BlueprintDocumentAsset,
    BlueprintOpsGraphAsset,
    BlueprintServiceCustomerAsset,
    BlueprintServiceExceptionAsset,
    BlueprintServicePolicyAsset,
    BlueprintServiceRequestAsset,
    BlueprintServiceWorkOrderAsset,
    BlueprintSlackChannelAsset,
    BlueprintTechnicianAsset,
    BlueprintTicketAsset,
    BlueprintWorkGraphAsset,
)

from .packs_helpers import _channel, _mail_message, _mail_thread, _slack_message


def build() -> BlueprintAsset:
    slack_channels: list[BlueprintSlackChannelAsset] = [
        _channel(
            "#clearwater-dispatch",
            unread=5,
            messages=[
                _slack_message(
                    "1712000000.000100",
                    "dispatch.lead",
                    "VIP outage just landed for Clearwater Medical Plaza. Original technician is now unavailable.",
                ),
                _slack_message(
                    "1712000060.000200",
                    "maya.ops",
                    "We need a replacement tech with HVAC + controls before the SLA clock goes red.",
                ),
                _slack_message(
                    "1712000120.000300",
                    "dispatch.lead",
                    "Billing also flagged an open dispute on this account. Do not let the field team walk in blind.",
                    thread_ts="1712000000.000100",
                ),
            ],
        ),
        _channel(
            "#vip-escalations",
            unread=2,
            messages=[
                _slack_message(
                    "1712000180.000400",
                    "account.mgr",
                    "Clearwater Medical is a board-visibility account. They want an ETA in 20 minutes.",
                ),
                _slack_message(
                    "1712000240.000500",
                    "maya.ops",
                    "One clean manager touch only. Route approvals once, not five times.",
                ),
            ],
        ),
        _channel(
            "#billing-ops",
            unread=3,
            messages=[
                _slack_message(
                    "1712000300.000600",
                    "billing.lead",
                    "Invoice INV-CFS-402 is still disputed and the account is tagged hold-until-reviewed.",
                ),
                _slack_message(
                    "1712000360.000700",
                    "collections",
                    "Auto-follow-up email is queued for 9:00 unless we stop it.",
                ),
                _slack_message(
                    "1712000420.000800",
                    "billing.lead",
                    "Please link the field response to the dispute note so finance sees the same story as dispatch.",
                    thread_ts="1712000300.000600",
                ),
            ],
        ),
        _channel(
            "#field-techs",
            unread=2,
            messages=[
                _slack_message(
                    "1712000480.000900",
                    "troy.tech",
                    "Calling out sick. I can't take WO-CFS-100 this morning.",
                ),
                _slack_message(
                    "1712000540.001000",
                    "dispatch.lead",
                    "Copy. Looking for backup with rooftop controls certification now.",
                    thread_ts="1712000480.000900",
                ),
            ],
        ),
        _channel(
            "#exec-brief",
            unread=1,
            messages=[
                _slack_message(
                    "1712000600.001100",
                    "coo",
                    "I want the same-day view: customer risk, dispatch status, and whether billing is safely contained.",
                ),
                _slack_message(
                    "1712000660.001150",
                    "cfo",
                    "If finance touches this account, I need the dispute posture reflected before any outbound billing note lands.",
                ),
            ],
        ),
    ]

    mail_threads = [
        _mail_thread(
            "MAIL-CFS-VIP",
            title="Urgent outage at Clearwater Medical Plaza",
            category="customer",
            messages=[
                _mail_message(
                    "facilities@clearwatermedical.example.com",
                    "dispatch@cfs.example.com",
                    "Urgent rooftop unit failure",
                    "Our main rooftop unit failed before clinic open. We need a qualified technician onsite immediately.",
                    time_ms=1712000900000,
                ),
                _mail_message(
                    "maya.ops@cfs.example.com",
                    "facilities@clearwatermedical.example.com",
                    "Re: Urgent rooftop unit failure",
                    "We are re-routing a certified technician now and will send a confirmed ETA shortly.",
                    unread=False,
                    time_ms=1712001080000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-CFS-BILLING",
            title="Invoice dispute INV-CFS-402",
            category="billing",
            messages=[
                _mail_message(
                    "ap@clearwatermedical.example.com",
                    "billing@cfs.example.com",
                    "Open dispute on January service charge",
                    "We still need clarification on the January controls charge. Please pause additional collection notices until resolved.",
                    time_ms=1712001200000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-CFS-SICK",
            title="Technician sick call",
            category="staffing",
            messages=[
                _mail_message(
                    "troy.tech@cfs.example.com",
                    "dispatch@cfs.example.com",
                    "Unable to cover morning route",
                    "Running a fever and cannot take Clearwater Medical or the south loop route today.",
                    time_ms=1712001320000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-CFS-MANAGER",
            title="Morning manager brief",
            category="internal_follow_through",
            messages=[
                _mail_message(
                    "maya.ops@cfs.example.com",
                    "coo@cfs.example.com",
                    "Morning flashpoint",
                    "VIP outage, technician no-show, and unresolved billing dispute are colliding on the same customer account.",
                    unread=False,
                    time_ms=1712001440000,
                ),
            ],
        ),
    ]

    documents = [
        BlueprintDocumentAsset(
            doc_id="DOC-CFS-RUNBOOK",
            title="Clearwater Medical Response Runbook",
            body=(
                "Service-day runbook.\n\n"
                "If the account is VIP and billing is disputed, pause collection follow-ups before dispatch confirmation goes out."
            ),
            tags=["runbook", "vip", "billing"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-CFS-DISPATCH",
            title="Morning Dispatch Board",
            body=(
                "WO-CFS-100 currently unassigned after technician sick call.\n"
                "Need controls-certified backup tech in the south zone."
            ),
            tags=["dispatch", "field"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-CFS-BILLING",
            title="Clearwater Billing Notes",
            body=(
                "INV-CFS-402 dispute remains open.\n"
                "Collections notice should be held until finance review is complete."
            ),
            tags=["billing", "finance"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-CFS-VIP",
            title="VIP Account Brief",
            body=(
                "Clearwater Medical Plaza is a high-retention account with escalation coverage and executive visibility."
            ),
            tags=["vip", "account"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-CFS-HANDOFF",
            title="Field-To-Billing Handoff Note",
            body=(
                "Update after dispatch is confirmed so billing and account teams see the same timeline."
            ),
            tags=["handoff", "ops"],
        ),
    ]

    tickets = [
        BlueprintTicketAsset(
            ticket_id="JRA-CFS-10",
            title="VIP outage command thread",
            status="open",
            assignee="maya.ops",
            description="Stabilize Clearwater Medical before clinic operations miss SLA.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-CFS-11",
            title="Backup dispatch routing",
            status="in_progress",
            assignee="dispatch.lead",
            description="Reassign the failed morning route to a controls-certified technician.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-CFS-12",
            title="Billing dispute follow-through",
            status="review",
            assignee="billing.lead",
            description="Keep the disputed invoice on hold while the service response is active.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-CFS-13",
            title="Tech roster refresh",
            status="open",
            assignee="field.manager",
            description="Confirm standby availability for the south zone backup bench.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-CFS-14",
            title="Routine PM follow-up",
            status="closed",
            assignee="dispatch.coordinator",
            description="Unrelated maintenance closeout from the prior week.",
        ),
    ]

    service_requests = [
        BlueprintServiceRequestAsset(
            request_id="SR-CFS-100",
            title="VIP emergency dispatch approval",
            status="pending_approval",
            requester="dispatch.lead",
            description="Approve emergency reassignment for Clearwater Medical before the SLA breach window.",
            approvals=[BlueprintApprovalAsset(stage="dispatch", status="PENDING")],
        ),
        BlueprintServiceRequestAsset(
            request_id="SR-CFS-101",
            title="Pause billing follow-up on disputed VIP account",
            status="in_progress",
            requester="billing.lead",
            description="Link the billing hold to the open dispute and active service response.",
            approvals=[BlueprintApprovalAsset(stage="finance", status="APPROVED")],
        ),
        BlueprintServiceRequestAsset(
            request_id="SR-CFS-102",
            title="Authorize standby technician overtime",
            status="approved",
            requester="field.manager",
            description="Pre-approve standby coverage if the dispatch swap crosses the overtime threshold.",
            approvals=[BlueprintApprovalAsset(stage="ops", status="APPROVED")],
        ),
        BlueprintServiceRequestAsset(
            request_id="SR-CFS-103",
            title="Customer communication check",
            status="review",
            requester="account.mgr",
            description="Ensure customer ETA note and billing posture tell the same story.",
            approvals=[BlueprintApprovalAsset(stage="account", status="PENDING")],
        ),
    ]

    return BlueprintAsset(
        name="service_ops.blueprint",
        title="Clearwater Field Services",
        description=(
            "Service-day collision across dispatch, billing, and exception handling for a VIP field-service account."
        ),
        scenario_name="service_day_collision",
        family_name="service_ops",
        workflow_name="service_ops",
        workflow_variant="service_day_collision",
        requested_facades=[
            "slack",
            "mail",
            "docs",
            "jira",
            "servicedesk",
            "service_ops",
        ],
        capability_graphs=BlueprintCapabilityGraphsAsset(
            organization_name="Clearwater Field Services",
            organization_domain="cfs.example.com",
            timezone="America/Chicago",
            scenario_brief=(
                "A VIP customer outage, technician no-show, and unresolved billing dispute all hit the same account at once."
            ),
            comm_graph=BlueprintCommGraphAsset(
                slack_initial_message="Service-day collision is now live for Clearwater Field Services.",
                slack_channels=slack_channels,
                mail_threads=mail_threads,
            ),
            doc_graph=BlueprintDocGraphAsset(documents=documents),
            work_graph=BlueprintWorkGraphAsset(
                tickets=tickets,
                service_requests=service_requests,
            ),
            ops_graph=BlueprintOpsGraphAsset(
                customers=[
                    BlueprintServiceCustomerAsset(
                        customer_id="CUST-CFS-100",
                        name="Clearwater Medical Plaza",
                        tier="enterprise",
                        account_status="active",
                        vip=True,
                        dispute_open=True,
                    ),
                    BlueprintServiceCustomerAsset(
                        customer_id="CUST-CFS-200",
                        name="Lakeside Retail Center",
                        tier="standard",
                        account_status="active",
                    ),
                    BlueprintServiceCustomerAsset(
                        customer_id="CUST-CFS-300",
                        name="North River Apartments",
                        tier="standard",
                        account_status="active",
                    ),
                ],
                work_orders=[
                    BlueprintServiceWorkOrderAsset(
                        work_order_id="WO-CFS-100",
                        service_request_id="SR-CFS-100",
                        customer_id="CUST-CFS-100",
                        title="Restore rooftop unit before clinic open",
                        status="pending_dispatch",
                        required_skill="controls",
                        appointment_id="APT-CFS-100",
                        estimated_amount_usd=1450.0,
                    ),
                    BlueprintServiceWorkOrderAsset(
                        work_order_id="WO-CFS-200",
                        service_request_id="SR-CFS-102",
                        customer_id="CUST-CFS-200",
                        title="Routine PM route",
                        status="scheduled",
                        required_skill="hvac",
                        technician_id="TECH-CFS-03",
                        appointment_id="APT-CFS-200",
                        estimated_amount_usd=320.0,
                    ),
                    BlueprintServiceWorkOrderAsset(
                        work_order_id="WO-CFS-300",
                        service_request_id="SR-CFS-103",
                        customer_id="CUST-CFS-300",
                        title="Warranty callback visit",
                        status="queued",
                        required_skill="plumbing",
                        appointment_id="APT-CFS-300",
                        estimated_amount_usd=180.0,
                    ),
                ],
                technicians=[
                    BlueprintTechnicianAsset(
                        technician_id="TECH-CFS-01",
                        name="Troy Hale",
                        status="unavailable",
                        skills=["hvac", "controls"],
                        current_appointment_id="APT-CFS-100",
                        home_zone="south",
                    ),
                    BlueprintTechnicianAsset(
                        technician_id="TECH-CFS-02",
                        name="Rhea Patel",
                        status="available",
                        skills=["controls", "electrical"],
                        home_zone="south",
                    ),
                    BlueprintTechnicianAsset(
                        technician_id="TECH-CFS-03",
                        name="Marco Diaz",
                        status="available",
                        skills=["hvac"],
                        current_appointment_id="APT-CFS-200",
                        home_zone="west",
                    ),
                    BlueprintTechnicianAsset(
                        technician_id="TECH-CFS-04",
                        name="Elena Brooks",
                        status="standby",
                        skills=["plumbing", "hvac"],
                        home_zone="north",
                    ),
                    BlueprintTechnicianAsset(
                        technician_id="TECH-CFS-05",
                        name="Noah Price",
                        status="available",
                        skills=["electrical"],
                        home_zone="central",
                    ),
                ],
                appointments=[
                    BlueprintDispatchAppointmentAsset(
                        appointment_id="APT-CFS-100",
                        service_request_id="SR-CFS-100",
                        customer_id="CUST-CFS-100",
                        work_order_id="WO-CFS-100",
                        status="risk",
                        technician_id="TECH-CFS-01",
                        scheduled_for_ms=1712004000000,
                        dispatch_status="at_risk",
                        reschedule_count=0,
                    ),
                    BlueprintDispatchAppointmentAsset(
                        appointment_id="APT-CFS-200",
                        service_request_id="SR-CFS-102",
                        customer_id="CUST-CFS-200",
                        work_order_id="WO-CFS-200",
                        status="scheduled",
                        technician_id="TECH-CFS-03",
                        scheduled_for_ms=1712007600000,
                        dispatch_status="assigned",
                        reschedule_count=0,
                    ),
                    BlueprintDispatchAppointmentAsset(
                        appointment_id="APT-CFS-300",
                        service_request_id="SR-CFS-103",
                        customer_id="CUST-CFS-300",
                        work_order_id="WO-CFS-300",
                        status="queued",
                        scheduled_for_ms=1712011200000,
                        dispatch_status="pending",
                        reschedule_count=0,
                    ),
                ],
                billing_cases=[
                    BlueprintBillingCaseAsset(
                        billing_case_id="BILL-CFS-100",
                        customer_id="CUST-CFS-100",
                        invoice_id="INV-CFS-402",
                        dispute_status="open",
                        hold=False,
                        amount_usd=2480.0,
                    ),
                    BlueprintBillingCaseAsset(
                        billing_case_id="BILL-CFS-200",
                        customer_id="CUST-CFS-200",
                        invoice_id="INV-CFS-550",
                        dispute_status="clear",
                        hold=False,
                        amount_usd=320.0,
                    ),
                ],
                exceptions=[
                    BlueprintServiceExceptionAsset(
                        exception_id="EXC-CFS-100",
                        type="technician_unavailable",
                        severity="high",
                        status="open",
                        customer_id="CUST-CFS-100",
                        service_request_id="SR-CFS-100",
                        work_order_id="WO-CFS-100",
                    ),
                    BlueprintServiceExceptionAsset(
                        exception_id="EXC-CFS-101",
                        type="billing_dispute_open",
                        severity="high",
                        status="open",
                        customer_id="CUST-CFS-100",
                        service_request_id="SR-CFS-101",
                        work_order_id="WO-CFS-100",
                    ),
                    BlueprintServiceExceptionAsset(
                        exception_id="EXC-CFS-102",
                        type="sla_risk",
                        severity="high",
                        status="open",
                        customer_id="CUST-CFS-100",
                        service_request_id="SR-CFS-100",
                        work_order_id="WO-CFS-100",
                    ),
                    BlueprintServiceExceptionAsset(
                        exception_id="EXC-CFS-103",
                        type="classification_low_confidence",
                        severity="medium",
                        status="review",
                        customer_id="CUST-CFS-300",
                        service_request_id="SR-CFS-103",
                        work_order_id="WO-CFS-300",
                    ),
                ],
                policy=BlueprintServicePolicyAsset(
                    approval_threshold_usd=1000.0,
                    vip_priority_override=True,
                    billing_hold_on_dispute=True,
                    max_auto_reschedules=2,
                ),
                metadata={"vertical": "service_ops"},
            ),
            metadata={
                "vertical": "service_ops",
                "what_if_branches": [
                    "Run without coordinated dispatch recovery and let the SLA slip",
                    "Raise the approval threshold and re-run the same morning",
                ],
            },
        ),
        metadata={"vertical": "service_ops"},
    )
