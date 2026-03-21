from __future__ import annotations

from vei.capability_graph.models import CapabilityGraphActionInput

from .models import PlayableMissionMoveSpec, PlayableMissionSpec


def mission_specs() -> list[PlayableMissionSpec]:
    return [
        PlayableMissionSpec(
            vertical_name="real_estate_management",
            mission_name="tenant_opening_conflict",
            title="Tenant Opening Conflict",
            briefing="A flagship Harbor Point tenant needs to open Monday morning, but the lease amendment, vendor assignment, and unit reservation are all drifting.",
            why_it_matters="This is the clearest proof that one company world can turn into a tense, business-real mission instead of a static workflow demo.",
            failure_impact="Missed opening day hurts tenant trust, leasing credibility, and property readiness.",
            scenario_variant="tenant_opening_conflict",
            default_objective="opening_readiness",
            supported_objectives=[
                "opening_readiness",
                "minimize_tenant_disruption",
                "safety_over_speed",
            ],
            branch_labels=[
                "Protect the opening with a clean readiness path",
                "Trade speed for a riskier opening branch",
            ],
            hero=True,
            action_budget=7,
            turn_budget=9,
            countdown_ms=180000,
            primary_domain="property_graph",
            manual_moves=[
                PlayableMissionMoveSpec(
                    move_id="risk:wrong_vendor",
                    title="Assign the wrong vendor anyway",
                    summary="Force a mismatched vendor onto the blocking work order.",
                    tier="risky",
                    graph_action=CapabilityGraphActionInput(
                        domain="property_graph",
                        action="assign_vendor",
                        args={
                            "work_order_id": "WO-HPM-88",
                            "vendor_id": "VEND-HPM-ELEC",
                            "note": "Forced risky assignment.",
                        },
                    ),
                    consequence_preview="You might move faster, but the opening could stay operationally invalid.",
                )
            ],
            tags=["hero", "real-estate"],
        ),
        PlayableMissionSpec(
            vertical_name="real_estate_management",
            mission_name="vendor_no_show",
            title="Vendor No-Show",
            briefing="The original HVAC vendor disappears late, and property ops has to recover the opening path fast.",
            why_it_matters="This makes the same company feel alive: one last-minute vendor shock turns the world into a different mission.",
            failure_impact="Without a recovery path, the anchor tenant misses the opening window.",
            scenario_variant="vendor_no_show",
            default_objective="safety_over_speed",
            supported_objectives=["opening_readiness", "safety_over_speed"],
            branch_labels=[
                "Route to a safe backup path",
                "Keep waiting and risk a missed opening",
            ],
            primary_domain="property_graph",
            manual_moves=[],
            tags=["hero", "real-estate"],
        ),
        PlayableMissionSpec(
            vertical_name="real_estate_management",
            mission_name="lease_revision_late",
            title="Lease Revision Late",
            briefing="Legal redlines arrive late and compress the opening deadline for the Harbor Point tenant.",
            why_it_matters="This mission shows deadline pressure without changing the underlying company.",
            failure_impact="A rushed handoff can create an invalid opening or a delayed move-in.",
            scenario_variant="lease_revision_late",
            default_objective="safety_over_speed",
            supported_objectives=["opening_readiness", "safety_over_speed"],
            branch_labels=[
                "Compress prep safely",
                "Force the opening and carry hidden risk",
            ],
            primary_domain="property_graph",
            manual_moves=[],
            tags=["hero", "real-estate"],
        ),
        PlayableMissionSpec(
            vertical_name="real_estate_management",
            mission_name="double_booked_unit",
            title="Double-Booked Unit",
            briefing="Unit 14A is reserved for the wrong tenant right before the flagship opening.",
            why_it_matters="The same property world can produce space and reservation conflicts, not just document drift.",
            failure_impact="The tenant arrives to a broken move-in and trust damage spreads fast.",
            scenario_variant="double_booked_unit",
            default_objective="minimize_tenant_disruption",
            supported_objectives=[
                "opening_readiness",
                "minimize_tenant_disruption",
            ],
            branch_labels=[
                "Reclaim the unit cleanly",
                "Leave the reservation conflict unresolved",
            ],
            primary_domain="property_graph",
            manual_moves=[],
            tags=["hero", "real-estate"],
        ),
        PlayableMissionSpec(
            vertical_name="real_estate_management",
            mission_name="maintenance_cascade",
            title="Maintenance Cascade",
            briefing="The opening checklist is stable on paper, but prep is cascading across maintenance and vendor coordination.",
            why_it_matters="This fifth Harbor Point mission gives the hero world more replay value without adding a whole new industry.",
            failure_impact="Prep debt spills into opening day and the property team loses control of the schedule.",
            scenario_variant="tenant_opening_conflict",
            default_objective="safety_over_speed",
            supported_objectives=[
                "opening_readiness",
                "minimize_tenant_disruption",
                "safety_over_speed",
            ],
            branch_labels=[
                "Stabilize the maintenance path",
                "Push the work downstream and carry hidden opening risk",
            ],
            primary_domain="property_graph",
            manual_moves=[
                PlayableMissionMoveSpec(
                    move_id="risk:skip_unit_reservation",
                    title="Skip unit reservation and rush the opening",
                    summary="Leave the reservation unresolved while declaring the opening path complete.",
                    tier="risky",
                    graph_action=CapabilityGraphActionInput(
                        domain="comm_graph",
                        action="post_message",
                        args={
                            "channel": "#harbor-point-ops",
                            "text": "Opening declared ready before unit reservation is confirmed.",
                        },
                    ),
                    consequence_preview="This looks fast, but it leaves a hidden operational gap in the world state.",
                )
            ],
            tags=["hero", "real-estate"],
        ),
        PlayableMissionSpec(
            vertical_name="digital_marketing_agency",
            mission_name="campaign_launch_guardrail",
            title="Campaign Launch Guardrail",
            briefing="Northstar Growth needs to stop a hot launch from going live with approval, pacing, and reporting drift.",
            why_it_matters="This is the cleanest proof that the kernel can model client work, approvals, budgets, and comms in one mission.",
            failure_impact="Bad launch state burns budget and client trust at the same time.",
            scenario_variant="campaign_launch_guardrail",
            default_objective="launch_safely",
            supported_objectives=[
                "launch_safely",
                "protect_budget",
                "client_comms_first",
            ],
            branch_labels=[
                "Launch with safety restored",
                "Let unsafe spend continue and absorb the fallout",
            ],
            primary_domain="campaign_graph",
            manual_moves=[
                PlayableMissionMoveSpec(
                    move_id="risk:overpace_campaign",
                    title="Keep spend hot to preserve reach",
                    summary="Raise pacing instead of cooling it down.",
                    tier="risky",
                    graph_action=CapabilityGraphActionInput(
                        domain="campaign_graph",
                        action="adjust_budget_pacing",
                        args={"campaign_id": "CMP-APEX-01", "pacing_pct": 150.0},
                    ),
                    consequence_preview="Reach may hold for a moment, but client budget risk climbs fast.",
                )
            ],
            tags=["support", "marketing"],
        ),
        PlayableMissionSpec(
            vertical_name="digital_marketing_agency",
            mission_name="budget_runaway",
            title="Budget Runaway",
            briefing="Spend accelerates beyond plan, and the agency has to protect the client before the launch budget blows up.",
            why_it_matters="This short mission makes the same agency world feel like a finance-and-trust problem instead of only an approval problem.",
            failure_impact="Overspend erodes margin and damages the client relationship.",
            scenario_variant="budget_runaway",
            default_objective="protect_budget",
            supported_objectives=["launch_safely", "protect_budget"],
            branch_labels=[
                "Throttle spend and protect budget",
                "Keep momentum and accept budget risk",
            ],
            primary_domain="campaign_graph",
            manual_moves=[],
            tags=["support", "marketing"],
        ),
        PlayableMissionSpec(
            vertical_name="storage_solutions",
            mission_name="capacity_quote_commitment",
            title="Capacity Quote Commitment",
            briefing="Atlas Storage Systems has to lock a feasible commitment for a strategic customer before ops overcommits the rollout.",
            why_it_matters="This shows the kernel handling capacity, quotes, vendors, and customer commitments as one playable operational world.",
            failure_impact="A broken commitment creates downstream fulfillment failure with a strategic account.",
            scenario_variant="capacity_quote_commitment",
            default_objective="no_overcommit",
            supported_objectives=[
                "no_overcommit",
                "maximize_feasible_revenue",
                "ops_consistency",
            ],
            branch_labels=[
                "Commit only what ops can really deliver",
                "Overpromise now and push failure downstream",
            ],
            primary_domain="inventory_graph",
            manual_moves=[
                PlayableMissionMoveSpec(
                    move_id="risk:inflate_quote",
                    title="Promise more capacity than the pool can support",
                    summary="Revise the quote upward without protecting feasibility first.",
                    tier="risky",
                    graph_action=CapabilityGraphActionInput(
                        domain="inventory_graph",
                        action="revise_quote",
                        args={
                            "quote_id": "QTE-ATLAS-01",
                            "site_id": "SITE-ATLAS-MKE",
                            "committed_units": 180,
                        },
                    ),
                    consequence_preview="Revenue might look higher, but the world drifts toward overcommit and failed delivery.",
                )
            ],
            tags=["support", "storage"],
        ),
        PlayableMissionSpec(
            vertical_name="storage_solutions",
            mission_name="vendor_dispatch_gap",
            title="Vendor Dispatch Gap",
            briefing="The preferred dispatch path is shaky, so Atlas has to stabilize fulfillment before a strategic promise becomes impossible.",
            why_it_matters="This short mission proves the same storage world can pivot from capacity planning to downstream execution discipline.",
            failure_impact="A weak dispatch path turns a promising deal into an operational miss.",
            scenario_variant="vendor_dispatch_gap",
            default_objective="ops_consistency",
            supported_objectives=["no_overcommit", "ops_consistency"],
            branch_labels=[
                "Stabilize downstream execution",
                "Keep the quote live without dispatch certainty",
            ],
            primary_domain="inventory_graph",
            manual_moves=[],
            tags=["support", "storage"],
        ),
    ]
