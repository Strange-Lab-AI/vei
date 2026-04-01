from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence

from vei.context.api import capture_context
from vei.context.models import ContextProviderConfig, ContextSnapshot
from vei.verticals import (
    default_vertical_contract_variant,
    default_vertical_scenario_variant,
    list_vertical_contract_variants,
    list_vertical_scenario_variants,
)

from .api import build_customer_twin
from .models import (
    ContextMoldConfig,
    TwinArchetype,
    TwinCrisisLevel,
    TwinDensityLevel,
    TwinMatrixBundle,
    TwinTemplateSpec,
    TwinVariantSpec,
)

TWIN_MATRIX_FILE = "twin_matrix.json"


def build_twin_matrix(
    output_root: str | Path,
    *,
    snapshot: ContextSnapshot | None = None,
    provider_configs: list[ContextProviderConfig] | None = None,
    organization_name: str | None = None,
    organization_domain: str = "",
    archetypes: Sequence[TwinArchetype] | None = None,
    density_levels: Sequence[TwinDensityLevel] | None = None,
    crisis_levels: Sequence[TwinCrisisLevel] | None = None,
    seeds: Sequence[int] | None = None,
    overwrite: bool = True,
) -> TwinMatrixBundle:
    matrix_root = Path(output_root).expanduser().resolve()
    matrix_root.mkdir(parents=True, exist_ok=True)

    resolved_snapshot = _resolve_snapshot(
        snapshot=snapshot,
        provider_configs=provider_configs,
        organization_name=organization_name,
        organization_domain=organization_domain,
    )
    resolved_name = organization_name or _organization_name(
        resolved_snapshot, fallback="Vector Operations"
    )
    resolved_domain = organization_domain or _organization_domain(resolved_snapshot)

    selected_archetypes = list(archetypes or _default_archetypes(resolved_snapshot))
    selected_densities = list(density_levels or ["small", "medium", "large"])
    selected_crises = list(crisis_levels or ["calm", "escalated", "adversarial"])
    selected_seeds = list(seeds or [42042])

    variants: list[TwinVariantSpec] = []
    for archetype in selected_archetypes:
        for density in selected_densities:
            for crisis_level in selected_crises:
                for seed in selected_seeds:
                    scenario_variant = _scenario_variant_for_level(
                        archetype, crisis_level
                    )
                    contract_variant = _contract_variant_for_level(
                        archetype, crisis_level
                    )
                    variant_id = f"{archetype}-{crisis_level}-{density}-seed{seed}"
                    workspace_root = matrix_root / "variants" / archetype / variant_id
                    mold = ContextMoldConfig(
                        archetype=archetype,
                        density_level=density,
                        crisis_family=crisis_level,
                        synthetic_expansion_strength=_synthetic_strength_for_density(
                            density
                        ),
                        named_team_expansion=_team_expansion_for_density(density),
                        scenario_variant=scenario_variant,
                        contract_variant=contract_variant,
                    )
                    build_customer_twin(
                        workspace_root,
                        snapshot=resolved_snapshot,
                        organization_name=resolved_name,
                        organization_domain=resolved_domain,
                        mold=mold,
                        overwrite=overwrite,
                    )
                    variants.append(
                        TwinVariantSpec(
                            variant_id=variant_id,
                            workspace_root=workspace_root,
                            organization_name=resolved_name,
                            organization_domain=resolved_domain,
                            archetype=archetype,
                            density_level=density,
                            crisis_level=crisis_level,
                            seed=seed,
                            mold=mold,
                            scenario_variant=scenario_variant,
                            contract_variant=contract_variant,
                        )
                    )

    bundle = TwinMatrixBundle(
        output_root=matrix_root,
        template=TwinTemplateSpec(
            organization_name=resolved_name,
            organization_domain=resolved_domain,
            source_snapshot_path=(
                str(matrix_root / "source_snapshot.json")
                if resolved_snapshot is not None
                else None
            ),
            archetypes=selected_archetypes,
            density_levels=selected_densities,
            crisis_levels=selected_crises,
            seeds=selected_seeds,
        ),
        variants=variants,
        generated_at=_iso_now(),
    )
    if resolved_snapshot is not None:
        (matrix_root / "source_snapshot.json").write_text(
            resolved_snapshot.model_dump_json(indent=2),
            encoding="utf-8",
        )
    (matrix_root / TWIN_MATRIX_FILE).write_text(
        bundle.model_dump_json(indent=2),
        encoding="utf-8",
    )
    return bundle


def load_twin_matrix(output_root: str | Path) -> TwinMatrixBundle:
    matrix_root = Path(output_root).expanduser().resolve()
    return TwinMatrixBundle.model_validate_json(
        (matrix_root / TWIN_MATRIX_FILE).read_text(encoding="utf-8")
    )


def _resolve_snapshot(
    *,
    snapshot: ContextSnapshot | None,
    provider_configs: list[ContextProviderConfig] | None,
    organization_name: str | None,
    organization_domain: str,
) -> ContextSnapshot | None:
    if snapshot is not None:
        return snapshot
    if provider_configs is None:
        return None
    if not organization_name:
        raise ValueError("organization_name is required when building from providers")
    return capture_context(
        provider_configs,
        organization_name=organization_name,
        organization_domain=organization_domain,
    )


def _default_archetypes(snapshot: ContextSnapshot | None) -> list[TwinArchetype]:
    if snapshot is not None:
        return ["b2b_saas"]
    return [
        "b2b_saas",
        "digital_marketing_agency",
        "real_estate_management",
        "storage_solutions",
    ]


def _organization_name(snapshot: ContextSnapshot | None, *, fallback: str) -> str:
    if snapshot is None or not snapshot.organization_name.strip():
        return fallback
    return snapshot.organization_name


def _organization_domain(snapshot: ContextSnapshot | None) -> str:
    if snapshot is None:
        return ""
    return snapshot.organization_domain


def _scenario_variant_for_level(
    archetype: TwinArchetype,
    crisis_level: TwinCrisisLevel,
) -> str:
    variants = list_vertical_scenario_variants(archetype)
    if not variants:
        return default_vertical_scenario_variant(archetype).name
    if crisis_level == "calm":
        return variants[0].name
    if crisis_level == "escalated":
        index = min(1, len(variants) - 1)
        return variants[index].name
    return variants[-1].name


def _contract_variant_for_level(
    archetype: TwinArchetype,
    crisis_level: TwinCrisisLevel,
) -> str:
    variants = list_vertical_contract_variants(archetype)
    if not variants:
        return default_vertical_contract_variant(archetype).name
    if crisis_level == "calm":
        return variants[0].name
    if crisis_level == "escalated":
        index = min(1, len(variants) - 1)
        return variants[index].name
    return variants[-1].name


def _synthetic_strength_for_density(
    density: TwinDensityLevel,
) -> str:
    if density == "small":
        return "light"
    if density == "large":
        return "strong"
    return "medium"


def _team_expansion_for_density(
    density: TwinDensityLevel,
) -> str:
    if density == "small":
        return "minimal"
    if density == "large":
        return "expanded"
    return "standard"


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()
