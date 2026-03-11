from __future__ import annotations

from vei.grounding.api import (
    build_grounding_bundle_example,
    compile_identity_governance_bundle,
    list_grounding_bundle_examples,
)


def test_grounding_bundle_example_manifest_and_compile() -> None:
    manifests = list_grounding_bundle_examples()
    bundle = build_grounding_bundle_example("acquired_user_cutover")
    asset = compile_identity_governance_bundle(bundle)

    assert any(item.name == "acquired_user_cutover" for item in manifests)
    assert manifests[0].wedge == "identity_access_governance"
    assert bundle.capability_graphs.identity_graph is not None
    assert bundle.capability_graphs.doc_graph is not None
    assert asset.capability_graphs is not None
    assert asset.workflow_parameters["employee_id"] == "EMP-2201"
    assert asset.metadata["grounding_bundle"] == "acquired_user_cutover"
    assert asset.metadata["grounding_wedge"] == "identity_access_governance"
