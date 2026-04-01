"""Check that cross-module imports go through api.py.

Rule: if a file in vei/X/ imports from vei/Y/ (X != Y), the import must
target vei.Y.api — not vei.Y.some_internal_module.

Importing vei.Y.models is allowed (type definitions), but flagged in
strict mode (--strict).

Exceptions:
  - cli/ can import from any module (it is the integration surface).
  - vei.data and vei.llm are utility/infra modules used everywhere.
  - Intra-module imports (vei.X.foo importing vei.X.bar) are fine.

Exit 0 if clean, 1 if violations found.
"""
from __future__ import annotations

import ast
from pathlib import Path
import sys

PACKAGE_ROOT = Path(__file__).resolve().parent.parent / "vei"

EXEMPT_IMPORTERS = {"cli"}

INFRA_MODULES = {"data", "llm", "policy", "behavior", "rl"}

TYPE_SUBMODULES = {"models", "errors"}


def module_of(path: Path) -> str | None:
    parts = path.relative_to(PACKAGE_ROOT).parts
    if len(parts) < 2:
        return None
    return parts[0]


def check_file(path: Path, *, strict: bool = False) -> list[tuple[str, bool]]:
    """Return (message, is_type_only) tuples."""
    source_module = module_of(path)
    if not source_module or source_module in EXEMPT_IMPORTERS:
        return []

    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return []

    violations: list[tuple[str, bool]] = []

    for node in ast.walk(tree):
        targets: list[str] = []
        if isinstance(node, ast.ImportFrom) and node.module:
            targets.append(node.module)
        elif isinstance(node, ast.Import):
            targets.extend(alias.name for alias in node.names)

        for target in targets:
            parts = target.split(".")
            if len(parts) < 3 or parts[0] != "vei":
                continue
            target_module = parts[1]
            if target_module == source_module:
                continue
            if target_module in INFRA_MODULES:
                continue
            if len(parts) == 3 and parts[2] == "api":
                continue
            if len(parts) == 2:
                continue

            sub = parts[2]
            is_type = sub in TYPE_SUBMODULES

            if is_type and not strict:
                continue

            tag = " [type-only]" if is_type else ""
            violations.append(
                (
                    f"{path}:{node.lineno}: "
                    f"vei.{source_module} imports vei.{target_module}.{'.'.join(parts[2:])}"
                    f"{tag}",
                    is_type,
                ),
            )

    return violations

def _parse_max_violations() -> int | None:
    for arg in sys.argv[1:]:
        if arg.startswith("--max-violations"):
            if "=" in arg:
                return int(arg.split("=", 1)[1])
            idx = sys.argv.index(arg)
            if idx + 1 < len(sys.argv):
                return int(sys.argv[idx + 1])
    return None


def main() -> int:
    strict = "--strict" in sys.argv
    max_violations = _parse_max_violations()

    all_violations: list[tuple[str, bool]] = []
    for py_file in sorted(PACKAGE_ROOT.rglob("*.py")):
        all_violations.extend(check_file(py_file, strict=strict))

    impl_violations = [v for v, is_type in all_violations if not is_type]
    type_violations = [v for v, is_type in all_violations if is_type]

    if impl_violations:
        print(f"Implementation boundary violations ({len(impl_violations)}):\n")
        for v in impl_violations:
            print(f"  {v}")
        print()

    if type_violations:
        print(f"Type-only boundary violations ({len(type_violations)}):\n")
        for v in type_violations:
            print(f"  {v}")
        print()

    total = len(impl_violations) + (len(type_violations) if strict else 0)
    if total:
        print(
            f"Total: {len(impl_violations)} implementation + "
            f"{len(type_violations)} type-only violations"
        )
        print(
            "Cross-module imports must go through api.py. "
            "Use 'from vei.<module>.api import ...' instead."
        )

    if max_violations is not None:
        if total > max_violations:
            print(
                f"\nBoundary ratchet FAILED: {total} violations "
                f"exceeds --max-violations {max_violations}"
            )
            return 1
        return 0

    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())
