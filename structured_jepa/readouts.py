from __future__ import annotations

import torch


def fit_least_squares(features: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
    design = torch.cat([features, torch.ones((features.size(0), 1))], dim=1)
    target_matrix = targets.unsqueeze(-1) if targets.ndim == 1 else targets
    result = torch.linalg.lstsq(design, target_matrix)
    if targets.ndim == 1:
        return result.solution.squeeze(-1)
    return result.solution


def apply_linear_readout(features: torch.Tensor, weights: torch.Tensor) -> torch.Tensor:
    design = torch.cat([features, torch.ones((features.size(0), 1))], dim=1)
    return design @ weights
