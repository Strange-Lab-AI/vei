from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import torch
import torch.nn.functional as F
from torch import nn

from .schema import ActionCandidateBatch, ModelConfig, StepBatch


class SIGReg(nn.Module):
    def __init__(self, knots: int = 17, num_proj: int = 512) -> None:
        super().__init__()
        t = torch.linspace(0, 3, knots, dtype=torch.float32)
        dt = 3 / (knots - 1)
        weights = torch.full((knots,), 2 * dt, dtype=torch.float32)
        weights[[0, -1]] = dt
        window = torch.exp(-t.square() / 2.0)
        self.num_proj = num_proj
        self.register_buffer("t", t)
        self.register_buffer("phi", window)
        self.register_buffer("weights", weights * window)

    def forward(self, projections: torch.Tensor) -> torch.Tensor:
        device = projections.device
        proj_dim = projections.size(-1)
        t = cast(torch.Tensor, self.t)
        phi = cast(torch.Tensor, self.phi)
        weights = cast(torch.Tensor, self.weights)
        random_basis = torch.randn(proj_dim, self.num_proj, device=device)
        random_basis = random_basis.div_(random_basis.norm(p=2, dim=0).clamp_min(1e-6))
        x_t = (projections @ random_basis).unsqueeze(-1) * t
        err = (x_t.cos().mean(-3) - phi).square() + x_t.sin().mean(-3).square()
        statistic = (err @ weights) * projections.size(-2)
        return statistic.mean()


class MLPBlock(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int, output_dim: int) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.LayerNorm(input_dim),
            nn.Linear(input_dim, hidden_dim),
            nn.GELU(),
            nn.Linear(hidden_dim, output_dim),
        )

    def forward(self, values: torch.Tensor) -> torch.Tensor:
        return self.net(values)


class FlatStateEncoder(nn.Module):
    def __init__(
        self,
        *,
        observation_numeric_dim: int,
        observation_mask_dim: int,
        observation_cardinalities: list[int],
        d_state: int,
    ) -> None:
        super().__init__()
        numeric_input_dim = observation_numeric_dim + observation_mask_dim
        hidden_dim = max(d_state, 64)
        self.numeric_encoder = (
            MLPBlock(numeric_input_dim, hidden_dim, d_state) if numeric_input_dim > 0 else None
        )
        self.categorical_embeddings = nn.ModuleList(
            [
                nn.Embedding(cardinality, _embedding_dim(cardinality))
                for cardinality in observation_cardinalities
            ]
        )
        cat_dim = sum(
            cast(nn.Embedding, embedding).embedding_dim for embedding in self.categorical_embeddings
        )
        self.categorical_projection = (
            MLPBlock(cat_dim, hidden_dim, d_state) if cat_dim > 0 else None
        )
        combined_dim = (d_state if self.numeric_encoder is not None else 0) + (
            d_state if self.categorical_projection is not None else 0
        )
        self.output_projection = (
            MLPBlock(combined_dim, max(d_state, combined_dim), d_state)
            if combined_dim > 0
            else None
        )
        self.empty_token = nn.Parameter(torch.zeros(d_state))

    def forward(
        self,
        observation_numeric: torch.Tensor,
        observation_masks: torch.Tensor,
        observation_categorical: torch.Tensor,
    ) -> torch.Tensor:
        parts: list[torch.Tensor] = []
        if self.numeric_encoder is not None:
            numeric_input = torch.cat([observation_numeric, observation_masks], dim=-1)
            parts.append(self.numeric_encoder(numeric_input))
        if self.categorical_projection is not None:
            embeddings = [
                cast(nn.Embedding, embedding)(observation_categorical[..., index])
                for index, embedding in enumerate(self.categorical_embeddings)
            ]
            categorical_input = torch.cat(embeddings, dim=-1)
            parts.append(self.categorical_projection(categorical_input))
        if not parts:
            return self._expand_default_token(observation_numeric, self.empty_token)
        if len(parts) == 1:
            return parts[0]
        output_projection = self.output_projection
        if output_projection is None:
            return parts[0]
        return output_projection(torch.cat(parts, dim=-1))

    @staticmethod
    def _expand_default_token(reference: torch.Tensor, token: torch.Tensor) -> torch.Tensor:
        batch_size, time_steps = reference.shape[:2]
        return token.view(1, 1, -1).expand(batch_size, time_steps, -1)


class FlatActionEncoder(nn.Module):
    def __init__(
        self,
        *,
        action_numeric_dim: int,
        action_mask_dim: int,
        action_cardinalities: list[int],
        d_action: int,
    ) -> None:
        super().__init__()
        numeric_input_dim = action_numeric_dim + action_mask_dim
        hidden_dim = max(d_action, 64)
        self.numeric_encoder = (
            MLPBlock(numeric_input_dim, hidden_dim, d_action) if numeric_input_dim > 0 else None
        )
        self.categorical_embeddings = nn.ModuleList(
            [
                nn.Embedding(cardinality, _embedding_dim(cardinality))
                for cardinality in action_cardinalities
            ]
        )
        cat_dim = sum(
            cast(nn.Embedding, embedding).embedding_dim for embedding in self.categorical_embeddings
        )
        self.categorical_projection = (
            MLPBlock(cat_dim, hidden_dim, d_action) if cat_dim > 0 else None
        )
        combined_dim = (d_action if self.numeric_encoder is not None else 0) + (
            d_action if self.categorical_projection is not None else 0
        )
        self.output_projection = (
            MLPBlock(combined_dim, max(d_action, combined_dim), d_action)
            if combined_dim > 0
            else None
        )
        self.null_action = nn.Parameter(torch.zeros(d_action))

    def forward(
        self,
        action_numeric: torch.Tensor,
        action_masks: torch.Tensor,
        action_categorical: torch.Tensor,
    ) -> torch.Tensor:
        parts: list[torch.Tensor] = []
        if self.numeric_encoder is not None:
            numeric_input = torch.cat([action_numeric, action_masks], dim=-1)
            parts.append(self.numeric_encoder(numeric_input))
        if self.categorical_projection is not None:
            embeddings = [
                cast(nn.Embedding, embedding)(action_categorical[..., index])
                for index, embedding in enumerate(self.categorical_embeddings)
            ]
            categorical_input = torch.cat(embeddings, dim=-1)
            parts.append(self.categorical_projection(categorical_input))
        if not parts:
            return self._expand_default_token(action_numeric, self.null_action)
        if len(parts) == 1:
            encoded = parts[0]
        else:
            output_projection = self.output_projection
            encoded = (
                parts[0]
                if output_projection is None
                else output_projection(torch.cat(parts, dim=-1))
            )
        no_action_rows = _no_action_rows(action_numeric, action_masks, action_categorical)
        null_action = self._expand_default_token(action_numeric, self.null_action)
        return torch.where(no_action_rows.unsqueeze(-1), null_action, encoded)

    @staticmethod
    def _expand_default_token(reference: torch.Tensor, token: torch.Tensor) -> torch.Tensor:
        batch_size, time_steps = reference.shape[:2]
        return token.view(1, 1, -1).expand(batch_size, time_steps, -1)


class NumericFeatureTokenizer(nn.Module):
    def __init__(self, feature_count: int, token_dim: int) -> None:
        super().__init__()
        self.feature_count = feature_count
        self.token_dim = token_dim
        self.value_projection = (
            MLPBlock(2, max(32, token_dim), token_dim) if feature_count > 0 else None
        )
        self.feature_embeddings = (
            nn.Parameter(torch.randn(feature_count, token_dim) * 0.02)
            if feature_count > 0
            else None
        )
        self.type_embedding = (
            nn.Parameter(torch.randn(token_dim) * 0.02) if feature_count > 0 else None
        )

    def forward(self, values: torch.Tensor, masks: torch.Tensor) -> torch.Tensor:
        if self.feature_count == 0 or self.value_projection is None:
            return values.new_zeros((*values.shape[:2], 0, self.token_dim))
        token_inputs = torch.stack([values, masks], dim=-1)
        tokens = self.value_projection(token_inputs)
        feature_embeddings = cast(torch.Tensor, self.feature_embeddings).view(
            1, 1, self.feature_count, self.token_dim
        )
        type_embedding = cast(torch.Tensor, self.type_embedding).view(1, 1, 1, self.token_dim)
        return tokens + feature_embeddings + type_embedding


class CategoricalFeatureTokenizer(nn.Module):
    def __init__(self, cardinalities: list[int], token_dim: int) -> None:
        super().__init__()
        self.feature_count = len(cardinalities)
        self.token_dim = token_dim
        self.value_embeddings = nn.ModuleList(
            [nn.Embedding(cardinality, token_dim) for cardinality in cardinalities]
        )
        self.feature_embeddings = (
            nn.Parameter(torch.randn(self.feature_count, token_dim) * 0.02)
            if self.feature_count > 0
            else None
        )
        self.type_embedding = (
            nn.Parameter(torch.randn(token_dim) * 0.02) if self.feature_count > 0 else None
        )

    def forward(self, categorical_ids: torch.Tensor) -> torch.Tensor:
        if self.feature_count == 0:
            return torch.zeros(
                (*categorical_ids.shape[:2], 0, self.token_dim),
                dtype=torch.float32,
                device=categorical_ids.device,
            )
        feature_embeddings = cast(torch.Tensor, self.feature_embeddings)
        type_embedding = cast(torch.Tensor, self.type_embedding)
        tokens = []
        for index, embedding in enumerate(self.value_embeddings):
            token = embedding(categorical_ids[..., index])
            token = token + feature_embeddings[index].view(1, 1, self.token_dim)
            token = token + type_embedding.view(1, 1, self.token_dim)
            tokens.append(token)
        return torch.stack(tokens, dim=-2)


class FeatureTokenMixer(nn.Module):
    def __init__(self, token_dim: int, depth: int, heads: int, dropout: float) -> None:
        super().__init__()
        self.token_dim = token_dim
        self.cls_token = nn.Parameter(torch.randn(token_dim) * 0.02)
        mixer_heads = _choose_attention_heads(token_dim, heads)
        layer = nn.TransformerEncoderLayer(
            d_model=token_dim,
            nhead=mixer_heads,
            dropout=dropout,
            batch_first=True,
            dim_feedforward=max(64, token_dim * 4),
            activation="gelu",
        )
        self.encoder = nn.TransformerEncoder(layer, num_layers=depth)
        self.output_projection = MLPBlock(token_dim, max(64, token_dim), token_dim)

    def forward(self, tokens: torch.Tensor) -> torch.Tensor:
        batch_size, time_steps, token_count, token_dim = tokens.shape
        cls_token = self.cls_token.view(1, 1, 1, token_dim).expand(
            batch_size, time_steps, 1, token_dim
        )
        hidden = torch.cat([cls_token, tokens], dim=-2)
        hidden = hidden.reshape(batch_size * time_steps, token_count + 1, token_dim)
        hidden = self.encoder(hidden)
        pooled = hidden[:, 0].reshape(batch_size, time_steps, token_dim)
        return self.output_projection(pooled)


class TokenizedStateEncoder(nn.Module):
    def __init__(
        self,
        *,
        observation_numeric_dim: int,
        observation_cardinalities: list[int],
        d_state: int,
        depth: int,
        heads: int,
        dropout: float,
    ) -> None:
        super().__init__()
        self.numeric_tokenizer = NumericFeatureTokenizer(observation_numeric_dim, d_state)
        self.categorical_tokenizer = CategoricalFeatureTokenizer(observation_cardinalities, d_state)
        self.mixer = FeatureTokenMixer(d_state, depth, heads, dropout)
        self.empty_token = nn.Parameter(torch.zeros(d_state))

    def forward(
        self,
        observation_numeric: torch.Tensor,
        observation_masks: torch.Tensor,
        observation_categorical: torch.Tensor,
    ) -> torch.Tensor:
        tokens = self._collect_tokens(
            observation_numeric=observation_numeric,
            observation_masks=observation_masks,
            observation_categorical=observation_categorical,
        )
        if tokens is None:
            batch_size, time_steps = observation_numeric.shape[:2]
            return self.empty_token.view(1, 1, -1).expand(batch_size, time_steps, -1)
        return self.mixer(tokens)

    def _collect_tokens(
        self,
        *,
        observation_numeric: torch.Tensor,
        observation_masks: torch.Tensor,
        observation_categorical: torch.Tensor,
    ) -> torch.Tensor | None:
        parts: list[torch.Tensor] = []
        numeric_tokens = self.numeric_tokenizer(observation_numeric, observation_masks)
        if numeric_tokens.size(-2) > 0:
            parts.append(numeric_tokens)
        categorical_tokens = self.categorical_tokenizer(observation_categorical)
        if categorical_tokens.size(-2) > 0:
            parts.append(categorical_tokens)
        if not parts:
            return None
        return torch.cat(parts, dim=-2)


class TokenizedActionEncoder(nn.Module):
    def __init__(
        self,
        *,
        action_numeric_dim: int,
        action_cardinalities: list[int],
        d_action: int,
        depth: int,
        heads: int,
        dropout: float,
    ) -> None:
        super().__init__()
        self.numeric_tokenizer = NumericFeatureTokenizer(action_numeric_dim, d_action)
        self.categorical_tokenizer = CategoricalFeatureTokenizer(action_cardinalities, d_action)
        self.mixer = FeatureTokenMixer(d_action, depth, heads, dropout)
        self.null_action = nn.Parameter(torch.zeros(d_action))

    def forward(
        self,
        action_numeric: torch.Tensor,
        action_masks: torch.Tensor,
        action_categorical: torch.Tensor,
    ) -> torch.Tensor:
        tokens = self._collect_tokens(
            action_numeric=action_numeric,
            action_masks=action_masks,
            action_categorical=action_categorical,
        )
        if tokens is None:
            batch_size, time_steps = action_numeric.shape[:2]
            return self.null_action.view(1, 1, -1).expand(batch_size, time_steps, -1)

        encoded = self.mixer(tokens)
        no_action_rows = _no_action_rows(action_numeric, action_masks, action_categorical)
        null_action = self.null_action.view(1, 1, -1).expand_as(encoded)
        return torch.where(no_action_rows.unsqueeze(-1), null_action, encoded)

    def _collect_tokens(
        self,
        *,
        action_numeric: torch.Tensor,
        action_masks: torch.Tensor,
        action_categorical: torch.Tensor,
    ) -> torch.Tensor | None:
        parts: list[torch.Tensor] = []
        numeric_tokens = self.numeric_tokenizer(action_numeric, action_masks)
        if numeric_tokens.size(-2) > 0:
            parts.append(numeric_tokens)
        categorical_tokens = self.categorical_tokenizer(action_categorical)
        if categorical_tokens.size(-2) > 0:
            parts.append(categorical_tokens)
        if not parts:
            return None
        return torch.cat(parts, dim=-2)


class CausalPredictor(nn.Module):
    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        self.config = config
        self.position_embeddings = nn.Parameter(
            torch.randn(1, config.context_length, config.d_state)
        )
        self.input_projection = nn.Linear(config.d_state + config.d_action, config.d_state)
        layer = nn.TransformerEncoderLayer(
            d_model=config.d_state,
            nhead=config.heads,
            dropout=config.dropout,
            batch_first=True,
            dim_feedforward=config.d_state * 4,
            activation="gelu",
        )
        self.encoder = nn.TransformerEncoder(layer, num_layers=config.depth)
        self.output_projection = nn.Linear(config.d_state, config.d_state)

    def forward(self, state_latents: torch.Tensor, action_latents: torch.Tensor) -> torch.Tensor:
        time_steps = state_latents.size(1)
        inputs = torch.cat([state_latents, action_latents], dim=-1)
        hidden = self.input_projection(inputs)
        hidden = hidden + self.position_embeddings[:, :time_steps]
        causal_mask = torch.triu(
            torch.ones(time_steps, time_steps, device=hidden.device, dtype=torch.bool),
            diagonal=1,
        )
        hidden = self.encoder(hidden, mask=causal_mask)
        return self.output_projection(hidden)


@dataclass
class ForwardPass:
    state_latents: torch.Tensor
    action_latents: torch.Tensor
    predicted_latents: torch.Tensor
    target_latents: torch.Tensor
    prediction_loss: torch.Tensor
    sigreg_loss: torch.Tensor
    total_loss: torch.Tensor


class StructuredStateJEPA(nn.Module):
    def __init__(
        self,
        *,
        config: ModelConfig,
        observation_numeric_dim: int,
        observation_mask_dim: int,
        observation_cardinalities: list[int],
        action_numeric_dim: int,
        action_mask_dim: int,
        action_cardinalities: list[int],
    ) -> None:
        super().__init__()
        self.config = config
        self.state_encoder = self._build_state_encoder(
            config=config,
            observation_numeric_dim=observation_numeric_dim,
            observation_mask_dim=observation_mask_dim,
            observation_cardinalities=observation_cardinalities,
        )
        self.action_encoder = self._build_action_encoder(
            config=config,
            action_numeric_dim=action_numeric_dim,
            action_mask_dim=action_mask_dim,
            action_cardinalities=action_cardinalities,
        )
        self.predictor = CausalPredictor(config)
        self.sigreg = SIGReg(knots=config.sigreg_knots, num_proj=config.sigreg_num_proj)

    @staticmethod
    def _build_state_encoder(
        *,
        config: ModelConfig,
        observation_numeric_dim: int,
        observation_mask_dim: int,
        observation_cardinalities: list[int],
    ) -> nn.Module:
        if config.encoder_type == "tokenized":
            return TokenizedStateEncoder(
                observation_numeric_dim=observation_numeric_dim,
                observation_cardinalities=observation_cardinalities,
                d_state=config.d_state,
                depth=config.feature_token_depth,
                heads=config.heads,
                dropout=config.dropout,
            )
        return FlatStateEncoder(
            observation_numeric_dim=observation_numeric_dim,
            observation_mask_dim=observation_mask_dim,
            observation_cardinalities=observation_cardinalities,
            d_state=config.d_state,
        )

    @staticmethod
    def _build_action_encoder(
        *,
        config: ModelConfig,
        action_numeric_dim: int,
        action_mask_dim: int,
        action_cardinalities: list[int],
    ) -> nn.Module:
        if config.encoder_type == "tokenized":
            return TokenizedActionEncoder(
                action_numeric_dim=action_numeric_dim,
                action_cardinalities=action_cardinalities,
                d_action=config.d_action,
                depth=config.feature_token_depth,
                heads=config.heads,
                dropout=config.dropout,
            )
        return FlatActionEncoder(
            action_numeric_dim=action_numeric_dim,
            action_mask_dim=action_mask_dim,
            action_cardinalities=action_cardinalities,
            d_action=config.d_action,
        )

    def encode_steps(self, batch: StepBatch) -> torch.Tensor:
        return self.state_encoder(
            batch.observation_numeric,
            batch.observation_masks,
            batch.observation_categorical,
        )

    def encode_actions(self, batch: StepBatch) -> torch.Tensor:
        return self.action_encoder(
            batch.action_numeric,
            batch.action_masks,
            batch.action_categorical,
        )

    def predict_next(self, z_ctx: torch.Tensor, a_ctx: torch.Tensor) -> torch.Tensor:
        return self.predictor(z_ctx, a_ctx)

    def forward(self, batch: StepBatch) -> ForwardPass:
        state_latents = self.encode_steps(batch)
        action_latents = self.encode_actions(batch)
        predicted = self.predict_next(state_latents[:, :-1], action_latents[:, :-1])
        targets = state_latents[:, 1:]
        mask = batch.valid_mask[:, 1:].unsqueeze(-1)
        prediction_error = (predicted - targets).pow(2)
        prediction_loss = (prediction_error * mask).sum() / mask.sum().clamp_min(1.0)
        sigreg_loss = self.sigreg(state_latents.transpose(0, 1))
        total_loss = prediction_loss + self.config.sigreg_lambda * sigreg_loss
        return ForwardPass(
            state_latents=state_latents,
            action_latents=action_latents,
            predicted_latents=predicted,
            target_latents=targets,
            prediction_loss=prediction_loss,
            sigreg_loss=sigreg_loss,
            total_loss=total_loss,
        )

    @torch.no_grad()
    def surprise_score(
        self, batch: StepBatch, forward_pass: ForwardPass | None = None
    ) -> torch.Tensor:
        if forward_pass is None:
            forward_pass = self.forward(batch)
        return (forward_pass.predicted_latents - forward_pass.target_latents).pow(2).mean(dim=-1)

    @torch.no_grad()
    def rollout(self, initial_steps: StepBatch, action_sequence: StepBatch) -> torch.Tensor:
        encoded_steps = self.encode_steps(initial_steps)
        encoded_actions = self.encode_actions(initial_steps)
        future_actions = self.action_encoder(
            action_sequence.action_numeric,
            action_sequence.action_masks,
            action_sequence.action_categorical,
        )

        history = encoded_steps.clone()
        action_history = encoded_actions.clone()
        predictions: list[torch.Tensor] = []
        for step_index in range(future_actions.size(1)):
            truncated_states = history[:, -self.config.context_length :]
            truncated_actions = action_history[:, -self.config.context_length :]
            predicted = self.predict_next(truncated_states, truncated_actions)[:, -1:]
            predictions.append(predicted)
            history = torch.cat([history, predicted], dim=1)
            action_history = torch.cat(
                [action_history, future_actions[:, step_index : step_index + 1]], dim=1
            )
        return (
            torch.cat(predictions, dim=1) if predictions else torch.zeros_like(encoded_steps[:, :0])
        )

    @torch.no_grad()
    def score_action_candidates(
        self,
        initial_steps: StepBatch,
        candidate_actions: ActionCandidateBatch,
        target_latent: torch.Tensor | None = None,
    ) -> torch.Tensor:
        batch_size, sample_count, horizon, _ = candidate_actions.action_numeric.shape
        flat_numeric = candidate_actions.action_numeric.reshape(
            batch_size * sample_count, horizon, -1
        )
        flat_masks = candidate_actions.action_masks.reshape(batch_size * sample_count, horizon, -1)
        flat_categorical = candidate_actions.action_categorical.reshape(
            batch_size * sample_count, horizon, -1
        )
        repeated_initial = StepBatch(
            observation_numeric=initial_steps.observation_numeric.repeat_interleave(
                sample_count, dim=0
            ),
            observation_masks=initial_steps.observation_masks.repeat_interleave(
                sample_count, dim=0
            ),
            observation_categorical=initial_steps.observation_categorical.repeat_interleave(
                sample_count, dim=0
            ),
            action_numeric=initial_steps.action_numeric.repeat_interleave(sample_count, dim=0),
            action_masks=initial_steps.action_masks.repeat_interleave(sample_count, dim=0),
            action_categorical=initial_steps.action_categorical.repeat_interleave(
                sample_count, dim=0
            ),
            auxiliary_numeric_targets=initial_steps.auxiliary_numeric_targets.repeat_interleave(
                sample_count, dim=0
            ),
            valid_mask=initial_steps.valid_mask.repeat_interleave(sample_count, dim=0),
        )
        action_batch = StepBatch(
            observation_numeric=torch.zeros(
                (batch_size * sample_count, horizon, 0), device=flat_numeric.device
            ),
            observation_masks=torch.zeros(
                (batch_size * sample_count, horizon, 0), device=flat_numeric.device
            ),
            observation_categorical=torch.zeros(
                (batch_size * sample_count, horizon, 0),
                dtype=torch.long,
                device=flat_numeric.device,
            ),
            action_numeric=flat_numeric,
            action_masks=flat_masks,
            action_categorical=flat_categorical,
            auxiliary_numeric_targets=torch.zeros(
                (batch_size * sample_count, horizon, 0), device=flat_numeric.device
            ),
            valid_mask=torch.ones((batch_size * sample_count, horizon), device=flat_numeric.device),
        )
        rollout = self.rollout(repeated_initial, action_batch)
        terminal = rollout[:, -1]
        if target_latent is None:
            target_latent = self.encode_steps(initial_steps)[:, -1].repeat_interleave(
                sample_count, dim=0
            )
        else:
            target_latent = target_latent.repeat_interleave(sample_count, dim=0)
        costs = F.mse_loss(terminal, target_latent, reduction="none").mean(dim=-1)
        return costs.view(batch_size, sample_count)


def _embedding_dim(cardinality: int) -> int:
    if cardinality <= 4:
        return 4
    if cardinality <= 16:
        return 8
    return 16


def _choose_attention_heads(model_dim: int, max_heads: int) -> int:
    for head_count in range(min(model_dim, max_heads), 0, -1):
        if model_dim % head_count == 0:
            return head_count
    return 1


def _no_action_rows(
    action_numeric: torch.Tensor,
    action_masks: torch.Tensor,
    action_categorical: torch.Tensor,
) -> torch.Tensor:
    no_numeric_signal = (action_numeric.abs().sum(dim=-1) + action_masks.abs().sum(dim=-1)) == 0
    if action_categorical.size(-1) == 0:
        return no_numeric_signal
    no_categorical_signal = action_categorical.eq(0).all(dim=-1)
    return no_numeric_signal & no_categorical_signal
