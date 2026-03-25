"""Internal delivery domain types (mirror v1 artifacts under ``.akc/delivery/``)."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, TypeAlias, cast

from akc.artifacts.schemas import SchemaKind
from akc.artifacts.validate import validate_obj

DeliveryPlatform: TypeAlias = Literal["web", "ios", "android"]
ReleaseLane: TypeAlias = Literal["beta", "store"]
ReleaseMode: TypeAlias = Literal["beta", "store", "both"]

RecipientDeliveryStatus: TypeAlias = Literal[
    "pending",
    "invited",
    "installed",
    "active",
    "failed",
    "blocked",
]

SessionPhase: TypeAlias = Literal[
    "accepted",
    "compiling",
    "building",
    "packaging",
    "distributing",
    "releasing",
    "completed",
    "failed",
    "blocked",
]

PipelineStageStatus: TypeAlias = Literal[
    "not_started",
    "pending",
    "in_progress",
    "completed",
    "failed",
    "skipped",
    "blocked",
]

ChannelStatus: TypeAlias = Literal[
    "not_started",
    "not_applicable",
    "pending",
    "in_progress",
    "completed",
    "failed",
    "skipped",
    "blocked",
]

RecipientActivationProofStatus: TypeAlias = Literal["pending", "partial", "satisfied", "blocked"]
ProviderProofStatus: TypeAlias = Literal["pending", "satisfied", "not_applicable"]
AppProofStatus: TypeAlias = Literal["pending", "satisfied"]

SessionActivationRollupStatus: TypeAlias = Literal["pending", "partial", "complete", "blocked"]

StoreReleaseOverallStatus: TypeAlias = Literal[
    "not_started",
    "pending",
    "promotion_requested",
    "in_progress",
    "submitted",
    "live",
    "failed",
    "blocked",
]

StorePlatformLaneStatus: TypeAlias = Literal[
    "not_started",
    "not_applicable",
    "pending",
    "in_progress",
    "submitted",
    "live",
    "failed",
    "blocked",
]


class DeliveryModelError(ValueError):
    """Raised when a delivery artifact dict cannot be mapped into typed models."""


def _assert_safe_delivery_id(delivery_id: str) -> None:
    if ".." in delivery_id or "/" in delivery_id or "\\" in delivery_id:
        raise DeliveryModelError("invalid delivery_id")
    if not re.fullmatch(r"[a-zA-Z0-9_.-]+", delivery_id):
        raise DeliveryModelError("invalid delivery_id")


def _require_valid_artifact(*, obj: Any, kind: SchemaKind, version: int) -> None:
    issues = validate_obj(obj=obj, kind=kind, version=version)
    if issues:
        msg = "; ".join(f"{issue.path}: {issue.message}" for issue in issues[:8])
        raise DeliveryModelError(f"invalid artifact kind={kind!r} version={version}: {msg}")


def _as_str(v: Any, *, ctx: str) -> str:
    if not isinstance(v, str):
        raise DeliveryModelError(f"{ctx}: expected str, got {type(v).__name__}")
    return v


def _as_int_or_none(v: Any, *, ctx: str) -> int | None:
    if v is None:
        return None
    if not isinstance(v, int):
        raise DeliveryModelError(f"{ctx}: expected int or null, got {type(v).__name__}")
    return v


def _as_str_or_none(v: Any, *, ctx: str) -> str | None:
    if v is None:
        return None
    return _as_str(v, ctx=ctx)


def _as_mapping(v: Any, *, ctx: str) -> dict[str, Any]:
    if not isinstance(v, dict):
        raise DeliveryModelError(f"{ctx}: expected object, got {type(v).__name__}")
    return cast(dict[str, Any], v)


def _as_list_str(v: Any, *, ctx: str) -> list[str]:
    if not isinstance(v, list):
        raise DeliveryModelError(f"{ctx}: expected array, got {type(v).__name__}")
    out: list[str] = []
    for i, x in enumerate(v):
        out.append(_as_str(x, ctx=f"{ctx}[{i}]"))
    return out


def _platforms_tuple(raw: Sequence[str], *, ctx: str) -> tuple[DeliveryPlatform, ...]:
    out: list[DeliveryPlatform] = []
    for i, p in enumerate(raw):
        if p not in ("web", "ios", "android"):
            raise DeliveryModelError(f"{ctx}[{i}]: invalid platform {p!r}")
        out.append(cast(DeliveryPlatform, p))
    return tuple(out)


@dataclass(frozen=True, slots=True)
class ActivationProof:
    provider_proof: ProviderProofStatus
    app_proof: AppProofStatus
    status: RecipientActivationProofStatus

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> ActivationProof:
        d = _as_mapping(row, ctx="activation_proof")
        return cls(
            provider_proof=cast(ProviderProofStatus, _as_str(d["provider_proof"], ctx="provider_proof")),
            app_proof=cast(AppProofStatus, _as_str(d["app_proof"], ctx="app_proof")),
            status=cast(
                RecipientActivationProofStatus,
                _as_str(d["status"], ctx="activation_proof.status"),
            ),
        )


@dataclass(frozen=True, slots=True)
class RecipientSpec:
    """Named recipient target for a delivery session (invite + platform scope)."""

    email: str
    platforms: tuple[DeliveryPlatform, ...]
    invite_token_id: str | None
    status: RecipientDeliveryStatus

    @classmethod
    def from_per_recipient_row(
        cls,
        *,
        email: str,
        row: Mapping[str, Any],
        default_platforms: Sequence[DeliveryPlatform],
    ) -> RecipientSpec:
        d = _as_mapping(row, ctx=f"per_recipient[{email}]")
        plat_raw = d.get("platforms")
        if plat_raw is None:
            platforms = tuple(default_platforms)
        else:
            platforms = _platforms_tuple(_as_list_str(plat_raw, ctx="platforms"), ctx="platforms")
        return cls(
            email=_as_str(email, ctx="email"),
            platforms=platforms,
            invite_token_id=_as_str_or_none(d.get("invite_token_id"), ctx="invite_token_id"),
            status=cast(RecipientDeliveryStatus, _as_str(d["status"], ctx="status")),
        )

    @classmethod
    def from_recipients_sidecar_row(cls, row: Mapping[str, Any]) -> RecipientSpec:
        d = _as_mapping(row, ctx="recipients_sidecar_row")
        email = _as_str(d["email"], ctx="email")
        platforms = _platforms_tuple(_as_list_str(d["platforms"], ctx="platforms"), ctx="platforms")
        return cls(
            email=email,
            platforms=platforms,
            invite_token_id=_as_str_or_none(d.get("invite_token_id"), ctx="invite_token_id"),
            status=cast(RecipientDeliveryStatus, _as_str(d["status"], ctx="status")),
        )


@dataclass(frozen=True, slots=True)
class DeliveryRequestV1:
    delivery_id: str
    request_text: str
    platforms: tuple[DeliveryPlatform, ...]
    recipients: tuple[str, ...]
    release_mode: ReleaseMode
    app_stack: str
    delivery_version: str
    derived_intent_ref: dict[str, Any] | None
    required_accounts: tuple[str, ...]
    parsed: dict[str, Any]
    required_human_inputs: tuple[dict[str, Any], ...]
    created_at_unix_ms: int

    @classmethod
    def from_artifact(cls, obj: dict[str, Any], *, version: int = 1) -> DeliveryRequestV1:
        _require_valid_artifact(obj=obj, kind="delivery_request", version=version)
        d = _as_mapping(obj, ctx="delivery_request")
        rh_raw = d.get("required_human_inputs")
        if not isinstance(rh_raw, list):
            raise DeliveryModelError("required_human_inputs: expected array")
        human_rows: list[dict[str, Any]] = []
        for i, row in enumerate(rh_raw):
            human_rows.append(_as_mapping(row, ctx=f"required_human_inputs[{i}]"))
        return cls(
            delivery_id=_as_str(d["delivery_id"], ctx="delivery_id"),
            request_text=_as_str(d["request_text"], ctx="request_text"),
            platforms=_platforms_tuple(_as_list_str(d["platforms"], ctx="platforms"), ctx="platforms"),
            recipients=tuple(_as_list_str(d["recipients"], ctx="recipients")),
            release_mode=cast(ReleaseMode, _as_str(d["release_mode"], ctx="release_mode")),
            app_stack=_as_str(d["app_stack"], ctx="app_stack"),
            delivery_version=_as_str(d["delivery_version"], ctx="delivery_version"),
            derived_intent_ref=cast(dict[str, Any] | None, d.get("derived_intent_ref")),
            required_accounts=tuple(_as_list_str(d["required_accounts"], ctx="required_accounts")),
            parsed=dict(_as_mapping(d["parsed"], ctx="parsed")),
            required_human_inputs=tuple(human_rows),
            created_at_unix_ms=int(d["created_at_unix_ms"]),
        )


@dataclass(frozen=True, slots=True)
class PipelineStage:
    status: PipelineStageStatus
    started_at_unix_ms: int | None
    completed_at_unix_ms: int | None
    error: str | None
    run_id: str | None

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any], *, ctx: str) -> PipelineStage:
        d = _as_mapping(row, ctx=ctx)
        return cls(
            status=cast(PipelineStageStatus, _as_str(d["status"], ctx=f"{ctx}.status")),
            started_at_unix_ms=_as_int_or_none(d.get("started_at_unix_ms"), ctx=f"{ctx}.started_at_unix_ms"),
            completed_at_unix_ms=_as_int_or_none(
                d.get("completed_at_unix_ms"),
                ctx=f"{ctx}.completed_at_unix_ms",
            ),
            error=_as_str_or_none(d.get("error"), ctx=f"{ctx}.error"),
            run_id=_as_str_or_none(d.get("run_id"), ctx=f"{ctx}.run_id"),
        )


@dataclass(frozen=True, slots=True)
class ChannelState:
    status: ChannelStatus
    details: dict[str, Any]

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any], *, ctx: str) -> ChannelState:
        d = _as_mapping(row, ctx=ctx)
        details_raw = d.get("details")
        if details_raw is None:
            details: dict[str, Any] = {}
        else:
            details = _as_mapping(details_raw, ctx=f"{ctx}.details")
        return cls(
            status=cast(ChannelStatus, _as_str(d["status"], ctx=f"{ctx}.status")),
            details=details,
        )


@dataclass(frozen=True, slots=True)
class PlatformChannels:
    beta: ChannelState
    store: ChannelState

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any], *, ctx: str) -> PlatformChannels:
        d = _as_mapping(row, ctx=ctx)
        ch = _as_mapping(d["channels"], ctx=f"{ctx}.channels")
        return cls(
            beta=ChannelState.from_mapping(ch["beta"], ctx=f"{ctx}.channels.beta"),
            store=ChannelState.from_mapping(ch["store"], ctx=f"{ctx}.channels.store"),
        )


@dataclass(frozen=True, slots=True)
class PerRecipientSessionState:
    status: RecipientDeliveryStatus
    invite_token_id: str | None
    platforms: tuple[DeliveryPlatform, ...] | None
    activation_proof: ActivationProof

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any], *, ctx: str) -> PerRecipientSessionState:
        d = _as_mapping(row, ctx=ctx)
        plat_raw = d.get("platforms")
        platforms: tuple[DeliveryPlatform, ...] | None
        if plat_raw is None:
            platforms = None
        else:
            platforms = _platforms_tuple(_as_list_str(plat_raw, ctx="platforms"), ctx="platforms")
        return cls(
            status=cast(RecipientDeliveryStatus, _as_str(d["status"], ctx="status")),
            invite_token_id=_as_str_or_none(d.get("invite_token_id"), ctx="invite_token_id"),
            platforms=platforms,
            activation_proof=ActivationProof.from_mapping(
                _as_mapping(d["activation_proof"], ctx="activation_proof"),
            ),
        )


@dataclass(frozen=True, slots=True)
class SessionActivationRollup:
    status: SessionActivationRollupStatus
    recipients_total: int
    recipients_provider_satisfied: int
    recipients_fully_satisfied: int

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> SessionActivationRollup:
        d = _as_mapping(row, ctx="activation_proof.rollup")
        return cls(
            status=cast(SessionActivationRollupStatus, _as_str(d["status"], ctx="rollup.status")),
            recipients_total=int(d["recipients_total"]),
            recipients_provider_satisfied=int(d["recipients_provider_satisfied"]),
            recipients_fully_satisfied=int(d["recipients_fully_satisfied"]),
        )


@dataclass(frozen=True, slots=True)
class StorePlatformLane:
    status: StorePlatformLaneStatus
    external_ref: str | None
    notes: str | None

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any], *, ctx: str) -> StorePlatformLane:
        d = _as_mapping(row, ctx=ctx)
        return cls(
            status=cast(StorePlatformLaneStatus, _as_str(d["status"], ctx=f"{ctx}.status")),
            external_ref=_as_str_or_none(d.get("external_ref"), ctx=f"{ctx}.external_ref"),
            notes=_as_str_or_none(d.get("notes"), ctx=f"{ctx}.notes"),
        )


@dataclass(frozen=True, slots=True)
class StoreReleaseState:
    status: StoreReleaseOverallStatus
    active_promotion_lane: ReleaseLane | None
    last_promotion_requested_at_unix_ms: int | None
    ios: StorePlatformLane
    android: StorePlatformLane

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> StoreReleaseState:
        d = _as_mapping(row, ctx="store_release")
        lane_raw = d.get("active_promotion_lane")
        lane: ReleaseLane | None
        if lane_raw is None:
            lane = None
        else:
            s = _as_str(lane_raw, ctx="active_promotion_lane")
            if s not in ("beta", "store"):
                raise DeliveryModelError(f"active_promotion_lane: invalid lane {s!r}")
            lane = cast(ReleaseLane, s)
        return cls(
            status=cast(StoreReleaseOverallStatus, _as_str(d["status"], ctx="store_release.status")),
            active_promotion_lane=lane,
            last_promotion_requested_at_unix_ms=_as_int_or_none(
                d.get("last_promotion_requested_at_unix_ms"),
                ctx="last_promotion_requested_at_unix_ms",
            ),
            ios=StorePlatformLane.from_mapping(_as_mapping(d["ios"], ctx="store_release.ios"), ctx="ios"),
            android=StorePlatformLane.from_mapping(
                _as_mapping(d["android"], ctx="store_release.android"),
                ctx="android",
            ),
        )


@dataclass(frozen=True, slots=True)
class DeliverySession:
    delivery_id: str
    session_phase: SessionPhase
    release_mode: ReleaseMode
    platforms: tuple[DeliveryPlatform, ...]
    compile_run_id: str | None
    delivery_version: str
    pipeline: dict[str, PipelineStage]
    per_platform: dict[str, PlatformChannels]
    per_recipient: dict[str, PerRecipientSessionState]
    activation_proof: SessionActivationRollup
    store_release: StoreReleaseState
    created_at_unix_ms: int
    updated_at_unix_ms: int

    @classmethod
    def from_artifact(cls, obj: dict[str, Any], *, version: int = 1) -> DeliverySession:
        _require_valid_artifact(obj=obj, kind="delivery_session", version=version)
        d = _as_mapping(obj, ctx="delivery_session")
        plat_list = _platforms_tuple(_as_list_str(d["platforms"], ctx="platforms"), ctx="platforms")
        pipe_raw = _as_mapping(d["pipeline"], ctx="pipeline")
        pipeline = {
            k: PipelineStage.from_mapping(cast(Mapping[str, Any], v), ctx=f"pipeline.{k}") for k, v in pipe_raw.items()
        }
        pp_raw = _as_mapping(d["per_platform"], ctx="per_platform")
        per_platform: dict[str, PlatformChannels] = {}
        for pf, v in pp_raw.items():
            per_platform[pf] = PlatformChannels.from_mapping(
                cast(Mapping[str, Any], v),
                ctx=f"per_platform[{pf}]",
            )
        pr_raw = _as_mapping(d["per_recipient"], ctx="per_recipient")
        per_recipient: dict[str, PerRecipientSessionState] = {}
        for email, v in pr_raw.items():
            per_recipient[email] = PerRecipientSessionState.from_mapping(
                cast(Mapping[str, Any], v),
                ctx=f"per_recipient[{email}]",
            )
        return cls(
            delivery_id=_as_str(d["delivery_id"], ctx="delivery_id"),
            session_phase=cast(SessionPhase, _as_str(d["session_phase"], ctx="session_phase")),
            release_mode=cast(ReleaseMode, _as_str(d["release_mode"], ctx="release_mode")),
            platforms=plat_list,
            compile_run_id=_as_str_or_none(d.get("compile_run_id"), ctx="compile_run_id"),
            delivery_version=_as_str(d["delivery_version"], ctx="delivery_version"),
            pipeline=pipeline,
            per_platform=per_platform,
            per_recipient=per_recipient,
            activation_proof=SessionActivationRollup.from_mapping(
                _as_mapping(d["activation_proof"], ctx="activation_proof"),
            ),
            store_release=StoreReleaseState.from_mapping(_as_mapping(d["store_release"], ctx="store_release")),
            created_at_unix_ms=int(d["created_at_unix_ms"]),
            updated_at_unix_ms=int(d["updated_at_unix_ms"]),
        )

    def recipient_specs(self) -> dict[str, RecipientSpec]:
        """Project per-recipient session rows into :class:`RecipientSpec` (fills missing platform lists)."""

        out: dict[str, RecipientSpec] = {}
        for email, st in self.per_recipient.items():
            row: dict[str, Any] = {
                "status": st.status,
                "invite_token_id": st.invite_token_id,
            }
            if st.platforms is not None:
                row["platforms"] = list(st.platforms)
            out[email] = RecipientSpec.from_per_recipient_row(
                email=email,
                row=row,
                default_platforms=self.platforms,
            )
        return out


@dataclass(frozen=True, slots=True)
class PlatformBuildSpec:
    """Per-platform packaging inputs for distribution adapters (non-secret metadata only)."""

    tenant_id: str
    repo_id: str
    delivery_id: str
    platform: DeliveryPlatform
    delivery_version: str
    release_lanes: tuple[ReleaseLane, ...]
    compile_run_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.tenant_id).strip():
            raise DeliveryModelError("tenant_id must be non-empty")
        if not str(self.repo_id).strip():
            raise DeliveryModelError("repo_id must be non-empty")
        if not str(self.delivery_version).strip():
            raise DeliveryModelError("delivery_version must be non-empty")
        _assert_safe_delivery_id(self.delivery_id)
        for lane in self.release_lanes:
            if lane not in ("beta", "store"):
                raise DeliveryModelError(f"invalid release lane: {lane!r}")
