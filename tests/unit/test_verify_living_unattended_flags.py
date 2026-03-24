"""Verify CLI passes lease/single-writer flags through to living unattended checks."""

from __future__ import annotations

from akc.cli import _build_parser


def test_verify_parser_accepts_lease_flags() -> None:
    parser = _build_parser()
    args = parser.parse_args(
        [
            "verify",
            "--tenant-id",
            "t1",
            "--repo-id",
            "r1",
            "--outputs-root",
            "/tmp/out",
            "--living-unattended",
            "--lease-backend",
            "k8s",
            "--lease-namespace",
            "akc-autopilot",
            "--expect-replicas",
            "3",
        ]
    )
    assert args.living_unattended is True
    assert args.lease_backend == "k8s"
    assert args.lease_namespace == "akc-autopilot"
    assert args.expect_replicas == 3
