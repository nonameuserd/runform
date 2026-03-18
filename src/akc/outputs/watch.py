from __future__ import annotations

import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class WatchConfig:
    poll_interval_s: float = 1.0
    debounce_s: float = 0.75

    def __post_init__(self) -> None:
        if float(self.poll_interval_s) <= 0:
            raise ValueError("poll_interval_s must be > 0")
        if float(self.debounce_s) < 0:
            raise ValueError("debounce_s must be >= 0")


def _iter_files(paths: Sequence[Path]) -> Iterable[Path]:
    for p in paths:
        if p.is_dir():
            # Keep it dependency-free; recurse using pathlib.
            yield from (x for x in p.rglob("*") if x.is_file())
        elif p.is_file():
            yield p


def snapshot_mtime_ns(paths: Sequence[str | Path]) -> dict[str, int]:
    ps = [Path(p).expanduser() for p in paths]
    snap: dict[str, int] = {}
    for f in _iter_files(ps):
        try:
            st = f.stat()
        except OSError:
            continue
        m = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
        snap[str(f)] = m
    return snap


def has_changes(prev: dict[str, int], curr: dict[str, int]) -> bool:
    if prev.keys() != curr.keys():
        return True
    return any(prev.get(k) != v for k, v in curr.items())


def watch_for_changes(*, paths: Sequence[str | Path], cfg: WatchConfig) -> Iterable[None]:
    """Yield an event each time the watched paths change (debounced)."""

    last = snapshot_mtime_ns(paths)
    last_change_at: float | None = None
    while True:
        time.sleep(float(cfg.poll_interval_s))
        curr = snapshot_mtime_ns(paths)
        if has_changes(last, curr):
            last = curr
            last_change_at = time.time()
            continue
        if last_change_at is not None and (time.time() - last_change_at) >= float(cfg.debounce_s):
            last_change_at = None
            yield None
