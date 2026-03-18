from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.compile.interfaces import TenantRepoScope
from akc.outputs.emitters import FileSystemEmitter, JsonManifestEmitter
from akc.outputs.models import OutputArtifact, OutputBundle


def test_filesystem_emitter_writes_scoped_paths(tmp_path: Path) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    bundle = OutputBundle(
        scope=scope,
        name="demo",
        artifacts=(
            OutputArtifact.from_text(path="a.txt", text="hello"),
            OutputArtifact.from_text(path="nested/b.txt", text="world"),
        ),
    )

    em = FileSystemEmitter()
    written = em.emit(bundle=bundle, root=tmp_path)

    assert (tmp_path / "t1" / "repo1" / "a.txt").read_text(encoding="utf-8") == "hello"
    assert (tmp_path / "t1" / "repo1" / "nested" / "b.txt").read_text(encoding="utf-8") == "world"
    assert (tmp_path / "t1" / "repo1").exists()
    assert set(written) >= {
        (tmp_path / "t1" / "repo1" / "a.txt").resolve(),
        (tmp_path / "t1" / "repo1" / "nested" / "b.txt").resolve(),
    }


def test_artifact_path_rejects_escape() -> None:
    with pytest.raises(ValueError, match="must not contain '\\.\\.'"):
        OutputArtifact.from_text(path="../x.txt", text="nope")


def test_manifest_emitter_writes_manifest(tmp_path: Path) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    art = OutputArtifact.from_text(path="out.txt", text="ok")
    bundle = OutputBundle(scope=scope, name="demo", artifacts=(art,))

    em = JsonManifestEmitter()
    written = em.emit(bundle=bundle, root=tmp_path)

    mpath = tmp_path / "t1" / "repo1" / "manifest.json"
    assert mpath in written or mpath.resolve() in written
    payload = json.loads(mpath.read_text(encoding="utf-8"))
    assert payload["tenant_id"] == "t1"
    assert payload["repo_id"] == "repo1"
    assert payload["name"] == "demo"
    assert payload["artifacts"][0]["path"] == "out.txt"
    assert payload["artifacts"][0]["sha256"] == art.sha256_hex()
