from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Literal

from akc.ingest.chunking import ChunkingConfig
from akc.ingest.connectors.messaging.slack import SlackMessagingClient
from akc.ingest.embedding import (
    GeminiEmbedder,
    HashEmbedder,
    OpenAICompatibleEmbedder,
    embed_query,
)
from akc.ingest.pipeline import (
    IngestionStateStore,
    build_vector_store,
    default_state_path,
    run_ingest,
)
from akc.utils.fingerprint import stable_json_fingerprint

from .common import configure_logging, env
from .profile_defaults import (
    resolve_developer_role_profile,
    resolve_ingest_profile_defaults,
    resolve_optional_project_string,
)
from .project_config import load_akc_project_config


def _build_embedder(args: argparse.Namespace):  # type: ignore[no-untyped-def]
    embedder_name: str = args.embedder
    if embedder_name == "none":
        return None
    if embedder_name == "hash":
        return HashEmbedder()
    if embedder_name == "openai":
        base_url = args.openai_base_url or env("AKC_OPENAI_BASE_URL") or "https://api.openai.com"
        model = args.openai_model or env("AKC_OPENAI_EMBED_MODEL") or "text-embedding-3-large"
        api_key = args.openai_api_key or env("AKC_OPENAI_API_KEY")
        if api_key is None:
            raise SystemExit("Missing OpenAI API key. Provide --openai-api-key or set AKC_OPENAI_API_KEY.")
        return OpenAICompatibleEmbedder(base_url=base_url, model=model, api_key=api_key)
    if embedder_name == "gemini":
        api_key = args.gemini_api_key or env("AKC_GEMINI_API_KEY")
        if api_key is None:
            raise SystemExit("Missing Gemini API key. Provide --gemini-api-key or set AKC_GEMINI_API_KEY.")
        model = args.gemini_model or env("AKC_GEMINI_EMBED_MODEL") or "text-embedding-004"
        base_url = args.gemini_base_url or env("AKC_GEMINI_BASE_URL") or "https://generativelanguage.googleapis.com"
        return GeminiEmbedder(api_key=api_key, model=model, base_url=base_url)
    raise SystemExit(f"Unknown embedder: {embedder_name}")


def _require_slack_token(args: argparse.Namespace) -> str:
    token = args.slack_token or env("AKC_SLACK_TOKEN")
    if token is None:
        raise SystemExit("Missing Slack token. Provide --slack-token or set AKC_SLACK_TOKEN.")
    return token


def cmd_ingest(args: argparse.Namespace) -> int:
    configure_logging(verbose=args.verbose)

    cwd = Path.cwd()
    proj = load_akc_project_config(cwd)
    tenant_r = resolve_optional_project_string(
        cli_value=getattr(args, "tenant_id", None),
        env_key="AKC_TENANT_ID",
        file_value=proj.tenant_id if proj is not None else None,
        env=os.environ,
    )
    if tenant_r.value is None:
        raise SystemExit(
            "Missing tenant id: provide --tenant-id, set AKC_TENANT_ID, or add tenant_id to .akc/project.json"
        )
    tenant_id: str = tenant_r.value
    connector: str = args.connector
    input_value: str = args.input
    developer_role_profile = resolve_developer_role_profile(
        cli_value=getattr(args, "developer_role_profile", None),
        cwd=cwd,
        env=os.environ,
        project=proj,
    ).value
    assertion_index_root_raw = getattr(args, "assertion_index_root", None)
    ingest_profile = resolve_ingest_profile_defaults(
        profile=developer_role_profile,
        tenant_id=tenant_id,
        cwd=Path.cwd(),
        no_index=bool(args.no_index),
        index_backend=str(args.index_backend),
        embedder=str(args.embedder),
        sqlite_path=str(args.sqlite_path) if args.sqlite_path is not None else None,
        assertion_index_root=(str(assertion_index_root_raw) if assertion_index_root_raw is not None else None),
    )
    args.index_backend = ingest_profile["index_backend"].value
    args.embedder = ingest_profile["embedder"].value
    if args.sqlite_path is None:
        args.sqlite_path = ingest_profile["sqlite_path"].value
    if getattr(args, "assertion_index_root", None) is None:
        args.assertion_index_root = ingest_profile["assertion_index_root"].value

    connector_options: dict[str, str] = {}
    if connector == "slack":
        connector_options["token"] = _require_slack_token(args)
        if args.slack_oldest is not None:
            connector_options["oldest"] = str(args.slack_oldest)
        if args.slack_latest is not None:
            connector_options["latest"] = str(args.slack_latest)
        connector_options["history_limit"] = str(int(args.slack_history_limit))
        connector_options["max_threads"] = str(int(args.slack_max_threads))
        connector_options["max_answers"] = str(int(args.slack_max_answers))
        connector_options["include_bot_answers"] = "true" if args.slack_include_bot_answers else "false"

    embedder = _build_embedder(args)
    vector_store = None
    if not args.no_index:
        sqlite_path = args.sqlite_path or env("AKC_SQLITE_INDEX_PATH")
        pg_dsn = args.pg_dsn or env("AKC_PGVECTOR_DSN")
        pg_dimension = args.pg_dimension
        if pg_dimension is None:
            dim_env = env("AKC_PGVECTOR_DIMENSION")
            pg_dimension = int(dim_env) if dim_env is not None else None
        pg_table = args.pg_table or env("AKC_PGVECTOR_TABLE") or "akc_documents"
        vector_store = build_vector_store(
            backend=args.index_backend,
            sqlite_path=sqlite_path,
            pg_dsn=pg_dsn,
            pg_dimension=pg_dimension,
            pg_table=pg_table,
        )
        if embedder is None:
            raise SystemExit("--embedder is required unless --no-index is set")

    base_dir = Path(args.state_dir).expanduser() if args.state_dir is not None else None
    state_path = (
        Path(args.state_path).expanduser()
        if args.state_path is not None
        else default_state_path(tenant_id=tenant_id, connector=connector, base_dir=base_dir)
    )
    state_store = IngestionStateStore(state_path) if not args.no_state else None

    rust_ingest_mode: Literal["cli", "pyo3"] = "pyo3" if getattr(args, "rust_ingest_mode", "cli") == "pyo3" else "cli"

    assertion_index_root = getattr(args, "assertion_index_root", None)
    assertion_index_repo_id = getattr(args, "assertion_index_repo_id", None)
    assertion_index_max = int(getattr(args, "assertion_index_max_per_batch", 256))

    result = run_ingest(
        connector_name=connector,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        input_value=input_value,
        connector_options=connector_options or None,
        chunking=(
            None
            if args.no_chunking
            else ChunkingConfig(
                chunk_size_chars=int(args.chunk_size_chars),
                overlap_chars=int(args.chunk_overlap_chars),
            )
        ),
        disable_chunking=bool(args.no_chunking),
        embedder=embedder,
        vector_store=vector_store,
        index_backend=None if vector_store is None else str(args.index_backend),
        state_store=state_store,
        incremental=not args.no_incremental,
        on_source_error="skip" if args.skip_sources_with_errors else "raise",
        use_rust_ingest_docs=bool(args.use_rust_ingest_docs),
        rust_ingest_min_bytes=args.rust_ingest_min_bytes,
        rust_ingest_mode=rust_ingest_mode,
        assertion_index_scope_root=Path(assertion_index_root).expanduser()
        if assertion_index_root is not None
        else None,
        assertion_index_repo_id=assertion_index_repo_id,
        assertion_index_max_per_batch=assertion_index_max,
    )

    print("Ingest complete.")
    print(f"  tenant_id: {result.tenant_id}")
    print(f"  connector: {result.connector}")
    print(f"  index: {result.index_backend or 'none'}")
    if result.state_path is not None:
        print(f"  state: {result.state_path}")
    s = result.stats
    print(
        "  stats:"
        f" sources_seen={s.sources_seen}"
        f" sources_skipped={s.sources_skipped}"
        f" docs_fetched={s.documents_fetched}"
        f" chunks={s.documents_chunked}"
        f" embedded={s.documents_embedded}"
        f" indexed={s.documents_indexed}"
        f" elapsed_s={s.elapsed_s:.2f}"
    )

    if args.query is not None:
        if embedder is None or vector_store is None:
            raise SystemExit("--query requires an embedder and an index backend")
        qvec = embed_query(embedder, args.query)
        results = vector_store.similarity_search_by_vector(tenant_id=tenant_id, query_vector=qvec, k=int(args.k))
        print("")
        print(f"Top {len(results)} results for query: {args.query!r}")
        for i, r in enumerate(results, start=1):
            src = r.document.metadata.get("source")
            st = r.document.metadata.get("source_type")
            print(f"{i:>2}. score={r.score:.4f} source_type={st} source={src} id={r.document.id}")

    decisions_payload: dict[str, object] = {
        "developer_role_profile": developer_role_profile,
        "tenant_id": tenant_id,
        "connector": connector,
        "resolved": {
            "index_backend": {
                "value": ingest_profile["index_backend"].value,
                "source": ingest_profile["index_backend"].source,
            },
            "embedder": {
                "value": ingest_profile["embedder"].value,
                "source": ingest_profile["embedder"].source,
            },
            "sqlite_path": {
                "value": ingest_profile["sqlite_path"].value,
                "source": ingest_profile["sqlite_path"].source,
            },
            "assertion_index_root": {
                "value": ingest_profile["assertion_index_root"].value,
                "source": ingest_profile["assertion_index_root"].source,
            },
        },
    }
    decisions_payload["fingerprint_sha256"] = stable_json_fingerprint(decisions_payload)
    decisions_root: Path
    assertion_root_raw = ingest_profile["assertion_index_root"].value
    if isinstance(assertion_root_raw, str) and assertion_root_raw.strip():
        decisions_root = Path(assertion_root_raw).expanduser() / tenant_id / ".akc" / "ingest"
    elif result.state_path is not None:
        decisions_root = Path(result.state_path).expanduser().parent
    else:
        decisions_root = Path.cwd() / ".akc" / "ingest" / tenant_id
    decisions_root.mkdir(parents=True, exist_ok=True)
    decisions_path = decisions_root / f"{connector}.developer_profile_decisions.json"
    decisions_path.write_text(json.dumps(decisions_payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"  developer_profile_decisions: {decisions_path}")

    return 0


def cmd_slack_list_channels(args: argparse.Namespace) -> int:
    configure_logging(verbose=args.verbose)
    token = _require_slack_token(args)
    client = SlackMessagingClient(token=token)
    channels = list(client.list_channels())
    for c in channels:
        name = c.name or ""
        print(f"{c.id}\t{name}")
    return 0
