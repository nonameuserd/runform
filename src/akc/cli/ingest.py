from __future__ import annotations

import argparse
from pathlib import Path

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

from .common import configure_logging, env


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
            raise SystemExit(
                "Missing OpenAI API key. Provide --openai-api-key or set AKC_OPENAI_API_KEY."
            )
        return OpenAICompatibleEmbedder(base_url=base_url, model=model, api_key=api_key)
    if embedder_name == "gemini":
        api_key = args.gemini_api_key or env("AKC_GEMINI_API_KEY")
        if api_key is None:
            raise SystemExit(
                "Missing Gemini API key. Provide --gemini-api-key or set AKC_GEMINI_API_KEY."
            )
        model = args.gemini_model or env("AKC_GEMINI_EMBED_MODEL") or "text-embedding-004"
        base_url = (
            args.gemini_base_url
            or env("AKC_GEMINI_BASE_URL")
            or "https://generativelanguage.googleapis.com"
        )
        return GeminiEmbedder(api_key=api_key, model=model, base_url=base_url)
    raise SystemExit(f"Unknown embedder: {embedder_name}")


def _require_slack_token(args: argparse.Namespace) -> str:
    token = args.slack_token or env("AKC_SLACK_TOKEN")
    if token is None:
        raise SystemExit("Missing Slack token. Provide --slack-token or set AKC_SLACK_TOKEN.")
    return token


def cmd_ingest(args: argparse.Namespace) -> int:
    configure_logging(verbose=args.verbose)

    tenant_id: str = args.tenant_id
    connector: str = args.connector
    input_value: str = args.input

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
        connector_options["include_bot_answers"] = (
            "true" if args.slack_include_bot_answers else "false"
        )

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
        results = vector_store.similarity_search_by_vector(
            tenant_id=tenant_id, query_vector=qvec, k=int(args.k)
        )
        print("")
        print(f"Top {len(results)} results for query: {args.query!r}")
        for i, r in enumerate(results, start=1):
            src = r.document.metadata.get("source")
            st = r.document.metadata.get("source_type")
            print(f"{i:>2}. score={r.score:.4f} source_type={st} source={src} id={r.document.id}")

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
