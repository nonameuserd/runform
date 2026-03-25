"""Build embedders for MCP index queries (mirrors ingest CLI choices, without argparse cycles)."""

from __future__ import annotations

import os

from akc.ingest.embedding import Embedder, GeminiEmbedder, HashEmbedder, OpenAICompatibleEmbedder


def build_embedder(
    *,
    name: str,
    openai_api_key: str | None,
    openai_base_url: str | None,
    openai_model: str | None,
    gemini_api_key: str | None,
    gemini_base_url: str | None,
    gemini_model: str | None,
    hash_dimension: int | None,
) -> Embedder | None:
    """Return an :class:`~akc.ingest.embedding.Embedder` or ``None`` when indexing is disabled."""

    embedder_name = str(name).strip().lower()
    if embedder_name in ("none", ""):
        return None
    if embedder_name == "hash":
        dim = int(hash_dimension) if hash_dimension is not None else 64
        return HashEmbedder(dimension=dim)
    if embedder_name == "openai":
        base_url = openai_base_url or os.environ.get("AKC_OPENAI_BASE_URL") or "https://api.openai.com"
        model = openai_model or os.environ.get("AKC_OPENAI_EMBED_MODEL") or "text-embedding-3-large"
        api_key = openai_api_key or os.environ.get("AKC_OPENAI_API_KEY")
        if api_key is None:
            raise ValueError("OpenAI embedder requires --openai-api-key or AKC_OPENAI_API_KEY")
        return OpenAICompatibleEmbedder(base_url=base_url, model=model, api_key=api_key)
    if embedder_name == "gemini":
        api_key = gemini_api_key or os.environ.get("AKC_GEMINI_API_KEY")
        if api_key is None:
            raise ValueError("Gemini embedder requires --gemini-api-key or AKC_GEMINI_API_KEY")
        model = gemini_model or os.environ.get("AKC_GEMINI_EMBED_MODEL") or "text-embedding-004"
        base_url = (
            gemini_base_url or os.environ.get("AKC_GEMINI_BASE_URL") or "https://generativelanguage.googleapis.com"
        )
        return GeminiEmbedder(api_key=api_key, model=model, base_url=base_url)
    raise ValueError(f"Unknown embedder: {name!r}")
