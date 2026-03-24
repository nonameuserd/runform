"""Shared doc-id coverage contract for knowledge-domain fixture integration tests.

Any new ``doc_id`` added to ``tests/fixtures/knowledge_domains/corpus_manifest.json``
must be intentionally mapped to at least one scenario constant below and consumed by
integration tests through ``_load_documents`` / ``_load_corpus_documents`` subsets.
"""

from __future__ import annotations

from typing import Final

KD_DOC_IDS_SECURITY_NETWORK_BASE: Final[frozenset[str]] = frozenset(
    {
        "kd-sec-net-docs-firewall",
        "kd-sec-net-docs-destructive",
    }
)

KD_DOC_IDS_SECURITY_NETWORK_MESSAGING: Final[frozenset[str]] = frozenset(
    {
        "kd-sec-net-msg-incident",
    }
)

KD_DOC_IDS_SECURITY_NETWORK_OPENAPI: Final[frozenset[str]] = frozenset(
    {
        "kd-sec-net-openapi-callbacks",
    }
)

KD_DOC_IDS_CONFLICTING_NORMS: Final[frozenset[str]] = frozenset(
    {
        "kd-conflict-retention-v1",
        "kd-conflict-retention-v2",
    }
)

KD_DOC_IDS_DOC_DERIVED: Final[frozenset[str]] = frozenset(
    {
        "kd-doc-derived-controls",
    }
)

KD_DOC_IDS_PAYMENTS_PCI_DOCS: Final[frozenset[str]] = frozenset(
    {
        "kd-payments-pci-docs-handling",
    }
)

KD_DOC_IDS_PAYMENTS_PCI_OPENAPI: Final[frozenset[str]] = frozenset(
    {
        "kd-payments-pci-openapi-callbacks",
    }
)

KD_DOC_IDS_PAYMENTS_PCI_MESSAGING: Final[frozenset[str]] = frozenset(
    {
        "kd-payments-pci-msg-incident",
    }
)

KD_DOC_IDS_PLATFORM_MIGRATIONS: Final[frozenset[str]] = frozenset(
    {
        "kd-platform-migrations-docs-destructive",
    }
)

KD_DOC_IDS_ALLOWLIST: Final[frozenset[str]] = frozenset().union(
    KD_DOC_IDS_SECURITY_NETWORK_BASE,
    KD_DOC_IDS_SECURITY_NETWORK_MESSAGING,
    KD_DOC_IDS_SECURITY_NETWORK_OPENAPI,
    KD_DOC_IDS_CONFLICTING_NORMS,
    KD_DOC_IDS_DOC_DERIVED,
    KD_DOC_IDS_PAYMENTS_PCI_DOCS,
    KD_DOC_IDS_PAYMENTS_PCI_OPENAPI,
    KD_DOC_IDS_PAYMENTS_PCI_MESSAGING,
    KD_DOC_IDS_PLATFORM_MIGRATIONS,
)
