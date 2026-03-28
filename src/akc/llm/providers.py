from __future__ import annotations

import importlib
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

from akc.compile.interfaces import LLMBackend, LLMRequest, LLMResponse, TenantRepoScope

from .config import LlmRuntimeConfig
from .http import post_json
from .usage import usage_from_anthropic_response, usage_from_gemini_response, usage_from_openai_response

_TRAVERSAL_LIKE = re.compile(r"(?:\.\./|\\\.\.\\|/\\\.\.|\\\.\./)")
_ABSOLUTE_PATH_LIKE = re.compile(r"(?m)^(--- |\+\+\+ )(?:/|~)")


def _validate_patch_safety(*, text: str) -> None:
    if _TRAVERSAL_LIKE.search(text or ""):
        raise ValueError("refusing response with path traversal-like sequences")
    if _ABSOLUTE_PATH_LIKE.search(text or ""):
        raise ValueError("refusing response with absolute/tilde paths in diff headers")


class OfflineLlmBackend(LLMBackend):
    def __init__(self, *, config: LlmRuntimeConfig) -> None:
        self.runtime_config = config

    def complete(
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        _ = request
        if stage == "system_design":
            return LLMResponse(
                text=json.dumps(
                    {
                        "spec_version": 1,
                        "tenant_id": scope.tenant_id,
                        "repo_id": scope.repo_id,
                        "system_id": "offline-system",
                        "services": [{"name": "compile-controller", "role": "orchestrator"}],
                    },
                    sort_keys=True,
                ),
                raw=None,
                usage=None,
            )
        if stage == "orchestration_spec":
            return LLMResponse(
                text=json.dumps(
                    {
                        "spec_version": 1,
                        "tenant_id": scope.tenant_id,
                        "repo_id": scope.repo_id,
                        "state_machine": {
                            "initial_state": "start",
                            "transitions": [{"from": "start", "event": "compile", "to": "done"}],
                        },
                    },
                    sort_keys=True,
                ),
                raw=None,
                usage=None,
            )
        if stage == "agent_coordination":
            return LLMResponse(
                text=json.dumps(
                    {
                        "spec_version": 1,
                        "tenant_id": scope.tenant_id,
                        "repo_id": scope.repo_id,
                        "agent_roles": {"planner": {"tools": ["llm.complete"]}},
                        "coordination_graph": {"nodes": ["planner"], "edges": []},
                    },
                    sort_keys=True,
                ),
                raw=None,
                usage=None,
            )
        if stage == "deployment_config":
            return LLMResponse(
                text=json.dumps(
                    {
                        "docker_compose": {"services": {"app": {"read_only": True}}},
                        "kubernetes": {"securityContext": {"runAsNonRoot": True}},
                    },
                    sort_keys=True,
                ),
                raw=None,
                usage=None,
            )
        text = "\n".join(
            [
                "--- /dev/null",
                "+++ b/src/akc_compiled.py",
                "@@ -0,0 +1,1 @@",
                f"+# compiled stage={stage} tenant={scope.tenant_id} repo={scope.repo_id}",
                "",
                "--- /dev/null",
                "+++ b/tests/test_akc_compiled.py",
                "@@ -0,0 +1,3 @@",
                "+def test_compiled_smoke():",
                "+    assert True",
                "+",
                "",
            ]
        )
        return LLMResponse(text=text, raw=None, usage=None)


def _joined_text(parts: Sequence[str]) -> str:
    return "\n".join(p for p in (str(x).strip() for x in parts) if p)


class OpenAiLlmBackend(LLMBackend):
    def __init__(self, *, config: LlmRuntimeConfig) -> None:
        self.runtime_config = config

    def complete(self, *, scope: TenantRepoScope, stage: str, request: LLMRequest) -> LLMResponse:
        url = f"{str(self.runtime_config.base_url or 'https://api.openai.com').rstrip('/')}/v1/responses"
        input_items: list[dict[str, Any]] = []
        for msg in request.messages:
            role = "developer" if msg.role == "system" else ("user" if msg.role == "tool" else msg.role)
            input_items.append({"role": role, "content": msg.content})
        body: dict[str, Any] = {
            "model": self.runtime_config.model,
            "input": input_items,
            "max_output_tokens": request.max_output_tokens,
            "temperature": float(request.temperature),
            "metadata": {"tenant_id": scope.tenant_id, "repo_id": scope.repo_id, "stage": stage},
        }
        raw = post_json(
            url=url,
            body=body,
            headers={"Authorization": f"Bearer {self.runtime_config.api_key}"},
            timeout_s=self.runtime_config.timeout_s,
            max_retries=self.runtime_config.max_retries,
        )
        text_raw = raw.get("output_text")
        text = str(text_raw).strip() if isinstance(text_raw, str) else ""
        if not text:
            collected: list[str] = []
            output = raw.get("output")
            if isinstance(output, list):
                for item in output:
                    if not isinstance(item, Mapping):
                        continue
                    content = item.get("content")
                    if not isinstance(content, list):
                        continue
                    for part in content:
                        if not isinstance(part, Mapping):
                            continue
                        if str(part.get("type", "")).strip() == "output_text" and isinstance(part.get("text"), str):
                            collected.append(str(part.get("text")))
            text = _joined_text(collected)
        if not text:
            raise ValueError("OpenAI response did not include output text")
        _validate_patch_safety(text=text)
        return LLMResponse(text=text, raw=dict(raw), usage=usage_from_openai_response(raw))


class AnthropicLlmBackend(LLMBackend):
    def __init__(self, *, config: LlmRuntimeConfig) -> None:
        self.runtime_config = config

    def complete(self, *, scope: TenantRepoScope, stage: str, request: LLMRequest) -> LLMResponse:
        url = f"{str(self.runtime_config.base_url or 'https://api.anthropic.com').rstrip('/')}/v1/messages"
        system_parts: list[str] = []
        messages: list[dict[str, Any]] = []
        for msg in request.messages:
            if msg.role in {"system", "tool"}:
                system_parts.append(msg.content)
                continue
            role = "assistant" if msg.role == "assistant" else "user"
            messages.append({"role": role, "content": msg.content})
        body: dict[str, Any] = {
            "model": self.runtime_config.model,
            "messages": messages,
            "max_tokens": int(request.max_output_tokens or 2048),
            "temperature": float(request.temperature),
            "metadata": {"tenant_id": scope.tenant_id, "repo_id": scope.repo_id, "stage": stage},
        }
        if system_parts:
            body["system"] = _joined_text(system_parts)
        raw = post_json(
            url=url,
            body=body,
            headers={
                "x-api-key": str(self.runtime_config.api_key),
                "anthropic-version": "2023-06-01",
            },
            timeout_s=self.runtime_config.timeout_s,
            max_retries=self.runtime_config.max_retries,
        )
        content = raw.get("content")
        collected: list[str] = []
        if isinstance(content, list):
            for part in content:
                if not isinstance(part, Mapping):
                    continue
                if str(part.get("type", "")).strip() == "text" and isinstance(part.get("text"), str):
                    collected.append(str(part.get("text")))
        text = _joined_text(collected)
        if not text:
            raise ValueError("Anthropic response did not include text content")
        _validate_patch_safety(text=text)
        return LLMResponse(text=text, raw=dict(raw), usage=usage_from_anthropic_response(raw))


class GeminiLlmBackend(LLMBackend):
    def __init__(self, *, config: LlmRuntimeConfig) -> None:
        self.runtime_config = config

    def complete(self, *, scope: TenantRepoScope, stage: str, request: LLMRequest) -> LLMResponse:
        _ = (scope, stage)
        model_name = self.runtime_config.model
        if not model_name.startswith("models/"):
            model_name = f"models/{model_name}"
        url = f"{str(self.runtime_config.base_url or 'https://generativelanguage.googleapis.com').rstrip('/')}/v1beta/{model_name}:generateContent"
        system_parts: list[str] = []
        contents: list[dict[str, Any]] = []
        for msg in request.messages:
            if msg.role in {"system", "tool"}:
                system_parts.append(msg.content)
                continue
            role = "model" if msg.role == "assistant" else "user"
            contents.append({"role": role, "parts": [{"text": msg.content}]})
        body: dict[str, Any] = {
            "contents": contents,
            "generationConfig": {
                "temperature": float(request.temperature),
                "maxOutputTokens": int(request.max_output_tokens or 2048),
                "stopSequences": list(request.stop or ()),
            },
        }
        if system_parts:
            body["systemInstruction"] = {"parts": [{"text": _joined_text(system_parts)}]}
        raw = post_json(
            url=url,
            body=body,
            headers={"x-goog-api-key": str(self.runtime_config.api_key)},
            timeout_s=self.runtime_config.timeout_s,
            max_retries=self.runtime_config.max_retries,
        )
        collected: list[str] = []
        candidates = raw.get("candidates")
        if isinstance(candidates, list):
            for candidate in candidates:
                if not isinstance(candidate, Mapping):
                    continue
                content = candidate.get("content")
                if not isinstance(content, Mapping):
                    continue
                parts = content.get("parts")
                if not isinstance(parts, list):
                    continue
                for part in parts:
                    if isinstance(part, Mapping) and isinstance(part.get("text"), str):
                        collected.append(str(part.get("text")))
        text = _joined_text(collected)
        if not text:
            raise ValueError("Gemini response did not include text content")
        _validate_patch_safety(text=text)
        return LLMResponse(text=text, raw=dict(raw), usage=usage_from_gemini_response(raw))


def load_custom_llm_backend(*, class_path: str) -> LLMBackend:
    raw = str(class_path).strip()
    if not raw:
        raise ValueError("llm backend class path must be non-empty")
    if ":" in raw:
        module_name, class_name = raw.split(":", 1)
    else:
        module_name, _, class_name = raw.rpartition(".")
    module_name = module_name.strip()
    class_name = class_name.strip()
    if not module_name or not class_name:
        raise ValueError("invalid llm backend class path; expected '<module>:<Class>' or '<module>.<Class>'")
    mod = importlib.import_module(module_name)
    cls = getattr(mod, class_name, None)
    if cls is None:
        raise ValueError(f"llm backend class not found: {raw}")
    inst = cls()
    if not isinstance(inst, LLMBackend):
        raise ValueError(f"llm backend does not implement LLMBackend: {raw}")
    return inst
