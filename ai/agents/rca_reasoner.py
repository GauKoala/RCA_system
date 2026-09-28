"""RCA agent với evidence-bounded context và validation sau LLM."""

import json
import os
import re
import time
from typing import Any, Dict, List, Set

from langchain_ollama import OllamaLLM
from langchain_core.prompts import PromptTemplate

from ai.core.config import RCA_MODEL, log
from ai.core.state import AgentState, RCAOutput

_MAX_TOKENS = 4096  # Tăng để JSON không bị truncate khi context nhiều service
_RETRY_BACKOFF_SEC = 3
_MAX_EVIDENCE_CHARS = 1_200  # Giảm hơn để giảm tải prompt
_MAX_AGENT_SUMMARY_CHARS = 600
_MAX_ERRORS = 5


def _truncate(value: Any, limit: int) -> str:
    text = str(value or "")
    return text if len(text) <= limit else f"{text[:limit]}\n[truncated]"


def _normalize_rca_data(data: dict) -> dict:
    """Chuẩn hóa dữ liệu trước khi parse vào Pydantic:
    - confidence_score: float 0-1 → int 0-100
    - confidence_score: str → int
    """
    for item in data.get("coarse_grained_rca", []):
        score = item.get("confidence_score", 0)
        if isinstance(score, float) and score <= 1.0:
            item["confidence_score"] = int(score * 100)
        elif isinstance(score, str):
            try:
                score = float(score)
                item["confidence_score"] = int(score * 100) if score <= 1.0 else int(score)
            except (ValueError, TypeError):
                item["confidence_score"] = 0
        else:
            item["confidence_score"] = int(score)
    return data


def _parse_rca_json(raw: str):
    """Trích JSON từ output LLM, kể cả thinking/model markdown wrapper."""
    text = raw.strip()
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    if "```" in text:
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
        if text.lstrip().startswith("json"):
            text = text.lstrip()[4:]
    try:
        return RCAOutput(**_normalize_rca_data(json.loads(text.strip())))
    except Exception:
        pass
    try:
        start = text.index("{")
        end = text.rindex("}") + 1
        return RCAOutput(**_normalize_rca_data(json.loads(text[start:end])))
    except Exception:
        return None


def _build_rca_context(state: AgentState) -> tuple[Dict[str, Any], Set[str]]:
    """Giữ evidence cho đúng service incident và giới hạn payload vào LLM."""
    bundle = state.get("context_bundle", {}) or {}
    source_services = bundle.get("services", {}) or {}
    affected = set(state.get("affected_services", []))
    if not affected:
        affected = set(source_services)

    services: Dict[str, Dict[str, Any]] = {}
    valid_refs: Set[str] = set()
    for service in sorted(affected):
        raw = source_services.get(service, {}) or {}
        item: Dict[str, Any] = {"errors": list(raw.get("errors", []))[:_MAX_ERRORS]}
        for field in ("metrics", "logs", "dependencies"):
            value = _truncate(raw.get(field), _MAX_EVIDENCE_CHARS)
            item[field] = value
            if value:
                valid_refs.add(f"{service}.{field}")
        services[service] = item

    return {
        "services": services,
        "agent_summary": _truncate(bundle.get("agent_summary"), _MAX_AGENT_SUMMARY_CHARS),
        "collection_errors": list(bundle.get("collection_errors", []))[:_MAX_ERRORS],
        "evidence_quality": state.get("evidence_quality", "unavailable"),
    }, valid_refs


def _manual_rca() -> Dict[str, Any]:
    return {
        "coarse_grained_rca": [{"service_name": "unknown", "confidence_score": 0}],
        "fine_grained_rca": [{
            "indicator_type": "Insufficient evidence",
            "description": "Cannot determine root cause because required evidence is unavailable.",
            "evidence_refs": [],
        }],
        "remediation": ["Manual engineer investigation required."],
        "reasoning_steps": [],
    }


def _validate_rca(
    result: RCAOutput, affected_services: Set[str], valid_refs: Set[str], evidence_quality: str
) -> Dict[str, Any]:
    """Chỉ cho phép RCA khẳng định điều có evidence và service trong scope."""
    payload = result.model_dump()
    candidates = [
        item for item in payload["coarse_grained_rca"]
        if item["service_name"] in affected_services
    ]
    if not candidates:
        candidates = [{"service_name": "unknown", "confidence_score": 0}]

    if evidence_quality == "partial":
        for item in candidates:
            item["confidence_score"] = min(item["confidence_score"], 50)

    indicators = []
    for item in payload["fine_grained_rca"]:
        refs = [ref for ref in item.get("evidence_refs", []) if ref in valid_refs]
        if refs:
            item["evidence_refs"] = refs
            indicators.append(item)
    if not indicators:
        indicators = [{
            "indicator_type": "Insufficient evidence",
            "description": "RCA output did not contain verifiable evidence references.",
            "evidence_refs": [],
        }]

    payload["coarse_grained_rca"] = candidates
    payload["fine_grained_rca"] = indicators
    return payload


def rca_reasoner_agent(state: AgentState) -> Dict:
    log.info("RCA Reasoner dang suy luan nguyen nhan (goi LLM)...")
    evidence_quality = state.get("evidence_quality", "unavailable")
    if evidence_quality == "unavailable":
        # Không tiêu tốn LLM quota cho context đã biết là không đủ.
        return {"rca_output": _manual_rca(), "errors": state.get("errors", []) or []}

    context, valid_refs = _build_rca_context(state)
    errors = list(state.get("errors", []) or [])
    inputs = {
        "triage_reasons": "\n".join(state.get("triage_reasons", [])),
        "anomalous_templates": json.dumps(
            state.get("anomalous_templates", [])[:3], ensure_ascii=False, indent=2
        ),
        "investigator_data": json.dumps(context, ensure_ascii=False, indent=2),
        "affected_services": ", ".join(sorted(context["services"])),
        "valid_refs": ", ".join(sorted(valid_refs)) or "none",
        "evidence_quality": evidence_quality,
        "errors": "\n".join(errors[:_MAX_ERRORS]) if errors else "None.",
    }

    # Dùng OllamaLLM với format="json" để buộc model trả JSON hợp lệ.
    # ChatOpenAI wrapper không truyền được format param xuống Ollama native API.
    ollama_host = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
    llm = OllamaLLM(
        model=RCA_MODEL,
        base_url=ollama_host,
        format="json",
        temperature=0.1,
        num_predict=_MAX_TOKENS,
    )

    _SYSTEM = (
        "You are an AIOps RCA expert. Output ONLY a JSON object with these exact keys: "
        "coarse_grained_rca (list of {{service_name, confidence_score}}), "
        "fine_grained_rca (list of {{indicator_type, description, evidence_refs}}), "
        "remediation (list of strings), reasoning_steps (list of strings). "
        "evidence_refs must only use: {valid_refs}. "
        "Root-cause service must be one of: {affected_services}. "
        "Do not add any text outside the JSON object."
    )
    prompt = PromptTemplate.from_template(
        _SYSTEM + "\n\n"
        "Evidence quality: {evidence_quality}\n"
        "Triage reasons: {triage_reasons}\n"
        "Anomalous patterns: {anomalous_templates}\n"
        "Investigator data: {investigator_data}\n"
        "Errors: {errors}"
    )
    retry_prompt = PromptTemplate.from_template(
        "Output ONLY valid JSON with keys: coarse_grained_rca, fine_grained_rca, remediation, reasoning_steps. "
        "Valid refs: {valid_refs}. Affected: {affected_services}. Quality: {evidence_quality}.\n"
        "Triage: {triage_reasons}\nEvidence: {investigator_data}\nErrors: {errors}\n"
        "Anomalous: {anomalous_templates}"
    )

    parsed = None
    try:
        raw = (prompt | llm).invoke(inputs)
        log.debug("RCA raw response: %s", raw[:500])
        parsed = _parse_rca_json(raw)
        if parsed is None:
            raise ValueError("RCA response was not valid schema-compliant JSON")
    except Exception as exc:
        errors.append(f"RCA initial attempt failed: {exc}")
        log.warning("RCA initial attempt failed: %s; retrying once in %ss", exc, _RETRY_BACKOFF_SEC)
        time.sleep(_RETRY_BACKOFF_SEC)
        try:
            raw = (retry_prompt | llm).invoke(inputs)
            log.debug("RCA retry raw response: %s", raw[:500])
            parsed = _parse_rca_json(raw)
            if parsed is None:
                raise ValueError("RCA retry response was not valid schema-compliant JSON")
        except Exception as retry_exc:
            errors.append(f"RCA retry failed: {retry_exc}")
            log.error("RCA retry failed: %s", retry_exc)

    if parsed is None:
        rca_output = _manual_rca()
    else:
        rca_output = _validate_rca(
            parsed,
            set(context["services"]),
            valid_refs,
            evidence_quality,
        )
    return {"rca_output": rca_output, "errors": errors}
