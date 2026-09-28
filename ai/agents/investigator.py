"""LLM Investigator Agent với tool loop và evidence safety-net.

LLM tự quyết định thứ tự/câu hỏi điều tra và nhìn thấy kết quả tool ở vòng kế
tiếp. Sau đó safety-net chỉ bù các nguồn evidence tối thiểu mà agent chưa gọi,
hoặc chạy khi LLM unavailable; nó không thay thế agent.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI

from ai.core.config import INVESTIGATOR_MODEL, log
from ai.core.state import AgentState
from ai.core.tools import (
    get_dependency_map,
    get_prometheus_metrics,
)

_MAX_TOOL_ROUNDS = 2
_TOOLS = {
    "get_prometheus_metrics": (get_prometheus_metrics, "metrics"),
    "get_dependency_map": (get_dependency_map, "dependencies"),
}


def _invoke(tool: Any, args: Dict[str, Any], label: str) -> Tuple[str, str | None]:
    try:
        return str(tool.invoke(args)), None
    except Exception as exc:
        message = f"{label} failed: {type(exc).__name__}: {exc}"
        log.warning(message)
        return "", message


def _new_service_evidence() -> Dict[str, Any]:
    return {"metrics": "", "logs": "", "dependencies": "", "errors": []}


def _run_agent_tool_loop(
    services: List[str], reasons: List[str], evidence: Dict[str, Dict[str, Any]]
) -> Tuple[str, List[str]]:
    """Cho LLM điều tra tối đa hai vòng và trả lại kết luận ngắn của agent."""
    llm = ChatOpenAI(
        model=INVESTIGATOR_MODEL,
        api_key="ollama",
        base_url=os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434") + "/v1",
        temperature=0,
        max_tokens=700,
    ).bind_tools([get_prometheus_metrics, get_dependency_map])

    triage_text = "\n".join(reasons)
    messages = [
        SystemMessage(
            content=(
                "You are an AIOps investigator. Investigate only the affected services. "
                "Use the available tools to gather evidence, inspect returned evidence, "
                "then give a concise factual summary. Do not invent observations."
            )
        ),
        HumanMessage(content=(
            f"Affected services: {', '.join(services)}\n"
            f"Triage reasons:\n{triage_text}\n\n"
            "Baseline evidence already collected in parallel:\n"
            f"{json.dumps(evidence, ensure_ascii=False)}\n\n"
            "Use tools only for focused follow-up questions; do not repeat a "
            "baseline query unless a more specific query is needed."
        )),
    ]
    errors: List[str] = []
    summary = ""

    for _ in range(_MAX_TOOL_ROUNDS):
        response = llm.invoke(messages)
        messages.append(response)
        calls = response.tool_calls or []
        if not calls:
            summary = str(response.content)
            break

        for call in calls:
            name = call["name"]
            args = dict(call.get("args", {}))
            service = args.get("service")
            call_id = call["id"]

            # Không cho model truy vấn service ngoài scope incident hiện tại.
            if service not in evidence or name not in _TOOLS:
                message = f"Tool {name} rejected: service outside incident scope or unknown tool."
                errors.append(message)
                messages.append(ToolMessage(content=message, tool_call_id=call_id))
                continue

            tool, evidence_key = _TOOLS[name]
            result, error = _invoke(tool, args, name)
            evidence[service][evidence_key] = result
            if error:
                evidence[service]["errors"].append(error)
                errors.append(f"[{service}] {error}")
                result = error
            messages.append(ToolMessage(content=result, tool_call_id=call_id))

    # Nếu round cuối vẫn gọi tool, yêu cầu một lượt tổng hợp cuối để Agent thực
    # sự quan sát các ToolMessage vừa nhận thay vì chỉ trả evidence thô.
    if not summary:
        final_response = llm.invoke(messages)
        summary = str(final_response.content)

    return summary, errors


def _collect_minimum_evidence_parallel(
    services: List[str], evidence: Dict[str, Dict[str, Any]]
) -> List[str]:
    """Lấy bộ evidence tối thiểu song song, trước khi gọi LLM agent."""
    errors: List[str] = []
    requests = []
    for service in services:
        requests.extend(
            (service, name, args)
            for name, args in (
            ("get_prometheus_metrics", {"service": service, "time_range_mins": 5}),
            ("get_dependency_map", {"service": service}),
            )
        )

    # I/O-bound sources như Prometheus/Elasticsearch không phụ thuộc nhau.
    # Chạy song song làm latency gần bằng tool chậm nhất, thay vì tổng ba tool.
    with ThreadPoolExecutor(max_workers=min(len(requests), 12)) as executor:
        futures = {}
        for service, name, args in requests:
            tool, evidence_key = _TOOLS[name]
            future = executor.submit(_invoke, tool, args, name)
            futures[future] = (service, evidence_key)

        for future in as_completed(futures):
            service, evidence_key = futures[future]
            result, error = future.result()
            evidence[service][evidence_key] = result
            if error:
                evidence[service]["errors"].append(error)
                errors.append(f"[{service}] {error}")
    return errors


def _retry_missing_evidence(
    services: List[str], evidence: Dict[str, Dict[str, Any]]
) -> List[str]:
    """Chỉ retry những nguồn baseline thất bại hoặc còn rỗng sau agent loop."""
    errors: List[str] = []
    for service in services:
        for name, args in (
            ("get_prometheus_metrics", {"service": service, "time_range_mins": 5}),
            ("get_dependency_map", {"service": service}),
        ):
            tool, evidence_key = _TOOLS[name]
            if evidence[service][evidence_key]:
                continue
            result, error = _invoke(tool, args, name)
            evidence[service][evidence_key] = result
            if error:
                evidence[service]["errors"].append(error)
                errors.append(f"[{service}] {error}")
    return errors


def investigator_agent(state: AgentState) -> Dict:
    services = list(dict.fromkeys(state.get("affected_services", [])))
    if not services:  # tương thích state cũ trong lúc rollout
        services = list(dict.fromkeys(
            anomaly.get("service")
            for anomaly in state.get("anomalous_templates", [])
            if anomaly.get("service")
        ))

    inherited_errors = list(state.get("errors", []) or [])
    evidence = {service: _new_service_evidence() for service in services}
    collection_errors: List[str] = []
    agent_summary = ""

    if services:
        # Fast path: evidence nền được lấy song song và không chờ LLM.
        collection_errors.extend(_collect_minimum_evidence_parallel(services, evidence))
        try:
            agent_summary, agent_errors = _run_agent_tool_loop(
                services, state.get("triage_reasons", []), evidence
            )
            collection_errors.extend(agent_errors)
        except Exception as exc:
            # LLM failure không làm incident mất evidence; fallback ở dưới sẽ chạy.
            message = f"Investigator agent unavailable: {type(exc).__name__}: {exc}"
            log.warning(message)
            collection_errors.append(message)

        collection_errors.extend(_retry_missing_evidence(services, evidence))

    source_count = len(services) * 3
    failed_count = sum(len(item["errors"]) for item in evidence.values())
    if not services or failed_count == source_count:
        quality = "unavailable"
    elif failed_count:
        quality = "partial"
    else:
        quality = "complete"

    bundle = {
        "services": evidence,
        "agent_summary": agent_summary,
        "collection_errors": collection_errors,
        "evidence_quality": quality,
    }
    log.info("Investigator completed %d services (%s evidence).", len(services), quality)
    return {
        "context_bundle": bundle,
        "evidence_quality": quality,
        "errors": inherited_errors + collection_errors,
    }
