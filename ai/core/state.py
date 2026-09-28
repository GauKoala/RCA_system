from typing import Any, Dict, List, Optional, TypedDict
from pydantic import BaseModel, Field

class AgentState(TypedDict):
    event_payload: Dict[str, Any]
    incident_id: str
    is_incident: bool
    triage_reasons: List[str]
    iforest_anomalies: List[Dict]     # [{service, score, is_anomaly, feature_snapshot}]
    sequence_anomalies: List[Dict]    # [{service, unknown_ngrams_count, sequence_len}]
    affected_services: List[str]      # hợp các service bị iForest hoặc sequence báo động
    # Kept for backward compat with investigator/rca prompts
    anomalous_templates: List[Dict]

    # Dữ liệu từ Investigator
    context_bundle: Dict[str, Any]
    evidence_quality: str             # complete | partial | unavailable
    errors: List[str]
    
    # Kết quả từ RCA Reasoner
    rca_output: Optional[Dict[str, Any]]
    
    # Kết quả từ Responder
    runbook_report: str

class SuspectedService(BaseModel):
    service_name: str = Field(description="Tên dịch vụ nghi ngờ là nguyên nhân gốc")
    confidence_score: int = Field(ge=0, le=100, description="Điểm số tin cậy từ 0 đến 100")

class RootCauseIndicator(BaseModel):
    indicator_type: str = Field(description="Loại chỉ báo (ví dụ: Metric vượt ngưỡng, Stack Trace, Log lỗi)")
    description: str = Field(description="Chi tiết chỉ báo nguyên nhân gốc")
    evidence_refs: List[str] = Field(
        default_factory=list,
        description="Các ref evidence hỗ trợ, ví dụ auth-service.metrics hoặc auth-service.logs",
    )

class RCAOutput(BaseModel):
    coarse_grained_rca: List[SuspectedService] = Field(description="Danh sách xếp hạng Top-k các dịch vụ nghi ngờ là nguyên nhân gốc")
    fine_grained_rca: List[RootCauseIndicator] = Field(description="Chỉ báo nguyên nhân gốc cụ thể")
    remediation: List[str] = Field(description="Đề xuất các bước khắc phục sự cố (Runbook/Remediation)")
    reasoning_steps: List[str] = Field(description="Lập luận giải thích cơ chế lan truyền lỗi (Reasoning steps)")
