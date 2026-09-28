import uuid
from typing import Dict

import redis

from ai.core.config import REDIS_URL, log
from ai.core.iforest_detector import IsolationForestDetector
from ai.core.sequence_detector import SequenceAnomalyDetector
from ai.core.state import AgentState


def _redis() -> redis.Redis:
    """Redis connection với decode_responses=False (iForest cần bytes cho model)."""
    return redis.Redis.from_url(REDIS_URL, decode_responses=False, socket_timeout=10)


def _redis_text() -> redis.Redis:
    """Redis connection decode_responses=True (SequenceDetector dùng string)."""
    return redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10)


def triage_agent(state: AgentState) -> Dict:
    log.info("🔍 Triage đang phân tích Window hiện tại...")
    event = state.get("event_payload", {})
    services_data = event.get("services", {})

    is_incident = False
    reasons = []
    iforest_anomalies = []
    sequence_anomalies = []
    affected_services = set()

    # Khởi tạo detectors (1 lần per triage call)
    r_bytes = _redis()
    r_text  = _redis_text()
    iforest  = IsolationForestDetector(r_bytes)
    seq_det  = SequenceAnomalyDetector(r_text)

    for svc, data in services_data.items():
        # Một incident ở service A không được làm service B ngừng học baseline.
        service_is_incident = False

        # ================================================================ #
        #  KHỐI 1: Isolation Forest (Count + Metric feature vector)
        # ================================================================ #
        feature_vector = data.get("feature_vector", {})
        if feature_vector:
            try:
                is_anom, score = iforest.fit_or_predict(svc, feature_vector)
                if is_anom:
                    is_incident = True
                    service_is_incident = True
                    affected_services.add(svc)
                    iforest_anomalies.append({
                        "service":          svc,
                        "score":            round(score, 6),
                        "is_anomaly":       True,
                        "feature_snapshot": {
                            k: v for k, v in list(feature_vector.items())[:10]
                        },  # top 10 features để tránh payload quá lớn
                    })
                    reasons.append(
                        f"[{svc}] iForest anomaly: score={score:.4f} "
                        f"(total_logs={feature_vector.get('total_logs', '?')}, "
                        f"error_rate={feature_vector.get('error_rate', 0):.1%})"
                    )
                else:
                    log.debug("iForest [%s] normal (score=%.4f)", svc, score)
            except Exception:
                log.exception("Lỗi iForest cho service '%s'", svc)

        # ================================================================ #
        #  KHỐI 2: N-gram Sequence Anomaly
        # ================================================================ #
        sequence = data.get("log_sequence", [])
        if sequence:
            try:
                unknown_ngrams = seq_det.detect(svc, sequence)
                if unknown_ngrams:
                    is_incident = True
                    service_is_incident = True
                    affected_services.add(svc)
                    sequence_anomalies.append({
                        "service":             svc,
                        "unknown_ngrams_count": len(unknown_ngrams),
                        "sequence_len":         len(sequence),
                    })
                    reasons.append(
                        f"[{svc}] Sequence anomaly: {len(unknown_ngrams)} N-gram lạ "
                        f"trong chuỗi {len(sequence)} sự kiện."
                    )

                # Anti-poisoning: chỉ học khi chính service này bình thường.
                if not service_is_incident:
                    seq_det.learn(svc, sequence)

            except Exception:
                log.exception("Lỗi Sequence Detector cho service '%s'", svc)

    if is_incident:
        log.warning("🚨 Triage phát hiện sự cố.")
        for r in reasons:
            log.warning("   → %s", r)
    else:
        log.info("✅ Window bình thường (Tổng log: %s).", event.get("total_logs"))

    # anomalous_templates: giữ để backward compat với investigator/rca (chuyển từ iforest_anomalies)
    anomalous_templates = [
        {
            "service":  a["service"],
            "template": f"iForest anomaly (score={a['score']:.4f})",
            "count":    int(a["feature_snapshot"].get("total_logs", 0)),
            "samples":  [],
        }
        for a in iforest_anomalies
    ]

    return {
        "incident_id":        str(uuid.uuid4()),
        "is_incident":        is_incident,
        "triage_reasons":     reasons,
        "iforest_anomalies":  iforest_anomalies,
        "sequence_anomalies": sequence_anomalies,
        "affected_services":  sorted(affected_services),
        "anomalous_templates": anomalous_templates,
    }
