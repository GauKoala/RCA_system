import uuid
import time
from typing import Dict
from ai.core.state import AgentState
from ai.core.config import log, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from ai.core.storage import insert_incident, insert_feedback, insert_rca_analysis
import requests

def responder_agent(state: AgentState) -> Dict:
    log.info("📝 Responder đang format báo cáo...")
    start_ts = time.monotonic()
    rca = state.get("rca_output", {})
    incident_id = state.get("incident_id", str(uuid.uuid4()))
    
    services = state.get("affected_services", []) or [
        anom.get("service", "unknown") for anom in state.get("anomalous_templates", [])
    ]
    service_str = ", ".join(list(set(services))) if services else "unknown"
    
    coarse_grained = rca.get('coarse_grained_rca', [])
    fine_grained = rca.get('fine_grained_rca', [])
    evidence_quality = state.get("evidence_quality", "unavailable")

    # Không biến kết quả từ evidence rỗng thành một RCA có vẻ chắc chắn.
    if evidence_quality == "unavailable":
        rca = dict(rca)
        rca["remediation"] = ["Manual engineer investigation required."]
        rca["reasoning_steps"] = []
        coarse_grained = [{"service_name": "unknown", "confidence_score": 0}]
        fine_grained = [{
            "indicator_type": "Insufficient evidence",
            "description": "Không thể thu thập metric, log hoặc dependency evidence; cần điều tra thủ công.",
            "evidence_refs": [],
        }]
    elif evidence_quality == "partial":
        # Defense in depth: RCA đã cap confidence, Responder cap lại trước khi
        # hiển thị/lưu để không có báo cáo partial-evidence quá tự tin.
        coarse_grained = [
            {**svc, "confidence_score": min(svc.get("confidence_score", 0), 50)}
            for svc in coarse_grained
        ]
    
    # Lấy top 1 service làm root_cause text cho DB compatibility
    top_service_name = coarse_grained[0].get("service_name", "unknown") if coarse_grained else "unknown"
    top_confidence = coarse_grained[0].get("confidence_score", 0) if coarse_grained else 0

    insert_incident(
        incident_id=incident_id,
        root_cause=top_service_name,
        confidence=top_confidence,
        evidence=fine_grained,
        remediation=rca.get('remediation', []),
        service=service_str,
        event_payload=state.get("event_payload", {})
    )

    # ── Lưu đầy đủ vào bảng rca_analysis cho dashboard hàng tuần ──────────
    duration_ms = int((time.monotonic() - start_ts) * 1000)
    insert_rca_analysis(
        incident_id=incident_id,
        service=service_str,
        root_cause=top_service_name,
        confidence=top_confidence,
        evidence=fine_grained,
        remediation=rca.get('remediation', []),
        reasoning_steps=rca.get('reasoning_steps', []),
        triage_reasons=state.get('triage_reasons', []),
        errors=state.get('errors', []),
        duration_ms=duration_ms,
    )
    # ───────────────────────────────────────────────────────────────────────
    
    # Rút gọn ID cho đẹp (ví dụ: 550E8400)
    short_id = str(incident_id)[:8].upper()
    
    report = f"""🚨 <b>SỰ CỐ HỆ THỐNG</b> 🚨
🆔 <b>ID:</b> <code>{short_id}</code>
🎯 <b>Nguồn lỗi:</b> <b>{top_service_name}</b> (Độ tự tin: {top_confidence}%)

🧠 <b>Lý do (Lập luận của AI):</b>
"""
    # Sử dụng trực tiếp lập luận của AI (Reasoning Steps)
    if rca.get('reasoning_steps'):
        for r in rca['reasoning_steps']:
            report += f"  • {r}\n"
    else:
        report += "  • Không có lập luận nào được đưa ra.\n"
        
    report += "\n🛠 <b>Hướng khắc phục:</b>\n"
    for rm in rca.get('remediation', []):
        report += f"  • {rm}\n"

    report += "\n➖➖➖➖➖➖➖➖\n<i>Đánh giá: [✅ Đúng] | [❌ Sai]</i>"
    
    # Gửi cảnh báo qua Telegram
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            log.info("Thực hiện gửi webhook cảnh báo tới Telegram...")
            telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": report,
                "parse_mode": "HTML"
            }
            # Cắt ngắn report nếu vượt quá giới hạn 4096 ký tự của Telegram
            if len(payload["text"]) > 4000:
                payload["text"] = payload["text"][:4000] + "\n... (báo cáo bị cắt ngắn do quá dài)"
                
            resp = requests.post(telegram_url, json=payload, timeout=10)
            if resp.status_code == 200:
                log.info("✅ Gửi cảnh báo Telegram thành công.")
            else:
                log.error(f"❌ Lỗi gửi Telegram: {resp.text}")
        except Exception as e:
            log.exception(f"❌ Ngoại lệ khi gửi Telegram: {e}")
    else:
        log.warning("Bỏ qua gửi Telegram vì chưa cấu hình TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID.")
    
    insert_feedback(
        incident_id=incident_id,
        feedback="PENDING",
        root_cause_snapshot=top_service_name
    )
    
    return {"runbook_report": report}
