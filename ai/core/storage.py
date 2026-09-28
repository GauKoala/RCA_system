import sqlite3
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from ai.core.config import log

# Đường dẫn đến file SQLite. Đặt env DATA_DIR để chỉ điến thư mục được mount sẵn.
# Mặc định: /data/feedback.db trong container (khớp với volume dưới đây).
DB_PATH = os.path.join(os.getenv("DATA_DIR", "/data"), "feedback.db")

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)  # tự tạo /data nếu chưa có
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS incident_feedbacks (
            incident_id TEXT PRIMARY KEY,
            feedback TEXT,
            timestamp DATETIME,
            root_cause_snapshot TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS incidents (
            incident_id TEXT PRIMARY KEY,
            root_cause TEXT,
            confidence INTEGER,
            evidence TEXT,
            remediation TEXT,
            service TEXT,
            timestamp DATETIME,
            event_payload TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS dead_letters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT,
            traceback TEXT,
            timestamp DATETIME
        )
    ''')
    # Bảng chuyên dụng cho dashboard phân tích lỗi hàng tuần
    c.execute('''
        CREATE TABLE IF NOT EXISTS rca_analysis (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id     TEXT NOT NULL,
            timestamp       TEXT NOT NULL,
            week_number     INTEGER NOT NULL,   -- ISO week (1-53)
            year            INTEGER NOT NULL,
            service         TEXT,
            root_cause      TEXT,
            confidence      INTEGER,
            evidence        TEXT,               -- JSON list
            remediation     TEXT,               -- JSON list
            reasoning_steps TEXT,               -- JSON list (Chain-of-Thought)
            triage_reasons  TEXT,               -- JSON list
            errors          TEXT,               -- JSON list lỗi hệ thống
            duration_ms     INTEGER             -- thời gian xử lý pipeline (nếu có)
        )
    ''')
    # Indexes để query nhanh theo tuần / service
    c.execute('CREATE INDEX IF NOT EXISTS idx_rca_week  ON rca_analysis (year, week_number)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_rca_service ON rca_analysis (service)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_rca_ts    ON rca_analysis (timestamp)')
    conn.commit()
    conn.close()

def insert_incident(incident_id, root_cause, confidence, evidence, remediation, service, event_payload):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            INSERT OR IGNORE INTO incidents (incident_id, root_cause, confidence, evidence, remediation, service, timestamp, event_payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            incident_id, 
            root_cause, 
            confidence, 
            json.dumps(evidence, ensure_ascii=False), 
            json.dumps(remediation, ensure_ascii=False), 
            service, 
            datetime.now(), 
            json.dumps(event_payload, ensure_ascii=False)
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        log.error("Lỗi ghi SQLite (incidents): %s", e)

def insert_feedback(incident_id, feedback, root_cause_snapshot):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            INSERT OR IGNORE INTO incident_feedbacks (incident_id, feedback, timestamp, root_cause_snapshot)
            VALUES (?, ?, ?, ?)
        ''', (incident_id, feedback, datetime.now(), root_cause_snapshot))
        conn.commit()
        conn.close()
    except Exception as e:
        log.error("Lỗi ghi SQLite (feedback): %s", e)

def insert_rca_analysis(
    incident_id: str,
    service: str,
    root_cause: str,
    confidence: int,
    evidence: List[str],
    remediation: List[str],
    reasoning_steps: List[str],
    triage_reasons: List[str],
    errors: Optional[List[str]] = None,
    duration_ms: Optional[int] = None,
) -> None:
    """Ghi kết quả phân tích RCA vào bảng rca_analysis để phục vụ dashboard hàng tuần."""
    try:
        now = datetime.now(timezone.utc)
        iso_cal = now.isocalendar()   # (year, week, weekday)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            INSERT INTO rca_analysis (
                incident_id, timestamp, week_number, year,
                service, root_cause, confidence,
                evidence, remediation, reasoning_steps,
                triage_reasons, errors, duration_ms
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (
            incident_id,
            now.isoformat(),
            iso_cal[1],          # ISO week
            iso_cal[0],          # year
            service,
            root_cause,
            confidence,
            json.dumps(evidence,        ensure_ascii=False),
            json.dumps(remediation,     ensure_ascii=False),
            json.dumps(reasoning_steps, ensure_ascii=False),
            json.dumps(triage_reasons,  ensure_ascii=False),
            json.dumps(errors or [],    ensure_ascii=False),
            duration_ms,
        ))
        conn.commit()
        conn.close()
        log.info("✅ Đã lưu RCA analysis vào SQLite (incident_id=%s, tuần=%s)", incident_id, iso_cal[1])
    except Exception as e:
        log.error("Lỗi ghi SQLite (rca_analysis): %s", e)


def get_weekly_summary(year: int, week: int) -> List[Dict[str, Any]]:
    """Lấy tổng hợp tất cả RCA trong một tuần ISO cụ thể.

    Trả về list dict với các trường đã deserialize sẵn.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            'SELECT * FROM rca_analysis WHERE year=? AND week_number=? ORDER BY timestamp ASC',
            (year, week)
        )
        rows = c.fetchall()
        conn.close()
        result = []
        for row in rows:
            d = dict(row)
            for field in ('evidence', 'remediation', 'reasoning_steps', 'triage_reasons', 'errors'):
                try:
                    d[field] = json.loads(d[field] or '[]')
                except Exception:
                    d[field] = []
            result.append(d)
        return result
    except Exception as e:
        log.error("Lỗi đọc SQLite (get_weekly_summary): %s", e)
        return []


def get_service_stats(limit_weeks: int = 4) -> List[Dict[str, Any]]:
    """Thống kê số incident & confidence trung bình theo service trong N tuần gần nhất.

    Hữu ích cho widget tổng quan trên dashboard.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        # Lấy week hiện tại
        now = datetime.now(timezone.utc)
        iso_cal = now.isocalendar()
        current_week = iso_cal[1]
        current_year = iso_cal[0]
        # Lấy (year, week_number) của N tuần gần nhất (đơn giản hóa: cùng năm)
        min_week = current_week - limit_weeks
        c.execute('''
            SELECT service,
                   COUNT(*)            AS total_incidents,
                   AVG(confidence)     AS avg_confidence,
                   MIN(confidence)     AS min_confidence,
                   MAX(confidence)     AS max_confidence
            FROM rca_analysis
            WHERE year = ? AND week_number >= ?
            GROUP BY service
            ORDER BY total_incidents DESC
        ''', (current_year, max(min_week, 1)))
        rows = c.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        log.error("Lỗi đọc SQLite (get_service_stats): %s", e)
        return []


def insert_dead_letter(payload, traceback_str):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            INSERT INTO dead_letters (payload, traceback, timestamp)
            VALUES (?, ?, ?)
        ''', (payload, traceback_str, datetime.now()))
        conn.commit()
        conn.close()
    except Exception as e:
        log.error("Lỗi ghi SQLite (dead_letters): %s", e)
