"""
worker/worker_utils.py
Các tiện ích đặc thù cho luồng xử lý log thô (Parsing) ở Giai đoạn 1.
"""

import base64
import gzip
import json
import os
import pickle
from datetime import datetime, timezone
from typing import Optional

import redis

# --- Config đặc thù cho Worker ---
LILAC_SNAPSHOT_INTERVAL_MIN = float(os.getenv("LILAC_SNAPSHOT_INTERVAL_MIN", "1"))
LOG_CONTENT_PREFIX = os.getenv("LOG_CONTENT_PREFIX", "log:")

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def parse_raw(raw: str) -> dict:
    """Xử lý JSON (từ Collector) hoặc raw text thuần."""
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass
    return {"message": raw}

def extract_fields(entry: dict, raw_fallback: str) -> dict:
    """Ánh xạ field theo format thực tế của Collector."""
    message = entry.get("raw") or entry.get("message") or entry.get("log") or raw_fallback
    ts_val = entry.get("timestamp")
    if isinstance(ts_val, (int, float)):
        timestamp = datetime.fromtimestamp(ts_val, tz=timezone.utc).isoformat()
    else:
        timestamp = ts_val or now_iso()
    return {
        "message": message,
        "timestamp": timestamp,
        "service": entry.get("service") or entry.get("container"),
        "trace_id": entry.get("id"),
    }

class RedisLILACPersistence:
    """Lưu snapshot state của ParsingCache vào Redis (nén gzip + base64)."""
    def __init__(self, r: redis.Redis, state_key: str):
        self.r = r
        self.state_key = state_key

    def save_state(self, cache_obj) -> None:
        state = pickle.dumps(cache_obj)
        payload = base64.b64encode(gzip.compress(state)).decode("ascii")
        self.r.set(self.state_key, payload)

    def load_state(self):
        raw = self.r.get(self.state_key)
        if raw is None:
            return None
        state = gzip.decompress(base64.b64decode(raw))
        return pickle.loads(state)