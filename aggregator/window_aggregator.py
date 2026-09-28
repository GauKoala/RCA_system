"""
aggregator/window_aggregator.py
ĐọcNormalizedLog từ queue:normalized_logs, gom nhóm theo Time Window & Count,
đóng gói thành Event và đẩy sang queue:agent_events cho LangGraph.
"""

import csv
import json
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from aggregator.param_extractor import extract_params

import redis

# --- Cấu hình riêng cho Aggregator ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
NORMALIZED_QUEUE = os.getenv("NORMALIZED_QUEUE", "queue:normalized_logs")
EVENT_QUEUE = os.getenv("EVENT_QUEUE", "queue:agent_events")

WINDOW_SIZE_SEC = int(os.getenv("WINDOW_SIZE_SEC", "60"))
MAX_LOGS_PER_WINDOW = int(os.getenv("MAX_LOGS_PER_WINDOW", "5000"))
BRPOP_TIMEOUT_SEC = int(os.getenv("BRPOP_TIMEOUT_SEC", "5"))
MAX_SEQUENCE_LEN = int(os.getenv("MAX_SEQUENCE_LEN", "1000"))  # giới hạn N-gram sequence buffer
DATA_DIR = os.getenv("DATA_DIR", "/data")
PARSED_CSV_PATH = os.path.join(DATA_DIR, "parsed_logs.csv")

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [aggregator] %(message)s")
log = logging.getLogger("aggregator")

def redis_client() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=15)

class ShutdownFlag:
    def __init__(self):
        self.stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame):
        self.stop = True

class ReliableConsumer:
    """Consumer đảm bảo không mất log bằng BRPOPLPUSH và processing list riêng"""
    def __init__(self, r: redis.Redis, source_queue: str, worker_id: str):
        self.r = r
        self.source_queue = source_queue
        self.processing_key = f"{source_queue}:processing:{worker_id}"
        self._recover()

    def _recover(self):
        moved = 0
        while self.r.rpoplpush(self.processing_key, self.source_queue):
            moved += 1
        if moved:
            log.warning("Khôi phục %d log dang dở từ lần chạy trước", moved)

    def pop(self, timeout: int) -> Optional[str]:
        try:
            return self.r.brpoplpush(self.source_queue, self.processing_key, timeout=timeout)
        except redis.exceptions.TimeoutError:
            return None

def run_aggregator():
    r = redis_client()
    shutdown = ShutdownFlag()
    consumer = ReliableConsumer(r, NORMALIZED_QUEUE, "aggregator_main")
    
    log.info("Window Aggregator started. Window=%ds, MaxLogs=%d -> %s", 
             WINDOW_SIZE_SEC, MAX_LOGS_PER_WINDOW, EVENT_QUEUE)

    current_window_start = time.time()
    services_data = defaultdict(lambda: {"template_stats": defaultdict(int), "samples": defaultdict(list)})
    pending_acks = []
    parsed_batch = []  # de ghi vao CSV

    # --- MỚI: thu thập sequence và param values per service ---
    # log_sequence[svc] = list template theo thứ tự thời gian (dùng cho Sequence Detector)
    log_sequence: dict = defaultdict(list)
    # param_buffer[svc][template][param_key] = list[float] (dùng cho iForest feature)
    param_buffer: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    def flush_window():
        nonlocal current_window_start, services_data, pending_acks, log_sequence, param_buffer
        if not pending_acks:
            current_window_start = time.time()
            return

        # --- Build feature_vector cho từng service ---
        # Cấu trúc: {template_id_count, ..., avg_param_N, p99_param_N, error_rate, total_logs}
        def build_feature_vector(svc: str) -> dict:
            stats = services_data[svc]["template_stats"]
            total = sum(stats.values()) or 1

            fv = {}
            # 1. Count per template (key: t_{template_hash8}_count)
            error_keywords = ("error", "timeout", "refused", "fail", "exception", "critical")
            error_count = 0
            for tmpl, cnt in stats.items():
                key = f"t_{abs(hash(tmpl)) % 100000}_count"
                fv[key] = float(cnt)
                if any(kw in tmpl.lower() for kw in error_keywords):
                    error_count += cnt

            # 2. Metric stats từ param_buffer (avg và p99 per param)
            pb = param_buffer.get(svc, {})
            for tmpl, params in pb.items():
                for param_key, values in params.items():
                    if not values:
                        continue
                    sorted_vals = sorted(values)
                    avg_val = sum(sorted_vals) / len(sorted_vals)
                    p99_idx = max(0, int(len(sorted_vals) * 0.99) - 1)
                    p99_val = sorted_vals[p99_idx]
                    safe_tmpl = abs(hash(tmpl)) % 100000
                    fv[f"t_{safe_tmpl}_{param_key}_avg"] = float(avg_val)
                    fv[f"t_{safe_tmpl}_{param_key}_p99"] = float(p99_val)

            # 3. Global metrics
            fv["total_logs"]  = float(total)
            fv["error_rate"]  = float(error_count) / total

            return fv

        # Prepare services payload for JSON serialization
        services_payload = {}
        for svc, data in services_data.items():
            services_payload[svc] = {
                "template_stats": dict(data["template_stats"]),
                "samples": dict(data["samples"]),
                "feature_vector": build_feature_vector(svc),
                "log_sequence": log_sequence.get(svc, []),
            }

        event_payload = {
            "window_start": current_window_start,
            "window_end": time.time(),
            "total_logs": len(pending_acks),
            "services": services_payload
        }

        # Dung pipeline de atomic day event va ACK
        pipe = r.pipeline(transaction=True)
        pipe.lpush(EVENT_QUEUE, json.dumps(event_payload))
        for raw_value in pending_acks:
            pipe.lrem(consumer.processing_key, 1, raw_value)
        pipe.execute()

        # Ghi vao CSV
        if parsed_batch:
            try:
                file_exists = os.path.exists(PARSED_CSV_PATH)
                with open(PARSED_CSV_PATH, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(["Timestamp", "Service", "Level", "Raw_Message", "Template"])
                    writer.writerows(parsed_batch)
            except Exception as e:
                log.error("Khong the ghi ra file CSV: %s", e)

        log.info("Flushed window: %d logs, %s services -> %s",
                 len(pending_acks), len(services_payload), EVENT_QUEUE)

        services_data.clear()
        pending_acks.clear()
        parsed_batch.clear()
        log_sequence.clear()
        param_buffer.clear()
        current_window_start = time.time()

    while not shutdown.stop:
        # Kiểm tra điều kiện Time-based window
        if (time.time() - current_window_start) >= WINDOW_SIZE_SEC:
            flush_window()

        raw_payload = consumer.pop(timeout=BRPOP_TIMEOUT_SEC)
        if raw_payload is None:
            continue

        try:
            data = json.loads(raw_payload)
            template = data.get("template", "UNKNOWN")
            service = data.get("service") or "UNKNOWN_SERVICE"
            raw_message = data.get("raw_message", "")

            services_data[service]["template_stats"][template] += 1
            if len(services_data[service]["samples"][template]) < 2:
                services_data[service]["samples"][template].append(raw_message)

            # --- MỚI: thu thập sequence (có giới hạn kích thước) ---
            if len(log_sequence[service]) < MAX_SEQUENCE_LEN:
                log_sequence[service].append(template)

            # --- MỚI: bóc tách param số từ raw_message ---
            params = extract_params(template, raw_message)
            for param_key, val in params.items():
                param_buffer[service][template][param_key].append(val)

            pending_acks.append(raw_payload)
            parsed_batch.append([
                data.get("timestamp", ""),
                service,
                data.get("level", ""),
                raw_message,
                template
            ])

            # Kiểm tra điều kiện Count-based window (phòng thủ khi bão log)
            if len(pending_acks) >= MAX_LOGS_PER_WINDOW:
                log.warning("Đạt ngưỡng max logs (%d), flush sớm...", MAX_LOGS_PER_WINDOW)
                flush_window()

        except Exception as e:
            log.error("Lỗi xử lý payload: %s", e)
            consumer.r.lrem(consumer.processing_key, 1, raw_payload)

    log.info("Đang tắt Aggregator, flush window cuối cùng...")
    flush_window()
    log.info("Aggregator đã dừng an toàn.")

if __name__ == "__main__":
    run_aggregator()