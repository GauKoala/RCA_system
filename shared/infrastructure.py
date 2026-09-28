"""
shared/infrastructure.py
Shared config, schema và các thành phần đảm bảo độ tin cậy (reliable consume, batch pipeline, graceful shutdown).
Tuyệt đối KHÔNG import drain3 hay langchain vào đây.
"""

import logging
import os
import signal
import time
from typing import List, Optional, Tuple

import redis
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(processName)s: %(message)s")

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

# --- Config ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RAW_QUEUE = os.getenv("RAW_QUEUE", "queue:raw_logs")
NORMALIZED_QUEUE = os.getenv("NORMALIZED_QUEUE", "queue:normalized_logs")
LLM_PENDING_QUEUE = os.getenv("LLM_PENDING_QUEUE", "queue:llm_pending_logs")
EVENT_QUEUE = os.getenv("EVENT_QUEUE", "queue:agent_events")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BATCH_FLUSH_INTERVAL_SEC = float(os.getenv("BATCH_FLUSH_INTERVAL_SEC", "2"))
BRPOP_TIMEOUT_SEC = int(os.getenv("BRPOP_TIMEOUT_SEC", "5"))
REDIS_SOCKET_TIMEOUT_SEC = BRPOP_TIMEOUT_SEC + 30   # đủ lớn để brpoplpush blocking hoàn thành

def redis_client() -> redis.Redis:
    return redis.Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_timeout=REDIS_SOCKET_TIMEOUT_SEC,
        socket_connect_timeout=5,
        retry_on_timeout=True,        # tự reconnect khi timeout, tránh crash
        health_check_interval=30,     # giữ kết nối sống
    )

# --- Schema ---
class NormalizedLog(BaseModel):
    trace_id: Optional[str] = None
    service: Optional[str] = None
    timestamp: str
    level: Optional[str] = None
    raw_message: str
    template: str
    template_id: Optional[int] = None
    source: str = Field(description="'lilac' hoặc 'lilac-llm'")
    confidence: float

# --- Core Tools ---
class ShutdownFlag:
    def __init__(self):
        self.stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame):
        self.stop = True

class ReliableConsumer:
    def __init__(self, r: redis.Redis, source_queue: str, worker_id: str, log: logging.Logger):
        self.r = r
        self.source_queue = source_queue
        self.processing_key = f"{source_queue}:processing:{worker_id}"
        self.log = log
        self._recover()

    def _recover(self) -> None:
        moved = 0
        while self.r.rpoplpush(self.processing_key, self.source_queue):
            moved += 1
        if moved:
            self.log.warning("Khôi phục %d log từ lần chạy trước (%s)", moved, self.processing_key)

    def pop(self, timeout: int) -> Optional[str]:
        try:
            return self.r.brpoplpush(self.source_queue, self.processing_key, timeout=timeout)
        except redis.exceptions.TimeoutError:
            return None

    def ack(self, raw_value: str) -> None:
        self.r.lrem(self.processing_key, 1, raw_value)

class BatchPusher:
    def __init__(self, r: redis.Redis, dest_queue: str, log: logging.Logger, batch_size: int = BATCH_SIZE, flush_interval: float = BATCH_FLUSH_INTERVAL_SEC):
        self.r = r
        self.dest_queue = dest_queue
        self.log = log
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._payloads: List[str] = []
        self._acks: List[Tuple[str, str]] = []  
        self._last_flush = time.monotonic()

    def add(self, payload: str, ack_info: Tuple[str, str]) -> None:
        self._payloads.append(payload)
        self._acks.append(ack_info)
        if len(self._payloads) >= self.batch_size or self._due():
            self.flush()

    def _due(self) -> bool:
        return (time.monotonic() - self._last_flush) >= self.flush_interval

    def flush(self) -> None:
        if not self._payloads:
            self._last_flush = time.monotonic()
            return
        pipe = self.r.pipeline(transaction=True)
        for payload in self._payloads:
            pipe.lpush(self.dest_queue, payload)
        for processing_key, raw_value in self._acks:
            pipe.lrem(processing_key, 1, raw_value)
        pipe.execute()
        self.log.debug("Flushed batch: %d log -> %s", len(self._payloads), self.dest_queue)
        self._payloads.clear()
        self._acks.clear()
        self._last_flush = time.monotonic()


# --- Parquet Exporter ---
import threading
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    pd = None

# Đường dẫn mặc định cho file kết quả parsing
PARSED_LOG_OUTPUT_PATH = os.getenv("PARSED_LOG_OUTPUT_PATH", "data/parsed_logs.parquet")


class ParquetExporter:
    """
    Ghi kết quả parsing ra file Parquet, thread-safe.
    Mỗi lần ghi sẽ đọc file cũ (nếu có), nối data và ghi đè lại.
    (Để tối ưu trên production nên ghi ra nhiều file nhỏ, 
     nhưng cách này phù hợp để build 1 file Kaggle đơn giản).
    """

    def __init__(self, filepath: str = PARSED_LOG_OUTPUT_PATH, log: Optional[logging.Logger] = None):
        if pd is None:
            raise ImportError("Vui lòng cài đặt 'pandas' và 'pyarrow' để xuất file Parquet.")
        
        self.filepath = Path(filepath)
        self.log = log or get_logger("parquet_exporter")
        self._lock = threading.Lock()
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

    def write(self, normalized: "NormalizedLog") -> None:
        """Ghi 1 NormalizedLog ra Parquet."""
        self.write_batch([normalized])

    def write_batch(self, logs: List["NormalizedLog"]) -> None:
        """Ghi nhiều NormalizedLog ra Parquet cùng lúc (atomic)."""
        if not logs:
            return
            
        # Đổi tên cột cho giống format logs.parquet của Kaggle (container_name, message)
        df_new = pd.DataFrame([{
            "timestamp": n.timestamp,
            "container_name": n.service or "",
            "level": n.level or "",
            "message": n.raw_message,
            "template": n.template
        } for n in logs])
        
        with self._lock:
            df_combined = df_new
            if self.filepath.exists() and self.filepath.stat().st_size > 0:
                try:
                    df_exist = pd.read_parquet(self.filepath)
                    df_combined = pd.concat([df_exist, df_new], ignore_index=True)
                except Exception as e:
                    self.log.error("Lỗi đọc Parquet cũ, sẽ ghi đè file mới: %s", e)
            
            # Ghi đè lại file
            df_combined.to_parquet(self.filepath, index=False)
            
        self.log.debug("Ghi %d dòng (tổng %d) vào %s", len(logs), len(df_combined), self.filepath)