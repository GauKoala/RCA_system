"""
Worker (LILAC fast-path):

  RAW_QUEUE --(BRPOPLPUSH, reliable)--> LILAC thống kê
      known/high-confidence   -> NORMALIZED_QUEUE (batch push)
      unknown/low-confidence  -> LLM_PENDING_QUEUE (batch push, xử lý bởi llm_worker.py riêng)

Không còn gọi LLM inline trong vòng lặp này nữa (điểm nghẽn cũ) — log lạ chỉ
được CHUYỂN TIẾP sang một hàng đợi khác, việc gọi LLM (chậm, blocking I/O) do
llm_worker.py — một service tách biệt — đảm nhiệm.

Chạy 1 process:      python worker.py
Chạy nhiều process:  NUM_WORKERS=4 python worker.py
"""

import multiprocessing
import os
import time
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 1. Import các thành phần Hạ tầng chung (Từ thư mục shared)
from shared.infrastructure import (
    BATCH_FLUSH_INTERVAL_SEC,
    BATCH_SIZE,
    BRPOP_TIMEOUT_SEC,
    LLM_PENDING_QUEUE,
    NORMALIZED_QUEUE,
    RAW_QUEUE,
    BatchPusher,
    NormalizedLog,
    ParquetExporter,
    ReliableConsumer,
    ShutdownFlag,
    get_logger,
    redis_client,
)

# 2. Import các công cụ đặc thù của riêng LILAC Worker
from logparser.LILAC.utils import (
    LILAC_SNAPSHOT_INTERVAL_MIN,
    LOG_CONTENT_PREFIX,
    RedisLILACPersistence,
    extract_fields,
    parse_raw,
)

from logparser.LILAC.parsing_cache import ParsingCache
from logparser.LILAC.LILAC import LILACNormalizer

NUM_WORKERS = int(os.getenv("NUM_WORKERS", "1"))


def build_lilac_cache(r) -> ParsingCache:
    persistence = RedisLILACPersistence(r, state_key="lilac:state:global")
    cache = persistence.load_state()
    if not cache:
        cache = ParsingCache()
    return cache


def run_worker(worker_id: str) -> None:
    log = get_logger(f"worker.{worker_id}")
    r = redis_client()
    shutdown = ShutdownFlag()

    cache = build_lilac_cache(r)
    normalizer = LILACNormalizer(cache)
    consumer = ReliableConsumer(r, RAW_QUEUE, worker_id, log)
    parquet_exporter = ParquetExporter(log=log)

    normalized_pusher = BatchPusher(
        r, NORMALIZED_QUEUE, log, batch_size=BATCH_SIZE, flush_interval=BATCH_FLUSH_INTERVAL_SEC
    )
    llm_pending_pusher = BatchPusher(
        r, LLM_PENDING_QUEUE, log, batch_size=BATCH_SIZE, flush_interval=BATCH_FLUSH_INTERVAL_SEC
    )
    
    # Save/Reload state periodically
    last_reload_time = time.time()
    reload_interval_sec = LILAC_SNAPSHOT_INTERVAL_MIN * 60

    log.info("LILAC worker '%s' started. RAW_QUEUE=%s", worker_id, RAW_QUEUE)

    while not shutdown.stop:
        if time.time() - last_reload_time > reload_interval_sec:
            # Tải lại cache từ Redis do llm_worker có thể đã thêm template mới
            new_cache = RedisLILACPersistence(r, state_key="lilac:state:global").load_state()
            if new_cache:
                normalizer.cache = new_cache
            last_reload_time = time.time()

        log_id = consumer.pop(timeout=BRPOP_TIMEOUT_SEC)
        if log_id is None:
            normalized_pusher.flush()
            llm_pending_pusher.flush()
            continue

        ack_info = (consumer.processing_key, log_id)

        raw_payload = r.get(f"{LOG_CONTENT_PREFIX}{log_id}")
        if not raw_payload:
            log.warning("Không tìm thấy payload cho log_id=%s (có thể đã quá TTL)", log_id)
            r.lrem(consumer.processing_key, 1, log_id)
            continue

        entry = parse_raw(raw_payload)
        fields = extract_fields(entry, raw_payload)
        message = fields["message"]

        template, cluster_id, confidence, is_known = normalizer.process(message)

        if is_known:
            normalized = NormalizedLog(
                trace_id=fields["trace_id"],
                service=fields["service"],
                timestamp=fields["timestamp"],
                level=entry.get("level"),
                raw_message=message,
                template=template,
                template_id=str(cluster_id) if cluster_id is not None else None,
                source="lilac",
                confidence=confidence,
            )
            parquet_exporter.write(normalized)
            normalized_pusher.add(normalized.model_dump_json(), ack_info)
        else:
            log.debug("Unknown log -> LLM_PENDING_QUEUE")
            llm_pending_pusher.add(raw_payload, ack_info)

    log.info("Đang tắt worker '%s', flush batch còn lại...", worker_id)
    normalized_pusher.flush()
    llm_pending_pusher.flush()
    log.info("Worker '%s' đã dừng an toàn.", worker_id)


if __name__ == "__main__":
    if NUM_WORKERS <= 1:
        run_worker(worker_id=os.getenv("WORKER_ID", "w0"))
    else:
        procs = [
            multiprocessing.Process(target=run_worker, args=(f"w{i}",), name=f"lilac-worker-{i}")
            for i in range(NUM_WORKERS)
        ]
        for p in procs:
            p.start()
        for p in procs:
            p.join()
