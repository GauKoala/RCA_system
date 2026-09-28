"""
LLM Worker (slow-path) using LILAC GPT Query:

  LLM_PENDING_QUEUE --(BRPOPLPUSH, reliable)--> LILAC GPT Query --> Update Cache & NORMALIZED_QUEUE

Chạy 1 process:      python llm_worker.py
Chạy nhiều process:  NUM_LLM_WORKERS=4 python llm_worker.py
"""

import multiprocessing
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from shared.infrastructure import (
    BATCH_FLUSH_INTERVAL_SEC,
    BATCH_SIZE,
    BRPOP_TIMEOUT_SEC,
    LLM_PENDING_QUEUE,
    NORMALIZED_QUEUE,
    BatchPusher,
    NormalizedLog,
    ParquetExporter,
    ReliableConsumer,
    ShutdownFlag,
    get_logger,
    redis_client,
)
from logparser.LILAC.utils import (
    RedisLILACPersistence,
    extract_fields,
    parse_raw,
)

from logparser.LILAC.gpt_query import query_template_from_gpt_with_check
from logparser.LILAC.LILAC import load_regs

NUM_LLM_WORKERS = int(os.getenv("NUM_LLM_WORKERS", "1"))

def run_llm_worker(worker_id: str) -> None:
    log = get_logger(f"llm_worker.{worker_id}")
    r = redis_client()
    shutdown = ShutdownFlag()
    consumer = ReliableConsumer(r, LLM_PENDING_QUEUE, worker_id, log)
    pusher = BatchPusher(
        r, NORMALIZED_QUEUE, log, batch_size=BATCH_SIZE, flush_interval=BATCH_FLUSH_INTERVAL_SEC
    )
    parquet_exporter = ParquetExporter(log=log)

    log.info("LILAC LLM worker '%s' started. LLM_PENDING_QUEUE=%s", worker_id, LLM_PENDING_QUEUE)
    
    try:
        regs_common = load_regs()
    except Exception as e:
        log.error("Failed to load LILAC regs_common: %s", e)
        regs_common = []

    while not shutdown.stop:
        raw_payload = consumer.pop(timeout=BRPOP_TIMEOUT_SEC)
        if raw_payload is None:
            pusher.flush()
            continue
            
        entry = parse_raw(raw_payload)
        fields = extract_fields(entry, raw_payload)
        message = fields["message"]
        ack_info = (consumer.processing_key, raw_payload)

        try:
            log.info("Gửi 1 log tới LLM (LILAC)...")
            model_name = os.getenv("LILAC_LLM_MODEL", "llama3.1:latest")
            template, success = query_template_from_gpt_with_check(
                message, 
                regs_common=regs_common, 
                model=model_name
            )
            
            if success:
                # Cập nhật cache toàn cục
                persistence = RedisLILACPersistence(r, state_key="lilac:state:global")
                cache = persistence.load_state()
                if cache is None:
                    from logparser.LILAC.parsing_cache import ParsingCache
                    cache = ParsingCache()
                
                cache.add_templates(template, True, [])
                persistence.save_state(cache)
                log.info("LILAC đã học template mới thành công.")
            else:
                template = message
                
        except Exception:
            log.exception("Gọi LLM thất bại, tạm dùng nguyên message làm template")
            template = message

        normalized = NormalizedLog(
            trace_id=fields["trace_id"],
            service=fields["service"],
            timestamp=fields["timestamp"],
            level=entry.get("level"),
            raw_message=message,
            template=template,
            template_id=None,
            source="lilac-llm",
            confidence=0.0,
        )
        parquet_exporter.write_batch([normalized])
        pusher.add(normalized.model_dump_json(), ack_info)

    log.info("Đang tắt LLM worker '%s', flush batch còn lại...", worker_id)
    pusher.flush()
    log.info("LLM worker '%s' đã dừng an toàn.", worker_id)

if __name__ == "__main__":
    if NUM_LLM_WORKERS <= 1:
        run_llm_worker(worker_id=os.getenv("WORKER_ID", "l0"))
    else:
        procs = [
            multiprocessing.Process(target=run_llm_worker, args=(f"l{i}",), name=f"llm-worker-{i}")
            for i in range(NUM_LLM_WORKERS)
        ]
        for p in procs:
            p.start()
        for p in procs:
            p.join()
