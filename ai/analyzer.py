import json
import traceback
import signal
import redis as _redis
from ai.core.config import EVENT_QUEUE, BRPOP_TIMEOUT_SEC, REDIS_URL, validate_api_key, log
from ai.core.storage import init_db, insert_dead_letter
from ai.graph import build_graph

class ShutdownFlag:
    def __init__(self):
        self.should_exit = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.should_exit = True

def run_analyzer():
    # validate_api_key()  # Không cần khi dùng Ollama local
    init_db()

    # BRPOP là lệnh blocking; không đặt socket timeout thấp hơn/tiệm cận
    # thời gian chờ của Redis, nếu không analyzer sẽ tự thoát khi queue rỗng.
    r = _redis.Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_timeout=None,          # brpop blocking — không giới hạn socket timeout
        socket_connect_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30,     # giữ kết nối, tránh Redis đóng idle connection
    )
    shutdown = ShutdownFlag()
    app = build_graph()

    log.info(f"Analyzer Agent trực chiến tại {EVENT_QUEUE}...")

    while not shutdown.should_exit:
        try:
            item = r.brpop(EVENT_QUEUE, timeout=BRPOP_TIMEOUT_SEC)
        except (_redis.exceptions.TimeoutError, _redis.exceptions.ConnectionError):
            # Timeout hoặc mất kết nối tạm thời — tự reconnect và tiếp tục.
            continue
        if not item:
            continue

        _, msg_bytes = item
        msg_str = msg_bytes if isinstance(msg_bytes, str) else msg_bytes.decode('utf-8')
        
        try:
            event = json.loads(msg_str)
            initial_state = {"event_payload": event}

            final_state = app.invoke(initial_state)

            if final_state.get("is_incident") and "runbook_report" in final_state:
                print("\n" + "="*50)
                print(final_state["runbook_report"])
                print("="*50 + "\n")
                
        except Exception as e:
            log.error("Lỗi khi xử lý event: %s", e)
            traceback_str = traceback.format_exc()
            insert_dead_letter(msg_str, traceback_str)

    log.info("Analyzer Agent stopped.")

if __name__ == "__main__":
    run_analyzer()
