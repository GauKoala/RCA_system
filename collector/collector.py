import time
import uuid
import json
import threading
import os

import docker
import redis


# ============================================================
# Connections & Config
# ============================================================

docker_client = docker.from_env()

redis_client = redis.Redis(
    connection_pool=redis.ConnectionPool.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
    )
)

RAW_LOG_QUEUE = "queue:raw_logs"
LOG_WINDOW = "window:logs"
LOG_CONTENT_PREFIX = "log:"

WINDOW_SECONDS = 300
LOG_CONTENT_TTL = WINDOW_SECONDS + 60   # nới thêm buffer cho độ trễ xử lý ở Worker
CLEANUP_INTERVAL = 10
DISCOVERY_INTERVAL = int(os.getenv("DISCOVERY_INTERVAL", "30"))
RECONNECT_BACKOFF = int(os.getenv("RECONNECT_BACKOFF", "5"))
# 0 nghĩa là chỉ nhận log mới, tránh đưa lại log cũ vào pipeline khi collector restart.
LOG_TAIL = int(os.getenv("LOG_TAIL", "0"))

# Label để nhóm nhiều container instance vào cùng 1 "service" logic
# (vd: web-app-1, web-app-2 cùng gắn label "log-monitor.service=web-app")
SERVICE_LABEL_KEY = "log-monitor.service"

DISCOVERY_LABEL_FILTER = {"label": os.getenv("DISCOVERY_LABEL", "log-monitor=true")}
FALLBACK_CONTAINER_NAMES = ["blog-source"]

# name -> {"thread": Thread, "stop_event": Event}
active_threads = {}
active_threads_lock = threading.Lock()


# ============================================================
# Phát hiện container cần theo dõi
# ============================================================

def discover_containers():
    try:
        containers = docker_client.containers.list(filters=DISCOVERY_LABEL_FILTER)
        if containers:
            return [c.name for c in containers]
    except docker.errors.APIError as e:
        print(f"[!] Lỗi khi tìm container theo label ({e}).")

    return FALLBACK_CONTAINER_NAMES


def get_service_name(container, fallback_name: str) -> str:
    try:
        return container.labels.get(SERVICE_LABEL_KEY, fallback_name)
    except Exception:
        return fallback_name


# ============================================================
# Stream log cho 1 container (chạy trong 1 thread riêng)
# ============================================================

def stream_container_logs(container_name: str, stop_event: threading.Event):
    print(f"[*] Bắt đầu lắng nghe container '{container_name}'...")
    last_cleanup_ts = time.time()

    while not stop_event.is_set():
        try:
            container = docker_client.containers.get(container_name)
            service_name = get_service_name(container, container_name)

            for line in container.logs(
                stream=True,
                follow=True,
                stdout=True,
                stderr=True,
                tail=LOG_TAIL,
            ):
                if stop_event.is_set():
                    break

                raw_log = line.decode("utf-8", errors="replace").strip()
                if not raw_log:
                    continue

                now_ts = time.time()
                log_id = str(uuid.uuid4())

                payload = {
                    "id": log_id,
                    "timestamp": now_ts,
                    "container": container_name,
                    "service": service_name,
                    "raw": raw_log
                }
                payload_str = json.dumps(payload)

                pipe = redis_client.pipeline(transaction=False)

                # 1. Nội dung đầy đủ, lưu MỘT LẦN DUY NHẤT, tự hết hạn theo TTL
                pipe.set(
                    f"{LOG_CONTENT_PREFIX}{log_id}",
                    payload_str,
                    ex=LOG_CONTENT_TTL
                )

                # 2. Raw log queue: chỉ đẩy id (nhẹ), Worker tự GET nội dung khi xử lý
                #    LPUSH (đẩy vào đầu trái) để khớp chiều pop của Worker
                #    (BRPOPLPUSH lấy từ đuôi phải) -> FIFO: log cũ được xử lý
                #    trước log mới, tránh log cũ bị "đói" và hết TTL khi có burst.
                pipe.lpush(RAW_LOG_QUEUE, log_id)

                # 3. Rolling window: chỉ lưu id + timestamp, không lưu lại full JSON
                pipe.zadd(LOG_WINDOW, {log_id: now_ts})

                if now_ts - last_cleanup_ts >= CLEANUP_INTERVAL:
                    pipe.zremrangebyscore(LOG_WINDOW, 0, now_ts - WINDOW_SECONDS)
                    last_cleanup_ts = now_ts

                pipe.execute()

                print(f"[{container_name}/{service_name} -> Redis] {raw_log[:100]}")

        except docker.errors.NotFound:
            print(
                f"[!] Chưa/không còn thấy container '{container_name}'. "
                f"Chờ tín hiệu dừng hoặc thử lại..."
            )
            # wait() thay vì sleep() để thread thoát ngay khi main loop
            # phát hiện container đã bị gỡ và gọi stop_event.set()
            stop_event.wait(RECONNECT_BACKOFF)

        except docker.errors.APIError as e:
            print(
                f"[!] Mất kết nối stream log của '{container_name}' "
                f"({e}). Thử lại sau {RECONNECT_BACKOFF}s..."
            )
            stop_event.wait(RECONNECT_BACKOFF)

        except Exception as e:
            # Bắt luôn các lỗi không thuộc docker.errors (vd: mất kết nối
            # socket/daemon ở tầng thấp hơn, requests.exceptions.*),
            # để thread không bao giờ chết âm thầm mà luôn tự thử lại.
            print(
                f"[!] Lỗi không xác định khi stream '{container_name}' "
                f"({type(e).__name__}: {e}). Thử lại sau {RECONNECT_BACKOFF}s..."
            )
            stop_event.wait(RECONNECT_BACKOFF)

    print(f"[*] Thread của container '{container_name}' đã dừng.")


# ============================================================
# Main: quản lý vòng đời thread theo từng nguồn (multi-source)
# ============================================================

def main():
    print("[*] Collector đa nguồn đang khởi động...")

    try:
        while True:
            current_names = set(discover_containers())

            with active_threads_lock:
                existing_names = set(active_threads.keys())

                # Khởi động thread cho container mới phát hiện
                for name in current_names - existing_names:
                    ev = threading.Event()
                    t = threading.Thread(
                        target=stream_container_logs,
                        args=(name, ev),
                        daemon=True
                    )
                    active_threads[name] = {"thread": t, "stop_event": ev}
                    t.start()

                # Khởi động lại nếu thread cũ đã chết nhưng container vẫn còn
                for name in current_names & existing_names:
                    entry = active_threads[name]
                    if not entry["thread"].is_alive():
                        ev = threading.Event()
                        t = threading.Thread(
                            target=stream_container_logs,
                            args=(name, ev),
                            daemon=True
                        )
                        active_threads[name] = {"thread": t, "stop_event": ev}
                        t.start()

                # Dừng & dọn thread cho container không còn tồn tại
                # -> tránh retry vô hạn vào container đã bị xoá
                for name in existing_names - current_names:
                    entry = active_threads.pop(name)
                    entry["stop_event"].set()
                    entry["thread"].join(timeout=2)
                    print(f"[*] Đã gỡ '{name}' khỏi danh sách theo dõi.")

            time.sleep(DISCOVERY_INTERVAL)

    except KeyboardInterrupt:
        print("\n[*] Dừng Collector chủ động.")
        with active_threads_lock:
            for entry in active_threads.values():
                entry["stop_event"].set()
            for entry in active_threads.values():
                entry["thread"].join(timeout=2)


if __name__ == "__main__":
    main()
