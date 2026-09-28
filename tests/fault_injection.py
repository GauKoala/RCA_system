import sys
# Fix UnicodeEncodeError trên Windows console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import docker
import time
import requests
import threading
import random

# ============================================================
# Cấu hình
# ============================================================
FRONTEND_URL = "http://localhost:8080"
docker_client = docker.from_env()

# iForest cần IFOREST_MIN_TRAIN_WINDOWS (=10) window per service.
# WINDOW_SIZE_SEC = 5s → cần ít nhất 50s baseline. Để 90s cho chắc.
BASELINE_DURATION_SEC = 90

# Endpoint bình thường (dùng cho baseline — chỉ traffic hợp lệ)
NORMAL_ENDPOINTS = [
    "/",
    "/cart",
    "/product/OLJCESPC7Z",
    "/product/66VCHSJNUP",
    "/product/1YMWWN1N4O",
]

# Các endpoint để bơm traffic (bao gồm cả lỗi)
ENDPOINTS = [
    "/", 
    "/cart", 
    "/product/OLJCESPC7Z", # ID hợp lệ
    "/product/invalid_id", # ID lỗi 
    "/cart/checkout", 
    "/setCurrency"
]

# ============================================================
# Hàm hỗ trợ
# ============================================================
def get_container_by_service(service_name):
    """Tìm container thông qua label bạn đã định nghĩa trong docker-compose"""
    containers = docker_client.containers.list(
        filters={"label": f"log-monitor.service={service_name}"}
    )
    if containers:
        return containers[0]
    return None

def generate_normal_traffic(duration_sec):
    """Bơm traffic bình thường (chỉ GET hợp lệ) để xây baseline cho iForest"""
    end_time = time.time() + duration_sec
    count = 0
    while time.time() < end_time:
        target = f"{FRONTEND_URL}{random.choice(NORMAL_ENDPOINTS)}"
        try:
            requests.get(target, timeout=3)
            count += 1
        except Exception:
            pass
        time.sleep(random.uniform(0.3, 0.8))  # Nhịp tự nhiên, không quá đều
    return count

def generate_traffic(duration_sec):
    """Bơm liên tục traffic thật lẫn lỗi vào Frontend"""
    end_time = time.time() + duration_sec
    while time.time() < end_time:
        target = f"{FRONTEND_URL}{random.choice(ENDPOINTS)}"
        try:
            if "checkout" in target or "setCurrency" in target:
                requests.post(target, data={"bad_key": "bad_value"}, timeout=2)
            else:
                requests.get(target, timeout=2)
        except Exception:
            pass # Bỏ qua lỗi timeout ở client để tiếp tục loop
        time.sleep(0.5)

def run_traffic_in_background(duration_sec, normal_only=False):
    fn = generate_normal_traffic if normal_only else generate_traffic
    t = threading.Thread(target=fn, args=(duration_sec,))
    t.start()
    return t

# ============================================================
# Pha 0: Xây Baseline (Warm-up cho iForest & N-gram)
# ============================================================

def baseline_phase():
    """
    Bơm traffic bình thường liên tục để pipeline tích lũy đủ dữ liệu.
    iForest cần >= IFOREST_MIN_TRAIN_WINDOWS (10) window per service.
    Sequence Detector cần >= SEQUENCE_MIN_LEARNS (5) lần.
    """
    print(f"\n{'='*60}")
    print(f"PHA 0: XÂY BASELINE — Bơm traffic bình thường {BASELINE_DURATION_SEC}s")
    print(f"iForest cần ≥10 window (50s), Sequence cần ≥5 lần học.")
    print(f"{'='*60}")

    start = time.time()
    # Bơm traffic trong background
    t = run_traffic_in_background(BASELINE_DURATION_SEC, normal_only=True)

    # Hiển thị progress
    while t.is_alive():
        elapsed = int(time.time() - start)
        windows_est = elapsed // 5  # WINDOW_SIZE_SEC = 5
        bar_len = 30
        progress = min(elapsed / BASELINE_DURATION_SEC, 1.0)
        filled = int(bar_len * progress)
        bar = '█' * filled + '░' * (bar_len - filled)
        print(f"\r   [{bar}] {elapsed}/{BASELINE_DURATION_SEC}s — ~{windows_est} window đã tạo", end='', flush=True)
        time.sleep(2)

    t.join()
    total_windows = BASELINE_DURATION_SEC // 5
    print(f"\n   ✅ Baseline hoàn tất! ~{total_windows} window đã được gửi vào pipeline.")
    print(f"   iForest đã có đủ dữ liệu để detect anomaly.\n")
    time.sleep(3)  # Đợi aggregator flush window cuối

# ============================================================
# Các kịch bản bơm lỗi (Chaos Scenarios)
# ============================================================

def scenario_1_redis_timeout():
    print("\n[Kịch bản 1] Gây lỗi dây chuyền (Cascading Failure): Đóng băng Redis")
    print("Mục tiêu: redis-cart (Treo) -> cartservice (Timeout) -> checkoutservice (Lỗi gRPC) -> frontend (HTTP 500)")
    
    container = get_container_by_service("redis-cart")
    if not container:
        print("[!] Không tìm thấy container redis-cart.")
        return

    print(" -> Đang đóng băng (pause) redis-cart...")
    container.pause()
    
    # Bơm traffic trong lúc redis đang sập để sinh ra log lỗi
    t = run_traffic_in_background(15)
    t.join()

    print(" -> Đang phục hồi (unpause) redis-cart...")
    container.unpause()
    time.sleep(5) # Đợi hệ thống ổn định lại

def scenario_2_service_restart():
    print("\n[Kịch bản 2] Gây đứt gãy kết nối nội bộ: Khởi động lại Product Catalog")
    print("Mục tiêu: Tạo ra các lỗi rpc error: code = Unavailable do mất kết nối đột ngột")
    
    container = get_container_by_service("productcatalogservice")
    if not container:
        print("[!] Không tìm thấy container productcatalogservice.")
        return

    # Khởi chạy traffic trước khi restart để bắt được khoảnh khắc đứt gãy
    t = run_traffic_in_background(20)
    time.sleep(5)
    
    print(" -> Đang khởi động lại (restart) productcatalogservice...")
    container.restart()
    t.join()
    time.sleep(5)

def scenario_3_application_logic():
    print("\n[Kịch bản 3] Gây lỗi Logic Ứng Dụng (HTTP 400/500)")
    print("Mục tiêu: Đẩy dữ liệu sai format, sai ID vào hệ thống để sinh log lỗi Exception")
    
    bad_requests = [
        {"url": f"{FRONTEND_URL}/cart", "method": "post", "data": {"product_id": "DROP TABLE", "quantity": "abc"}},
        {"url": f"{FRONTEND_URL}/setCurrency", "method": "post", "data": {}},
        {"url": f"{FRONTEND_URL}/product/1234_not_exist", "method": "get", "data": None},
    ]

    for req in bad_requests:
        print(f" -> Đang gửi Bad Request tới: {req['url']}")
        try:
            if req["method"] == "post":
                requests.post(req["url"], data=req["data"], timeout=3)
            else:
                requests.get(req["url"], timeout=3)
        except Exception as e:
            print(f"    Lỗi mạng: {e}")
        time.sleep(2)

# ============================================================
# Main Execution
# ============================================================
if __name__ == "__main__":
    print("="*60)
    print("KỊCH BẢN CHAOS ENGINEERING — FULL PIPELINE TEST")
    print("Hãy đảm bảo Pipeline (docker compose up) đang chạy!")
    print("="*60)

    # ── Pha 0: Baseline (xây dữ liệu "bình thường" cho AI) ──
    baseline_phase()

    # ── Pha 1: Bơm lỗi (AI sẽ phát hiện sự khác biệt) ──
    print("="*60)
    print("PHA 1: BƠM LỖI — Bắt đầu các kịch bản Chaos")
    print("="*60)

    scenario_3_application_logic()
    time.sleep(3)
    
    scenario_1_redis_timeout()
    time.sleep(3)
    
    scenario_2_service_restart()
    
    # Đợi thêm để analyzer xử lý window cuối
    print("\n[*] Đợi 15s để Analyzer xử lý các window cuối...")
    time.sleep(15)
    
    print("\n" + "="*60)
    print("HOÀN TẤT! Kiểm tra kết quả:")
    print("  docker compose logs --tail=50 analyzer")
    print("  docker compose logs --tail=20 llm_worker")
    print("="*60)