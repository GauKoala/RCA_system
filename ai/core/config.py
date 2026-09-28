import os
import sys
import logging
from dotenv import load_dotenv

# Tự động nạp biến môi trường từ file .env
load_dotenv()


logging.basicConfig(level=logging.INFO, format="%(asctime)s [analyzer] %(message)s")
log = logging.getLogger("analyzer")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
EVENT_QUEUE = os.getenv("EVENT_QUEUE", "queue:agent_events")
BRPOP_TIMEOUT_SEC = int(os.getenv("BRPOP_TIMEOUT_SEC", "5"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

BASELINE_WINDOW_SIZE = int(os.getenv("BASELINE_WINDOW_SIZE", "20"))
MIN_DATA_POINTS = int(os.getenv("MIN_DATA_POINTS", "3"))
MIN_COUNT_THRESHOLD = int(os.getenv("MIN_COUNT_THRESHOLD", "5"))

# Isolation Forest
IFOREST_MIN_TRAIN_WINDOWS = int(os.getenv("IFOREST_MIN_TRAIN_WINDOWS", "10"))
IFOREST_CONTAMINATION     = float(os.getenv("IFOREST_CONTAMINATION", "0.05"))
IFOREST_N_ESTIMATORS      = int(os.getenv("IFOREST_N_ESTIMATORS", "100"))

# Sequence N-gram
SEQUENCE_NGRAM_SIZE = int(os.getenv("SEQUENCE_NGRAM_SIZE", "3"))
SEQUENCE_MIN_LEARNS = int(os.getenv("SEQUENCE_MIN_LEARNS", "5"))

# Model defaults - override qua env var
# openai/gpt-oss-20b: nhe nhat, khong phai thinking model, it bi rate limit nhat
# openai/gpt-oss-120b: manh hon nhung OTPM thap hon
# qwen/qwen3.x-27b: thinking model (co <think> tags), OTPM 1000/phut, de bi limit
INVESTIGATOR_MODEL = os.getenv("INVESTIGATOR_MODEL", "qwen2.5:7b")
RCA_MODEL          = os.getenv("RCA_MODEL",          "qwen2.5:7b")
RESPONDER_MODEL    = os.getenv("RESPONDER_MODEL",    "qwen2.5:7b")

def validate_api_key():
    key = os.getenv("OPENROUTER_API_KEY")
    if not key or key.strip() == "":
        log.critical("Thiếu OPENROUTER_API_KEY hợp lệ. Hãy set biến môi trường này.")
        sys.exit(1)
