import redis
import statistics
from typing import List, Optional
from ai.core.config import BASELINE_WINDOW_SIZE, REDIS_URL

def redis_client() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10)

class BaselineStore:
    def __init__(self, r: redis.Redis):
        self.r = r

    def _key(self, service: str, template: str) -> str:
        return f"triage:baseline:{service}:{template}"

    def get_history(self, service: str, template: str) -> List[int]:
        key = self._key(service, template)
        data = self.r.lrange(key, 0, -1)
        return [int(x) for x in data]

    def push_count(self, service: str, template: str, count: int):
        key = self._key(service, template)
        self.r.rpush(key, count)
        self.r.ltrim(key, -BASELINE_WINDOW_SIZE, -1)


