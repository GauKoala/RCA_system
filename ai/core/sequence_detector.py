"""
ai/core/sequence_detector.py

N-gram Sequence Anomaly Detector per service.

Logic:
  - Mỗi window, Triage gọi detect(service, sequence) để kiểm tra
    xem chuỗi event ID có chứa N-gram chưa từng thấy không.
  - Nếu window bình thường (is_incident=False), gọi learn(service, sequence)
    để mở rộng tập N-gram đã biết.
  - N-gram được lưu trong Redis set (key: seq:ngram:{svc}:{n}).

Lưu ý:
  - "Sequence per service" → có thể có FP khi nhiều luồng song song.
    Đây là trade-off chấp nhận được ở MVP.
  - N mặc định = 3 (configurable qua SEQUENCE_NGRAM_SIZE).
"""

import hashlib
import logging
from typing import List

import redis

from ai.core.config import REDIS_URL, SEQUENCE_MIN_LEARNS, SEQUENCE_NGRAM_SIZE

log = logging.getLogger("analyzer.sequence")


class SequenceAnomalyDetector:
    _KEY_NGRAM  = "seq:ngram:{svc}:{n}"    # Redis Set chứa các N-gram hash đã học
    _KEY_LEARNS = "seq:learns:{svc}"       # Redis counter số lần đã học

    def __init__(self, r: redis.Redis, n: int = SEQUENCE_NGRAM_SIZE):
        self.r = r
        self.n = n

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #

    def learn(self, service: str, sequence: List[str]) -> None:
        """Ghi nhớ tất cả N-gram trong sequence vào Redis set."""
        ngrams = self._build_ngrams(sequence)
        if not ngrams:
            return
        key = self._ngram_key(service)
        self.r.sadd(key, *ngrams)
        self.r.incr(self._learns_key(service))

    def detect(self, service: str, sequence: List[str]) -> List[str]:
        """
        Trả về list N-gram (dạng readable) chưa từng thấy trong baseline.
        Trả về [] nếu chưa đủ SEQUENCE_MIN_LEARNS để tránh FP khi mới start.
        """
        learns = self._get_learns(service)
        if learns < SEQUENCE_MIN_LEARNS:
            log.debug("Sequence [%s] warm-up %d/%d", service, learns, SEQUENCE_MIN_LEARNS)
            return []

        ngrams = self._build_ngrams(sequence)
        if not ngrams:
            return []

        known_key = self._ngram_key(service)
        unknown = []
        for ng in ngrams:
            if not self.r.sismember(known_key, ng):
                unknown.append(ng)

        return unknown

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    def _build_ngrams(self, sequence: List[str]) -> List[str]:
        """Tạo danh sách N-gram hash từ sequence event IDs."""
        if len(sequence) < self.n:
            return []
        ngrams = []
        for i in range(len(sequence) - self.n + 1):
            gram = tuple(sequence[i : i + self.n])
            # Hash để tiết kiệm bộ nhớ Redis, dùng SHA1 (8 hex = 32-bit)
            gram_hash = hashlib.sha1("->".join(gram).encode()).hexdigest()[:12]
            ngrams.append(gram_hash)
        return ngrams

    def _ngram_key(self, svc: str) -> str:
        return self._KEY_NGRAM.format(svc=svc, n=self.n)

    def _learns_key(self, svc: str) -> str:
        return self._KEY_LEARNS.format(svc=svc)

    def _get_learns(self, svc: str) -> int:
        val = self.r.get(self._learns_key(svc))
        if val is None:
            return 0
        try:
            return int(val)
        except ValueError:
            return 0
