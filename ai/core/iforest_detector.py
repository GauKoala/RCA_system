"""
ai/core/iforest_detector.py

Isolation Forest Detector per service.

Luồng hoạt động:
  1. Mỗi window, Triage gọi fit_or_predict(service, feature_vector).
  2. Trong IFOREST_MIN_TRAIN_WINDOWS đầu: chỉ tích lũy dữ liệu (warm-up),
     trả về (False, 0.0) — không phát cảnh báo FP trong giai đoạn học.
  3. Sau giai đoạn học: fit lại IsolationForest trên toàn bộ buffer đã tích
     lũy, predict window hiện tại, lưu model vào Redis.
  4. State (fitted model) được serialize pickle+gzip và lưu Redis để sống
     qua các lần restart. Buffer window cũng lưu Redis (JSON list).

Feature vector được căn chỉnh (aligned) theo danh sách key toàn cục
(global_feature_keys:{service}) để đảm bảo số chiều nhất quán.
Khi có key mới, model sẽ được re-fit ở window tiếp theo.
"""

import gzip
import json
import logging
import pickle
from typing import Dict, List, Tuple

import numpy as np
import redis
from sklearn.ensemble import IsolationForest

from ai.core.config import (
    IFOREST_CONTAMINATION,
    IFOREST_MIN_TRAIN_WINDOWS,
    IFOREST_N_ESTIMATORS,
    REDIS_URL,
)

log = logging.getLogger("analyzer.iforest")


def _redis_client() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=False, socket_timeout=10)


class IsolationForestDetector:
    """
    Isolation Forest online per service.
    Mỗi instance dùng chung 1 Redis connection (thread-safe: mỗi Triage
    invocation tạo 1 instance mới từ redis_client truyền vào).
    """

    # Key prefixes trong Redis
    _KEY_BUFFER  = "iforest:buffer:{svc}"   # List[JSON str] — lịch sử feature vectors
    _KEY_KEYS    = "iforest:keys:{svc}"     # JSON str — list tên feature key (thứ tự cố định)
    _KEY_MODEL   = "iforest:model:{svc}"    # bytes — pickle+gzip fitted IsolationForest

    def __init__(self, r: redis.Redis):
        self.r = r

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #

    def fit_or_predict(
        self,
        service: str,
        feature_vector: Dict[str, float],
    ) -> Tuple[bool, float]:
        """
        Thêm feature_vector vào buffer, fit/predict nếu đủ dữ liệu.

        Returns:
            (is_anomaly, anomaly_score)
            anomaly_score: giá trị âm, càng nhỏ (âm sâu) → càng bất thường.
            Trả về (False, 0.0) khi chưa đủ dữ liệu warm-up.
        """
        # 1. Cập nhật danh sách key toàn cục
        global_keys = self._get_keys(service)
        new_keys = [k for k in feature_vector if k not in global_keys]
        if new_keys:
            global_keys = global_keys + new_keys
            self._save_keys(service, global_keys)
            # Reset model khi số chiều thay đổi
            self._delete_model(service)

        # 2. Align vector theo global_keys (padding 0 cho key vắng mặt)
        aligned = [feature_vector.get(k, 0.0) for k in global_keys]

        # 3. Lưu vào buffer
        self._push_to_buffer(service, aligned)
        buffer = self._get_buffer(service)

        # 4. Warm-up: chưa đủ dữ liệu
        if len(buffer) < IFOREST_MIN_TRAIN_WINDOWS:
            log.debug(
                "iForest [%s] warm-up %d/%d",
                service, len(buffer), IFOREST_MIN_TRAIN_WINDOWS,
            )
            return False, 0.0

        # 5. Fit (hoặc load model đã fit)
        model = self._load_model(service)

        # Pad các vector cũ trong buffer cho cùng chiều với global_keys hiện tại.
        # Khi có key mới, vector cũ ngắn hơn → np.array() sẽ lỗi nếu không pad.
        n_features = len(global_keys)
        padded_buffer = []
        for vec in buffer:
            if len(vec) < n_features:
                vec = vec + [0.0] * (n_features - len(vec))
            padded_buffer.append(vec[:n_features])  # cắt nếu dài hơn (phòng thủ)

        X = np.array(padded_buffer, dtype=float)

        # Re-fit nếu model chưa có hoặc số chiều không khớp
        if model is None or X.shape[1] != model.n_features_in_:
            log.info("iForest [%s] fitting trên %d windows, %d features...",
                     service, X.shape[0], X.shape[1])
            model = IsolationForest(
                n_estimators=IFOREST_N_ESTIMATORS,
                contamination=IFOREST_CONTAMINATION,
                random_state=42,
            )
            model.fit(X)
            self._save_model(service, model)

        # 6. Predict window hiện tại
        x_current = np.array([aligned], dtype=float)
        prediction = model.predict(x_current)[0]   # 1 = normal, -1 = anomaly
        score = float(model.score_samples(x_current)[0])  # càng âm → càng lạ

        is_anomaly = (prediction == -1)
        return is_anomaly, score

    # ------------------------------------------------------------------ #
    #  Redis helpers
    # ------------------------------------------------------------------ #

    def _buf_key(self, svc: str) -> str:
        return self._KEY_BUFFER.format(svc=svc)

    def _keys_key(self, svc: str) -> str:
        return self._KEY_KEYS.format(svc=svc)

    def _model_key(self, svc: str) -> str:
        return self._KEY_MODEL.format(svc=svc)

    def _get_keys(self, svc: str) -> List[str]:
        raw = self.r.get(self._keys_key(svc))
        if raw is None:
            return []
        try:
            return json.loads(raw)
        except Exception:
            return []

    def _save_keys(self, svc: str, keys: List[str]) -> None:
        self.r.set(self._keys_key(svc), json.dumps(keys))

    def _push_to_buffer(self, svc: str, aligned: List[float]) -> None:
        """Giữ tối đa 200 window gần nhất."""
        key = self._buf_key(svc)
        self.r.rpush(key, json.dumps(aligned))
        self.r.ltrim(key, -200, -1)

    def _get_buffer(self, svc: str) -> List[List[float]]:
        raw_list = self.r.lrange(self._buf_key(svc), 0, -1)
        result = []
        for raw in raw_list:
            try:
                result.append(json.loads(raw))
            except Exception:
                pass
        return result

    def _load_model(self, svc: str):
        raw = self.r.get(self._model_key(svc))
        if raw is None:
            return None
        try:
            return pickle.loads(gzip.decompress(raw))
        except Exception:
            return None

    def _save_model(self, svc: str, model: IsolationForest) -> None:
        payload = gzip.compress(pickle.dumps(model))
        self.r.set(self._model_key(svc), payload)

    def _delete_model(self, svc: str) -> None:
        self.r.delete(self._model_key(svc))
