"""
aggregator/param_extractor.py
Bóc tách giá trị số từ raw_message bằng cách align với template Drain/LLM.

Template ví dụ : "Connection timeout to <*> after <*> ms"
raw_message    : "Connection timeout to 192.168.1.5 after 3000 ms"
Kết quả        : {"param_1": 3000.0}   # param_0 (IP) không phải số nên bỏ qua
"""

import re
from functools import lru_cache
from typing import Dict

_PLACEHOLDER = "<*>"


@lru_cache(maxsize=2048)
def _build_pattern(template: str) -> re.Pattern:
    """
    Biên dịch template thành regex pattern — cached theo template string.
    Split trên <*> trước, escape từng đoạn literal, sau đó ghép capture group.
    Cache LRU 2048 entry: đủ cho số lượng template thực tế (thường < 500).
    """
    parts = template.split(_PLACEHOLDER)
    pattern = "(.+?)".join(re.escape(p) for p in parts)
    return re.compile(f"^{pattern}$", re.DOTALL)


def extract_params(template: str, raw_message: str) -> Dict[str, float]:
    """
    Trả về dict {param_N: float} cho các capture group có thể cast sang số.
    Trả về {} nếu:
      - template không có <*>
      - regex không match raw_message
      - không có group nào là số
    """
    if _PLACEHOLDER not in template:
        return {}

    try:
        pattern = _build_pattern(template)
        m = pattern.match(raw_message.strip())
        if not m:
            return {}

        result: Dict[str, float] = {}
        for i, group in enumerate(m.groups()):
            try:
                result[f"param_{i}"] = float(group)
            except (ValueError, TypeError):
                pass  # bỏ qua các group không phải số (IP, path, string, ...)

        return result

    except Exception:
        # Không bao giờ crash vòng lặp chính của aggregator
        return {}
