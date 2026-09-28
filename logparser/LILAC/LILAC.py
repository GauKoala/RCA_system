import regex as re
import json
from .parsing_cache import ParsingCache

def load_regs():
    regs_common = []
    try:
        with open("logparser/LILAC/common.json", "r") as fr:
            dic = json.load(fr)
        
        patterns = dic['COMMON']['regex']
        for pattern in patterns:
            regs_common.append(re.compile(pattern))
    except Exception:
        pass
    return regs_common

class LILACNormalizer:
    def __init__(self, cache: ParsingCache):
        self.cache = cache

    def process(self, message: str):
        # results: (template, template_id, parameter_str) or ("NoMatch", "NoMatch", relevant_templates)
        results = self.cache.match_event(message)
        if results[0] == "NoMatch":
            return message, None, 0.0, False
        
        template = results[0]
        cluster_id = results[1]
        confidence = 1.0
        is_known = True
        return template, cluster_id, confidence, is_known
