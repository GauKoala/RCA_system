from langchain_core.tools import tool
import pandas as pd

@tool
def get_prometheus_metrics(service: str, time_range_mins: int = 5) -> str:
    """Truy vấn metric hệ thống (CPU, RAM, Net I/O) của service trong khoảng thời gian (phút). Dùng để kiểm tra service bị quá tải tài nguyên không."""
    try:
        # Trong môi trường Jupyter Notebook, lấy CASE_NAME từ global scope
        import __main__
        case_name = getattr(__main__, 'CASE_NAME', 're2ob_checkoutservice_cpu_1')
        
        path = f"data/RCAvel/{case_name}/metrics.parquet"
        df = pd.read_parquet(path)
        
        # Lấy các cột liên quan đến service này
        cols = [c for c in df.columns if service in c]
        if not cols:
            # Fallback nếu tên service chứa hậu tố -service nhưng metric không có
            service_short = service.replace("-service", "service")
            cols = [c for c in df.columns if service_short in c]
            if not cols:
                return f"Không tìm thấy metric nào cho service {service}."
        
        # Lấy dữ liệu 5 phút cuối (5 hàng cuối vì mỗi hàng là 1 phút hoặc 1 giây tuỳ dataset, ở đây lấy 10 dòng cuối)
        recent = df[cols].tail(10)
        # Tính trung bình để báo cáo cho LLM
        avg = recent.mean().to_dict()
        
        result = [f"- {k}: {v:.4f}" for k, v in avg.items()]
        return f"Dữ liệu Metric (CPU/Mem/Net) của {service} trong thời gian xảy ra sự cố:\n" + "\n".join(result)
    except Exception as e:
        return f"Lỗi đọc metric offline: {e}"

@tool
def get_dependency_map(service: str) -> str:
    """Lấy dependency map của service (những service khác mà nó tương tác) từ hệ thống Tracing (Jaeger)."""
    try:
        import __main__
        case_name = getattr(__main__, 'CASE_NAME', 're2ob_checkoutservice_cpu_1')
        
        path = f"data/RCAvel/{case_name}/traces.parquet"
        df = pd.read_parquet(path)
        
        # Cách lấy dependency từ trace: những service nào xuất hiện trong cùng 1 traceID với service hiện tại
        trace_ids = df[df['serviceName'] == service]['traceID'].unique()
        related_services = df[df['traceID'].isin(trace_ids)]['serviceName'].unique()
        deps = [s for s in related_services if s != service and s != 'unknown']
        
        if deps:
            return f"Trace Graph: {service} có gọi tới/được gọi bởi các dependencies sau: {', '.join(deps)}"
        return f"Không tìm thấy dependency rõ ràng nào cho {service} trong Tracing."
    except Exception as e:
        return f"Lỗi đọc trace offline: {e}"
