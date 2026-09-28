# Dùng bản Python tối giản để nhẹ máy
FROM python:3.10-slim

WORKDIR /app

# Copy requirements của từng module
COPY logparser/requirements.txt logparser/
COPY aggregator/requirements.txt aggregator/
COPY ai/requirements.txt ai/
COPY collector/requirements.txt collector/

# Cài đặt dependencies
RUN pip install --no-cache-dir -r logparser/requirements.txt
RUN pip install --no-cache-dir -r aggregator/requirements.txt
RUN pip install --no-cache-dir -r ai/requirements.txt
RUN pip install --no-cache-dir -r collector/requirements.txt

# Copy toàn bộ source code
COPY . .

# Set biến môi trường để Python hiểu thư mục gốc
ENV PYTHONPATH=/app
