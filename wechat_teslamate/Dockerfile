# syntax = docker/dockerfile:1.4
FROM python:3.11-slim AS builder

# 安装编译依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libpq-dev \
    libffi-dev \
    libssl-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 先装最慢的几个包（固定版本，优先用 wheel）
COPY requirements.txt .
RUN pip install --no-cache-dir --prefer-binary \
    numpy==1.24.3 \
    psycopg2-binary==2.9.9 \
    asyncpg==0.29.0 \
    paho-mqtt==1.6.1 \
    urllib3==2.0.7 \
    certifi==2024.2.2 \
    charset-normalizer==3.3.2 \
    idna==3.6 \
    requests==2.31.0 \
    pytz==2024.1 \
    schedule==1.2.0 \
    PyJWT==2.8.0 \
    urllib3==1.26.18

# 再装剩余 requirements（复用上层缓存）
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# 运行时镜像（多阶段构建，极小）
FROM python:3.11-slim
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
WORKDIR /app
COPY . .
CMD ["python", "wechat_teslamate.py"]
