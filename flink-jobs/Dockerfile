# ================================================================
# Dockerfile — flink-jobs/
# Base: Flink 1.18 chính thức, cài thêm Python + dependencies
# ================================================================

FROM flink:1.18.1-scala_2.12-java11

# --- Cài Python 3.10 ---
RUN apt-get update && apt-get install -y \
        python3.10 \
        python3.10-dev \
        python3-pip \
        wget \
    && ln -sf /usr/bin/python3.10 /usr/bin/python3 \
    && ln -sf /usr/bin/python3.10 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# --- Cài Python dependencies từ requirements.txt ---
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# --- Tải Kafka Connector JAR ---
# Tên package đúng cho Flink 1.18.x trên Maven Central là:
# flink-sql-connector-kafka phiên bản 3.1.0-1.18
RUN wget -q \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" \
    -O /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
    && echo "✓ Kafka connector JAR downloaded"

# --- Copy toàn bộ code vào image ---
WORKDIR /opt/flink/usrlib
COPY stream_processor.py .
COPY config.yaml         .
COPY utils/              ./utils/

# Biến môi trường mặc định
ENV PYTHONUNBUFFERED=1