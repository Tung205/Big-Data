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
        curl \
    && ln -sf /usr/bin/python3.10 /usr/bin/python3 \
    && ln -sf /usr/bin/python3.10 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# --- Cài Python dependencies từ requirements.txt ---
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# --- Tải Kafka Connector JAR (Flink cần JAR này để đọc từ Kafka) ---
RUN curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.18.0/flink-sql-connector-kafka-1.18.0.jar" \
    -o /opt/flink/lib/flink-sql-connector-kafka-1.18.0.jar

# --- Copy toàn bộ code vào image ---
WORKDIR /opt/flink/usrlib
COPY stream_processor.py .
COPY config.yaml         .
COPY utils/              ./utils/

# Biến môi trường mặc định (ghi đè bằng docker-compose nếu cần)
ENV PYTHONUNBUFFERED=1