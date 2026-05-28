# Dùng nền Debian 11 (Bullseye) để hỗ trợ tốt Java 11
FROM python:3.9-slim-bullseye

# Cài đặt Java 11 (Bắt buộc) và công cụ wget để tải file
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y openjdk-11-jre-headless wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy và cài đặt thư viện Python
COPY requirements.txt .
RUN pip install --default-timeout=1000 --no-cache-dir -r requirements.txt

# Tải file JAR kết nối Kafka của Flink (Khớp với phiên bản 1.17.1)
RUN wget -P /app/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar

# Copy toàn bộ code và mô hình XGBoost vào
COPY . .

# Lệnh khởi chạy
CMD ["python", "flink_pipeline.py"]