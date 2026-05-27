# Dùng bản Python 3.9 trên nền Debian 11 (Bullseye) để chắc chắn có Java 11
FROM python:3.9-slim-bullseye

# Cài đặt Java 11 (Phiên bản ổn định nhất cho Apache Spark)
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Lệnh khởi chạy luồng Spark Structured Streaming
CMD ["python", "batch_feature_pipeline.py"]