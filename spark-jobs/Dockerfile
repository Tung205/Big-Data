# # Dùng bản Python 3.9 trên nền Debian 11 (Bullseye) để chắc chắn có Java 11
# FROM python:3.9-slim-bullseye

# # Cài đặt Java 11 (Phiên bản ổn định nhất cho Apache Spark)
# RUN mkdir -p /usr/share/man/man1 && \
#     apt-get update && \
#     apt-get install -y openjdk-11-jre-headless && \
#     apt-get clean && rm -rf /var/lib/apt/lists/*

# WORKDIR /app

# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# COPY . .

# # Lệnh khởi chạy luồng Spark Structured Streaming
# CMD ["python", "batch_feature_pipeline.py"]

# 1. Dùng ảnh bộ chứa gốc
FROM apache/spark:3.4.1

# 2. Chuyển sang quyền root
USER root

# Cài đặt Python pip
RUN apt-get update && \
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

# 3. FIX LỖI TẢI THƯ VIỆN: Tạo sẵn thư mục cache và cấp quyền cho user spark
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark

WORKDIR /app
COPY requirements.txt .

# 4. Cài đặt các thư viện Python
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

# 5. Trả lại quyền user 'spark'
USER spark

# 6. Khởi động Job
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2", \
     "batch_feature_pipeline.py"]