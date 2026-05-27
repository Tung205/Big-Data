import os

# Địa chỉ Kafka Broker (Lấy từ biến môi trường, mặc định là kafka1 của docker-compose)
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka1:9092").split(",")

# Tên Topic chứa dữ liệu thô
TOPIC_NAME = os.getenv("TOPIC_NAME", "raw_transactions")

# Đường dẫn tới file CSV (File này Mount vào Container qua Docker Compose)
DATA_FILE = os.getenv("DATA_FILE", "/app/data/fraudTrain.csv")