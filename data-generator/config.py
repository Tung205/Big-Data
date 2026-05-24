import os

# Địa chỉ 3 Kafka Broker của em (Lấy từ biến môi trường trong docker-compose)
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9092,kafka3:9092").split(",")

# Tên Topic chứa dữ liệu thô
TOPIC_NAME = os.getenv("TOPIC_NAME", "raw_transactions")

# Đường dẫn tới file CSV (sẽ được mount qua Docker Volumes)
DATA_FILE = os.getenv("DATA_FILE", "data/train.csv")

# Cấu hình nhịp độ sinh dữ liệu (Tính bằng giây)
# Việc để thời gian ngẫu nhiên giúp mô phỏng chân thực các đợt "cao điểm" và "thấp điểm" quẹt thẻ
SLEEP_TIME_MIN = 1 
SLEEP_TIME_MAX = 2