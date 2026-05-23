import time
import json
import random
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
import config

def create_producer():
    # Khởi tạo Kafka Producer
    # value_serializer: Tự động chuyển đổi dictionary của Python thành chuỗi JSON và mã hóa UTF-8
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def simulate_stream():
    producer = create_producer()
    print(f"[*] Đã kết nối thành công tới Kafka Brokers: {config.KAFKA_BROKERS}")
    print(f"[*] Bắt đầu đọc luồng dữ liệu từ: {config.DATA_FILE}")

    # Đọc dữ liệu theo từng lô (chunk) 1000 dòng để tránh tràn RAM
    chunksize = 1000
    try:
        for chunk in pd.read_csv(config.DATA_FILE, chunksize=chunksize):
            
            # Tiền xử lý nhẹ: Lọc bỏ các cột NaN nếu có để tránh lỗi JSON
            chunk = chunk.fillna("")

            for index, row in chunk.iterrows():
                # Chuyển dòng dữ liệu thành dạng Dictionary
                transaction = row.to_dict()

                # BÍ QUYẾT TINH TẾ Ở ĐÂY:
                # Dữ liệu gốc trong CSV có timestamp tĩnh (trong quá khứ).
                # Ta cấy thêm thời gian thực tại thời điểm phát sinh để Flink/Spark 
                # có thể dùng mốc này làm Event Time (tính Velocity, Time Window...)
                transaction['ingestion_timestamp'] = datetime.utcnow().isoformat()
                
                # Bắn gói JSON vào Topic
                producer.send(config.TOPIC_NAME, transaction)

                # Mô phỏng độ trễ ngẫu nhiên giữa các giao dịch
                # Giúp dữ liệu sinh ra có nhịp điệu tự nhiên, tạo độ nhiễu cần thiết để test thuật toán
                sleep_duration = random.uniform(config.SLEEP_TIME_MIN, config.SLEEP_TIME_MAX)
                time.sleep(sleep_duration)

            # Ép đẩy dữ liệu còn tồn đọng trong buffer lên Kafka
            producer.flush()
            print(f"[+] Đã bắn thành công lô {chunksize} giao dịch vào topic '{config.TOPIC_NAME}'...")
            
    except FileNotFoundError:
        print(f"[!] LỖI: Không tìm thấy file {config.DATA_FILE}. Hãy kiểm tra lại cấu hình Volumes trong Docker.")
    except Exception as e:
        print(f"[!] Đã xảy ra lỗi trong quá trình streaming: {e}")

if __name__ == "__main__":
    print("=== KÍCH HOẠT HỆ THỐNG MÔ PHỎNG GIAO DỊCH ===")
    simulate_stream()