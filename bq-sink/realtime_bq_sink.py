import os
import json
import time
from kafka import KafkaConsumer
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# --- CẤU HÌNH ---
# Chạy trong Docker nên trỏ vào kafka1 thay vì localhost
# KAFKA_BROKER = 'kafka1:9092' 
KAFKA_BROKER = "kafka1:9092,kafka2:9092,kafka3:9092"
TOPIC_NAME = 'fraud_alerts'
PROJECT_ID = 'project-7942816b-eab8-4949-8b8' 
DATASET_ID = 'fraud_features'
TABLE_ID = 'realtime_fraud_logs'

def create_table_if_not_exists(client, table_ref):
    """Kiểm tra và tự động tạo bảng trên BigQuery nếu chưa có"""
    try:
        client.get_table(table_ref)
        print(f"[*] Bảng {TABLE_ID} đã tồn tại. Sẵn sàng ghi dữ liệu.")
    except NotFound:
        print(f"[*] Chưa có bảng {TABLE_ID}. Đang tiến hành tạo mới...")
        # Định nghĩa đúng các cột mà luồng Flink đẩy ra
        schema = [
            bigquery.SchemaField("cc_num", "STRING"),
            bigquery.SchemaField("trans_date_trans_time", "STRING"),
            bigquery.SchemaField("amt", "FLOAT"),
            bigquery.SchemaField("ml_fraud_probability", "FLOAT"),
            bigquery.SchemaField("is_fraud_predicted", "INTEGER"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"[+] Khởi tạo bảng {TABLE_ID} thành công!")

def main():
    # 1. Kết nối BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # 2. Tự động khởi tạo bảng
    create_table_if_not_exists(client, table_ref)

    # 3. Lắng nghe ống Kafka (Nâng cấp tính năng Kiên Nhẫn)
    print(f"[*] Đang kết nối tới Kafka {KAFKA_BROKER}...")
    consumer = None
    retries = 0
    while retries < 10:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER.split(','),
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"[+] Kết nối Kafka thành công!")
            break
        except Exception as e:
            retries += 1
            print(f"[*] Kafka chưa sẵn sàng. Đợi 5 giây rồi thử lại... (Lần {retries}/10)")
            time.sleep(5)

    if not consumer:
        raise Exception("Không thể kết nối Kafka. Vui lòng kiểm tra lại hệ thống!")

    print(f"[*] Đang trực chiến chờ dữ liệu từ Kafka Topic: {TOPIC_NAME}...")

    # 4. Hút và Ghi tốc độ cao
    batch = []
    for message in consumer:
        record = message.value
        batch.append(record)

        # Hễ có cảnh báo là đẩy lên mẻ nhỏ (có thể set len(batch) >= 1 để đẩy real-time tức thời)
        if len(batch) >= 100: 
            errors = client.insert_rows_json(table_ref, batch)
            if not errors:
                print(f"[+] Đã chốt hạ {len(batch)} cảnh báo lên BigQuery!")
            else:
                print(f"[!] Lỗi khi ghi BigQuery: {errors}")
            batch = []

if __name__ == "__main__":
    main()