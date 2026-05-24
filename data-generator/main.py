import time
import json
import random
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
import config

def create_producer():
    max_retries = 5
    retries = 0
    
    while retries < max_retries:
        try:
            # Cố gắng kết nối với Kafka
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return producer
        except Exception as e:
            retries += 1
            print(f"[*] Kafka chưa sẵn sàng (Lỗi: {e}). Đang đợi và thử lại ({retries}/{max_retries})...")
            time.sleep(5)  # Nghỉ 5 giây rồi gõ cửa tiếp
            
    # Nếu thử 5 lần (25 giây) mà vẫn thất bại thì mới báo lỗi
    raise Exception("[!] Cảnh báo: Không thể kết nối đến Kafka sau nhiều lần thử. Hãy kiểm tra lại hệ thống!")

def simulate_stream():
    producer = create_producer()
    print(f"[*] Đã kết nối thành công tới Kafka Brokers: {config.KAFKA_BROKERS}")
    
    # Tính độ dài của toàn bộ tập dữ liệu gốc (từ dòng đầu đến dòng cuối)
    # Để biết mỗi lần lặp lại, ta cần cộng thêm bao nhiêu ngày
    try:
        df_temp = pd.read_csv(config.DATA_FILE, usecols=['trans_date_trans_time'])
        first_time_obj = datetime.strptime(df_temp.iloc[0]['trans_date_trans_time'], "%Y-%m-%d %H:%M:%S")
        last_time_obj = datetime.strptime(df_temp.iloc[-1]['trans_date_trans_time'], "%Y-%m-%d %H:%M:%S")
        dataset_duration = (last_time_obj - first_time_obj)
        print(f"[*] Độ dài tập dữ liệu: {dataset_duration.days} ngày {dataset_duration.seconds // 3600} giờ.")
    except Exception as e:
        print(f"[!] Lỗi khi đọc độ dài dataset: {e}")
        return

    # Lấy ngày hôm qua làm mốc tịnh tiến cho vòng lặp ĐẦU TIÊN
    target_date = datetime.now().date() - timedelta(days=1)
    time_shift_delta = target_date - first_time_obj.date()
    
    loop_count = 0

    # VÒNG LẶP VÔ TẬN: Đọc hết file lại quay lại đọc từ đầu
    while loop_count<=1:
        loop_count += 1
        print(f"\n{'='*40}")
        print(f"=== BẮT ĐẦU ĐỌC FILE LẦN THỨ {loop_count} ===")
        print(f"=== Tịnh tiến thêm: {time_shift_delta.days} ngày ===")
        print(f"{'='*40}\n")
        
        chunksize = 1000
        for chunk in pd.read_csv(config.DATA_FILE, chunksize=chunksize):
            chunk = chunk.fillna("")
            
            for index, row in chunk.iterrows():
                transaction = row.to_dict()

                orig_time_str = transaction['trans_date_trans_time']
                try:
                    orig_time_obj = datetime.strptime(orig_time_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue

                # Cộng thêm thời gian tịnh tiến của vòng lặp hiện tại + nhiễu
                noise_seconds = random.randint(-15, 15)
                new_time_obj = orig_time_obj + time_shift_delta + timedelta(seconds=noise_seconds)

                transaction['trans_date_trans_time'] = new_time_obj.strftime("%Y-%m-%d %H:%M:%S")
                transaction['unix_time'] = int(new_time_obj.timestamp())
                transaction['ingestion_timestamp'] = datetime.utcnow().isoformat()
                
                producer.send(config.TOPIC_NAME, transaction)
                
                print("\n[+] ĐÃ BẮN GIAO DỊCH MỚI VÀO KAFKA:")
                print(json.dumps(transaction, indent=2, ensure_ascii=False))
                
                time.sleep(random.uniform(config.SLEEP_TIME_MIN, config.SLEEP_TIME_MAX))

        # KẾT THÚC 1 VÒNG ĐỌC FILE
        print(f"\n[*] ĐÃ ĐỌC HẾT FILE LẦN THỨ {loop_count}. CHUẨN BỊ VÒNG LẶP MỚI...")
        
        # Cập nhật mốc tịnh tiến cho vòng lặp tiếp theo
        # Cộng thêm toàn bộ độ dài của dataset để dòng thời gian luôn tiến về phía trước
        time_shift_delta += dataset_duration

if __name__ == "__main__":
    simulate_stream()