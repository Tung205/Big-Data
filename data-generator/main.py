import time
import json
import random
import os
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
import config

def create_producer():
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            # Tối ưu hóa Producer cho High Throughput
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                linger_ms=50,       # Gom các message lại trong 50ms trước khi gửi để tăng tốc
                batch_size=131072   # Tăng kích thước lô (batch) lên 128KB
            )
            return producer
        except Exception as e:
            retries += 1
            print(f"[*] Kafka chưa sẵn sàng (Lỗi: {e}). Đợi 5s... ({retries}/{max_retries})")
            time.sleep(5)
    raise Exception("[!] Không thể kết nối Kafka.")

def simulate_stream():
    producer = create_producer()
    print(f"[*] Đã kết nối thành công tới Kafka Brokers: {config.KAFKA_BROKERS}")
    
    # 1. CẤU HÌNH KỊCH BẢN DEMO (Lấy từ biến môi trường hoặc file config)
    # Nếu đang chạy trên GCE thì set biến môi trường ENV=cloud, chạy ở máy em thì ENV=local
    environment = os.getenv("ENV", "local").lower() 
    
    if environment == "cloud":
        TARGET_PER_MINUTE = 1000000
    else:
        TARGET_PER_MINUTE = 1000
        
    TARGET_TPS = TARGET_PER_MINUTE // 60  # Số giao dịch cần bắn trong 1 giây
    DEMO_DURATION_SECONDS = 5 * 60        # Demo đúng 5 phút (300 giây)
    
    print(f"[*] CHẾ ĐỘ DEMO: {environment.upper()}")
    print(f"[*] MỤC TIÊU: {TARGET_PER_MINUTE} messages/phút (~{TARGET_TPS} msgs/giây)")
    print(f"[*] THỜI GIAN DEMO: 5 Phút\n")

    # Lấy thông tin tịnh tiến thời gian
    try:
        df_temp = pd.read_csv(config.DATA_FILE, usecols=['trans_date_trans_time'])
        first_time_obj = datetime.strptime(df_temp.iloc[0]['trans_date_trans_time'], "%Y-%m-%d %H:%M:%S")
        dataset_duration = (datetime.strptime(df_temp.iloc[-1]['trans_date_trans_time'], "%Y-%m-%d %H:%M:%S") - first_time_obj)
    except Exception as e:
        print(f"[!] Lỗi đọc dataset: {e}"); return

    target_date = datetime.now().date() - timedelta(days=1)
    time_shift_delta = target_date - first_time_obj.date()

    # 2. KHỞI TẠO BỘ ĐẾM VÀ ĐỒNG HỒ DEMO
    demo_start_time = time.time()
    second_start_time = time.time()
    messages_sent_this_second = 0
    total_messages_sent = 0
    loop_count = 0

    # Tăng chunksize để Pandas đọc file nhanh hơn
    chunksize = 10000 

    while True:
        # Kiểm tra xem đã hết 5 phút demo chưa
        if time.time() - demo_start_time >= DEMO_DURATION_SECONDS:
            print("\n" + "="*50)
            print(f"[!] ĐÃ HẾT 5 PHÚT DEMO. DỪNG SINH DỮ LIỆU.")
            print(f"[!] TỔNG SỐ GIAO DỊCH ĐÃ BẮN: {total_messages_sent}")
            print("="*50)
            break

        loop_count += 1
        print(f"\n[*] Đang đọc file vòng {loop_count}...")
        
        for chunk in pd.read_csv(config.DATA_FILE, chunksize=chunksize):
            # Kiểm tra thời gian demo ngay trong lúc duyệt chunk
            if time.time() - demo_start_time >= DEMO_DURATION_SECONDS:
                break
                
            chunk = chunk.fillna("")
            
            for index, row in chunk.iterrows():
                # Xử lý thời gian
                orig_time_str = row['trans_date_trans_time']
                try:
                    orig_time_obj = datetime.strptime(orig_time_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue

                new_time_obj = orig_time_obj + time_shift_delta + timedelta(seconds=random.randint(-15, 15))
                
                transaction = row.to_dict()
                transaction['trans_date_trans_time'] = new_time_obj.strftime("%Y-%m-%d %H:%M:%S")
                transaction['unix_time'] = int(new_time_obj.timestamp())
                transaction['ingestion_timestamp'] = datetime.utcnow().isoformat()
                
                # Bắn vào Kafka
                producer.send(config.TOPIC_NAME, transaction)
                
                messages_sent_this_second += 1
                total_messages_sent += 1

                # 3. THUẬT TOÁN GIỮ NHỊP (RATE LIMITING)
                if messages_sent_this_second >= TARGET_TPS:
                    # Đã bắn đủ chỉ tiêu của 1 giây, kiểm tra xem đã hết 1 giây chưa
                    elapsed_time_this_second = time.time() - second_start_time
                    
                    if elapsed_time_this_second < 1.0:
                        # Nếu bắn quá nhanh (chưa tới 1s mà đã xong), thì bắt nó ngủ phần thời gian còn lại
                        time.sleep(1.0 - elapsed_time_this_second)
                    
                    # Reset lại bộ đếm cho giây tiếp theo
                    messages_sent_this_second = 0
                    second_start_time = time.time()

                    # Chỉ in log 1 lần mỗi giây thay vì in từng message để tránh treo máy
                    elapsed_demo = int(time.time() - demo_start_time)
                    print(f"[Demo Time: {elapsed_demo}s/300s] Đã gửi {TARGET_TPS} giao dịch...")
                    
            # Bắt buộc xả buffer sau mỗi chunk để dữ liệu đi ngay
            producer.flush()

        time_shift_delta += dataset_duration

    # Đảm bảo các message cuối cùng được gửi đi
    producer.flush()
    producer.close()

if __name__ == "__main__":
    simulate_stream()