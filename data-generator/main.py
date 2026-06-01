import time
import json
import os
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
import config

def create_producer():
    max_retries = 20
    retries = 0
    
    # Hỗ trợ truyền mảng nhiều broker từ docker-compose (phân cách bằng dấu phẩy)
    brokers = os.getenv('KAFKA_BROKERS', config.KAFKA_BROKERS).split(',')
    
    while retries < max_retries:
        try:
            # TỐI ƯU HÓA SIÊU TỐC CHO KAFKA PRODUCER
            producer = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                linger_ms=20,           # Chờ 20ms để gom nhiều message thành 1 cục
                batch_size=262144,      # Tăng RAM cho 1 mẻ lên 256KB
                compression_type='lz4', # BẮT BUỘC CÓ: Nén dữ liệu để chống nghẽn mạng TCP
                acks=1                  # Tăng tốc: Chỉ cần Leader Broker xác nhận là đi tiếp
            )
            return producer
        except Exception as e:
            retries += 1
            print(f"[*] Kafka chưa sẵn sàng (Lỗi: {e}). Đợi 5s... ({retries}/{max_retries})")
            time.sleep(5)
    raise Exception("[!] Không thể kết nối Kafka.")

def simulate_stream():
    producer = create_producer()
    print(f"[*] Đã kết nối Kafka Brokers: {os.getenv('KAFKA_BROKERS', config.KAFKA_BROKERS)}")
    
    # 1. CẤU HÌNH KỊCH BẢN BENCHMARK
    environment = os.getenv("ENV", "local").lower() 
    
    if environment == "benchmark":
        # ==========================================
        # MÔI TRƯỜNG GCE (CHẠY TRÊN CLOUD)
        # ==========================================
        # Bám sát đúng cam kết NFR trong báo cáo đồ án: 100 TPS
        TARGET_PER_MINUTE = 6000     # Tương đương 100 TPS (Giao dịch/giây)
        DEMO_DURATION_SECONDS = 600  # Chạy demo liên tục trong 10 phút (600s)
        TOTAL_TARGET = 60000         # 100 TPS * 600s = 60.000 giao dịch
    else:
        # ==========================================
        # MÔI TRƯỜNG LOCAL (CHẠY TRÊN MÁY TÍNH CÁ NHÂN)
        # ==========================================
        # Máy cá nhân chạy Docker thường bị giới hạn CPU/RAM, 
        # nên để 30 TPS để Flink XGBoost xử lý mượt mà, không bị dồn ứ.
        TARGET_PER_MINUTE = 1800     # Tương đương 30 TPS
        DEMO_DURATION_SECONDS = 300  # Chạy demo nhanh trong 5 phút (300s)
        TOTAL_TARGET = 9000          # 30 TPS * 300s = 9.000 giao dịch
    TARGET_TPS = TARGET_PER_MINUTE // 60 
    
    print(f"[*] CHẾ ĐỘ CHẠY: {environment.upper()}")
    print(f"[*] MỤC TIÊU: {TARGET_TPS} msgs/giây")
    print(f"[*] THỜI GIAN: {DEMO_DURATION_SECONDS} Giây (Hoặc đạt mốc {TOTAL_TARGET} msgs)\n")

    # ==========================================================
    # CHIẾN THUẬT WAIT-FOR-IT ĐƯỢC CHUYỂN SANG ĐÂY
    # ==========================================================
    print("\n⏳ DATA GENERATOR ĐANG CHỜ 40 GIÂY ĐỂ FLINK WARM-UP...")
    time.sleep(40)
    print("🚀 BẮT ĐẦU XẢ LŨ DỮ LIỆU!\n")

  
    # 2. KHỞI TẠO BỘ ĐẾM
    demo_start_time = time.time()
    second_start_time = time.time()
    messages_sent_this_second = 0
    total_messages_sent = 0
    loop_count = 0

    # TỐI ƯU 1: Tăng chunksize lên rất lớn để giảm I/O ổ cứng
    chunksize = 100000 

    while True:
        # Điều kiện thoát tổng
        if time.time() - demo_start_time >= DEMO_DURATION_SECONDS or total_messages_sent >= TOTAL_TARGET:
            break

        loop_count += 1
        print(f"\n[*] Đang đọc CSV vòng {loop_count}...")
        
        for chunk in pd.read_csv(config.DATA_FILE, chunksize=chunksize):
            chunk = chunk.fillna("")
            
            # TỐI ƯU 2: Biến cả dataframe thành list dictionary siêu nhanh (bỏ iterrows)
            records = chunk.to_dict('records')
            
            for row in records:
                # Cập nhật thời gian nhanh gọn (bỏ parse strptime)
                now = datetime.now()
                row['trans_date_trans_time'] = now.strftime("%Y-%m-%d %H:%M:%S")
                row['unix_time'] = int(now.timestamp())
                row['ingestion_timestamp'] = now.isoformat()
                row['ingestion_time_ms'] = int(time.time() * 1000)
                # Bắn vào Kafka (Lệnh này bất đồng bộ, đẩy thẳng vào RAM của Kafka C client)
                producer.send(config.TOPIC_NAME, row)
                
                messages_sent_this_second += 1
                total_messages_sent += 1

                # 3. THUẬT TOÁN GIỮ NHỊP TỐI ƯU
                if messages_sent_this_second >= TARGET_TPS:
                    elapsed_time_this_second = time.time() - second_start_time
                    
                    if elapsed_time_this_second < 1.0:
                        time.sleep(1.0 - elapsed_time_this_second)
                    
                    # Reset cho giây tiếp theo
                    second_start_time = time.time()
                    messages_sent_this_second = 0

                    # In log mỗi 5 giây 1 lần để terminal không bị lag giật
                    elapsed_demo = int(time.time() - demo_start_time)
                    if elapsed_demo % 5 == 0:
                        print(f"[{elapsed_demo}s / {DEMO_DURATION_SECONDS}s] Đã bắn {total_messages_sent:,} giao dịch...")

                # Cắt cầu dao khẩn cấp nếu đủ số lượng hoặc hết giờ
                if total_messages_sent >= TOTAL_TARGET or (time.time() - demo_start_time >= DEMO_DURATION_SECONDS):
                    break
            
            # TỐI ƯU 3: Đã XÓA lệnh producer.flush() ở đây. Để cho hệ thống tự đẩy.
            if total_messages_sent >= TOTAL_TARGET or (time.time() - demo_start_time >= DEMO_DURATION_SECONDS):
                break

    print("\n" + "="*50)
    print(f"[!] ĐÃ KẾT THÚC BENCHMARK.")
    print(f"[!] TỔNG SỐ GIAO DỊCH THỰC TẾ: {total_messages_sent:,}")
    print("="*50)

    # Chỉ flush duy nhất 1 lần ở cuối cùng để vét cạn RAM
    print("[*] Đang xả bộ đệm (Flushing) nốt lên Kafka...")
    producer.flush()
    producer.close()

if __name__ == "__main__":
    simulate_stream()