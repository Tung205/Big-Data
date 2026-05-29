# import time
# import json
# import random
# import os
# import pandas as pd
# from datetime import datetime, timedelta
# from kafka import KafkaProducer
# import config

# def create_producer():
#     max_retries = 5
#     retries = 0
#     while retries < max_retries:
#         try:
#             # Tối ưu hóa Producer cho High Throughput
#             producer = KafkaProducer(
#                 bootstrap_servers=config.KAFKA_BROKERS,
#                 value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#                 linger_ms=50,       # Gom các message lại trong 50ms trước khi gửi để tăng tốc
#                 batch_size=131072   # Tăng kích thước lô (batch) lên 128KB
#             )
#             return producer
#         except Exception as e:
#             retries += 1
#             print(f"[*] Kafka chưa sẵn sàng (Lỗi: {e}). Đợi 5s... ({retries}/{max_retries})")
#             time.sleep(5)
#     raise Exception("[!] Không thể kết nối Kafka.")

# def simulate_stream():
#     producer = create_producer()
#     print(f"[*] Đã kết nối thành công tới Kafka Brokers: {config.KAFKA_BROKERS}")
    
#     # 1. CẤU HÌNH KỊCH BẢN DEMO (Lấy từ biến môi trường hoặc file config)
#     # Nếu đang chạy trên GCE thì set biến môi trường ENV=cloud, chạy ở máy em thì ENV=local
#     environment = os.getenv("ENV", "local").lower() 
    
#     if environment == "cloud":
#         TARGET_PER_MINUTE = 1000000
#     else:
#         TARGET_PER_MINUTE = 1000
        
#     TARGET_TPS = TARGET_PER_MINUTE // 60  # Số giao dịch cần bắn trong 1 giây
#     DEMO_DURATION_SECONDS = 5 * 60        # Demo đúng 5 phút (300 giây)
    
#     print(f"[*] CHẾ ĐỘ DEMO: {environment.upper()}")
#     print(f"[*] MỤC TIÊU: {TARGET_PER_MINUTE} messages/phút (~{TARGET_TPS} msgs/giây)")
#     print(f"[*] THỜI GIAN DEMO: 5 Phút\n")

#     # Lấy thông tin tịnh tiến thời gian
#     try:
#         df_temp = pd.read_csv(config.DATA_FILE, usecols=['trans_date_trans_time'])
#         first_time_obj = datetime.strptime(df_temp.iloc[0]['trans_date_trans_time'], "%Y-%m-%d %H:%M:%S")
#         dataset_duration = (datetime.strptime(df_temp.iloc[-1]['trans_date_trans_time'], "%Y-%m-%d %H:%M:%S") - first_time_obj)
#     except Exception as e:
#         print(f"[!] Lỗi đọc dataset: {e}"); return

#     target_date = datetime.now().date() - timedelta(days=1)
#     time_shift_delta = target_date - first_time_obj.date()

#     # 2. KHỞI TẠO BỘ ĐẾM VÀ ĐỒNG HỒ DEMO
#     demo_start_time = time.time()
#     second_start_time = time.time()
#     messages_sent_this_second = 0
#     total_messages_sent = 0
#     loop_count = 0

#     # Tăng chunksize để Pandas đọc file nhanh hơn
#     chunksize = 10000 

#     while True:
#         # Kiểm tra xem đã hết 5 phút demo chưa
#         if time.time() - demo_start_time >= DEMO_DURATION_SECONDS:
#             print("\n" + "="*50)
#             print(f"[!] ĐÃ HẾT 5 PHÚT DEMO. DỪNG SINH DỮ LIỆU.")
#             print(f"[!] TỔNG SỐ GIAO DỊCH ĐÃ BẮN: {total_messages_sent}")
#             print("="*50)
#             break

#         loop_count += 1
#         print(f"\n[*] Đang đọc file vòng {loop_count}...")
        
#         for chunk in pd.read_csv(config.DATA_FILE, chunksize=chunksize):
#             # Kiểm tra thời gian demo ngay trong lúc duyệt chunk
#             if time.time() - demo_start_time >= DEMO_DURATION_SECONDS:
#                 break
                
#             chunk = chunk.fillna("")
            
#             for index, row in chunk.iterrows():
#                 # Xử lý thời gian
#                 orig_time_str = row['trans_date_trans_time']
#                 try:
#                     orig_time_obj = datetime.strptime(orig_time_str, "%Y-%m-%d %H:%M:%S")
#                 except ValueError:
#                     continue

#                 new_time_obj = orig_time_obj + time_shift_delta + timedelta(seconds=random.randint(-15, 15))
                
#                 transaction = row.to_dict()
#                 transaction['trans_date_trans_time'] = new_time_obj.strftime("%Y-%m-%d %H:%M:%S")
#                 transaction['unix_time'] = int(new_time_obj.timestamp())
#                 transaction['ingestion_timestamp'] = datetime.utcnow().isoformat()
                
#                 # Bắn vào Kafka
#                 producer.send(config.TOPIC_NAME, transaction)
                
#                 messages_sent_this_second += 1
#                 total_messages_sent += 1

#                 # 3. THUẬT TOÁN GIỮ NHỊP (RATE LIMITING)
#                 if messages_sent_this_second >= TARGET_TPS:
#                     # Đã bắn đủ chỉ tiêu của 1 giây, kiểm tra xem đã hết 1 giây chưa
#                     elapsed_time_this_second = time.time() - second_start_time
                    
#                     if elapsed_time_this_second < 1.0:
#                         # Nếu bắn quá nhanh (chưa tới 1s mà đã xong), thì bắt nó ngủ phần thời gian còn lại
#                         time.sleep(1.0 - elapsed_time_this_second)
                    
#                     # Reset lại bộ đếm cho giây tiếp theo
#                     messages_sent_this_second = 0
#                     second_start_time = time.time()

#                     # Chỉ in log 1 lần mỗi giây thay vì in từng message để tránh treo máy
#                     elapsed_demo = int(time.time() - demo_start_time)
#                     print(f"[Demo Time: {elapsed_demo}s/300s] Đã gửi {TARGET_TPS} giao dịch...")
                    
#             # Bắt buộc xả buffer sau mỗi chunk để dữ liệu đi ngay
#             producer.flush()

#         time_shift_delta += dataset_duration

#     # Đảm bảo các message cuối cùng được gửi đi
#     producer.flush()
#     producer.close()

# if __name__ == "__main__":
#     simulate_stream()


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
        TARGET_PER_MINUTE = 50000  # 100k msgs/phút
        DEMO_DURATION_SECONDS = 600  # 10 phút
        TOTAL_TARGET = 500000    
    else:
        TARGET_PER_MINUTE = 6000    # 1000 TPS cho test local nhẹ nhàng
        DEMO_DURATION_SECONDS = 300  # 5 phút
        TOTAL_TARGET = 200000
        
    TARGET_TPS = TARGET_PER_MINUTE // 60 
    
    print(f"[*] CHẾ ĐỘ CHẠY: {environment.upper()}")
    print(f"[*] MỤC TIÊU: {TARGET_TPS} msgs/giây")
    print(f"[*] THỜI GIAN: {DEMO_DURATION_SECONDS} Giây (Hoặc đạt mốc {TOTAL_TARGET} msgs)\n")

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