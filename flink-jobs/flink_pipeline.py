import json
import math
import time
from datetime import datetime
import xgboost as xgb

from pyflink.common import Types, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor

# --- CẤU HÌNH ---
# KAFKA_BROKER = "kafka1:9092"
INPUT_TOPIC = "raw_transactions"
OUTPUT_TOPIC = "fraud_alerts"
KAFKA_BROKER = "kafka1:9092,kafka2:9092,kafka3:9092"

# 1. HÀM STATELESS: XỬ LÝ ĐẶC TRƯNG CƠ BẢN (Haversine, Datetime, Age)
class BasicFeatureExtractor(MapFunction):
    def __init__(self):
        # Nạp sẵn mapping cho Label Encoding (Lấy từ quá trình train)
        self.gender_map = {'M': 1, 'F': 0}
        # Thêm toàn bộ category của em vào đây
        self.category_map = {
            'misc_net': 0, 'grocery_pos': 1, 'entertainment': 2, 'gas_transport': 3,
            # ... khai báo cho đủ các class
        }

    def calculate_haversine(self, lat1, lon1, lat2, lon2):
        R = 6371.0 # Bán kính Trái Đất (km)
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
        return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    def map(self, value):
        data = json.loads(value)
        
        # Datetime Extraction
        dt = datetime.strptime(data['trans_date_trans_time'], "%Y-%m-%d %H:%M:%S")
        data['hour'] = dt.hour
        data['day'] = dt.day
        data['month'] = dt.month
        data['day_of_week'] = dt.weekday()
        
        # Age Extraction
        dob = datetime.strptime(data['dob'], "%Y-%m-%d")
        data['age'] = dt.year - dob.year
        
        # Distance (Haversine)
        data['distance_km'] = self.calculate_haversine(
            data['lat'], data['long'], data['merch_lat'], data['merch_long']
        )
        
        # Label Encoding
        data['gender_encoded'] = self.gender_map.get(data['gender'], -1)
        data['category_encoded'] = self.category_map.get(data['category'], -1)
        
        return data

# 2. HÀM STATEFUL: QUẢN LÝ LỊCH SỬ GIAO DỊCH (Thời gian thực)
class VelocityAndHistoryProcess(KeyedProcessFunction):
    def open(self, runtime_context):
        # Bộ nhớ RAM 1: Lưu timestamp của giao dịch liền trước
        self.last_time_state = runtime_context.get_state(
            ValueStateDescriptor("last_time", Types.LONG())
        )
        # Bộ nhớ RAM 2: Lưu danh sách giao dịch trong 24h qua [ (timestamp, amount), ... ]
        self.history_1d_state = runtime_context.get_list_state(
            ListStateDescriptor("history_1d", Types.STRING()) # Dùng JSON string để lách luật pickle
        )

    # 1. Bỏ tham số 'out' đi
    def process_element(self, data, ctx):
        current_ts = data['unix_time']
        amt = float(data['amt'])

        # --- A. Tính time_since_last_trans_sec ---
        last_ts = self.last_time_state.value()
        if last_ts is None:
            data['time_since_last_trans_sec'] = 0.0
        else:
            data['time_since_last_trans_sec'] = float(current_ts - last_ts)
        self.last_time_state.update(current_ts)

        # --- B. Tính Velocity 1 Ngày (24h) ---
        history_list = self.history_1d_state.get()
        valid_history = []
        
        if history_list is not None:
            for item_str in history_list:
                item = json.loads(item_str)
                if current_ts - item['ts'] <= 86400:
                    valid_history.append(item)

        valid_history.append({'ts': current_ts, 'amt': amt})
        self.history_1d_state.update([json.dumps(x) for x in valid_history])

        data['velocity_1d_count'] = float(len(valid_history))
        data['velocity_1d_amt_sum'] = sum(x['amt'] for x in valid_history)

        # 2. Dùng yield để trả dữ liệu về luồng thay vì out.collect()
        yield data

# 3. HÀM AI: LOAD MODEL XGBOOST VÀ DỰ ĐOÁN
class FraudPredictor(MapFunction):
    def open(self, runtime_context):
        # RichMapFunction giúp model chỉ load đúng 1 lần khi Worker khởi động, không bị load lại mỗi dòng dữ liệu
        self.model = xgb.Booster()
        self.model.load_model('xgb_fraud_model.json')
        
        # Thứ tự các cột phải khớp 100% với lúc em train model
        self.feature_cols = [
            'amt', 'city_pop', 'hour', 'day', 'month', 'day_of_week', 'age', 
            'distance_km', 'time_since_last_trans_sec', 'velocity_1d_count', 
            'velocity_1d_amt_sum', 'gender_encoded', 'category_encoded'
        ]

    def map(self, data):
        # Rút trích đúng mảng đặc trưng
        feature_vector = [float(data.get(col, 0.0)) for col in self.feature_cols]
        
        # Đưa vào DMatrix của XGBoost
        dmatrix = xgb.DMatrix([feature_vector], feature_names=self.feature_cols)
        
        # Dự đoán (trả về xác suất)
        prob = self.model.predict(dmatrix)[0]
        
        # Gắn nhãn dự đoán (Threshold = 0.7)
        data['ml_fraud_probability'] = float(prob)
        data['is_fraud_predicted'] = 1 if prob >= 0.7 else 0
        
        # ======================================================
        # TÍNH TOÁN LATENCY (ĐỘ TRỄ) THEO CÔNG THỨC TRONG BÁO CÁO
        # ======================================================
        alert_time_ms = int(time.time() * 1000) 
        # Lấy ingestion_time_ms từ Kafka, nếu không có thì mặc định bằng alert_time
        ingestion_time_ms = int(data.get('ingestion_time_ms', alert_time_ms))
        
        # Công thức: Latency = alertTime - ingestionTime
        data['latency_ms'] = alert_time_ms - ingestion_time_ms
        data['alert_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] # Lưu lại mốc thời gian báo động
        # ======================================================


        # Chỉ giữ lại những cột cần thiết để ném ra Kafka / Dashboard
        alert_data = {
            'cc_num': data['cc_num'],
            'trans_date_trans_time': data['trans_date_trans_time'],
            'amt': data['amt'],
            'ml_fraud_probability': data['ml_fraud_probability'],
            'is_fraud_predicted': data['is_fraud_predicted'],
            'latency_ms': data['latency_ms'],
            'alert_time': data['alert_time'],
        }
        return json.dumps(alert_data)

def main():
    # ==========================================================
    # 1. CHIẾN THUẬT "NHƯỜNG ĐƯỜNG" (WAIT-FOR-IT)
    # ==========================================================
    print("\n" + "="*60)
    print("⏳ FLINK DRIVER ĐANG TẠM NGỦ 45 GIÂY...")
    print("⏳ Lý do: Chờ Kafka khởi động và Data Generator tạo Topic.")
    print("="*60 + "\n")

    for i in range(45, 0, -5):
        print(f"[*] Flink đang chờ... {i} giây còn lại")
        time.sleep(5)
        
    print("\n[+] ĐÃ HẾT GIỜ CHỜ! TIẾN HÀNH NẠP MODEL XGBOOST VÀO CỤM...\n")
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    env.add_jars("file:///app/flink-sql-connector-kafka-1.17.1.jar")

    # 1. Cấu hình đọc từ Kafka
    kafka_consumer = FlinkKafkaConsumer(
        topics=INPUT_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': KAFKA_BROKER, 'group.id': 'flink_fraud_group'}
    )
    
    stream = env.add_source(kafka_consumer)

    # 2. Xây dựng Data Pipeline
    processed_stream = stream \
        .map(BasicFeatureExtractor(), output_type=Types.PICKLED_BYTE_ARRAY()) \
        .key_by(lambda data: data['cc_num']) \
        .process(VelocityAndHistoryProcess(), output_type=Types.PICKLED_BYTE_ARRAY()) \
        .map(FraudPredictor(), output_type=Types.STRING())
    
    # --- ĐOẠN CODE THÊM MỚI ---
    # Lọc ra CHỈ những giao dịch bị AI phán quyết là gian lận (is_fraud_predicted == 1)
    fraud_only_stream = processed_stream.filter(
        lambda data_str: json.loads(data_str)['is_fraud_predicted'] == 1
    )

    # In ra màn hình Terminal với màu sắc/tiền tố để dễ nhận biết
    fraud_only_stream.map(
        lambda x: f"🚨 [GIAN LẬN] Thẻ: {json.loads(x)['cc_num']} | Tỉ lệ: {json.loads(x)['ml_fraud_probability']*100:.2f}% | Tiền: ${json.loads(x)['amt']} | ⚡ Độ trễ: {json.loads(x)['latency_ms']} ms",
        output_type=Types.STRING()
    ).print()
    # --------------------------

    # 3. Ghi kết quả dự đoán ra Topic mới (fraud_alerts) để Grafana bắt lấy
    kafka_producer = FlinkKafkaProducer(
        topic=OUTPUT_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': KAFKA_BROKER}
    )
    processed_stream.add_sink(kafka_producer)

    print("[*] Bắt đầu chạy Flink Real-time Fraud Detection Pipeline...")
    env.execute("Flink XGBoost Pipeline")

if __name__ == '__main__':
    main()