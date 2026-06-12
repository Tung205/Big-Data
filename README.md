# 🛡️ Real-time Fraud Detection System

> **Hệ thống phát hiện gian lận giao dịch tài chính theo thời gian thực** — Xây dựng trên nền tảng Big Data với Apache Kafka, Apache Flink, Apache Spark và Google BigQuery, tích hợp mô hình ML (XGBoost) và AI phân tích nguyên nhân (LLM + RAG).

---

## 📐 Sơ đồ kiến trúc tổng quan

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                        │
│   ┌────────────────┐      ┌────────────────┐      ┌─────────────────┐                  │
│   │ Data Producer  ├─────►│ Data Generator ├─────►│  Apache Kafka   │                  │
│   └────────────────┘      └────────────────┘      │     (TOPIC:     │                  │
│                                                   │raw_transactions)│                  │
│                                                   └────────┬────────┘                  │
│                                                            │                           │
│                                                            ▼                           │
│                                                   ┌─────────────────┐                  │
│                                                   │ [DATA INGESTION]│                  │
│                                                   └────────┬────────┘                  │
│                                                            │                           │
│                                       ┌────────────────────┴────────────────────┐      │
│                                       ▼                                         ▼      │
│                            ┌────────────────────┐                    ┌────────────────┐│
│                            │    Apache Spark    │                    │  Apache Flink  ││
│                            │ (Batch Processing) │                    │(Stream Process)││
│                            └────────┬───────────┘                    └────────┬───────┘│
│                                     │                                         │        │
│                                     │                                         ▼        │
│                                     │                               ┌────────────────┐ │
│                                     │                               │    ML Model    │ │
│                                     │                               │   (XGBoost)    │ │
│                                     │                               └────────┬───────┘ │
│                                     │  cleansed facts                        │         │
│                                     └───────────────────────┬────────────────┘         │
│                                                             │                          │
│                                                             ▼                          │
│                                                   ┌─────────────────┐                  │
│                                                   │ Google BigQuery │ ◄── real-time    │
│                                                   │ [DATA STORAGE]  │     audit log    │
│                                                   └────────┬────────┘                  │
│                                                            │                           │
│                                                            │ SQL queries               │
│                                                            ▼                           │
│                                                   ┌─────────────────┐                  │
│                                                   │     Grafana     │ ──► gemini API   │
│                                                   │ [VISUALIZATION] │     calls        │
│                                                   └────────┬────────┘                  │
│                                                            │                           │
│                                                            │ response                  │
│                                                            ▼                           │
│                                                   ┌─────────────────┐                  │
│                                                   │    LLM + RAG    │                  │
│                                                   │(Gemini + FAISS) │                  │
│                                                   └─────────────────┘                  │
│                                                                                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🧩 Các thành phần hệ thống

Hệ thống được chia thành **5 lớp** chính, xử lý dữ liệu từ nguồn đến dashboard:

### Lớp 1 — Data Ingestion (Thu nạp dữ liệu)

| Thành phần | Công nghệ | Mô tả |
|---|---|---|
| **Data Generator** | Python, Kafka Producer | Đọc dataset `fraudTrain.csv`, phát sinh luồng giao dịch giả lập với tốc độ có thể cấu hình (30 TPS - local / 100 TPS - benchmark), đẩy vào Kafka topic `raw_transactions` |
| **Apache Kafka Cluster** | Confluent Kafka 7.3.0 (3 Brokers + ZooKeeper) | Message broker trung tâm. Duy trì 2 topic: `raw_transactions` (đầu vào) và `fraud_alerts` (đầu ra phát hiện gian lận) với replication factor = 2 và 3 partitions |

### Lớp 2 — Data Processing (Xử lý dữ liệu)

| Thành phần | Công nghệ | Mô tả |
|---|---|---|
| **Stream Processing** | Apache Flink 1.17 (1 JobManager + 2 TaskManagers) | Xử lý luồng thời gian thực. Pipeline gồm 3 bước: (1) trích xuất đặc trưng cơ bản (Haversine, Datetime, Age), (2) tính toán trạng thái lịch sử per-user (Velocity 24h, time since last txn), (3) gọi model XGBoost dự đoán |
| **ML Model (XGBoost)** | XGBoost, FAISS | Model phân loại nhị phân, threshold = 0.7. Được load vào bộ nhớ của Flink TaskManager lúc khởi động, dự đoán từng giao dịch với độ trễ thấp. Tính toán latency từ lúc nhập vào Kafka đến khi có cảnh báo |
| **Batch Processing** | Apache Spark 3.4.1 (1 Master + 2 Workers) | Đọc từ Kafka, cứ 2 phút kích hoạt 1 micro-batch, tổng hợp thống kê theo Gender / Category / Geo, ghi xuống BigQuery |

### Lớp 3 — Data Storage (Lưu trữ)

| Thành phần | Công nghệ | Mô tả |
|---|---|---|
| **Google BigQuery** | BigQuery (dataset: `fraud_features`) | Lưu trữ kết quả xử lý. Gồm 5 bảng: `realtime_fraud_logs` (log cảnh báo real-time), `gender_summary`, `category_summary`, `geo_summary`, `overall_summary` (batch aggregation từ Spark) |
| **BQ Sink** | Python Kafka Consumer | Lắng nghe topic `fraud_alerts`, ghi theo batch (100 records/lần) xuống bảng `realtime_fraud_logs` trong BigQuery |

### Lớp 4 — Visualization & AI Analysis (Trực quan hoá & Phân tích AI)

| Thành phần | Công nghệ | Mô tả |
|---|---|---|
| **Grafana Dashboard** | Grafana + BigQuery Plugin | Dashboard giám sát real-time: gauge tổng giao dịch/tổng giá trị, bảng log cảnh báo, biểu đồ theo danh mục/giới tính/địa lý |
| **Model API (LLM + RAG)** | FastAPI, Gemini 2.5 Flash, LangChain, FAISS | Khi click vào một cảnh báo gian lận trên Grafana, gọi API này. Hệ thống tìm kiếm kịch bản phù hợp trong cơ sở tri thức RAG (12 case study), ghép với thông số giao dịch, gửi lên Gemini để phân tích nguyên nhân gốc rễ (Root Cause Analysis). Kết quả được nhúng trực tiếp trong Grafana qua iframe |

### Lớp 5 — Infrastructure (Hạ tầng)

Toàn bộ hệ thống được container hoá bằng **Docker Compose**, chia sẻ mạng `fraud-network`.

---

## 🗂️ Cấu trúc thư mục

```
Big-Data/
├── data-generator/          # Producer: Sinh dữ liệu giao dịch → Kafka
│   ├── main.py              # Logic phát luồng có rate-limiting
│   ├── config.py            # Cấu hình Kafka, topic, data file
│   ├── Dockerfile
│   └── requirements.txt
│
├── flink-jobs/              # Stream Processing + XGBoost Inference
│   ├── flink_pipeline.py    # Pipeline: Feature Extraction → Stateful → Predict
│   ├── xgb_fraud_model.json # Model XGBoost đã huấn luyện (~50MB)
│   ├── config.yaml
│   ├── Dockerfile
│   └── requirements.txt
│
├── spark-jobs/              # Batch Aggregation → BigQuery
│   ├── batch_feature_pipeline.py   # Micro-batch 2 phút, ghi 4 bảng thống kê
│   ├── utils.py
│   ├── config.json
│   ├── Dockerfile
│   └── requirements.txt
│
├── bq-sink/                 # Real-time sink: Kafka fraud_alerts → BigQuery
│   ├── realtime_bq_sink.py  # Consumer → batch insert 100 records
│   ├── Dockerfile
│   └── requirements.txt
│
├── model-api/               # FastAPI: LLM + RAG Root Cause Analysis
│   ├── app.py               # Endpoint GET / → HTML phân tích cho Grafana iframe
│   ├── ingest_data.py       # Script tạo FAISS index từ 12 case study gian lận
│   ├── evaluate_pipeline.py # Đánh giá pipeline hybrid
│   ├── models/              # Chứa FAISS index (faiss_index/)
│   ├── Dockerfile
│   └── requirements.txt
│
├── simulator/               # Grafana Simulator Dashboard (HTML)
│   ├── index.html
│   └── Dockerfile
│
├── flink-cluster/           # Custom Flink Docker image (có cài Java, PyFlink, Kafka connector)
│
├── fraud_dashboard.json     # Grafana Dashboard config (import sẵn)
├── docker-compose.yml       # Orchestration toàn bộ hệ thống
├── application_default_credentials.json  # GCP credentials (KHÔNG commit lên git)
└── .env                     # Biến môi trường (GOOGLE_API_KEY, Spark/Flink resources)
```

---

## ⚙️ Yêu cầu hệ thống

| Môi trường | RAM khuyến nghị | CPU |
|---|---|---|
| Local (Docker Desktop) | ≥ 16 GB | ≥ 4 cores |
| GCE / Cloud VM (Benchmark) | ≥ 64 GB | ≥ 8 cores |

**Phần mềm cần cài:**
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (hoặc Docker Engine + Docker Compose)
- Python 3.9+ (chỉ cần cho bước tạo FAISS index)
- Tài khoản Google Cloud với BigQuery enabled

---

## 🚀 Hướng dẫn chạy

### Bước 1 — Cấu hình môi trường

Tạo file `.env` ở thư mục gốc (hoặc chỉnh sửa file có sẵn):

```env
# Google Gemini API Key (lấy tại https://aistudio.google.com)
GOOGLE_API_KEY=your_gemini_api_key_here

# Tài nguyên cho môi trường local
SPARK_WORKER_CPU=2.0
SPARK_WORKER_MEM=4G
FLINK_TM_CPU=2.0
FLINK_TM_MEM=4G

# Chế độ chạy: "local" (30 TPS) hoặc "benchmark" (100 TPS)
APP_ENV=local
```

Đặt file `application_default_credentials.json` (GCP Service Account key) vào thư mục gốc.

### Bước 2 — Tạo FAISS Index cho RAG (chỉ cần làm 1 lần)

```bash
cd model-api
pip install langchain langchain-community langchain-huggingface faiss-cpu sentence-transformers
python ingest_data.py
cd ..
```

> Script sẽ tạo thư mục `model-api/models/faiss_index/` chứa cơ sở tri thức 12 kịch bản gian lận.

### Bước 3 — Chuẩn bị dữ liệu

Tải dataset [Credit Card Fraud Detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection) từ Kaggle, đặt file `fraudTrain.csv` vào thư mục `data/`:

```bash
mkdir -p data
# copy fraudTrain.csv vào thư mục data/
```

### Bước 4 — Khởi động toàn bộ hệ thống

```bash
docker compose up --build
```

Thứ tự khởi động tự động:
1. ZooKeeper → Kafka Cluster (3 brokers) → tạo 2 topics
2. Spark Master + 2 Workers
3. Flink JobManager + 2 TaskManagers
4. Flink Driver (submit pipeline, chờ 20s warm-up)
5. Data Generator (chờ 40s để Flink sẵn sàng, rồi bắt đầu phát dữ liệu)
6. Spark Driver (batch pipeline)
7. BQ Sink (real-time audit log)
8. Grafana + Model API

### Bước 5 — Truy cập các giao diện

| Dịch vụ | URL | Thông tin đăng nhập |
|---|---|---|
| **Grafana Dashboard** | http://localhost:3000 | admin / admin |
| **Flink Web UI** | http://localhost:8081 | — |
| **Spark Master UI** | http://localhost:8080 | — |
| **Model API (Swagger)** | http://localhost:8000/docs | — |
| **Simulator Dashboard** | http://localhost:8082 | — |

### Bước 6 — Import Grafana Dashboard

1. Mở Grafana → **Dashboards** → **Import**
2. Upload file `fraud_dashboard.json`
3. Chọn BigQuery datasource và cấu hình kết nối GCP

---

## 🔄 Luồng dữ liệu chi tiết

```
[ fraudTrain.csv ]
                                        │
                                        ▼ (pandas chunk read, 100k rows/chunk)
                                [ Data Generator ]
                                 • Rate control: 30 TPS (local) / 100 TPS (benchmark)
                                 • Compression: LZ4, batch_size: 256KB, acks=1
                                 • Gắn ingestion_timestamp + ingestion_time_ms
                                        │
                                        ▼ topic: raw_transactions (3 partitions, RF=2)
                             [ Apache Kafka Cluster ]
                                        │
                    ┌───────────────────┴───────────────────┐
                    ▼ (FlinkKafkaConsumer)                  ▼ (Spark Structured Streaming)
            [ Flink Pipeline ]                      [ Spark Batch Pipeline ]
                    │                                       │
                    ├─ BasicFeatureExtractor                └─ Trigger: 2 phút/lần
                    │  • Haversine distance                    • gender_summary   ──► BigQuery
                    │  • Hour/Day/Month/DayOfWeek              • category_summary ──► BigQuery
                    │  • Age từ DOB                            • geo_summary      ──► BigQuery
                    │  • Label Encoding                        • overall_summary  ──► BigQuery
                    │                                                               │
                    ├─ VelocityAndHistoryProcess                                    │
                    │  (Stateful, keyed by cc_num)                                  │
                    │  • time_since_last_trans_sec (ValueState)                     │
                    │  • velocity_1d_count + amt_sum (ListState, TTL=24h)           │
                    │                                                               │
                    ├─ FraudPredictor (XGBoost)                                     │
                    │  • 13 features đầu vào                                        │
                    │  • Threshold: 0.7 → is_fraud_predicted                        │
                    │  • Latency_ms = alert_time - ingestion_time                   │
                    │                                                               │
                    ▼                                                               │
          topic: fraud_alerts                                                       │
                    │                                                               │
                    ▼ (KafkaConsumer, batch 100 records)                            │
               [ BQ Sink ]                                                          │
                    │                                                               │
                    ▼                                                               │
      [ BigQuery: realtime_fraud_logs ] ◄───────────────────────────────────────────┘
                    │
                    ▼ SQL queries (Grafana BigQuery Plugin)
           [ Grafana Dashboard ]
                    │
                    ▼ (iframe, GET request với transaction params)
          [ Model API: FastAPI ]
                    │
                    ├─ FAISS similarity search (top-2 case studies từ 12 kịch bản)
                    │
                    └─► [ Gemini 2.5 Flash ] ──► Root Cause Analysis (Markdown → HTML)
```

---

## 🤖 ML Model & AI Analysis

### XGBoost Fraud Classifier

| Thuộc tính | Giá trị |
|---|---|
| Model file | `flink-jobs/xgb_fraud_model.json` |
| Input features | 13 features (amt, city_pop, hour, day, month, day_of_week, age, distance_km, time_since_last_trans_sec, velocity_1d_count, velocity_1d_amt_sum, gender_encoded, category_encoded) |
| Threshold | 0.7 (xác suất gian lận) |
| Inference | Tích hợp trong Flink TaskManager, online inference |

### RAG Knowledge Base (12 Kịch bản gian lận)

| Case | Tên kịch bản | Đặc điểm nhận dạng |
|---|---|---|
| FRAUD-01 | Velocity Bot Attack | time_since_last_trans_sec < 60s, velocity cao |
| FRAUD-02 | Elderly Account Takeover | age > 50, giao dịch online đêm muộn |
| FRAUD-03 | High-Value Midnight Siphoning | amt > 500 USD, hour 22h–3h |
| FRAUD-04 | Card Testing Fraud | amt nhỏ, velocity tăng bất thường |
| FRAUD-05 | Geo-Location Anomaly | distance_km > 100 km |
| FRAUD-06 | Mule Account Activity | age < 30, velocity_1d_count > 8 |
| FRAUD-07 | Splurge After Inactivity | time_since_last_trans_sec rất lớn → đột ngột nhiều giao dịch lớn |
| FRAUD-08 | Small Town Income Anomaly | city_pop nhỏ, amt > 1000 USD |
| FRAUD-09 | High-Liquidity Asset Siphoning | category = gift card/crypto, amt sát hạn mức |
| FRAUD-10 | Impossible Travel Anomaly | distance_km > 120 km + time_since rất ngắn |
| FRAUD-11 | Travel Category Phishing | category = travel, amt > 800 USD |
| FRAUD-12 | Dawn Exhaustion Attack | hour = 4h–6h sáng, giao dịch leo thang |

---

## 📊 Thông số hiệu năng (Non-Functional Requirements)

| Chỉ số | Local (Docker) | Benchmark (GCE 64GB) |
|---|---|---|
| Throughput mục tiêu | 30 TPS | **100 TPS** |
| Tổng giao dịch | 9,000 (5 phút) | 60,000 (10 phút) |
| Latency (Flink pipeline) | < 500ms | < 200ms |
| Flink Parallelism | 4 | 4 |
| Kafka Partitions | 3 | 3 |
| Replication Factor | 2 | 2 |

---

## 🛠️ Công nghệ sử dụng

| Lớp | Công nghệ | Phiên bản |
|---|---|---|
| Message Broker | Apache Kafka (Confluent) | 7.3.0 |
| Stream Processing | Apache Flink + PyFlink | 1.17.1 |
| Batch Processing | Apache Spark (PySpark) | 3.4.1 |
| ML Model | XGBoost | — |
| Data Warehouse | Google BigQuery | — |
| LLM | Google Gemini 2.5 Flash Lite | — |
| Vector Search | FAISS + sentence-transformers | all-MiniLM-L6-v2 |
| API Framework | FastAPI + Uvicorn | — |
| Orchestration | LangChain | — |
| Dashboard | Grafana | Latest |
| Containerization | Docker + Docker Compose | — |

---

## 🔧 Biến môi trường

| Biến | Mô tả | Mặc định |
|---|---|---|
| `GOOGLE_API_KEY` | Gemini API Key | — |
| `APP_ENV` | Chế độ chạy: `local` hoặc `benchmark` | `local` |
| `KAFKA_BROKERS` | Danh sách broker Kafka | `kafka1:9092,...` |
| `SPARK_WORKER_CPU` | CPU giới hạn mỗi Spark Worker | `2.0` |
| `SPARK_WORKER_MEM` | RAM giới hạn mỗi Spark Worker | `4G` |
| `FLINK_TM_CPU` | CPU giới hạn mỗi Flink TaskManager | `2.0` |
| `FLINK_TM_MEM` | RAM giới hạn mỗi Flink TaskManager | `4G` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Đường dẫn file GCP credentials | — |

---

## ⚠️ Lưu ý quan trọng

- File `xgb_fraud_model.json` (~50MB) nên được quản lý bằng Git LFS hoặc tải thủ công.
- Khi chạy lần đầu, `ingest_data.py` sẽ tải embedding model (~90MB) từ HuggingFace về máy.
- Data Generator có cơ chế tự chờ 40 giây để Flink pipeline warm-up trước khi phát dữ liệu.

---

## 👥 Nhóm phát triển

Đồ án môn **Big Data** — Đại học Công nghệ, Đại học Quốc gia Hà Nội (UET-VNU).
23021715 - Nguyễn Thanh Tùng
23021473 - Vũ Việt Anh
23021521 - Nguyễn Tiến Đạt
23021653 - Mạch Trần Quang Nhật
23020048 - Lê Phan Trí Đức
