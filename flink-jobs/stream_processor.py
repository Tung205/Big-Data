"""
stream_processor.py — TRÁI TIM CỦA FLINK JOB
==============================================
Luồng xử lý (Hot Path):

  Kafka (topic: transactions)
        │
        ▼  [1] Ingestion
  ParseTransactionFn         ← Parse JSON thô từ Kafka
        │
        ▼  [2] Enrichment + [3] Assembly
  EnrichAndAssembleFn        ← Query Redis + ghép feature vector
        │  (dùng redis_client.py + format_data.py)
        │
        ▼  [4] Inference Call
  CallModelAPIFn             ← Gọi HTTP POST đến Model API
        │
        ▼
   (Kết thúc nhiệm vụ Flink)
   Model API tự đẩy kết quả xuống BigQuery

Chạy thủ công (để test):
    python stream_processor.py

Submit lên Flink cluster:
    flink run --python stream_processor.py --pyFiles utils/
"""

import json
import logging
import os
import sys
import time

import requests
import yaml
from pyflink.common import Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import MapFunction

# Đảm bảo import được utils/ khi chạy cả local lẫn trong Flink cluster
sys.path.insert(0, os.path.dirname(__file__))
from utils.redis_client import create_redis_client, get_velocity_features
from utils.format_data import build_feature_vector, build_model_api_payload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("StreamProcessor")


# ─────────────────────────────────────────────────────────────────
# Load config
# ─────────────────────────────────────────────────────────────────

def load_config(path: str = "config.yaml") -> dict:
    config_path = os.path.join(os.path.dirname(__file__), path)
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ─────────────────────────────────────────────────────────────────
# [1] Parse JSON từ Kafka
# ─────────────────────────────────────────────────────────────────

class ParseTransactionFn(MapFunction):
    """
    Nhận raw JSON string từ Kafka, trả về dict Python.
    Trả về None nếu message bị lỗi để filter bỏ ở bước sau.

    Kafka message schema (do Data Generator gửi lên):
    {
        "trans_date_trans_time": "2024-01-15 14:32:00",
        "cc_num":                "2703186189652095",
        "merchant":              "fraud_Rippin, Kub and Mann",
        "category":              "misc_net",
        "amt":                   4.97,
        "gender":                "F",
        "lat":                   36.0788,
        "long":                  -81.1781,
        "city_pop":              3495,
        "dob":                   "1988-03-09",
        "trans_num":             "0b242abb623afc578575680df30655b9",
        "merch_lat":             36.011293,
        "merch_long":            -82.048315
    }
    """

    def map(self, raw_message: str):
        try:
            return json.loads(raw_message)
        except json.JSONDecodeError as e:
            logger.error(f"Không parse được JSON: {e} | raw={raw_message[:200]}")
            return None


# ─────────────────────────────────────────────────────────────────
# [2] + [3] Enrichment từ Redis + Feature Assembly
# ─────────────────────────────────────────────────────────────────

class EnrichAndAssembleFn(MapFunction):
    """
    Nhiệm vụ 2 & 3 theo đề bài thầy:
      - Cầm cc_num chạy sang hỏi Redis lấy velocity features.
      - Ghép [dữ liệu Kafka] + [dữ liệu Redis] thành feature vector hoàn chỉnh.

    Redis client được khởi tạo 1 lần trong open() và tái sử dụng
    cho toàn bộ messages (tránh tạo connection mới mỗi request).
    """

    def __init__(self, redis_cfg: dict):
        self.redis_cfg = redis_cfg
        self.redis_client = None

    def open(self, runtime_context):
        """Gọi 1 lần khi TaskManager khởi động."""
        self.redis_client = create_redis_client(self.redis_cfg)

    def map(self, kafka_data: dict):
        if kafka_data is None:
            return None

        cc_num = str(kafka_data.get("cc_num", ""))
        trans_num = kafka_data.get("trans_num", "unknown")

        try:
            # [2] Enrichment: hỏi Redis lấy velocity features của thẻ này
            velocity_features = get_velocity_features(self.redis_client, cc_num)

            # [3] Assembly: ghép lại thành feature vector hoàn chỉnh
            features = build_feature_vector(kafka_data, velocity_features)

            # Đóng gói thành payload chuẩn để gửi lên Model API
            payload = build_model_api_payload(kafka_data, features)

            logger.debug(
                f"Assembled | tx={trans_num} | "
                f"amt={features['amt']} | dist={features['distance_km']:.1f}km | "
                f"v1d={velocity_features['velocity_1d_count']}"
            )
            return payload

        except Exception as e:
            logger.error(f"Lỗi enrich/assembly tx={trans_num}: {e}")
            return None


# ─────────────────────────────────────────────────────────────────
# [4] Gọi Model API (Inference Call)
# ─────────────────────────────────────────────────────────────────

class CallModelAPIFn(MapFunction):
    """
    Nhiệm vụ 4: Đóng gói JSON và gửi HTTP POST đến Model API.
    Flink "giao mâm" tại đây — Model tự xử lý và đẩy xuống BigQuery.

    Dùng Synchronous call theo lời thầy khuyên (đơn giản, đủ dùng cho đồ án).
    Từ khóa nâng cao cần biết khi bảo vệ: "Async I/O trong Flink".
    """

    def __init__(self, model_cfg: dict):
        self.model_url     = model_cfg["url"]
        self.timeout       = model_cfg.get("timeout_sec", 5)
        self.max_retries   = model_cfg.get("max_retries", 3)

    def map(self, payload: dict):
        if payload is None:
            return None

        trans_num = payload.get("trans_num", "unknown")

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.post(
                    self.model_url,
                    json=payload,
                    timeout=self.timeout,
                )
                resp.raise_for_status()

                logger.info(
                    f"✓ Giao mâm thành công | tx={trans_num} | "
                    f"status={resp.status_code}"
                )
                # Trả về response của Model để log (không dùng để sink)
                return resp.json()

            except requests.Timeout:
                logger.warning(
                    f"Model API timeout (lần {attempt}/{self.max_retries}) "
                    f"| tx={trans_num}"
                )
            except requests.ConnectionError:
                logger.warning(
                    f"Không kết nối được Model API (lần {attempt}/{self.max_retries}) "
                    f"| tx={trans_num}"
                )
            except requests.HTTPError as e:
                logger.error(f"Model API trả lỗi HTTP: {e} | tx={trans_num}")
                break  # Lỗi HTTP (4xx/5xx) → không retry

            if attempt < self.max_retries:
                time.sleep(1)

        logger.error(f"Bỏ qua tx={trans_num} sau {self.max_retries} lần thử.")
        return None


# ─────────────────────────────────────────────────────────────────
# Main: Khai báo Flink DataStream pipeline
# ─────────────────────────────────────────────────────────────────

def main():
    cfg = load_config("config.yaml")

    # --- Cấu hình Flink Environment ---
    flink_cfg = cfg["flink"]
    env_config = Configuration()
    env_config.set_string("python.fn-execution.memory.managed.fraction", "0.3")

    env = StreamExecutionEnvironment.get_execution_environment(env_config)
    env.set_parallelism(flink_cfg["parallelism"])
    env.enable_checkpointing(flink_cfg["checkpoint_interval_ms"])

    # --- [1] Source: đọc từ Kafka ---
    kafka_cfg = cfg["kafka"]
    start_offset = (
        KafkaOffsetsInitializer.latest()
        if kafka_cfg["start_offset"] == "latest"
        else KafkaOffsetsInitializer.earliest()
    )

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_cfg["bootstrap_servers"])
        .set_topics(kafka_cfg["topic"])
        .set_group_id(kafka_cfg["group_id"])
        .set_starting_offsets(start_offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    logger.info(
        f"Khởi động Flink Job | "
        f"Kafka={kafka_cfg['bootstrap_servers']} | "
        f"Topic={kafka_cfg['topic']} | "
        f"Parallelism={flink_cfg['parallelism']}"
    )

    # --- Định nghĩa DataStream pipeline ---
    (
        env
        # Nguồn: đọc raw JSON string từ Kafka
        .from_source(
            kafka_source,
            WatermarkStrategy.no_watermarks(),
            "Kafka-Source: transactions",
        )

        # [1] Parse JSON → dict
        .map(ParseTransactionFn(), output_type=None)
        .filter(lambda x: x is not None)
        .name("ParseTransaction")

        # [2] + [3] Enrich từ Redis + ghép feature vector
        .map(EnrichAndAssembleFn(cfg["redis"]))
        .filter(lambda x: x is not None)
        .name("EnrichAndAssemble")

        # [4] Gọi Model API — Flink kết thúc nhiệm vụ tại đây
        .map(CallModelAPIFn(cfg["model_api"]))
        .filter(lambda x: x is not None)
        .name("CallModelAPI")
    )

    # Không có Sink vì Model API tự đẩy kết quả xuống BigQuery
    env.execute("FraudDetection-HotPath")


if __name__ == "__main__":
    main()