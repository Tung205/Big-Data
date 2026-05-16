"""
utils/format_data.py
====================
Chứa 2 nhóm hàm chính:

1. Feature Engineering (stateless):
   Tính toán các features từ dữ liệu thô Kafka.
   Logic giống hệt notebook fraud-detection-training.ipynb.

2. Feature Assembly:
   Gộp [dữ liệu Kafka đã xử lý] + [velocity features từ Redis]
   thành một cục JSON chuẩn để gửi lên Model API.

Features cuối cùng (13 features, đúng thứ tự model yêu cầu):
    amt, city_pop, hour, day, month, day_of_week,
    age, distance_km, time_since_last_trans_sec,
    velocity_1d_count, velocity_1d_amt_sum,
    gender_encoded, category_encoded
"""

import math
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# Label Encoding mappings — phải khớp với notebook lúc train model
# ─────────────────────────────────────────────────────────────────

GENDER_MAP = {
    "F": 0,
    "M": 1,
}

# 14 category trong dataset Kaggle (theo thứ tự alphabet giống LabelEncoder)
CATEGORY_MAP = {
    "entertainment":  0,
    "food_dining":    1,
    "gas_transport":  2,
    "grocery_net":    3,
    "grocery_pos":    4,
    "health_fitness": 5,
    "home":           6,
    "kids_pets":      7,
    "misc_net":       8,
    "misc_pos":       9,
    "personal_care":  10,
    "shopping_net":   11,
    "shopping_pos":   12,
    "travel":         13,
}

# Thứ tự features gửi vào model (phải khớp với lúc training)
FEATURE_ORDER = [
    "amt",
    "city_pop",
    "hour",
    "day",
    "month",
    "day_of_week",
    "age",
    "distance_km",
    "time_since_last_trans_sec",
    "velocity_1d_count",
    "velocity_1d_amt_sum",
    "gender_encoded",
    "category_encoded",
]


# ─────────────────────────────────────────────────────────────────
# Nhóm 1: Feature Engineering từ dữ liệu thô Kafka
# ─────────────────────────────────────────────────────────────────

def extract_datetime_features(trans_dt: datetime) -> dict:
    """
    Trích xuất các features thời gian từ datetime object.
    (Tham chiếu: hàm datetime_exaction() trong notebook)

    Returns:
        dict gồm: hour, day, month, day_of_week
    """
    return {
        "hour":        trans_dt.hour,
        "day":         trans_dt.day,
        "month":       trans_dt.month,
        "day_of_week": trans_dt.weekday(),  # 0=Thứ Hai, 6=Chủ Nhật
    }


def calculate_age(trans_dt: datetime, dob_str: str) -> int:
    """
    Tính tuổi khách hàng tại thời điểm giao dịch.
    (Tham chiếu: hàm age_exaction() trong notebook)

    Args:
        trans_dt : datetime của giao dịch
        dob_str  : chuỗi ngày sinh, format "YYYY-MM-DD"

    Returns:
        Tuổi (int)
    """
    dob = datetime.strptime(dob_str, "%Y-%m-%d")
    return trans_dt.year - dob.year


def calculate_distance_km(lat1: float, lon1: float,
                           lat2: float, lon2: float) -> float:
    """
    Tính khoảng cách (km) giữa vị trí khách hàng và merchant
    bằng công thức Haversine.
    (Tham chiếu: hàm calculate_haversine() trong notebook)

    Args:
        lat1, lon1 : tọa độ khách hàng
        lat2, lon2 : tọa độ merchant

    Returns:
        Khoảng cách tính bằng km (float)
    """
    R = 6371.0  # Bán kính Trái Đất (km)

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)

    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2)

    return R * 2 * math.asin(math.sqrt(a))


def encode_gender(gender: str) -> int:
    """Label encoding cho gender (F=0, M=1)."""
    return GENDER_MAP.get(gender, -1)


def encode_category(category: str) -> int:
    """Label encoding cho category (14 loại, giống notebook)."""
    return CATEGORY_MAP.get(category, -1)


# ─────────────────────────────────────────────────────────────────
# Nhóm 2: Feature Assembly — ghép Kafka + Redis thành JSON chuẩn
# ─────────────────────────────────────────────────────────────────

def build_feature_vector(kafka_data: dict, velocity_features: dict) -> dict:
    """
    Bước trung tâm: gộp dữ liệu từ Kafka và Redis, trả về
    dict chứa đúng 13 features theo thứ tự model yêu cầu.

    Args:
        kafka_data        : dict parse từ Kafka message (dữ liệu thô giao dịch)
        velocity_features : dict từ redis_client.get_velocity_features()

    Returns:
        dict với key là tên feature, value là số (float/int)

    Raises:
        ValueError nếu kafka_data thiếu trường bắt buộc
    """
    # --- Parse datetime ---
    try:
        trans_dt = datetime.strptime(
            kafka_data["trans_date_trans_time"], "%Y-%m-%d %H:%M:%S"
        )
    except (KeyError, ValueError) as e:
        raise ValueError(f"Lỗi parse trans_date_trans_time: {e}")

    # --- Các features thời gian ---
    dt_features = extract_datetime_features(trans_dt)

    # --- Age ---
    age = calculate_age(trans_dt, kafka_data["dob"])

    # --- Distance ---
    distance_km = calculate_distance_km(
        lat1=float(kafka_data["lat"]),
        lon1=float(kafka_data["long"]),
        lat2=float(kafka_data["merch_lat"]),
        lon2=float(kafka_data["merch_long"]),
    )

    # --- Label encoding ---
    gender_encoded   = encode_gender(kafka_data.get("gender", ""))
    category_encoded = encode_category(kafka_data.get("category", ""))

    # --- Ghép lại thành feature vector hoàn chỉnh ---
    features = {
        "amt":                       float(kafka_data["amt"]),
        "city_pop":                  int(kafka_data["city_pop"]),
        "hour":                      dt_features["hour"],
        "day":                       dt_features["day"],
        "month":                     dt_features["month"],
        "day_of_week":               dt_features["day_of_week"],
        "age":                       age,
        "distance_km":               distance_km,
        # 3 features dưới đây đến từ Redis (Spark đã tính sẵn)
        "time_since_last_trans_sec": velocity_features["time_since_last_trans_sec"],
        "velocity_1d_count":         velocity_features["velocity_1d_count"],
        "velocity_1d_amt_sum":       velocity_features["velocity_1d_amt_sum"],
        "gender_encoded":            gender_encoded,
        "category_encoded":          category_encoded,
    }

    return features


def build_model_api_payload(kafka_data: dict, features: dict) -> dict:
    """
    Đóng gói feature vector thành JSON payload chuẩn để gửi lên Model API.

    Args:
        kafka_data : dict parse từ Kafka message
        features   : dict từ build_feature_vector()

    Returns:
        dict sẵn sàng để json.dumps() và gửi qua requests.post()
    """
    return {
        # Metadata để Model API log và đẩy xuống BigQuery
        "trans_num": kafka_data.get("trans_num", ""),
        "cc_num":    str(kafka_data.get("cc_num", "")),

        # 13 features theo đúng thứ tự model yêu cầu
        "features": {col: features[col] for col in FEATURE_ORDER},

        # Context bổ sung cho tầng LLM+RAG của Model API
        "context": {
            "merchant":    kafka_data.get("merchant", ""),
            "category":    kafka_data.get("category", ""),
            "amt":         features["amt"],
            "distance_km": features["distance_km"],
            "hour":        features["hour"],
        },
    }