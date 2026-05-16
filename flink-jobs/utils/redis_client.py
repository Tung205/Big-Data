"""
utils/redis_client.py
=====================
Chứa hàm kết nối và query Redis.
Redis là nơi Spark (cold path) đã tính sẵn các velocity features
và lưu theo từng cc_num (số thẻ).

Key schema do Spark ghi vào (Flink chỉ đọc):
    Key  : "velocity:{cc_num}"
    Value: JSON string với các trường sau:
        {
            "velocity_1d_count":          float,  # số giao dịch trong 24h qua
            "velocity_1d_amt_sum":         float,  # tổng tiền trong 24h qua
            "time_since_last_trans_sec":   float   # giây kể từ giao dịch cuối
        }
"""

import json
import logging

import redis

logger = logging.getLogger(__name__)


def create_redis_client(cfg: dict) -> redis.Redis:
    """
    Tạo và trả về Redis client từ config.
    Gọi hàm này một lần trong open() của MapFunction
    để tái sử dụng connection thay vì tạo mới mỗi message.

    Args:
        cfg: dict từ config.yaml, section "redis"

    Returns:
        redis.Redis instance đã kết nối
    """
    client = redis.Redis(
        host=cfg["host"],
        port=cfg["port"],
        db=cfg["db"],
        decode_responses=True,
        socket_connect_timeout=cfg.get("socket_connect_timeout", 5),
        socket_timeout=cfg.get("socket_timeout", 2),
    )
    # Ping để kiểm tra kết nối ngay lúc khởi động
    client.ping()
    logger.info(f"Redis connected: {cfg['host']}:{cfg['port']}")
    return client


def get_velocity_features(client: redis.Redis, cc_num: str) -> dict:
    """
    Truy vấn Redis để lấy velocity features của một thẻ.
    Đây là dữ liệu lịch sử do Spark (cold path) tính và lưu sẵn.

    Args:
        client : Redis client đã khởi tạo
        cc_num : Số thẻ tín dụng (string)

    Returns:
        dict với 3 keys:
            - velocity_1d_count          (float)
            - velocity_1d_amt_sum        (float)
            - time_since_last_trans_sec  (float)
        Nếu thẻ chưa có lịch sử (lần đầu xuất hiện), trả về dict với giá trị 0.
    """
    DEFAULT = {
        "velocity_1d_count":         0.0,
        "velocity_1d_amt_sum":       0.0,
        "time_since_last_trans_sec": 0.0,
    }

    redis_key = f"velocity:{cc_num}"

    try:
        raw = client.get(redis_key)
        if raw is None:
            logger.debug(f"No Redis entry for card {str(cc_num)[:6]}**** — using defaults")
            return DEFAULT

        data = json.loads(raw)
        return {
            "velocity_1d_count":         float(data.get("velocity_1d_count", 0.0)),
            "velocity_1d_amt_sum":       float(data.get("velocity_1d_amt_sum", 0.0)),
            "time_since_last_trans_sec": float(data.get("time_since_last_trans_sec", 0.0)),
        }

    except redis.RedisError as e:
        logger.warning(
            f"Redis error for card {str(cc_num)[:6]}****: {e} — using defaults"
        )
        return DEFAULT

    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(
            f"Malformed Redis value for key '{redis_key}': {e} — using defaults"
        )
        return DEFAULT