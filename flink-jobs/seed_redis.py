"""
seed_redis.py
=============
Chạy một lần để giả lập dữ liệu Spark đã ghi vào Redis.
Dùng khi test Flink job độc lập (chưa có Spark chạy).

Chạy lệnh:
    docker exec redis redis-cli ping          # kiểm tra Redis sống
    docker run --rm --network flink-jobs_fraud-net \
        -v "%cd%":/app -w /app \
        python:3.10-slim \
        bash -c "pip install redis -q && python seed_redis.py"
"""

import json
import redis

REDIS_HOST = "redis"
REDIS_PORT = 6379

# Dữ liệu giả lập — khớp với 3 thẻ trong mock-generator
VELOCITY_DATA = [
    {
        "cc_num": "2703186189652095",
        "velocity_1d_count":         3.0,
        "velocity_1d_amt_sum":       87.50,
        "time_since_last_trans_sec": 7200.0,   # 2 tiếng trước
    },
    {
        "cc_num": "630423337322",
        "velocity_1d_count":         15.0,     # nhiều giao dịch → đáng ngờ
        "velocity_1d_amt_sum":       4230.00,  # tổng tiền lớn → đáng ngờ
        "time_since_last_trans_sec": 180.0,    # 3 phút trước → rất nhanh
    },
    {
        "cc_num": "38859492057661",
        "velocity_1d_count":         1.0,
        "velocity_1d_amt_sum":       220.11,
        "time_since_last_trans_sec": 86400.0,  # 1 ngày trước
    },
]

def main():
    client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT,
        decode_responses=True
    )
    client.ping()
    print(f"✓ Kết nối Redis thành công: {REDIS_HOST}:{REDIS_PORT}\n")

    for entry in VELOCITY_DATA:
        cc_num = entry.pop("cc_num")
        key    = f"velocity:{cc_num}"
        client.set(key, json.dumps(entry), ex=86400)
        print(f"  ✓ Đã ghi key='{key}'")
        print(f"      velocity_1d_count         = {entry['velocity_1d_count']}")
        print(f"      velocity_1d_amt_sum        = {entry['velocity_1d_amt_sum']}")
        print(f"      time_since_last_trans_sec  = {entry['time_since_last_trans_sec']}\n")

    print(f"✓ Seed xong {len(VELOCITY_DATA)} thẻ vào Redis.")

if __name__ == "__main__":
    main()