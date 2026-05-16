"""
mock_generator.py
=================
Gửi 3 transaction giả lên Kafka để test Flink pipeline.
Chạy bên trong container mock-generator.
"""

import json
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "transactions"

transactions = [
    {
        "trans_date_trans_time": "2024-01-15 14:32:00",
        "cc_num": "2703186189652095",
        "merchant": "fraud_Rippin_Kub",
        "category": "misc_net",
        "amt": 4.97,
        "gender": "F",
        "lat": 36.0788, "long": -81.1781,
        "city_pop": 3495,
        "dob": "1988-03-09",
        "trans_num": "tx_001",
        "merch_lat": 36.011293, "merch_long": -82.048315,
    },
    {
        "trans_date_trans_time": "2024-01-15 02:15:00",
        "cc_num": "630423337322",
        "merchant": "fraud_BigShop",
        "category": "shopping_pos",
        "amt": 899.99,
        "gender": "M",
        "lat": 48.8878, "long": -118.2105,
        "city_pop": 149,
        "dob": "1978-06-21",
        "trans_num": "tx_002",
        "merch_lat": 49.159047, "merch_long": -118.186462,
    },
    {
        "trans_date_trans_time": "2024-01-15 23:50:00",
        "cc_num": "38859492057661",
        "merchant": "fraud_Lind",
        "category": "entertainment",
        "amt": 220.11,
        "gender": "M",
        "lat": 42.1808, "long": -112.2620,
        "city_pop": 4154,
        "dob": "1962-01-19",
        "trans_num": "tx_003",
        "merch_lat": 43.150704, "merch_long": -112.154481,
    },
]


def main():
    print(f"Ket noi Kafka tai {KAFKA_BOOTSTRAP}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print("Ket noi thanh cong!\n")

    print("Generator: bat dau gui transactions...")
    for i, tx in enumerate(transactions):
        producer.send(TOPIC, value=tx)
        amt = tx["amt"]
        card = str(tx["cc_num"])[:6]
        num = tx["trans_num"]
        print(f"  Da gui {num} | amt={amt} | card={card}****")
        time.sleep(2)

    producer.flush()
    print(f"\nGenerator: xong! Da gui {len(transactions)} giao dich len topic '{TOPIC}'.")


if __name__ == "__main__":
    main()