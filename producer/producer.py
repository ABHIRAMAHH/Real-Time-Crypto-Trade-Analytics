from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

coins = ["BTC", "ETH", "SOL", "DOGE", "XRP"]

TOTAL_RECORDS = 1000

for i in range(TOTAL_RECORDS):

    trade = {
        "coin": random.choice(coins),
        "price": round(random.uniform(100, 120000), 2),
        "volume": random.randint(1, 100),
        "trade_time": datetime.now().isoformat()
    }

    producer.send(
        "crypto_trades",
        value=trade
    )

    print(f"{i + 1}/{TOTAL_RECORDS} -> {trade}")

    time.sleep(0.01)

producer.flush()
producer.close()

print(f"\nSuccessfully produced {TOTAL_RECORDS} records to Kafka.")