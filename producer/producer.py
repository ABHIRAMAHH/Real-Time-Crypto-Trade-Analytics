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

exchanges = ["Binance", "Coinbase", "Kraken", "Bybit"]

trade_types = ["BUY", "SELL"]

regions = ["Asia", "Europe", "North America", "South America"]

TOTAL_RECORDS = 1000

for i in range(TOTAL_RECORDS):

    trade = {
        "trade_id": f"TRD{i+1}",

        "coin": random.choice(coins),

        "exchange": random.choice(exchanges),

        "price": round(random.uniform(100, 120000), 2),

        "volume": random.randint(1, 100),

        "trade_type": random.choice(trade_types),

        "trader_region": random.choice(regions),

        "fee": round(random.uniform(1, 20), 2),

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