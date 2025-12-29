import json
import os
from collections import Counter
from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "analytics-service")

c = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
)

sku_counter = Counter()
orders = 0
revenue = 0.0

print(f"[analytics] started bootstrap={BOOTSTRAP} topic={TOPIC} group={GROUP_ID}")
c.subscribe([TOPIC])

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(msg.error())

        event = json.loads(msg.value().decode("utf-8"))
        orders += 1
        revenue += float(event.get("total") or 0.0)

        for it in event.get("items", []):
            sku = (it.get("sku") or "").strip().lower()
            qty = int(it.get("qty") or 0)
            if sku and qty > 0:
                sku_counter[sku] += qty

        if orders % 5 == 0:
            print(f"[analytics] orders={orders} revenue={round(revenue,2)} top={sku_counter.most_common(3)}")
        else:
            print(f"[analytics] consumed order_id={event.get('order_id')} offset={msg.offset()}")

finally:
    c.close()
