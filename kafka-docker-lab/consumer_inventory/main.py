import json
import os
import time
from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "inventory-service")

c = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)

def reserve_inventory(event: dict) -> dict:
    reserved = []
    for it in event.get("items", []):
        sku = (it.get("sku") or "").strip().lower()
        qty = int(it.get("qty") or 0)
        if not sku or qty <= 0:
            raise ValueError(f"Invalid item: {it}")
        reserved.append({"sku": sku, "qty": qty})
    time.sleep(0.05)
    return {"order_id": event["order_id"], "reserved": reserved}

print(f"[inventory] started bootstrap={BOOTSTRAP} topic={TOPIC} group={GROUP_ID}")
c.subscribe([TOPIC])

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            err = msg.error()
            code = err.code()

            if code == KafkaError._PARTITION_EOF:
                continue

            if code == KafkaError.UNKNOWN_TOPIC_OR_PART or code == 3:
                print(f"[inventory] topic not ready yet, waiting... err={err}")
                time.sleep(1.0)
                continue

            if code in (KafkaError._ALL_BROKERS_DOWN, KafkaError._TRANSPORT, KafkaError._TIMED_OUT):
                print(f"[inventory] kafka not ready yet, waiting... err={err}")
                time.sleep(1.0)
                continue

            raise KafkaException(err)

        try:
            event = json.loads(msg.value().decode("utf-8"))
            result = reserve_inventory(event)
            key = msg.key().decode("utf-8") if msg.key() else None

            print(
                f"[inventory] OK key={key} partition={msg.partition()} offset={msg.offset()} "
                f"reserved_items={len(result['reserved'])}"
            )

            c.commit(msg)
        except Exception as e:
            print(f"[inventory] ERROR offset={msg.offset()} err={e} (no commit, will retry)")
            time.sleep(1.0)

finally:
    c.close()
