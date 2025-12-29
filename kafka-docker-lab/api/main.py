import json
import os
import time
import uuid
from typing import Any, Dict, List

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")

app = FastAPI(title="REST -> Kafka (Orders)")

producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "retries": 10,
        "socket.timeout.ms": 10000,
    }
)

class OrderItem(BaseModel):
    sku: str = Field(..., examples=["Coffee-Beans-1kg"])
    qty: int = Field(..., ge=1, examples=[2])
    price: float = Field(..., gt=0, examples=[12.5])

class OrderIn(BaseModel):
    customer_id: str = Field(..., examples=["cust-42"])
    items: List[OrderItem]

@app.get("/health")
def health():
    return {"status": "ok"}

def _delivery_report(err, msg):
    if err is not None:
        print(f"[producer] Delivery failed: {err}")
    else:
        print(f"[producer] Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

@app.post("/orders")
def create_order(order: OrderIn) -> Dict[str, Any]:
    if not order.items:
        raise HTTPException(status_code=400, detail="items must not be empty")

    order_id = str(uuid.uuid4())
    now_ms = int(time.time() * 1000)
    total = round(sum(i.qty * i.price for i in order.items), 2)

    event = {
        "event_type": "OrderCreated",
        "order_id": order_id,
        "customer_id": order.customer_id,
        "created_at_ms": now_ms,
        "items": [i.model_dump() for i in order.items],
        "total": total,
        "schema_version": 1,
    }

    producer.produce(
        TOPIC,
        key=order_id.encode("utf-8"),
        value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
        on_delivery=_delivery_report,
    )

    remaining = producer.flush(10)
    if remaining > 0:
        raise HTTPException(status_code=503, detail="Kafka unavailable, message not delivered")

    return {"order_id": order_id, "event": event, "status": "sent"}
