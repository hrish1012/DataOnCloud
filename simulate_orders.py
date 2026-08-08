import json
import random
import time
from datetime import datetime, timezone

from google.cloud import pubsub_v1
from src.common.gcp_config import GCP_PROJECT_ID

TOPIC_ID = "orders-events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(GCP_PROJECT_ID, TOPIC_ID)

PRODUCTS = ["Laptop", "Headphones", "Keyboard", "Monitor", "Mouse"]


def generate_order():
    return {
        "order_id": random.randint(10000, 99999),
        "product": random.choice(PRODUCTS),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(500, 50000), 2),
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def publish_orders(count: int = 5, delay_seconds: float = 1.0):
    for _ in range(count):
        order = generate_order()
        data = json.dumps(order).encode("utf-8")

        future = publisher.publish(topic_path, data)
        message_id = future.result()

        print(f"Published order {order['order_id']} -> message_id: {message_id}")
        time.sleep(delay_seconds)


if __name__ == "__main__":
    publish_orders(count=5, delay_seconds=1.0)