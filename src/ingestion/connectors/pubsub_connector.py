import json
from typing import List, Dict, Any

from google.cloud import pubsub_v1
from google.api_core.exceptions import DeadlineExceeded

from src.ingestion.connectors.base import BaseConnector
from src.common.gcp_config import GCP_PROJECT_ID


class PubSubConnector(BaseConnector):
    """Pulls a batch of messages from a Pub/Sub subscription."""

    def __init__(self, subscription_id: str, source_name: str = "pubsub_source", max_messages: int = 10):
        self.subscription_id = subscription_id
        self.source_name = source_name
        self.max_messages = max_messages
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(
            GCP_PROJECT_ID, subscription_id
        )

    def extract(self) -> List[Dict[str, Any]]:
        response = self.subscriber.pull(
            request={
                "subscription": self.subscription_path,
                "max_messages": self.max_messages,
            },
            timeout=10,
        )

        records = []
        ack_ids = []

        for received_message in response.received_messages:
            try:
                data = json.loads(received_message.message.data.decode("utf-8"))
                records.append(data)
                ack_ids.append(received_message.ack_id)
            except json.JSONDecodeError:
                # Malformed message — acknowledge it anyway so it doesn't block the queue forever
                ack_ids.append(received_message.ack_id)

        if ack_ids:
            self.subscriber.acknowledge(
                request={"subscription": self.subscription_path, "ack_ids": ack_ids}
            )

        return records