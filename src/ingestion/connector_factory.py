import yaml
from pathlib import Path
from typing import List

from src.ingestion.connectors.base import BaseConnector
from src.ingestion.connectors.file_connector import FileConnector
from src.ingestion.connectors.api_connector import APIConnector
from src.common.schema_validator import SchemaField
from src.ingestion.connectors.pubsub_connector import PubSubConnector

# Maps the "type" string in YAML to the actual connector class that handles it
CONNECTOR_REGISTRY = {
    "file": FileConnector,
    "api": APIConnector,
    "pubsub": PubSubConnector,
}


def load_connectors(config_path: str = "src/ingestion/config/sources.yaml") -> List[BaseConnector]:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    connectors = []
    for source_cfg in config["sources"]:
        source_type = source_cfg.pop("type")
        source_name = source_cfg.pop("name")
        schema_cfg = source_cfg.pop("schema", None)
        key_field_cfg = source_cfg.pop("key_field", None)

        connector_class = CONNECTOR_REGISTRY.get(source_type)
        if connector_class is None:
            raise ValueError(f"Unknown connector type: {source_type}")

        connector = connector_class(source_name=source_name, **source_cfg)

        if schema_cfg:
            connector.schema = [SchemaField(**f) for f in schema_cfg]

        if key_field_cfg:
            connector.key_field = key_field_cfg

        connectors.append(connector)

    return connectors