import json
from google.cloud import bigquery
from typing import List, Dict, Any

from src.common.gcp_config import GCP_PROJECT_ID
from src.common.logger import get_logger

logger = get_logger(__name__)

DATASET_ID = "dataoncloud_raw"


class BigQueryLoader:
    def __init__(self):
        self.client = bigquery.Client(project=GCP_PROJECT_ID)

    def _table_ref(self, table_name: str) -> str:
        return f"{GCP_PROJECT_ID}.{DATASET_ID}.{table_name}"

    def _flatten_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        flattened = {}
        for key, value in record.items():
            if isinstance(value, (dict, list)):
                flattened[key] = json.dumps(value)
            else:
                flattened[key] = value
        return flattened
    
    def _ensure_table_exists(self, table_name: str, sample_record: Dict[str, Any]):
        table_id = self._table_ref(table_name)
        try:
            self.client.get_table(table_id)
        except Exception:
            logger.info("Table not found, creating it", extra={
                "extra_fields": {"table": table_id}
            })
            schema = [
                bigquery.SchemaField(key, "STRING")
                for key in sample_record.keys()
            ]
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)

    def load(self, table_name: str, records: List[Dict[str, Any]]) -> int:
        if not records:
            return 0

        flattened_records = [self._flatten_record(r) for r in records]

        self._ensure_table_exists(table_name, flattened_records[0])
        table_id = self._table_ref(table_name)

        errors = self.client.insert_rows_json(table_id, flattened_records)

        if errors:
            logger.error("BigQuery insert had errors", extra={
                "extra_fields": {"table": table_id, "errors": errors}
            })
            raise RuntimeError(f"Failed to insert rows: {errors}")

        logger.info("Loaded records into BigQuery", extra={
            "extra_fields": {"table": table_id, "row_count": len(flattened_records)}
        })
        return len(flattened_records) 