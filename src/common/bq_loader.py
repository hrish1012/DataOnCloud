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

    def load(self, table_name: str, records: List[Dict[str, Any]], key_field: str = None) -> int:
        if not records:
            return 0

        flattened_records = [self._flatten_record(r) for r in records]
        self._ensure_table_exists(table_name, flattened_records[0])
        table_id = self._table_ref(table_name)

        if not key_field:
            # No natural key provided — fall back to simple append (old behavior)
            errors = self.client.insert_rows_json(table_id, flattened_records)
            if errors:
                logger.error("BigQuery insert had errors", extra={
                    "extra_fields": {"table": table_id, "errors": errors}
                })
                raise RuntimeError(f"Failed to insert rows: {errors}")
            return len(flattened_records)

        # Key provided -> use a temporary table + MERGE for idempotent upsert,
        # loaded via a load job (not streaming insert) to avoid consistency delays
        staging_table_id = f"{table_id}_staging"
        self.client.delete_table(staging_table_id, not_found_ok=True)

        job_config = bigquery.LoadJobConfig(
            schema=self.client.get_table(table_id).schema,
            write_disposition="WRITE_TRUNCATE",
        )
        load_job = self.client.load_table_from_json(
            flattened_records, staging_table_id, job_config=job_config
        )
        load_job.result()  # blocks until the load job actually finishes

        columns = list(flattened_records[0].keys())
        change_condition = " OR ".join(
            [f"target.{c} IS DISTINCT FROM source.{c}" for c in columns if c != key_field]
        )
        update_clause = ", ".join([f"target.{c} = source.{c}" for c in columns if c != key_field])
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{c}" for c in columns])

        merge_query = f"""
            MERGE `{table_id}` AS target
            USING `{staging_table_id}` AS source
            ON target.{key_field} = source.{key_field}
            WHEN MATCHED AND ({change_condition}) THEN
              UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_columns}) VALUES ({insert_values})
        """
        merge_job = self.client.query(merge_query)
        merge_job.result()  # wait for completion

        dml_stats = merge_job.dml_stats
        inserted = dml_stats.inserted_row_count if dml_stats else 0
        updated = dml_stats.updated_row_count if dml_stats else 0
        self.client.delete_table(staging_table_id, not_found_ok=True)

        if inserted and updated:
            logger.info("Upserted records into BigQuery (inserts and updates)", extra={
                "extra_fields": {"table": table_id, "inserted": inserted, "updated": updated}
            })
        elif inserted:
            logger.info("Inserted new records into BigQuery", extra={
                "extra_fields": {"table": table_id, "inserted": inserted}
            })
        elif updated:
            logger.info("Updated existing records in BigQuery", extra={
                "extra_fields": {"table": table_id, "updated": updated}
            })
        else:
            logger.info("No new or changed records to load", extra={
                "extra_fields": {"table": table_id}
            })

        return len(flattened_records)