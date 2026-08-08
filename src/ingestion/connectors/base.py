import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Any,Optional
from src.common.schema_validator import validate_records, SchemaField
from src.common.logger import get_logger
from src.common.bq_loader import BigQueryLoader

logger = get_logger(__name__)


@dataclass
class IngestionResult:
    """A structured record of one ingestion run — this becomes our audit trail."""
    run_id: str
    source_name: str
    status: str
    rows_extracted: int = 0
    rows_valid: int = 0
    rows_rejected: int = 0
    error_message: str = ""
    started_at: str = ""
    finished_at: str = ""
    rows_loaded: int = 0


class BaseConnector(ABC):
    """
    Every data source connector inherits from this.
    extract() is abstract -> each subclass MUST implement it.
    run() is concrete -> shared logic every connector gets for free.
    """

    source_name: str = "unnamed_source"
    schema: Optional[List[SchemaField]] = None
    key_field: Optional[str] = None

    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """Fetch raw records from the source. Subclass-specific."""
        raise NotImplementedError

    def run(self) -> IngestionResult:
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc).isoformat()

        logger.info("Ingestion run started", extra={
            "extra_fields": {"run_id": run_id, "source": self.source_name}
        })

        try:
            records = self.extract()

            if self.schema:
                valid_records, rejected_records = validate_records(records, self.schema)
            else:
                valid_records, rejected_records = records, []

            if rejected_records:
                logger.warning("Some records failed schema validation", extra={"extra_fields": {
                    "run_id": run_id,
                    "rejected_count": len(rejected_records),
                    "sample_error": rejected_records[0]["errors"],
                }})

            loaded_count = 0
            if valid_records:
                loader = BigQueryLoader()
                loaded_count = loader.load(self.source_name, valid_records, key_field=self.key_field)

            result = IngestionResult(
                run_id=run_id, source_name=self.source_name, status="success",
                rows_extracted=len(records),
                rows_valid=len(valid_records),
                rows_rejected=len(rejected_records),
                started_at=started_at,
                finished_at=datetime.now(timezone.utc).isoformat(),
                rows_loaded=loaded_count
            )
            logger.info("Ingestion run succeeded", extra={"extra_fields": {
                "run_id": run_id, "rows_valid": result.rows_valid, "rows_rejected": result.rows_rejected
            }})
            return result

        except Exception as e:
            result = IngestionResult(
                run_id=run_id, source_name=self.source_name, status="failed",
                error_message=str(e), started_at=started_at,
                finished_at=datetime.now(timezone.utc).isoformat(),
            )
            logger.error("Ingestion run failed", extra={"extra_fields": {
                "run_id": run_id, "error": str(e)
            }})
            return result