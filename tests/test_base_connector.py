import pytest
from src.ingestion.connectors.base import BaseConnector


class FailingConnector(BaseConnector):
    """A test-only connector that always raises, to verify run() handles it gracefully."""
    source_name = "failing_test_source"

    def extract(self):
        raise ValueError("Simulated extraction failure")


class WorkingConnector(BaseConnector):
    """A test-only connector that returns fixed data, no external dependencies."""
    source_name = "working_test_source"

    def extract(self):
        return [{"id": 1, "name": "Test Record"}]


def test_failed_extraction_does_not_raise(mocker):
    # Prevent this test from trying to actually write to BigQuery
    mocker.patch("src.ingestion.connectors.base.BaseConnector._write_audit_record")

    connector = FailingConnector()
    result = connector.run()  # should NOT raise, even though extract() does

    assert result.status == "failed"
    assert "Simulated extraction failure" in result.error_message
    assert result.rows_extracted == 0


def test_successful_extraction_returns_success(mocker):
    mocker.patch("src.ingestion.connectors.base.BaseConnector._write_audit_record")
    mocker.patch("src.common.bq_loader.BigQueryLoader.load", return_value=1)

    connector = WorkingConnector()
    result = connector.run()

    assert result.status == "success"
    assert result.rows_extracted == 1


def test_every_run_gets_a_unique_run_id(mocker):
    mocker.patch("src.ingestion.connectors.base.BaseConnector._write_audit_record")
    mocker.patch("src.common.bq_loader.BigQueryLoader.load", return_value=1)

    connector = WorkingConnector()
    result1 = connector.run()
    result2 = connector.run()

    assert result1.run_id != result2.run_id