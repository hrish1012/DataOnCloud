import csv
from pathlib import Path
from typing import List, Dict, Any

from src.ingestion.connectors.base import BaseConnector


class FileConnector(BaseConnector):
    """Reads a CSV file and yields one dict per row."""

    def __init__(self, file_path: str, source_name: str = "file_source"):
        self.file_path = Path(file_path)
        self.source_name = source_name

    def extract(self) -> List[Dict[str, Any]]:
        if not self.file_path.exists():
            raise FileNotFoundError(f"No file found at {self.file_path}")

        records = []
        with open(self.file_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(dict(row))

        return records