from dataclasses import dataclass
from typing import List, Dict, Any, Tuple


@dataclass
class SchemaField:
    field: str
    dtype: str
    required: bool = True


TYPE_MAP = {"int": int, "str": str, "float": float, "bool": bool}


def validate_records(
    records: List[Dict[str, Any]],
    schema: List[SchemaField],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Checks each record against the schema.
    Returns (valid_records, rejected_records_with_reason)."""
    valid_records = []
    rejected_records = []

    for record in records:
        errors = []

        for field_def in schema:
            value = record.get(field_def.field)

            if value is None or value == "":
                if field_def.required:
                    errors.append(f"missing required field '{field_def.field}'")
                continue

            expected_type = TYPE_MAP.get(field_def.dtype)
            if expected_type and expected_type is not str:
                try:
                    expected_type(value)
                except (ValueError, TypeError):
                    errors.append(
                        f"field '{field_def.field}' expected {field_def.dtype}, got '{value}'"
                    )

        if errors:
            rejected_records.append({"record": record, "errors": errors})
        else:
            valid_records.append(record)

    return valid_records, rejected_records