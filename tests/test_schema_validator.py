from src.common.schema_validator import validate_records, SchemaField


def test_valid_record_passes():
    schema = [
        SchemaField(field="customer_id", dtype="int"),
        SchemaField(field="email", dtype="str"),
    ]
    records = [{"customer_id": "1", "email": "test@example.com"}]

    valid, rejected = validate_records(records, schema)

    assert len(valid) == 1
    assert len(rejected) == 0


def test_missing_required_field_is_rejected():
    schema = [
        SchemaField(field="customer_id", dtype="int"),
        SchemaField(field="email", dtype="str"),
    ]
    records = [{"customer_id": "1", "email": ""}]

    valid, rejected = validate_records(records, schema)

    assert len(valid) == 0
    assert len(rejected) == 1
    assert "missing required field 'email'" in rejected[0]["errors"]


def test_wrong_type_is_rejected():
    schema = [SchemaField(field="customer_id", dtype="int")]
    records = [{"customer_id": "not_a_number"}]

    valid, rejected = validate_records(records, schema)

    assert len(valid) == 0
    assert len(rejected) == 1


def test_mixed_valid_and_invalid_records():
    schema = [SchemaField(field="customer_id", dtype="int")]
    records = [
        {"customer_id": "1"},
        {"customer_id": "not_a_number"},
        {"customer_id": "3"},
    ]

    valid, rejected = validate_records(records, schema)

    assert len(valid) == 2
    assert len(rejected) == 1