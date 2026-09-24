import json


REQUIRED_EVENT_FIELDS = {
    "event_id",
    "user_id",
    "session_id",
    "event_type",
    "timestamp",
    "product_id",
}


def validate_jsonl_line(line: str):
    if not isinstance(line, str):
        return False, None, "Input is not a string"

    line = line.strip()

    if not line:
        return False, None, "Empty line"

    try:
        event_dict = json.loads(line)

    except json.JSONDecodeError as error:
        return False, None, f"Invalid JSON: {error}"

    if not isinstance(event_dict, dict):
        return False, None, "JSON record is not an object"

    missing_fields = sorted(
        REQUIRED_EVENT_FIELDS - set(event_dict.keys())
    )

    if missing_fields:
        return (
            False,
            None,
            "Missing required fields: "
            + ", ".join(missing_fields)
        )

    for field_name in REQUIRED_EVENT_FIELDS:

        value = event_dict.get(field_name)

        if value is None:
            return (
                False,
                None,
                f"Required field '{field_name}' is null"
            )

        if isinstance(value, str) and not value.strip():
            return (
                False,
                None,
                f"Required field '{field_name}' is empty"
            )

    return True, event_dict, ""