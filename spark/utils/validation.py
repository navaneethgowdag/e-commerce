import json
from typing import Dict, Any, Tuple
from .schema import VALID_EVENT_TYPES
from .logging_config import get_logger

logger = get_logger("validation")

def validate_event(event: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validates a single event dictionary.
    Returns (is_valid: bool, error_message: str)
    """
    required_fields = ["event_id", "user_id", "product_id", "event_type", "timestamp"]
    
    # 1. Check required fields
    for field in required_fields:
        if field not in event or event[field] is None:
            return False, f"Missing required field: {field}"
            
    # 2. Validate event_type
    if event["event_type"] not in VALID_EVENT_TYPES:
        return False, f"Invalid event_type: {event['event_type']}"
        
    # 3. Validate numeric fields if present
    if "price" in event and event["price"] is not None:
        if not isinstance(event["price"], (int, float)) or event["price"] < 0:
            return False, "Price must be a non-negative number"
            
    if "quantity" in event and event["quantity"] is not None:
        if not isinstance(event["quantity"], int) or event["quantity"] < 0:
            return False, "Quantity must be a non-negative integer"
            
    return True, "Valid"

def validate_jsonl_line(line: str) -> Tuple[bool, Dict[str, Any], str]:
    """
    Validates a single line of JSONL.
    Returns (is_valid, parsed_dict, error_message)
    """
    try:
        event = json.loads(line)
        is_valid, msg = validate_event(event)
        return is_valid, event, msg
    except json.JSONDecodeError as e:
        return False, {}, f"JSON decode error: {str(e)}"