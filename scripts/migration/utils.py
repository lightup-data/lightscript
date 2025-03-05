import logging
import re
from typing import Any


def setup_logging(log_file: str, log_level: str):
    logging.basicConfig(
        filename=log_file,
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def matches_patterns(value: str, patterns: list[str]) -> bool:
    return any(re.search(pattern, value) for pattern in patterns)


def update_nested_dict(original: dict[str, Any], updates: dict[str, Any]) -> None:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(original.get(key), dict):
            update_nested_dict(original[key], value)
        else:
            original[key] = value


def get_entity_name(entity: dict[str, Any]):
    return entity.get("metadata", {}).get("name", "")
