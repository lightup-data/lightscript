import json
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class EntityType(str, Enum):
    METRIC = "metric"
    MONITOR = "monitor"
    WORKSPACE = "workspace"
    SOURCE = "source"
    PROFILE = "profile"


class ResourceFilter(BaseModel):
    include: list[str] = Field(default_factory=lambda: [".*"])
    exclude: list[str] = Field(default_factory=list)


class SourceConfig(BaseModel):
    credentials_file: Path
    workspace: str
    entity_types: list[EntityType] = Field(default_factory=list)
    query_params: dict[str, Any] = Field(default_factory=dict)
    resource_filters: dict[str, ResourceFilter] = Field(default_factory=dict)


class TargetConfig(BaseModel):
    credentials_file: Path
    workspace: str
    override_fields: dict[str, dict[str, Any]] = Field(default_factory=dict)


class RetryPolicy(BaseModel):
    max_retries: int = 3
    retry_delay_seconds: int = 5


class LoggingConfig(BaseModel):
    enabled: bool = True
    log_level: str = "info"
    log_file: str = "migration.log"


class MigrationSettings(BaseModel):
    dry_run: bool = True
    concurrency: int = 5
    batch_size: int = 100
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


class Config(BaseModel):
    source: SourceConfig
    target: TargetConfig
    migration_settings: MigrationSettings = Field(default_factory=MigrationSettings)

    @classmethod
    def from_json(cls, file_path: str) -> "Config":
        with open(file_path) as f:
            data = json.load(f)
        return cls(**data)
