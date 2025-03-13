import json
import logging
from typing import Any, Optional
from urllib.parse import urlencode
from uuid import UUID

from api_handler import LightupAPIHandler
from models import Config, EntityType, ResourceFilter
from utils import get_entity_name, matches_patterns, setup_logging, update_nested_dict


class MigrationHandler:
    def __init__(self, config: Config) -> None:
        self.source_config = config.source
        self.target_config = config.target
        migration_settings = config.migration_settings

        if migration_settings.logging.enabled:
            setup_logging(
                migration_settings.logging.log_file,
                migration_settings.logging.log_level,
            )

        self.source_handler = LightupAPIHandler(
            self.source_config.credentials_file, self.source_config.workspace
        )
        self.target_handler = LightupAPIHandler(
            self.target_config.credentials_file, self.target_config.workspace
        )

        self.source_query_params = self.source_config.query_params
        self.dry_run = migration_settings.dry_run

    def migrate(self):
        for entity_type in self.source_config.entity_types:
            logging.info(f"Migrating data for entity type: {entity_type.name}")
            source_filters = self.source_config.resource_filters.get(
                entity_type.value, ResourceFilter()
            )
            target_override_fields = self.target_config.override_fields.get(
                entity_type.value, {}
            )

            self.migrate_entity(
                entity_type,
                source_filters,
                target_override_fields,
            )

        if self.dry_run:
            logging.info(f"Dry run enabled. No data was actually posted.")

    def migrate_entity(
        self,
        entity_type: EntityType,
        source_filters: ResourceFilter,
        target_override_fields: dict[str, Any],
    ):
        query_string = urlencode(self.source_query_params)
        data = self.source_handler.list(entity_type, query_string)

        logging.info(f"Found {len(data)} {entity_type.name} entities")
        filtered_data = self._apply_filters(
            data, source_filters.include, source_filters.exclude
        )

        logging.info(
            f"Removing system generated/ read-only fields for {entity_type.name} entities"
        )
        cleaned_data = [
            self._drop_system_fields(entity_type, entity) for entity in filtered_data
        ]

        logging.info(f"Validating {len(cleaned_data)} {entity_type.name} entities")
        validated_data = []
        for entity in cleaned_data:
            if self._validate_entity(entity_type, entity):
                validated_data.append(entity)
            else:
                logging.debug(f"Validation failed for entity {json.dumps(entity)}")

        logging.info(f"Creating {len(validated_data)} {entity_type.name} entities")
        for entity in validated_data:
            update_nested_dict(entity, target_override_fields)
            logging.info(f"Creating {entity_type.name} : {get_entity_name(entity)}")
            if not self.dry_run:
                self.target_handler.post(entity_type, entity)

    def _apply_filters(
        self,
        data: list[dict[str, Any]],
        include_patterns: list[str],
        exclude_patterns: list[str],
    ) -> list[dict[str, Any]]:
        logging.info("Filtering data based on specified patterns")
        return [
            entity
            for entity in data
            if matches_patterns(get_entity_name(entity), include_patterns)
            and not matches_patterns(get_entity_name(entity), exclude_patterns)
        ]

    def _drop_system_fields(
        self, entity_type: EntityType, entity: dict[str, Any]
    ) -> dict[str, Any]:
        entity.pop("status", None)
        entity.pop("draftMetadata", None)

        metadata = entity.get("metadata", {})
        entity["metadata"] = {
            key: metadata.get(key, default)
            for key, default in {
                "name": "",
                "description": "",
                "tags": [],
                "creationType": "custom",
            }.items()
        }

        entity_config = entity.get("config", {})
        entity_config["relatedMetrics"] = []

        table_keys = ["schemaUuid", "tableUuid", "columnUuid"]

        if "table" in entity_config:
            for key in table_keys:
                entity_config["table"].pop(key, None)

        if value_columns := entity_config.get("valueColumns"):
            for column in value_columns:
                column.pop("columnUuid", None)

        for table in filter(
            None, [entity_config.get("sourceTable"), entity_config.get("targetTable")]
        ):
            if "table" in table:
                for key in table_keys:
                    table["table"].pop(key, None)

        return entity

    def _validate_entity(self, entity_type: EntityType, entity: dict[str, Any]) -> bool:
        entity_name = get_entity_name(entity)
        logging.info(
            f"Validating Fully Qualified Name in target workspace for {entity_type.name} : {entity_name}"
        )

        config = entity.get("config", {})

        if config.get("compares"):
            logging.warning(
                f"Aggregation Compare Metrics are not supported for migration. Metric: {entity_name}"
            )
            return False

        source_ids = config.get("sources", [])
        target_source_ids = [
            self._get_target_source(source_id, entity_name) for source_id in source_ids
        ]
        if None in target_source_ids:
            return False
        config["sources"] = target_source_ids

        target_tree = self.target_handler.get_source_tree(target_source_ids[0])
        (
            schema_to_validate,
            table_to_validate,
            columns_to_validate,
        ) = self._get_columns_to_validate(config, entity_name)
        if not self._validate_tree(
            target_tree,
            schema_to_validate,
            table_to_validate,
            columns_to_validate,
            entity_name,
        ):
            return False

        for table in filter(
            None, [config.get("sourceTable"), config.get("targetTable")]
        ):
            source_id = table["sourceUuid"]
            target_source_id = self._get_target_source(source_id, entity_name)
            if target_source_id is None:
                return False
            table["sourceUuid"] = target_source_id

            target_tree = self.target_handler.get_source_tree(target_source_id)
            (
                schema_to_validate,
                table_to_validate,
                columns_to_validate,
            ) = self._get_columns_to_validate(table, entity_name)
            if not self._validate_tree(
                target_tree,
                schema_to_validate,
                table_to_validate,
                columns_to_validate,
                entity_name,
            ):
                return False

        logging.info(f"Validation successful for {entity_type.name} : {entity_name}")
        return True

    def _get_columns_to_validate(
        self, config: dict[str, Any], entity_name: str
    ) -> tuple[Optional[str], Optional[str], list[str]]:
        schema_name = config.get("table", {}).get("schemaName")
        table_name = config.get("table", {}).get("tableName")

        columns = []
        if "table" in config and "columnName" in config["table"]:
            columns.append(config["table"]["columnName"])

        column_keys = [
            "valueColumns",
            "sliceByColumns",
            "partitions",
            "attributeColumns",
        ]
        if config.get("table", {}).get("type") == "customSql":
            return schema_name, table_name, columns

        for key in column_keys:
            if key not in config:
                continue
            if isinstance(config[key], str):
                columns.append(config[key])
            elif isinstance(config[key], list):
                for col in config[key]:
                    if isinstance(col, dict):
                        column_name = col.get("columnName")
                        if column_name:
                            columns.append(column_name)
                    elif isinstance(col, str):
                        columns.append(col)
                    else:
                        logging.warning(
                            f"Unexpected format for column in {key}: {col} for entity {entity_name}"
                        )
            else:
                logging.warning(
                    f"Unexpected format for key {key}: {config[key]} for entity {entity_name}"
                )

        return schema_name, table_name, columns

    def _validate_tree(
        self,
        target_tree: dict[str, Any],
        schema_name: Optional[str],
        table_name: Optional[str],
        column_names: list[str],
        entity_name: str,
    ) -> bool:
        schemas = target_tree.get("schemas", [])

        if schema_name:
            matching_schemas = (s for s in schemas if s["name"] == schema_name)
            try:
                first_match = next(matching_schemas)
                second_match = next(matching_schemas, None)
                if second_match:
                    logging.warning(
                        f"Multiple schemas with name {schema_name} found in target workspace for entity {entity_name}"
                    )
                    return False
            except StopIteration:
                logging.warning(
                    f"Schema {schema_name} not found in target workspace for entity {entity_name}"
                )
                return False
            schemas = [first_match]

        tables = [tbl for schema in schemas for tbl in schema.get("tables", [])]
        if table_name:
            matching_tables = (t for t in tables if t["tableName"] == table_name)
            try:
                first_match = next(matching_tables)
                second_match = next(matching_tables, None)
                if second_match:
                    logging.warning(
                        f"Multiple tables with name {table_name} found in target workspace for entity {entity_name}"
                    )
                    return False
            except StopIteration:
                logging.warning(
                    f"Table {table_name} not found in target workspace for entity {entity_name}"
                )
                return False
            tables = [first_match]

        for column_name in column_names:
            matching_columns = (
                c
                for table in tables
                for c in table.get("columns", [])
                if c["columnName"] == column_name
            )
            try:
                first_match = next(matching_columns)
                second_match = next(matching_columns, None)
                if second_match:
                    logging.warning(
                        f"Multiple columns with name {column_name} found in target workspace for entity {entity_name}"
                    )
                    return False
            except StopIteration:
                logging.warning(
                    f"Column {column_name} not found in target workspace for entity {entity_name}"
                )
                return False

        return True

    def _get_target_source(self, source_id: UUID, entity_name: str) -> Optional[str]:
        source = self.source_handler.get(EntityType.SOURCE, source_id)
        source_metadata = source.get("metadata", {})
        source_name = source_metadata.get("name", "")

        target_sources = self.target_handler.list(EntityType.SOURCE, "")

        matches = (
            s
            for s in target_sources
            if s.get("metadata", {}).get("name", "") == source_name
        )

        try:
            first_match = next(matches)
            second_match = next(matches, None)
            if second_match:
                logging.warning(
                    f"Multiple sources with name {source_name} found in target workspace for entity {entity_name}"
                )
                return None
            return first_match.get("metadata", {}).get("uuid", "")
        except StopIteration:
            logging.warning(
                f"Source {source_name} not found in target workspace for entity {entity_name}"
            )
            return None
