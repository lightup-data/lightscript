import logging
import os
from typing import Any, Optional
from uuid import UUID

from api_handler import LightupAPIHandler
from models import Config, EntityType, ResourceFilter
from utils import matches_patterns, setup_logging, update_nested_dict


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
        data = self.source_handler.list(entity_type, self.source_query_params)

        logging.info(f"Found {len(data)} entities")
        filtered_data = self._apply_filters(
            data, source_filters.include, source_filters.exclude
        )

        logging.info("Removing system generated/ read-only fields.")
        cleaned_data = [
            self._drop_system_fields(entity_type, entity) for entity in filtered_data
        ]

        logging.info(f"Validating {len(cleaned_data)} entities")
        validated_data = [
            entity
            for entity in cleaned_data
            if self._validate_entity(entity_type, entity)
        ]

        logging.info(f"Creating {len(validated_data)} entities")
        for entity in validated_data:
            update_nested_dict(entity, target_override_fields)
            logging.info(
                f"Creating {entity_type.name} : {entity.get('metadata', {}).get('name', '')}"
            )
            if not self.dry_run:
                self.target_handler.post(entity_type, entity)

    def _apply_filters(
        self,
        data: list[dict[str, Any]],
        include_patterns: list[str],
        exclude_patterns: list[str],
    ) -> list[dict[str, Any]]:
        logging.info("Filtering data based on specified patterns")
        filtered_data = []
        for entity in data:
            name = entity.get("metadata", {}).get("name", "")
            if matches_patterns(name, include_patterns) and not matches_patterns(
                name, exclude_patterns
            ):
                filtered_data.append(entity)

        return filtered_data

    def _drop_system_fields(
        self, entity_type: EntityType, entity: dict[str, Any]
    ) -> dict[str, Any]:
        entity.pop("status", None)

        metadata = entity.get("metadata", {})
        entity["metadata"] = {
            "name": metadata.get("name", ""),
            "description": metadata.get("description", ""),
            "tags": metadata.get("tags", []),
            "creationType": metadata.get("creationType", "custom"),
        }

        entity.pop("draftMetadata", None)

        entity["config"]["relatedMetrics"] = []
        if "table" in entity["config"]:
            entity["config"]["table"].pop("schemaUuid", None)
            entity["config"]["table"].pop("tableUuid", None)
            entity["config"]["table"].pop("columnUuid", None)
        if "valueColumns" in entity["config"] and entity["config"]["valueColumns"]:
            for column in entity["config"]["valueColumns"]:
                column.pop("columnUuid", None)

        return entity

    def _validate_entity(self, entity_type: EntityType, entity: dict[str, Any]) -> bool:
        entity_name = entity.get("metadata", {}).get("name", "")
        logging.info(
            f"Validating Fully Qualified Name in target workspace for {entity_type.name} : {entity_name}"
        )
        config = entity.get("config", {})

        if "compares" in config and config["compares"]:
            logging.warning(
                "Aggregation Compare Metrics are not supported for migration."
            )
            return False

        source_ids = config.get("sources", [])
        target_source_ids = []
        for source_id in source_ids:
            target_source_id = self._get_target_source(source_id)
            if target_source_id is None:
                return False
            target_source_ids.append(target_source_id)
        config["sources"] = target_source_ids

        target_source_id = target_source_ids[0]
        target_tree = self.target_handler.get_source_tree(target_source_id)

        schema_to_validate = None
        table_to_validate = None
        columns_to_validate = []
        if "table" in config:
            source_table = config["table"]
            if "schemaName" in source_table:
                schema_to_validate = source_table["schemaName"]
            if "tableName" in source_table:
                table_to_validate = source_table["tableName"]
            if "columnName" in source_table:
                columns_to_validate.append(source_table["columnName"])
        if config.get("table", {}).get("type", "") != "customSql":
            if "valueColumns" in config and config["valueColumns"]:
                columns_to_validate.extend(
                    [column["columnName"] for column in config["valueColumns"]]
                )
            if "sliceByColumns" in config and config["sliceByColumns"]:
                columns_to_validate.extend(config["sliceByColumns"])
            if "timestampColumn" in config and config["timestampColumn"]:
                columns_to_validate.append(config["timestampColumn"])
            if "partitions" in config and config["partitions"]:
                columns_to_validate.extend(
                    [column["columnName"] for column in config["partitions"]]
                )

        is_valid_tree = self._validate_tree(
            target_tree, schema_to_validate, table_to_validate, columns_to_validate
        )
        if not is_valid_tree:
            return False

        for table in [config.get("sourceTable", None), config.get("targetTable", None)]:
            if table is None:
                continue
            source_id = table["sourceUuid"]
            target_source_id = self._get_target_source(source_id)
            if target_source_id is None:
                return False
            table["sourceUuid"] = target_source_id

            target_tree = self.target_handler.get_source_tree(target_source_id)

            schema_to_validate = None
            table_to_validate = None
            columns_to_validate = []
            if "table" in table:
                source_table = table["table"]
                if "schemaName" in source_table:
                    schema_to_validate = source_table["schemaName"]
                if "tableName" in source_table:
                    table_to_validate = source_table["tableName"]
                if "columnName" in source_table:
                    columns_to_validate.append(source_table["columnName"])
            if "valueColumns" in config and config["valueColumns"]:
                columns_to_validate.extend(config["valueColumns"])
            if "sliceByColumns" in config and config["sliceByColumns"]:
                columns_to_validate.extend(config["sliceByColumns"])
            if "attributeColumns" in config and config["attributeColumns"]:
                columns_to_validate.extend(config["attributeColumns"])
            if "timestampColumn" in config and config["timestampColumn"]:
                columns_to_validate.append(config["timestampColumn"])
            if "partitions" in config and config["partitions"]:
                columns_to_validate.extend(
                    [column["columnName"] for column in config["partitions"]]
                )

            is_valid_tree = self._validate_tree(
                target_tree, schema_to_validate, table_to_validate, columns_to_validate
            )
            if not is_valid_tree:
                return False

        logging.info(f"Validation successful for {entity_type.name} : {entity_name}")
        return True

    def _validate_tree(
        self,
        target_tree: dict[str, Any],
        schema_name: Optional[str],
        table_name: Optional[str],
        column_names: list[str],
    ) -> bool:
        if schema_name is not None:
            matching_schemas = [
                schema
                for schema in target_tree["schemas"]
                if schema["name"] == schema_name
            ]
            if len(matching_schemas) == 0:
                logging.warning(f"Schema {schema_name} not found in target workspace.")
                return False
            if len(matching_schemas) > 1:
                logging.warning(
                    f"Multiple schemas with name {schema_name} found in target workspace."
                )
                return False
        else:
            # Consider all schemas for further matching
            matching_schemas = target_tree["schemas"]

        if table_name is not None:
            matching_tables = []
            for schema in matching_schemas:
                matching_tables.extend(
                    [tbl for tbl in schema["tables"] if tbl["tableName"] == table_name]
                )
            if len(matching_tables) == 0:
                logging.warning(f"Table {table_name} not found in target workspace.")
                return False
            if len(matching_tables) > 1:
                logging.warning(
                    f"Multiple tables with name {table_name} found in target workspace."
                )
                return False
        else:
            matching_tables = [
                tbl for schema in matching_schemas for tbl in schema["tables"]
            ]

        for column_name in column_names:
            matching_columns = []
            for table in matching_tables:
                matching_columns.extend(
                    [
                        clm
                        for clm in table["columns"]
                        if clm["columnName"] == column_name
                    ]
                )
            if len(matching_columns) == 0:
                logging.warning(f"Column {column_name} not found in target workspace.")
                return False
            if len(matching_columns) > 1:
                logging.warning(
                    f"Multiple columns with name {column_name} found in target workspace."
                )
                return False

        return True

    def _get_target_source(self, source_id: UUID) -> Optional[str]:
        source = self.source_handler.get(EntityType.SOURCES, source_id)
        source_name = source.get("metadata", {}).get("name", "")
        target_sources = self.target_handler.list(EntityType.SOURCES, {})
        matching_sources = [
            source
            for source in target_sources
            if source.get("metadata", {}).get("name", "") == source_name
        ]
        if len(matching_sources) == 0:
            logging.warning(f"Source {source_name} not found in target workspace.")
            return None
        if len(matching_sources) > 1:
            logging.warning(
                f"Multiple sources with name {source_name} found in target workspace."
            )
            return None
        return matching_sources[0].get("metadata", {}).get("uuid", "")


if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    config = Config.from_json(config_path)
    handler = MigrationHandler(config)
    handler.migrate()
