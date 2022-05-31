from lightctl.client.profiler_client import ProfilerClient

WORKSPACE_UUID = ""
SOURCE_UUID = ""
TABLE_LIST = {"schema": ["list", "of", "tables"]}
TIMESTAMP_COLUMN_NAME = ""


TABLE_PROFILER_CONFIG = {
    "enabled": True,
    "timestampColumn": TIMESTAMP_COLUMN_NAME,
    "window": "hour",
    "tableSchemaChange": {"enabled": False},
    "dataDelay": {"enabled": True},
    "volume": {"enabled": True},
}

pc = ProfilerClient()


for schema in TABLE_LIST:
    table_list = TABLE_LIST[schema]
    for table in table_list:
        table_uuid = pc.table_uuid_from_table_name(
            workspace_id=WORKSPACE_UUID,
            source_uuid=SOURCE_UUID,
            table_name=table,
            schema_name=schema,
        )
        if not table_uuid:
            print(
                f"error - could not find {schema=}, {table=} in {WORKSPACE_UUID=}, {SOURCE_UUID=}"
            )
            continue

        pc.update_table_profiler_config(
            workspace_id=WORKSPACE_UUID,
            source_uuid=SOURCE_UUID,
            table_uuid=table_uuid,
            data=TABLE_PROFILER_CONFIG,
        )
