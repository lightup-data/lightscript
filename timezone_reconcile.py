from lightctl.client.metric_client import MetricClient
from lightctl.client.profiler_client import ProfilerClient

"""
This script reconciles timezones for tables that ended up with mismatched
timezones.
"""


pc = ProfilerClient()
mc = MetricClient()

# user input
ws = ""  # workspace uuid to update
sources = []  # list of source uuids within workspace to update

for source in sources:
    tables = pc.list_tables(ws, source)

    tables_with_mismatched_timezones = []
    for table in tables["data"]:
        if not table["profilerConfig"]["enabled"]:
            continue
        config = table["profilerConfig"]
        if config["dataTimezone"] != config["timezone"]:
            tables_with_mismatched_timezones.append(table)

    print(
        "uuid schemaName tableName timestampColumnName timestampColumnType queryTimezone dataTimezone"
    )
    for table in tables_with_mismatched_timezones:
        columns = pc.list_columns(ws, source, table["uuid"])
        timestamp_column_type = None
        for col in columns:
            if col["columnName"] == table["profilerConfig"]["timestampColumn"]:
                timestamp_column_type = col["columnType"]
                break
        print(
            table["uuid"],
            table["schemaName"],
            table["tableName"],
            table["profilerConfig"]["timestampColumn"],
            timestamp_column_type,
            table["profilerConfig"]["timezone"],
            table["profilerConfig"]["dataTimezone"],
        )

    if input("Update the tables above? Only 'yes' will update the tables: ") == "yes":
        print("Updating tables .... ")
        for table in tables_with_mismatched_timezones:
            config = table["profilerConfig"]
            config["dataTimezone"] = config["timezone"]
            pc.update_table_profiler_config(ws, source, table["uuid"], config)
        print("done")
    else:
        print("Skipping updates.")
