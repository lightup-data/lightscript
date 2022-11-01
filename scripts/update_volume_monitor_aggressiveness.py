#!/usr/bin/env python3
"""
Run using python3

python3 update_volume_monitor_aggressiveness.py

This script updates the aggressiveness for all anomaly detection monitors for volume
metrics. If the aggressiveness level is set to 7, it will get updated to level 3.

The script extracts all metrics and monitors to help match the schema. The filtering
is done in the code rather than querying the backend to make it easy to update.

It reconfigures the monitors by:
1. turning the monitor offline
2. updating the aggressiveness level and then turning it back online.
"""

from lightctl.client.metric_client import MetricClient
from lightctl.client.monitor_client import MonitorClient

# update these with the values.
WORKSPACE_ID = "updateme"  # workspace uuid
SCHEMA_NAME = "updateme"  # schema name
DATASOURCE_ID = "updateme"  # datasource uuid
DRY_RUN = True  # set to False to update configuration

monitor_client = MonitorClient()
metric_client = MetricClient()


def monitor_str(monitor):
    return f'{monitor["metadata"]["name"]} ({monitor["metadata"]["uuid"]})'


def get_volume_monitors_in_workspace_and_schema(workspace_id, source_uuid, schema_name):
    assert workspace_id != "updateme"
    assert source_uuid != "updateme"
    assert schema_name != "updateme"

    monitors = monitor_client.list_monitors(workspace_id)
    metrics = metric_client.list_metrics(workspace_id)
    metric_dict = {}
    for metric in metrics:
        if metric["config"].get("configType") != "metricConfig":
            continue

        if metric["config"]["table"]["type"] == "customSql":
            continue

        # filter out only volume metrics associated with the source and schema name
        if (
            metric["config"]["aggregation"]["type"] == "volume"
            and metric["config"]["table"]["schemaName"] == schema_name
            and source_uuid in metric["config"]["sources"]
        ):
            metric_uuid = metric["metadata"]["uuid"]
            metric_dict[metric_uuid] = metric

    monitor_list = []

    for monitor in monitors:
        # skip monitors that are not running
        if monitor["config"]["isLive"] is False:
            continue

        metric_uuid = monitor["config"]["metrics"][0]
        if metric_uuid in metric_dict:
            monitor_list.append(monitor)

    return monitor_list


def update_aggressiveness_retrain_and_enable(monitors):
    for monitor in monitors:
        if monitor["config"]["symptom"]["type"] not in [
            "valueOutsideExpectations",
            "valueOutsideExpectationsWithTrend",
        ]:
            continue

        if monitor["config"]["symptom"]["aggressiveness"]["level"] != 7:
            continue

        print(f"Updating monitor {monitor_str(monitor)}")

        if not DRY_RUN:
            # disable monitor
            monitor["config"]["isLive"] = False
            monitor_client.update_monitor(
                monitor["metadata"]["workspaceId"], monitor["metadata"]["uuid"], monitor
            )

            # update training configuration
            monitor["config"]["symptom"]["aggressiveness"] = {"level": 3}
            monitor["config"]["isLive"] = True
            monitor_client.update_monitor(
                monitor["metadata"]["workspaceId"], monitor["metadata"]["uuid"], monitor
            )

            print(f"monitor update succeeded - {monitor['metadata']['uuid']}")


candidate_monitors = get_volume_monitors_in_workspace_and_schema(
    WORKSPACE_ID, DATASOURCE_ID, SCHEMA_NAME
)

update_aggressiveness_retrain_and_enable(candidate_monitors)
