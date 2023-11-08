#!/usr/bin/env python3

"""
Export metrics on a per workspace basis along with associated metadata incuding
data asset information as well as monitor information. Depends on lightctl, if
the dependencies for this repo are met, this script will run.

Export metrics for each workspace in the following path:
path/export_epoch_time/workspace_uuid.csv

"""

import csv
import os
import time

from lightctl.client.metric_client import MetricClient
from lightctl.client.monitor_client import MonitorClient
from lightctl.client.source_client import SourceClient
from lightctl.client.workspace_client import WorkspaceClient

source_client = SourceClient()
metric_client = MetricClient()
workspace_client = WorkspaceClient()
monitor_client = MonitorClient()

workspaces = workspace_client.list_workspaces()

# update to appropriate path
EXPORT_DIRECTORY_PATH = input("Output Path: ") + "/metric_output"


def export_to_csv(workspace_id, metric_map, start_time):
    path = EXPORT_DIRECTORY_PATH.rstrip("/") + f"/{start_time}"

    csv_file = f"{path}/{workspace_id}.csv"
    csv_columns = [
        "workspaceId",
        "workspaceName",
        "metricName",
        "metricUuid",
        "metricId",
        "metricCreationType",
        "metricDescription",
        "metricTags",
        "metricDimension",
        "metricConfigType",
        "sourceUuid",
        "sourceName",
        "schemaName",
        "tableName",
        "schemaUuid",
        "tableUuid",
        "collectionMode",
        "columnName",
        "columnUuid",
        "metricIsLive",
        "metricLastSampleTs",
        "metricConfigUpdatedTs",
        "metricRunStatus",
        "monitors",
    ]

    if not os.path.exists(path):
        os.makedirs(path)

    try:
        with open(csv_file, "w", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in metric_map.values():
                writer.writerow(data)
    except OSError:
        print("I/O error {ex}")
        raise


def main():
    start_time = export_time = time.time()

    for ws in workspaces:
        workspace_id = ws["uuid"]

        sources = source_client.list_sources(workspace_id)
        metrics = metric_client.list_metrics(workspace_id)
        monitors = monitor_client.list_monitors(workspace_id)

        source_map = {
            source["metadata"]["uuid"]: source["metadata"]["name"] for source in sources
        }

        metric_map = {}
        for metric in metrics:
            # skip compare metrics
            if metric["config"]["configType"] not in [
                "metricConfig",
                "fullTableMetricConfig",
            ]:
                continue

            metric_uuid = metric["metadata"]["uuid"]
            metric_map[metric_uuid] = {
                "workspaceId": workspace_id,
                "workspaceName": ws["name"],
                "metricName": metric["metadata"]["name"],
                "metricUuid": metric_uuid,
                "metricId": metric["metadata"]["idSerial"],
                "metricCreationType": metric["metadata"]["creationType"],
                "metricDescription": metric["metadata"].get("description", ""),
                "metricTags": metric["metadata"].get("tags", []),
                "metricConfigType": metric["config"]["configType"],
                "metricDimension": metric["config"]["dimension"],
                "sourceUuid": metric["config"]["sources"][0],
                "sourceName": source_map.get(metric["config"]["sources"][0], ""),
                "schemaUuid": metric["config"]["table"].get("schemaUuid", ""),
                "tableUuid": metric["config"]["table"].get("tableUuid", ""),
                "schemaName": metric["config"]["table"].get("schemaName", ""),
                "tableName": metric["config"]["table"].get("tableName", ""),
                "collectionMode": "",
                "columnName": "",
                "columnUuid": "",
                "metricIsLive": metric["config"]["isLive"],
                "metricLastSampleTs": metric["status"].get("lastSampleTs", ""),
                "metricConfigUpdatedTs": metric["status"].get("configUpdatedTs"),
                "metricRunStatus": metric["status"].get("runStatus"),
                "monitors": [],
            }

            if columns := metric["config"].get("valueColumns"):
                metric_map[metric_uuid].update(
                    {
                        "columnName": columns[0]["columnName"],
                        "columnUuid": columns[0].get("columnUuid"),
                    }
                )

            if collection_mode := metric["config"].get("collectionMode"):
                metric_map[metric_uuid]["collectionMode"] = collection_mode["type"]

        for monitor in monitors:
            metric_uuid = monitor["config"]["metrics"][0]

            if metric_map.get(metric_uuid) is None:
                # skip monitors for compare metrics
                continue

            metric_map[metric_uuid]["monitors"].append(
                {
                    "monitorName": monitor["metadata"]["name"],
                    "monitorUuid": monitor["metadata"]["uuid"],
                    "monitorId": monitor["metadata"]["idSerial"],
                    "monitorTags": monitor["metadata"].get("tags", []),
                    "monitorIsLive": monitor["config"]["isLive"],
                    "monitorLiveStartTs": monitor["config"].get("liveStartTs", ""),
                    "monitorLastSampleTs": monitor["status"].get("lastSampleTs", ""),
                    "monitorRunStatus": monitor["status"].get("runStatus", ""),
                    "monitorConfigUpdatedTs": monitor["status"].get("configUpdatedTs"),
                }
            )

        export_to_csv(workspace_id, metric_map, int(export_time))
        print(
            f"metric export for workspace '{ws['name']}' completed in "
            f"{time.time()-start_time} seconds."
        )
        start_time = time.time()


if __name__ == "__main__":
    main()
