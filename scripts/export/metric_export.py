#!/usr/bin/env python3

"""
Export metrics on a per workspace basis along with associated metadata including
data asset information as well as monitor information. Depends on lightctl, if
the dependencies for this repo are met, this script will run.

Export metrics for each workspace in the following path:
path/<export_epoch_time>/<workspace_uuid>.csv

See usage: python metric_export.py --help
"""

import argparse
import csv
import os
import time

from lightctl.client.metric_client import MetricClient
from lightctl.client.monitor_client import MonitorClient
from lightctl.client.source_client import SourceClient
from lightctl.client.workspace_client import WorkspaceClient

EXPORT_DIRECTORY_PATH = "/tmp/lightupexport/"
DEBUG = False


def dprint(*args):
    if DEBUG:
        print(*args)


def export_to_csv(workspace_id: str, metric_map: dict, start_time: int):
    start_ts = time.time()

    if not metric_map:
        return

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

    dprint(f"write to {csv_file} completed in {time.time() - start_ts} seconds")


def main():
    export_time = time.time()

    workspace_client = WorkspaceClient()
    source_client = SourceClient()
    metric_client = MetricClient()
    monitor_client = MonitorClient()

    workspaces = workspace_client.list_workspaces()

    for ws in workspaces:
        start_time = time.time()
        workspace_id = ws["uuid"]
        dprint()
        dprint(f"processing workspace {ws['name']} ({ws['uuid']})")

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
        dprint(
            f"metric export for workspace '{ws['name']}' completed in "
            f"{time.time()-start_time} seconds."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export list of configured metrics and associated monitors"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--path", type=str, help="Path to store the csv files")

    args = parser.parse_args()

    DEBUG = args.debug or DEBUG
    EXPORT_DIRECTORY_PATH = args.path or EXPORT_DIRECTORY_PATH

    main()
