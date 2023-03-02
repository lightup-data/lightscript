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
EXPORT_DIRECTORY_PATH = "/tmp/lightupexport/"


def export_to_csv(workspace_id, metric_map, start_time):
    path = EXPORT_DIRECTORY_PATH.rstrip("/") + f"/{start_time}"

    csv_file = f"{path}/{workspace_id}.csv"
    csv_columns = [
        "name",
        "uuid",
        "id",
        "sourceUuid",
        "sourceName",
        "schemaName",
        "tableName",
        "monitors",
    ]

    if not os.path.exists(path):
        os.makedirs(path)

    try:
        with open(csv_file, "w") as csvfile:
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

            metric_map[metric["metadata"]["uuid"]] = {
                "name": metric["metadata"]["name"],
                "uuid": metric["metadata"]["uuid"],
                "id": metric["metadata"]["idSerial"],
                "sourceUuid": metric["config"]["sources"][0],
                "sourceName": source_map.get(metric["config"]["sources"][0], ""),
                "schemaName": metric["config"]["table"].get("schemaName", ""),
                "tableName": metric["config"]["table"].get("tableName", ""),
                "monitors": [],
            }

        for monitor in monitors:
            metric_uuid = monitor["config"]["metrics"][0]

            if metric_map.get(metric_uuid) is None:
                # skip monitors for compare metrics
                continue

            metric_map[metric_uuid]["monitors"].append(
                {
                    "name": monitor["metadata"]["name"],
                    "uuid": monitor["metadata"]["uuid"],
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
