#!/usr/bin/env python3

"""
Export metric datapoints on a per workspace basis along with associated metadata
including monitor runs if available. Depends on lightctl, if the dependencies
for this repo are met, this script will run.

Export metrics for each workspace in the following path:
path/<export_epoch_time>/<workspace_uuid>_datapoints.csv

"""
import argparse
import csv
import os
import time
from collections import defaultdict
from copy import deepcopy
from math import isnan

import arrow
from lightctl.client.datapoint_client import DatapointClient
from lightctl.client.incident_client import IncidentClient
from lightctl.client.metric_client import MetricClient
from lightctl.client.monitor_client import MonitorClient
from lightctl.client.source_client import SourceClient
from lightctl.client.workspace_client import WorkspaceClient

workspace_client = WorkspaceClient()
source_client = SourceClient()
metric_client = MetricClient()
datapoint_client = DatapointClient()
monitor_client = MonitorClient()
incident_client = IncidentClient()

# update to appropriate path
EXPORT_DIRECTORY_PATH = "/tmp/lightupexport/"

DEBUG = False


def dprint(*args):
    if DEBUG:
        print(*args)


def export_to_csv(workspace_id, datapoints, start_time):
    start_ts = time.time()

    path = EXPORT_DIRECTORY_PATH.rstrip("/") + f"/{start_time}"

    csv_file = f"{path}/{workspace_id}_datapoints.csv"
    csv_columns = [
        "workspaceUuid",
        "metricUuid",
        "eventTs",
        "slice",
        "value",
        "recordedTs",
        "metricId",
        "metricName",
        "metricDimension",
        "sourceUuid",
        "sourceName",
        "schemaName",
        "tableName",
        "columnName",
        "monitorUuid",
        "monitorName",
        "monitoredValue",
        "monitorLowerBound",
        "monitorUpperBound",
        "incidentExists",
    ]

    if not os.path.exists(path):
        os.makedirs(path)

    try:
        with open(csv_file, "w", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in datapoints:
                writer.writerow(data)
    except OSError:
        print("I/O error {ex}")
        raise

    dprint(f"write to {csv_file} completed in {time.time() - start_ts} seconds")


def get_event_ts_interval(datapoints: list):
    cur_min = float("inf")
    cur_max = -float("inf")
    for dp in datapoints:
        cur_min = min(cur_min, dp["eventTs"])
        cur_max = max(cur_max, dp["eventTs"])
    return cur_min, cur_max


def add_incident_data(dp: dict, incidents: list, monitor_uuid: str) -> dict:
    dp["incidentExists"] = False
    for incident in incidents:
        if incident.get("filter_uuid") != monitor_uuid:
            continue
        if incident.get("slice") != dp["slice"]:
            continue
        if incident["start_ts"] <= dp["eventTs"] <= incident["end_ts"]:
            dp["incidentExists"] = True
    return dp


def join_datapoint_with_filter_stats(
    dp: dict, filter_stats: list, incidents: list, precision: float = 0.001
) -> dict:
    for stat in filter_stats:
        if stat["slice"] != dp["slice"]:
            continue

        if abs(dp["eventTs"] - stat["time"]) < precision:
            dp["monitoredValue"] = stat["filtered_obs_val"]
            dp["monitorLowerBound"] = stat["lower_exp_limit"]
            dp["monitorUpperBound"] = stat["upper_exp_limit"]
            dp = add_incident_data(dp, incidents, stat["filter_uuid"])
    return dp


def get_workspace_datapoints(ws: dict, start_ts: float, end_ts: float) -> list[dict]:
    workspace_datapoints = []

    workspace_id = ws["uuid"]

    metrics = metric_client.list_metrics(workspace_id)
    monitors = monitor_client.list_monitors(workspace_id)
    sources = source_client.list_sources(workspace_id)

    dprint(f"- {len(sources)=}, {len(metrics)=}, {len(monitors)=}")

    source_map = {
        source["metadata"]["uuid"]: source["metadata"]["name"] for source in sources
    }

    monitor_map = {}
    metric_to_monitor_map = defaultdict(list)
    for monitor in monitors:
        metric_uuid = monitor["config"]["metrics"][0]
        metric_to_monitor_map[metric_uuid].append(monitor)
        monitor_map[monitor["metadata"]["uuid"]] = monitor

    for metric in metrics:
        # skip compare metrics
        if metric["config"]["configType"] not in [
            "metricConfig",
            "fullTableMetricConfig",
        ]:
            continue

        metric_uuid = metric["metadata"]["uuid"]
        datapoints = datapoint_client.get_metric_datapoints(
            workspace_id, metric_uuid, start_ts, end_ts
        )

        if not datapoints:
            continue

        dprint(f"- processing metric {metric['metadata']['name']} - {len(datapoints)=}")

        monitor_datapoints_map = {}
        monitor_incidents_map = {}

        for monitor in metric_to_monitor_map.get(metric_uuid, []):
            monitor_uuid = monitor["metadata"]["uuid"]
            monitor_datapoints = datapoint_client.get_monitor_datapoints(
                workspace_id, monitor_uuid, start_ts, end_ts
            )
            monitor_datapoints_map[monitor_uuid] = []
            if monitor_datapoints:
                monitor_datapoints_map[monitor_uuid] = monitor_datapoints

            monitor_incidents_map[monitor_uuid] = []
            monitor_incidents = incident_client.list_incidents(
                workspace_id, start_ts, end_ts, monitor_id=monitor_uuid
            )
            if monitor_incidents:
                monitor_incidents_map[monitor_uuid] = monitor_incidents

        # annotate datapoint
        for dp in datapoints:
            if dp.get("value") is not None and isnan(dp["value"]):
                dp["value"] = None
            dp["workspaceUuid"] = workspace_id
            dp["metricName"] = metric["metadata"]["name"]
            dp["metricId"] = metric["metadata"]["idSerial"]
            dp["metricDimension"] = metric["config"]["dimension"]
            dp["sourceUuid"] = metric["config"]["sources"][0]
            dp["sourceName"] = source_map.get(dp["sourceUuid"], "")
            dp["schemaName"] = metric["config"].get("table", {}).get("schemaName")
            dp["tableName"] = metric["config"].get("table", {}).get("tableName")
            if columns := metric["config"].get("valueColumns"):
                dp["columnName"] = columns[0]["columnName"]
            else:
                dp["columnName"] = ""

            if metric_monitors := metric_to_monitor_map.get(metric_uuid):
                # for each monitor, add a duplicate row if the monitor has processed the datapoint
                for monitor in metric_monitors:
                    monitor_dp = deepcopy(dp)
                    monitor_dp["monitorUuid"] = monitor_uuid = monitor["metadata"][
                        "uuid"
                    ]
                    monitor_dp["monitorName"] = monitor["metadata"]["name"]
                    join_datapoint_with_filter_stats(
                        monitor_dp,
                        monitor_datapoints_map[monitor_uuid],
                        monitor_incidents_map[monitor_uuid],
                    )
                    workspace_datapoints.append(monitor_dp)
            else:
                # append datapoint even if there are no monitors
                workspace_datapoints.append(dp)

    return workspace_datapoints


def main(num_days: int = 1):
    main_start_ts = time.time()

    end_ts = arrow.utcnow().floor("day")
    start_ts = end_ts.shift(days=-num_days).timestamp()
    end_ts = end_ts.timestamp()

    dprint(
        f"start_ts={arrow.get(start_ts).format()}, end_ts={arrow.get(end_ts).format()}"
    )

    workspaces = workspace_client.list_workspaces()
    dprint(f"processing {len(workspaces)=}")

    export_time = int(time.time())

    for ws in workspaces:
        ws_start_ts = time.time()

        workspace_id = ws["uuid"]
        dprint()
        dprint(f"processing workspace {ws['name']}")
        datapoints = get_workspace_datapoints(ws, start_ts, end_ts)
        export_to_csv(workspace_id, datapoints, int(export_time))
        dprint(
            f"metric datapoints export for workspace '{ws['name']}' completed in "
            f"{time.time()-ws_start_ts} seconds."
        )

    print(f"export completed in {time.time() - main_start_ts} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export datapoints for all workspaces for the last n days"
    )
    parser.add_argument(
        "--days", type=int, help="Number of days to lookback", required=True
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--path", type=str, help="Path to store the csv files")

    args = parser.parse_args()

    DEBUG = args.debug or DEBUG
    EXPORT_DIRECTORY_PATH = args.path or EXPORT_DIRECTORY_PATH

    print(
        f"exporting datapoints for the last {args.days} days to "
        f"path={EXPORT_DIRECTORY_PATH}. debug={DEBUG} "
    )

    main(args.days)
