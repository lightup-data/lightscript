import json
from pathlib import Path

from lightctl.client.metric_client import MetricClient
from lightctl.client.monitor_client import MonitorClient
from lightctl.util import LightupException

WORKSPACE_ID = "497d2c3e-2e24-47ec-b33a-dcf3999062a7"
DRY_RUN = True

monitor_client = MonitorClient()
metric_client = MetricClient()


def monitor_str(monitor):
    return f'{monitor["metadata"]["name"]} ({monitor["metadata"]["uuid"]})'


def download_monitor_configurations(dir=None):
    if dir is None:
        dir = Path.home()
    else:
        dir = Path(dir)
        assert dir.is_dir() and dir.exists()

    filename = dir / f"monitor_backup_{WORKSPACE_ID}.json"

    monitors = monitor_client.list_monitors(WORKSPACE_ID)
    json.dump(monitors, open(filename, mode="w"))
    print(f"Dumped all monitor configuration to {filename}")


def replay_monitor_configurations(dir=None):
    if dir is None:
        dir = Path.home()
    else:
        dir = Path(dir)
        assert dir.is_dir() and dir.exists()

    filename = dir / f"monitor_backup_{WORKSPACE_ID}.json"

    print(f"Reading all monitor configurations from {filename}")

    monitors = json.load(open(filename, mode="r"))

    for monitor in monitors:
        assert WORKSPACE_ID == monitor["metadata"]["workspaceId"]
        mon_uuid = monitor["metadata"]["uuid"]
        metric_uuid = monitor["config"]["metrics"][0]

        try:
            metric_client.get_metric(WORKSPACE_ID, metric_uuid)
        except LightupException:
            # metric doesn't exist - nothing to do here
            print(
                f"Error: metric associated with {monitor_str(monitor)} no longer exists"
            )
            continue

        try:
            monitor_client.get_monitor(WORKSPACE_ID, mon_uuid)
            print(f"{monitor_str(monitor)} does not need to be recreated")
        except LightupException:
            # monitor doesn't exist
            # create monitor
            if DRY_RUN:
                print(f"!!! {monitor_str(monitor)} will be recreated")
            else:
                print(f"RECREATING {monitor_str(monitor)}")
                monitor["metadata"].pop("uuid", None)
                monitor_client.create_monitor(WORKSPACE_ID, monitor)


print(f"WorkspaceID: {WORKSPACE_ID} DRY RUN: {DRY_RUN}")


if (
    input(
        "download monitor configurations? [Only 'yes' will download/overwrite]: "
    ).lower()
    == "yes"
):
    download_monitor_configurations()

if input("replay monitor configurations? [Only 'yes' will replay]: ").lower() == "yes":
    replay_monitor_configurations()
