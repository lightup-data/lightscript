#!/usr/bin/env python3
"""
This script updates metric dimensions using a metric's tags.
"""
import logging
from typing import Dict

from lightctl.client.metric_client import MetricClient
from lightctl.client.workspace_client import WorkspaceClient

# comparison is done after tag is made lower case.
# update this map with configured tag to dimension equivalent
TAGS_TO_DIMENSION_MAP = {
    "custom": "custom",
    "dimension:custom": "custom",
    "completeness": "completeness",
    "dimension:completeness": "completeness",
    "accuracy": "accuracy",
    "dimension:accuracy": "accuracy",
    "timeliness": "timeliness",
    "dimension:timeliness": "timeliness",
}

# set to False to execute update, if True, the program will simply output what
# changes it is going to make.
DRY_RUN = True

logger = logging.getLogger(__name__)

wc = WorkspaceClient()
mc = MetricClient()


def update_dimension_from_tag(workspace_id: str, metric: Dict) -> bool:
    """
    updates metric dimension based on tag. the tag will not be deleted.
    """
    tags = metric["metadata"].get("tags")
    if not tags:
        return

    for tag in tags:
        dimension = TAGS_TO_DIMENSION_MAP.get(tag.lower())
        if dimension is None or metric["config"]["dimension"] == dimension:
            continue

        output_str = f"{metric['metadata']['name']} --> {dimension}"

        if DRY_RUN:
            print(output_str)
            return False
        else:
            logger.info(output_str)
            metric["config"]["dimension"] = dimension
            mc.update_metric(workspace_id, metric["metadata"]["uuid"], metric)
            return True  # updated

    return False  # not updated


def main():
    assert set(TAGS_TO_DIMENSION_MAP.values()) == {
        "accuracy",
        "completeness",
        "timeliness",
        "custom",
    }

    update = input("update dimensions using map (only 'yes' will update): ")
    if update.lower() != "yes":
        print("exiting .. ")
        exit(0)

    print("updating metric dimension from tags using tag map")
    workspaces = wc.list_workspaces()
    for workspace in workspaces:
        workspace_id = workspace["uuid"]

        metrics = mc.list_metrics(workspace_id)
        for metric in metrics:
            update_dimension_from_tag(workspace_id, metric)


if __name__ == "__main__":
    main()
