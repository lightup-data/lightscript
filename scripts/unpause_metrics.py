from lightctl.client.metric_client import MetricClient
from lightctl.client.workspace_client import WorkspaceClient

wc = WorkspaceClient()
mc = MetricClient()

workspaces = wc.list_workspaces()

for workspace in workspaces:
    workspace_id = workspace["uuid"]

    paused_metrics = []
    metrics = mc.list_metrics(workspace_id)
    for metric in metrics:
        if not metric["config"]["isLive"]:
            paused_metrics.append(metric)

    if len(paused_metrics) == 0:
        continue

    print(
        f"Number of paused metrics in workspace {workspace['name']}: {len(paused_metrics)}"
    )
    user_input = input(
        "press 1 to unpause all, 2 to pick which metrics to unpause, any other key to skip: "
    )
    if user_input in ["1", "2"]:
        for metric in paused_metrics:
            unpause = "yes"
            if user_input == "2":
                unpause = input(
                    f'unpause {metric["metadata"]["name"]} {metric["metadata"]["uuid"]} - only yes will unpause: '
                )
            if unpause.lower() == "yes":
                metric["config"]["isLive"] = True
                mc.update_metric(workspace_id, metric["metadata"]["uuid"], metric)
