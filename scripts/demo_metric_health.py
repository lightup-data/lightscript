import arrow
from lightctl.client.incident_client import IncidentClient
from lightctl.client.metric_client import MetricClient
from lightctl.client.monitor_client import MonitorClient


class bcolors:
    OK = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def get_metric_tags(metric):
    tags = metric["metadata"].get("tags")
    if not tags:
        return ""
    schema = ""
    table = ""
    column = ""
    ret = ""
    for tag in tags:
        if tag["key"] == "lightup/schemaName":
            schema = tag["value"]
        if tag["key"] == "lightup/tableName":
            table = tag["value"]
        if tag["key"] == "lightup/columnName":
            column = tag["value"]

    if table:
        ret = f"table: {schema}.{table}"
    if column:
        ret = f"column: {column} in {ret}"

    if ret:
        return f"({ret})"

    return ret


mc = MetricClient()
rc = MonitorClient()
ic = IncidentClient()

# List of metrics to monitor
metric_list = mc.list_metrics()

# ---------------------------- USER INPUT ------------------------------------
# check health of all monitored metrics between start time and end time
start_time = "2021-05-12T21:34:40+00:00"
end_time = "2021-05-13T21:34:40+00:00"
# ----------------------------------------------------------------------------

start_ts = arrow.get(start_time).timestamp()
end_ts = arrow.get(end_time).timestamp()

for metric in metric_list:
    rules = rc.get_rules_by_metric(metric["metadata"]["uuid"])
    if rules is None:
        continue

    live_rules = [rule for rule in rules if rule["config"]["isLive"]]

    if len(live_rules) == 0:
        continue

    print()
    tags = get_metric_tags(metric)
    print(f'{bcolors.BOLD}Metric:{bcolors.ENDC} {metric["metadata"]["name"]} {tags}')

    for rule in live_rules:
        last_processed_timestamp = rule["status"].get("lastSampleTs", 0)

        rulename = rule["metadata"]["name"]

        if last_processed_timestamp < start_ts:
            print(f"- Rule {rulename} hasn't started processing the time range")
            continue

        if last_processed_timestamp <= end_ts:
            print(
                f"- Rule {rulename} only processed upto {arrow.get(last_processed_timestamp).format()}"
            )
        else:
            print(f"- Rule has processed the time range {start_time} to {end_time}")

        incidents = ic.get_incidents(rule["metadata"]["uuid"], start_ts, end_ts)
        if incidents is None:
            print(f"  * Error - unable to get response from api")
            continue

        if len(incidents) > 0:
            print(
                f"  * {bcolors.FAIL}UNHEALTHY{bcolors.ENDC} - {len(incidents)} incidents detected."
            )
        else:
            print(f"  - {bcolors.OK}HEALTHY{bcolors.ENDC}")
print()
