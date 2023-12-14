import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode

import pandas as pd
from collibra_api import CollibraAPI
from lightctl.lightup_client import LightupClient

logger = logging.getLogger(__name__)

INCIDENT_LOOKBACK_WINDOW = 60 * 60 * 24 * 7

COMMUNITY_ID = "00000000-0000-0000-0001-000100000001"
DOMAIN_TYPE_ID = "00000000-0000-0000-0000-000000030023"
ASSET_PARENT_ID = "00000000-0000-0000-0000-000000031203"
ASSET_MAIN_ID = "00000000-0000-0000-0000-000000031000"
STATUS_ID = "00000000-0000-0000-0000-000000005020"
TABLE_ASSET_TYPE_ID = "00000000-0000-0000-0000-000000031007"

# Lightup IDs on Collibra created by this script
LIGHTUP_DOMAIN_ID = "b4316413-0101-0101-0102-dab063b4c111"
ASSET_ID = "b4316413-0101-0101-0102-dab063b4c112"
RELATION_TYPE_ID = "b4316413-0101-0101-0102-dab063b4c114"
ASSIGMENT_ID = "3320dcb0-3b2d-4453-ade6-cde32045c618"


def _make_url(
    cluster_name: str,
    workspace_id: str,
    path: str,
    params: Optional[dict[str, str]] = None,
) -> str:
    cluster_name = cluster_name.removeprefix("https://").removesuffix("/")

    url = f"https://{cluster_name}/#/ws/{workspace_id}/{path}"
    if params is not None:
        url += "?" + urlencode(params)

    return url


def make_explorer_url(
    cluster_name: str,
    workspace_id: str,
    source_uuid: str,
    schema_uuid: str,
    table_uuid: str,
    column_uuid: Optional[str] = None,
) -> str:
    params = {
        "dataSourceUuid": source_uuid,
        "tableUuid": table_uuid,
        "schemaUuid": schema_uuid,
    }
    if column_uuid is not None:
        params["columnUuid"] = column_uuid
    params["tabKey"] = "autoMetrics"
    return _make_url(cluster_name, workspace_id, "profiler", params)


def make_metric_url(
    cluster_name: str,
    workspace_id: str,
    source_uuid: str,
    metric_uuid: str,
) -> str:
    params = {
        "dataSourceUuid": source_uuid,
        "metricUuid": metric_uuid,
    }
    return _make_url(cluster_name, workspace_id, "profiler", params)


class CollibraSync:
    def __init__(self, workspace_source_to_collibra_mapping: dict):
        self.collibra = CollibraAPI(log_level=logging.INFO)
        self.lightup = LightupClient()
        self.workspace_source_to_collibra_mapping = workspace_source_to_collibra_mapping
        self.url_base = self.lightup.healthz.url_base

    @staticmethod
    def get_lightup_attributes() -> dict:
        """
        Returns a list of dictionaries representing the Lightup attributes.

        Each dictionary contains the following keys:
        - id: The unique identifier of the attribute.
        - name: The name of the attribute.
        - description: The description of the attribute.
        - kind: The kind of the attribute (e.g., STRING, NUMERIC).
        - stringType: The string type of the attribute (e.g., PLAIN_TEXT, RICH_TEXT).
        - statisticsEnabled: Indicates whether statistics are enabled for the attribute (only applicable for NUMERIC attributes).
        - isInteger: Indicates whether the attribute is an integer (only applicable for NUMERIC attributes).
        """

        return [
            {
                "id": "b4316413-0101-0101-0101-dab063b4c100",
                "name": "Lightup Workspace Name",
                "description": "Lightup Attributes",
                "kind": "STRING",
                "stringType": "PLAIN_TEXT",
            },
            {
                "id": "b4316413-0101-0101-0101-dab063b4c101",
                "name": "Lightup Monitor Name",
                "description": "Lightup Attributes",
                "kind": "STRING",
                "stringType": "PLAIN_TEXT",
            },
            {
                "id": "b4316413-0101-0101-0101-dab063b4c102",
                "name": "Lightup Metric Name",
                "description": "Lightup Attributes",
                "kind": "STRING",
                "stringType": "PLAIN_TEXT",
            },
            {
                "id": "b4316413-0101-0101-0101-dab063b4c103",
                "name": "Lightup Incident Count",
                "description": "Lightup Attributes",
                "kind": "NUMERIC",
                "statisticsEnabled": "False",
                "isInteger": "True",
            },
            {
                "id": "b4316413-0101-0101-0101-dab063b4c104",
                "name": "Lightup Status",
                "description": "Lightup Attributes - Ongoing Incident Count",
                "kind": "STRING",
                "stringType": "RICH_TEXT",
            },
            {
                "id": "b4316413-0101-0101-0101-dab063b4c105",
                "name": "Lightup Metrics URL",
                "description": "Lightup Attributes",
                "kind": "STRING",
                "stringType": "RICH_TEXT",
            },
            {
                "id": "b4316413-0101-0101-0101-dab063b4c106",
                "name": "Lightup Database Name",
                "description": "Lightup Database Name",
                "kind": "STRING",
                "stringType": "PLAIN_TEXT",
            },
            {
                "id": "b4316413-0101-0101-0101-dab063b4c107",
                "name": "Lightup Schema Name",
                "description": "Lightup Schema Name",
                "kind": "STRING",
                "stringType": "PLAIN_TEXT",
            },
            {
                "id": "b4316413-0101-0101-0101-dab063b4c108",
                "name": "Lightup Table Name",
                "description": "Lightup Table Name",
                "kind": "STRING",
                "stringType": "PLAIN_TEXT",
            },
            {
                "id": "b4316413-0101-0101-0101-dab063b4c109",
                "name": "Lightup Field Name",
                "description": "Lightup Field Name",
                "kind": "STRING",
                "stringType": "PLAIN_TEXT",
            },
        ]

    def clear_collibra(self):
        """
        This function deletes all Lightup related objects from within Collibra.
        """
        self.collibra.delete(f"assignments/{ASSIGMENT_ID}")
        self.collibra.delete(f"relationTypes/{RELATION_TYPE_ID}")
        self.collibra.delete(f"assetTypes/{ASSET_ID}")
        self.collibra.delete(f"domains/{LIGHTUP_DOMAIN_ID}")

        # Delete all attributes created by this script
        for attribute in self.get_lightup_attributes():
            self.collibra.delete(f"attributeTypes/{attribute['id']}")

        logger.info("Deleted all Lightup Collibra objects")

    def prepare_collibra(self):
        """
        This function prepares collibra for Lightup integration by creating the
        required attributes/objects/assets
        """

        for attribute in self.get_lightup_attributes():
            # get attribute by id to check if exist or not
            get_attribute = self.collibra.get(f"attributeTypes/{attribute['id']}")
            if get_attribute == None:
                # create attribute if not exist on collibra
                create = self.collibra.post(f"attributeTypes", data=attribute)
                logger.info(f"attribute: {create}")
            else:
                # update attribute if exist on collibra
                self.collibra.patch(f"attributeTypes/{attribute['id']}", data=attribute)
                logger.info(f"Update Attribute ID: {attribute['id']} Updated")

        # Create Asset Type
        data = {
            "id": ASSET_ID,
            "name": "Lightup Incident",
            "description": "Example of Asset Type creation from the REST API documentation",
            "parentId": ASSET_PARENT_ID,
            "symbolType": "NONE",
            "color": "#9FD193",
            "displayNameEnabled": "False",
            "ratingEnabled": "False",
        }

        # Create Asset Type on Collibra
        self.collibra.post(f"assetTypes", data=data)

        # Create Domain
        data = {
            "id": LIGHTUP_DOMAIN_ID,
            "name": "Lightup Incident Domain",
            "communityId": COMMUNITY_ID,
            "typeId": DOMAIN_TYPE_ID,
            "description": "Lightup Incident Domain",
            "excludedFromAutoHyperlinking": "True",
        }

        self.collibra.post(f"domains", data=data)

        # Create Relation Type
        data = {
            "id": RELATION_TYPE_ID,
            "sourceTypeId": ASSET_ID,
            "role": "has",
            "targetTypeId": ASSET_MAIN_ID,
            "coRole": "metrics in",
            "description": "Asset Has Metrics In",
        }

        self.collibra.post(f"relationTypes", data=data)

        data = {
            "id": ASSIGMENT_ID,
            "assetTypeId": ASSET_ID,
            "statusIds": [STATUS_ID],
            "characteristicTypes": [
                {
                    "id": "b4316413-0101-0101-0101-dab063b4c100",
                    "min": 0,
                    "max": 250,
                    "type": "StringAttributeType",
                },
                {
                    "id": "b4316413-0101-0101-0101-dab063b4c101",
                    "min": 0,
                    "max": 250,
                    "type": "StringAttributeType",
                },
                {
                    "id": "b4316413-0101-0101-0101-dab063b4c102",
                    "min": 0,
                    "max": 250,
                    "type": "StringAttributeType",
                },
                {
                    "id": "b4316413-0101-0101-0101-dab063b4c103",
                    "min": 0,
                    "max": 250,
                    "type": "StringAttributeType",
                },
                {
                    "id": "b4316413-0101-0101-0101-dab063b4c104",
                    "min": 0,
                    "max": 250,
                    "type": "StringAttributeType",
                },
                {
                    "id": "b4316413-0101-0101-0101-dab063b4c105",
                    "min": 0,
                    "max": 250,
                    "type": "StringAttributeType",
                },
                {
                    "id": "b4316413-0101-0101-0101-dab063b4c106",
                    "min": 0,
                    "max": 250,
                    "type": "StringAttributeType",
                },
                {
                    "id": "b4316413-0101-0101-0101-dab063b4c107",
                    "min": 0,
                    "max": 250,
                    "type": "StringAttributeType",
                },
                {
                    "id": "b4316413-0101-0101-0101-dab063b4c108",
                    "min": 0,
                    "max": 250,
                    "type": "StringAttributeType",
                },
                {
                    "id": "b4316413-0101-0101-0101-dab063b4c109",
                    "min": 0,
                    "max": 250,
                    "type": "StringAttributeType",
                },
                {
                    "id": "00000000-0000-0000-0000-000000007018",
                    "min": 0,
                    "max": 250,
                    "type": "RelationType",
                    "relationTypeDirection": "TO_SOURCE",
                    "relationTypeRestriction": "00000000-0000-0000-0000-000000031000",
                },
                {
                    "id": RELATION_TYPE_ID,
                    "min": 0,
                    "max": 250,
                    "type": "RelationType",
                    "relationTypeDirection": "TO_SOURCE",
                    "relationTypeRestriction": ASSET_ID,
                },
            ],
            "domainTypeIds": [DOMAIN_TYPE_ID],
            "defaultStatusId": STATUS_ID,
        }

        self.collibra.post(f"assignments", data=data)
        logger.info("Created all Lightup Collibra objects")

    def get_asset_details(
        self, source: dict, metric: dict
    ) -> tuple[str, str, str, str, str, str]:
        # return source_name, db_name, schema_name, table_name, column_name

        source_name = source["metadata"]["name"]

        db_name = source["config"]["connection"].get("dbname") or source["config"][
            "connection"
        ].get("catalog")

        table = metric["config"]["table"]

        schema_name = table.get("schemaName")
        schema_uuid = table.get("schemaUuid")
        table_name = table.get("tableName")
        table_uuid = table.get("tableUuid")

        column_name = column_uuid = None

        if table["type"] == "customSql":
            column_name = table.get("columnName")
            column_uuid = table.get("columnUuid")

        if table["type"] == "table":
            columns = metric["config"].get("valueColumns")
            if columns:
                column_name = columns[0].get("columnName")
                column_uuid = columns[0].get("columnUuid")

        return {
            "sourceName": source_name,
            "dbName": db_name,
            "schemaName": schema_name,
            "tableName": table_name,
            "columnName": column_name,
            "schemaUuid": schema_uuid,
            "tableUuid": table_uuid,
            "columnUuid": column_uuid,
        }

    def get_object_key(
        self, collibra_datasource_id, dbname, schema_name, table_name, column_name
    ):
        # unique cross workspace representation of a collibra data object
        if dbname:
            return tuple(
                f"{collibra_datasource_id}.{dbname}.{schema_name}.{table_name}.{column_name}".split(
                    "."
                )
            )
        return tuple(
            f"{collibra_datasource_id}.{schema_name}.{table_name}.{column_name}".split(
                "."
            )
        )

    def get_metric_info_map(self, sources, metrics):
        source_map = {source["metadata"]["uuid"]: source for source in sources}
        metric_info_map = {}

        for metric in metrics:
            if metric["config"]["configType"] not in [
                "metricConfig",
                "fullTableMetricConfig",
            ]:
                continue

            metric_uuid = metric["metadata"]["uuid"]
            source_uuid = metric["config"]["sources"][0]
            source = source_map.get(source_uuid)
            if source is None:
                continue

            asset_details = self.get_asset_details(source, metric)

            metric_info_map[metric_uuid] = {
                "metricName": metric["metadata"]["name"],
                "isAutoMetric": metric["metadata"]["creationType"] == "auto",
                "sourceUuid": source_uuid,
                **asset_details,
            }
        return metric_info_map

    def get_monitor_info_map(
        self, monitors, metric_info_map, workspace, collibra_source_id
    ):
        # get all workspaces
        workspaces = self.lightup.workspace.list_workspaces()

        # for each workspace, get the workspace id and name to match with the workspace id from the mapping
        for ws in workspaces:
            if ws["uuid"] == str(workspace):
                workspace_id = ws["uuid"]
                workspace_name = ws["name"]
                break
            else:
                workspace_id = None

        # if workspace id is not found, raise exception
        if not workspace_id:
            raise Exception(f"Workspace {workspace} not found")

        monitor_info_map = {}

        for monitor in monitors:
            metric_uuid = monitor["config"]["metrics"][0]
            if metric_uuid not in metric_info_map:
                continue

            metric_info = metric_info_map.get(metric_uuid)
            if metric_info is None:
                continue

            object_key = self.get_object_key(
                collibra_source_id,
                metric_info["dbName"],
                metric_info["schemaName"],
                metric_info["tableName"],
                metric_info["columnName"],
            )

            monitor_uuid = monitor["metadata"]["uuid"]
            monitor_info_map[monitor_uuid] = {
                "monitorName": monitor["metadata"]["name"],
                "monitorUuid": monitor_uuid,
                "workspaceId": workspace_id,
                "workspaceName": workspace_name,
                "metricUuid": metric_uuid,
                "metric": metric_info,
                "objectKey": object_key,
                "incidentCount": 0,
                "ongoingIncidentCount": 0,
            }
        return monitor_info_map

    def get_url(
        self,
        is_auto_metric: bool,
        workspace_id: str,
        source_uuid: str,
        metric_uuid: Optional[str],
        schema_uuid: Optional[str],
        table_uuid: Optional[str],
        column_uuid: Optional[str],
    ) -> str:
        if not is_auto_metric:
            return make_metric_url(
                self.url_base, workspace_id, source_uuid, metric_uuid
            )

        return make_explorer_url(
            self.url_base,
            workspace_id,
            source_uuid,
            schema_uuid,
            table_uuid,
            column_uuid,
        )

    def get_object_key_to_table_info_map(self, monitor_info_map):
        key_to_table_info_map = {}
        for monitor_info in monitor_info_map.values():
            object_key = monitor_info["objectKey"]
            metric_info = monitor_info["metric"]

            table_info = key_to_table_info_map.get(object_key, [])

            table_info.append(
                {
                    "workspaceId": monitor_info["workspaceId"],
                    "workspaceName": monitor_info["workspaceName"],
                    "monitorName": monitor_info["monitorName"],
                    "monitorUuid": monitor_info["monitorUuid"],
                    "metricName": metric_info["metricName"],
                    "incidentCount": monitor_info["incidentCount"],
                    "ongoingIncidentCount": monitor_info["ongoingIncidentCount"],
                    "url": self.get_url(
                        metric_info["isAutoMetric"],
                        monitor_info["workspaceId"],
                        metric_info["sourceUuid"],
                        monitor_info["metricUuid"],
                        metric_info["schemaUuid"],
                        metric_info["tableUuid"],
                        metric_info["columnUuid"],
                    ),
                }
            )

            key_to_table_info_map[object_key] = table_info

        return key_to_table_info_map

    def get_lightup_state(
        self, lightup_workspace_id, lightup_source_id, collibra_source_id
    ):
        """
        Retrieves the lightup state by performing the following steps:
        1. Get the list of workspaces.
        2. For each workspace, retrieve the list of datasources with Collibra configuration.
        3. Get information about monitors and metrics configured on the datasources.
        4. Retrieve incidents associated with the monitors.
        5. Assemble information per table.
        """
        lookback_end_ts = int(datetime.utcnow().timestamp())
        lookback_start_ts = lookback_end_ts - INCIDENT_LOOKBACK_WINDOW
        object_key_to_table_info_map = {}

        # 2. For all monitors configured on the list of sources, get info about
        # the monitor as well as the underlying metric

        sources = self.lightup.source.list_sources(lightup_workspace_id)
        metrics = self.lightup.metric.list_metrics(lightup_workspace_id)
        monitors = self.lightup.monitor.list_monitors(lightup_workspace_id)

        sources = [
            source
            for source in sources
            if source["metadata"]["uuid"] in lightup_source_id
        ]

        metrics = [
            metric
            for metric in metrics
            if metric["config"]["sources"][0] in lightup_source_id
        ]

        metric_info_map = self.get_metric_info_map(sources, metrics)
        monitor_info_map = self.get_monitor_info_map(
            monitors, metric_info_map, lightup_workspace_id, collibra_source_id
        )

        # 3. Get incidents associated with the list of monitors.
        incidents = self.lightup.incident.list_incidents(
            lightup_workspace_id, start_ts=lookback_start_ts, end_ts=lookback_end_ts
        )
        for incident in incidents:
            monitor_uuid = incident.get("filter_uuid")
            if monitor_info := monitor_info_map.get(monitor_uuid):
                monitor_info["incidentCount"] += 1
                monitor_info["ongoingIncidentCount"] += bool(incident["ongoing"])

        # 4. Assemble information per table (accrue information in case the same table
        # is configured across multiple workspaces)
        object_key_to_table_info_map = self.get_object_key_to_table_info_map(
            monitor_info_map
        )

        return object_key_to_table_info_map

    def update_collibra_attributes(self, assetId, typeId, value):
        # Attributes for Asset Type Lightup Incident on Collibra
        payload = {"assetId": assetId, "typeId": typeId, "value": value}
        self.collibra.post(f"attributes", data=payload)

    def update_collibra(self, object_key_to_table_info_map):
        asset_ids = []

        for key, value_list in object_key_to_table_info_map.items():
            for value in value_list:
                domainId = LIGHTUP_DOMAIN_ID
                monitorUuid = value["monitorUuid"]
                typeId = ASSET_ID

                payload = {
                    "name": monitorUuid,
                    "id": monitorUuid,
                    "displayName": monitorUuid,
                    "domainId": domainId,
                    "typeId": typeId,
                }

                try:
                    asset = self.collibra.post(f"assets", data=payload)
                    get_asset_id = asset["id"]
                except:
                    continue

                if value["incidentCount"] > 0:
                    if value["ongoingIncidentCount"] == 0:
                        status = '<div style="background-color: orange; width: 100.0px; padding: 3.0px; text-align: center; color: white; font-weight: bold;">Warning</div>'
                    else:
                        status = '<div style="background-color: red; width: 100.0px; padding: 3.0px; text-align: center; color: white; font-weight: bold;">Issue</div>'
                else:
                    status = '<div style="background-color: green; width: 100.0px; padding: 3.0px; text-align: center; color: white; font-weight: bold;">Healthy</div>'

                url = value["url"]
                self.update_collibra_attributes(
                    get_asset_id,
                    "b4316413-0101-0101-0101-dab063b4c100",
                    value["workspaceName"],
                )
                self.update_collibra_attributes(
                    get_asset_id,
                    "b4316413-0101-0101-0101-dab063b4c101",
                    value["monitorName"],
                )
                self.update_collibra_attributes(
                    get_asset_id,
                    "b4316413-0101-0101-0101-dab063b4c102",
                    value["metricName"],
                )
                self.update_collibra_attributes(
                    get_asset_id,
                    "b4316413-0101-0101-0101-dab063b4c103",
                    value["incidentCount"],
                )
                self.update_collibra_attributes(
                    get_asset_id, "b4316413-0101-0101-0101-dab063b4c104", status
                )
                self.update_collibra_attributes(
                    get_asset_id,
                    "b4316413-0101-0101-0101-dab063b4c105",
                    f'<a href="{url}" target="_blank">View</a>',
                )
                self.update_collibra_attributes(
                    get_asset_id, "b4316413-0101-0101-0101-dab063b4c106", key[1]
                )
                self.update_collibra_attributes(
                    get_asset_id, "b4316413-0101-0101-0101-dab063b4c107", key[2]
                )
                self.update_collibra_attributes(
                    get_asset_id, "b4316413-0101-0101-0101-dab063b4c108", key[3]
                )
                self.update_collibra_attributes(
                    get_asset_id, "b4316413-0101-0101-0101-dab063b4c109", key[4]
                )

                asset_ids.append(
                    {
                        "collibra_asset_id": get_asset_id,
                        "database_name": key[1],
                        "schema_name": key[2],
                        "table_name": key[3],
                        "column_name": key[4],
                    }
                )

        return asset_ids

    def collibra_tables(self, source_data, target_data):
        # Define the source data
        source_data = source_data

        # Define the target data
        target_data = target_data

        # Convert the data to DataFrames read as dictionaries
        source_df = pd.DataFrame(source_data)
        target_df = pd.DataFrame(target_data)

        # Merge the DataFrames
        source_df.merge(target_df, on=["table_name", "schema_name"], how="inner")

        # Create an empty list to store the results
        results = []

        # Loop through the target data
        for index, row in target_df.iterrows():
            # Get the table_id and table_name
            table_id = row["table_id"]
            table_name = row["table_name"]
            schema_name = row["schema_name"]

            # Filter the source data for matching tables
            filtered_source_df = source_df[
                (source_df["table_name"] == table_name)
                & (source_df["schema_name"] == schema_name)
            ]

            # Convert the filtered source data to a dictionary
            sources = filtered_source_df.to_dict(orient="records")

            # Append the result to the results list
            results.append(
                {
                    "colibra_table_id": table_id,
                    "colibra_table_name": table_name,
                    "colibra_schema_name": schema_name,
                    "metrics_sources": sources,
                }
            )

        return results

    def run(self):
        # for collibra source id in the mapping run the sync for each source id
        for cs in self.workspace_source_to_collibra_mapping["collibra_sources"]:
            # for each sync, get the lightup source id and collibra source id to match with the source id from the mapping and run the sync
            collibra_source = cs["collibra_source_id"]

            # get all assets id created by update_collibra function
            metrics_list = []

            response = self.collibra.get(
                f"assets?typeId={TABLE_ASSET_TYPE_ID}&domainId={collibra_source}"
            )

            collibra_tables_list = []

            for a in response["results"]:
                # breadcrumb = self.collibra.get(f"assets/{a['id']}/breadcrumb")
                # breadcrumb = ' > '.join(d['name'] for d in breadcrumb)
                # print(f"{breadcrumb} > {a['name']}")
                # print(a['id'], a['name'], a['domain']['id'], a['domain']['name'].lower())

                # get all assets with the same target id and relation type id
                response = self.collibra.get(
                    f"relations?targetId={a['id']}&relationTypeId={RELATION_TYPE_ID}"
                )

                if response and response["total"] > 0:
                    # delete all assets with the same target id and relation type id
                    for r in response["results"]:
                        self.collibra.delete(f"assets/{r['source']['id']}")
                else:
                    print("No assets found")

                collibra_tables_list.append(
                    {
                        "table_id": a["id"],
                        "table_name": a["name"],
                        "schema_id": a["domain"]["id"],
                        "schema_name": a["domain"]["name"].lower(),
                    }
                )

            for ls in cs["lightup_sources"]:
                workspace_id = ls["workspace_id"]
                lightup_source_id = ls["lightup_source_id"]
                object_key_to_table_info_map = self.get_lightup_state(
                    workspace_id, lightup_source_id, collibra_source
                )
                collibra_ids = self.update_collibra(object_key_to_table_info_map)

                # merge all assets id created by update_collibra function
                metrics_list.extend(collibra_ids)

            tables = self.collibra_tables(metrics_list, collibra_tables_list)

            for table in tables:
                ids = []

                colibra_table_id = table["colibra_table_id"]
                metrics_sources = table["metrics_sources"]
                for metric in metrics_sources:
                    collibra_asset_id = metric["collibra_asset_id"]
                    ids.append(collibra_asset_id)

                # create relation between collibra source and all assets id created by update_collibra function
                payload = {
                    "typeId": RELATION_TYPE_ID,
                    "relatedAssetIds": ids,
                    "relationDirection": "TO_SOURCE",
                }

                # update relation between collibra source and all assets id created by update_collibra function
                self.collibra.put(f"assets/{colibra_table_id}/relations", data=payload)
                logger.info("Updated all Lightup Collibra objects")
