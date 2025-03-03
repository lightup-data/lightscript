import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Optional, overload
from urllib.parse import urlencode
from uuid import UUID

from lightctl.client.base_client import BaseClient
from lightctl.client.metric_client import MetricClient
from lightctl.client.monitor_client import MonitorClient
from lightctl.client.profiler_client import ProfilerClient
from lightctl.client.source_client import SourceClient
from lightctl.client.workspace_client import WorkspaceClient
from lightctl.util import LightupException
from models import EntityType

CLIENT_TYPE_MAP: dict[EntityType, type[BaseClient]] = {
    EntityType.METRICS: MetricClient,
    EntityType.MONITORS: MonitorClient,
    EntityType.WORKSPACES: WorkspaceClient,
    EntityType.SOURCES: SourceClient,
    EntityType.PROFILES: ProfilerClient,
}


def credential_wrapper_factory(
    client_class: type[BaseClient], credentials_file: Path
) -> type[BaseClient]:
    class ClientWithCredentials(client_class):
        def __init__(self):
            with open(credentials_file) as f:
                self.credential = json.load(f)
                self.refresh_token = self.credential["data"]["refresh"]
                self.url_base = self.credential["data"]["server"]

            self.access_token: Optional[str] = self._refresh_access_token()

    ClientWithCredentials.__name__ = f"{client_class.__name__}WithCredentials"
    return ClientWithCredentials


class LightupAPIHandler:
    def __init__(self, credentials_file: Path, workspace: str):
        self.credentials_file = credentials_file
        self.workspace = workspace

        self.clients: dict[EntityType, BaseClient] = {
            entity_type: credential_wrapper_factory(
                CLIENT_TYPE_MAP[entity_type], credentials_file
            )()
            for entity_type in CLIENT_TYPE_MAP
        }

        workspace_id = self._get_workspace_id(workspace)
        if workspace_id is None:
            raise LightupException(f"Workspace {workspace} not found")
        self.workspace_id = workspace_id

    @overload
    def _get_client(self, entity_type: Literal[EntityType.METRICS]) -> MetricClient:
        ...

    @overload
    def _get_client(self, entity_type: Literal[EntityType.MONITORS]) -> MonitorClient:
        ...

    @overload
    def _get_client(
        self, entity_type: Literal[EntityType.WORKSPACES]
    ) -> WorkspaceClient:
        ...

    @overload
    def _get_client(self, entity_type: Literal[EntityType.SOURCES]) -> SourceClient:
        ...

    @overload
    def _get_client(self, entity_type: Literal[EntityType.PROFILES]) -> ProfilerClient:
        ...

    def _get_client(self, entity_type: EntityType) -> BaseClient:
        return self.clients[entity_type]

    @lru_cache(maxsize=2)
    def _get_workspace_id(self, workspace_name: str) -> Optional[str]:
        logging.info(f"Fetching workspaces with name {workspace_name}")
        workspaces_client = self._get_client(EntityType.WORKSPACES)
        workspaces = workspaces_client.list_workspaces()
        for workspace in workspaces:
            if workspace["name"] == workspace_name:
                logging.info(
                    f"Found workspace {workspace_name} with ID: {workspace['uuid']}"
                )
                return workspace["uuid"]
        logging.error(f"Failed to find workspace with name {workspace_name}")
        return None

    @lru_cache
    def get(self, entity_type: EntityType, id: UUID) -> dict[str, Any]:
        logging.info(f"Fetching data for entity type: {entity_type.name} with id: {id}")
        if entity_type == EntityType.METRICS:
            client = self._get_client(EntityType.METRICS)
            return client.get_metric(self.workspace_id, id)
        elif entity_type == EntityType.MONITORS:
            client = self._get_client(EntityType.MONITORS)
            return client.get_monitor(self.workspace_id, id)
        elif entity_type == EntityType.SOURCES:
            client = self._get_client(EntityType.SOURCES)
            return client.get_source(self.workspace_id, id)
        else:
            raise LightupException(f"Unsupported entity type {entity_type.name}")

    def list(
        self, entity_type: EntityType, query_params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        logging.info(
            f"Fetching data for entity type: {entity_type.name} with query params: {query_params}"
        )
        if entity_type == EntityType.METRICS:
            client = self._get_client(EntityType.METRICS)
            url = client.metrics_url(self.workspace_id)
        elif entity_type == EntityType.MONITORS:
            client = self._get_client(EntityType.MONITORS)
            url = client.monitors_url(self.workspace_id)
        elif entity_type == EntityType.SOURCES:
            client = self._get_client(EntityType.SOURCES)
            url = client.sources_url(self.workspace_id)
        else:
            raise LightupException(f"Unsupported entity type {entity_type}")

        query_string = urlencode(query_params)
        url_with_query_params = f"{url}?{query_string}"

        return client.get(url_with_query_params)  # type: ignore

    def post(self, entity_type: EntityType, data: dict[str, Any]) -> None:
        logging.info(
            f"Posting data for entity type: {entity_type.name} with name {data.get('metadata', {}).get('name', '')}"
        )
        if entity_type == EntityType.METRICS:
            client = self._get_client(EntityType.METRICS)
            client.create_metric(self.workspace_id, data)
        elif entity_type == EntityType.MONITORS:
            client = self._get_client(EntityType.MONITORS)
            client.create_monitor(self.workspace_id, data)
        else:
            raise LightupException(f"Unsupported entity type {entity_type.name}")

    @lru_cache
    def get_source_tree(self, source_id: UUID) -> dict[str, Any]:
        logging.info(f"Fetching source tree with source ID: {source_id}")
        client = self._get_client(EntityType.PROFILES)
        url = client.profiler_base_url(self.workspace_id, source_id)
        tree = client.get(f"{url}tree")
        return tree
