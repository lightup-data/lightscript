import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Optional, overload
from uuid import UUID

from lightctl.client.base_client import BaseClient
from lightctl.client.metric_client import MetricClient
from lightctl.client.monitor_client import MonitorClient
from lightctl.client.profiler_client import ProfilerClient
from lightctl.client.source_client import SourceClient
from lightctl.client.workspace_client import WorkspaceClient
from lightctl.util import LightupException
from models import EntityType
from utils import get_entity_name

CLIENT_TYPE_MAP: dict[EntityType, type[BaseClient]] = {
    EntityType.METRIC: MetricClient,
    EntityType.MONITOR: MonitorClient,
    EntityType.WORKSPACE: WorkspaceClient,
    EntityType.SOURCE: SourceClient,
    EntityType.PROFILE: ProfilerClient,
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
    def _get_client(self, entity_type: Literal[EntityType.METRIC]) -> MetricClient:
        ...

    @overload
    def _get_client(self, entity_type: Literal[EntityType.MONITOR]) -> MonitorClient:
        ...

    @overload
    def _get_client(
        self, entity_type: Literal[EntityType.WORKSPACE]
    ) -> WorkspaceClient:
        ...

    @overload
    def _get_client(self, entity_type: Literal[EntityType.SOURCE]) -> SourceClient:
        ...

    @overload
    def _get_client(self, entity_type: Literal[EntityType.PROFILE]) -> ProfilerClient:
        ...

    def _get_client(self, entity_type: EntityType) -> BaseClient:
        return self.clients[entity_type]

    @lru_cache(maxsize=2)
    def _get_workspace_id(self, workspace_name: str) -> Optional[str]:
        logging.info(f"Fetching workspaces with name {workspace_name}")
        workspaces_client = self._get_client(EntityType.WORKSPACE)
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
        if entity_type == EntityType.METRIC:
            client = self._get_client(EntityType.METRIC)
            return client.get_metric(self.workspace_id, id)
        elif entity_type == EntityType.MONITOR:
            client = self._get_client(EntityType.MONITOR)
            return client.get_monitor(self.workspace_id, id)
        elif entity_type == EntityType.SOURCE:
            client = self._get_client(EntityType.SOURCE)
            return client.get_source(self.workspace_id, id)
        else:
            raise LightupException(f"Unsupported entity type {entity_type.name}")

    @lru_cache
    def list(self, entity_type: EntityType, query_string: str) -> list[dict[str, Any]]:
        logging.info(
            f"Fetching data for entity type: {entity_type.name} with query string: {query_string}"
        )
        if entity_type == EntityType.METRIC:
            client = self._get_client(EntityType.METRIC)
            url = client.metrics_url(self.workspace_id)
        elif entity_type == EntityType.MONITOR:
            client = self._get_client(EntityType.MONITOR)
            url = client.monitors_url(self.workspace_id)
        elif entity_type == EntityType.SOURCE:
            client = self._get_client(EntityType.SOURCE)
            url = client.sources_url(self.workspace_id)
        else:
            raise LightupException(f"Unsupported entity type {entity_type}")

        url_with_query_params = f"{url}?{query_string}"

        return client.get(url_with_query_params)  # type: ignore

    def post(self, entity_type: EntityType, data: dict[str, Any]) -> None:
        logging.info(
            f"Posting data for entity type: {entity_type.name} with name {get_entity_name(data)}"
        )
        if entity_type == EntityType.METRIC:
            client = self._get_client(EntityType.METRIC)
            client.create_metric(self.workspace_id, data)
        elif entity_type == EntityType.MONITOR:
            client = self._get_client(EntityType.MONITOR)
            client.create_monitor(self.workspace_id, data)
        else:
            raise LightupException(f"Unsupported entity type {entity_type.name}")

    @lru_cache
    def get_source_tree(self, source_id: UUID) -> dict[str, Any]:
        logging.info(f"Fetching source tree with source ID: {source_id}")
        client = self._get_client(EntityType.PROFILE)
        url = client.profiler_base_url(self.workspace_id, source_id)
        tree = client.get(f"{url}tree")
        return tree
