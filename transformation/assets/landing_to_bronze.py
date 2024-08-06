from typing import Dict
import sys

from dagster import (
    AssetExecutionContext,
    AssetKey,
    ResourceParam,
    SourceAsset,
    asset,
    open_pipes_session,
)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from dagster_databricks.pipes import (
    PipesDbfsContextInjector,
    PipesDbfsLogReader,
    PipesDbfsMessageReader,
)

from transformation.resources import DatabricksResource


lei_records_landing = SourceAsset(key=AssetKey("lei_records_landing"))



@asset(
    group_name="lei_records",
    compute_kind="databricks",
)
def lei_records_bronze(
    context: AssetExecutionContext,
    dbx_client: ResourceParam[WorkspaceClient],
    bronze: DatabricksResource,
):
    """lei records Transform to bronze"""
    with open_pipes_session(
        context=context,
        extras={"foo": "bar"},
        context_injector=PipesDbfsContextInjector(client=dbx_client),
        message_reader=PipesDbfsMessageReader(
            client=dbx_client,
            log_readers=[
                PipesDbfsLogReader(
                    client=dbx_client,
                    remote_log_name="stdout",
                    target_stream=sys.stdout,
                ),
                PipesDbfsLogReader(
                    client=dbx_client,
                    remote_log_name="stderr",
                    target_stream=sys.stderr,
                ),
            ],
        ),
    ) as pipes_session:
        env_vars = pipes_session.get_bootstrap_env_vars()
        env_vars["file_type"] = "delta_files"
        env_vars["date"] = "2024/08/02"
        bronze.launch_databricks_notebook(env_vars)

    yield from pipes_session.get_results()
