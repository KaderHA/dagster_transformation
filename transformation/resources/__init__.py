from typing import Dict
from dagster import ConfigurableResource
from pydantic import Field

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

databricks_client_resource = WorkspaceClient()


class DatabricksResource(ConfigurableResource):
    source: str = Field(description=("Path to source data"))
    dest: str = Field(description=("Path to destination data"))
    notebook_path: str = Field(description=("Path to notebook on Databricks"))
    cluster_id: str = Field(description=("Databricks cluster id"))

    def launch_databricks_notebook(self, params: Dict[str, str]):
        params["src"] = self.source
        params["dest"] = self.dest
        task = jobs.SubmitTask(
            task_key=self.notebook_path.split("/").pop(),
            existing_cluster_id=self.cluster_id,
            notebook_task=jobs.NotebookTask(
                notebook_path=self.notebook_path, base_parameters=params
            ),
        )

        databricks_client_resource.jobs.submit(
            run_name="dagster_pipes_job", tasks=[task]
        ).result()


dbx_bronze_resource = DatabricksResource(
    source="abfss://demo@saintern.dfs.core.windows.net",
    dest="abfss://demo1@saintern1.dfs.core.windows.net",
    notebook_path="/Users/wm1371b@norges-bank.no/.bundle/transformation/dev/files/src/transformation/gleif/gleif-lei-files_landing-to-bronze",
    cluster_id="0731-085114-wmrqrgur",
)
dbx_silver_resource = DatabricksResource(
    source="abfss://demo1@saintern1.dfs.core.windows.net",
    dest="abfss://demo2@saintern2.dfs.core.windows.net",
    notebook_path="/Users/wm1371b@norges-bank.no/.bundle/transformation/dev/files/src/transformation/gleif/gleif-lei-records_bronze-to-silver",
    cluster_id="0731-085114-wmrqrgur",
)
