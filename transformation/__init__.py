from dagster import Definitions, load_assets_from_modules

from .assets import landing_to_bronze, bronze_to_silver
from .resources import (
    databricks_client_resource,
    dbx_bronze_resource,
    dbx_silver_resource,
)

all_assets = load_assets_from_modules([landing_to_bronze, bronze_to_silver])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbx_client": databricks_client_resource,
        "bronze": dbx_bronze_resource,
        "silver": dbx_silver_resource,
    },
)
