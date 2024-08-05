from dagster import AssetSelection, define_asset_job

lei_records_bronze = AssetSelection.assets("lei_records_bronze")
lei_records_silver = AssetSelection.assets("lei_records_silver")

transformation_job = define_asset_job(
    name="transformation_job", selection=AssetSelection.all()
)
