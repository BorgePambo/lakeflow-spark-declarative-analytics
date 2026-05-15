
from pyspark import pipelines as dp


dp.create_streaming_table(
    name="northwind.silver_dimensional.dim_shippers"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_dimensional.dim_shippers",
    source="northwind.silver_clean.shippers",
    keys=["shipper_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=2
)
