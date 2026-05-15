from pyspark import pipelines as dp
from pyspark.sql.functions import expr


dp.create_streaming_table(
    name="northwind.silver_dimensional.dim_customers"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_dimensional.dim_customers",
    source="northwind.silver_clean.customers",
    keys=["customer_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=2
)



