from pyspark import pipelines as dp


dp.create_streaming_table(
    name="northwind.silver_dimensional.dim_categories"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_dimensional.dim_categories",
    source="northwind.silver_clean.categories",
    keys=["category_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=2
)