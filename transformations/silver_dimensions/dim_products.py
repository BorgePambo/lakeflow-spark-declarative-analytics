from pyspark import pipelines as dp


# tabela dimensional (SCD2)
dp.create_streaming_table(
    name="northwind.silver_dimensional.dim_products"
)


# CDC Type 2 (histórico)
dp.create_auto_cdc_flow(
    target="northwind.silver_dimensional.dim_products",
    source="northwind.silver_clean.products",
    keys=["product_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=2
)
