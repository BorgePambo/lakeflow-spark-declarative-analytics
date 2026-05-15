from pyspark import pipelines as dp


dp.create_streaming_table(
    name="northwind.silver_dimensional.dim_employees"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_dimensional.dim_employees",
    source="northwind.silver_clean.employees",
    keys=["employee_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=2
)




