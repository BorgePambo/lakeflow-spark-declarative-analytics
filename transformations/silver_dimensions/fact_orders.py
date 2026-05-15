
from pyspark import pipelines as dp


@dp.temporary_view(name="fact_orders_view")
def fact_orders_view():

    df = spark.readStream.table("northwind.silver_clean.orders")

    return df


dp.create_streaming_table(
    name="northwind.silver_dimensional.fact_orders"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_dimensional.fact_orders",
    source="fact_orders_view",
    keys=["order_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1
)


