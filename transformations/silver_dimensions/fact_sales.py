
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


@dp.temporary_view(name="fact_sales_view")
def fact_sales_view():

    df = spark.readStream.table("northwind.silver_clean.order_details")

    return df.withColumn(
        "total_amount",
        F.expr("unit_price * quantity * (1 - discount)")
         .cast(DecimalType(10, 2))
    )


dp.create_streaming_table(
    name="northwind.silver_dimensional.fact_sales"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_dimensional.fact_sales",
    source="fact_sales_view",
    keys=["order_id", "product_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1
)

