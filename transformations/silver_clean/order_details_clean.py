from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils import cast_decimal, cast_integer


@dp.expect_all({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_unit_price": "unit_price > 0",
    "valid_qty": "quantity > 0",
    "valid_discount": "discount >= 0"
})

@dp.temporary_view(name="order_details_view")
def orders_details_stg_view():

    df = spark.readStream.table("northwind.bronze.order_details")

    df = cast_decimal(df, ["unitPrice", "discount"])
    df = cast_integer(df, ["quantity"])

    df = df.withColumn(
        "total_amount",
        F.expr("unitPrice * quantity * (1 - discount)")
    )

    return df.select(
        F.col("orderID").alias("order_id"),
        F.col("productID").alias("product_id"),
        F.col("unitPrice").alias("unit_price"),
        F.col("quantity"),
        F.col("discount"),
        F.col("total_amount"),
        F.col("ingestion_timestamp")
    )


dp.create_streaming_table(
    name="northwind.silver_clean.order_details"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_clean.order_details",
    source="order_details_view",
    keys=["order_id", "product_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1
)