from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils import cast_decimal, remove_duplicates, to_date


@dp.expect_all({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_employee_id": "employee_id IS NOT NULL",
    "valid_shipper_id": "shipper_id IS NOT NULL",
    "valid_freight": "freight IS NOT NULL"
})

@dp.temporary_view(name="orders_view")
def orders_stg_view():

    df = spark.readStream.table("northwind.bronze.orders")

    df = cast_decimal(df, ["freight"])
    df = to_date(df, ["orderDate", "requiredDate", "shippedDate"])

    return df.select(
        F.col("orderID").alias("order_id"),
        F.col("customerID").alias("customer_id"),
        F.col("employeeID").alias("employee_id"),
        F.col("orderDate").alias("order_date"),
        F.col("requiredDate").alias("required_date"),
        F.col("shippedDate").alias("shipped_date"),
        F.col("shipperID").alias("shipper_id"),
        F.col("freight").alias("freight"),
        F.col("ingestion_timestamp")
    )


dp.create_streaming_table(
    name="northwind.silver_clean.orders"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_clean.orders",
    source="orders_view",
    keys=["order_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1
)