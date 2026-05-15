from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils import clean_strings, cast_decimal


@dp.expect_all_or_drop({
    "valid_product_id": "product_id IS NOT NULL",
    "valid_price": "unit_price > 0",
})
@dp.temporary_view(name="products_view")
def products_stg_view():

    df = spark.readStream.table("northwind.bronze.products")

    df = clean_strings(df)
    df = cast_decimal(df, ["unitPrice"])

    return df.select(
        F.col("productID").alias("product_id"),
        F.col("productName").alias("product_name"),
        F.col("quantityPerUnit").alias("qty_per_unit"),
        F.col("unitPrice").alias("unit_price"),
        F.col("discontinued"),
        F.col("categoryID").alias("category_id"),
        F.col("ingestion_timestamp")
    )


dp.create_streaming_table(
    name="northwind.silver_clean.products"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_clean.products",
    source="products_view",
    keys=["product_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1
)