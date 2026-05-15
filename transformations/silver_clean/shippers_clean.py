from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils import clean_strings


@dp.expect_or_drop("valid_shipper_id", "shipper_id IS NOT NULL")
@dp.temporary_view(name="shippers_view")
def shippers_view():

    df = spark.readStream.table("northwind.bronze.shippers")

    df = clean_strings(df)

    return df.select(
        F.col("shipperID").alias("shipper_id"),
        F.col("companyName").alias("company_name"),
        F.col("ingestion_timestamp")
    )


dp.create_streaming_table(
    name="northwind.silver_clean.shippers"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_clean.shippers",
    source="shippers_view",
    keys=["shipper_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1
)