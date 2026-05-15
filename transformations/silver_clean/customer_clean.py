from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils import clean_strings, remove_duplicates


@dp.expect_or_drop("valid_id", "customer_id IS NOT NULL")
@dp.expect("valid_name", "contact_name IS NOT NULL")

@dp.temporary_view(name="customer_view")
def customer_stg_view():

    df = spark.readStream.table("northwind.bronze.customers")

    df = clean_strings(df)

    df = df.select(
        F.col("customerID").alias("customer_id"),
        F.col("companyName").alias("customer_name"),
        F.col("contactName").alias("contact_name"),
        F.col("contactTitle").alias("contact_title"),
        F.col("city"),
        F.col("country"),
        F.col("ingestion_timestamp")
    )

    return df


dp.create_streaming_table(
    name="northwind.silver_clean.customers"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_clean.customers",
    source="customer_view",
    keys=["customer_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1
)