from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils import clean_strings


@dp.expect("valid_category_id", "category_id IS NOT NULL")

@dp.temporary_view(name="categories_view")
def categories_stg_view():

    df = spark.readStream.table("northwind.bronze.categories")

    df = clean_strings(df)

    return df.select(
        F.col("categoryID").alias("category_id"),
        F.col("categoryName").alias("category_name"),
        F.col("description").alias("category_description"),
        F.col("ingestion_timestamp")
    )


dp.create_streaming_table(
    name="northwind.silver_clean.categories"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_clean.categories",
    source="categories_view",
    keys=["category_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1
)