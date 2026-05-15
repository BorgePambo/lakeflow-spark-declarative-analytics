from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils import clean_strings, remove_duplicates


@dp.expect_all({
    "valid_employee_name": "employee_name IS NOT NULL",
    "valid_title": "title IS NOT NULL",
    "valid_city": "city IS NOT NULL",
    "valid_country": "country IS NOT NULL",
    "valid_reports_to": "reports_to IS NOT NULL"
})

@dp.temporary_view(name="employees_view")
def employees_stg_view():

    df = spark.readStream.table("northwind.bronze.employees")

    df = clean_strings(df)

    return df.select(
        F.col("employeeID").alias("employee_id"),
        F.col("employeeName").alias("employee_name"),
        F.col("title"),
        F.col("city"),
        F.col("country"),
        F.col("reportsTo").alias("reports_to"),
        F.col("ingestion_timestamp")
    )


dp.create_streaming_table(
    name="northwind.silver_clean.employees"
)


dp.create_auto_cdc_flow(
    target="northwind.silver_clean.employees",
    source="employees_view",
    keys=["employee_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1
)