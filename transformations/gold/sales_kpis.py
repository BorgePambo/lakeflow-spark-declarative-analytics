from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="northwind.gold.sales_kpis")
def sales_kpis():

    df = spark.table("northwind.silver_dimensional.fact_sales")

    return df.groupBy().agg(
        F.sum("total_amount").alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_sold"),
        (F.sum("total_amount") / F.countDistinct("order_id")).alias("avg_ticket")
    )