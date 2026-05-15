from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="northwind.gold.product_performance")
def product_performance():

    df = spark.table("northwind.silver_dimensional.fact_sales")

    return df.groupBy("product_id").agg(
        F.sum("total_amount").alias("total_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        (F.sum("total_amount") / F.countDistinct("order_id")).alias("avg_order_value")
    )