from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="northwind.gold.daily_sales_kpis")
def daily_sales_kpis():

    fact = spark.table("northwind.silver_dimensional.fact_sales")
    orders = spark.table("northwind.silver_dimensional.fact_orders")
    products = spark.table("northwind.silver_dimensional.dim_products")
    categories = spark.table("northwind.silver_dimensional.dim_categories")
    customers = spark.table("northwind.silver_dimensional.dim_customers")
    shippers = spark.table("northwind.silver_dimensional.dim_shippers")

    df = (
        fact
        .join(orders, "order_id", "left")
        .join(products, "product_id", "left")
        .join(categories, "category_id", "left")
        .join(customers, "customer_id", "left")
        .join(shippers, "shipper_id", "left")
    )

    return df.groupBy(
        F.to_date("order_date").alias("order_date"),
        "product_id",
        "product_name",
        "category_id",
        "category_name",
        "customer_id",
        "customer_name",
        "shipper_id",
        "company_name"
    ).agg(
        F.sum("total_amount").alias("total_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_id").alias("total_orders")
    )