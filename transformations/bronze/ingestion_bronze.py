
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Caminho base onde estão os arquivos CSV do projeto Northwind
base_path = "/Volumes/northwind/raw/datasets/northwind_traders/"


# =========================================================
# TABELA BRONZE - CUSTOMERS
# Ingestão de clientes usando Auto Loader (cloudFiles)
# =========================================================
@dp.table(name="northwind.bronze.customers")
def customers_source():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}customers")
        
        # Adiciona timestamp de ingestão
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# =========================================================
# TABELA BRONZE - EMPLOYEES
# Ingestão de funcionários
# =========================================================
@dp.table(name="northwind.bronze.employees")
def employees_source():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}employees")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# =========================================================
# TABELA BRONZE - CATEGORIES
# Ingestão das categorias de produtos
# =========================================================
@dp.table(name="northwind.bronze.categories")
def categories_source():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}categories")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# =========================================================
# TABELA BRONZE - PRODUCTS
# Ingestão dos produtos
# =========================================================
@dp.table(name="northwind.bronze.products")
def products_source():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}products")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# =========================================================
# TABELA BRONZE - ORDERS
# Ingestão dos pedidos
# =========================================================
@dp.table(name="northwind.bronze.orders")
def orders_source():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}orders")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# =========================================================
# TABELA BRONZE - ORDER DETAILS
# Ingestão dos detalhes dos pedidos
# =========================================================
@dp.table(name="northwind.bronze.order_details")
def order_details_source():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}order_details")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# =========================================================
# TABELA BRONZE - SHIPPERS
# Ingestão das transportadoras
# =========================================================
@dp.table(name="northwind.bronze.shippers")
def shippers_source():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}shippers")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
