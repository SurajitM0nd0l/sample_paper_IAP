from pyspark import pipelines as dp
from pyspark.sql.functions import *

# --------------------------- Auto-loader pipeline read ----------------------------

# ------------------- Stores Ingestion -------------------
@dp.table(name="fk_ce_2026_07_1382.bronze.stores",
          comment="Stores ingestion data")
def bronze_stores():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("checkpointLocation", "/Volumes/fk_ce_2026_07_1382/ops/pipeline_state/stores_checkpoint/")
        .load("/Volumes/fk_ce_2026_07_1382/landing/raw/stores/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )

# ------------------- Customers Ingestion -------------------
@dp.table(name="fk_ce_2026_07_1382.bronze.customers",
          comment="Customers ingestion data")
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("checkpointLocation", "/Volumes/fk_ce_2026_07_1382/ops/pipeline_state/customers_checkpoint/")
        .load("/Volumes/fk_ce_2026_07_1382/landing/raw/customers/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )

# ------------------- Products Ingestion -------------------
@dp.table(name="fk_ce_2026_07_1382.bronze.products",
          comment="Products ingestion data")
def bronze_products():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("checkpointLocation", "/Volumes/fk_ce_2026_07_1382/ops/pipeline_state/products_checkpoint/")
        .load("/Volumes/fk_ce_2026_07_1382/landing/raw/products/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )

# ------------------- Orders Ingestion -------------------
@dp.table(name="fk_ce_2026_07_1382.bronze.orders",
          comment="Orders ingestion data")
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("checkpointLocation", "/Volumes/fk_ce_2026_07_1382/ops/pipeline_state/orders_checkpoint/")
        .load("/Volumes/fk_ce_2026_07_1382/landing/raw/orders/")
        .select(
            col("*"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )