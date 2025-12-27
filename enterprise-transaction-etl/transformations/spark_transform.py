import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    to_date
)

# Paths
RAW_DATA_PATH = "data/raw/transactions/ingestion_date=2024-01-01/data.csv"
CURATED_DATA_PATH = "data/curated/transactions"


# Spark Session
def create_spark_session():
    return (
        SparkSession.builder
        .appName("TransactionTransformation")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.permissions.enabled", "false") \
        .getOrCreate()
    )


# Transformation Logic
def transform_transactions(spark: SparkSession):
    print("Reading raw transaction data...")

    df_raw = (
        spark.read
        .option("header", True)
        .csv(RAW_DATA_PATH)
    )

    print(f"Raw records count: {df_raw.count()}")

    # Standardize + Cast Columns
    df = df_raw.select(
        col("InvoiceNo").alias("transaction_id"),
        col("StockCode").alias("product_id"),
        col("Description").alias("product_name"),
        col("Quantity").cast("int").alias("quantity"),
        col("UnitPrice").cast("double").alias("unit_price"),
        trim(col("CustomerID").cast("string")).alias("customer_id"),
        col("InvoiceDate").cast("timestamp").alias("transaction_timestamp"),
        col("Country").alias("country")
    )

    # Basic Data Cleaning
    df = df.filter(
        (col("unit_price") > 0) &
        (col("customer_id").isNotNull())
    )

    # Deduplication
    df = df.dropDuplicates(["transaction_id", "product_id"])

    # Business Logic
    df = df.withColumn(
        "transaction_status",
        when(col("transaction_id").startswith("C"), "CANCELLED")
        .when(col("quantity") < 0, "RETURN")
        .otherwise("COMPLETED")
    )

    df = df.withColumn(
        "revenue",
        col("quantity") * col("unit_price")
    )

    df = df.withColumn(
        "transaction_date",
        to_date(col("transaction_timestamp"))
    )

    print(f"Curated records count: {df.count()}")

    # Write Curated Data
    print("Writing curated data as Parquet...")

    (
        df.write
        .mode("overwrite")
        .partitionBy("transaction_date")
        .parquet(CURATED_DATA_PATH)
    )

    print(f"Curated data written to {CURATED_DATA_PATH}")


# Entry Point
if __name__ == "__main__":
    spark = create_spark_session()

    try:
        transform_transactions(spark)
    except Exception as e:
        print("Transformation failed:", str(e))
        sys.exit(1)
    finally:
        spark.stop()
