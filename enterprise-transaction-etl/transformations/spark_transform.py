"""
PySpark transformation script for transaction data processing
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, to_date

# Paths
RAW_DATA_PATH = "data/raw/transactions/ingestion_date=2024-01-01/data.csv"
CURATED_DATA_PATH = "data/curated/transactions"

def create_spark_session():
    """Create Spark session with optimized settings for Windows"""
    return (
        SparkSession.builder
        .appName("TransactionTransformation")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g") 
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.permissions.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Windows-specific fixes for file system issues
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
        .getOrCreate()
    )

def transform_transactions(spark: SparkSession):
    """Transform raw transaction data to curated parquet format"""
    spark.sparkContext.setLogLevel("ERROR")
    
    print("=" * 60)
    print("Reading raw transaction data...")
    print("=" * 60)

    df_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_DATA_PATH)
    )

    raw_count = df_raw.count()
    print(f"[OK] Raw records count: {raw_count:,}")

    # Standardize + Cast Columns
    print("\nTransforming data...")
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

    curated_count = df.count()
    print(f"[OK] Curated records count: {curated_count:,}")
    print(f"[OK] Filtered out: {raw_count - curated_count:,} records")

    # Write Curated Data - simplified for Windows compatibility
    print("\n" + "=" * 60)
    print("Writing curated data as Parquet...")
    print("=" * 60)

    (
        df.coalesce(1)  # Reduce to single partition to avoid Windows file system issues
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(CURATED_DATA_PATH)
    )

    print(f"\n[OK] SUCCESS! Curated data written to {CURATED_DATA_PATH}")
    print("=" * 60)

if __name__ == "__main__":
    spark = create_spark_session()

    try:
        transform_transactions(spark)
    except Exception as e:
        print(f"\n[FAIL] Transformation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("\nSpark session closed.")
