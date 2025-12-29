import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format

WAREHOUSE_DB = "warehouse/warehouse.db"
CURATED_DATA_PATH = "data/curated/transactions"


def create_spark():
    return SparkSession.builder \
        .appName("LoadFactSales") \
        .getOrCreate()


def load_fact_sales(spark, conn):
    print("Reading curated transactions...")

    df = spark.read.parquet(CURATED_DATA_PATH)

    # Prepare date_key
    df = df.withColumn(
        "date_key",
        date_format(col("transaction_date"), "yyyyMMdd").cast("int")
    )

    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO fact_sales (
            transaction_id,
            date_key,
            product_key,
            customer_key,
            country_key,
            quantity,
            unit_price,
            revenue,
            transaction_status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    rows_inserted = 0

    for row in df.collect():
        # Lookup dimension keys
        cursor.execute(
            "SELECT product_key FROM dim_product WHERE product_id = ?",
            (row.product_id,)
        )
        product_key = cursor.fetchone()

        cursor.execute(
            "SELECT customer_key FROM dim_customer WHERE customer_id = ?",
            (row.customer_id,)
        )
        customer_key = cursor.fetchone()

        cursor.execute(
            "SELECT country_key FROM dim_country WHERE country_name = ?",
            (row.country,)
        )
        country_key = cursor.fetchone()

        # Skip if any dimension is missing
        if not product_key or not customer_key or not country_key:
            continue

        cursor.execute(
            insert_sql,
            (
                row.transaction_id,
                row.date_key,
                product_key[0],
                customer_key[0],
                country_key[0],
                row.quantity,
                row.unit_price,
                row.revenue,
                row.transaction_status
            )
        )
        rows_inserted += 1

    conn.commit()
    print(f"Inserted {rows_inserted} rows into fact_sales")


if __name__ == "__main__":
    spark = create_spark()
    conn = sqlite3.connect(WAREHOUSE_DB)

    try:
        load_fact_sales(spark, conn)
    finally:
        conn.close()
        spark.stop()
