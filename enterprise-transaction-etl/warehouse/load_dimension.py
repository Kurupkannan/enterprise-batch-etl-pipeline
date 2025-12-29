import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format

CURATED_PATH = "data/curated/transactions"


def create_spark():
    return SparkSession.builder \
        .appName("LoadDimensions") \
        .getOrCreate()


def load_dim_date(spark, conn):
    print("Loading dim_date...")

    df = spark.read.parquet(CURATED_PATH)

    dates = (
        df.select(to_date("transaction_date").alias("full_date"))
        .distinct()
        .orderBy("full_date")
        .collect()
    )

    cursor = conn.cursor()

    for row in dates:
        date = row["full_date"]
        cursor.execute("""
            INSERT OR IGNORE INTO dim_date
            (date_key, full_date, day, month, month_name, year, quarter, day_of_week, day_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            int(date.strftime("%Y%m%d")),
            date,
            date.day,
            date.month,
            date.strftime("%B"),
            date.year,
            (date.month - 1) // 3 + 1,
            date.weekday() + 1,
            date.strftime("%A")
        ))

    conn.commit()
    print("dim_date loaded")


def load_dim_product(spark, conn):
    print("Loading dim_product...")

    df = spark.read.parquet(CURATED_PATH)

    products = df.select(
        col("product_id"),
        col("product_name")
    ).distinct().collect()

    cursor = conn.cursor()

    for row in products:
        cursor.execute("""
            INSERT OR IGNORE INTO dim_product (product_id, product_name)
            VALUES (?, ?)
        """, (row["product_id"], row["product_name"]))

    conn.commit()
    print("dim_product loaded")


def load_dim_customer(spark, conn):
    print("Loading dim_customer...")

    df = spark.read.parquet(CURATED_PATH)

    customers = df.select("customer_id").distinct().collect()

    cursor = conn.cursor()

    for row in customers:
        cursor.execute("""
            INSERT OR IGNORE INTO dim_customer (customer_id)
            VALUES (?)
        """, (row["customer_id"],))

    conn.commit()
    print("dim_customer loaded")


def load_dim_country(spark, conn):
    print("Loading dim_country...")

    df = spark.read.parquet(CURATED_PATH)

    countries = df.select("country").distinct().collect()

    cursor = conn.cursor()

    for row in countries:
        cursor.execute("""
            INSERT OR IGNORE INTO dim_country (country_name)
            VALUES (?)
        """, (row["country"],))

    conn.commit()
    print("dim_country loaded")


if __name__ == "__main__":
    spark = create_spark()
    conn = sqlite3.connect("warehouse/warehouse.db")

    load_dim_date(spark, conn)
    load_dim_product(spark, conn)
    load_dim_customer(spark, conn)
    load_dim_country(spark, conn)

    conn.close()
    spark.stop()

    print("All dimensions loaded successfully")
