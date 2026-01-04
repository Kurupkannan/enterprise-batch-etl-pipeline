import sqlite3
import sys

DB_PATH = "warehouse/warehouse.db"


def run_quality_checks():
    print("\n Running data quality checks...\n")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    #  Check fact_sales row count
    cursor.execute("SELECT COUNT(*) FROM fact_sales;")
    fact_count = cursor.fetchone()[0]

    if fact_count == 0:
        print(" DATA QUALITY CHECK FAILED")
        print("Reason: fact_sales table is empty")
        sys.exit(1)

    print(f" fact_sales has {fact_count} records")

    # 2️⃣ Check NULL foreign keys
    cursor.execute("""
        SELECT COUNT(*) 
        FROM fact_sales
        WHERE date_key IS NULL
           OR product_key IS NULL
           OR customer_key IS NULL
           OR country_key IS NULL;
    """)
    null_fk_count = cursor.fetchone()[0]

    if null_fk_count > 0:
        print(" DATA QUALITY CHECK FAILED")
        print(f"Reason: {null_fk_count} records have NULL foreign keys")
        sys.exit(1)

    print(" No NULL foreign keys in fact_sales")

    #  Validate negative revenue logic
    # Negative revenue is VALID for RETURN / CANCELLED transactions
    cursor.execute("""
        SELECT COUNT(*)
        FROM fact_sales
        WHERE revenue < 0
          AND transaction_status NOT IN ('RETURN', 'CANCELLED');
    """)
    invalid_negative_revenue = cursor.fetchone()[0]

    if invalid_negative_revenue > 0:
        print(" DATA QUALITY CHECK FAILED")
        print(
            f"Reason: {invalid_negative_revenue} records have negative revenue "
            f"with invalid transaction_status"
        )
        sys.exit(1)

    print(" Negative revenue values are valid (returns / cancellations)")


    # Unit price should never be negative
    cursor.execute("""
        SELECT COUNT(*)
        FROM fact_sales
        WHERE unit_price < 0;
    """)
    negative_price_count = cursor.fetchone()[0]

    if negative_price_count > 0:
        print(" DATA QUALITY CHECK FAILED")
        print(f"Reason: {negative_price_count} records have negative unit_price")
        sys.exit(1)


    print("\n DATA QUALITY CHECK PASSED — GOLD LAYER IS TRUSTED")

    conn.close()


if __name__ == "__main__":
    run_quality_checks()
