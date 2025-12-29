-- DIMENSION: DATE
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       TEXT,
    day             INTEGER,
    month           INTEGER,
    month_name      TEXT,
    year            INTEGER,
    quarter         INTEGER,
    day_of_week     INTEGER,
    day_name        TEXT
);

-- DIMENSION: PRODUCT
CREATE TABLE IF NOT EXISTS dim_product (
    product_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id      TEXT UNIQUE,
    product_name    TEXT
);

-- DIMENSION: CUSTOMER
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id     INTEGER UNIQUE
);

-- DIMENSION: COUNTRY
CREATE TABLE IF NOT EXISTS dim_country (
    country_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name    TEXT UNIQUE
);

-- FACT: SALES
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key           INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id      TEXT,

    date_key            INTEGER,
    product_key         INTEGER,
    customer_key        INTEGER,
    country_key         INTEGER,

    quantity            INTEGER,
    unit_price          REAL,
    revenue             REAL,
    transaction_status  TEXT,

    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (country_key) REFERENCES dim_country(country_key)
);
