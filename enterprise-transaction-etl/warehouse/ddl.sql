-- ============================================
-- GOLD LAYER : DATA WAREHOUSE (STAR SCHEMA)
-- ============================================

-- =========================
-- DIMENSION: DATE
-- =========================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT PRIMARY KEY,        -- YYYYMMDD
    full_date       DATE NOT NULL,
    day             INT,
    month           INT,
    month_name      VARCHAR(20),
    year            INT,
    quarter         INT,
    day_of_week     INT,
    day_name        VARCHAR(20)
);

-- =========================
-- DIMENSION: PRODUCT
-- =========================
CREATE TABLE IF NOT EXISTS dim_product (
    product_key     SERIAL PRIMARY KEY,
    product_id      VARCHAR(50) UNIQUE,
    product_name    VARCHAR(255)
);

-- =========================
-- DIMENSION: CUSTOMER
-- =========================
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     INT UNIQUE
);

-- =========================
-- DIMENSION: COUNTRY
-- =========================
CREATE TABLE IF NOT EXISTS dim_country (
    country_key     SERIAL PRIMARY KEY,
    country_name    VARCHAR(100) UNIQUE
);

-- =========================
-- FACT: SALES
-- =========================
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key           SERIAL PRIMARY KEY,

    transaction_id      VARCHAR(50),

    date_key            INT,
    product_key         INT,
    customer_key        INT,
    country_key         INT,

    quantity            INT,
    unit_price          DECIMAL(10, 2),
    revenue             DECIMAL(12, 2),
    transaction_status  VARCHAR(20),

    -- Foreign Keys
    CONSTRAINT fk_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date (date_key),

    CONSTRAINT fk_product
        FOREIGN KEY (product_key)
        REFERENCES dim_product (product_key),

    CONSTRAINT fk_customer
        FOREIGN KEY (customer_key)
        REFERENCES dim_customer (customer_key),

    CONSTRAINT fk_country
        FOREIGN KEY (country_key)
        REFERENCES dim_country (country_key)
);
