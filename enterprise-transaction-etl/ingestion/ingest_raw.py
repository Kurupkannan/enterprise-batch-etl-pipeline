import os
import sys
import json
import pandas as pd
from datetime import datetime

RAW_BASE_PATH = "data/raw/transactions"
SCHEMA_PATH = "config/schema.json"

print(">>> Script started")
print(">>> Current working directory:", os.getcwd())
print(">>> Arguments received:", sys.argv)


def load_schema(schema_path):
    print(">>> Loading schema from:", schema_path)
    with open(schema_path, "r") as f:
        return json.load(f)["columns"]


def read_source_file(file_path):
    print(">>> Reading source file:", file_path)
    if file_path.endswith(".xlsx"):
        return pd.read_excel(file_path, engine="openpyxl")
    elif file_path.endswith(".csv"):
        return pd.read_csv(file_path)
    else:
        raise ValueError("Unsupported file format")


def ingest(file_path, ingestion_date):
    print(">>> Ingest function entered")

    expected_columns = load_schema(SCHEMA_PATH)
    df = read_source_file(file_path)

    print(">>> Rows read:", len(df))
    print(">>> Columns found:", list(df.columns))

    output_path = os.path.join(
        RAW_BASE_PATH, f"ingestion_date={ingestion_date}"
    )

    print(">>> Output path will be:", output_path)

    os.makedirs(output_path, exist_ok=True)

    df["ingestion_date"] = ingestion_date
    df["source_file"] = os.path.basename(file_path)
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    output_file = os.path.join(output_path, "data.csv")
    df.to_csv(output_file, index=False)

    print(">>> Data written to:", output_file)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise ValueError("Usage: python ingest_raw.py <file_path> <ingestion_date>")

    source_file_path = sys.argv[1]
    ingestion_date = sys.argv[2]

    ingest(source_file_path, ingestion_date)
