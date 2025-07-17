import sys
import boto3
import pandas as pd
from io import StringIO

AWS_REGION = "us-east-1"

STAGED_BUCKET = "my-stagingdata-1"
STAGED_KEY = "financial_data_staged.csv"

CURATED_BUCKET = "my-curateddata-1"
CURATED_KEY = "financial_data_curated.csv"

s3 = boto3.client("s3", region_name=AWS_REGION)

def read_csv_from_s3(bucket, key):
    print(f"Reading from s3://{bucket}/{key} ...")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])

def write_csv_to_s3(df, bucket, key):
    print(f"Writing to s3://{bucket}/{key} ...")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"Saved to s3://{bucket}/{key}")

print("Loading STAGED data...")
df_staged = read_csv_from_s3(STAGED_BUCKET, STAGED_KEY)
print(f"Staged data shape: {df_staged.shape}")

print("Transforming for CURATED layer...")

df_curated = df_staged.copy()

# Preserve the original column order
original_columns = df_curated.columns.tolist()

# 1. Replace NaN in notes column with 'unknown'
if "notes" in df_curated.columns:
    df_curated["notes"] = df_curated["notes"].fillna("unknown")

# 2. Replace NaN in all other columns with 0
cols_except_notes = [c for c in df_curated.columns if c != "notes"]
df_curated[cols_except_notes] = df_curated[cols_except_notes].fillna(0)

# 3. Sort rows: first by company A→Z, then by profit_margin DESC
if "company" in df_curated.columns and "profit_margin" in df_curated.columns:
    df_curated = df_curated.sort_values(by=["company", "profit_margin"], ascending=[True, False])
elif "company" in df_curated.columns:
    # If profit_margin doesn't exist, just sort by company
    df_curated = df_curated.sort_values(by="company", ascending=True)
elif "profit_margin" in df_curated.columns:
    # If company doesn't exist, just sort by profit_margin DESC
    df_curated = df_curated.sort_values(by="profit_margin", ascending=False)

# Keep original column order
df_curated = df_curated[original_columns]

write_csv_to_s3(df_curated, CURATED_BUCKET, CURATED_KEY)

print("CURATED layer completed (sorted by company A-Z, then profit_margin DESC, columns unchanged).")
