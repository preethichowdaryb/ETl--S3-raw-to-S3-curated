import sys
import boto3
import pandas as pd
from io import StringIO

# -----------------------------
# CONFIGURATION
# -----------------------------
AWS_REGION = "us-east-1"

RAW_BUCKET = "my-rawdata-1"               # Must not have trailing space
RAW_KEY = "financial_raw data.csv"        # Must exactly match S3 key

STAGED_BUCKET = "my-stagingdata-1"
STAGED_KEY = "financial_data_staged.csv"

# S3 Client
s3 = boto3.client("s3", region_name=AWS_REGION)

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def read_csv_from_s3(bucket, key):
    """Read CSV from S3 into pandas dataframe"""
    print(f"Reading from s3://{bucket}/{key} ...")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])

def write_csv_to_s3(df, bucket, key):
    """Write pandas dataframe back to S3 as CSV"""
    print(f"Writing to s3://{bucket}/{key} ...")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"Saved to s3://{bucket}/{key}")

# -----------------------------
# MAIN PROCESS
# -----------------------------
print("Loading RAW data from S3...")
df_raw = read_csv_from_s3(RAW_BUCKET, RAW_KEY)
print(f"Raw data shape: {df_raw.shape}")

print("Removing duplicates for STAGED layer...")

df_staged = df_raw.copy()

# Normalize column names
df_staged.columns = [c.strip().lower().replace(" ", "_") for c in df_staged.columns]

# Remove duplicates
before = df_staged.shape[0]
df_staged = df_staged.drop_duplicates()
after = df_staged.shape[0]
print(f"Removed {before - after} duplicate rows.")

# Save STAGED data
write_csv_to_s3(df_staged, STAGED_BUCKET, STAGED_KEY)

print("STAGED layer transformation completed (duplicates removed).")
