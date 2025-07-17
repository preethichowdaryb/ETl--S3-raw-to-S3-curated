import boto3
import pandas as pd
import io

# Correct lowercase bucket names
staging_bucket = "my-stagingdata-1"        # staging bucket
staging_prefix = ""                      # files are in root
curated_bucket = "my-curateddata-1"        # curated bucket
curated_key = "curated/final_curated.csv"

s3 = boto3.client("s3")

# 1️⃣ List staged files
print("Listing staged files...")
staged_files = []
response = s3.list_objects_v2(Bucket=staging_bucket, Prefix=staging_prefix)

for obj in response.get("Contents", []):
    key = obj["Key"]
    if key.endswith(".csv"):
        staged_files.append(key)

if not staged_files:
    print("No staged CSV files found!")
    exit(1)

print(f"Found {len(staged_files)} staged files")

# Merge all CSVs into one DataFrame
all_dfs = []
for key in staged_files:
    print(f"Reading {key}...")
    obj = s3.get_object(Bucket=staging_bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    all_dfs.append(df)

df_curated = pd.concat(all_dfs, ignore_index=True)
print(f"Merged dataframe shape: {df_curated.shape}")

original_columns = df_curated.columns.tolist()

#  Apply transformations
if "notes" in df_curated.columns:
    df_curated["notes"] = df_curated["notes"].fillna("unknown")

cols_except_notes = [c for c in df_curated.columns if c != "notes"]
df_curated[cols_except_notes] = df_curated[cols_except_notes].fillna(0)

if "company" in df_curated.columns and "profit_margin" in df_curated.columns:
    df_curated = df_curated.sort_values(by=["company", "profit_margin"], ascending=[True, False])
elif "company" in df_curated.columns:
    df_curated = df_curated.sort_values(by="company", ascending=True)
elif "profit_margin" in df_curated.columns:
    df_curated = df_curated.sort_values(by="profit_margin", ascending=False)

df_curated = df_curated[original_columns]

print("Transformations complete")

#  Save curated single CSV back to S3
csv_buffer = io.StringIO()
df_curated.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=curated_bucket,
    Key=curated_key,
    Body=csv_buffer.getvalue().encode("utf-8")
)

print(f"Final curated CSV written to s3://{curated_bucket}/{curated_key}")
