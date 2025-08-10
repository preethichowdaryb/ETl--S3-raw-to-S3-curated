The aim of this project is to build a simple and well-organized data pipeline on AWS. I started by creating three S3 buckets named raw, staging, and curated. The dataset is downloaded from Kaggle and stored in the raw bucket exactly as it is, without any changes. To allow AWS Glue to work with these buckets, I set up an IAM role with the right permissions.

Next, I used AWS Glue to run the first round of transformations, doing some basic cleaning and formatting of the raw data. This partially processed data will then be saved into the staging bucket. After that, I executed another AWS Glue job to further clean, validate, and enrich the data from staging, making it ready for analytics. Finally, the fully processed and cleaned data is stored in the curated bucket, which will act as the final source for reporting and analysis.

Files in Repo :
Glue - Raw to Staged : Executed via script.
Glue Job - Staged to Curated : Executed via script.
ETL Glue Job - 1 : AWS Glue job from S3 raw → staged -Created using the visual editor.
Glue Job Visual - Staged to Curated : Executed via script.
