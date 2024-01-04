
Workflow Steps
Extract Data:

Downloads US traffic accidents data from Kaggle using Kaggle API.
Transform Data:

Fills missing values, fixes data types, and transforms the data.
Write Locally:

Writes the transformed data to a local Parquet file.
Upload to GCS:

Uploads the local Parquet file to Google Cloud Storage.
Download from GCS:

Downloads the data from Google Cloud Storage.
Write to BigQuery:

Writes the data to BigQuery, replacing the existing table if it exists.





