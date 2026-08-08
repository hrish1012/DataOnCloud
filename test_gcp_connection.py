from google.cloud import bigquery
from src.common.gcp_config import GCP_PROJECT_ID

client = bigquery.Client(project=GCP_PROJECT_ID)

datasets = list(client.list_datasets())
print(f"Connected to project: {GCP_PROJECT_ID}")
print(f"Found {len(datasets)} existing dataset(s).")
for ds in datasets:
    print(f" - {ds.dataset_id}")