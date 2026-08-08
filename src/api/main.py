# from fastapi import FastAPI, HTTPException
# from google.cloud import bigquery

# from src.common.gcp_config import GCP_PROJECT_ID

# app = FastAPI(title="DataOnCloud API", version="1.0")

# DATASET_ID = "dataoncloud_raw"


# def get_bq_client():
#     return bigquery.Client(project=GCP_PROJECT_ID)


# @app.get("/health")
# def health_check():
#     return {"status": "ok"}


# @app.get("/customers")
# def get_customers(limit: int = 10):
#     client = get_bq_client()
#     query = f"""
#         SELECT customer_id, name, email, signup_date
#         FROM `{GCP_PROJECT_ID}.{DATASET_ID}.customers_csv`
#         LIMIT @limit
#     """
#     job_config = bigquery.QueryJobConfig(
#         query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
#     )
#     try:
#         results = client.query(query, job_config=job_config).result()
#         return {"count": results.total_rows, "customers": [dict(row) for row in results]}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @app.get("/users")
# def get_users(limit: int = 10):
#     client = get_bq_client()
#     query = f"""
#         SELECT id, name, email, username
#         FROM `{GCP_PROJECT_ID}.{DATASET_ID}.users_api`
#         LIMIT @limit
#     """
#     job_config = bigquery.QueryJobConfig(
#         query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
#     )
#     try:
#         results = client.query(query, job_config=job_config).result()
#         return {"count": results.total_rows, "users": [dict(row) for row in results]}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/orders")
# def get_orders(limit: int = 10):
#     client = get_bq_client()
#     query = f"""
#         SELECT order_id, product, quantity, price, order_timestamp
#         FROM `{GCP_PROJECT_ID}.{DATASET_ID}.orders_stream`
#         ORDER BY order_timestamp DESC
#         LIMIT @limit
#     """
#     job_config = bigquery.QueryJobConfig(
#         query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
#     )
#     try:
#         results = client.query(query, job_config=job_config).result()
#         return {"count": results.total_rows, "orders": [dict(row) for row in results]}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


from fastapi import FastAPI, HTTPException
from google.cloud import bigquery

from src.common.gcp_config import GCP_PROJECT_ID

app = FastAPI(title="DataOnCloud API", version="1.0")

DATASET_ID = "dataoncloud_raw"

client = bigquery.Client(project=GCP_PROJECT_ID)


def fetch_table(table_name: str, columns: str, limit: int, order_by: str = None) -> dict:
    order_clause = f"ORDER BY {order_by} DESC" if order_by else ""
    query = f"""
        SELECT {columns}
        FROM `{GCP_PROJECT_ID}.{DATASET_ID}.{table_name}`
        {order_clause}
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    try:
        results = client.query(query, job_config=job_config).result()
        return {"count": results.total_rows, "data": [dict(row) for row in results]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/customers")
def get_customers(limit: int = 5):
    return fetch_table("customers_csv", "customer_id, name, email, signup_date", limit)


@app.get("/users")
def get_users(limit: int = 10):
    return fetch_table("users_api", "id, name, email, username", limit)


@app.get("/orders")
def get_orders(limit: int = 10):
    return fetch_table("orders_stream", "order_id, product, quantity, price, order_timestamp", limit, order_by="order_timestamp")