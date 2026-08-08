import os
from dotenv import load_dotenv

load_dotenv()  # reads .env and loads its values into the environment

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not GCP_PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID is not set in .env")
if not CREDENTIALS_PATH:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS is not set in .env")