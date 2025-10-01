import os

# Mock environment variables
os.environ["PROJECT_ID"] = "test"
os.environ["DATASET_ID"] = "test"
os.environ["API_KEY_SECRET"] = "test"

# Mock BigQuery client before importing app
from unittest.mock import MagicMock
import sys
mock_bq = MagicMock()
sys.modules['app.bigquery_client'] = MagicMock(bq_client=mock_bq)

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200

def test_root():
    response = client.get("/")
    assert response.status_code == 200
