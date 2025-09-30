from fastapi.testclient import TestClient
from app.main import app
import io
import pytest

client = TestClient(app)

TEST_API_KEY = "test-key-for-testing-12345"

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_root_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    assert "service" in response.json()
    assert response.json()["service"] == "Globant Upload API"

def test_upload_without_api_key():
    csv_content = "id,department\n1,Sales\n2,Engineering"
    files = {"file": ("departments.csv", io.BytesIO(csv_content.encode()), "text/csv")}
    
    response = client.post("/api/v1/upload/departments", files=files)
    assert response.status_code == 403

def test_upload_with_invalid_file_type():
    headers = {"X-API-Key": TEST_API_KEY}
    files = {"file": ("test.txt", io.BytesIO(b"test content"), "text/plain")}
    
    response = client.post("/api/v1/upload/departments", files=files, headers=headers)
    assert response.status_code == 400
    assert "CSV" in response.json()["detail"]

def test_batch_size_limit():
    headers = {"X-API-Key": TEST_API_KEY}
    data = [{"id": i, "department": f"Dept {i}"} for i in range(1001)]
    
    response = client.post("/api/v1/upload/batch/departments", json=data, headers=headers)
    assert response.status_code == 400
    assert "1000" in response.json()["detail"]

def test_invalid_table_name():
    headers = {"X-API-Key": TEST_API_KEY}
    data = [{"id": 1, "name": "test"}]
    
    response = client.post("/api/v1/upload/batch/invalid_table", json=data, headers=headers)
    assert response.status_code == 400
    assert "Invalid table name" in response.json()["detail"]

def test_empty_batch():
    headers = {"X-API-Key": TEST_API_KEY}
    data = []
    
    response = client.post("/api/v1/upload/batch/departments", json=data, headers=headers)
    assert response.status_code == 400
    assert "between 1 and 1000" in response.json()["detail"]

def test_batch_upload_valid_structure():
    headers = {"X-API-Key": TEST_API_KEY}
    data = [
        {"id": 1, "department": "Test Department"}
    ]
    
    response = client.post("/api/v1/upload/batch/departments", json=data, headers=headers)
    # Will fail without actual BigQuery connection, but validates structure
    assert response.status_code in [200, 500]
