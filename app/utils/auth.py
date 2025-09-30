import os
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from google.cloud import secretmanager
import logging

logger = logging.getLogger(__name__)

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

def get_api_key_from_secret():
    """Retrieve API key from Secret Manager"""
    project_id = os.getenv("PROJECT_ID")
    secret_id = os.getenv("API_KEY_SECRET")
    
    if not project_id or not secret_id:
        logger.error("PROJECT_ID or API_KEY_SECRET not set")
        return None
    
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    
    try:
        response = client.access_secret_version(request={"name": name})
        api_key = response.payload.data.decode("UTF-8")
        logger.info(f"Successfully retrieved API key from secret: {secret_id}")
        return api_key
    except Exception as e:
        logger.error(f"Error retrieving API key: {e}")
        return None

VALID_API_KEY = None

def get_valid_api_key():
    """Get and cache the valid API key"""
    global VALID_API_KEY
    if VALID_API_KEY is None:
        VALID_API_KEY = get_api_key_from_secret()
    return VALID_API_KEY

async def verify_api_key(api_key: str = Security(API_KEY_HEADER)):
    """Verify API key from request header"""
    valid_key = get_valid_api_key()
    
    if not valid_key:
        raise HTTPException(
            status_code=500,
            detail="API key not configured on server"
        )
    
    if api_key is None:
        raise HTTPException(
            status_code=403,
            detail="Missing API key. Include X-API-Key header in your request"
        )
    
    if api_key != valid_key:
        logger.warning(f"Invalid API key attempt")
        raise HTTPException(
            status_code=403,
            detail="Invalid API key"
        )
    
    return api_key
