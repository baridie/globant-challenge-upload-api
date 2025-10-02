from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from .routers import upload
from .utils.auth import verify_api_key
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Globant Upload API",
    description="Data Upload API for Globant Challenge - Handles CSV uploads insertions",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    upload.router, 
    prefix="/api/v1",
    tags=["Upload"],
    dependencies=[Depends(verify_api_key)]
)

@app.get("/")
def read_root():
    return {
        "service": "Globant Upload API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "upload": "/api/v1/upload"
        }
    }

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "upload-api"
    }

@app.on_event("startup")
async def startup_event():
    logger.info("Upload API starting up...")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Upload API shutting down...")
