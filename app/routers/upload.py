from fastapi import APIRouter, File, UploadFile, HTTPException
from typing import List
import pandas as pd
import io
from datetime import datetime
import pytz
from ..bigquery_client import bq_client
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/upload/departments")
async def upload_departments(file: UploadFile = File(...)):
    """
    Upload departments from CSV file
    
    Expected CSV format:
    - id (integer): Department ID
    - department (string): Department name
    
    """

    logger.info('Check file extension')
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    try:
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        required_cols = ["id", "department"]

        if not set(required_cols).intersection(set(df.columns)):
            df = pd.read_csv(io.StringIO(contents.decode('utf-8')), header=None)
            df.columns = required_cols[:len(df.columns)] 
        
        if len(df) == 0:
            raise HTTPException(
                status_code=400, 
                detail="CSV file is empty"
            )
        
        df['id'] = df['id'].astype(int)
        df['department'] = df['department'].astype(str).str.strip()

        ba_tz = pytz.timezone("America/Argentina/Buenos_Aires")
        df['loaded_at'] = pd.Timestamp.now(tz=ba_tz)
        
        logger.info('Prepared to upload to BigQuery')
        rows_inserted = bq_client.load_from_dataframe('departments', df)
        
        logger.info(f"Successfully uploaded {rows_inserted} departments")
        return {
            "message": f"Successfully uploaded {rows_inserted} departments",
            "rows": rows_inserted,
            "table": "departments"
        }
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="CSV file is empty or invalid")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Data validation error: {str(e)}")
    except Exception as e:
        logger.error(f"Error uploading departments: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

@router.post("/upload/jobs")
async def upload_jobs(file: UploadFile = File(...)):
    """
    Upload jobs from CSV file
    
    Expected CSV format:
    - id (integer): Job ID
    - job (string): Job title
    
    """
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    try:
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        required_cols = ["id", "job"]

        if not set(required_cols).intersection(set(df.columns)):
            df = pd.read_csv(io.StringIO(contents.decode('utf-8')), header=None)
            df.columns = required_cols[:len(df.columns)] 


        if len(df) == 0:
            raise HTTPException(
                status_code=400, 
                detail="CSV file is empty"
            )
        
        df['id'] = df['id'].astype(int)
        df['job'] = df['job'].astype(str).str.strip()

        ba_tz = pytz.timezone("America/Argentina/Buenos_Aires")
        df['loaded_at'] = pd.Timestamp.now(tz=ba_tz)
        
        logger.info('Prepared to upload to BigQuery')
        rows_inserted = bq_client.load_from_dataframe('jobs', df)
        
        logger.info(f"Successfully uploaded {rows_inserted} jobs")
        return {
            "message": f"Successfully uploaded {rows_inserted} jobs",
            "rows": rows_inserted,
            "table": "jobs"
        }
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="CSV file is empty or invalid")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Data validation error: {str(e)}")
    except Exception as e:
        logger.error(f"Error uploading jobs: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

@router.post("/upload/employees")
async def upload_employees(file: UploadFile = File(...)):
    """
    Upload hired employees from CSV file
    
    Expected CSV format:
    - id (integer): Employee ID
    - name (string): Employee name
    - datetime (string): Hire datetime in ISO format
    - department_id (integer): Department ID (nullable)
    - job_id (integer): Job ID (nullable)
    
    """
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    try:
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        required_cols = ['id', 'name', 'datetime', 'department_id', 'job_id']

        if not set(required_cols).intersection(set(df.columns)):
            df = pd.read_csv(io.StringIO(contents.decode('utf-8')), header=None)
            df.columns = required_cols[:len(df.columns)] 
        
        if len(df) == 0:
            raise HTTPException(
                status_code=400, 
                detail="CSV file is empty"
            )
        
        df['id'] = df['id'].astype(int)
        df['name'] = df['name'].astype(str).str.strip()
        df['datetime'] = pd.to_datetime(df['datetime']).dt.round('ms')
        df['department_id'] = df['department_id'].astype('Int64')
        df['job_id'] = df['job_id'].astype('Int64')

        ba_tz = pytz.timezone("America/Argentina/Buenos_Aires")
        df['loaded_at'] = pd.Timestamp.now(tz=ba_tz)
        
        logger.info('Ready to load df to bq')
        # rows_inserted = bq_client.load_from_dataframe_gbq('hired_employees', df)
        rows_inserted = bq_client.load_from_dataframe_buffer('hired_employees', df)
        

        logger.info(f"Successfully uploaded {rows_inserted} employees")
        return {
            "message": f"Successfully uploaded {rows_inserted} employees",
            "rows": rows_inserted,
            "table": "hired_employees"
        }
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="CSV file is empty or invalid")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Data validation error: {str(e)}")
    except Exception as e:
        logger.error(f"Error uploading employees: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

@router.post("/upload/batch/{table_name}")
async def upload_batch(table_name: str, data: List[dict]):
    """
    Upload batch data (1-1000 rows) as JSON
    Table name must be: departments, jobs, or hired_employees
    """
    if table_name not in ['departments', 'jobs', 'hired_employees']:
        raise HTTPException(status_code=400, detail="Invalid table name")
    
    if len(data) < 1 or len(data) > 1000:
        raise HTTPException(
            status_code=400, 
            detail="Batch size must be between 1 and 1000"
        )
    
    try:
        df = pd.DataFrame(data)
        
        if table_name == 'departments':
            df['id'] = df['id'].astype(int)
            df['department'] = df['department'].str.strip()
        
        elif table_name == 'jobs':
            df['id'] = df['id'].astype(int)
            df['job'] = df['job'].str.strip()
        
        elif table_name == 'hired_employees':
            df['id'] = df['id'].astype(int)
            df['name'] = df['name'].str.strip()
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['department_id'] = df['department_id'].astype('Int64')
            df['job_id'] = df['job_id'].astype('Int64')

        ba_tz = pytz.timezone("America/Argentina/Buenos_Aires")
        df['loaded_at'] = pd.Timestamp.now(tz=ba_tz)
        
        rows_inserted = bq_client.load_from_dataframe(table_name, df)
        
        return {
            "message": f"Successfully inserted {rows_inserted} rows into {table_name}",
            "rows": rows_inserted
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))