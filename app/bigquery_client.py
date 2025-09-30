import os
from google.cloud import bigquery
from google.api_core import exceptions
import logging

logger = logging.getLogger(__name__)

class BigQueryClient:
    def __init__(self):
        self.project_id = os.getenv("PROJECT_ID")
        self.dataset_id = os.getenv("DATASET_ID")
        
        if not self.project_id or not self.dataset_id:
            raise ValueError("PROJECT_ID and DATASET_ID environment variables must be set")
        
        self.client = bigquery.Client(project=self.project_id)
        logger.info(f"BigQuery client initialized for project: {self.project_id}")
        
    def get_table_ref(self, table_name: str):
        """Get full table reference"""
        return f"{self.project_id}.{self.dataset_id}.{table_name}"
    
    def insert_rows(self, table_name: str, rows: list):
        """
        Insert rows into BigQuery table using insert_rows_json
        Returns: tuple (errors, row_count)
        """
        table_ref = self.get_table_ref(table_name)
        
        try:
            errors = self.client.insert_rows_json(table_ref, rows)
            if errors:
                logger.error(f"Errors inserting rows: {errors}")
                return errors, 0
            
            logger.info(f"Successfully inserted {len(rows)} rows into {table_name}")
            return None, len(rows)
        except Exception as e:
            logger.error(f"Error inserting rows into {table_name}: {str(e)}")
            raise Exception(f"Error inserting rows: {str(e)}")
    
    def load_from_dataframe(self, table_name: str, dataframe, write_disposition="WRITE_APPEND"):
        """
        Load data from pandas DataFrame to BigQuery
        """
        table_ref = self.get_table_ref(table_name)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=False
        )
        
        try:
            job = self.client.load_table_from_dataframe(
                dataframe, 
                table_ref, 
                job_config=job_config
            )
            job.result()
            
            logger.info(f"Loaded {job.output_rows} rows into {table_name}")
            return job.output_rows
        except Exception as e:
            logger.error(f"Error loading dataframe into {table_name}: {str(e)}")
            raise Exception(f"Error loading dataframe: {str(e)}")
    
    def table_exists(self, table_name: str):
        """Check if table exists"""
        table_ref = self.get_table_ref(table_name)
        try:
            self.client.get_table(table_ref)
            return True
        except exceptions.NotFound:
            return False

bq_client = BigQueryClient()
