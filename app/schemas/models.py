from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class Department(BaseModel):
    id: int = Field(..., description="Department ID")
    department: str = Field(..., description="Department name")

class Job(BaseModel):
    id: int = Field(..., description="Job ID")
    job: str = Field(..., description="Job title")

class Employee(BaseModel):
    id: int = Field(..., description="Employee ID")
    name: str = Field(..., description="Employee name")
    datetime: datetime = Field(..., description="Hire datetime")
    department_id: Optional[int] = Field(None, description="Department ID")
    job_id: Optional[int] = Field(None, description="Job ID")

class UploadResponse(BaseModel):
    message: str
    rows: int
    table: str
