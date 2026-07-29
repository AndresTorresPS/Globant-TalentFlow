# api/schemas.py
from pydantic import BaseModel, Field
from typing import List
from datetime import datetime as dt

batch_limit: int = 1000  # Maximum number of records allowed in a single batch request

# -----------------
# 1. Base Models (Single Row)
# -----------------
class Employee(BaseModel):
    id: int = Field(..., gt=0, description="Employee ID must be an integer greater than 0")

    # Enforces that the name must be at least 2 characters long
    name: str = Field(..., min_length=2, description="Employee name must be at least 2 characters long") 
    datetime: dt = Field(..., description="ISO 8601 format expected")

    # Enforces that department_id must be between 1 and 12, and job_id must be between 1 and 183
    department_id: int = Field(..., gt=0, le=12, description="Department ID must be between 1 and 12")
    job_id: int = Field(..., gt=0, le=183, description="Job ID must be between 1 and 183")

class Department(BaseModel):
    id: int = Field(..., gt=0, description="Department ID must be an integer greater than 0")
    department: str = Field(..., min_length=2, description="Department name must be at least 2 characters long")

class Job(BaseModel):
    id: int = Field(..., gt=0, description="Job ID must be an integer greater than 0")
    job: str = Field(..., min_length=2, description="Job title must be at least 2 characters long")

# -----------------
# 2. Batch Models (1 to batch_limit rows)
# -----------------
class EmployeeBatch(BaseModel):
    # Enforces batch constraint natively before the code even executes
    data: List[Employee] = Field(..., min_length=1, max_length=batch_limit)

class DepartmentBatch(BaseModel):
    data: List[Department] = Field(..., min_length=1, max_length=batch_limit)

class JobBatch(BaseModel):
    data: List[Job] = Field(..., min_length=1, max_length=batch_limit)