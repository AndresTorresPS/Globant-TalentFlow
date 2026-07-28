# api/schemas.py
from pydantic import BaseModel, Field
from typing import Optional, List

# -----------------
# 1. Base Models (Single Row)
# -----------------
class Employee(BaseModel):
    id: int = Field(..., gt=0, description="Employee ID must be an integer greater than 0")
    name: str = Field(..., min_length=1)
    datetime: str = Field(..., description="ISO 8601 format expected")
    department_id: int = Field(..., gt=0, description="Department ID must be an integer greater than 0")
    job_id: int = Field(..., gt=0)

class Department(BaseModel):
    id: int = Field(..., gt=0, description="Department ID must be an integer greater than 0")
    department: str = Field(..., min_length=1)

class Job(BaseModel):
    id: int = Field(..., gt=0, description="Job ID must be an integer greater than 0")
    job: str = Field(..., min_length=1)

# -----------------
# 2. Batch Models (1 to 1000 rows)
# -----------------
class EmployeeBatch(BaseModel):
    # Enforces batch constraint natively before the code even executes
    data: List[Employee] = Field(..., min_length=1, max_length=10000)

class DepartmentBatch(BaseModel):
    data: List[Department] = Field(..., min_length=1, max_length=10000)

class JobBatch(BaseModel):
    data: List[Job] = Field(..., min_length=1, max_length=10000)