# api/schemas.py
from pydantic import BaseModel, Field, model_validator
from typing import List
from datetime import datetime as dt 

# Batch limit for all payloads (departments, jobs, employees)
batch_limit: int = 1000 

# -----------------
# 1. Base Models (Single Row)
# -----------------
class Department(BaseModel):
    id: int = Field(..., gt=0, description="Department ID must be an integer greater than 0")
    department: str = Field(..., min_length=2, description="Department name must be at least 2 characters long")

class Job(BaseModel):
    id: int = Field(..., gt=0, description="Job ID must be an integer greater than 0")
    job: str = Field(..., min_length=2, description="Job title must be at least 2 characters long")

class Employee(BaseModel):
    id: int = Field(..., gt=0, description="Employee ID must be an integer greater than 0")
    name: str = Field(..., min_length=2, description="Employee name must be at least 2 characters long")
    datetime: dt = Field(..., description="ISO 8601 format expected")
    department_id: int = Field(..., gt=0, description="Department ID must be an integer greater than 0")
    job_id: int = Field(..., gt=0, description="Job ID must be an integer greater than 0")

# -----------------
# 2. Unified Batch Model
# -----------------
class UnifiedBatch(BaseModel):
    departments: List[Department] = Field(..., max_length=batch_limit)
    jobs: List[Job] = Field(..., max_length=batch_limit)
    employees: List[Employee] = Field(..., max_length=batch_limit)

    @model_validator(mode='after')
    def validate_foreign_keys(self) -> 'UnifiedBatch':
        """
        Checks the integrity of foreign keys in the employees list against 
        the provided departments and jobs lists.
        """
        # Sets of valid department and job IDs for quick lookup 
        valid_department_ids = {dep.id for dep in self.departments}
        valid_job_ids = {job.id for job in self.jobs}

        # Each employee's department_id and job_id must exist in the respective lists
        for emp in self.employees:
            if emp.department_id not in valid_department_ids:
                raise ValueError(f"Integrity Error: Employee {emp.id} references department_id {emp.department_id} which is not in the departments list.")
            
            if emp.job_id not in valid_job_ids:
                raise ValueError(f"Integrity Error: Employee {emp.id} references job_id {emp.job_id} which is not in the jobs list.")
        
        return self