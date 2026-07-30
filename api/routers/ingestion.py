import json
import logging
from fastapi import APIRouter, HTTPException
from schemas import UnifiedBatch
from services.azure_blob import upload_json_to_blob

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v3/ingestion",
    tags=["Ingestion"]
)

# Endpoint for unified batch ingestion
@router.post("/unified-batch", status_code=201)
async def ingest_unified_data(batch: UnifiedBatch):
    """
    Ingests a unified batch containing departments, jobs, and employees.
    Validates referential integrity automatically before processing.
    """
    try:
        uploaded_files = []

        # Extract and serialize Departments
        departments_dicts = [record.model_dump(mode="json") for record in batch.departments]
        if departments_dicts:
            dep_path = upload_json_to_blob(
                table_name="departments",
                json_data=json.dumps({"data": departments_dicts})
            )
            uploaded_files.append(dep_path)

        # Extract and serialize Jobs
        jobs_dicts = [record.model_dump(mode="json") for record in batch.jobs]
        if jobs_dicts:
            job_path = upload_json_to_blob(
                table_name="jobs",
                json_data=json.dumps({"data": jobs_dicts})
            )
            uploaded_files.append(job_path)

        # Extract and serialize Employees
        employees_dicts = [record.model_dump(mode="json") for record in batch.employees]
        if employees_dicts:
            emp_path = upload_json_to_blob(
                table_name="employees",
                json_data=json.dumps({"data": employees_dicts})
            )
            uploaded_files.append(emp_path)

        return {
            "message": "Unified batch processed, validated, and uploaded successfully.",
            "metrics": {
                "departments_inserted": len(departments_dicts),
                "jobs_inserted": len(jobs_dicts),
                "employees_inserted": len(employees_dicts)
            },
            "files": uploaded_files
        }

    except Exception as e:
        logger.error("Error during unified data ingestion: %s", e)
        raise HTTPException(status_code=500, detail="An error occurred during data ingestion.")