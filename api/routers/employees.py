import json
from fastapi import APIRouter, HTTPException
from schemas import EmployeeBatch
from services.azure_blob import upload_json_to_blob

router = APIRouter(
    prefix="/api/v1/employees",
    tags=["Employees"]
)

@router.post("/batch", status_code=201)
async def ingest_employees(batch: EmployeeBatch):
    """
    Ingest a batch of hired employees.
    Pydantic automatically enforces the batch_limit constraint (max 10,000).
    """
    records = batch.data
    total_records = len(records)

    try:

        # Convert the list of Pydantic models to a list of dictionaries and then to JSON
        records_dicts = [record.model_dump(mode="json") for record in records]
        json_payload = json.dumps({"data": records_dicts})
        
        blob_path = upload_json_to_blob(
            table_name="employees", 
            chunk_index=1, 
            json_data=json_payload
        )

        return {
            "message": "Batch processed and uploaded successfully.",
            "total_records_inserted": total_records,
            "file": blob_path
        }

    except Exception as e:
        # En un entorno real de producción, esto iría a Application Insights o Datadog
        raise HTTPException(status_code=500, detail="An error occurred during data ingestion.")