# api/routers/employees.py
import json
from fastapi import APIRouter, HTTPException
from schemas import EmployeeBatch
from services.azure_blob import upload_json_to_blob

router = APIRouter(
    prefix="/api/v1/employees",
    tags=["Employees"]
)

CHUNK_SIZE = 1000

@router.post("/batch", status_code=201)
async def ingest_employees(batch: EmployeeBatch):
    """
    Ingest a batch of hired employees.
    Automatically splits payloads larger than 1,000 records into optimized chunks for Delta Live Tables / Auto Loader.
    """
    records = batch.data
    total_records = len(records)
    uploaded_files = []

    try:
        # Lógica de partición (Chunking) usando list comprehensions
        chunks = [records[i:i + CHUNK_SIZE] for i in range(0, total_records, CHUNK_SIZE)]

        for index, chunk in enumerate(chunks, start=1):
            # Serializamos solo el bloque actual
            # Usamos model_dump para convertir los objetos Pydantic a diccionarios y luego a JSON
            chunk_dicts = [record.model_dump() for record in chunk]
            json_payload = json.dumps({"data": chunk_dicts})
            
            # Subimos el bloque
            blob_path = upload_json_to_blob(
                table_name="employees", 
                chunk_index=index, 
                json_data=json_payload
            )
            uploaded_files.append(blob_path)

        return {
            "message": "Batch processed and chunked successfully.",
            "total_records_inserted": total_records,
            "chunks_created": len(chunks),
            "files": uploaded_files
        }

    except Exception as e:
        # En un entorno real de producción, esto iría a Application Insights o Datadog
        raise HTTPException(status_code=500, detail="An error occurred during data ingestion and chunking.")