from fastapi import FastAPI
from routers import ingestion, reports 

app = FastAPI(
    title="TalentFlow API",
    description="REST API for unified-batch data ingestion and BI reporting. Uploaded to Azure Blob Storage after referential integrity validation, and queried via Databricks SQL.",
    version="3.0.0"
)

# Routers registration
app.include_router(ingestion.router)
app.include_router(reports.router)

@app.get("/health", tags=["System"])
async def health_check():
    return {"status": "healthy"}